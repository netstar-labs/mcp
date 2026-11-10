# mcp — architecture

A flat library at the repo root (`package mcp`), one concern per file, with the
reference daemon under `app/mcpd`. It implements **both roles** — the server
(most of the files below) and a matching `Client` (`client.go`) that consumes an MCP
server over the same wire — and **three transport families**: stdio, Unix sockets,
and streamable HTTP. The README is the full reference (security, performance,
protocol coverage, the bugs the hardening found); this is the guided tour of the
subsystems and the data flow.

## Data flow

```
transport (stdio | unix | systemd | http)
   │  accept + peer creds + admission   (http: Origin/Host guard, session lookup, identity bind)
   ▼
framer (NDJSON lines | Content-Length headers)   ── size-capped as it reads
   │  one message at a time, buffer reused (RawMessage is copied by encoding/json)
   ▼
session  (per-connection state machine: read loop → dispatch → write loop)
   │  initialize on the read loop; notifications inline in arrival order
   ▼
dispatch (method → handler; URI-template match for resources)
   │
   ▼
registry (tools / resources / prompts)  ──▶ handler ──▶ result ──▶ framer ──▶ peer
```

## Subsystems (one file each)

- **`jsonrpc.go`** — JSON-RPC 2.0 wire types, error codes, and id normalisation
  (integers take an exact `int64` path so ids above 2^53 can't collide — a real
  correlation bug the hardening caught).
- **`framing.go`** — two `Framer`s: newline-delimited JSON (the MCP stdio norm) and
  LSP-style `Content-Length`. Reads are size-capped incrementally; the read buffer
  is reused (safe because `encoding/json` copies each `RawMessage`).
- **`protocol.go`** — the MCP data types for all three revisions: `Content`
  constructors, `Tool`/`Resource`/`Prompt`, results, capabilities, annotations.
- **`server.go`** — the registry, capability derivation (from what's registered),
  encoded list-page cache (keyed by a generation counter every mutation bumps),
  pagination cursors, lifecycle, `New`/`Add*`/`Broadcast`/`Notify*`.
- **`session.go`** — the per-connection state machine: read/write loops, request
  concurrency, outbound server→client calls, drain-on-close (finish in-flight
  handlers and flush queued replies before teardown).
- **`dispatch.go`** — method handlers and URI-template matching (`Matches` answers
  yes/no without building the variable map).
- **`client.go`** — the other role on the same wire. `Dial` opens a `Client` over
  any `io.ReadWriteCloser` (Unix socket, stdio pipe pair, in-memory pipe); a
  background read loop correlates responses by JSON-RPC id, serialises writes, and
  dispatches server→client requests (`elicitation/create`, and `ping`, which it
  always answers) to an `OnRequest` handler, each on its own goroutine so a handler
  may block on a human or call back into the client. A call whose context ends sends
  `notifications/cancelled` (never for `initialize`); `ListTools` follows
  `nextCursor` (bounded, and a repeated cursor is an error). `Initialize` /
  `ListTools` / `CallTool` / `Close`. Reuses `framing.go`, `jsonrpc.go`, and
  `protocol.go` unchanged — one wire, two roles — and is deliberately minimal (no
  resources/prompts/sampling, no server notifications). Its production consumer is
  [`mcpgw`](https://github.com/netstar-labs/mcpgw), which mounts an upstream tool
  server's tools as gated proxies over a unix socket or a spawned stdio child.
- **`http.go`** — the streamable-HTTP server transport; see "The HTTP bridge" below.
- **`http_client.go`** — `DialHTTP`: the same `Client`, over a `Framer` that turns
  each message into a POST. A request is sent on its own goroutine (the client holds
  a write lock around `WriteMessage`, and a POST that waited for its response would
  serialise every concurrent call); a notification or reply is sent before
  `WriteMessage` returns, because separate HTTP requests have no order and
  `notifications/initialized` must reach the server before the next request.
- **`sse.go`** — a `text/event-stream` reader (size-capped as bytes arrive, like the
  framer) and a writer that puts a deadline on every write.
- **`transport.go`** — `ServeStdio`, `ListenUnix` (umask creation, stale reclaim,
  symlink refusal, mode/owner verification), `ListenSystemd`, the accept loop with
  admission, and the connection gate (total + per-UID caps).
- **`peercred_linux.go` / `peercred_other.go`** — `SO_PEERCRED` on Linux; a
  fail-closed stub elsewhere (macOS/BSD would need `golang.org/x/sys`, against the
  stdlib-only stance).
- **`umask_unix.go`** — socket creation under a serialized umask so the file never
  exists wider than requested.

## Design trade-offs

- **Capabilities are derived, not declared** — the advertisement can't drift from
  the code; the cost is that "turning off" a capability means not registering it.
- **List pages cached in encoded form** — re-encoding 100 tool definitions per
  `tools/list` was the most expensive thing a well-behaved client could ask for;
  only page-boundary offsets are cached, so a client can't grow the cache.
- **Deadlines below the framer** — idle/handshake/write timeouts drop silent,
  never-initialising, or non-reading peers before they can wedge the writer.
- **Everything peer-growable is bounded** — frame/id/method sizes, batch members,
  subscriptions, pending calls, per-UID connections. Hostile input is the design
  center, not an afterthought.
- **Peer creds are Linux-only** — the honest platform boundary of a stdlib-only
  build; off-Linux, admission that needs creds (an allow-list, or `MaxConnsPerUID`)
  fails closed and socket-file permissions carry the access control.
- **stdio owns stdout** — nothing else in the process may write there; the logger
  defaults to stderr.
- **HTTP is wire, not policy** — the handler does not authenticate. It takes the
  answer of whatever wraps it (`HTTPOptions.Identify`) and enforces only that a
  session belongs to the identity that made it. That keeps the credentials, the
  rate limits and the audit trail in the layer that owns them (as in
  [`mcpgw`](https://github.com/netstar-labs/mcpgw)), and the transport small enough
  to audit.

## The HTTP bridge

Streamable HTTP is request-scoped; a `Session` wants one persistent `Framer`. The
bridge is `httpSession`, a `Framer` per HTTP session, and the only hard part of the
transport:

- **Inbound.** Each POST body is parsed once at the boundary (size, JSON, id and
  method limits, kind) and queued, in order, for `ReadMessage` — which is the
  Session's unchanged read loop. A full queue answers 503, never blocks.
- **Outbound.** The Session has one writer goroutine, and every handler's reply
  queues behind it, so `WriteMessage` must never block. It routes each frame with
  non-blocking sends: a **response** to the POST waiting for that id; a
  **server-initiated** frame to the GET stream if one is open, else to the oldest
  POST still waiting (a progress notification or elicitation mid-call), else into a
  bounded backlog for the next GET. A stream that does not drain is closed, not
  waited for. Nothing a client does can stall another client's replies.
- **JSON or SSE, decided late.** A POST waits on its own channels. If the response
  is the first thing the Session says, the reply is one JSON body; if anything is
  routed to the POST first, the reply becomes an SSE stream carrying those frames
  and then the response. Everything queued ahead of the response is delivered
  before it (one writer, in order).
- **Cancellation is two different things.** `notifications/cancelled` ends the
  POST of the request it names (the Session suppresses that reply, so nothing else
  would end it). A dropped connection is *not* a cancellation: the request keeps
  running and its response is discarded when it arrives. Its id stays reserved
  (the waiter is marked abandoned, and nothing is routed to it) until the Session
  answers it or the client cancels it, so a retry that reuses the id is answered
  400 rather than handed the first call's reply.
- **Lifecycle.** A session id is minted at `initialize` (128 random bits) and
  returned only with a successful handshake. Sessions are capped, expire on a short
  handshake timer and then an idle TTL (an open stream or in-flight request holds
  one open), and are released from one place, `Close`, whoever asks: `DELETE`, the
  timer, `Server.Close`, or the Session ending. Closing cancels the context the
  Session's handlers run under.
- **Known limits.** Elicitations are routed by "oldest waiting POST", not by the
  request that raised them, because `Session.Call` does not carry the originating
  request id; the spec says such messages SHOULD relate to the request, and a client
  correlates by JSON-RPC id regardless of stream, so this is cosmetic. No
  resumption: a frame in flight when a stream drops is lost.
