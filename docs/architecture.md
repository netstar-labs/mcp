# mcp — architecture

A flat library at the repo root (`package mcp`), one concern per file, with the
reference daemon under `app/mcpd`. It now implements **both roles** — the server
(most of the files below) and a matching `Client` (`client.go`) that consumes an MCP
server over the same wire. The README is the full reference (security, performance,
protocol coverage, the bugs the hardening found); this is the guided tour of the
subsystems and the data flow.

## Data flow

```
transport (stdio | unix | systemd)
   │  accept + peer creds + admission
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
  dispatches server→client requests (`elicitation/create`) to an `OnRequest`
  handler. `Initialize` / `ListTools` / `CallTool` / `Close`. Reuses `framing.go`,
  `jsonrpc.go`, and `protocol.go` unchanged — one wire, two roles — and is
  deliberately minimal (no resources/prompts/sampling, no pagination past the first
  page).
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
  build; off-Linux, admission that needs creds fails closed and socket-file
  permissions carry the access control.
- **stdio owns stdout** — nothing else in the process may write there; the logger
  defaults to stderr.
- **HTTP is scoped, not built** — server and client both speak stdio + Unix-socket
  framing today. The streamable-HTTP transport is designed (a per-session
  `httpFramer` presenting a `Framer` to the existing Session, unchanged —
  [plan/scope-http-transport.md](plan/scope-http-transport.md)) but not yet
  implemented; the `Framer` interface is the seam it plugs into.
