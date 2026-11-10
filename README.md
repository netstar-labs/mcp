# mcp — an MCP server and client in Go, standard library only

No third-party dependencies, no code generation, no reflection tricks. `go.mod`
has no require block and it stays that way. The package **serves** MCP (stdio,
unix socket, streamable HTTP) and **consumes** it (the `Client`, over a byte
stream or HTTP), sharing one `Framer` / JSON-RPC / protocol core across both
roles — which is what lets one service front another.

```
mcp/
  jsonrpc.go        JSON-RPC 2.0 wire types, ID normalisation, error codes
  framing.go        line framing (NDJSON) and Content-Length framing
  protocol.go       MCP data types for all three protocol revisions
  server.go         registry, capability derivation, pagination, lifecycle
  session.go        per-connection state machine, read/write loops, outbound calls
  dispatch.go       method handlers, URI template matching
  transport.go      stdio, Unix sockets, systemd activation, accept loop
  http.go           streamable-HTTP server transport: HTTPHandler, the per-session Framer bridge
  http_client.go    DialHTTP: the Client over streamable HTTP
  sse.go            text/event-stream reader and writer
  client.go         MCP client: Dial, Initialize, ListTools, CallTool, server→client dispatch
  doc.go            package documentation
  peercred_linux.go SO_PEERCRED
  peercred_other.go fallback
  umask_unix.go     socket creation under a umask
  server_test.go    protocol conformance tests over a real socket
  client_test.go    client↔server round trip over an in-memory pipe
  client_requests_test.go  client behaviour against a hand-played server: ping, cancel, paging, versions
  http_test.go      the HTTP transport over a real listener: round trips, elicitation, admission, sessions
  session_calls_test.go    Elicit / CreateMessage / outbound-call bounds
  dispatch_lists_test.go   completion, resources/templates/prompts lists, list_changed broadcasts
  transport_run_test.go    ServeStdio, ListenAndServeUnix
  fuzz_test.go      fuzz targets for every trust boundary
  a1_*_test.go      regressions for the A1 audit's findings (docs/audits/a1.md)
  harden_test.go    limits, quotas, deadlines, injection, cache invalidation
  audit_fixes_test.go regressions: cursor-cache bound, large-id keys, dup Content-Length
  protocol_content_test.go  Content constructors, SessionFromContext
  transport_test.go systemd no-activation paths
  helpers_test.go   test helpers (short socket paths)
  bench_test.go     benchmarks
app/mcpd/           reference daemon (the mcpd binary)
example/stdio/      minimal runnable server (one echo tool over stdio)
example/http/       a tool over streamable HTTP behind a bearer-token wrapper
docs/               introduction · executive-summary · architecture · userguide (plan/ untracked)
hack/               gate.sh (the commit gate) · fuzz.sh
build/mcp           cross-compile + package + install the daemon
```

## Documentation

- **Start here** — [docs/introduction.md](docs/introduction.md) · [docs/executive-summary.md](docs/executive-summary.md)
- **Deep dive** — [docs/architecture.md](docs/architecture.md) (this README is the reference; the docs are the guided tour)
- **Operations** — [docs/userguide.md](docs/userguide.md)
- **Examples** — [example/README.md](example/README.md); for the client/gateway
  pattern in production, [`mcpgw`](https://github.com/netstar-labs/mcpgw)

Module `github.com/netstar-labs/mcp` — the root package `mcp` is the importable
library; `app/mcpd` is the reference daemon.

## Usage

```go
srv := mcp.New(mcp.Options{Name: "mcpd", Version: "1.0.0"})

srv.AddTool(mcp.Tool{
    Name:        "sha256",
    InputSchema: json.RawMessage(`{"type":"object","properties":{"input":{"type":"string"}},"required":["input"]}`),
}, func(ctx context.Context, s *mcp.Session, args json.RawMessage) (*mcp.CallToolResult, error) {
    var in struct{ Input string `json:"input"` }
    if err := json.Unmarshal(args, &in); err != nil {
        return mcp.ToolError("invalid arguments: %v", err), nil
    }
    sum := sha256.Sum256([]byte(in.Input))
    return mcp.ToolText("%x", sum), nil
})

// Any transport, or all at once from the same registry.
go srv.ServeStdio(ctx)
go http.ListenAndServe("127.0.0.1:8080", authenticate(srv.HTTPHandler(mcp.HTTPOptions{})))
srv.ListenAndServeUnix(ctx, "/run/mcpd/mcpd.sock", mcp.UnixOptions{
    Mode:         0o660,
    Group:        "analysts",
    ReplaceStale: true,
    AllowGID:     []uint32{2001},
})
```

Capabilities are derived from what is actually registered rather than declared
separately, so the advertisement cannot drift from the implementation.

## Transports

**stdio** — line-delimited JSON on stdin/stdout, as the MCP stdio transport
specifies. Nothing else in the process may write to stdout; the default logger
goes to stderr.

**Unix socket** — the same framing over `AF_UNIX`, one session per connection,
all sessions sharing one registry. `@name` selects the Linux abstract
namespace. `FramingHeaders` switches to LSP-style `Content-Length` framing when
something in the path mangles line boundaries.

**systemd** — `ListenSystemd()` adopts `LISTEN_FDS` descriptors, so a daemon built
on this package can be socket-activated and run unprivileged. (`mcpd` itself does
not call it: it binds its own `-socket`.)

**Streamable HTTP** — `Server.HTTPHandler(HTTPOptions)` returns an `http.Handler`
(the transport as defined from revision 2025-03-26; the Server's usual version
negotiation applies, so a 2024-11-05 client is also served over it). `POST` carries
client→server messages; the reply is one JSON body or, when the server has something to say first (an
elicitation, a progress notification), an SSE stream; `GET` opens a stream for
server-initiated messages; `DELETE` ends the session. It reuses the Session
unchanged — each HTTP session is a `Framer` that queues POST bodies for the
Session's read loop and routes the Session's single writer's frames to the right
stream without ever blocking it. **It does not authenticate**: wrap the handler
in whatever does, and use `HTTPOptions.Identify` to hand that wrapper's verdict to
the transport, which binds each session to the identity that created it and shows
it to tool handlers as `Session.Identity()`. See [example/http](example/http/main.go).
Mount it on one path; set `ReadHeaderTimeout` and `ReadTimeout` (not
`WriteTimeout`, which would end every stream) on the `http.Server`.

## Client

The package consumes MCP as well as serving it — a consumer (a gateway fanning
out to a local tool server, a tool composing another) drives an upstream over the
same framing:

```go
c := mcp.Dial(conn, mcp.ClientOptions{Info: mcp.Implementation{Name: "mcpgw", Version: "0.1.0"}})
defer c.Close()
if _, err := c.Initialize(ctx); err != nil { /* … */ }
tools, _ := c.ListTools(ctx)
res, _ := c.CallTool(ctx, "resolve", map[string]any{"host": "example.com"})
```

`Dial` runs a background read loop that correlates responses by id and dispatches
server→client requests (`elicitation/create`, …) to `ClientOptions.OnRequest`,
each on its own goroutine (a handler may block on a human, or call back into the
client). The client answers `ping`, sends `notifications/cancelled` when a call's
context ends (never for `initialize`), follows `nextCursor` across `ListTools`
pages, and refuses a server's protocol version it does not speak. It is
deliberately minimal — initialize, list, call, and server-request dispatch —
enough to drive a tool server, not a feature-complete client: no resources,
prompts or sampling, and server notifications are not surfaced. Client and Server
share one `Framer` / JSON-RPC / protocol core, so they are wire-compatible by
construction.

`DialHTTP(ctx, endpoint, opts)` is the same client over streamable HTTP. Put a
bearer token in `ClientOptions.Header`, or TLS and a `RoundTripper` in
`ClientOptions.HTTPClient`. `Close` ends the server's session. A server that no
longer knows the session ends the client with `ErrSessionExpired`: dial again.

## Security notes

- The socket is created **under a umask**, so it never exists with wider
  permissions than requested, not even between `bind` and `chmod`. The parent
  directory is created `0700`, ownership and mode are verified after bind, and
  a symlink at the socket path is refused rather than followed. (`umask` is
  process-global; the window is serialised by a mutex and is a few
  microseconds around `bind`.)
- `AllowUID`/`AllowGID` are enforced from `SO_PEERCRED`, which the kernel fills
  in at connect time and the peer cannot forge. This is the only meaningful
  authentication on an abstract socket, which has no filesystem permissions at
  all. Authorise on UID/GID; treat the PID as an audit hint, since it can be
  recycled after the peer exits.
- Peer credentials are Linux-only here. Darwin and the BSDs need
  `LOCAL_PEERCRED`/`getpeereid`; see the comment in `peercred_other.go` for why
  this package does not take the dependency that would supply them.
- A live socket is never unlinked. `ReplaceStale` removes a leftover file only
  after a dial proves nothing is listening, so a restart cannot silently
  hijack a running instance.
- **Everything a peer can grow is bounded.** Frames are size-capped as they are
  read rather than after; ids (512 B), method names (128 B), batch members,
  subscriptions per session, outstanding server-to-client calls, header count
  and header line length all have ceilings. A handler panic costs one request,
  and its stack trace is truncated before it reaches the log.
- **Deadlines sit below the framer.** `IdleTimeout` drops a silent peer;
  `HandshakeTimeout` (a timer, not a read deadline: a Ready session that is merely
  quiet is not affected) drops one that connects and never initialises;
  `WriteTimeout` drops one that stops reading — without it a client can stall
  the session writer and back-pressure every handler behind it.
- **`MaxConnsPerUID`** stops one local account from consuming the whole
  connection budget, which matters most on an abstract socket where filesystem
  permissions do not apply.
- **The HTTP transport bounds what a stranger can hold.** Sessions are capped
  (`MaxSessions`) and a session that has not finished the handshake is dropped on a
  short timer (`HandshakeTimeout`), so unauthenticated `initialize` requests cannot
  park on slots for the full idle TTL; idle sessions expire (`SessionTTL`); a request
  body is read through the Server's `MaxMessageBytes`; the queue to the Session and
  the queue to each stream are bounded; a stream whose reader stops draining is
  closed rather than allowed to stall the Session's writer; every stream write
  carries a deadline. A request carrying a browser `Origin` is refused unless the
  origin is listed (the DNS-rebinding guard the spec requires), and `AllowedHosts`
  closes the rebinding case that carries no `Origin`. A session id is 128 random
  bits, and — when `Identify` is set — is useless to any other identity (a mismatch
  looks like an unknown session).
- **Peer-supplied text is sanitised before it is logged.** A client naming
  itself `"evil\n2026/01/01 auth: access granted"` would otherwise forge an
  audit record. Sanitisation happens once, at the initialize boundary, so no
  downstream logging site has to remember.
- `ToolAnnotations` are server-supplied hints and are untrusted by
  construction. Nothing in this package treats them as a control.

## Protocol coverage

Negotiates `2024-11-05`, `2025-03-26`, `2025-06-18`; echoes the client's
version when supported, otherwise answers with the newest and lets the client
decide.

| Area | Methods |
|---|---|
| Lifecycle | `initialize`, `notifications/initialized`, `ping` |
| Tools | `tools/list`, `tools/call`, `notifications/tools/list_changed` |
| Resources | `list`, `templates/list`, `read`, `subscribe`, `unsubscribe`, `notifications/resources/updated` |
| Prompts | `prompts/list`, `prompts/get`, `notifications/prompts/list_changed` |
| Logging | `logging/setLevel`, `notifications/message` (RFC 5424 severity gate) |
| Completion | `completion/complete` |
| Progress / cancel | `notifications/progress`, `notifications/cancelled` |
| Server → client | `sampling/createMessage`, `roots/list`, `elicitation/create` |

Structured tool output (`structuredContent`, `outputSchema`), resource links,
tool annotations, `_meta` passthrough, and opaque scoped pagination cursors are
all supported.

JSON-RPC batching existed only in `2025-03-26`; `2025-06-18` removed it. Batches
are accepted when `Options.AcceptBatch` is set and are never emitted.

## Performance

Measured on the benchmarks in `bench_test.go`; before/after refers to the
first working version of this package.

| | before | after |
|---|---|---|
| `tools/list`, 100 tools, server side | 90.9 µs, 39.5 KB | **884 ns, 956 B** |
| URI template match (dispatch path) | 375 ns, 5 allocs | **23 ns, 0 allocs** |
| Frame read | 167 ns, 374 B | **32 ns, 17 B** |
| `idKey` | 196 ns, 2 allocs | **50 ns, 1 alloc** |
| Marshal a tool result | 679 ns, 2 allocs | **587 ns, 1 alloc** |
| Memory per new connection | 148 KB | **24.6 KB** |
| Tool call, full socket round trip | 24.1 µs, 76 allocs | 23.8 µs, 66 allocs |

Where the wins come from:

- **List pages are cached in encoded form**, keyed by a generation counter that
  every registry mutation bumps. Re-encoding a hundred tool definitions on
  every `tools/list` was by far the most expensive thing a well-behaved client
  could ask for. Only offsets on a page boundary are cached, so a client
  cannot grow the cache by asking for arbitrary ones.
- **URI templates compile at registration**, not per read, and the dispatcher
  uses `Matches`, which answers yes/no without building the variable map.
- **The framer reuses its read buffer.** This is safe because `encoding/json`
  copies every `json.RawMessage` it produces; `TestPipelinedFramesDoNotAlias`
  pins that down under `-race`.
- **`marshal` pools its buffer and encoder**, returning a fresh copy and
  keeping oversized scratch buffers out of the pool.
- **Per-connection buffers dropped from 64 KiB to 4 KiB** per direction. Two
  64 KiB buffers per session is 128 MiB at a thousand sessions — a
  memory-exhaustion vector wearing a performance costume. Larger frames are
  read incrementally.
- **`_meta` is probed with a substring test** before the second decode of
  params that progress-token extraction used to cost on every single request.

The full round trip barely moves because it is latency-bound, not CPU-bound;
the allocation count is what the server actually controls.

## A correctness bug the hardening found

`idKey` normalised every numeric id through `float64`. Ids above 2^53 therefore
collided: `9007199254740993` and `9007199254740992` produced the same
correlation key, so a client using large integer ids could have had two
responses cross-wired. Integers now take an exact `int64` path, with the float
path reserved for non-integral values so `7`, `7.0` and `7e0` still agree.

## Two ordering rules the tests pin down

Both were bugs during development, and both are the kind that only appear under
a client that pipelines:

1. **Notifications are processed inline, in arrival order.** Dispatching them
   concurrently lets a request overtake the `notifications/initialized` that
   authorises it, and lets a cancellation be applied before the request it
   names has been registered.
2. **`initialize` runs on the read loop, not in a worker.** A conformant client
   waits for the result before sending `notifications/initialized`, but a
   pipelining one does not, and the notification must not overtake the request
   it completes.

A third: when the peer stops sending, the session drains in-flight handlers and
flushes queued replies before tearing down the transport. Without it, a client
that half-closes — or a pipe that hits EOF right after the last request —
silently loses its answers.

## What is deliberately not here

- **Authentication, authorisation and OAuth on the HTTP transport.** The handler
  is wire, not policy: it takes whatever answer a wrapper gives it (`Identify`) and
  enforces only that a session belongs to the identity that made it. Tokens, mTLS,
  rate limits and audit belong to the layer in front, as in
  [`mcpgw`](https://github.com/netstar-labs/mcpgw). The protocol's own OAuth
  resource-server metadata is not served.
- **Stream resumption.** `Last-Event-ID` is ignored and a frame in flight when an
  SSE stream drops is lost; a dropped POST stream is not a cancellation: the request's
  response is discarded when it arrives, and its id stays reserved until it does (a
  retry that reuses the id is answered 400 until the first call finishes or the
  client sends `notifications/cancelled`).
- **The 2024-11-05 HTTP+SSE transport, and HTTP batching** (removed from the
  protocol in 2025-06-18).
- **Sessions shared between processes.** An HTTP session lives in the memory of the
  process that made it; a load balancer must pin a session to one node.
- **JSON Schema validation of tool arguments.** Schemas are published to the
  client; validating them server-side means a schema validator, and there
  isn't one in the standard library. Handlers unmarshal into a typed struct and
  return `ToolError` on mismatch, which is where the check belongs anyway.
- **URI template expansion.** Only matching is implemented, for the RFC 6570
  simple (`{var}`) and reserved (`{+var}`) forms MCP servers actually use.

## Tests

```
hack/gate.sh                          # the commit gate: vet (both platforms), -race, staticcheck, deadcode, fuzz, tidy
go test -race -count=2 ./...
go test -run=XXX -bench=. ./...
FUZZTIME=30s hack/fuzz.sh             # every fuzz target
```

(Build/test standalone with `GOWORK=off`; the module has no third-party
requires.)

The tests cover about 85% of the root package's statements (`go test -cover .`), and
there are seven fuzz targets (the two framers, the SSE reader, `Accept` parsing, id
keys, URI templates, and the HTTP POST path). Two tests skip off Linux, the ones that
need `SO_PEERCRED`. The hardening controls were mutation-tested — neutering
`sanitize`, the batch cap, cache invalidation and the integer id path each produces
a failing test — and so were the HTTP transport's and the A1 audit's fixes: removing
the identity binding, the Origin and Host guards, the session cap, the handshake
timers, the cancel handling, the non-blocking stream route, the backlog bound, the
body cap, the version check, the duplicate-id and reserved-id checks, the client's
ordering of notifications, its redirect refusal and its slot accounting each fails a
test.

The suite drives a real Unix socket with a hand-rolled client, so it exercises
the wire format rather than the server's own encoder: handshake and version
fallback, pipelined handshake, pre-initialize rejection, tool dispatch and
structured output, the execution-error vs protocol-error distinction, panic
containment, pagination and cross-collection cursor rejection, cancellation
reply suppression, resource templates, subscription routing, prompt argument
validation, a server-initiated `roots/list` round trip, progress tokens, the
logging severity gate, batching, parse errors, frame size limits, header
framing, `SO_PEERCRED`, the peer allow-list, and concurrent dispatch.

`client_test.go` drives the `Client` against a real `Server` over an in-memory
pipe: initialize, list, call, the execution-error vs protocol-error distinction,
and unknown-tool — client and server from the same package, so the round trip
proves the two are wire-compatible.

`client_requests_test.go` plays the server by hand to see what the `Client` puts on
the wire: `ping` is answered, a blocked or re-entrant `OnRequest` does not stall the
read loop, a cancelled call sends `notifications/cancelled` (not for `initialize`),
`ListTools` pages and refuses a cursor loop, an unsupported protocol version is an
error. `http_test.go` drives the HTTP transport over a real listener: round trips
through `DialHTTP`, a differential against the pipe transport (one Session, two
transports, the same answers), JSON-or-SSE replies, an elicitation answered by a
separate POST, Origin and Host admission, malformed requests, session binding,
eviction, the session cap, cancellation, shutdown, and the bridge's own bounds.

`harden_test.go` covers the limits and quotas: log injection, oversized ids and
method names, batch member caps, subscription quotas, idle and handshake
timeouts, per-uid connection limits, symlinked socket paths, socket permission
width, header floods, read-buffer aliasing, list cache invalidation, and
large-integer id collisions.

## License

Licensed under the Apache License, Version 2.0 — see [LICENSE](LICENSE).
