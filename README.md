# mcp — an MCP server in Go, standard library only

No third-party dependencies, no code generation, no reflection tricks. `go.mod`
has no require block and it stays that way.

```
mcp/
  jsonrpc.go        JSON-RPC 2.0 wire types, ID normalisation, error codes
  framing.go        line framing (NDJSON) and Content-Length framing
  protocol.go       MCP data types for all three protocol revisions
  server.go         registry, capability derivation, pagination, lifecycle
  session.go        per-connection state machine, read/write loops, outbound calls
  dispatch.go       method handlers, URI template matching
  transport.go      stdio, Unix sockets, systemd activation, accept loop
  peercred_linux.go SO_PEERCRED
  peercred_other.go fallback
  umask_unix.go     socket creation under a umask
  server_test.go    protocol conformance tests over a real socket
  harden_test.go    limits, quotas, deadlines, injection, cache invalidation
  audit_fixes_test.go regressions: cursor-cache bound, large-id keys, dup Content-Length
  protocol_content_test.go  Content constructors, SessionFromContext
  transport_test.go systemd no-activation paths
  helpers_test.go   test helpers (short socket paths)
  bench_test.go     benchmarks
app/mcpd/           reference daemon (the mcpd binary)
example/stdio/      minimal runnable server (one echo tool over stdio)
docs/               introduction · executive-summary · architecture · userguide
build/mcp           cross-compile + package + install the daemon
```

## Documentation

- **Start here** — [docs/introduction.md](docs/introduction.md) · [docs/executive-summary.md](docs/executive-summary.md)
- **Deep dive** — [docs/architecture.md](docs/architecture.md) (this README is the reference; the docs are the guided tour)
- **Operations** — [docs/userguide.md](docs/userguide.md)
- **Examples** — [example/README.md](example/README.md)

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

// Either transport, or both at once from the same registry.
go srv.ServeStdio(ctx)
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

**systemd** — `ListenSystemd()` adopts `LISTEN_FDS` descriptors, so the daemon
can be socket-activated and run unprivileged.

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
  `LOCAL_PEERCRED`/`getpeereid`, neither of which package `syscall` wraps —
  see the comment in `peercred_other.go`.
- A live socket is never unlinked. `ReplaceStale` removes a leftover file only
  after a dial proves nothing is listening, so a restart cannot silently
  hijack a running instance.
- **Everything a peer can grow is bounded.** Frames are size-capped as they are
  read rather than after; ids (512 B), method names (128 B), batch members,
  subscriptions per session, outstanding server-to-client calls, header count
  and header line length all have ceilings. A handler panic costs one request,
  and its stack trace is truncated before it reaches the log.
- **Deadlines sit below the framer.** `IdleTimeout` drops a silent peer;
  `HandshakeTimeout` drops one that connects and never initialises;
  `WriteTimeout` drops one that stops reading — without it a client can stall
  the session writer and back-pressure every handler behind it.
- **`MaxConnsPerUID`** stops one local account from consuming the whole
  connection budget, which matters most on an abstract socket where filesystem
  permissions do not apply.
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

- **HTTP/SSE transport.** Streamable HTTP needs session resumption, origin
  validation, and an auth story; that is a separate piece of work, and the
  `Framer` interface is where it plugs in.
- **JSON Schema validation of tool arguments.** Schemas are published to the
  client; validating them server-side means a schema validator, and there
  isn't one in the standard library. Handlers unmarshal into a typed struct and
  return `ToolError` on mismatch, which is where the check belongs anyway.
- **URI template expansion.** Only matching is implemented, for the RFC 6570
  simple (`{var}`) and reserved (`{+var}`) forms MCP servers actually use.

## Tests

```
go test -race -count=2 ./...
go test -run=XXX -bench=. ./...
```

(Build/test standalone with `GOWORK=off`; the module has no third-party
requires.)

55 tests, no skips on Linux. The hardening controls were mutation-tested —
neutering `sanitize`, the batch cap, cache invalidation and the integer id path
each produces a failing test, so none of those tests are vacuous.

The suite drives a real Unix socket with a hand-rolled client, so it exercises
the wire format rather than the server's own encoder: handshake and version
fallback, pipelined handshake, pre-initialize rejection, tool dispatch and
structured output, the execution-error vs protocol-error distinction, panic
containment, pagination and cross-collection cursor rejection, cancellation
reply suppression, resource templates, subscription routing, prompt argument
validation, a server-initiated `roots/list` round trip, progress tokens, the
logging severity gate, batching, parse errors, frame size limits, header
framing, `SO_PEERCRED`, the peer allow-list, and concurrent dispatch.

`harden_test.go` covers the limits and quotas: log injection, oversized ids and
method names, batch member caps, subscription quotas, idle and handshake
timeouts, per-uid connection limits, symlinked socket paths, socket permission
width, header floods, read-buffer aliasing, list cache invalidation, and
large-integer id collisions.
