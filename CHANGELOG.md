# Changelog

All notable changes to mcp. The format follows Keep a Changelog; versions are `v0.y.z`, and the
module has no dependencies.

## Unreleased

Adds the streamable-HTTP transport (server and client), fixes three client defects, and fills the
test, fuzz, CI and documentation gaps a review found. The Client changes below alter behaviour that
existing callers may have relied on; the notes say which.

### Behaviour changes to check

- `Client.ListTools` returns every page, following `nextCursor`; it returned only the first page.
  A server that repeats a cursor, or pages past 1000 pages, is now an error.
- `Client.Initialize` returns an error when the server's chosen protocol version is not one this
  package speaks, and returns the error from writing `notifications/initialized` instead of
  discarding it.
- A `ClientOptions.OnRequest` handler now runs on its own goroutine, with a context that ends when
  the server cancels that request or the client closes. It ran on the read loop with
  `context.Background()`. A handler that relied on being serialised with other server requests no
  longer is.
- A call on a client whose connection ended for a reason returns that reason; it returned "client
  closed".

### Added

- `Server.HTTPHandler(HTTPOptions)`: MCP over streamable HTTP (revisions 2025-03-26 and
  2025-06-18). POST for client→server messages, replied to with one JSON body or an SSE stream
  (chosen after the handler has run, so an elicitation or progress notification mid-call upgrades
  the reply); GET for a server→client stream; DELETE to end the session. Session ids are 128 random
  bits. Sessions are capped (`MaxSessions`), dropped on a short timer until the handshake completes
  (`HandshakeTimeout`) and on an idle TTL (`SessionTTL`). Browser `Origin`s are refused unless
  listed; `AllowedHosts` guards DNS rebinding. It does not authenticate: `HTTPOptions.Identify`
  carries the wrapper's verdict, binds each session to the identity that created it, and is shown
  to tool handlers as `Session.Identity()` (new, with `ConnConfig.Identity`). It does not resume a
  dropped stream, batch, speak the 2024-11-05 HTTP+SSE transport, or share sessions across nodes.
- `DialHTTP(ctx, endpoint, ClientOptions)`: the Client over streamable HTTP, with
  `ClientOptions.HTTPClient` and `ClientOptions.Header` (a bearer token goes there), and
  `ErrSessionExpired`.
- Client: answers `ping`; sends `notifications/cancelled` when a call's context ends (never for
  `initialize`); applies `notifications/cancelled` from the server to the handler of that request.
- `CodeRequestCancelled` (-32800).
- `mcpd -http`, `-http-origin`, `-http-host`, `-http-any-iface`; `-http` refuses a non-loopback
  address unless `-http-any-iface` is given. `example/http`: a tool behind a bearer-token wrapper.
- Package documentation (`doc.go`), `SECURITY.md`, this changelog.
- Tests: the HTTP transport over a real listener (round trips, a differential against the pipe
  transport, elicitation over a POST stream and a GET stream, admission, sessions, cancellation,
  shutdown, the bridge's bounds), the Client against a hand-played server, `Session.Elicit` /
  `CreateMessage` / outbound-call bounds, completion, resource/template/prompt lists, `list_changed`
  broadcasts, `ServeStdio`, `ListenAndServeUnix`, `mcpd`'s HTTP flags. Coverage of the root package
  72.0% → 84.1%. Seven fuzz targets: both framers, the SSE reader, `Accept` parsing, id keys, URI
  templates, the HTTP POST path.
- `hack/gate.sh` (the commit gate), `hack/fuzz.sh`, CI (`.github/workflows/ci.yml`: test on Linux
  and macOS, fuzz, staticcheck, deadcode, tidy, govulncheck) and Dependabot for the pinned actions.

### Fixed

- The client answered a server's `ping` with method-not-found.
- A blocked or re-entrant `OnRequest` handler stalled the client's read loop, so unrelated
  responses were not delivered and a handler that called back into the client deadlocked.
- A cancelled client call sent no `notifications/cancelled`, so the work continued on the server.

## v0.2.0

Adds the `Client` (`Dial`, `Initialize`, `ListTools`, `CallTool`), which shares the server's
Framer, JSON-RPC and protocol types, and advertises elicitation only when a handler can answer it.

## v0.1.0

The server: stdio, Unix-socket and systemd transports; tools, resources, prompts, completion,
logging, progress and cancellation; server→client sampling, roots and elicitation; bounded
everything, deadlines below the framer, sanitised logs.
