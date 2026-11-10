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
- `Client.Initialize` advertises the elicitation capability only when `ClientOptions.OnRequest`
  is set (commit ed9b40f, after v0.2.0); v0.2.0 advertised it always, so a server's
  `Session.Elicit` against a client with no handler now fails at the capability check instead of
  mid-call with method-not-found.
- `ClientOptions.HTTPClient == nil` means a client that follows no redirects (a 307/308 would
  replay the POST, its body and the caller's credential headers to the redirect target); a
  redirect is reported as an error (on the GET stream it ends the stream loop instead of retrying
  the same URL), so an `http`→`https` 301/308 on a POST now fails. Supply your own `HTTPClient` to
  follow redirects.
- `ListTools` returns an error once a server's listing exceeds 32 MiB across pages.
- `UnixOptions.HandshakeTimeout` no longer sets a read deadline for the connection's whole life.
  With `IdleTimeout == 0` ("disabled") a Ready session that was merely quiet was dropped after
  `HandshakeTimeout` (30s under `mcpd`, which always sets one); the handshake itself is still
  bounded by its own timer.
- `mcpd`'s `read_file` and the `file://` template read regular files only, up to 1 MiB; a FIFO,
  device or directory, or a larger file, is an error. (A FIFO under the root parked the handler
  forever, and a larger reply is a frame a default client refuses, which ends its session.)
- A client's `notifications/initialized` no longer carries `"params":null`; params are omitted, as
  the Session already did.
- `jsonrpc` must be `"2.0"` on every message over stdio and sockets, as it already was for a single
  request and over HTTP: a notification or response with `"1.0"` or no version, and a batch
  member with either, is answered with an invalid-request error (null id unless it was a request)
  and not processed; the error does not quote the peer's version string. A peer that omits
  `jsonrpc` on `notifications/initialized` never becomes Ready. Unchanged: a non-string `jsonrpc`
  is a parse error (null id), and member names match case-insensitively (`JSONRPC` is accepted).
  The Client still accepts any version from a server.
- `Options.MaxQueuedBytes` (default 8 × `MaxMessageBytes`, 64 MiB at the default) bounds the bytes
  held by a session's queued and running requests; past it a request gets the "overloaded" error.
  A client pipelining many large requests now gets errors where it got silent queueing; one
  session could previously pin about 4 GiB.
- `Server.Broadcast` and `NotifyResourceUpdated` no longer block on a peer that stopped reading:
  a session whose outbound queue is full is waited on while its writer makes progress and closed
  once it makes none for 5 s; the other sessions are served first. A peer that reads slowly is
  not closed, and `Broadcast` then waits as long as it takes to drain what is queued ahead of the
  notification. `Session.Notify` still blocks.
- `mcpd -socket` on macOS and the BSDs serves connections with the default flags. `MaxConnsPerUID`
  alone no longer makes admission demand peer credentials; a connection whose credentials cannot be
  read is admitted and not counted per uid, and `Serve` logs that once. Linux behaviour is
  unchanged for unix-socket listeners; on Linux a listener that cannot supply credentials (a
  systemd-passed TCP socket, a wrapping `Listener`) used to refuse every connection under this
  option and now serves them uncounted. An explicit policy (`RequirePeerCred`, `AllowUID`,
  `AllowGID`) still fails closed.
- `idKey` no longer converts out-of-range floats: the id `9223372036854775808.0` (2^63) aliased
  `9223372036854775807` on arm64 and not on amd64.

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
- `HTTPOptions.MaxSessionsPerIdentity` (default 0, off): bounds the live sessions one `Identify`
  result may hold; initialize past it is answered 429. Sessions with no identity are not counted.
- Package documentation (`doc.go`), `SECURITY.md`, this changelog.
- Tests: the HTTP transport over a real listener (round trips, a differential against the pipe
  transport, elicitation over a POST stream and a GET stream, admission, sessions, cancellation,
  shutdown, the bridge's bounds), the Client against a hand-played server, `Session.Elicit` /
  `CreateMessage` / outbound-call bounds, completion, resource/template/prompt lists, `list_changed`
  broadcasts, `ServeStdio`, `ListenAndServeUnix`, `mcpd`'s HTTP flags. Coverage of the root package
  72.0% → about 85%. Seven fuzz targets: both framers, the SSE reader, `Accept` parsing, id keys, URI
  templates, the HTTP POST path.
- `hack/gate.sh` (the commit gate), `hack/fuzz.sh`, CI (`.github/workflows/ci.yml`: test on Linux
  and macOS, fuzz, staticcheck, deadcode, tidy, govulncheck) and Dependabot for the pinned actions.

### Fixed

Found by the A1 audit of this branch (records in `docs/audits/`); ids are the audit's.

- A1 C-2: `streamFramer.Close` took the write lock before closing the connection, so a peer that
  stopped reading blocked `Close`, and with it `Session.Close`, `Serve` on shutdown,
  `Server.Close` and `Client.Close`, for as long as the peer cared to stay silent.
- A1 C-3: `Client.call` ignored its context while the write to the peer was blocked, and so did
  every caller queued behind it. The write lock is now a semaphore a caller waits for with its
  own context, so a call that gives up leaves nothing behind but the one write already in
  progress; a write abandoned for its context sends `notifications/cancelled` (bounded, 5s),
  since it may have reached the wire, and a request whose caller gave up before it was written
  is never sent. `Framer.Close` no longer flushes if a write is in progress (see C-2).
- A1 C-13: header framing allocated the declared body length before any body byte arrived; 27
  bytes of input could cost 8 MiB. It now grows as the body arrives.
- A1 C-1: an HTTP request id could be reused after its POST dropped, while the Session was still
  running the first call, and the first call's reply was delivered as the answer to the second. An
  id now stays reserved until the Session answers it or the client cancels it.
- A1 C-7: an open GET stream kept a session that never finished the handshake alive past
  `HandshakeTimeout`, so one connection per slot defeated the option. The handshake has its own
  timer, which a stream does not hold off.
- A1 C-4: the HTTP client's exchange slots were released only when the server ended a POST, so 64
  calls to a server that ignored cancellation left the client unable to make another. A cancelled
  call now aborts its own POST before its cancellation is queued, so it does not depend on the
  cancellation getting through; cancellations are sent asynchronously on a budget of their own (8,
  extras dropped); a request past the slots is refused at once; and a notification waits a
  bounded time for a slot.
- A1 C-5: an HTTP 200 JSON reply that was not the response to a request (a null-id error, a batch,
  another id) left the call waiting for its deadline. It now fails the call.
- A1 C-6: a notification the server rejected (a 401 on `notifications/initialized`) was
  swallowed, so `Initialize` reported success. The write now returns the error. (A notification
  answered 200 or 201 with no body is accepted, though the spec says 202.)
- A1 C-16, B-1: see the behaviour changes above.
- The client answered a server's `ping` with method-not-found.
- A blocked or re-entrant `OnRequest` handler stalled the client's read loop, so unrelated
  responses were not delivered and a handler that called back into the client deadlocked.
- A cancelled client call sent no `notifications/cancelled`, so the work continued on the server.

## v0.2.0

Adds the `Client` (`Dial`, `Initialize`, `ListTools`, `CallTool`), which shares the server's
Framer, JSON-RPC and protocol types.

## v0.1.0

The server: stdio, Unix-socket and systemd transports; tools, resources, prompts, completion,
logging, progress and cancellation; server→client sampling, roots and elicitation; bounded
everything, deadlines below the framer, sanitised logs.
