# Security

`mcp` is a library (and a reference daemon, `mcpd`) that serves and consumes the Model Context
Protocol. It reads attacker-controlled bytes from stdio, Unix sockets and HTTP, so the parsers
and limits are the security surface.

## Reporting

Report a vulnerability privately, through the repository's Security tab: choose "Report a
vulnerability" (GitHub private vulnerability reporting), rather than opening a public issue. If that
option is not shown, the repository has not enabled it; open an issue that asks for a private contact
and says nothing about the problem itself. Expect an acknowledgement within a week.

Only the latest release is supported; versions are `v0.y.z`, and a fix ships as a new release.

## What it defends against

- **Hostile input on any transport.** Frame, id and method sizes, JSON-RPC batch members,
  subscriptions, in-flight and outstanding calls, connections and (over HTTP) sessions, queues and
  stream buffers are all bounded, and the size caps are enforced as bytes arrive, not after.
- **A peer that stalls.** Idle, handshake and write deadlines sit below the framer on sockets; over
  HTTP every stream write carries a deadline, a stream that stops draining is closed, and a session
  that never finishes the handshake is dropped on a short timer.
- **Log forging.** Peer-supplied text (client names, ids, methods, remote addresses) is sanitised
  before it reaches a log line.
- **Local socket hijack and over-wide sockets.** Unix sockets are created under a umask, ownership
  and mode are verified, a symlink at the path is refused, a live socket is never unlinked, and
  admission can be limited by kernel-attested UID/GID (`SO_PEERCRED`, Linux only).
- **DNS rebinding and cross-origin use of the HTTP endpoint.** A request carrying a browser `Origin`
  is refused unless listed in `HTTPOptions.AllowedOrigins`; `AllowedHosts` closes the rebinding case
  that carries no `Origin`.
- **One authenticated caller using another's HTTP session.** With `HTTPOptions.Identify` set, a
  session is bound to the identity that created it, and any other identity is told the session does
  not exist.

## What it does not defend against

- **Unauthenticated callers on the HTTP transport.** `HTTPHandler` does not authenticate, by design:
  it is wire, not policy. Anything that can reach the handler can use it. Put it behind something
  that authenticates (a token check, mTLS, a gateway) and use `Identify` to bind sessions to the
  result. `mcpd -http` binds loopback unless `-http-any-iface` is given, for that reason.
- **Rate limiting and abuse beyond the caps above.** There is no per-client request rate limit and
  no per-address session limit; `MaxSessions` is global, and `MaxSessionsPerIdentity` bounds only
  an identity the wrapper supplied through `Identify`. Those belong to the layer in front.
- **Transport confidentiality.** The HTTP handler speaks plain HTTP; terminate TLS in the
  `http.Server` or in front of it.
- **A malicious tool handler.** Handlers run with the process's privileges. `ToolAnnotations` are
  server-supplied hints and are untrusted by construction; nothing here treats them as a control.
- **Peer credentials off Linux.** On macOS and the BSDs UID/GID admission fails closed and
  socket-file permissions are the access control. The per-UID connection cap
  (`MaxConnsPerUID`, `mcpd -max-conns-per-uid`, default 8 in mcpd) is not counted there, so only
  the total cap (`MaxConns`) bounds connections. The same holds on Linux for a listener that cannot
  supply credentials (not a unix socket); `Serve` logs once that the cap is not enforced.
- **Resumption, replay and cross-node sessions.** A frame in flight when an HTTP stream drops is
  lost, and sessions live in one process's memory.
