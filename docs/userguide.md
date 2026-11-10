# mcp — user guide

## Library

```go
import "github.com/netstar-labs/mcp"

srv := mcp.New(mcp.Options{Name: "mcpd", Version: "1.0.0"})

srv.AddTool(mcp.Tool{
    Name:        "sha256",
    Description: "Hash the input string.",
    InputSchema: json.RawMessage(`{"type":"object","properties":{"input":{"type":"string"}},"required":["input"]}`),
}, func(ctx context.Context, s *mcp.Session, args json.RawMessage) (*mcp.CallToolResult, error) {
    var in struct{ Input string `json:"input"` }
    if err := json.Unmarshal(args, &in); err != nil {
        return mcp.ToolError("invalid arguments: %v", err), nil
    }
    sum := sha256.Sum256([]byte(in.Input))
    return mcp.ToolText("%x", sum), nil
})

ctx := context.Background()
go srv.ServeStdio(ctx)                                  // stdio transport
go http.ListenAndServe("127.0.0.1:8080", srv.HTTPHandler(mcp.HTTPOptions{})) // HTTP: see "HTTP" below
srv.ListenAndServeUnix(ctx, "/run/mcpd/mcpd.sock", mcp.UnixOptions{
    Mode: 0o660, Group: "analysts", ReplaceStale: true, AllowGID: []uint32{2001},
})
```

Also: `AddResource`, `AddResourceTemplate`, `AddPrompt`, `SetCompleter`; content
helpers `TextContent`/`ImageContent`/`AudioContent`/`ResourceLink`/
`EmbeddedResource`/`BlobResource`; `ToolText`/`ToolError`; `Broadcast` +
`Notify*ListChanged`/`NotifyResourceUpdated`; `Sessions()`; `SessionFromContext`.
See the runnable [example](../example/README.md) and the README for the full
protocol surface.

## Client

The same package is also an MCP **client** — enough to drive a tool server (a
gateway fronting another service, a tool that composes one). It speaks the same
framing over any byte stream: a Unix socket, a stdio pipe pair, an in-memory
`net.Pipe`.

```go
conn, _ := net.Dial("unix", "/run/mcpd/mcpd.sock")
c := mcp.Dial(conn, mcp.ClientOptions{
    Info:      mcp.Implementation{Name: "gateway", Version: "1.0.0"},
    OnRequest: nil, // server→client requests (e.g. elicitation/create) ⇒ method-not-found
})
defer c.Close()

if _, err := c.Initialize(ctx); err != nil {           // handshake + notifications/initialized
    log.Fatal(err)
}
tools, _ := c.ListTools(ctx)                            // first page of tools
res, err := c.CallTool(ctx, "sha256", map[string]any{"input": "hi"})
// err is a JSON-RPC error (unknown tool, bad arguments);
// a tool that ran and failed is res with res.IsError set.
```

`ClientOptions` also carries `Framing` (defaults to the `lines` framer) and
`MaxMessageBytes`; set `OnRequest` to answer server→client requests (it runs on its
own goroutine, so it may block on a person or call back into the client; its
context ends when the server cancels the request or the client closes). `ListTools`
follows `nextCursor` across pages. The client is deliberately minimal — tools only —
and reuses the server's Framer, JSON-RPC, and protocol types: one wire, two roles.

Over HTTP the same client is `DialHTTP`:

```go
c, err := mcp.DialHTTP(ctx, "https://svc.example/mcp", mcp.ClientOptions{
    Info:   mcp.Implementation{Name: "gateway", Version: "1.0.0"},
    Header: http.Header{"Authorization": {"Bearer " + token}},
    // HTTPClient: your TLS config / proxy / RoundTripper
})
```

`ctx` bounds the connection's whole life (cancelling it closes the client), not any
one call; per-call deadlines are the context each call is given. `Close` ends the
server's session. If the server no longer knows the session (it expired, or the
server restarted) every call fails with `ErrSessionExpired`: dial again.

For a worked example of the gateway pattern, see
[`mcpgw`](https://github.com/netstar-labs/mcpgw): it dials a local tool server with
this client (unix socket or spawned stdio child), republishes the tools it
advertises as gated proxies, and keeps auth/capture/quota on its own side.

## HTTP

```go
h := srv.HTTPHandler(mcp.HTTPOptions{
    AllowedHosts: []string{"localhost:8080"},          // when binding loopback
    Identify:     func(r *http.Request) string { return subject(r.Context()) },
})
mux.Handle("/mcp", authenticate(h))                    // YOUR wrapper; the handler authenticates no one
hs := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second}
```

- **Authentication is yours.** The handler is wire. Put whatever decides who may
  call (a token check, mTLS, a gateway) in front of it. `Identify` runs on every
  request and returns who your wrapper decided the caller is; that identity is
  stored when the session is created, must match on every later request (another
  identity presenting the session id is told "unknown session"), and reaches your
  tool handlers as `Session.Identity()`. [example/http](../example/http/main.go) is
  a complete small wrapper.
- **Browsers are refused by default.** A request with an `Origin` header is
  rejected unless it is in `AllowedOrigins`. Requests with no `Origin` (every
  non-browser client) are unaffected. When binding loopback also set `AllowedHosts`,
  or a DNS-rebinding page can reach the server under its own hostname.
- **Timeouts on the `http.Server`.** Set `ReadHeaderTimeout` and `ReadTimeout`. Do
  not set `WriteTimeout`: it would end every SSE stream (the transport puts its own
  deadline on each stream write, `HTTPOptions.WriteTimeout`).
- **Sessions.** `MaxSessions` (default 1024), `HandshakeTimeout` (30s: a session
  that never finishes the handshake is dropped), `SessionTTL` (30m idle; an open
  stream or a request in flight holds a session open). One node: pin a session to
  one process in a load balancer.
- **What a client sees.** `POST` with `Content-Type: application/json` and
  `Accept: application/json, text/event-stream`; the first request is `initialize`,
  its reply carries `Mcp-Session-Id`, and every later request echoes it (and
  `MCP-Protocol-Version`). A reply is one JSON body, or an SSE stream when the
  server had something to send first. `GET` (with `Accept: text/event-stream`) opens
  a stream for server-initiated messages; `DELETE` ends the session. 404 means the
  session is gone: initialize again.
- **Not supported:** resuming a dropped stream (`Last-Event-ID` is ignored), batching,
  the 2024-11-05 HTTP+SSE transport.

## The `mcpd` daemon

Build: `go build -o mcpd ./app/mcpd` (standalone: `GOWORK=off`).

```sh
mcpd -stdio                                             # stdio only
mcpd -socket /run/mcpd/mcpd.sock -mode 0660 -group analysts
mcpd -socket @mcpd -allow-uid 1000                      # abstract namespace, uid-gated
mcpd -stdio -socket /tmp/mcpd.sock                      # both, one shared registry
mcpd -http 127.0.0.1:8080                               # streamable HTTP at /mcp, loopback only
```

| Flag | Default | Meaning |
|---|---|---|
| `-stdio` | false | serve the MCP stdio transport |
| `-socket` | — | Unix socket path (`@name` = Linux abstract namespace) |
| `-mode` | `0600` | octal permission bits for the socket file |
| `-group` | — | group name or gid to own the socket |
| `-allow-uid` | — | comma-separated uids permitted to connect |
| `-allow-gid` | — | comma-separated gids permitted to connect |
| `-replace-stale` | true | reclaim a leftover socket file when no listener holds it |
| `-framing` | `lines` | socket wire framing: `lines` or `headers` |
| `-root` | `.` | directory exposed via the `file://` resource template |
| `-max-conns` | 64 | max concurrent socket connections |
| `-max-conns-per-uid` | 8 | max concurrent connections from any one uid |
| `-idle-timeout` | 10m | drop connections idle this long (0 disables) |
| `-write-timeout` | 30s | bound on a single socket write (0 disables) |
| `-http` | — | serve streamable HTTP at `/mcp` on this `host:port`; loopback only unless `-http-any-iface` |
| `-http-origin` | — | comma-separated browser origins allowed (default none) |
| `-http-host` | — | comma-separated `Host` values accepted (recommended on loopback) |
| `-http-any-iface` | false | allow a non-loopback `-http` bind — the endpoint is unauthenticated, so something in front of it must authenticate |

## Transports & framing

- **stdio** — newline-delimited JSON on stdin/stdout; nothing else may write to
  stdout (the logger defaults to stderr).
- **Unix socket** — the same framing over `AF_UNIX`, one session per connection,
  all sharing one registry. `@name` selects the Linux abstract namespace;
  `-framing headers` switches to `Content-Length` framing when line boundaries are
  at risk.
- **systemd** — `ListenSystemd()` adopts `LISTEN_FDS`, so the daemon can be
  socket-activated and run unprivileged.
- **HTTP** — streamable HTTP via `Server.HTTPHandler` and `DialHTTP`; see the HTTP
  section above. A service that wants a gateway in front of its tool servers still
  has one: [`mcpgw`](https://github.com/netstar-labs/mcpgw).

## Security operations

- **Prefer a UID/GID allow-list** (`-allow-uid`/`-allow-gid`) — enforced from
  kernel `SO_PEERCRED`, unforgeable, and the *only* authentication on an abstract
  socket. Peer creds are **Linux-only**; off-Linux, credential-gated admission
  fails closed and socket-file permissions apply.
- **Least privilege** — run unprivileged; socket-activate via systemd; the socket
  is created under a umask and never exists wider than `-mode`.
- **Limits are on by default** — `-max-conns`, `-max-conns-per-uid`, idle/write
  timeouts; a handler panic costs one request, not the session.
- **`-http` binds loopback only** unless `-http-any-iface` is given, because the
  endpoint does not authenticate. Beyond the host, run an authenticating proxy in
  front, or wrap the handler as in [example/http](../example/http/main.go).

## Build & deploy

`build/mcp [--server] [user@host]` cross-compiles `mcpd` to `linux/amd64` with a
`git describe` stamp, packages a self-contained installer + `.tgz` (with an
optional systemd unit under `--server`), and — given a host — scp's it over and
installs via ssh.
