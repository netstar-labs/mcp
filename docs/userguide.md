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

## The `mcpd` daemon

Build: `go build -o mcpd ./app/mcpd` (standalone: `GOWORK=off`).

```sh
mcpd -stdio                                             # stdio only
mcpd -socket /run/mcpd/mcpd.sock -mode 0660 -group analysts
mcpd -socket @mcpd -allow-uid 1000                      # abstract namespace, uid-gated
mcpd -stdio -socket /tmp/mcpd.sock                      # both, one shared registry
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

## Transports & framing

- **stdio** — newline-delimited JSON on stdin/stdout; nothing else may write to
  stdout (the logger defaults to stderr).
- **Unix socket** — the same framing over `AF_UNIX`, one session per connection,
  all sharing one registry. `@name` selects the Linux abstract namespace;
  `-framing headers` switches to `Content-Length` framing when line boundaries are
  at risk.
- **systemd** — `ListenSystemd()` adopts `LISTEN_FDS`, so the daemon can be
  socket-activated and run unprivileged.

## Security operations

- **Prefer a UID/GID allow-list** (`-allow-uid`/`-allow-gid`) — enforced from
  kernel `SO_PEERCRED`, unforgeable, and the *only* authentication on an abstract
  socket. Peer creds are **Linux-only**; off-Linux, credential-gated admission
  fails closed and socket-file permissions apply.
- **Least privilege** — run unprivileged; socket-activate via systemd; the socket
  is created under a umask and never exists wider than `-mode`.
- **Limits are on by default** — `-max-conns`, `-max-conns-per-uid`, idle/write
  timeouts; a handler panic costs one request, not the session.

## Build & deploy

`build/mcp [--server] [user@host]` cross-compiles `mcpd` to `linux/amd64` with a
`git describe` stamp, packages a self-contained installer + `.tgz` (with an
optional systemd unit under `--server`), and — given a host — scp's it over and
installs via ssh.
