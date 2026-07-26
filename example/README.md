# mcp examples

| Example | What it shows | Run |
|---|---|---|
| [stdio](stdio/main.go) | a minimal MCP server — one `echo` tool — over the stdio transport | `go run ./example/stdio` |

Build standalone with `GOWORK=off` (the module has no third-party requires).

Once running, speak newline-delimited JSON-RPC 2.0 on stdin and read replies on
stdout — the MCP stdio wire format:

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"demo","version":"0"}}}
{"jsonrpc":"2.0","method":"notifications/initialized"}
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"echo","arguments":{"text":"hi"}}}
```

For the reference daemon over a Unix socket (with peer-credential admission), see
[../docs/userguide.md](../docs/userguide.md) and `go run ./app/mcpd -h`.
