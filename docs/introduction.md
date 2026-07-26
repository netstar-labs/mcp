# Meet mcp — the doorway an agent knocks on

**MCP** — the Model Context Protocol — is the language an AI host speaks to reach
the *tools*, *resources*, and *prompts* a server chooses to expose. When an agent
runs a command, reads a file, or fills a template on your behalf, an MCP server is
what it's talking to. This package is that server — the side that decides what an
agent may touch, and answers safely.

## What it actually is

`mcp` is a pure-Go, **standard-library-only** implementation of the MCP server: a
JSON-RPC 2.0 engine speaking MCP over stdio, a Unix socket, or systemd-activated
descriptors — one registry of tools/resources/prompts served across every
transport at once. It negotiates all three protocol revisions (`2024-11-05`,
`2025-03-26`, `2025-06-18`), and **derives its advertised capabilities from what is
actually registered**, so the server can never claim a capability it doesn't
implement. `go.mod` has no require block, and it stays that way — no code
generation, no reflection tricks.

## The capability nobody else brings here

It is built to be **safe to open on a locked-down box.** The socket is created
under a umask (never wider than requested, not even between `bind` and `chmod`);
peers are authenticated by kernel-attested `SO_PEERCRED` UID/GID, the only real
identity on an abstract socket; and *everything a peer can grow* — frame size, id
length, batch members, subscriptions, in-flight calls, connections per UID — is
bounded, with deadlines sitting below the framer so a silent or stalled client
can't wedge the session. Peer-supplied text is sanitised before it is ever logged,
so a client can't forge an audit line. This is the MCP surface for a
threat-intelligence stack: an agent reaches the tools, and nothing else.

## The scope it keeps

It is the **server** side, and deliberately not everything: no HTTP/SSE transport
yet (the `Framer` interface is where it plugs in), no server-side JSON-Schema
validation (handlers unmarshal into typed structs and return a tool error on
mismatch), and URI templates are *matched*, not expanded. What it does, it does to
the letter of the spec and the limits of the hostile-input threat model.

*Read next:* [executive-summary.md](executive-summary.md) ·
[architecture.md](architecture.md) · [userguide.md](userguide.md) ·
[../example/README.md](../example/README.md)
