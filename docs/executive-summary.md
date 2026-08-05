# mcp — executive summary

**What it is.** A pure-Go, standard-library-only implementation of the Model
Context Protocol — now **both roles, server *and* client**. The server is the
interface an AI agent host speaks to reach the tools, resources, and prompts you
expose; the client (`Dial`) is the consumer side, letting one netstar service drive
another's tool server. A library (`package mcp`) plus a reference daemon (`mcpd`).

**Why it exists.** Agents need a controlled doorway to the stack's tools and data,
and that doorway has to be safe to run where the data lives — often a hardened,
no-outbound box. Off-the-shelf MCP servers pull in dependency trees and assume a
friendly environment; this one assumes attacker-controlled input on the wire and a
locked-down host, and carries **zero third-party dependencies** so it drops in
anywhere and has nothing to audit but its own code.

**What you get.**
- One registry of tools/resources/prompts served over **stdio, Unix socket, and
  systemd** at once; **capabilities derived from what's registered**, so the
  advertisement can't drift from the implementation.
- All three protocol revisions negotiated; structured tool output, resource links,
  pagination, progress/cancel, and server→client calls (`sampling`, `roots`,
  `elicitation`) supported.
- **Hardened for hostile input:** umask-created sockets, `SO_PEERCRED` UID/GID
  admission, and ceilings on every peer-growable quantity, with deadlines below the
  framer and log-injection sanitisation.
- **Fast where the server controls it:** encoded list-page cache, compile-once URI
  templates, buffer reuse — `tools/list` over 100 tools went 90.9 µs → 884 ns;
  per-connection memory 148 KB → 24.6 KB.
- **A client too** (`Dial` → `Initialize`/`ListTools`/`CallTool`): drives a tool
  server over any byte stream — Unix socket, stdio pipe pair, in-memory pipe —
  reusing the *same* Framer, JSON-RPC, and protocol types as the server. Minimal by
  design (tools, first page; no resources/prompts/sampling), enough for a gateway to
  front another service. Still **zero dependencies**.

**Where it fits.** The standalone MCP surface for the netstar stack (inbox already
embeds an MCP mode; this is the general server other tools/data plug into). An
agent host connects; the server exposes exactly the registered tools and nothing
more. With the client in the same package, one netstar service can now front
another — the gateway pattern: serve on one side, consume on the other, over one
wire.

**What it is not.** Not yet an HTTP transport — the streamable-HTTP transport is now
**scoped** ([design](plan/scope-http-transport.md); the `Framer` interface is the
seam) but not built, so server and client both speak stdio + Unix-socket framing
today. Not a server-side schema validator, not a URI template expander, and the
client stops at tools (no resources/prompts/sampling). And not multi-dependency —
the empty require block is a feature, on both ends.
