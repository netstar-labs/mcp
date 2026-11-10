// Package mcp is a Model Context Protocol server and client in Go, standard
// library only.
//
// A Server holds one registry of tools, resources and prompts and serves it over
// any transport at once: stdio (ServeStdio), a Unix socket (ListenUnix, Serve,
// ListenAndServeUnix, ListenSystemd), and streamable HTTP (HTTPHandler). Every
// transport hands the Server a Framer and gets a Session back, so the protocol
// logic exists once. Capabilities are derived from what is registered, so the
// advertisement cannot drift from the implementation.
//
//	srv := mcp.New(mcp.Options{Name: "svc", Version: "1.0.0"})
//	srv.AddTool(mcp.Tool{Name: "echo", InputSchema: schema}, func(ctx context.Context, s *mcp.Session, args json.RawMessage) (*mcp.CallToolResult, error) {
//		return mcp.ToolText("%s", args), nil
//	})
//	http.Handle("/mcp", authenticate(srv.HTTPHandler(mcp.HTTPOptions{})))
//
// A Client (Dial over a byte stream, DialHTTP over streamable HTTP) drives
// another server: initialize, list tools, call a tool, and answer the server's
// own requests (elicitation, sampling, roots). It is deliberately small; see
// Client for what it leaves out.
//
// # Trust
//
// Everything a peer controls is bounded: frame and id and method sizes, batch
// members, subscriptions, in-flight and outstanding calls, connections,
// sessions. Peer-supplied text is sanitised before it reaches a log. This
// package does not authenticate. The Unix transports can admit by kernel peer
// credentials (Linux); the HTTP transport takes no position on who may call and
// expects the handler to be wrapped by something that does (HTTPOptions.Identify
// carries that wrapper's answer to the handlers that must authorise by it).
//
// # Protocol
//
// Revisions 2024-11-05, 2025-03-26 and 2025-06-18 are negotiated. JSON Schema
// validation of tool arguments is not done here: schemas are published to the
// client, and a handler unmarshals into a typed struct and returns ToolError on
// mismatch. go.mod has no require block and stays that way.
package mcp
