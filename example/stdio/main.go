// Example stdio is a minimal MCP server over stdio: it registers one "echo" tool
// and serves the Model Context Protocol on stdin/stdout, the transport an MCP
// client (e.g. an AI agent host) speaks.
//
//	go run ./example/stdio
//
// Then send newline-delimited JSON-RPC on stdin — initialize, tools/list,
// tools/call — and read replies on stdout.
package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"

	"github.com/netstar-labs/mcp"
)

func main() {
	srv := mcp.New(mcp.Options{Name: "example-echo", Version: "0.1.0"})

	must(srv.AddTool(mcp.Tool{
		Name:        "echo",
		Description: "Echo the input text back.",
		InputSchema: json.RawMessage(`{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}`),
	}, func(ctx context.Context, s *mcp.Session, raw json.RawMessage) (*mcp.CallToolResult, error) {
		var args struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(raw, &args); err != nil {
			return mcp.ToolError("invalid arguments: %v", err), nil
		}
		return mcp.ToolText("%s", args.Text), nil
	}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := srv.ServeStdio(ctx); err != nil {
		panic(err)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
