// Example http serves an MCP tool over streamable HTTP behind a bearer-token
// check. The transport has no authentication of its own, by design: it is wire,
// and whatever decides who may call is wrapped around it. This is that wrapper,
// small enough to read in one sitting.
//
//	MCP_TOKENS='s3cret:alice,t0ken:bob' go run ./example/http
//
// Then, from another shell:
//
//	curl -si localhost:8080/mcp -H 'Authorization: Bearer s3cret' \
//	  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
//	  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'
//
// The reply carries an Mcp-Session-Id header; send it on every later request.
// The session belongs to the identity that created it: bob presenting alice's
// session id is told the session does not exist.
package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/netstar-labs/mcp"
)

type ctxKey struct{}

func main() {
	tokens := parseTokens(os.Getenv("MCP_TOKENS"))
	if len(tokens) == 0 {
		log.Fatal("set MCP_TOKENS to token:identity pairs, e.g. 's3cret:alice,t0ken:bob'")
	}

	srv := mcp.New(mcp.Options{Name: "example-http", Version: "0.1.0"})
	must(srv.AddTool(mcp.Tool{
		Name:        "whoami",
		Description: "Return the identity the caller authenticated as.",
		InputSchema: json.RawMessage(`{"type":"object"}`),
	}, func(ctx context.Context, s *mcp.Session, _ json.RawMessage) (*mcp.CallToolResult, error) {
		// Session.Identity is what Identify returned when the session was made.
		return mcp.ToolText("%s", s.Identity()), nil
	}))

	h := srv.HTTPHandler(mcp.HTTPOptions{
		// Identify reads the answer the wrapper below put on the request.
		Identify: func(r *http.Request) string {
			id, _ := r.Context().Value(ctxKey{}).(string)
			return id
		},
	})

	mux := http.NewServeMux()
	mux.Handle("/mcp", authenticate(tokens, h))

	hs := &http.Server{
		Addr:              "127.0.0.1:8080",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second, // not WriteTimeout: it would end every stream
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	go func() {
		<-ctx.Done()
		_ = srv.Close()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hs.Shutdown(sctx)
	}()
	log.Printf("listening on http://%s/mcp", hs.Addr)
	if err := hs.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

// authenticate admits a request only with a known bearer token and records who it
// belongs to for the handler below it. The comparison is constant-time per
// candidate, so a wrong guess does not reveal how much of it was right.
func authenticate(tokens map[string]string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		identity, ok := "", false
		for tok, id := range tokens {
			if subtle.ConstantTimeCompare([]byte(got), []byte(tok)) == 1 {
				identity, ok = id, true
			}
		}
		if !ok {
			w.Header().Set("WWW-Authenticate", `Bearer realm="mcp"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, identity)))
	})
}

func parseTokens(s string) map[string]string {
	out := map[string]string{}
	for _, pair := range strings.Split(s, ",") {
		if tok, id, ok := strings.Cut(strings.TrimSpace(pair), ":"); ok && tok != "" && id != "" {
			out[tok] = id
		}
	}
	return out
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
