package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/netstar-labs/mcp"
)

func TestLoopbackAddr(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:8080": true,
		"[::1]:8080":     true,
		"localhost:80":   true,
		"LOCALHOST:80":   true,
		":8080":          false, // every interface
		"0.0.0.0:8080":   false,
		"[::]:8080":      false,
		"10.1.2.3:8080":  false,
		"example.com:80": false,
		"127.0.0.1":      false, // no port: not a valid listen address
		"":               false,
	} {
		if got := loopbackAddr(addr); got != want {
			t.Errorf("loopbackAddr(%q) = %v, want %v", addr, got, want)
		}
	}
}

func TestListenHTTPRefusesNonLoopbackUnlessAsked(t *testing.T) {
	if _, err := listenHTTP(httpConfig{addr: "0.0.0.0:0"}); err == nil {
		t.Fatal("a non-loopback bind was accepted without -http-any-iface")
	}
	l, err := listenHTTP(httpConfig{addr: "0.0.0.0:0", anyIface: true})
	if err != nil {
		t.Fatalf("explicit non-loopback bind refused: %v", err)
	}
	_ = l.Close()
}

func TestServeHTTPServesAndStops(t *testing.T) {
	logger := log.New(io.Discard, "", 0)
	srv := mcp.New(mcp.Options{Name: "t", Logger: logger})
	l, err := listenHTTP(httpConfig{addr: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveHTTP(ctx, srv, l, httpConfig{}, logger) }()

	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}`
	req, _ := http.NewRequest(http.MethodPost, "http://"+l.Addr().String()+"/mcp", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.Header.Get("Mcp-Session-Id") == "" {
		t.Fatalf("initialize over mcpd's HTTP: status %d, session id %q", resp.StatusCode, resp.Header.Get("Mcp-Session-Id"))
	}
	// Anything but /mcp is not the endpoint.
	if r, err := http.Get("http://" + l.Addr().String() + "/"); err == nil {
		r.Body.Close()
		if r.StatusCode != http.StatusNotFound {
			t.Errorf("GET / status %d, want 404", r.StatusCode)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveHTTP returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serveHTTP did not stop with its context")
	}
}
