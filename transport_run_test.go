package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ServeStdio reads os.Stdin and writes os.Stdout, so the test swaps both for
// pipes. Nothing else in this binary may touch them while it runs.
func TestServeStdio(t *testing.T) {
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldIn, oldOut := os.Stdin, os.Stdout
	os.Stdin, os.Stdout = inR, outW
	defer func() { os.Stdin, os.Stdout = oldIn, oldOut }()

	srv := New(Options{Name: "stdio-test", Logger: log.New(io.Discard, "", 0)})
	echoTool(srv, "echo")
	done := make(chan error, 1)
	go func() { done <- srv.ServeStdio(context.Background()) }()

	r := bufio.NewReader(outR)
	send := func(line string) {
		if _, err := inW.WriteString(line + "\n"); err != nil {
			t.Fatal(err)
		}
	}
	read := func() wireMessage {
		_ = outR.SetReadDeadline(time.Now().Add(5 * time.Second))
		line, err := r.ReadBytes('\n')
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		var m wireMessage
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		return m
	}
	send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}`)
	if m := read(); m.Error != nil || string(m.ID) != "1" {
		t.Fatalf("initialize reply %+v", m)
	}
	send(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	send(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`)
	if m := read(); m.Error != nil || string(m.ID) != "2" {
		t.Fatalf("tools/list reply %+v", m)
	}

	// stdin closing ends the session.
	_ = inW.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ServeStdio did not return after stdin closed")
	}
}

func TestListenAndServeUnix(t *testing.T) {
	srv := New(Options{Name: "lasu", Logger: log.New(io.Discard, "", 0)})
	echoTool(srv, "echo")
	path := filepath.Join(shortTempDir(t), "l.sock")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServeUnix(ctx, path, UnixOptions{ReplaceStale: true}) }()

	var conn net.Conn
	eventually(t, "the socket to accept", func() bool {
		var err error
		conn, err = net.Dial("unix", path)
		return err == nil
	})
	defer conn.Close()
	c := &testClient{t: t, conn: conn, r: bufio.NewReader(conn)}
	c.handshake()
	if raw := c.request(MethodToolsList, struct{}{}); len(raw) == 0 {
		t.Fatal("empty tools/list")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil && !isBenignClose(err) {
			t.Fatalf("ListenAndServeUnix returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ListenAndServeUnix did not return after its context ended")
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatal("the socket file was left behind")
	}
}

func TestListenAndServeUnixRefusesLiveSocket(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "live.sock")
	l, err := ListenUnix(path, UnixOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	srv := New(Options{Name: "x", Logger: log.New(io.Discard, "", 0)})
	if err := srv.ListenAndServeUnix(context.Background(), path, UnixOptions{ReplaceStale: true}); err == nil {
		t.Fatal("a live socket must not be taken over")
	}
}
