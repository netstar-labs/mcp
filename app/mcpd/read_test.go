package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// A1 C-15: read_file and the file:// template read regular files, bounded. A FIFO
// under the root parked the handler in the read forever (it never observes
// cancellation), and a large file cost many times its size per call and produced a
// reply the client could not accept, which ends the client's session.
func TestReadRegularBoundsWhatItReads(t *testing.T) {
	dir := t.TempDir()
	small := filepath.Join(dir, "small.txt")
	big := filepath.Join(dir, "big.txt")
	if err := os.WriteFile(small, []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(big, []byte(strings.Repeat("x", maxReadFileBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if b, err := readRegular(small); err != nil || string(b) != "hello" {
		t.Fatalf("small file = %q, %v", b, err)
	}
	if _, err := readRegular(big); err == nil || !strings.Contains(err.Error(), "limit") {
		t.Fatalf("a file over the limit: err = %v, want the limit named", err)
	}
	if _, err := readRegular(dir); err == nil {
		t.Fatal("a directory was read")
	}
}

func TestReadRegularRefusesAFIFOWithoutBlocking(t *testing.T) {
	fifo := filepath.Join(t.TempDir(), "pipe")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("no FIFOs here: %v", err)
	}
	done := make(chan error, 1)
	go func() { _, err := readRegular(fifo); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a FIFO was read")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reading a FIFO under the root blocked")
	}
}
