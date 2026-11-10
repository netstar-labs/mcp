package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/netstar-labs/mcp"
)

// httpConfig is the HTTP transport's configuration from the command line.
type httpConfig struct {
	addr     string
	origins  []string
	hosts    []string
	anyIface bool
}

func splitCSV(s string) []string {
	var out []string
	for _, f := range strings.Split(s, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}

// loopbackAddr reports whether addr (host:port) binds only to loopback. An empty
// host (":8080") binds every interface, so it is not loopback.
func loopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// listenHTTP binds the HTTP listener, refusing a non-loopback address unless the
// operator asked for it: the transport does not authenticate, so reaching it
// from the network is a deliberate decision, not a default.
func listenHTTP(c httpConfig) (net.Listener, error) {
	if !c.anyIface && !loopbackAddr(c.addr) {
		return nil, fmt.Errorf("%q is not a loopback address; the HTTP endpoint is unauthenticated (use -http-any-iface to bind it anyway, behind something that authenticates)", c.addr)
	}
	return net.Listen("tcp", c.addr)
}

// serveHTTP serves srv at /mcp until ctx ends. Timeouts that bound a request's
// read are set here; WriteTimeout is not, because it would end every SSE stream
// (the transport sets a deadline on each stream write itself).
func serveHTTP(ctx context.Context, srv *mcp.Server, l net.Listener, c httpConfig, logger *log.Logger) error {
	mux := http.NewServeMux()
	mux.Handle("/mcp", srv.HTTPHandler(mcp.HTTPOptions{AllowedOrigins: c.origins, AllowedHosts: c.hosts}))
	hs := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    16 << 10,
		ErrorLog:          logger,
	}
	errc := make(chan error, 1)
	go func() { errc <- hs.Serve(l) }()
	select {
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}
	// Close the MCP sessions first: their streams are open requests, which a
	// graceful Shutdown would otherwise wait on.
	_ = srv.Close()
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := hs.Shutdown(sctx); err != nil {
		_ = hs.Close()
	}
	return nil
}
