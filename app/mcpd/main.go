// Command mcpd is a reference MCP server built on the mcp package. It serves
// stdio, a Unix-domain socket, streamable HTTP, or any combination at once from a
// single registry.
//
//	mcpd -stdio
//	mcpd -socket /run/mcpd/mcpd.sock -mode 0660 -group analysts
//	mcpd -socket @mcpd -allow-uid 1000        # abstract namespace, uid-gated
//	mcpd -stdio -socket /tmp/mcpd.sock        # both, sharing one registry
//	mcpd -http 127.0.0.1:8080                 # streamable HTTP at /mcp, loopback only
//
// The HTTP transport has no authentication of its own. mcpd binds it to loopback
// unless told otherwise; to serve it beyond the host, put something that
// authenticates in front of it (see example/http).
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/netstar-labs/mcp"
)

func main() {
	var (
		useStdio   = flag.Bool("stdio", false, "serve the MCP stdio transport")
		socketPath = flag.String("socket", "", "Unix socket path ('@name' selects the Linux abstract namespace)")
		socketMode = flag.String("mode", "0600", "octal permission bits for the socket file")
		group      = flag.String("group", "", "group name or gid to own the socket")
		allowUID   = flag.String("allow-uid", "", "comma-separated uids permitted to connect")
		allowGID   = flag.String("allow-gid", "", "comma-separated gids permitted to connect")
		replace    = flag.Bool("replace-stale", true, "reclaim a leftover socket file when no listener holds it")
		framing    = flag.String("framing", "lines", "wire framing for the socket: lines|headers")
		rootDir    = flag.String("root", ".", "directory exposed through the file:// resource template")
		maxConns   = flag.Int("max-conns", 64, "maximum concurrent socket connections")
		maxPerUID  = flag.Int("max-conns-per-uid", 8, "maximum concurrent connections from any one uid")
		idleTO     = flag.Duration("idle-timeout", 10*time.Minute, "drop connections idle for this long (0 disables)")
		writeTO    = flag.Duration("write-timeout", 30*time.Second, "bound on a single socket write (0 disables)")

		httpAddr    = flag.String("http", "", "serve streamable HTTP at /mcp on this address (host:port); loopback only unless -http-any-iface")
		httpOrigins = flag.String("http-origin", "", "comma-separated browser origins allowed to call the HTTP endpoint (default: none)")
		httpHosts   = flag.String("http-host", "", "comma-separated Host header values accepted (recommended when binding loopback)")
		httpAny     = flag.Bool("http-any-iface", false, "allow -http to bind a non-loopback address; the endpoint is unauthenticated, so something in front of it must authenticate")
	)
	flag.Parse()

	if !*useStdio && *socketPath == "" && *httpAddr == "" {
		fmt.Fprintln(os.Stderr, "mcpd: specify -stdio, -socket, -http, or a combination")
		flag.Usage()
		os.Exit(2)
	}

	// Diagnostics must never touch stdout while the stdio transport is live.
	logger := log.New(os.Stderr, "mcpd ", log.LstdFlags|log.Lmsgprefix)

	absRoot, err := filepath.Abs(*rootDir)
	if err != nil {
		logger.Fatalf("resolve root: %v", err)
	}

	srv := mcp.New(mcp.Options{
		Name:         "mcpd",
		Title:        "Reference MCP Daemon",
		Version:      "1.0.0",
		Instructions: "Reference server: hashing, DNS lookup, and read-only file access under a fixed root.",
		Logger:       logger,
		AcceptBatch:  false, // batching existed only in revision 2025-03-26
		OnSessionStart: func(s *mcp.Session) {
			ci := s.ClientInfo()
			logger.Printf("session ready: %s client=%s/%s protocol=%s",
				s.RemoteAddr(), ci.Name, ci.Version, s.ProtocolVersion())
		},
		OnSessionEnd: func(s *mcp.Session, err error) {
			logger.Printf("session closed: %s (%v)", s.RemoteAddr(), err)
		},
	})

	registerTools(srv, absRoot)
	registerResources(srv, absRoot)
	registerPrompts(srv)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	fail := make(chan error, 3)

	if *socketPath != "" {
		opts, err := unixOptions(socketConfig{
			mode: *socketMode, group: *group, allowUID: *allowUID, allowGID: *allowGID,
			framing: *framing, replace: *replace, maxConns: *maxConns, maxPerUID: *maxPerUID,
			idle: *idleTO, write: *writeTO,
		})
		if err != nil {
			logger.Fatalf("socket options: %v", err)
		}
		l, err := mcp.ListenUnix(*socketPath, opts)
		if err != nil {
			logger.Fatalf("%v", err)
		}
		logger.Printf("listening on unix:%s (framing=%s)", *socketPath, *framing)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer l.Close()
			if err := srv.Serve(ctx, l, opts); err != nil {
				fail <- fmt.Errorf("socket: %w", err)
			}
		}()
	}

	if *httpAddr != "" {
		cfg := httpConfig{addr: *httpAddr, origins: splitCSV(*httpOrigins), hosts: splitCSV(*httpHosts), anyIface: *httpAny}
		l, err := listenHTTP(cfg)
		if err != nil {
			logger.Fatalf("http: %v", err)
		}
		logger.Printf("listening on http://%s/mcp", l.Addr())
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := serveHTTP(ctx, srv, l, cfg, logger); err != nil {
				fail <- fmt.Errorf("http: %w", err)
			}
		}()
	}

	if *useStdio {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := srv.ServeStdio(ctx); err != nil && !errors.Is(err, context.Canceled) {
				fail <- fmt.Errorf("stdio: %w", err)
			}
			// stdin closing is a shutdown signal for the whole daemon.
			stop()
		}()
	}

	wg.Wait()
	_ = srv.Close()
	close(fail)
	exit := 0
	for err := range fail {
		logger.Printf("%v", err)
		exit = 1
	}
	logger.Printf("shutdown complete")
	os.Exit(exit)
}

type socketConfig struct {
	mode, group, allowUID, allowGID, framing string
	replace                                  bool
	maxConns, maxPerUID                      int
	idle, write                              time.Duration
}

func unixOptions(c socketConfig) (mcp.UnixOptions, error) {
	var opts mcp.UnixOptions

	m, err := strconv.ParseUint(c.mode, 8, 32)
	if err != nil {
		return opts, fmt.Errorf("bad -mode %q: %w", c.mode, err)
	}
	opts.Mode = fs.FileMode(m)
	opts.Group = c.group
	opts.ReplaceStale = c.replace
	opts.MaxConns = c.maxConns
	opts.MaxConnsPerUID = c.maxPerUID
	opts.HandshakeTimeout = 30 * time.Second
	opts.IdleTimeout = c.idle
	opts.WriteTimeout = c.write

	switch c.framing {
	case "lines", "":
		opts.Framing = mcp.FramingLines
	case "headers":
		opts.Framing = mcp.FramingHeaders
	default:
		return opts, fmt.Errorf("bad -framing %q", c.framing)
	}

	if opts.AllowUID, err = parseIDs(c.allowUID); err != nil {
		return opts, fmt.Errorf("bad -allow-uid: %w", err)
	}
	if opts.AllowGID, err = parseIDs(c.allowGID); err != nil {
		return opts, fmt.Errorf("bad -allow-gid: %w", err)
	}
	// If an allow-list is configured, credentials must be readable or the
	// list would be silently unenforced.
	opts.RequirePeerCred = len(opts.AllowUID) > 0 || len(opts.AllowGID) > 0
	return opts, nil
}

func parseIDs(csv string) ([]uint32, error) {
	if strings.TrimSpace(csv) == "" {
		return nil, nil
	}
	var out []uint32
	for _, field := range strings.Split(csv, ",") {
		n, err := strconv.ParseUint(strings.TrimSpace(field), 10, 32)
		if err != nil {
			return nil, err
		}
		out = append(out, uint32(n))
	}
	return out, nil
}

func registerTools(srv *mcp.Server, root string) {
	must(srv.AddTool(mcp.Tool{
		Name:        "sha256",
		Title:       "SHA-256 digest",
		Description: "Compute the SHA-256 digest of a UTF-8 string.",
		InputSchema: json.RawMessage(`{
			"type":"object",
			"properties":{"input":{"type":"string","description":"data to hash"}},
			"required":["input"],
			"additionalProperties":false
		}`),
		OutputSchema: json.RawMessage(`{
			"type":"object",
			"properties":{"hex":{"type":"string"},"bytes":{"type":"integer"}},
			"required":["hex","bytes"]
		}`),
		Annotations: &mcp.ToolAnnotations{
			ReadOnlyHint:   ptr(true),
			IdempotentHint: ptr(true),
			OpenWorldHint:  ptr(false),
		},
	}, func(ctx context.Context, s *mcp.Session, raw json.RawMessage) (*mcp.CallToolResult, error) {
		var args struct {
			Input string `json:"input"`
		}
		if err := json.Unmarshal(raw, &args); err != nil {
			return mcp.ToolError("invalid arguments: %v", err), nil
		}
		sum := sha256.Sum256([]byte(args.Input))
		digest := hex.EncodeToString(sum[:])

		structured, _ := json.Marshal(map[string]any{"hex": digest, "bytes": len(args.Input)})
		return &mcp.CallToolResult{
			Content:           []mcp.Content{mcp.TextContent(digest)},
			StructuredContent: structured,
		}, nil
	}))

	must(srv.AddTool(mcp.Tool{
		Name:        "resolve",
		Title:       "Resolve hostname",
		Description: "Resolve a hostname to its A/AAAA records.",
		InputSchema: json.RawMessage(`{
			"type":"object",
			"properties":{"host":{"type":"string"}},
			"required":["host"],
			"additionalProperties":false
		}`),
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: ptr(true), OpenWorldHint: ptr(true)},
	}, func(ctx context.Context, s *mcp.Session, raw json.RawMessage) (*mcp.CallToolResult, error) {
		var args struct {
			Host string `json:"host"`
		}
		if err := json.Unmarshal(raw, &args); err != nil {
			return mcp.ToolError("invalid arguments: %v", err), nil
		}
		if args.Host == "" {
			return mcp.ToolError("host is required"), nil
		}

		// Bound the work independently of the client, then let either
		// deadline or an inbound notifications/cancelled abort the lookup.
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		s.ReportProgress(ctx, 0, ptrf(1), "resolving "+args.Host)
		addrs, err := net.DefaultResolver.LookupHost(ctx, args.Host)
		if err != nil {
			return mcp.ToolError("lookup %s: %v", args.Host, err), nil
		}
		s.ReportProgress(ctx, 1, ptrf(1), "done")
		return mcp.ToolText("%s", strings.Join(addrs, "\n")), nil
	}))

	must(srv.AddTool(mcp.Tool{
		Name:        "read_file",
		Title:       "Read file",
		Description: "Read a UTF-8 text file beneath the server's configured root.",
		InputSchema: json.RawMessage(`{
			"type":"object",
			"properties":{"path":{"type":"string","description":"path relative to the server root"}},
			"required":["path"],
			"additionalProperties":false
		}`),
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: ptr(true), OpenWorldHint: ptr(false)},
	}, func(ctx context.Context, s *mcp.Session, raw json.RawMessage) (*mcp.CallToolResult, error) {
		var args struct {
			Path string `json:"path"`
		}
		if err := json.Unmarshal(raw, &args); err != nil {
			return mcp.ToolError("invalid arguments: %v", err), nil
		}
		full, err := resolveUnder(root, args.Path)
		if err != nil {
			return mcp.ToolError("%v", err), nil
		}
		data, err := readRegular(full)
		if err != nil {
			return mcp.ToolError("read: %v", err), nil
		}
		return mcp.ToolText("%s", string(data)), nil
	}))
}

// maxReadFileBytes bounds what read_file and the file:// template return. A reply
// larger than the client's MaxMessageBytes (8 MiB by default) is a frame the client
// refuses, which ends its session, so the bound sits well under it.
const maxReadFileBytes = 1 << 20

// readRegular reads a regular file of at most maxReadFileBytes. Anything else under
// the root (a FIFO, a device, a directory) is refused: opening a FIFO for reading
// blocks until a writer appears, in a handler that cannot be cancelled, so the file
// is opened non-blocking and its type checked before anything is read.
func readRegular(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, maxReadFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxReadFileBytes {
		return nil, fmt.Errorf("file is over the %d byte limit", maxReadFileBytes)
	}
	return data, nil
}

func registerResources(srv *mcp.Server, root string) {
	must(srv.AddResource(mcp.Resource{
		URI:         "mcpd://status",
		Name:        "status",
		Title:       "Daemon status",
		Description: "Live counters for this daemon.",
		MIMEType:    "application/json",
	}, func(ctx context.Context, s *mcp.Session, uri string) ([]mcp.ResourceContents, error) {
		payload, err := json.Marshal(map[string]any{
			"pid":             os.Getpid(),
			"sessions":        len(s.Server().Sessions()),
			"protocolVersion": s.ProtocolVersion(),
			"peer":            s.Peer().String(),
			"root":            root,
			"serverTimeUTC":   time.Now().UTC().Format(time.RFC3339),
		})
		if err != nil {
			return nil, err
		}
		return []mcp.ResourceContents{mcp.TextResource(uri, "application/json", string(payload))}, nil
	}))

	// Compiled once at startup rather than re-parsed on every read.
	fileTemplate, err := mcp.CompileURITemplate("file:///{+path}")
	must(err)

	must(srv.AddResourceTemplate(mcp.ResourceTemplate{
		URITemplate: fileTemplate.String(),
		Name:        "workspace-file",
		Title:       "Workspace file",
		Description: "Any file beneath the server root, addressed as file:///relative/path.",
	}, func(ctx context.Context, s *mcp.Session, uri string) ([]mcp.ResourceContents, error) {
		vars, ok := fileTemplate.Match(uri)
		if !ok {
			return nil, mcp.Errorf(mcp.CodeInvalidParams, "uri does not match %s", fileTemplate)
		}
		full, err := resolveUnder(root, vars["path"])
		if err != nil {
			return nil, mcp.Errorf(mcp.CodeInvalidParams, "%v", err)
		}
		data, err := readRegular(full)
		if err != nil {
			return nil, mcp.Errorf(mcp.CodeResourceNotFound, "read %s: %v", uri, err)
		}
		return []mcp.ResourceContents{mcp.TextResource(uri, "text/plain; charset=utf-8", string(data))}, nil
	}))
}

func registerPrompts(srv *mcp.Server) {
	must(srv.AddPrompt(mcp.Prompt{
		Name:        "triage",
		Title:       "Triage an indicator",
		Description: "Draft a triage plan for a single indicator of compromise.",
		Arguments: []mcp.PromptArgument{
			{Name: "indicator", Description: "hash, domain, IP, or URL", Required: true},
			{Name: "context", Description: "where it was observed"},
		},
	}, func(ctx context.Context, s *mcp.Session, args map[string]string) (*mcp.GetPromptResult, error) {
		body := fmt.Sprintf("Triage this indicator: %s", args["indicator"])
		if c := args["context"]; c != "" {
			body += "\nObserved: " + c
		}
		return &mcp.GetPromptResult{
			Description: "Indicator triage",
			Messages: []mcp.PromptMessage{
				{Role: "user", Content: mcp.TextContent(body)},
			},
		}, nil
	}))

	srv.SetCompleter(func(ctx context.Context, s *mcp.Session, p *mcp.CompleteParams) ([]string, error) {
		if p.Ref.Type == "ref/prompt" && p.Ref.Name == "triage" && p.Argument.Name == "context" {
			all := []string{"edr-alert", "proxy-log", "mail-gateway", "threat-feed"}
			var out []string
			for _, v := range all {
				if strings.HasPrefix(v, p.Argument.Value) {
					out = append(out, v)
				}
			}
			return out, nil
		}
		return nil, nil
	})
}

// resolveUnder joins rel to root and proves the result stays inside root, with
// symlinks resolved. Lexical cleaning alone is not sufficient: a symlink inside
// the root can point anywhere.
func resolveUnder(root, rel string) (string, error) {
	if rel == "" {
		return "", errors.New("path is required")
	}
	rel = strings.TrimPrefix(rel, "/")
	candidate := filepath.Join(root, filepath.Clean("/"+rel))

	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("root is unreadable: %w", err)
	}
	realPath, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		// The file may not exist yet; validate the deepest existing ancestor.
		realPath = candidate
		for {
			parent := filepath.Dir(realPath)
			if parent == realPath {
				break
			}
			if resolved, err := filepath.EvalSymlinks(parent); err == nil {
				realPath = filepath.Join(resolved, filepath.Base(realPath))
				break
			}
			realPath = parent
		}
	}
	if realPath != realRoot && !strings.HasPrefix(realPath, realRoot+string(os.PathSeparator)) {
		return "", errors.New("path escapes the server root")
	}
	return realPath, nil
}

func ptr(b bool) *bool        { return &b }
func ptrf(f float64) *float64 { return &f }

func must(err error) {
	if err != nil {
		panic(err)
	}
}
