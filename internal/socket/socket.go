// Package socket is the local IPC path between the short-lived `mesh` CLI
// and its long-lived sidecar: a permissioned unix socket with strict
// one-request → one-reply → close semantics.
//
// This package deliberately has no bus dependency — it only moves typed
// request/reply frames. The sidecar supplies the handler; the CLI maps typed
// error codes onto exit codes.
package socket

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const protocolVersion = 1

// Typed error codes carried in Response.Code. The CLI maps these to its
// documented exit codes (not_joined → 5, etc.).
const (
	CodeOK          = ""
	CodeBadRequest  = "bad_request"
	CodeNotJoined   = "not_joined"
	CodeUnavailable = "unavailable" // sidecar up but bus/coordinator unreachable
	CodeInternal    = "internal"
)

// Typed client-side errors.
var (
	// ErrNoSocket: nothing is listening at the path (no sidecar). The CLI
	// maps this to exit 5 (not joined).
	ErrNoSocket = errors.New("socket: no sidecar listening")
	// ErrProtocol: the peer sent something that is not a valid frame.
	ErrProtocol = errors.New("socket: protocol error")
)

// Request is one CLI verb invocation.
type Request struct {
	V    int             `json:"v"`
	Verb string          `json:"verb"`
	Args json.RawMessage `json:"args,omitempty"`
}

// Response is the single reply to a Request.
type Response struct {
	V       int             `json:"v"`
	OK      bool            `json:"ok"`
	Code    string          `json:"code,omitempty"`
	Message string          `json:"message,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// OKData builds a success response carrying a JSON-encodable payload.
func OKData(data any) Response {
	raw, err := json.Marshal(data)
	if err != nil {
		return Fail(CodeInternal, fmt.Sprintf("encode response: %v", err))
	}
	return Response{V: protocolVersion, OK: true, Data: raw}
}

// Fail builds a typed error response.
func Fail(code, message string) Response {
	return Response{V: protocolVersion, OK: false, Code: code, Message: message}
}

// Handler processes one request. It must not block indefinitely.
type Handler func(Request) Response

// Server owns the sidecar end of the socket.
type Server struct {
	path    string
	handler Handler

	mu     sync.Mutex
	ln     net.Listener
	closed bool
	wg     sync.WaitGroup
}

// NewServer creates a server for the given socket path.
func NewServer(path string, handler Handler) *Server {
	return &Server{path: path, handler: handler}
}

// Start binds the socket with owner-only permissions and begins serving.
func (s *Server) Start() error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("socket: create dir: %w", err)
	}
	ln, err := listenUnix(s.path)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ln.Close()
		return errors.New("socket: server already stopped")
	}
	s.ln = ln
	s.mu.Unlock()

	s.wg.Add(1)
	go s.acceptLoop(ln)
	return nil
}

func listenUnix(path string) (net.Listener, error) {
	if _, err := os.Stat(path); err == nil {
		conn, err := net.DialTimeout("unix", path, 250*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil, fmt.Errorf("socket: %s already in use", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("socket: remove stale socket: %w", err)
		}
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("socket: listen %s: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, fmt.Errorf("socket: chmod: %w", err)
	}
	return ln, nil
}

// Stop closes the listener, waits for in-flight requests, removes the file.
func (s *Server) Stop() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	ln := s.ln
	s.mu.Unlock()

	if ln != nil {
		ln.Close()
	}
	s.wg.Wait()
	os.Remove(s.path)
}

// Path returns the socket path the server serves on.
func (s *Server) Path() string { return s.path }

func (s *Server) acceptLoop(ln net.Listener) {
	defer s.wg.Done()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.serveOne(conn)
		}()
	}
}

// serveOne implements the strict one-request/one-reply/close contract.
func (s *Server) serveOne(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(10 * time.Second)) //nolint:errcheck

	reader := bufio.NewReaderSize(conn, 64*1024)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		return
	}
	var req Request
	resp := Fail(CodeBadRequest, "unparseable request")
	if jsonErr := json.Unmarshal(line, &req); jsonErr == nil {
		if req.V != protocolVersion {
			resp = Fail(CodeBadRequest, fmt.Sprintf("unsupported protocol version %d", req.V))
		} else {
			resp = s.handler(req)
			if resp.V == 0 {
				resp.V = protocolVersion
			}
		}
	}
	out, err := json.Marshal(resp)
	if err != nil {
		out, _ = json.Marshal(Fail(CodeInternal, "encode response")) //nolint:errcheck
	}
	conn.Write(append(out, '\n')) //nolint:errcheck // peer may have gone away
}

// Do sends one request and returns the typed response. dial/protocol failures
// return typed errors the CLI can map onto exit codes.
func Do(path string, req Request, timeout time.Duration) (Response, error) {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	req.V = protocolVersion

	conn, err := net.DialTimeout("unix", path, timeout)
	if err != nil {
		return Response{}, fmt.Errorf("%w (dial %s: %v)", ErrNoSocket, path, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout)) //nolint:errcheck

	out, err := json.Marshal(req)
	if err != nil {
		return Response{}, fmt.Errorf("socket: marshal request: %w", err)
	}
	if _, err := conn.Write(append(out, '\n')); err != nil {
		return Response{}, fmt.Errorf("%w (write: %v)", ErrNoSocket, err)
	}

	line, err := bufio.NewReaderSize(conn, 64*1024).ReadBytes('\n')
	if err != nil {
		return Response{}, fmt.Errorf("%w (read: %v)", ErrProtocol, err)
	}
	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return Response{}, fmt.Errorf("%w (decode: %v)", ErrProtocol, err)
	}
	return resp, nil
}
