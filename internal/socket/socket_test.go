package socket

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/testsock"
)

func echoHandler(req Request) Response {
	if req.Verb == "fail" {
		return Fail(CodeNotJoined, "agent has not joined")
	}
	return OKData(map[string]string{"verb": req.Verb})
}

func startServer(t *testing.T) (*Server, string) {
	t.Helper()
	path := testsock.Path(t, "agent.sock")
	s := NewServer(path, echoHandler)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	return s, path
}

func TestRoundTrip(t *testing.T) {
	_, path := startServer(t)
	resp, err := Do(path, Request{Verb: "who"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK {
		t.Fatalf("resp not ok: %+v", resp)
	}
	var data map[string]string
	if err := json.Unmarshal(resp.Data, &data); err != nil || data["verb"] != "who" {
		t.Fatalf("data = %s err = %v", resp.Data, err)
	}
}

func TestTypedErrorResponse(t *testing.T) {
	_, path := startServer(t)
	resp, err := Do(path, Request{Verb: "fail"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if resp.OK || resp.Code != CodeNotJoined {
		t.Fatalf("want typed not_joined, got %+v", resp)
	}
}

func TestNoSocketIsTyped(t *testing.T) {
	path := testsock.Path(t, "absent.sock")
	_, err := Do(path, Request{Verb: "who"}, 250*time.Millisecond)
	if !errors.Is(err, ErrNoSocket) {
		t.Fatalf("want ErrNoSocket, got %v", err)
	}
}

func TestSocketPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows Unix-domain sockets do not expose Unix chmod permissions")
	}
	_, path := startServer(t)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket perms = %o, want 600", perm)
	}
}

func TestProtocolGarbageIsTypedNotPanic(t *testing.T) {
	_, path := startServer(t)
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("this is not json\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	var resp Response
	if err := json.Unmarshal(buf[:n], &resp); err != nil {
		t.Fatalf("server reply unparseable: %v", err)
	}
	if resp.OK || resp.Code != CodeBadRequest {
		t.Fatalf("want bad_request, got %+v", resp)
	}
}

func TestUnsupportedProtocolVersion(t *testing.T) {
	_, path := startServer(t)
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(`{"v":99,"verb":"who"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second)) //nolint:errcheck
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	var resp Response
	if err := json.Unmarshal(buf[:n], &resp); err != nil {
		t.Fatal(err)
	}
	if resp.OK || resp.Code != CodeBadRequest {
		t.Fatalf("want bad_request for v99, got %+v", resp)
	}
}

func TestStopDoesNotLeakGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	path := testsock.Path(t, "agent.sock")
	s := NewServer(path, echoHandler)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := Do(path, Request{Verb: "who"}, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	s.Stop()
	s.Stop() // idempotent

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("goroutines: before=%d after=%d", before, runtime.NumGoroutine())
}
