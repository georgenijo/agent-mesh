package dashboard

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestIsLoopbackHost exercises the pure host-matching logic for all expected
// forms: bare hostnames, numeric IPv4/IPv6, and bracketed IPv6 (the form
// net.SplitHostPort produces when splitting "[::1]:port").
func TestIsLoopbackHost(t *testing.T) {
	cases := []struct {
		host string
		want bool
	}{
		{"localhost", true},
		{"127.0.0.1", true},
		{"::1", true},
		{"[::1]", true}, // bracketed IPv6 after port-strip
		{"evil.example", false},
		{"evil.localhost", false}, // subdomain of localhost is NOT loopback
		{"127.0.0.2", false},
		{"0.0.0.0", false},
		{"", false},
	}
	for _, tc := range cases {
		got := isLoopbackHost(tc.host)
		if got != tc.want {
			t.Errorf("isLoopbackHost(%q) = %v, want %v", tc.host, got, tc.want)
		}
	}
}

// TestHostCheckMiddlewareRejectsEvilHost is the primary security pin: a request
// with Host: evil.example must be rejected with 403 before the handler fires,
// on both the token endpoint and the job-create endpoint. This asserts the
// DNS-rebinding defence at the middleware level without requiring a live stack.
func TestHostCheckMiddlewareRejectsEvilHost(t *testing.T) {
	// A sentinel handler that records whether it was called.
	handlerCalled := false
	sentinel := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})
	handler := hostCheckMiddleware(sentinel)

	for _, host := range []string{"evil.example", "attacker.com", "evil.localhost"} {
		handlerCalled = false
		req := httptest.NewRequest("GET", "/api/write-token", nil)
		req.Host = host
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusForbidden {
			t.Errorf("Host %q: status %d, want 403", host, rr.Code)
		}
		if handlerCalled {
			t.Errorf("Host %q: inner handler was called despite rejection", host)
		}
		body := rr.Body.String()
		if !strings.Contains(body, "forbidden") {
			t.Errorf("Host %q: response body %q does not contain 'forbidden'", host, body)
		}
	}
}

// TestHostCheckMiddlewareAllowsConfiguredHosts pins the MESH_DASHBOARD_ALLOWED_HOSTS
// escape hatch: a configured remote Host (e.g. a tailnet name or IP) passes,
// case- and port-insensitively, while an unconfigured host still 403s and
// loopback keeps working — so the allow-list extends, never replaces, the
// secure default.
func TestHostCheckMiddlewareAllowsConfiguredHosts(t *testing.T) {
	allowed := []string{"George-Ubuntu.tail1fdfb0.ts.net", "100.117.125.1"}

	pass := []string{
		"george-ubuntu.tail1fdfb0.ts.net",      // exact (lower-cased)
		"george-ubuntu.tail1fdfb0.ts.net:8800", // with port
		"GEORGE-UBUNTU.TAIL1FDFB0.TS.NET",      // upper-cased Host header
		"100.117.125.1:8800",                   // configured IP with port
		"127.0.0.1",                            // loopback still allowed
	}
	reject := []string{"evil.example", "george-ubuntu.other.ts.net", "100.117.125.2"}

	newHandler := func() (http.Handler, *bool) {
		called := false
		sentinel := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
		})
		return hostCheckMiddleware(sentinel, allowed...), &called
	}

	for _, host := range pass {
		handler, called := newHandler()
		req := httptest.NewRequest("GET", "/api/write-token", nil)
		req.Host = host
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK || !*called {
			t.Errorf("allowed host %q: status %d called=%v, want 200/true", host, rr.Code, *called)
		}
	}
	for _, host := range reject {
		handler, called := newHandler()
		req := httptest.NewRequest("GET", "/api/write-token", nil)
		req.Host = host
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusForbidden || *called {
			t.Errorf("rejected host %q: status %d called=%v, want 403/false", host, rr.Code, *called)
		}
	}
}

// TestHostCheckMiddlewareAllowsLoopbackHosts pins that all canonical loopback
// forms pass the middleware and reach the inner handler.
func TestHostCheckMiddlewareAllowsLoopbackHosts(t *testing.T) {
	cases := []struct {
		name string
		host string
	}{
		{"bare localhost", "localhost"},
		{"localhost with port", "localhost:8080"},
		{"IPv4 loopback", "127.0.0.1"},
		{"IPv4 loopback with port", "127.0.0.1:3000"},
		{"IPv6 loopback", "[::1]"},
		{"IPv6 loopback with port", "[::1]:8080"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			handlerCalled := false
			sentinel := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handlerCalled = true
				w.WriteHeader(http.StatusOK)
			})
			handler := hostCheckMiddleware(sentinel)

			req := httptest.NewRequest("GET", "/api/write-token", nil)
			req.Host = tc.host
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Errorf("host %q: status %d, want 200", tc.host, rr.Code)
			}
			if !handlerCalled {
				t.Errorf("host %q: inner handler was not called", tc.host)
			}
		})
	}
}

// TestHostCheckMiddlewareMissingHost pins that a request without a Host header
// is rejected with 403 (HTTP/1.1 requires Host; no Host means unknown origin).
func TestHostCheckMiddlewareMissingHost(t *testing.T) {
	sentinel := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	handler := hostCheckMiddleware(sentinel)

	req := httptest.NewRequest("GET", "/api/write-token", nil)
	req.Host = "" // explicitly absent
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusForbidden {
		t.Errorf("missing Host: status %d, want 403", rr.Code)
	}
}

// TestEvilHostRejectedOnWriteTokenEndpoint and TestEvilHostRejectedOnPostJobs
// are the integration-level pins that exercise the full live stack:
// Host: evil.example must produce 403 on both write endpoints.
func TestEvilHostRejectedOnWriteTokenEndpoint(t *testing.T) {
	_, _, d := startStack(t)

	req, err := http.NewRequest("GET", "http://"+d.Addr()+"/api/write-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "evil.example"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("GET /api/write-token with Host: evil.example: status %d, want 403", resp.StatusCode)
	}
}

// TestEvilHostRejectedOnPostJobs pins that POST /api/jobs with Host: evil.example
// is rejected with 403, even when a valid bearer token is supplied. The Host
// check fires before auth, so the token never matters in the evil-host case.
func TestEvilHostRejectedOnPostJobs(t *testing.T) {
	_, _, d := startStack(t)
	token := d.WriteToken()

	req, err := http.NewRequest("POST", "http://"+d.Addr()+"/api/jobs",
		strings.NewReader(`{"repo":"r","title":"t"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "evil.example"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("POST /api/jobs with Host: evil.example: status %d, want 403", resp.StatusCode)
	}
}

// TestLoopbackHostPassesOnWriteTokenEndpoint is the positive integration pin:
// a request with an explicit loopback Host must succeed (the middleware must not
// block legitimate local traffic).
func TestLoopbackHostPassesOnWriteTokenEndpoint(t *testing.T) {
	_, _, d := startStack(t)

	req, err := http.NewRequest("GET", "http://"+d.Addr()+"/api/write-token", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "127.0.0.1:" + addrPort(d.Addr())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/write-token with loopback Host: status %d, want 200", resp.StatusCode)
	}
}

// addrPort returns just the port from a "host:port" string, for constructing
// test Host headers from the dynamically-bound listener address.
func addrPort(addr string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return port
}
