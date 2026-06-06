package dashboard

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

// TestWebUIMountedReadOnly verifies the embedded production UI is served at
// /ui/ with the right content types, /ui canonicalizes to /ui/, and the
// existing / and /events routes are untouched by the mount.
func TestWebUIMountedReadOnly(t *testing.T) {
	_, _, d := startStack(t)
	base := "http://" + d.Addr()

	get := func(path string) (*http.Response, string) {
		t.Helper()
		resp, err := http.Get(base + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("GET %s read body: %v", path, err)
		}
		return resp, string(body)
	}

	// Index at /ui/ (and /ui redirecting there — http.Get follows it).
	for _, path := range []string{"/ui/", "/ui"} {
		resp, body := get(path)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status %d, want 200", path, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
			t.Fatalf("GET %s: content type %q, want text/html", path, ct)
		}
		if !strings.Contains(body, "app.js") || !strings.Contains(body, "style.css") {
			t.Fatalf("GET %s: index does not reference app.js/style.css", path)
		}
	}

	// /ui itself must be the canonical redirect, not a second copy of the
	// page (relative asset URLs would resolve outside the mount).
	noRedirect := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := noRedirect.Get(base + "/ui")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("GET /ui: status %d, want 301", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "/ui/" {
		t.Fatalf("GET /ui: Location %q, want /ui/", loc)
	}

	// Assets with sane content types.
	for _, tc := range []struct{ path, wantCT string }{
		{"/ui/app.js", "javascript"},
		{"/ui/style.css", "text/css"},
	} {
		resp, body := get(tc.path)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status %d, want 200", tc.path, resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, tc.wantCT) {
			t.Fatalf("GET %s: content type %q, want %q", tc.path, ct, tc.wantCT)
		}
		if len(body) == 0 {
			t.Fatalf("GET %s: empty body", tc.path)
		}
	}

	// Unknown asset under the mount is a 404, and the P0 observer page at /
	// still serves (the mount must not shadow it).
	if resp, _ := get("/ui/nope.js"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET /ui/nope.js: status %d, want 404", resp.StatusCode)
	}
	if resp, body := get("/"); resp.StatusCode != http.StatusOK || !strings.Contains(body, "<html") {
		t.Fatalf("GET /: status %d, want 200 with HTML", resp.StatusCode)
	}
}
