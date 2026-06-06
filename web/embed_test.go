package web

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"

	"github.com/georgenijo/agent-mesh/internal/envelope"
)

func TestAssetsContainIndex(t *testing.T) {
	data, err := fs.ReadFile(Assets, "index.html")
	if err != nil {
		t.Fatalf("Assets missing index.html: %v", err)
	}
	page := strings.ToLower(string(data))
	if !strings.Contains(page, "<!doctype html>") {
		t.Fatal("index.html does not look like an HTML document")
	}
	for _, ref := range []string{"app.js", "style.css"} {
		if !strings.Contains(page, ref) {
			t.Fatalf("index.html does not reference %s", ref)
		}
	}
}

func TestAssetsComplete(t *testing.T) {
	for _, name := range []string{"index.html", "app.js", "style.css"} {
		info, err := fs.Stat(Assets, name)
		if err != nil {
			t.Fatalf("Assets missing %s: %v", name, err)
		}
		if info.Size() == 0 {
			t.Fatalf("%s is embedded but empty", name)
		}
	}
}

// TestAppKindsMatchWire pins the UI's kind vocabulary to the wire contract:
// every kind app.js names — the KINDS array and every env.kind case literal —
// must be a kind internal/envelope actually accepts. An invented kind (e.g.
// a "release" that does not exist on the wire) could never traverse the bus:
// it is rejected at the publish edge and dropped by the SSE bridge's decode,
// so any view keyed on it would be permanently dead.
func TestAppKindsMatchWire(t *testing.T) {
	data, err := fs.ReadFile(Assets, "app.js")
	if err != nil {
		t.Fatalf("Assets missing app.js: %v", err)
	}
	js := string(data)

	arr := regexp.MustCompile(`const KINDS = \[([^\]]*)\]`).FindStringSubmatch(js)
	if arr == nil {
		t.Fatal("app.js no longer declares the KINDS array")
	}
	var kinds []string
	for _, m := range regexp.MustCompile(`"([a-z_]+)"`).FindAllStringSubmatch(arr[1], -1) {
		kinds = append(kinds, m[1])
	}
	if len(kinds) == 0 {
		t.Fatal("KINDS array is empty")
	}
	// Every switch in app.js with string case literals discriminates on
	// env.kind, so each case literal must be a wire kind too.
	for _, m := range regexp.MustCompile(`case "([a-z_]+)":`).FindAllStringSubmatch(js, -1) {
		kinds = append(kinds, m[1])
	}

	for _, kind := range kinds {
		if _, err := envelope.New(envelope.Kind(kind), "web-test", "mesh.test", struct{}{}); err != nil {
			t.Errorf("app.js names kind %q, which the wire rejects: %v", kind, err)
		}
	}
}

// TestAppConsumesSSE pins the asset to the observer contract: the UI must
// consume GET /events with EventSource and never invent another transport.
func TestAppConsumesSSE(t *testing.T) {
	data, err := fs.ReadFile(Assets, "app.js")
	if err != nil {
		t.Fatalf("Assets missing app.js: %v", err)
	}
	js := string(data)
	if !strings.Contains(js, `new EventSource("/events")`) {
		t.Fatal("app.js does not open EventSource(\"/events\")")
	}
	if strings.Contains(js, "WebSocket") {
		t.Fatal("app.js must use SSE, not WebSocket (stdlib-only server)")
	}
}
