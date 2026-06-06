package observe

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/testsock"
)

func TestSnapshotEndpoint(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	if err := cfg.EnsureDirs(); err != nil {
		t.Fatal(err)
	}
	srv := New(cfg, "127.0.0.1:0", nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)

	resp, err := http.Get(fmt.Sprintf("http://%s/api/snapshot", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var snap Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatal(err)
	}
	if snap.Meta.MeshDir != cfg.MeshDir {
		t.Fatalf("meshDir = %q", snap.Meta.MeshDir)
	}
	if snap.Meta.CollectedAt.IsZero() {
		t.Fatal("collectedAt missing")
	}
}

func TestIndexServesHTML(t *testing.T) {
	cfg := config.Config{MeshDir: testsock.Dir(t)}
	srv := New(cfg, "127.0.0.1:0", nil)
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Stop)

	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s/", srv.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
