// Package testsock provides short unix-socket paths for tests.
//
// Unix socket paths are limited to ~104 bytes on darwin (108 on linux);
// t.TempDir() embeds the full test name and routinely exceeds that, failing
// with "bind: invalid argument". Tests must use Dir/Path instead of
// t.TempDir() for anything that binds a socket.
package testsock

import (
	"os"
	"path/filepath"
	"testing"
)

// Dir returns a short-lived temp dir with a short path, removed on cleanup.
func Dir(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "mesh")
	if err != nil {
		t.Fatalf("testsock: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// Path returns a short socket path inside a fresh short temp dir.
func Path(t testing.TB, name string) string {
	t.Helper()
	return filepath.Join(Dir(t), name)
}
