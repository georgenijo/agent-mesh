package observe

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Run-file protocol shared by the optional HTTP daemons (dashboard, observe):
// on start each writes <name>.pid then <name>.addr under MESH_DIR; the addr
// file carries the REAL bound address and is written last and atomically, so
// a reader who sees it is guaranteed a complete pidfile and an accepting
// listener. `mesh up` gates readiness on the addr file; the ops plane treats
// the pidfile as the liveness authority.

// ListenWithFallback binds addr, falling back to an OS-picked loopback port
// when the configured one is taken by a foreign process (TCP ports are global
// while everything else is MESH_DIR-namespaced — two meshes must coexist).
// Any other listen error stays fatal: a privileged-port or bad-addr config
// mistake must not silently move ports.
func ListenWithFallback(addr string, log *slog.Logger) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err == nil {
		return ln, nil
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		return nil, err
	}
	ln, fbErr := net.Listen("tcp", "127.0.0.1:0")
	if fbErr != nil {
		return nil, fmt.Errorf("%v (and :0 fallback failed: %w)", err, fbErr)
	}
	log.Warn("configured address in use; fell back to an OS-picked port",
		"configured", addr, "bound", ln.Addr().String())
	return ln, nil
}

// WriteRunFiles records the daemon's pid and real bound address. Pid first;
// addr last and atomically (tmp + rename) — the ordering is the readiness
// contract documented above.
func WriteRunFiles(pidPath, addrPath, addr string) error {
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o600); err != nil {
		return fmt.Errorf("write pid file: %w", err)
	}
	tmp := addrPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(addr+"\n"), 0o600); err != nil {
		os.Remove(pidPath) //nolint:errcheck
		return fmt.Errorf("write addr file: %w", err)
	}
	if err := os.Rename(tmp, addrPath); err != nil {
		os.Remove(tmp)     //nolint:errcheck
		os.Remove(pidPath) //nolint:errcheck
		return fmt.Errorf("commit addr file: %w", err)
	}
	return nil
}

// RemoveRunFiles is best-effort cleanup on graceful stop; SIGKILL residue is
// `mesh ops clean`'s job.
func RemoveRunFiles(pidPath, addrPath string) {
	os.Remove(pidPath)  //nolint:errcheck
	os.Remove(addrPath) //nolint:errcheck
}

// ReadAddrFile reads a run-file address. Empty or whitespace-only content is
// an error — never a silently-empty address.
func ReadAddrFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	addr := strings.TrimSpace(string(b))
	if addr == "" {
		return "", fmt.Errorf("observe: addr file %s is empty", path)
	}
	return addr, nil
}

// DialableTCP reports whether a TCP listener answers at addr. Never treat
// this alone as "ours" — pair it with a live pidfile (a foreign process may
// hold a recycled port).
func DialableTCP(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}
