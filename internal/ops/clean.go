package ops

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/observe"
)

// ArtifactKind is what kind of mesh-owned filesystem residue an entry is.
type ArtifactKind string

const (
	ArtifactAgentSocket  ArtifactKind = "agent_socket"
	ArtifactAgentPidfile ArtifactKind = "agent_pidfile"
	ArtifactBusSocket    ArtifactKind = "bus_socket"
	ArtifactCoordPidfile ArtifactKind = "coordinator_pidfile"
)

// CleanAction is the per-artifact janitor outcome.
type CleanAction string

const (
	CleanRemoved      CleanAction = "removed"
	CleanKeptAlive    CleanAction = "kept_alive"    // backing pid is alive
	CleanKeptDialable CleanAction = "kept_dialable" // something is serving the socket
	CleanSkipped      CleanAction = "skipped"       // not a plain socket/file (e.g. symlink)
	CleanFailed       CleanAction = "failed"        // unlink error
)

// CleanEntry is one inspected artifact.
type CleanEntry struct {
	Path   string       `json:"path"`
	Kind   ArtifactKind `json:"kind"`
	Action CleanAction  `json:"action"`
	Reason string       `json:"reason,omitempty"`
}

// CleanReport is the `mesh ops clean` contract.
type CleanReport struct {
	Meta    observe.Meta `json:"meta"`
	Entries []CleanEntry `json:"entries"`
}

// Clean removes stale sockets and pidfiles with a confirm-dead-then-unlink
// rule: an artifact is only deleted once its backing process is provably not
// there (socket undialable AND the associated pid dead). A hung-but-live
// process is `down`'s job; clean never signals anything.
//
// Scope is MESH_DIR-owned paths only — every candidate comes from a cfg
// accessor or a glob under cfg.AgentsDir(); clean never roams $TMPDIR (the
// leaked meshbin dirs of issue #34 were fixed at the source, not janitored).
// Symlinks are skipped, not followed. coordinator.lock is deliberately left
// alone: unlinking the flock file would race the autostart election, and an
// empty O_CREATE-recreated lock is harmless.
func Clean(cfg config.Config) (CleanReport, error) {
	snap, err := observe.Collect(cfg)
	if err != nil {
		return CleanReport{}, err
	}
	rep := CleanReport{Meta: snap.Meta}

	// Agent sockets and pidfiles, via the same name-keyed view the
	// collector builds (drift already computed; we re-check liveness at
	// unlink time anyway — clean must not trust a stale snapshot).
	sockets, _ := filepath.Glob(filepath.Join(cfg.AgentsDir(), "*.sock"))
	for _, path := range sockets {
		name := strings.TrimSuffix(filepath.Base(path), ".sock")
		entry := CleanEntry{Path: path, Kind: ArtifactAgentSocket}
		switch {
		case !plainArtifact(path, true):
			entry.Action, entry.Reason = CleanSkipped, "not a regular unix socket"
		case observe.Dialable(path):
			entry.Action = CleanKeptDialable
		case pidfileAlive(cfg.AgentPIDFile(name)):
			entry.Action, entry.Reason = CleanKeptAlive, "sidecar pid alive (hung socket is `ops down`'s job)"
		default:
			entry.Action, entry.Reason = unlink(path)
		}
		rep.Entries = append(rep.Entries, entry)
	}

	pidfiles, _ := filepath.Glob(filepath.Join(cfg.AgentsDir(), "*.pid"))
	for _, path := range pidfiles {
		entry := CleanEntry{Path: path, Kind: ArtifactAgentPidfile}
		switch {
		case !plainArtifact(path, false):
			entry.Action, entry.Reason = CleanSkipped, "not a regular file"
		case pidfileAlive(path):
			// Conservative on pid reuse: a live pid keeps its pidfile even
			// if it might belong to someone else now; doctor surfaces it.
			entry.Action = CleanKeptAlive
		default:
			entry.Action, entry.Reason = unlink(path)
		}
		rep.Entries = append(rep.Entries, entry)
	}

	// Coordinator artifacts: bus.sock goes only when nothing serves it AND
	// the coordinator pid is dead or absent.
	coordAlive := pidfileAlive(cfg.CoordinatorPID())
	if _, err := os.Lstat(cfg.BusSocket()); err == nil {
		entry := CleanEntry{Path: cfg.BusSocket(), Kind: ArtifactBusSocket}
		switch {
		case !plainArtifact(cfg.BusSocket(), true):
			entry.Action, entry.Reason = CleanSkipped, "not a regular unix socket"
		case observe.Dialable(cfg.BusSocket()):
			entry.Action = CleanKeptDialable
		case coordAlive:
			entry.Action, entry.Reason = CleanKeptAlive, "coordinator pid alive (hung bus is `ops down`'s job)"
		default:
			entry.Action, entry.Reason = unlink(cfg.BusSocket())
		}
		rep.Entries = append(rep.Entries, entry)
	}
	if _, err := os.Lstat(cfg.CoordinatorPID()); err == nil {
		entry := CleanEntry{Path: cfg.CoordinatorPID(), Kind: ArtifactCoordPidfile}
		switch {
		case !plainArtifact(cfg.CoordinatorPID(), false):
			entry.Action, entry.Reason = CleanSkipped, "not a regular file"
		case coordAlive:
			entry.Action = CleanKeptAlive
		default:
			entry.Action, entry.Reason = unlink(cfg.CoordinatorPID())
		}
		rep.Entries = append(rep.Entries, entry)
	}

	return rep, nil
}

// plainArtifact guards against planted symlinks: sockets must be sockets,
// pidfiles must be regular files. Lstat so links are seen, not followed.
func plainArtifact(path string, wantSocket bool) bool {
	fi, err := os.Lstat(path)
	if err != nil {
		return false
	}
	if wantSocket {
		return fi.Mode()&os.ModeSocket != 0
	}
	return fi.Mode().IsRegular()
}

// pidfileAlive reports whether path names a pid that is currently alive.
// Unreadable/garbage pidfiles count as dead — they are residue.
func pidfileAlive(path string) bool {
	pid, err := observe.ReadPIDFile(path)
	if err != nil {
		return false
	}
	alive := aliveByPS([]int{pid})
	return alive[pid]
}

func unlink(path string) (CleanAction, string) {
	if err := os.Remove(path); err != nil {
		return CleanFailed, err.Error()
	}
	return CleanRemoved, "confirmed dead"
}
