// Package e2e — CLI contract tests: golden --json snapshots + exit-code matrix.
//
// Issue #39: the mesh CLI's --json output is the agent-facing API. Nothing
// previously pinned its shape. This file pins it.
//
// Golden files live in testdata/golden/. Run with -update to regenerate:
//
//	go test ./test/e2e -run TestCLIContract -update
//
// Volatile fields (UUIDs, pids, timestamps, paths) are replaced with stable
// placeholders before comparison so goldens contain no host-specific data.
package e2e

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

var updateGolden = flag.Bool("update", false, "regenerate golden files instead of comparing")

// goldenDir is the directory containing *.json golden files.
const goldenDir = "testdata/golden"

// volatileKeys lists JSON object keys whose values are replaced with stable
// placeholders during normalization. Any key anywhere in the tree matching
// these names is replaced, EXCEPT "message" which is only replaced in error
// objects (see replaceVolatile).
var volatileKeys = map[string]string{
	// UUIDs / IDs
	"id":         "<id>",
	"ticket":     "<ticket>",
	"job":        "<job>",
	"answeredBy": "<answeredBy>",
	// PIDs
	"pid":        "<pid>",
	"sidecarPid": "<pid>",
	"pidFilePid": "<pid>",
	// Timestamps — all keys that carry time.Time values in any verb output.
	"registeredAt": "<ts>",
	"lastSeen":     "<ts>",
	"lastStatusAt": "<ts>",
	"collectedAt":  "<ts>",
	"startedAt":    "<ts>",
	"expiresAt":    "<ts>",
	"answeredAt":   "<ts>",
	"at":           "<ts>",
	"ts":           "<ts>",
	"createdAt":    "<ts>",
	"since":        "<ts>",
	// Paths / host-specific
	"meshDir":  "<meshDir>",
	"socket":   "<socket>",
	"cwd":      "<cwd>",
	"logPath":  "<logPath>",
	"pidFile":  "<pidFile>",
	"addrFile": "<addrFile>",
	// Other volatile
	"uptime": "<uptime>",
	"cmd":    "<cmd>",
	"addr":   "<addr>",
	// NOTE: "message" is intentionally omitted here; it is only normalized when
	// it appears inside a typed error object (ok:false or alongside "code").
	// See replaceVolatile for the scoped logic.
}

// normalizeJSON decodes raw JSON into map[string]any, recursively replaces
// volatile fields, sorts array-of-object elements by "name" or "card.name"
// so ordering is deterministic, then re-marshals with sorted map keys.
func normalizeJSON(raw string) (string, error) {
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return "", fmt.Errorf("normalizeJSON: unmarshal: %w", err)
	}
	v = replaceVolatile(v)
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", fmt.Errorf("normalizeJSON: marshal: %w", err)
	}
	return string(b) + "\n", nil
}

// isErrorObject returns true when the map looks like a typed CLI error object,
// i.e. it has "ok": false or contains both "code" and "message" keys.
func isErrorObject(m map[string]any) bool {
	if ok, exists := m["ok"]; exists {
		if b, isBool := ok.(bool); isBool && !b {
			return true
		}
	}
	_, hasCode := m["code"]
	_, hasMsg := m["message"]
	return hasCode && hasMsg
}

func replaceVolatile(v any) any {
	switch val := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(val))
		errObj := isErrorObject(val)
		for k, sub := range val {
			if placeholder, ok := volatileKeys[k]; ok {
				// Replace the value but keep the key so golden structure is visible.
				out[k] = placeholder
			} else if k == "message" && errObj {
				// Scope "message" normalization to typed error objects only, so that
				// non-error contract fields named "message" are not silently masked.
				out[k] = "<message>"
			} else {
				out[k] = replaceVolatile(sub)
			}
		}
		return out
	case []any:
		result := make([]any, len(val))
		for i, item := range val {
			result[i] = replaceVolatile(item)
		}
		// Sort arrays of objects by "name" field (agents, sidecars) so order
		// is deterministic regardless of registry scan order.
		sortByName(result)
		return result
	default:
		return v
	}
}

func sortByName(arr []any) {
	sort.SliceStable(arr, func(i, j int) bool {
		ni := nameOf(arr[i])
		nj := nameOf(arr[j])
		if ni != nj {
			return ni < nj
		}
		// Both lack a distinguishing name: fall back to marshaled JSON so the
		// sort is still fully deterministic (avoids golden flakes on unnamed elements).
		bi, _ := json.Marshal(arr[i])
		bj, _ := json.Marshal(arr[j])
		return string(bi) < string(bj)
	})
}

func nameOf(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		// Non-object elements: use marshaled JSON as the sort key so arrays of
		// scalars are also ordered deterministically.
		b, _ := json.Marshal(v)
		return string(b)
	}
	// Try "name" directly (sidecar info).
	if n, ok := m["name"].(string); ok {
		return n
	}
	// Try card.name (registry record in who result).
	if card, ok := m["card"].(map[string]any); ok {
		if n, ok := card["name"].(string); ok {
			return n
		}
	}
	return ""
}

// checkGolden compares got against the named golden file. If -update is set,
// writes the file instead. On mismatch it prints the remedy message verbatim.
func checkGolden(t *testing.T, name, got string) {
	t.Helper()
	normalized, err := normalizeJSON(got)
	if err != nil {
		t.Fatalf("normalize %s: %v\nraw output: %s", name, err, got)
	}

	path := filepath.Join(goldenDir, name+".json")
	if *updateGolden {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, []byte(normalized), 0o644); err != nil {
			t.Fatalf("write golden %s: %v", path, err)
		}
		t.Logf("updated golden: %s", path)
		return
	}

	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s: %v\nRun: go test ./test/e2e -run TestCLIContract -update", path, err)
	}
	if normalized != string(want) {
		t.Fatalf("mesh %s --json contract changed; if intentional, run "+
			"`go test ./test/e2e -run TestCLIContract -update` and commit the golden diff in this PR\n\n"+
			"GOT:\n%s\nWANT:\n%s", name, normalized, string(want))
	}
}

// TestCLIContract runs every current verb with --json against a real
// coordinator+sidecar, normalizes volatile fields, and byte-compares against
// the golden file.
func TestCLIContract(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()

	// --- join fresh → {"card":{...},"rejoined":false} ---------------------------
	t.Run("join_fresh", func(t *testing.T) {
		code, stdout, stderr := m.run("join", "--name", "alpha", "--role", "builder",
			"--caps", "go", "--repo", "testrepo", "--json")
		if code != 0 {
			t.Fatalf("join exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
		}
		checkGolden(t, "join_fresh", stdout)
	})

	// Wait for alpha to be fully visible before snapshotting who.
	m.eventually(3*time.Second, "alpha is live in registry", func() bool {
		agents, exit := m.who("alpha")
		if exit != 0 {
			return false
		}
		_, ok := findAgent(agents, "alpha")
		return ok
	})

	// --- join rejoined → {"card":{...},"rejoined":true} -------------------------
	t.Run("join_rejoined", func(t *testing.T) {
		code, stdout, stderr := m.run("join", "--name", "alpha", "--role", "builder",
			"--caps", "go", "--repo", "testrepo", "--json",
			"--socket", m.agentSocket("alpha"))
		if code != 0 {
			t.Fatalf("join rejoined exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
		}
		checkGolden(t, "join_rejoined", stdout)
	})

	// --- status → {"id":"...","text":"..."} -------------------------------------
	t.Run("status", func(t *testing.T) {
		code, stdout, stderr := m.run("status", "working on tests",
			"--socket", m.agentSocket("alpha"), "--json")
		if code != 0 {
			t.Fatalf("status exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
		}
		checkGolden(t, "status", stdout)
	})

	// Wait for alpha's status to appear in the registry.
	m.eventually(3*time.Second, "alpha status visible", func() bool {
		agents, exit := m.who("alpha")
		if exit != 0 {
			return false
		}
		rec, ok := findAgent(agents, "alpha")
		return ok && rec.LastStatus == "working on tests"
	})

	// --- who → {"agents":[RegistryRecord{...}]} ---------------------------------
	t.Run("who", func(t *testing.T) {
		code, stdout, stderr := m.run("who", "--socket", m.agentSocket("alpha"), "--json")
		if code != 0 {
			t.Fatalf("who exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
		}
		checkGolden(t, "who", stdout)
	})

	// --- ops → observe.Snapshot (with alpha sidecar running) --------------------
	t.Run("ops", func(t *testing.T) {
		code, stdout, stderr := m.run("ops", "--json")
		if code != 0 {
			t.Fatalf("ops exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
		}
		checkGolden(t, "ops", stdout)
	})

	// --- leave → {"id":"..."} ---------------------------------------------------
	t.Run("leave", func(t *testing.T) {
		code, stdout, stderr := m.run("leave", "--socket", m.agentSocket("alpha"), "--json")
		if code != 0 {
			t.Fatalf("leave exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
		}
		checkGolden(t, "leave", stdout)
	})
}

// TestCLIContractErrorObjectShape pins the typed error JSON printed to stdout
// by emit() when --json is set and the sidecar returns a typed failure. We use
// a real sidecar (from the coordinator setup) and send it an ask with a role
// that has no responder, triggering a not_joined response from the bus path.
//
// The alternative (pre-socket failure where resolveSocket returns not-joined
// when NO socket file exists) bypasses emit() and prints to stderr even with
// --json. That asymmetry is frozen as-is; see testdata/golden/README.md.
func TestCLIContractErrorObjectShape(t *testing.T) {
	m := newMesh(t)
	m.startCoordinator()

	// Join an agent.
	if code, _, stderr := m.run("join", "--name", "err-agent", "--role", "builder"); code != 0 {
		t.Fatalf("setup join: %s", stderr)
	}

	// ask a role with no responder → the sidecar returns a not_joined (or
	// similar typed) error via the bus path, which goes through emit().
	code, stdout, stderr := m.run("ask", "--role", "no-such-role",
		"working?", "--json", "--socket", m.agentSocket("err-agent"))

	// ask with --role no-such-role must fail: the coordinator should reject the
	// request with a typed error because no agent owns that role.
	// We FAIL (not skip) if ask succeeds, so the golden cannot rot silently —
	// a future change that makes ask succeed would need to update this test and
	// the golden file explicitly.
	if code == 0 {
		t.Fatalf("ask with unknown role unexpectedly succeeded (exit 0); "+
			"golden error_ask_no_role.json would silently stop being tested. "+
			"If this behavior is now intentional, update this test and remove the golden. "+
			"stdout=%q stderr=%q", stdout, stderr)
	}
	if !strings.Contains(stdout, `"ok"`) {
		t.Fatalf("expected JSON error object on stdout, got stdout=%q stderr=%q", stdout, stderr)
	}
	checkGolden(t, "error_ask_no_role", stdout)
}

// TestExitCodeMatrix is the cross-process exit-code contract table.
// Each row runs one scenario end-to-end and asserts the exact exit code.
//
// Codes 3 (no-answer-yet) and 4 (no-such-ticket) are P2 poll codes —
// represented here as reserved rows that are skipped until P2 lands.
// Code 7 (dirty) is tested in ops_test.go (TestOpsDoctorClassifies).
func TestExitCodeMatrix(t *testing.T) {
	type row struct {
		name     string
		skip     string // non-empty → t.Skip with this reason
		wantCode int
		setup    func(m *mesh) // called after mesh+coordinator is up
		run      func(m *mesh) (int, string, string)
	}

	rows := []row{
		// --- exit 0: happy path for each verb -----------------------------------
		{
			name:     "join_ok",
			wantCode: 0,
			run: func(m *mesh) (int, string, string) {
				return m.run("join", "--name", "ec-agent", "--role", "builder")
			},
		},
		{
			name:     "status_ok",
			wantCode: 0,
			setup: func(m *mesh) {
				if code, _, stderr := m.run("join", "--name", "ec-agent", "--role", "builder"); code != 0 {
					t.Fatalf("setup join: %s", stderr)
				}
			},
			run: func(m *mesh) (int, string, string) {
				return m.run("status", "ok", "--socket", m.agentSocket("ec-agent"))
			},
		},
		{
			name:     "who_ok",
			wantCode: 0,
			setup: func(m *mesh) {
				if code, _, stderr := m.run("join", "--name", "ec-agent", "--role", "builder"); code != 0 {
					t.Fatalf("setup join: %s", stderr)
				}
			},
			run: func(m *mesh) (int, string, string) {
				return m.run("who", "--socket", m.agentSocket("ec-agent"))
			},
		},
		{
			name:     "leave_ok",
			wantCode: 0,
			setup: func(m *mesh) {
				if code, _, stderr := m.run("join", "--name", "ec-agent", "--role", "builder"); code != 0 {
					t.Fatalf("setup join: %s", stderr)
				}
			},
			run: func(m *mesh) (int, string, string) {
				return m.run("leave", "--socket", m.agentSocket("ec-agent"))
			},
		},
		{
			name:     "ops_ok",
			wantCode: 0,
			run: func(m *mesh) (int, string, string) {
				return m.run("ops")
			},
		},
		{
			name:     "version_ok",
			wantCode: 0,
			run: func(m *mesh) (int, string, string) {
				return m.run("version")
			},
		},

		// --- exit 2: usage errors -----------------------------------------------
		{
			name:     "usage_no_args",
			wantCode: 2,
			run: func(m *mesh) (int, string, string) {
				return m.run()
			},
		},
		{
			name:     "usage_unknown_verb",
			wantCode: 2,
			run: func(m *mesh) (int, string, string) {
				return m.run("frobnicate")
			},
		},
		{
			name:     "usage_join_missing_name",
			wantCode: 2,
			run: func(m *mesh) (int, string, string) {
				return m.run("join", "--role", "builder")
			},
		},
		{
			name:     "usage_join_missing_role",
			wantCode: 2,
			run: func(m *mesh) (int, string, string) {
				return m.run("join", "--name", "x")
			},
		},
		{
			name:     "usage_join_invalid_name",
			wantCode: 2,
			run: func(m *mesh) (int, string, string) {
				return m.run("join", "--name", "bad.name", "--role", "builder")
			},
		},
		{
			name:     "usage_status_no_text",
			wantCode: 2,
			run: func(m *mesh) (int, string, string) {
				return m.run("status")
			},
		},
		{
			name:     "usage_multi_socket_ambiguous",
			wantCode: 2,
			setup: func(m *mesh) {
				// Join two agents so resolveSocket sees multiple sockets.
				for _, name := range []string{"amb-a", "amb-b"} {
					if code, _, stderr := m.run("join", "--name", name, "--role", "builder"); code != 0 {
						t.Fatalf("setup join %s: %s", name, stderr)
					}
				}
			},
			run: func(m *mesh) (int, string, string) {
				// who without --socket → multiple sockets → exit 2
				return m.run("who")
			},
		},

		// --- exit 5: not joined -------------------------------------------------
		{
			name:     "not_joined_who",
			wantCode: 5,
			// No setup: no sidecar running.
			run: func(m *mesh) (int, string, string) {
				return m.run("who")
			},
		},
		{
			name:     "not_joined_status",
			wantCode: 5,
			run: func(m *mesh) (int, string, string) {
				return m.run("status", "hi")
			},
		},
		{
			name:     "not_joined_second_leave",
			wantCode: 5,
			setup: func(m *mesh) {
				if code, _, stderr := m.run("join", "--name", "ec-solo", "--role", "builder"); code != 0 {
					t.Fatalf("setup join: %s", stderr)
				}
				if code, _, stderr := m.run("leave", "--socket", m.agentSocket("ec-solo")); code != 0 {
					t.Fatalf("first leave: %s", stderr)
				}
				// Wait for socket to disappear.
				m.eventually(3*time.Second, "socket gone", func() bool {
					_, err := os.Stat(m.agentSocket("ec-solo"))
					return os.IsNotExist(err)
				})
			},
			run: func(m *mesh) (int, string, string) {
				return m.run("leave")
			},
		},

		// --- exit 3: reserved (P2 poll, no answer yet) --------------------------
		{
			name:     "reserved_exit_3_no_answer_yet",
			skip:     "exit 3 is reserved for P2 poll verb (not yet implemented in contract tests)",
			wantCode: 3,
			run:      func(m *mesh) (int, string, string) { return 3, "", "" },
		},

		// --- exit 4: reserved (P2 poll, no such ticket) -------------------------
		{
			name:     "reserved_exit_4_no_such_ticket",
			skip:     "exit 4 is reserved for P2 poll verb (not yet implemented in contract tests)",
			wantCode: 4,
			run:      func(m *mesh) (int, string, string) { return 4, "", "" },
		},
	}

	for _, row := range rows {
		row := row
		t.Run(row.name, func(t *testing.T) {
			if row.skip != "" {
				t.Skip(row.skip)
			}
			m := newMesh(t)
			m.startCoordinator()
			if row.setup != nil {
				row.setup(m)
			}
			code, _, _ := row.run(m)
			if code != row.wantCode {
				t.Errorf("exit code = %d, want %d", code, row.wantCode)
			}
		})
	}
}
