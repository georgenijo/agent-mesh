// Package claim is the CAS claim engine: the one mechanism by which an agent
// takes, holds, and releases an exclusive claim on a path (locked decision:
// "KV revision-CAS is the single claim/lock primitive"). The KV record IS the
// lock — one authority per fact: whoever's record lives at the claims-bucket
// key holds the path. announce stays advisory pub/sub; a real edit
// additionally takes a claim here.
//
// Claims are TTL leases. The engine itself is stateless — pure functions over
// a bus.Client — and its callers divide the lifecycle: the sidecar renews its
// agent's claims on every heartbeat (RenewOwned), the coordinator promptly
// reclaims a departed agent's claims on evict/leave (ReleaseAllOwnedBy), and
// the store-level TTL is the backstop that frees claims even when the
// coordinator itself is down.
package claim

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// Typed errors. Callers must be able to tell a malformed request from a
// transport failure; both surface as ClaimError/ReleaseError outcomes with
// the distinguishing error in Err.
var (
	// ErrBadPath means the path cannot be claimed: empty, the repo root
	// itself, or escaping above it.
	ErrBadPath = errors.New("claim: bad path")
	// ErrBadAgent means the agent id is empty — a claim nobody owns could
	// never be renewed or released.
	ErrBadAgent = errors.New("claim: missing agent id")
	// ErrBadRepo means the repo id fails envelope.ValidRepo.
	ErrBadRepo = errors.New("claim: invalid repo id")
	// ErrRetriesExhausted means an operation kept colliding with claims that
	// expired or changed mid-flight. Transient by nature: retrying the whole
	// operation is reasonable.
	ErrRetriesExhausted = errors.New("claim: retries exhausted")
)

// Record is the stored claim. The record at the key is the single source of
// truth for "who holds this path"; everything else (announce, dashboard) is
// derived or advisory. TS is when the claim was first taken — lease renewals
// deliberately do not move it, so observers can see how long a path has been
// held.
type Record struct {
	Agent string    `json:"agent"`
	Path  string    `json:"path"`
	Repo  string    `json:"repo"`
	TS    time.Time `json:"ts"`
}

// Outcome is the result of a Take. Result is the typed claim state; Owner and
// Rev identify the live record (your own on claimed, the winner's on lost).
// Err carries detail only when Result is ClaimError — never a boolean that
// conflates "lost the race" with "transport broke" (audit Avoid #4).
type Outcome struct {
	Result envelope.ClaimResult
	Owner  Record
	Rev    uint64
	Err    error
}

// ReleaseResult is the outcome of a release attempt.
type ReleaseResult string

const (
	Released     ReleaseResult = "released"  // the claim is gone (incl. already-gone)
	NotOwner     ReleaseResult = "not_owner" // someone else holds it; nothing deleted
	ReleaseError ReleaseResult = "error"     // transport/store failure; retryable
)

// ReleaseOutcome is the result of a Release. Owner is filled on not_owner so
// the caller can see who actually holds the path. Err carries detail only
// when Result is ReleaseError.
type ReleaseOutcome struct {
	Result ReleaseResult
	Owner  Record
	Err    error
}

// Held is a live claim with its store revision, for the coordinator sweep and
// the dashboard.
type Held struct {
	Record
	Rev uint64
}

// NormalizePath canonicalizes a claim path so equivalent spellings collide on
// the same key: "./a", "a//b", and "a/./b" must contend with "a" and "a/b" —
// a lock that two spellings can slip past is no lock at all. Cleaning uses
// slash form (filepath.ToSlash) so keys are stable across platforms.
//
// Rejected with ErrBadPath: empty, "." (the repo root is not a claimable
// path), and anything cleaning to ".." or starting "../" (escapes the repo
// root). Absolute paths are accepted and cleaned but NOT relativized here —
// abs-vs-rel aliasing is resolved one layer up, at the sidecar, which knows
// the agent's repo root (card.CWD) and folds an absolute in-tree path to its
// repo-relative form before calling this. That matters because the two
// contenders for a file do not always spell it the same way: the Claude Code
// edit hook hands the tool an absolute file_path, while a human running
// `mesh claim src/foo.go` passes a repo-relative one. Both must land on one
// key or the lock is no lock at all.
func NormalizePath(p string) (string, error) {
	if strings.TrimSpace(p) == "" {
		return "", fmt.Errorf("%w: empty", ErrBadPath)
	}
	n := filepath.ToSlash(filepath.Clean(p))
	n = strings.TrimPrefix(n, "./") // Clean never leaves "./"; belt-and-braces
	if n == "." {
		return "", fmt.Errorf("%w: %q is the repo root", ErrBadPath, p)
	}
	if n == ".." || strings.HasPrefix(n, "../") {
		return "", fmt.Errorf("%w: %q escapes the repo root", ErrBadPath, p)
	}
	return n, nil
}

// Key builds the claims-bucket key for a normalized path. NUL separates the
// parts because it can appear in neither a repo id (envelope.ValidRepo) nor a
// cleaned path: under any printable delimiter that paths may contain,
// ("a", "b/c") and ("a/b", "c") could collide.
func Key(repo, normPath string) string { return repo + "\x00" + normPath }

// takeAttempts bounds the expiry-race retry inside Take. Each retry needs the
// winning record to vanish between our losing put and the ownership read — a
// window of one round-trip — so recurring three times in a row means
// something is systemically wrong and the caller should hear about it.
const takeAttempts = 3

func validateArgs(agent, repo string) error {
	if strings.TrimSpace(agent) == "" {
		return ErrBadAgent
	}
	if !envelope.ValidRepo(repo) {
		return fmt.Errorf("%w: %q", ErrBadRepo, repo)
	}
	return nil
}

// Take attempts to claim path in repo for agent, with a TTL lease. Exactly
// one contender wins a create-only CAS put; the loser learns who owns the
// path. A legitimate loss is NEVER retried (locked decision: lost means
// lost) — the only retried case is the expiry race where the winning record
// vanished between our losing put and the ownership read, i.e. nobody holds
// the path anymore.
//
// Re-taking a claim the agent already holds is idempotent (the edit hook
// re-fires on every tool call): the lease is refreshed in place, guarded by
// revision so a lose-then-reclaim by another agent can never be clobbered.
func Take(cli *bus.Client, agent, repo, path string, ttl time.Duration) Outcome {
	if err := validateArgs(agent, repo); err != nil {
		return Outcome{Result: envelope.ClaimError, Err: err}
	}
	norm, err := NormalizePath(path)
	if err != nil {
		return Outcome{Result: envelope.ClaimError, Err: err}
	}
	key := Key(repo, norm)

	for attempt := 0; attempt < takeAttempts; attempt++ {
		rec := Record{Agent: agent, Path: norm, Repo: repo, TS: time.Now().UTC()}
		rev, err := cli.KVPut(envelope.BucketClaims, key, rec,
			bus.PutOptions{CAS: bus.CreateOnly(), TTL: ttl})
		if err == nil {
			return Outcome{Result: envelope.ClaimClaimed, Owner: rec, Rev: rev}
		}
		if !errors.Is(err, bus.ErrCASLost) {
			return Outcome{Result: envelope.ClaimError, Err: err}
		}

		// Create lost: someone holds the key. Read who, so the loser can
		// negotiate with (or wait out) the actual owner.
		kv, found, gerr := cli.KVGet(envelope.BucketClaims, key)
		if gerr != nil {
			return Outcome{Result: envelope.ClaimError, Err: gerr}
		}
		if !found {
			// The winner's TTL expired between our losing put and this read.
			// Nobody holds the path, so this was not a legitimate loss —
			// retry the whole take.
			continue
		}
		var owner Record
		if uerr := json.Unmarshal(kv.Value, &owner); uerr != nil {
			// Unreadable holder: the claim genuinely exists, so this is a
			// loss — just with an anonymous owner. Reporting an empty Owner
			// honestly beats inventing one.
			return Outcome{Result: envelope.ClaimLost, Rev: kv.Rev}
		}
		if owner.Agent == agent {
			// Idempotent re-claim: refresh the lease, re-putting the stored
			// bytes so TS keeps saying when the claim was first taken.
			newRev, perr := cli.KVPut(envelope.BucketClaims, key, json.RawMessage(kv.Value),
				bus.PutOptions{CAS: bus.Rev(kv.Rev), TTL: ttl})
			if perr == nil {
				return Outcome{Result: envelope.ClaimClaimed, Owner: owner, Rev: newRev}
			}
			if errors.Is(perr, bus.ErrCASLost) {
				continue // record changed under us; re-evaluate from scratch
			}
			return Outcome{Result: envelope.ClaimError, Err: perr}
		}
		return Outcome{Result: envelope.ClaimLost, Owner: owner, Rev: kv.Rev}
	}
	return Outcome{Result: envelope.ClaimError, Err: ErrRetriesExhausted}
}

// Release frees agent's claim on path. Releasing an absent claim is an
// idempotent success — the fact is already gone. A claim held by someone
// else is not_owner: release-if-owner, never release-by-force. The delete is
// revision-guarded so an expire-and-reclaim between our read and the delete
// can never remove the new owner's claim; on that CAS loss we re-read once
// and re-evaluate who owns the path now.
func Release(cli *bus.Client, agent, repo, path string) ReleaseOutcome {
	if err := validateArgs(agent, repo); err != nil {
		return ReleaseOutcome{Result: ReleaseError, Err: err}
	}
	norm, err := NormalizePath(path)
	if err != nil {
		return ReleaseOutcome{Result: ReleaseError, Err: err}
	}
	key := Key(repo, norm)

	for attempt := 0; attempt < 2; attempt++ {
		kv, found, gerr := cli.KVGet(envelope.BucketClaims, key)
		if gerr != nil {
			return ReleaseOutcome{Result: ReleaseError, Err: gerr}
		}
		if !found {
			return ReleaseOutcome{Result: Released}
		}
		var owner Record
		if uerr := json.Unmarshal(kv.Value, &owner); uerr != nil {
			// Ownership of an unreadable record cannot be proven — refuse to
			// delete it (degrade, don't guess). The TTL will collect it.
			return ReleaseOutcome{Result: NotOwner}
		}
		if owner.Agent != agent {
			return ReleaseOutcome{Result: NotOwner, Owner: owner}
		}
		derr := cli.KVDeleteRev(envelope.BucketClaims, key, kv.Rev)
		if derr == nil {
			return ReleaseOutcome{Result: Released, Owner: owner}
		}
		if !errors.Is(derr, bus.ErrCASLost) {
			return ReleaseOutcome{Result: ReleaseError, Err: derr}
		}
	}
	return ReleaseOutcome{Result: ReleaseError, Err: ErrRetriesExhausted}
}

// ListAll returns every live claim with its revision, sorted by (repo, path)
// so sweeps and dashboards render deterministically. Unparseable records are
// skipped with a warning — one corrupt record must never blind the sweep to
// all the others (degrade-don't-throw at the boundary).
func ListAll(cli *bus.Client) ([]Held, error) {
	keys, err := cli.KVList(envelope.BucketClaims)
	if err != nil {
		return nil, err
	}
	held := make([]Held, 0, len(keys))
	for key, kv := range keys {
		var rec Record
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			slog.Warn("claim: skipping unparseable record", "key", key, "err", err)
			continue
		}
		held = append(held, Held{Record: rec, Rev: kv.Rev})
	}
	sortByRepoPath(held, func(h Held) Record { return h.Record })
	return held, nil
}

// RenewOwned refreshes the TTL lease on every claim agent holds; the sidecar
// calls it on each heartbeat so a live agent never loses claims to the TTL
// backstop. A renewal that loses its CAS is skipped, not retried: the
// revision changed since we listed, so the claim was legitimately lost
// (expired and retaken) and renewing it would steal it back. Returns the
// number renewed; a transport error stops the pass (partial count + error).
func RenewOwned(cli *bus.Client, agent string, ttl time.Duration) (int, error) {
	if strings.TrimSpace(agent) == "" {
		return 0, ErrBadAgent
	}
	keys, err := cli.KVList(envelope.BucketClaims)
	if err != nil {
		return 0, err
	}
	renewed := 0
	for key, kv := range keys {
		var rec Record
		if err := json.Unmarshal(kv.Value, &rec); err != nil || rec.Agent != agent {
			continue
		}
		// Re-put the stored bytes unchanged: only the lease moves; TS keeps
		// saying when the claim was first taken.
		_, perr := cli.KVPut(envelope.BucketClaims, key, json.RawMessage(kv.Value),
			bus.PutOptions{CAS: bus.Rev(kv.Rev), TTL: ttl})
		if perr == nil {
			renewed++
			continue
		}
		if errors.Is(perr, bus.ErrCASLost) {
			continue
		}
		return renewed, perr
	}
	return renewed, nil
}

// ReleaseAllOwnedBy guarded-deletes every claim agent holds and returns the
// records actually released, sorted by (repo, path), so the coordinator can
// audit each one. It never returns an error: this runs on the evict/leave
// path where there is no caller to surface a failure to, and any claim it
// fails to free is collected by the store TTL — the backstop for when the
// coordinator itself is down. A delete that loses its CAS is skipped: the
// claim was already released and retaken by someone else.
func ReleaseAllOwnedBy(cli *bus.Client, agent string) []Record {
	if strings.TrimSpace(agent) == "" {
		return nil
	}
	keys, err := cli.KVList(envelope.BucketClaims)
	if err != nil {
		return nil
	}
	var released []Record
	for key, kv := range keys {
		var rec Record
		if err := json.Unmarshal(kv.Value, &rec); err != nil || rec.Agent != agent {
			continue
		}
		if err := cli.KVDeleteRev(envelope.BucketClaims, key, kv.Rev); err != nil {
			continue
		}
		released = append(released, rec)
	}
	sortByRepoPath(released, func(r Record) Record { return r })
	return released
}

// sortByRepoPath orders claim slices deterministically (KV listing is a map).
func sortByRepoPath[T any](s []T, rec func(T) Record) {
	sort.Slice(s, func(i, j int) bool {
		a, b := rec(s[i]), rec(s[j])
		if a.Repo != b.Repo {
			return a.Repo < b.Repo
		}
		return a.Path < b.Path
	})
}
