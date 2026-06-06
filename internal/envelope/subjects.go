package envelope

import "regexp"

// Subject taxonomy, KV bucket and stream names — the shared spine every
// component must agree on (docs/components.md "The shared spine"). Defined
// here, beside the envelope codec, so the whole wire contract lives in one
// package and no component hand-rolls a subject string.

// Fixed subjects.
const (
	SubjectRegister = "mesh.register"
	SubjectLeave    = "mesh.leave"
)

// Parameterized subjects.
func SubjectHeartbeat(id string) string  { return "mesh.heartbeat." + id }
func SubjectStatus(id string) string     { return "mesh.status." + id }
func SubjectAnnounce(repo string) string { return "mesh.announce." + repo }

// Subscription patterns.
const (
	PatternAll        = "mesh.>"
	PatternHeartbeats = "mesh.heartbeat.>"
	PatternStatuses   = "mesh.status.>"
	PatternAnnounces  = "mesh.announce.>"
)

// KV buckets. One authority per fact: the registry bucket is the single
// source of truth for "who exists and in what presence state"; only the
// coordinator writes it. The claims bucket is the single source of truth for
// "who holds which path" — the CAS record is the lock, announce is advisory.
const (
	BucketRegistry = "registry"
	BucketClaims   = "claims"
)

// Streams (bounded).
const (
	StreamAudit = "audit"
)

// StreamNotes is the per-repo durable blackboard stream name.
func StreamNotes(repo string) string { return "notes-" + repo }

// repoRE constrains repo ids: they become subject tokens
// (mesh.announce.<repo>) and stream names (notes-<repo>), so dots, wildcards,
// slashes, and whitespace are forbidden, and length is bounded so derived
// store names stay within the bus's 64-char name limit. A repo id is a label
// chosen at join/claim time, not a filesystem path.
var repoRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,48}$`)

// ValidRepo reports whether s is a legal repo id.
func ValidRepo(s string) bool { return repoRE.MatchString(s) }

// DefaultRepo is the repo identity used when an agent does not set one.
const DefaultRepo = "default"
