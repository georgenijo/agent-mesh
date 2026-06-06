package envelope

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
func SubjectHeartbeat(id string) string { return "mesh.heartbeat." + id }
func SubjectStatus(id string) string    { return "mesh.status." + id }

// Subscription patterns.
const (
	PatternAll        = "mesh.>"
	PatternHeartbeats = "mesh.heartbeat.>"
	PatternStatuses   = "mesh.status.>"
)

// KV buckets. One authority per fact: the registry bucket is the single
// source of truth for "who exists and in what presence state"; only the
// coordinator writes it.
const (
	BucketRegistry = "registry"
)

// Streams (bounded).
const (
	StreamAudit = "audit"
)
