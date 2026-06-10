package task

// The planner-output plan: the document a triage planner must emit
// (#24). internal/triage owns invoking the planner and the prompt that
// requests this shape; this file owns what a *valid* plan is — strict,
// typed validation (never scraped from prose) and the fold from validated
// plan nodes into authoritative task Records.

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// PlanVersion is the plan document version this package accepts. The planner
// prompt pins it; any other value is a typed validation error.
const PlanVersion = 1

// MaxPlanNodes bounds a single plan. A triage decomposition past this size is
// almost certainly a runaway planner, not a real work breakdown.
const MaxPlanNodes = 64

// nodeIDRE constrains planner node ids: short slugs, no whitespace or JSON
// noise, bounded length. Node ids are plan-internal (task ids are UUIDv7s),
// but they appear in events and reasons, so keep them printable and small.
var nodeIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

// Plan is the document the planner must answer with.
type Plan struct {
	Version int    `json:"version"`
	Nodes   []Node `json:"nodes"`
}

// Node is one planned unit of work — a DAG node.
type Node struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Description string   `json:"description,omitempty"`
	Role        string   `json:"role"`
	DependsOn   []string `json:"dependsOn,omitempty"`
	Files       []string `json:"files,omitempty"`
	Acceptance  []string `json:"acceptance,omitempty"`
}

// PlanErrorCode classifies why a plan failed validation. These surface as
// detail inside the wire-level envelope.TriageInvalidDAG triage error; they
// are typed here so tests assert codes, not prose.
type PlanErrorCode string

const (
	PlanBadVersion   PlanErrorCode = "bad_version"
	PlanNoNodes      PlanErrorCode = "no_nodes"
	PlanTooManyNodes PlanErrorCode = "too_many_nodes"
	PlanBadNodeID    PlanErrorCode = "bad_node_id"
	PlanDuplicateID  PlanErrorCode = "duplicate_node_id"
	PlanMissingTitle PlanErrorCode = "missing_title"
	PlanUnknownRole  PlanErrorCode = "unknown_role"
	PlanUnknownDep   PlanErrorCode = "unknown_dep"
	PlanCycle        PlanErrorCode = "cycle"
)

// PlanError is the typed validation failure for a plan document.
type PlanError struct {
	Code   PlanErrorCode
	Detail string
}

func (e *PlanError) Error() string { return fmt.Sprintf("plan: %s: %s", e.Code, e.Detail) }

// IsPlanError reports whether err is a PlanError with the given code.
func IsPlanError(err error, code PlanErrorCode) bool {
	pe, ok := err.(*PlanError)
	return ok && pe.Code == code
}

// DecodePlan strictly parses a plan document from the planner's result text.
// The text must be exactly one JSON object (unknown fields tolerated for
// forward compat, trailing garbage rejected). One deterministic normalization
// is applied before parsing: a single markdown code fence wrapping the whole
// document is stripped, because instruction-following on "no fences" is not
// reliable enough to fail an otherwise valid plan over. Anything else —
// prose, partial JSON, multiple documents — is a typed error, never scraped.
func DecodePlan(text string) (Plan, error) {
	body := strings.TrimSpace(text)
	if fenced, ok := stripFence(body); ok {
		body = fenced
	}
	var p Plan
	if err := json.Unmarshal([]byte(body), &p); err != nil {
		return Plan{}, fmt.Errorf("plan: not a JSON plan document: %w", err)
	}
	return p, nil
}

// stripFence removes one whole-document markdown code fence (``` or
// ```json). It only fires when the fence wraps the entire trimmed text.
func stripFence(s string) (string, bool) {
	if !strings.HasPrefix(s, "```") || !strings.HasSuffix(s, "```") {
		return "", false
	}
	inner := strings.TrimSuffix(s, "```")
	nl := strings.IndexByte(inner, '\n')
	if nl < 0 {
		return "", false
	}
	return strings.TrimSpace(inner[nl+1:]), true
}

// Validate checks a plan document against the DAG invariants: version,
// bounds, well-formed unique node ids, titles, roles drawn from the allowed
// set (exact-token match — roles are open data, never substring-matched),
// dependencies that reference known nodes exactly once, and acyclicity.
// The first violation found is returned as a typed *PlanError.
func (p Plan) Validate(allowedRoles []string) error {
	if p.Version != PlanVersion {
		return &PlanError{Code: PlanBadVersion, Detail: fmt.Sprintf("version %d, want %d", p.Version, PlanVersion)}
	}
	if len(p.Nodes) == 0 {
		return &PlanError{Code: PlanNoNodes, Detail: "plan has no nodes"}
	}
	if len(p.Nodes) > MaxPlanNodes {
		return &PlanError{Code: PlanTooManyNodes, Detail: fmt.Sprintf("%d nodes exceeds limit %d", len(p.Nodes), MaxPlanNodes)}
	}
	roles := make(map[string]bool, len(allowedRoles))
	for _, r := range allowedRoles {
		roles[r] = true
	}

	ids := make(map[string]bool, len(p.Nodes))
	for _, n := range p.Nodes {
		if !nodeIDRE.MatchString(n.ID) {
			return &PlanError{Code: PlanBadNodeID, Detail: fmt.Sprintf("node id %q (want %s)", n.ID, nodeIDRE)}
		}
		if ids[n.ID] {
			return &PlanError{Code: PlanDuplicateID, Detail: fmt.Sprintf("node id %q appears twice", n.ID)}
		}
		ids[n.ID] = true
		if strings.TrimSpace(n.Title) == "" {
			return &PlanError{Code: PlanMissingTitle, Detail: fmt.Sprintf("node %q has no title", n.ID)}
		}
		if !roles[n.Role] {
			return &PlanError{Code: PlanUnknownRole, Detail: fmt.Sprintf("node %q role %q not in %v", n.ID, n.Role, allowedRoles)}
		}
	}
	for _, n := range p.Nodes {
		seen := make(map[string]bool, len(n.DependsOn))
		for _, dep := range n.DependsOn {
			if !ids[dep] {
				return &PlanError{Code: PlanUnknownDep, Detail: fmt.Sprintf("node %q depends on unknown node %q", n.ID, dep)}
			}
			if seen[dep] {
				return &PlanError{Code: PlanUnknownDep, Detail: fmt.Sprintf("node %q lists dependency %q twice", n.ID, dep)}
			}
			seen[dep] = true
		}
	}
	if cycle := findCycle(p.Nodes); len(cycle) > 0 {
		return &PlanError{Code: PlanCycle, Detail: "cycle through nodes " + strings.Join(cycle, ", ")}
	}
	return nil
}

// findCycle runs Kahn's algorithm and returns the node ids left undrained —
// the members of at least one cycle — sorted for deterministic errors. A
// self-dependency is a cycle of one.
func findCycle(nodes []Node) []string {
	indegree := make(map[string]int, len(nodes))
	dependents := make(map[string][]string, len(nodes))
	for _, n := range nodes {
		indegree[n.ID] += 0
		for _, dep := range n.DependsOn {
			indegree[n.ID]++
			dependents[dep] = append(dependents[dep], n.ID)
		}
	}
	queue := make([]string, 0, len(nodes))
	for id, d := range indegree {
		if d == 0 {
			queue = append(queue, id)
		}
	}
	drained := 0
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		drained++
		for _, next := range dependents[id] {
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
			}
		}
	}
	if drained == len(nodes) {
		return nil
	}
	var cycle []string
	for id, d := range indegree {
		if d > 0 {
			cycle = append(cycle, id)
		}
	}
	sort.Strings(cycle)
	return cycle
}

// FromPlan folds a validated plan into authoritative task Records for one
// job: a fresh UUIDv7 per node, node-id dependencies resolved to task ids,
// state TaskPending, a shared creation timestamp. The caller must have run
// Validate first; FromPlan assumes referential integrity.
func FromPlan(job string, p Plan, now time.Time) []Record {
	idFor := make(map[string]string, len(p.Nodes))
	for _, n := range p.Nodes {
		idFor[n.ID] = envelope.NewID()
	}
	recs := make([]Record, 0, len(p.Nodes))
	for _, n := range p.Nodes {
		deps := make([]string, 0, len(n.DependsOn))
		for _, dep := range n.DependsOn {
			deps = append(deps, idFor[dep])
		}
		if len(deps) == 0 {
			deps = nil
		}
		recs = append(recs, Record{
			ID:          idFor[n.ID],
			Job:         job,
			Node:        n.ID,
			Title:       strings.TrimSpace(n.Title),
			Description: strings.TrimSpace(n.Description),
			Role:        n.Role,
			DependsOn:   deps,
			Files:       n.Files,
			Acceptance:  n.Acceptance,
			State:       envelope.TaskPending,
			CreatedAt:   now,
		})
	}
	return recs
}
