// Package agentcard defines the agent identity card published at join and
// surfaced by `mesh who`.
//
// Role and caps are open data — any string an agent registers — exact-token
// matched by consumers, never substring matched, and treated as claims to
// verify, not ground truth (see docs/decisions/DECISIONS.md: "One versioned
// envelope, one authority per fact, open-data roles/caps").
package agentcard

import (
	"fmt"
	"regexp"
	"strings"
)

// nameRE constrains agent ids/names: they become subject tokens
// (mesh.status.<id>) and socket filenames (<name>.sock), so dots, wildcards,
// slashes, and whitespace are forbidden.
var nameRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// ValidName reports whether s is a legal agent id/name.
func ValidName(s string) bool { return nameRE.MatchString(s) }

// Card is the capability advertisement an agent publishes on join.
type Card struct {
	ID    string   `json:"id"`   // unique within the mesh; P0: equals Name
	Name  string   `json:"name"` // human-friendly agent name
	Role  string   `json:"role"` // coarse role for role addressing (open data)
	Caps  []string `json:"caps,omitempty"`
	Repo  string   `json:"repo,omitempty"`
	CWD   string   `json:"cwd,omitempty"`
	Model string   `json:"model,omitempty"`
	PID   int      `json:"pid,omitempty"` // sidecar pid, diagnostics only
}

// Validate reports whether the card carries the minimum identity fields and
// that id/name are safe to embed in subjects and socket paths.
func (c Card) Validate() error {
	if !ValidName(c.ID) {
		return fmt.Errorf("agentcard: invalid id %q (want [A-Za-z0-9_-]{1,64})", c.ID)
	}
	if !ValidName(c.Name) {
		return fmt.Errorf("agentcard: invalid name %q (want [A-Za-z0-9_-]{1,64})", c.Name)
	}
	if strings.TrimSpace(c.Role) == "" {
		return fmt.Errorf("agentcard: missing role")
	}
	if !ValidName(c.Role) {
		return fmt.Errorf("agentcard: invalid role %q (want [A-Za-z0-9_-]{1,64})", c.Role)
	}
	for _, cap := range c.Caps {
		if strings.TrimSpace(cap) == "" {
			return fmt.Errorf("agentcard: empty capability token")
		}
	}
	return nil
}

// HasCap reports whether the card advertises the exact capability token.
// Exact set membership only — never substring matching (audit Avoid #7).
func (c Card) HasCap(token string) bool {
	for _, cap := range c.Caps {
		if cap == token {
			return true
		}
	}
	return false
}
