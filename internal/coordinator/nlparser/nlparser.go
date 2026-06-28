// Package nlparser parses natural-language and structured references into
// coordinator work items. It handles GitHub issue references in two forms:
//
//   - Full URL:  https://github.com/owner/repo/issues/42
//   - Short form: owner/repo#42
//
// The parsed IssueRef carries the split owner/repo/number so callers do not
// need to re-split. NormalizeRepo converts the "owner/repo" pair to its last
// path segment — the plain repo name — so job.Repo satisfies the
// envelope.ValidRepo constraint (no slashes, ≤48 chars).
package nlparser

import (
	"fmt"
	"path"
	"regexp"
	"strconv"
)

// shortRefRE matches the owner/repo#N short form.
var shortRefRE = regexp.MustCompile(`^([^/\s#]+)/([^/\s#]+)#(\d+)$`)

// urlRefRE matches https://github.com/owner/repo/issues/N (and optional trailing slash).
var urlRefRE = regexp.MustCompile(`^https://github\.com/([^/]+)/([^/]+)/issues/(\d+)/?$`)

// IssueRef is a parsed GitHub issue reference.
type IssueRef struct {
	Owner  string
	Repo   string
	Number int
}

// OwnerRepo returns "owner/repo".
func (r IssueRef) OwnerRepo() string { return r.Owner + "/" + r.Repo }

// ParseIssueRef parses a GitHub issue reference in either of the two supported
// forms. An unrecognised or malformed reference returns a descriptive error.
func ParseIssueRef(ref string) (IssueRef, error) {
	if m := urlRefRE.FindStringSubmatch(ref); m != nil {
		n, _ := strconv.Atoi(m[3])
		return IssueRef{Owner: m[1], Repo: m[2], Number: n}, nil
	}
	if m := shortRefRE.FindStringSubmatch(ref); m != nil {
		n, _ := strconv.Atoi(m[3])
		return IssueRef{Owner: m[1], Repo: m[2], Number: n}, nil
	}
	return IssueRef{}, fmt.Errorf("invalid issue reference %q (want https://github.com/owner/repo/issues/N or owner/repo#N)", ref)
}

// NormalizeRepo returns the last path segment of an "owner/repo" string so
// that it satisfies the envelope.ValidRepo constraint (no slashes). When ref
// is already a plain name (no slash) it is returned unchanged.
func NormalizeRepo(ownerRepo string) string {
	return path.Base(ownerRepo)
}
