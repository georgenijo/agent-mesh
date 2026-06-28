// Package nlparser interprets natural-language job control phrases and
// returns a typed result the caller uses to drive job submissions. Three
// phrasings are recognised:
//
//   - "work on issue N"  or "issue N"  → KindSingle, issue N
//   - "issues N-M"                     → KindRange, inclusive range N..M
//   - "all issues"                     → KindAll (caller resolves the list)
//
// The parser is pure (no I/O) so it is fully unit-testable without a GitHub
// connection. Resolution of "all issues" and per-issue metadata lookup are
// left to the caller (typically the `mesh work` verb in internal/cli).
package nlparser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Kind describes the type of job control request the phrase represents.
type Kind string

const (
	KindSingle Kind = "single" // one explicit issue number
	KindRange  Kind = "range"  // inclusive range of issue numbers
	KindAll    Kind = "all"    // all open issues (caller resolves)
)

// Result carries the parsed intent from a natural-language phrase.
type Result struct {
	Kind Kind
	From int // issue number for single; range start for range; 0 for all
	To   int // same as From for single; range end for range; 0 for all
}

var (
	// reSingle matches "work on issue N", "issue N", "work on issue #N", "issue #N"
	reSingle = regexp.MustCompile(`(?i)^(?:work\s+on\s+)?issue\s+#?(\d+)$`)

	// reRange matches "issues N-M", "issues N–M", "issues N to M"
	reRange = regexp.MustCompile(`(?i)^issues?\s+#?(\d+)\s*(?:[-–]|to)\s*#?(\d+)$`)

	// reAll matches "all issues" or "all issue"
	reAll = regexp.MustCompile(`(?i)^all\s+issues?$`)
)

// Parse interprets phrase and returns the parsed Result. phrase is trimmed
// before matching. Returns a descriptive error for empty, unparseable, or
// logically invalid input (e.g. non-positive numbers, inverted range).
func Parse(phrase string) (Result, error) {
	phrase = strings.TrimSpace(phrase)
	if phrase == "" {
		return Result{}, fmt.Errorf(`empty phrase: want "work on issue N", "issues N-M", or "all issues"`)
	}

	if m := reSingle.FindStringSubmatch(phrase); m != nil {
		n, err := strconv.Atoi(m[1])
		if err != nil || n <= 0 {
			return Result{}, fmt.Errorf("issue number %q must be a positive integer", m[1])
		}
		return Result{Kind: KindSingle, From: n, To: n}, nil
	}

	if m := reRange.FindStringSubmatch(phrase); m != nil {
		from, err1 := strconv.Atoi(m[1])
		to, err2 := strconv.Atoi(m[2])
		if err1 != nil || from <= 0 || err2 != nil || to <= 0 {
			return Result{}, fmt.Errorf("issue range: both numbers must be positive integers")
		}
		if from > to {
			return Result{}, fmt.Errorf("issue range %d–%d: start must be ≤ end", from, to)
		}
		return Result{Kind: KindRange, From: from, To: to}, nil
	}

	if reAll.MatchString(phrase) {
		return Result{Kind: KindAll}, nil
	}

	return Result{}, fmt.Errorf("unrecognised phrase %q: want \"work on issue N\", \"issues N-M\", or \"all issues\"", phrase)
}
