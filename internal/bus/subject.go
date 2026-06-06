package bus

import "strings"

// Match reports whether a subject matches a subscription pattern.
//
// NATS-style semantics: subjects are dot-separated tokens; `*` matches exactly
// one token; `>` matches one or more trailing tokens and must be the last
// pattern token. Examples:
//
//	mesh.status.*  matches mesh.status.codex   (not mesh.status.a.b)
//	mesh.>         matches mesh.status.codex   (not "mesh")
func Match(pattern, subject string) bool {
	if pattern == "" || subject == "" {
		return false
	}
	pt := strings.Split(pattern, ".")
	st := strings.Split(subject, ".")

	for i, p := range pt {
		if p == ">" {
			// ">" must be last and match at least one remaining token.
			return i == len(pt)-1 && len(st) > i
		}
		if i >= len(st) {
			return false
		}
		if p != "*" && p != st[i] {
			return false
		}
	}
	return len(st) == len(pt)
}

// ValidSubject reports whether s is a publishable subject: non-empty
// dot-separated tokens with no wildcards.
func ValidSubject(s string) bool {
	if s == "" {
		return false
	}
	for _, tok := range strings.Split(s, ".") {
		if tok == "" || tok == "*" || tok == ">" {
			return false
		}
	}
	return true
}

// maxStoreNames caps the number of distinct KV buckets and streams a server
// will lazily create — names are peer-supplied, so an uncapped map would
// grow without bound.
const maxStoreNames = 64

// validStoreName constrains KV bucket and stream names to safe tokens.
func validStoreName(s string) bool {
	if s == "" || len(s) > 64 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return false
		}
	}
	return true
}

// ValidPattern reports whether p is a subscribable pattern: non-empty tokens,
// `>` only in last position.
func ValidPattern(p string) bool {
	if p == "" {
		return false
	}
	toks := strings.Split(p, ".")
	for i, tok := range toks {
		if tok == "" {
			return false
		}
		if tok == ">" && i != len(toks)-1 {
			return false
		}
	}
	return true
}
