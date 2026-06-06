package bus

import "testing"

func TestMatch(t *testing.T) {
	cases := []struct {
		pattern, subject string
		want             bool
	}{
		{"mesh.status.codex", "mesh.status.codex", true},
		{"mesh.status.codex", "mesh.status.claude", false},
		{"mesh.status.*", "mesh.status.codex", true},
		{"mesh.status.*", "mesh.status.a.b", false},
		{"mesh.*.codex", "mesh.status.codex", true},
		{"mesh.>", "mesh.status.codex", true},
		{"mesh.>", "mesh.register", true},
		{"mesh.>", "mesh", false},
		{"mesh.>", "other.status", false},
		{"mesh.heartbeat.>", "mesh.heartbeat.a", true},
		{"mesh.heartbeat.>", "mesh.heartbeat", false},
		{"", "mesh.x", false},
		{"mesh.x", "", false},
	}
	for _, tc := range cases {
		if got := Match(tc.pattern, tc.subject); got != tc.want {
			t.Errorf("Match(%q, %q) = %v, want %v", tc.pattern, tc.subject, got, tc.want)
		}
	}
}

func TestValidSubjectAndPattern(t *testing.T) {
	if ValidSubject("mesh.status.*") || ValidSubject("mesh.>") || ValidSubject("") || ValidSubject("a..b") {
		t.Error("wildcards/empties must not be publishable subjects")
	}
	if !ValidSubject("mesh.status.codex-7") {
		t.Error("plain subject should be valid")
	}
	if !ValidPattern("mesh.>") || !ValidPattern("mesh.status.*") || !ValidPattern("mesh.register") {
		t.Error("good patterns rejected")
	}
	if ValidPattern("mesh.>.x") || ValidPattern("") || ValidPattern("a..b") {
		t.Error("bad patterns accepted")
	}
}
