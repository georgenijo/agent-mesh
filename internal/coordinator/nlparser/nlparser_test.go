package nlparser

import "testing"

func TestParseIssueRefShortForm(t *testing.T) {
	ref, err := ParseIssueRef("octo/cat#42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ref.Owner != "octo" || ref.Repo != "cat" || ref.Number != 42 {
		t.Errorf("got owner=%q repo=%q number=%d", ref.Owner, ref.Repo, ref.Number)
	}
	if ref.OwnerRepo() != "octo/cat" {
		t.Errorf("OwnerRepo() = %q, want octo/cat", ref.OwnerRepo())
	}
}

func TestParseIssueRefURL(t *testing.T) {
	cases := []struct {
		url    string
		owner  string
		repo   string
		number int
	}{
		{"https://github.com/octo/cat/issues/42", "octo", "cat", 42},
		{"https://github.com/octo/cat/issues/42/", "octo", "cat", 42},
		{"https://github.com/georgenijo/agent-mesh/issues/110", "georgenijo", "agent-mesh", 110},
	}
	for _, c := range cases {
		ref, err := ParseIssueRef(c.url)
		if err != nil {
			t.Errorf("%q: unexpected error: %v", c.url, err)
			continue
		}
		if ref.Owner != c.owner || ref.Repo != c.repo || ref.Number != c.number {
			t.Errorf("%q: got owner=%q repo=%q number=%d", c.url, ref.Owner, ref.Repo, ref.Number)
		}
	}
}

func TestParseIssueRefInvalid(t *testing.T) {
	invalid := []string{
		"",
		"justtext",
		"owner/repo",
		"owner/repo#",
		"owner#42",
		"o/r#abc",
		"http://github.com/o/r/issues/1",
		"https://github.com/o/r/pull/1",
		"https://github.com/o/r/issues/abc",
	}
	for _, ref := range invalid {
		if _, err := ParseIssueRef(ref); err == nil {
			t.Errorf("ParseIssueRef(%q): want error, got nil", ref)
		}
	}
}

func TestNormalizeRepo(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"octo/cat", "cat"},
		{"agent-mesh", "agent-mesh"},
		{"georgenijo/agent-mesh", "agent-mesh"},
		{"a/b/c", "c"},
	}
	for _, c := range cases {
		got := NormalizeRepo(c.in)
		if got != c.want {
			t.Errorf("NormalizeRepo(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
