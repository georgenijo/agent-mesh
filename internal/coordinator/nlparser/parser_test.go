package nlparser

import "testing"

func TestParseSingle(t *testing.T) {
	cases := []struct {
		phrase string
		want   int
	}{
		{"work on issue 42", 42},
		{"Work On Issue 42", 42},
		{"issue 1", 1},
		{"Issue 99", 99},
		{"work on issue #7", 7},
		{"issue #123", 123},
		{"  work on issue 10  ", 10},
	}
	for _, c := range cases {
		r, err := Parse(c.phrase)
		if err != nil {
			t.Errorf("Parse(%q) error = %v", c.phrase, err)
			continue
		}
		if r.Kind != KindSingle {
			t.Errorf("Parse(%q).Kind = %q, want single", c.phrase, r.Kind)
		}
		if r.From != c.want || r.To != c.want {
			t.Errorf("Parse(%q): From=%d To=%d, want %d", c.phrase, r.From, r.To, c.want)
		}
	}
}

func TestParseRange(t *testing.T) {
	cases := []struct {
		phrase   string
		wantFrom int
		wantTo   int
	}{
		{"issues 10-20", 10, 20},
		{"issues 100-115", 100, 115},
		{"Issues 1-1", 1, 1},
		{"issues 5 to 9", 5, 9},
		{"issues #10-#20", 10, 20},
	}
	for _, c := range cases {
		r, err := Parse(c.phrase)
		if err != nil {
			t.Errorf("Parse(%q) error = %v", c.phrase, err)
			continue
		}
		if r.Kind != KindRange {
			t.Errorf("Parse(%q).Kind = %q, want range", c.phrase, r.Kind)
		}
		if r.From != c.wantFrom || r.To != c.wantTo {
			t.Errorf("Parse(%q): From=%d To=%d, want From=%d To=%d",
				c.phrase, r.From, r.To, c.wantFrom, c.wantTo)
		}
	}
}

func TestParseAll(t *testing.T) {
	cases := []string{
		"all issues",
		"All Issues",
		"ALL ISSUES",
		"all issue",
	}
	for _, phrase := range cases {
		r, err := Parse(phrase)
		if err != nil {
			t.Errorf("Parse(%q) error = %v", phrase, err)
			continue
		}
		if r.Kind != KindAll {
			t.Errorf("Parse(%q).Kind = %q, want all", phrase, r.Kind)
		}
	}
}

func TestParseErrors(t *testing.T) {
	cases := []struct {
		phrase string
		desc   string
	}{
		{"", "empty phrase"},
		{"   ", "whitespace only"},
		{"issue 0", "zero is not a valid issue number"},
		{"issues 5-3", "inverted range"},
		{"do something", "unrecognised phrase"},
		{"issues abc-xyz", "non-numeric range"},
		{"work on it", "non-matching phrase"},
		{"fix issue 42", "unrecognised prefix"},
	}
	for _, c := range cases {
		_, err := Parse(c.phrase)
		if err == nil {
			t.Errorf("Parse(%q) (%s): want error, got nil", c.phrase, c.desc)
		}
	}
}
