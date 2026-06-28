package cli

import (
	"errors"
	"strings"
	"testing"

	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/socket"
)

func TestBuildSubmitArgsManual(t *testing.T) {
	args, code, err := buildSubmitArgs([]string{"add an RRULE builder\nmore detail"}, "demo", "", "")
	if err != nil || code != socket.CodeOK {
		t.Fatalf("code=%q err=%v", code, err)
	}
	if args.Source != job.SourceManual {
		t.Errorf("source = %q, want manual", args.Source)
	}
	if args.Repo != "demo" {
		t.Errorf("repo = %q", args.Repo)
	}
	if args.Body != "add an RRULE builder\nmore detail" {
		t.Errorf("body = %q", args.Body)
	}
	// Title defaults to the first line of the body.
	if args.Title != "add an RRULE builder" {
		t.Errorf("title = %q, want first line", args.Title)
	}
}

func TestBuildSubmitArgsManualExplicitTitle(t *testing.T) {
	args, code, err := buildSubmitArgs([]string{"body text"}, "demo", "Custom Title", "")
	if err != nil || code != socket.CodeOK {
		t.Fatalf("code=%q err=%v", code, err)
	}
	if args.Title != "Custom Title" {
		t.Errorf("title = %q", args.Title)
	}
}

func TestBuildSubmitArgsTitleTruncated(t *testing.T) {
	long := strings.Repeat("x", 200)
	args, _, err := buildSubmitArgs([]string{long}, "demo", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len([]rune(args.Title)) != maxTitleFromBody {
		t.Errorf("title len = %d, want %d", len([]rune(args.Title)), maxTitleFromBody)
	}
}

func TestBuildSubmitArgsManualRequiresRepo(t *testing.T) {
	_, code, err := buildSubmitArgs([]string{"do X"}, "", "", "")
	if code != socket.CodeBadRequest || err == nil {
		t.Fatalf("code=%q err=%v, want bad_request", code, err)
	}
}

func TestBuildSubmitArgsExactlyOneForm(t *testing.T) {
	// Neither task nor issue.
	if _, code, _ := buildSubmitArgs(nil, "demo", "", ""); code != socket.CodeBadRequest {
		t.Errorf("empty: code = %q, want bad_request", code)
	}
	// Both task and issue.
	if _, code, _ := buildSubmitArgs([]string{"do X"}, "demo", "", "o/r#1"); code != socket.CodeBadRequest {
		t.Errorf("both: code = %q, want bad_request", code)
	}
}

func TestBuildSubmitArgsMalformedIssue(t *testing.T) {
	for _, ref := range []string{"justtext", "owner/repo", "owner/repo#", "owner#1", "o/r#abc"} {
		if _, code, _ := buildSubmitArgs(nil, "", "", ref); code != socket.CodeBadRequest {
			t.Errorf("%q: code = %q, want bad_request", ref, code)
		}
	}
}

func TestBuildSubmitArgsGitHubSuccess(t *testing.T) {
	orig := ghIssueView
	t.Cleanup(func() { ghIssueView = orig })
	ghIssueView = func(ownerRepo, number string) (ghIssue, error) {
		if ownerRepo != "octo/cat" || number != "42" {
			t.Fatalf("gh args = %q %q", ownerRepo, number)
		}
		return ghIssue{Title: "Fix the bug", Body: "details here", URL: "https://x/42"}, nil
	}
	args, code, err := buildSubmitArgs(nil, "", "", "octo/cat#42")
	if err != nil || code != socket.CodeOK {
		t.Fatalf("code=%q err=%v", code, err)
	}
	if args.Source != job.SourceGitHub {
		t.Errorf("source = %q, want github", args.Source)
	}
	if args.SourceRef != "octo/cat#42" {
		t.Errorf("sourceRef = %q", args.SourceRef)
	}
	// Repo is normalized to the last path segment (no slash).
	if args.Repo != "cat" {
		t.Errorf("repo = %q, want normalized last segment %q", args.Repo, "cat")
	}
	if args.Title != "Fix the bug" || args.Body != "details here" {
		t.Errorf("title/body = %q / %q", args.Title, args.Body)
	}
}

func TestBuildSubmitArgsGitHubURL(t *testing.T) {
	orig := ghIssueView
	t.Cleanup(func() { ghIssueView = orig })
	ghIssueView = func(ownerRepo, number string) (ghIssue, error) {
		if ownerRepo != "octo/cat" || number != "42" {
			t.Fatalf("gh args = %q %q", ownerRepo, number)
		}
		return ghIssue{Title: "URL issue", Body: "body"}, nil
	}
	args, code, err := buildSubmitArgs(nil, "", "", "https://github.com/octo/cat/issues/42")
	if err != nil || code != socket.CodeOK {
		t.Fatalf("code=%q err=%v", code, err)
	}
	if args.Source != job.SourceGitHub {
		t.Errorf("source = %q, want github", args.Source)
	}
	if args.SourceRef != "octo/cat#42" {
		t.Errorf("sourceRef = %q, want octo/cat#42", args.SourceRef)
	}
	if args.Repo != "cat" {
		t.Errorf("repo = %q, want cat", args.Repo)
	}
	if args.Title != "URL issue" {
		t.Errorf("title = %q", args.Title)
	}
}

func TestBuildSubmitArgsGitHubRepoOverride(t *testing.T) {
	orig := ghIssueView
	t.Cleanup(func() { ghIssueView = orig })
	ghIssueView = func(string, string) (ghIssue, error) {
		return ghIssue{Title: "T", Body: "B"}, nil
	}
	args, _, err := buildSubmitArgs(nil, "myrepo", "", "octo/cat#42")
	if err != nil {
		t.Fatal(err)
	}
	if args.Repo != "myrepo" {
		t.Errorf("repo = %q, want override", args.Repo)
	}
}

func TestBuildSubmitArgsGitHubUnavailable(t *testing.T) {
	orig := ghIssueView
	t.Cleanup(func() { ghIssueView = orig })
	ghIssueView = func(string, string) (ghIssue, error) {
		return ghIssue{}, errors.New("gh: not authenticated")
	}
	_, code, err := buildSubmitArgs(nil, "", "", "octo/cat#42")
	if code != socket.CodeUnavailable || err == nil {
		t.Fatalf("code=%q err=%v, want unavailable", code, err)
	}
	// The unavailable code must map to the existing socket-unavailable exit.
	if exitForCode(code) != ExitError {
		t.Errorf("exit for unavailable = %d, want %d", exitForCode(code), ExitError)
	}
}
