package cli

// P3 verb: submit. `mesh submit` is the autonomous entry point — it records a
// top-level Job in the coordinator's jobs KV and returns immediately. Same
// thin-client discipline as the rest: shape the args, one socket request, one
// printed reply, a meaningful exit code. Two forms:
//
//	mesh submit "<task>" --repo R [--title T]      (manual)
//	mesh submit --issue owner/repo#N [--repo R]    (GitHub ingest via `gh`)
//
// The GitHub form shells out to `gh issue view`; gh missing/unauthenticated is
// a typed unavailable error (reusing the socket-unavailable exit mapping), and
// a malformed --issue ref is a usage error.

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strings"

	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
	"github.com/georgenijo/agent-mesh/internal/socket"
)

// issueRefRE matches an --issue reference: owner/repo#N.
var issueRefRE = regexp.MustCompile(`^([^/\s#]+/[^/\s#]+)#(\d+)$`)

// maxTitleFromBody is the first-line title truncation length (~80 chars).
const maxTitleFromBody = 80

func runSubmit(args []string, stdout, stderr io.Writer) int {
	var repo, title, issue string
	vs, code, err := setupVerb("submit", args, stderr, func(fs *flag.FlagSet) {
		fs.StringVar(&repo, "repo", "", "repo id (required for the manual form)")
		fs.StringVar(&title, "title", "", "job title (default: first line of the task)")
		fs.StringVar(&issue, "issue", "", "ingest a GitHub issue: owner/repo#N")
	})
	if err != nil {
		fmt.Fprintln(stderr, "mesh:", err)
		return code
	}

	submitArgs, codeStr, err := buildSubmitArgs(vs.positional, repo, title, issue)
	if err != nil {
		exit := exitForCode(codeStr)
		if vs.jsonOut {
			obj := map[string]any{"ok": false, "code": codeStr, "message": err.Error()}
			b, _ := json.Marshal(obj) //nolint:errcheck
			fmt.Fprintln(stdout, string(b))
			return exit
		}
		fmt.Fprintln(stderr, "mesh:", err)
		return exit
	}

	resp, code, err := doVerb(vs.socketPath, meshapi.VerbSubmit, submitArgs)
	if err != nil {
		return emit(stdout, stderr, vs.jsonOut, resp, code, err, nil)
	}
	var res meshapi.SubmitResult
	if jerr := json.Unmarshal(resp.Data, &res); jerr != nil {
		fmt.Fprintln(stderr, "mesh: bad submit response:", jerr)
		return ExitError
	}
	if vs.jsonOut {
		fmt.Fprintln(stdout, string(resp.Data))
		return ExitOK
	}
	fmt.Fprintf(stdout, "submitted job %s (%s)\n", res.Job, res.State)
	return ExitOK
}

// buildSubmitArgs resolves the two surfaces into SubmitArgs. Exactly one of a
// positional task or --issue must be present. On failure it returns the typed
// socket error code (CodeBadRequest for usage, CodeUnavailable for gh) so both
// the exit code and the --json error object stay truthful.
func buildSubmitArgs(positional []string, repo, title, issue string) (meshapi.SubmitArgs, string, error) {
	hasTask := len(positional) == 1 && strings.TrimSpace(positional[0]) != ""
	if len(positional) > 1 {
		return meshapi.SubmitArgs{}, socket.CodeBadRequest, fmt.Errorf(`usage: mesh submit "<task>" --repo R  |  mesh submit --issue owner/repo#N`)
	}
	if hasTask == (issue != "") {
		return meshapi.SubmitArgs{}, socket.CodeBadRequest, fmt.Errorf(`exactly one of a task description or --issue is required`)
	}

	if issue != "" {
		return githubSubmitArgs(issue, repo, title)
	}

	// Manual form.
	body := strings.TrimSpace(positional[0])
	if title == "" {
		title = firstLine(body, maxTitleFromBody)
	}
	if repo == "" {
		return meshapi.SubmitArgs{}, socket.CodeBadRequest, fmt.Errorf("--repo is required")
	}
	return meshapi.SubmitArgs{
		Repo: repo, Source: job.SourceManual, Title: title, Body: body,
	}, socket.CodeOK, nil
}

// githubSubmitArgs ingests a GitHub issue via `gh issue view`. Repo defaults to
// the issue's owner/repo when --repo is not given.
func githubSubmitArgs(issue, repo, title string) (meshapi.SubmitArgs, string, error) {
	m := issueRefRE.FindStringSubmatch(issue)
	if m == nil {
		return meshapi.SubmitArgs{}, socket.CodeBadRequest, fmt.Errorf("invalid --issue %q (want owner/repo#N)", issue)
	}
	ownerRepo, number := m[1], m[2]

	out, err := ghIssueView(ownerRepo, number)
	if err != nil {
		// gh missing or unauthenticated: typed unavailable (reuses the
		// socket-unavailable exit mapping — exit 1, typed in --json).
		return meshapi.SubmitArgs{}, socket.CodeUnavailable, fmt.Errorf("github ingest unavailable: %v", err)
	}

	if title == "" {
		title = strings.TrimSpace(out.Title)
	}
	if title == "" {
		title = issue
	}
	if repo == "" {
		repo = ownerRepo
	}
	return meshapi.SubmitArgs{
		Repo: repo, Source: job.SourceGitHub, SourceRef: issue,
		Title: title, Body: out.Body,
	}, socket.CodeOK, nil
}

// ghIssue is the slice of `gh issue view --json` we consume.
type ghIssue struct {
	Title string `json:"title"`
	Body  string `json:"body"`
	URL   string `json:"url"`
}

// ghIssueView shells `gh issue view` and parses the JSON. Replaceable in tests.
var ghIssueView = func(ownerRepo, number string) (ghIssue, error) {
	cmd := exec.Command("gh", "issue", "view", number, "--repo", ownerRepo, "--json", "title,body,url")
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok && len(ee.Stderr) > 0 {
			return ghIssue{}, fmt.Errorf("gh: %s", strings.TrimSpace(string(ee.Stderr)))
		}
		return ghIssue{}, err
	}
	var iss ghIssue
	if jerr := json.Unmarshal(out, &iss); jerr != nil {
		return ghIssue{}, fmt.Errorf("gh json: %w", jerr)
	}
	return iss, nil
}

// firstLine returns the first line of s, truncated to max runes.
func firstLine(s string, max int) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	s = strings.TrimSpace(s)
	r := []rune(s)
	if len(r) > max {
		return string(r[:max])
	}
	return s
}
