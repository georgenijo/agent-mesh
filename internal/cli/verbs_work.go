package cli

// P3 verb: work. `mesh work` is the natural-language job control entry point.
// It interprets a phrase ("work on issue N", "issues N-M", "all issues"),
// resolves the referenced GitHub issues from the configured repo
// (MESH_GITHUB_REPO), and submits one job per issue via the existing submit
// path. The job/task contract is unchanged — only the entry surface differs.
//
// Usage:
//
//	mesh work "work on issue 42"
//	mesh work "issues 100-115"
//	mesh work "all issues"

import (
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"

	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/coordinator/nlparser"
	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
)

// ghListItem is one entry from `gh issue list --json number,title,body`.
type ghListItem struct {
	Number int    `json:"number"`
	Title  string `json:"title"`
	Body   string `json:"body"`
}

// ghIssueList lists open issues for ownerRepo. Replaceable in tests.
var ghIssueList = func(ownerRepo string) ([]ghListItem, error) {
	cmd := exec.Command("gh", "issue", "list",
		"--repo", ownerRepo,
		"--state", "open",
		"--json", "number,title,body",
		"--limit", "1000",
	)
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok && len(ee.Stderr) > 0 {
			return nil, fmt.Errorf("gh: %s", strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, err
	}
	var items []ghListItem
	if jerr := json.Unmarshal(out, &items); jerr != nil {
		return nil, fmt.Errorf("gh json: %w", jerr)
	}
	return items, nil
}

func runWork(args []string, stdout, stderr io.Writer) int {
	vs, code, err := setupVerb("work", args, stderr, nil)
	if err != nil {
		return emitSetupErr(stdout, stderr, vs.jsonOut, code, err)
	}
	if len(vs.positional) == 0 {
		fmt.Fprintln(stderr, "mesh work: phrase required")
		fmt.Fprintln(stderr, `  usage: mesh work "<phrase>"`)
		fmt.Fprintln(stderr, `  phrases: "work on issue N" | "issues N-M" | "all issues"`)
		return ExitUsage
	}

	// Join multiple positional tokens in case the user omitted quotes.
	phrase := strings.Join(vs.positional, " ")

	cfg, cerr := config.Load()
	if cerr != nil {
		return emitSetupErr(stdout, stderr, vs.jsonOut, ExitError, cerr)
	}
	if cfg.GitHubRepo == "" {
		err := fmt.Errorf("MESH_GITHUB_REPO is not set: configure the target GitHub repo (e.g. owner/repo)")
		return emitSetupErr(stdout, stderr, vs.jsonOut, ExitUsage, err)
	}

	parsed, perr := nlparser.Parse(phrase)
	if perr != nil {
		msg := fmt.Errorf("mesh work: %w", perr)
		return emitSetupErr(stdout, stderr, vs.jsonOut, ExitUsage, msg)
	}

	// Resolve issue data before submitting anything; fail cleanly if any
	// GitHub call fails so we never submit a partial set.
	type issueData struct {
		number int
		issue  ghIssue
	}

	var issues []issueData

	switch parsed.Kind {
	case nlparser.KindSingle:
		iss, ferr := ghIssueView(cfg.GitHubRepo, strconv.Itoa(parsed.From))
		if ferr != nil {
			msg := fmt.Errorf("github ingest unavailable: %v", ferr)
			return emitSetupErr(stdout, stderr, vs.jsonOut, ExitError, msg)
		}
		issues = []issueData{{number: parsed.From, issue: iss}}

	case nlparser.KindRange:
		for n := parsed.From; n <= parsed.To; n++ {
			iss, ferr := ghIssueView(cfg.GitHubRepo, strconv.Itoa(n))
			if ferr != nil {
				msg := fmt.Errorf("github ingest unavailable for issue %d: %v", n, ferr)
				return emitSetupErr(stdout, stderr, vs.jsonOut, ExitError, msg)
			}
			issues = append(issues, issueData{number: n, issue: iss})
		}

	case nlparser.KindAll:
		items, lerr := ghIssueList(cfg.GitHubRepo)
		if lerr != nil {
			msg := fmt.Errorf("github issue list unavailable: %v", lerr)
			return emitSetupErr(stdout, stderr, vs.jsonOut, ExitError, msg)
		}
		for _, it := range items {
			issues = append(issues, issueData{
				number: it.Number,
				issue:  ghIssue{Title: it.Title, Body: it.Body},
			})
		}
	}

	if len(issues) == 0 {
		if vs.jsonOut {
			b, _ := json.Marshal(map[string]any{"ok": true, "submitted": 0}) //nolint:errcheck
			fmt.Fprintln(stdout, string(b))
			return ExitOK
		}
		fmt.Fprintln(stdout, "no issues found; nothing submitted")
		return ExitOK
	}

	// Submit one job per issue via the existing submit path.
	type submitResult struct {
		number int
		result meshapi.SubmitResult
	}
	var results []submitResult

	for _, id := range issues {
		ref := fmt.Sprintf("%s#%d", cfg.GitHubRepo, id.number)
		title := strings.TrimSpace(id.issue.Title)
		if title == "" {
			title = ref
		}

		submitArgs := meshapi.SubmitArgs{
			Repo:      cfg.GitHubRepo,
			Source:    job.SourceGitHub,
			SourceRef: ref,
			Title:     title,
			Body:      id.issue.Body,
		}

		resp, scode, serr := doVerb(vs.socketPath, meshapi.VerbSubmit, submitArgs)
		if serr != nil {
			if vs.jsonOut {
				obj := map[string]any{"ok": false, "code": resp.Code, "message": serr.Error()}
				b, _ := json.Marshal(obj) //nolint:errcheck
				fmt.Fprintln(stdout, string(b))
				return scode
			}
			fmt.Fprintf(stderr, "mesh work: submit issue %d: %v\n", id.number, serr)
			return scode
		}
		var res meshapi.SubmitResult
		if jerr := json.Unmarshal(resp.Data, &res); jerr != nil {
			fmt.Fprintln(stderr, "mesh work: bad submit response:", jerr)
			return ExitError
		}
		results = append(results, submitResult{number: id.number, result: res})
	}

	if vs.jsonOut {
		type jsonItem struct {
			Issue int    `json:"issue"`
			Job   string `json:"job"`
			State string `json:"state"`
			Repo  string `json:"repo"`
		}
		items := make([]jsonItem, 0, len(results))
		for _, r := range results {
			items = append(items, jsonItem{
				Issue: r.number,
				Job:   r.result.Job,
				State: string(r.result.State),
				Repo:  r.result.Repo,
			})
		}
		b, _ := json.Marshal(map[string]any{"ok": true, "submitted": len(items), "jobs": items}) //nolint:errcheck
		fmt.Fprintln(stdout, string(b))
		return ExitOK
	}

	for _, r := range results {
		fmt.Fprintf(stdout, "submitted job %s for issue %d (%s)\n",
			r.result.Job, r.number, r.result.State)
	}
	return ExitOK
}
