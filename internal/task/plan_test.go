package task

import (
	"fmt"
	"testing"
	"time"

	"github.com/georgenijo/agent-mesh/internal/envelope"
)

var testRoles = []string{"builder", "reviewer"}

func validPlan() Plan {
	return Plan{
		Version: 1,
		Nodes: []Node{
			{ID: "design", Title: "sketch the API", Role: "builder"},
			{ID: "impl", Title: "implement it", Role: "builder", DependsOn: []string{"design"},
				Files: []string{"src/api.go"}, Acceptance: []string{"unit tests pass"}},
			{ID: "review", Title: "review the change", Role: "reviewer", DependsOn: []string{"impl"}},
		},
	}
}

func TestValidateAcceptsWellFormedPlan(t *testing.T) {
	if err := validPlan().Validate(testRoles); err != nil {
		t.Fatalf("valid plan rejected: %v", err)
	}
}

// TestValidateTypedErrors pins every typed validation failure the issue
// acceptance names (cycles, missing/duplicate node ids, unknown roles) plus
// the bounds and referential checks around them.
func TestValidateTypedErrors(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Plan)
		code   PlanErrorCode
	}{
		{"bad version", func(p *Plan) { p.Version = 2 }, PlanBadVersion},
		{"no nodes", func(p *Plan) { p.Nodes = nil }, PlanNoNodes},
		{"too many nodes", func(p *Plan) {
			p.Nodes = make([]Node, MaxPlanNodes+1)
			for i := range p.Nodes {
				p.Nodes[i] = Node{ID: nodeID(i), Title: "t", Role: "builder"}
			}
		}, PlanTooManyNodes},
		{"missing node id", func(p *Plan) { p.Nodes[0].ID = "" }, PlanBadNodeID},
		{"whitespace node id", func(p *Plan) { p.Nodes[0].ID = "a b" }, PlanBadNodeID},
		{"duplicate node id", func(p *Plan) { p.Nodes[1].ID = "design" }, PlanDuplicateID},
		{"missing title", func(p *Plan) { p.Nodes[2].Title = "  " }, PlanMissingTitle},
		{"unknown role", func(p *Plan) { p.Nodes[0].Role = "wizard" }, PlanUnknownRole},
		{"empty role", func(p *Plan) { p.Nodes[0].Role = "" }, PlanUnknownRole},
		{"role is not substring matched", func(p *Plan) { p.Nodes[0].Role = "build" }, PlanUnknownRole},
		{"unknown dep", func(p *Plan) { p.Nodes[1].DependsOn = []string{"nope"} }, PlanUnknownDep},
		{"duplicate dep", func(p *Plan) { p.Nodes[1].DependsOn = []string{"design", "design"} }, PlanUnknownDep},
		{"self dependency", func(p *Plan) { p.Nodes[0].DependsOn = []string{"design"} }, PlanCycle},
		{"two node cycle", func(p *Plan) { p.Nodes[0].DependsOn = []string{"impl"} }, PlanCycle},
		{"three node cycle", func(p *Plan) { p.Nodes[0].DependsOn = []string{"review"} }, PlanCycle},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := validPlan()
			tc.mutate(&p)
			err := p.Validate(testRoles)
			if err == nil {
				t.Fatal("invalid plan accepted")
			}
			if !IsPlanError(err, tc.code) {
				t.Fatalf("error = %v, want code %s", err, tc.code)
			}
		})
	}
}

func nodeID(i int) string { return fmt.Sprintf("n%d", i) }

func TestDecodePlanStrict(t *testing.T) {
	doc := `{"version":1,"nodes":[{"id":"a","title":"t","role":"builder"}]}`

	t.Run("bare JSON", func(t *testing.T) {
		p, err := DecodePlan(doc)
		if err != nil || len(p.Nodes) != 1 {
			t.Fatalf("p=%+v err=%v", p, err)
		}
	})
	t.Run("single fence stripped", func(t *testing.T) {
		p, err := DecodePlan("```json\n" + doc + "\n```")
		if err != nil || len(p.Nodes) != 1 {
			t.Fatalf("p=%+v err=%v", p, err)
		}
	})
	t.Run("unknown fields tolerated", func(t *testing.T) {
		if _, err := DecodePlan(`{"version":1,"future":true,"nodes":[{"id":"a","title":"t","role":"builder","novel":1}]}`); err != nil {
			t.Fatalf("unknown fields rejected: %v", err)
		}
	})
	for name, text := range map[string]string{
		"prose":              "Sure! Here is the plan you asked for.",
		"prose then JSON":    "Here you go:\n" + doc,
		"trailing garbage":   doc + " trailing",
		"two documents":      doc + "\n" + doc,
		"empty":              "",
		"fence without body": "``````",
	} {
		t.Run(name+" rejected", func(t *testing.T) {
			if _, err := DecodePlan(text); err == nil {
				t.Fatalf("malformed plan accepted: %q", text)
			}
		})
	}
}

func TestFromPlanResolvesDepsToTaskIDs(t *testing.T) {
	now := time.Date(2026, 6, 9, 0, 0, 0, 0, time.UTC)
	recs := FromPlan("job-1", validPlan(), now)
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
	byNode := map[string]Record{}
	for _, r := range recs {
		if r.Job != "job-1" || r.State != envelope.TaskPending || !r.CreatedAt.Equal(now) {
			t.Fatalf("bad record: %+v", r)
		}
		if r.ID == "" || r.ID == r.Node {
			t.Fatalf("task id not minted: %+v", r)
		}
		byNode[r.Node] = r
	}
	impl, review := byNode["impl"], byNode["review"]
	if len(impl.DependsOn) != 1 || impl.DependsOn[0] != byNode["design"].ID {
		t.Fatalf("impl deps = %v, want [%s]", impl.DependsOn, byNode["design"].ID)
	}
	if len(review.DependsOn) != 1 || review.DependsOn[0] != impl.ID {
		t.Fatalf("review deps = %v, want [%s]", review.DependsOn, impl.ID)
	}
}
