package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/config"
	"github.com/georgenijo/agent-mesh/internal/envelope"
)

type demoEvent struct {
	kind    envelope.Kind
	from    string
	to      string
	subject string
	payload any
}

func main() {
	meshDir := flag.String("mesh-dir", "", "mesh directory containing bus.sock")
	delay := flag.Duration("delay", 750*time.Millisecond, "delay between published demo events")
	flag.Parse()

	if *meshDir != "" {
		os.Setenv(config.EnvMeshDir, *meshDir)
	}

	cfg, err := config.Load()
	if err != nil {
		fatal(err)
	}

	client, err := bus.Dial(cfg.BusSocket(), bus.ClientOptions{})
	if err != nil {
		fatal(err)
	}
	defer client.Close()

	events := fullTicketDay()
	for i, event := range events {
		env, err := envelope.New(event.kind, event.from, event.subject, event.payload)
		if err != nil {
			fatal(err)
		}
		env.To = event.to
		if err := client.Publish(env); err != nil {
			fatal(err)
		}
		fmt.Printf("%02d %s %s\n", i+1, event.kind, event.subject)
		time.Sleep(*delay)
	}
}

func fullTicketDay() []demoEvent {
	return []demoEvent{
		note("observer-one", "Fake ticket scenario started: workers claim AM-201..AM-204, ask clarifying questions, and route expert asks through expert-runtime."),
		status("observer-one", "seeding fake tickets and watching the full ask/answer loop"),

		announce("builder-one", "picking up AM-201 runtime proxy crash drill", []string{"AM-201 Runtime proxy crash drill"}),
		claim("builder-one", "AM-201 Runtime proxy crash drill"),
		status("builder-one", "AM-201 picked up: reproducing resident-process crash evidence"),

		announce("designer-one", "picking up AM-202 ticket/question visual flow", []string{"AM-202 Ticket/question visual flow"}),
		claim("designer-one", "AM-202 Ticket/question visual flow"),
		status("designer-one", "AM-202 picked up: mapping ticket cards, prompt panes, and transcript"),

		announce("reviewer-one", "picking up AM-203 prompt redaction review", []string{"AM-203 Prompt redaction review"}),
		claim("reviewer-one", "AM-203 Prompt redaction review"),
		status("reviewer-one", "AM-203 picked up: checking prompt visibility and local/demo labeling"),

		announce("observer-one", "picking up AM-204 transcript proof", []string{"AM-204 Transcript proof"}),
		claim("observer-one", "AM-204 Transcript proof"),
		status("observer-one", "AM-204 picked up: comparing pretty panels against raw bus envelopes"),

		ask("builder-one", "expert-runtime", "expert", "AM-201-q1", "When the child process dies, what exact crash fields should the dashboard surface so the spike proves typed crash detection?", "AM-201 needs runtime guidance"),
		status("builder-one", "AM-201 blocked on expert crash-field guidance"),
		ask("designer-one", "expert-runtime", "expert", "AM-202-q1", "Should the Questions panel group by worker ticket, by ask thread, or by expert route?", "AM-202 needs UX guidance"),
		status("designer-one", "AM-202 waiting on expert grouping guidance"),
		ask("reviewer-one", "expert-runtime", "expert", "AM-203-q1", "Can the prompt panel show raw runtime prompts by default, or should the redaction toggle start enabled?", "AM-203 needs safety guidance"),
		status("reviewer-one", "AM-203 waiting on expert redaction guidance"),

		ask("observer-one", "builder-one", "builder", "AM-204-q1", "For transcript proof, do you want heartbeats hidden by default so the asks and answers stay readable?", "AM-204 asks worker for clarification"),
		answer("builder-one", "observer-one", "AM-204-q1", "Yes. Hide heartbeats by default and keep a settings toggle so the raw stream can still be inspected."),
		status("observer-one", "AM-204 clarified: heartbeats hidden by default, toggle remains available"),

		status("expert-runtime", "triaging expert inbox: AM-201, AM-202, AM-203"),
		answer("expert-runtime", "builder-one", "AM-201-q1", "Show child pid, exit signal or exit code, crash kind, timestamp, session id, and whether the supervisor killed or observed the process. Keep it typed, not prose-only."),
		status("builder-one", "AM-201 unblocked: adding crash summary and session proof"),
		answer("expert-runtime", "designer-one", "AM-202-q1", "Group by worker ticket first, then show each ask thread inside it. The route matters, but ownership matters more for scanability."),
		status("designer-one", "AM-202 unblocked: grouping by ticket owner with nested expert threads"),
		answer("expert-runtime", "reviewer-one", "AM-203-q1", "Default to visible demo prompt profiles in the spike, but label them local demo data. Real runtime prompts should honor redaction before P3 ships."),
		status("reviewer-one", "AM-203 unblocked: prompt visibility marked as demo-only"),

		ask("reviewer-one", "designer-one", "designer", "AM-203-q2", "Can the redaction setting stay visible even when prompts are hidden, so reviewers can tell why prompt text disappeared?", "AM-203 asks designer for UI clarification"),
		answer("designer-one", "reviewer-one", "AM-203-q2", "Yes. Keep the setting visible and change the prompt body to [redacted] instead of removing the panel."),
		status("reviewer-one", "AM-203 clarified: redaction keeps the panel, replaces body text"),

		ask("expert-runtime", "reviewer-one", "reviewer", "AM-203-q3", "Should expert answers also be hidden when prompt redaction is enabled?", "expert asks reviewer for policy clarification"),
		answer("reviewer-one", "expert-runtime", "AM-203-q3", "No. Redact prompts and hidden context, not answer text. Answers are part of the collaboration transcript."),
		status("expert-runtime", "expert guidance complete for AM-201, AM-202, AM-203"),

		note("observer-one", "AM-201 owned by builder-one, AM-202 by designer-one, AM-203 by reviewer-one, AM-204 by observer-one; expert-runtime answered three worker questions and asked one policy clarification back."),
		status("observer-one", "full fake-ticket scenario complete: claims, clarifications, expert answers, and transcript evidence are visible"),
	}
}

func status(from, text string) demoEvent {
	return demoEvent{
		kind:    envelope.KindStatus,
		from:    from,
		subject: envelope.SubjectStatus(from),
		payload: &envelope.StatusPayload{ID: from, Text: text},
	}
}

func announce(from, intent string, paths []string) demoEvent {
	return demoEvent{
		kind:    envelope.KindAnnounce,
		from:    from,
		subject: "mesh.announce." + from,
		payload: &envelope.AnnouncePayload{
			ID:     from,
			Intent: intent,
			Paths:  paths,
			Repo:   "agent-mesh",
		},
	}
}

func claim(from, ticket string) demoEvent {
	return demoEvent{
		kind:    envelope.KindClaim,
		from:    from,
		subject: "mesh.claim." + from,
		payload: &envelope.ClaimPayload{
			ID:     from,
			Path:   ticket,
			Repo:   "agent-mesh",
			Result: envelope.ClaimClaimed,
		},
	}
}

func ask(from, to, role, ticket, question, context string) demoEvent {
	return demoEvent{
		kind:    envelope.KindAsk,
		from:    from,
		to:      to,
		subject: "mesh.ask." + ticket,
		payload: &envelope.AskPayload{
			Ticket: ticket,
			To:     to,
			Role:   role,
			Q:      question,
			Ctx:    context,
		},
	}
}

func answer(from, to, ticket, text string) demoEvent {
	return demoEvent{
		kind:    envelope.KindAnswer,
		from:    from,
		to:      to,
		subject: "mesh.answer." + ticket,
		payload: &envelope.AnswerPayload{
			Ticket: ticket,
			Answer: text,
		},
	}
}

func note(from, decision string) demoEvent {
	return demoEvent{
		kind:    envelope.KindNote,
		from:    from,
		subject: "mesh.note.agent-mesh",
		payload: &envelope.NotePayload{
			Decision: decision,
			Repo:     "agent-mesh",
		},
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
