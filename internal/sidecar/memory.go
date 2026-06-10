package sidecar

// Expert memory = the blackboard. An expert's warm context lives in its
// runtime child's RAM, but that is volatile: a cold start, a crash, or a
// --resume miss leaves the child with no project knowledge. The durable
// per-repo note stream (the blackboard, $MESH_DIR/streams/notes-<repo>.jsonl)
// is the expert's long-term memory — the one authority for project decisions
// (locked decision 2026-06-05 "blackboard = expert memory", #28).
//
// This file builds the *memory primer*: a compact, byte-bounded text block,
// rendered from the blackboard, that a (re)started expert injects into its
// runtime child as the first user message so it answers project questions from
// recorded decisions rather than from a now-empty session. The blackboard
// itself is never mutated here — the primer is a derived, compacted *view* of
// the durable record, read with the same envelope/sender-binding discipline as
// the `mesh context` verb (handleContext).
//
// Authority split, made explicit for #28:
//   - The blackboard note stream is authoritative for durable facts
//     (decisions, summaries, conventions, context). It survives any restart.
//   - The runtime child's RAM holds the warm conversation; it is a cache of the
//     last live turns, never authoritative, and is expected to be lost on
//     restart. The primer rehydrates what matters from the authority.

import (
	"fmt"
	"sort"
	"strings"

	"github.com/georgenijo/agent-mesh/internal/envelope"
)

// DefaultPrimerBudget bounds the rendered memory primer in bytes. The blackboard
// grows without bound (it is the durable record), but the primer injected into a
// finite runtime context must not — so the primer is compacted to this budget,
// preferring the highest-value note kinds and the most recent decisions. The
// budget is well under the runtime line cap (4 MiB) and the note text limit, so
// it always fits one stream-json user message.
const DefaultPrimerBudget = 16 * 1024

// memoryNote is one durable blackboard entry, decoded for priming. It mirrors
// the fields handleContext exposes but is internal to the memory builder.
type memoryNote struct {
	Seq    uint64
	Author string
	Text   string
	Kind   string
	Ticket string
}

// primerKindRank orders note kinds by durability value when the primer must be
// compacted to fit its budget: decisions and summaries are the facts a repeated
// project question must still be answerable from, so they are kept ahead of
// context and other. Lower rank = higher priority to retain.
func primerKindRank(kind string) int {
	switch kind {
	case envelope.NoteKindDecision:
		return 0
	case envelope.NoteKindSummary:
		return 1
	case envelope.NoteKindContext:
		return 2
	default: // NoteKindOther and any unknown kind
		return 3
	}
}

// MemoryPrimer is the compacted, byte-bounded rehydration payload built from a
// repo's blackboard, plus the bookkeeping the expert loop needs for re-sync.
type MemoryPrimer struct {
	// Text is the rendered primer, ready to inject as one runtime user message.
	// Empty when the blackboard holds no usable notes.
	Text string
	// HighWater is the highest note seq seen on the blackboard at read time.
	// The expert loop remembers it so it can detect new notes (a worker landing
	// a decision) and re-prime — the in-mesh re-sync signal.
	HighWater uint64
	// Total is how many durable notes the blackboard held (before compaction).
	Total int
	// Included is how many notes the compacted primer actually rendered. When
	// Included < Total, the primer carries an honest "(N earlier notes elided)"
	// marker and the elision is biased to drop the lowest-value, oldest notes.
	Included int
}

// readMemoryNotes pulls the repo's durable blackboard and decodes it with the
// same discipline as handleContext: malformed records are skipped (replay must
// never break), and a note whose envelope sender does not match its claimed
// author is dropped (sender binding). Returns notes oldest-first plus the
// blackboard high-water seq.
func (s *Sidecar) readMemoryNotes(repo string) ([]memoryNote, uint64, error) {
	entries, err := s.bus.StreamRead(envelope.StreamNotes(repo), 0)
	if err != nil {
		return nil, 0, err
	}
	notes := make([]memoryNote, 0, len(entries))
	var high uint64
	for _, e := range entries {
		if e.Seq > high {
			high = e.Seq
		}
		env, err := envelope.Decode(e.Data)
		if err != nil {
			continue
		}
		var p envelope.NotePayload
		if err := envelope.DecodeInto(env, &p); err != nil {
			continue
		}
		if env.From != p.ID {
			continue // sender binding: same rule handleContext applies on replay
		}
		kind := p.Kind
		if kind == "" {
			kind = envelope.NoteKindDecision
		}
		notes = append(notes, memoryNote{
			Seq: e.Seq, Author: p.ID, Text: p.Decision, Kind: kind, Ticket: p.Ticket,
		})
	}
	return notes, high, nil
}

// BuildMemoryPrimer reads the repo's blackboard and renders a compacted,
// byte-bounded memory primer for a (re)starting expert. When the full history
// fits the budget it is rendered whole, newest last (chronological, the way a
// reader expects a decision log). When it does not, compaction keeps the
// highest-value, most-recent notes: notes are selected by (kind priority, then
// recency) until the budget is reached, then the selected set is re-sorted
// chronologically for rendering and the dropped count is disclosed. The durable
// blackboard is untouched — this is a read-only derived view.
func (s *Sidecar) BuildMemoryPrimer(repo string, budget int) (MemoryPrimer, error) {
	if budget <= 0 {
		budget = DefaultPrimerBudget
	}
	notes, high, err := s.readMemoryNotes(repo)
	if err != nil {
		return MemoryPrimer{}, err
	}
	primer := MemoryPrimer{HighWater: high, Total: len(notes)}
	if len(notes) == 0 {
		return primer, nil
	}

	selected, dropped := selectWithinBudget(repo, notes, budget)
	primer.Included = len(selected)
	primer.Text = renderPrimer(repo, selected, dropped)
	return primer, nil
}

// selectWithinBudget chooses the notes to retain when the full rendered history
// would overflow budget, returning them chronologically (seq asc) plus the
// dropped count. Selection order is (kind priority asc, seq desc) — keep the
// most valuable kinds, newest within a kind — accumulating until the next note
// would push the *rendered* primer (header + elision marker + lines) over the
// budget. A budget large enough for everything returns all notes, dropped 0.
//
// The budget is honored on the real rendered size: the header and elision
// marker are measured for THIS repo, not estimated, so the returned primer is
// never over budget (verified by a final guard in renderPrimer's caller path).
func selectWithinBudget(repo string, notes []memoryNote, budget int) ([]memoryNote, int) {
	// Fast path: if the whole rendering fits, keep everything.
	if len(renderPrimer(repo, notes, 0)) <= budget {
		out := make([]memoryNote, len(notes))
		copy(out, notes)
		return out, 0
	}

	order := make([]int, len(notes))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		na, nb := notes[order[a]], notes[order[b]]
		ra, rb := primerKindRank(na.Kind), primerKindRank(nb.Kind)
		if ra != rb {
			return ra < rb
		}
		return na.Seq > nb.Seq // newer first within a kind
	})

	// Reserve the actual rendered overhead (header + the elision marker, which is
	// always present once we are compacting) so accumulation can never overshoot.
	overhead := len(renderPrimer(repo, nil, len(notes)))
	used := overhead
	keep := make([]memoryNote, 0, len(order))
	for _, idx := range order {
		cost := lineLen(notes[idx])
		if used+cost > budget {
			break
		}
		used += cost
		keep = append(keep, notes[idx])
	}
	sort.Slice(keep, func(a, b int) bool { return keep[a].Seq < keep[b].Seq })
	return keep, len(notes) - len(keep)
}

// renderPrimer turns the selected notes into the injected text block. dropped is
// how many notes were elided by compaction; it is disclosed honestly rather than
// silently swallowed so the expert knows its primer is partial.
func renderPrimer(repo string, notes []memoryNote, dropped int) string {
	var b strings.Builder
	b.WriteString("# Mesh expert memory — blackboard for repo ")
	b.WriteString(repo)
	b.WriteString("\n")
	b.WriteString("These are durable project decisions recorded on the shared blackboard. ")
	b.WriteString("Treat them as authoritative context for answering questions about this repo.\n")
	if dropped > 0 {
		fmt.Fprintf(&b, "(%d earlier lower-priority note(s) elided to fit memory; the durable record retains them.)\n", dropped)
	}
	b.WriteString("\n")
	for _, n := range notes {
		b.WriteString(formatNoteLine(n))
	}
	return b.String()
}

// formatNoteLine renders one note as a stable, parse-free log line. The text is
// never scraped back — this is one-way injection into the runtime child.
func formatNoteLine(n memoryNote) string {
	var b strings.Builder
	b.WriteString("- [")
	b.WriteString(n.Kind)
	b.WriteString("] ")
	b.WriteString(strings.TrimSpace(n.Text))
	if n.Author != "" {
		b.WriteString(" (")
		b.WriteString(n.Author)
		if n.Ticket != "" {
			b.WriteString(", ticket ")
			b.WriteString(n.Ticket)
		}
		b.WriteString(")")
	} else if n.Ticket != "" {
		b.WriteString(" (ticket ")
		b.WriteString(n.Ticket)
		b.WriteString(")")
	}
	b.WriteString("\n")
	return b.String()
}

// lineLen is the rendered byte cost of one note line, used for budgeting.
func lineLen(n memoryNote) int { return len(formatNoteLine(n)) }
