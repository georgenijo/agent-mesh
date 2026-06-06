package sidecar

// P1 verb handlers: claim/release (CAS file-claims), announce (advisory
// pub/sub), note/context (durable blackboard). The sidecar is the publish
// edge: every envelope is built and validated here, sender-bound to this
// agent's id — the CLI never touches the bus, and a CLI cannot author a
// mutation for another agent because it can only reach its own sidecar.

import (
	"encoding/json"
	"fmt"

	"github.com/georgenijo/agent-mesh/internal/claim"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
	"github.com/georgenijo/agent-mesh/internal/socket"
)

// resolveRepo picks the repo identity for a P1 verb: explicit argument →
// card repo → DefaultRepo. An explicit or card-carried repo that is not a
// valid repo id is a loud typed error, never silently remapped — silent
// fallback would scatter one agent's claims across two identities.
func (s *Sidecar) resolveRepo(explicit string) (string, error) {
	if explicit != "" {
		if !envelope.ValidRepo(explicit) {
			return "", fmt.Errorf("invalid repo id %q (want [A-Za-z0-9_-]{1,48})", explicit)
		}
		return explicit, nil
	}
	s.mu.Lock()
	cardRepo := s.card.Repo
	s.mu.Unlock()
	if cardRepo != "" {
		if !envelope.ValidRepo(cardRepo) {
			return "", fmt.Errorf("agent card repo %q is not a valid repo id; pass --repo or re-join with a token repo", cardRepo)
		}
		return cardRepo, nil
	}
	return envelope.DefaultRepo, nil
}

// joinedID snapshots (id, joined) under the lock.
func (s *Sidecar) joinedID() (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.card.ID, s.joined
}

func (s *Sidecar) handleClaim(req socket.Request) socket.Response {
	var args meshapi.ClaimArgs
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("claim args: %v", err))
	}
	id, joined := s.joinedID()
	if !joined {
		return socket.Fail(socket.CodeNotJoined, "agent has not joined")
	}
	repo, err := s.resolveRepo(args.Repo)
	if err != nil {
		return socket.Fail(socket.CodeBadRequest, err.Error())
	}
	norm, err := claim.NormalizePath(args.Path)
	if err != nil {
		return socket.Fail(socket.CodeBadRequest, err.Error())
	}

	out := claim.Take(s.bus, id, repo, norm, s.cfg.ClaimTTL)
	if out.Result == envelope.ClaimError {
		return socket.Fail(socket.CodeUnavailable, fmt.Sprintf("claim store: %v", out.Err))
	}
	return socket.OKData(meshapi.ClaimVerbResult{
		Result: out.Result,
		Path:   norm,
		Repo:   repo,
		Owner:  out.Owner.Agent,
		Since:  out.Owner.TS,
	})
}

func (s *Sidecar) handleRelease(req socket.Request) socket.Response {
	var args meshapi.ReleaseArgs
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("release args: %v", err))
	}
	id, joined := s.joinedID()
	if !joined {
		return socket.Fail(socket.CodeNotJoined, "agent has not joined")
	}
	repo, err := s.resolveRepo(args.Repo)
	if err != nil {
		return socket.Fail(socket.CodeBadRequest, err.Error())
	}
	norm, err := claim.NormalizePath(args.Path)
	if err != nil {
		return socket.Fail(socket.CodeBadRequest, err.Error())
	}

	out := claim.Release(s.bus, id, repo, norm)
	if out.Result == claim.ReleaseError {
		return socket.Fail(socket.CodeUnavailable, fmt.Sprintf("release store: %v", out.Err))
	}
	return socket.OKData(meshapi.ReleaseVerbResult{
		Result: meshapi.ReleaseResultKind(out.Result),
		Path:   norm,
		Repo:   repo,
		Owner:  out.Owner.Agent,
	})
}

func (s *Sidecar) handleAnnounce(req socket.Request) socket.Response {
	var args meshapi.AnnounceArgs
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("announce args: %v", err))
	}
	if args.Intent == "" {
		return socket.Fail(socket.CodeBadRequest, "empty announce intent")
	}
	if len(args.Intent) > meshapi.MaxIntentLen {
		return socket.Fail(socket.CodeBadRequest,
			fmt.Sprintf("intent %d bytes exceeds limit %d", len(args.Intent), meshapi.MaxIntentLen))
	}
	id, joined := s.joinedID()
	if !joined {
		return socket.Fail(socket.CodeNotJoined, "agent has not joined")
	}
	repo, err := s.resolveRepo(args.Repo)
	if err != nil {
		return socket.Fail(socket.CodeBadRequest, err.Error())
	}

	// Paths in an announce are advisory context, normalized for display
	// consistency but never validated as claims — announce must stay cheap
	// and unconditional (fire-and-forget, no lock semantics).
	paths := make([]string, 0, len(args.Paths))
	for _, p := range args.Paths {
		if n, err := claim.NormalizePath(p); err == nil {
			paths = append(paths, n)
		} else {
			paths = append(paths, p)
		}
	}
	if len(paths) == 0 {
		paths = nil
	}

	env, err := envelope.New(envelope.KindAnnounce, id, envelope.SubjectAnnounce(repo),
		&envelope.AnnouncePayload{ID: id, Intent: args.Intent, Paths: paths, Repo: repo})
	if err != nil {
		return socket.Fail(socket.CodeInternal, err.Error())
	}
	if err := s.bus.Publish(env); err != nil {
		return socket.Fail(socket.CodeUnavailable, fmt.Sprintf("bus publish: %v", err))
	}
	return socket.OKData(meshapi.AnnounceResult{ID: id, Repo: repo, Intent: args.Intent, Paths: paths})
}

func (s *Sidecar) handleNote(req socket.Request) socket.Response {
	var args meshapi.NoteArgs
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("note args: %v", err))
	}
	if args.Text == "" {
		return socket.Fail(socket.CodeBadRequest, "empty note text")
	}
	if len(args.Text) > meshapi.MaxNoteLen {
		return socket.Fail(socket.CodeBadRequest,
			fmt.Sprintf("note %d bytes exceeds limit %d", len(args.Text), meshapi.MaxNoteLen))
	}
	kind := args.Kind
	if kind == "" {
		kind = envelope.NoteKindDecision
	}
	if !envelope.ValidNoteKind(kind) {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("unknown note kind %q", args.Kind))
	}
	id, joined := s.joinedID()
	if !joined {
		return socket.Fail(socket.CodeNotJoined, "agent has not joined")
	}
	repo, err := s.resolveRepo(args.Repo)
	if err != nil {
		return socket.Fail(socket.CodeBadRequest, err.Error())
	}

	// The stream entry is a full validated envelope, not a bare struct: the
	// blackboard is replayed by late joiners, so its records must carry the
	// same versioned wire contract as everything else.
	env, err := envelope.New(envelope.KindNote, id, envelope.SubjectNote(repo),
		&envelope.NotePayload{ID: id, Decision: args.Text, Repo: repo, Kind: kind, Ticket: args.Ticket})
	if err != nil {
		return socket.Fail(socket.CodeInternal, err.Error())
	}
	raw, err := envelope.Encode(env)
	if err != nil {
		return socket.Fail(socket.CodeInternal, err.Error())
	}
	seq, err := s.bus.StreamAppend(envelope.StreamNotes(repo), json.RawMessage(raw))
	if err != nil {
		return socket.Fail(socket.CodeUnavailable, fmt.Sprintf("blackboard append: %v", err))
	}
	return socket.OKData(meshapi.NoteResult{Seq: seq, Repo: repo})
}

func (s *Sidecar) handleContext(req socket.Request) socket.Response {
	var args meshapi.ContextArgs
	if len(req.Args) > 0 {
		if err := json.Unmarshal(req.Args, &args); err != nil {
			return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("context args: %v", err))
		}
	}
	_, joined := s.joinedID()
	if !joined {
		return socket.Fail(socket.CodeNotJoined, "agent has not joined")
	}
	repo, err := s.resolveRepo(args.Repo)
	if err != nil {
		return socket.Fail(socket.CodeBadRequest, err.Error())
	}

	entries, err := s.bus.StreamRead(envelope.StreamNotes(repo), 0)
	if err != nil {
		return socket.Fail(socket.CodeUnavailable, fmt.Sprintf("blackboard read: %v", err))
	}
	notes := make([]meshapi.ContextNote, 0, len(entries))
	for _, e := range entries {
		env, err := envelope.Decode(e.Data)
		if err != nil {
			continue // malformed record must never break replay
		}
		var p envelope.NotePayload
		if err := envelope.DecodeInto(env, &p); err != nil {
			continue
		}
		// Sender binding on replay: a note speaking for an author other
		// than its envelope sender is dropped, same rule the coordinator
		// applies to presence mutations.
		if env.From != p.ID {
			continue
		}
		kind := p.Kind
		if kind == "" {
			kind = envelope.NoteKindDecision
		}
		notes = append(notes, meshapi.ContextNote{
			Seq:    e.Seq,
			TS:     env.TS,
			Author: p.ID,
			Text:   p.Decision,
			Kind:   kind,
			Ticket: p.Ticket,
		})
	}
	return socket.OKData(meshapi.ContextResult{Repo: repo, Notes: notes})
}
