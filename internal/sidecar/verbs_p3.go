package sidecar

// P3 verbs: ticket intake. `mesh submit` records a top-level Job — the
// autonomous entry point (#23). The jobs KV record (internal/job) is the one
// authority; a KindJob envelope is published on mesh.job.<id> so the dashboard
// and any mesh.> tap see the intake without polling the KV. Same handler
// discipline as handleAsk: parse → require joined → store → publish → typed
// result.

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
	"github.com/georgenijo/agent-mesh/internal/socket"
)

func (s *Sidecar) jobStore() job.Store { return job.NewStore(s.bus) }

func (s *Sidecar) handleSubmit(req socket.Request) socket.Response {
	var args meshapi.SubmitArgs
	if err := json.Unmarshal(req.Args, &args); err != nil {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("submit args: %v", err))
	}
	args.Repo = strings.TrimSpace(args.Repo)
	args.Title = strings.TrimSpace(args.Title)
	if args.Repo == "" {
		return socket.Fail(socket.CodeBadRequest, "repo is required")
	}
	if args.Title == "" {
		return socket.Fail(socket.CodeBadRequest, "title is required")
	}
	if len(args.Title) > meshapi.MaxJobTitleLen {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("title %d bytes exceeds limit %d", len(args.Title), meshapi.MaxJobTitleLen))
	}
	if len(args.Body) > meshapi.MaxJobBodyLen {
		return socket.Fail(socket.CodeBadRequest, fmt.Sprintf("body %d bytes exceeds limit %d", len(args.Body), meshapi.MaxJobBodyLen))
	}
	id, joined := s.joinedID()
	if !joined {
		return socket.Fail(socket.CodeNotJoined, "agent has not joined")
	}

	rec, err := s.jobStore().Create(job.Record{
		Repo: args.Repo, Source: args.Source, SourceRef: args.SourceRef,
		Title: args.Title, Body: args.Body,
	})
	if err != nil {
		return socket.Fail(socket.CodeUnavailable, err.Error())
	}

	env, err := envelope.New(envelope.KindJob, id, envelope.SubjectJob(rec.ID), &envelope.JobPayload{
		ID: rec.ID, Repo: rec.Repo, Source: rec.Source, SourceRef: rec.SourceRef, Title: rec.Title, State: rec.State,
	})
	if err != nil {
		return socket.Fail(socket.CodeInternal, err.Error())
	}
	if err := s.bus.Publish(env); err != nil {
		return socket.Fail(socket.CodeUnavailable, fmt.Sprintf("bus publish: %v", err))
	}

	return socket.OKData(meshapi.SubmitResult{
		Job: rec.ID, Repo: rec.Repo, State: rec.State,
		Source: rec.Source, SourceRef: rec.SourceRef,
	})
}
