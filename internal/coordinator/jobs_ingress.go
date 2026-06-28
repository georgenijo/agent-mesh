package coordinator

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/georgenijo/agent-mesh/internal/bus"
	"github.com/georgenijo/agent-mesh/internal/envelope"
	"github.com/georgenijo/agent-mesh/internal/job"
	"github.com/georgenijo/agent-mesh/internal/meshapi"
)

// jobsIngress is the HTTP POST /jobs dispatch endpoint (#119). It is started by
// the coordinator when MESH_JOBS_ADDR is set and accepts JSON {repo,title,body},
// creating a job through the same job.Store path that `mesh submit` uses.
type jobsIngress struct {
	srv *http.Server
	lis net.Listener
	cli *bus.Client
}

func newJobsIngress(addr string, cli *bus.Client) (*jobsIngress, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("jobs ingress: listen %s: %w", addr, err)
	}
	ji := &jobsIngress{cli: cli, lis: lis}
	mux := http.NewServeMux()
	mux.HandleFunc("/jobs", ji.serveJobs)
	ji.srv = &http.Server{Handler: mux}
	return ji, nil
}

// start serves in a background goroutine. The caller owns stopping via close().
func (ji *jobsIngress) start() {
	go ji.srv.Serve(ji.lis) //nolint:errcheck
}

// addr returns the real bound address (useful when the caller passed ":0").
func (ji *jobsIngress) addr() string { return ji.lis.Addr().String() }

func (ji *jobsIngress) close() { ji.srv.Close() }

// jobsIngressRequest is the POST /jobs request body.
type jobsIngressRequest struct {
	Repo  string `json:"repo"`
	Title string `json:"title"`
	Body  string `json:"body,omitempty"`
}

func (ji *jobsIngress) serveJobs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		ingressJSONError(w, `{"error":"method_not_allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	const maxBody = meshapi.MaxJobBodyLen + meshapi.MaxJobTitleLen + 4096
	r.Body = http.MaxBytesReader(w, r.Body, maxBody)

	var req jobsIngressRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			ingressJSONError(w, `{"error":"bad_request","message":"body too large"}`, http.StatusBadRequest)
		} else {
			ingressJSONError(w, `{"error":"bad_request","message":"invalid JSON body"}`, http.StatusBadRequest)
		}
		return
	}

	req.Repo = strings.TrimSpace(req.Repo)
	req.Title = strings.TrimSpace(req.Title)

	if req.Repo == "" {
		ingressJSONError(w, `{"error":"bad_request","message":"repo is required"}`, http.StatusBadRequest)
		return
	}
	if req.Title == "" {
		ingressJSONError(w, `{"error":"bad_request","message":"title is required"}`, http.StatusBadRequest)
		return
	}
	if len(req.Title) > meshapi.MaxJobTitleLen {
		ingressJSONError(w, fmt.Sprintf(`{"error":"bad_request","message":"title exceeds %d bytes"}`, meshapi.MaxJobTitleLen), http.StatusBadRequest)
		return
	}
	if len(req.Body) > meshapi.MaxJobBodyLen {
		ingressJSONError(w, fmt.Sprintf(`{"error":"bad_request","message":"body exceeds %d bytes"}`, meshapi.MaxJobBodyLen), http.StatusBadRequest)
		return
	}

	store := job.NewStore(ji.cli)
	rec, err := store.Create(job.Record{
		Repo:   req.Repo,
		Source: job.SourceManual,
		Title:  req.Title,
		Body:   req.Body,
	})
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]any{"error": "unavailable", "message": err.Error()}) //nolint:errcheck
		return
	}

	// Publish a KindJob observability event so mesh.> subscribers see the
	// intake exactly as `mesh submit` does.
	env, err := envelope.New(envelope.KindJob, "jobs-ingress", envelope.SubjectJob(rec.ID), &envelope.JobPayload{
		ID: rec.ID, Repo: rec.Repo, Source: rec.Source, Title: rec.Title, State: rec.State,
	})
	if err == nil {
		ji.cli.Publish(env) //nolint:errcheck // best-effort: KV write is authoritative
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{"job": rec.ID}) //nolint:errcheck
}

func ingressJSONError(w http.ResponseWriter, body string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write([]byte(body)) //nolint:errcheck
}
