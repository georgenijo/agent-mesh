package observe

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/georgenijo/agent-mesh/internal/config"
)

//go:embed observe.html
var indexHTML []byte

// Server serves read-only runtime snapshots over HTTP.
type Server struct {
	cfg  config.Config
	addr string
	log  *slog.Logger

	httpSrv *http.Server
	ln      net.Listener
}

// New creates an observe server listening on addr (default cfg.ObserveAddr).
func New(cfg config.Config, addr string, log *slog.Logger) *Server {
	if addr == "" {
		addr = cfg.ObserveAddr
	}
	if log == nil {
		log = slog.Default()
	}
	return &Server{cfg: cfg, addr: addr, log: log}
}

// Start binds HTTP and begins serving.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.serveIndex)
	mux.HandleFunc("GET /api/snapshot", s.serveSnapshot)

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("observe: listen %s: %w", s.addr, err)
	}
	s.ln = ln
	s.httpSrv = &http.Server{Handler: mux}

	go s.httpSrv.Serve(ln) //nolint:errcheck // closed on Stop
	s.log.Info("observe started", "addr", s.Addr())
	return nil
}

// Addr returns the bound listen address.
func (s *Server) Addr() string {
	if s.ln == nil {
		return s.addr
	}
	return s.ln.Addr().String()
}

// Stop shuts down HTTP.
func (s *Server) Stop() {
	if s.httpSrv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s.httpSrv.Shutdown(ctx) //nolint:errcheck
}

// serveSnapshot returns the current runtime snapshot as JSON.
func (s *Server) serveSnapshot(w http.ResponseWriter, r *http.Request) {
	snap, err := Collect(s.cfg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(snap) //nolint:errcheck
}

func (s *Server) serveIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML) //nolint:errcheck
}
