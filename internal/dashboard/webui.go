package dashboard

import (
	"net/http"

	"github.com/georgenijo/agent-mesh/web"
)

// mountWebUI serves the embedded production dashboard UI (issue #31, the
// web package) on mux under /ui/, alongside the P0 observer page at /. Both
// consume the same SSE bridge (GET /events); the UI assets are static and
// read-only, so the dashboard stays a pure observer.
//
// /ui redirects to /ui/ so the page's relative asset references (app.js,
// style.css) resolve under the mount — the same trailing-slash
// canonicalization http.FileServer applies to directories.
func mountWebUI(mux *http.ServeMux) {
	mux.Handle("GET /ui/", http.StripPrefix("/ui/", http.FileServerFS(web.Assets)))
	mux.HandleFunc("GET /ui", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusMovedPermanently)
	})
}
