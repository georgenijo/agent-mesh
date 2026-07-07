// Package web embeds the production dashboard UI (issue #31): the promotion
// of docs/mockups/dashboard-bus.html into a live, read-only mesh.> tap.
//
// The UI consumes the SSE bridge the dashboard server already exposes
// (internal/dashboard, GET /events): data-only JSON frames discriminated by
// "type" — "roster" frames carry the authoritative registry snapshot,
// "event" frames carry one tapped envelope. It never publishes anything.
//
// This package only exports the static assets; wiring them into the
// dashboard server's mux is the integration step, done separately.
package web

import "embed"

// Assets holds the dashboard UI files, embedded so meshd stays a single
// static binary with zero external dependencies.
//
//go:embed index.html app.js style.css enhance.js jobform.js settings.html settings.js
var Assets embed.FS
