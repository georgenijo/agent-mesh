# Observer redesign — interactive mockups (2026-06)

Self-contained HTML mockups (no build step — open in a browser) exploring a
redesign of the Agent Mesh dashboard. These came out of a design session on the
existing `web/` observer and the `docs/mockups/dashboard-*.html` concepts.

> All files are standalone: inline CSS/JS, a small live simulation drives the
> data so the interactions are real. Near-black navy + cyan/teal + monospace to
> match the existing terminal aesthetic.

## Files

| File | What it is |
|---|---|
| **`dashboard.html`** | **Entry point.** A `Console` / `Wall` view toggle (press `V`). Embeds the two observer modes below and remembers the last choice. |
| `observer-v2.html` | **Console** — attention-first operator cockpit. Triage strip (blocked / unanswered questions / budget / file contention / reviews) → sortable fleet table → click a row for a drill-in drawer with intervene actions (Answer, Break lock, Pause, Reassign, Kill). |
| `observer.html` | **Wall** — constellation-hero live observer. Topology with **heat-edge traffic** (messages flare the edge + arrow, then decay — no flying-packet noise), a linked dialogue/events stream, file locks, claim history, shared blackboard, tasks/jobs, activity ribbon. |
| `topology-redesign.html` | Topology layout lab. Constellation vs lanes, scrub agent count 2→40 and traffic rate to see which stays readable. Level-of-detail: worker cards → dots → idle cluster. |
| `control-panel.html` | Settings / Control Panel cockpit — model routing per role, concurrency & budget, autonomy toggles, repos & access, save/load profiles, live/needs-restart badges. |

## Design rationale (why this shape)

The current `web/` observer uses a **bus topology** ported from
`dashboard-bus.html`. Two problems it hits:

1. **It conflates structure and traffic.** The graph tries to be both the map
   (who routes to whom) and the message log (what was said), so under load it
   becomes noise.
2. **Topology-as-hero is a demo instinct.** Operators live in lists, timelines,
   and triage — a graph can't be sorted, scanned, or searched. The graph is best
   for onboarding and a wall display, not the everyday operator screen.

The redesign splits the product into two modes (the `dashboard.html` toggle):

- **Console (operator):** triage → sortable fleet → drill-in + intervene. Answers
  "what needs me right now" in <2s. This should be the default day-to-day screen.
- **Wall (glance/demo):** the live constellation with heat-edge traffic. Great as
  a second monitor or the landing-page hero.

Key topology fixes carried by both:
- **Heat edges, not packets** — a message flares its edge (color = kind, arrow =
  direction) then decays. Hot routes glow; the firehose disappears.
- **Structure vs flow separated** — the topology answers *where*; the events
  stream answers *what*. They're linked (click a node → filter the stream +
  highlight its locks; hover an event → flare its route).
- **Blackboard is NOT an agent** — it's shared memory, rendered as an infra slab
  (dashed teal), not a node in the agent ring. The coordinator is the infra hub.
- **Level-of-detail** — full worker cards at low N; shrink to labeled dots past
  ~12; cluster idle workers into an expandable pod. No hard "+N more" hide.

## Suggested implementation path (for whoever picks this up)

The mockups are the target; the real surface is `web/` (`index.html`, `app.js`,
`style.css`) served by `internal/dashboard/`.

1. **Replace the bus renderer** (`web/app.js` `syncStageNodes` / `STAGE_*` /
   `agentX`) with the **heat-edge constellation**: coordinator hub center,
   planner/experts/reviewer inner ring, workers outer ring (LOD to dots +
   idle cluster), blackboard as a pinned infra slab. Drive edge flare/decay from
   real envelope frames instead of flying packet dots.
2. **Add the Console mode** — triage strip + sortable fleet table + drill-in
   drawer wired to the existing roster / locks / jobs / blackboard data already
   on the SSE feed. Intervene actions map to mesh commands.
3. **Wrap both in the `dashboard.html` Console/Wall toggle.**
4. Treat **`control-panel.html`** as a separate settings screen for model
   routing / concurrency / autonomy / profiles.

Phase: this is **P4 (live dashboard)** work.
