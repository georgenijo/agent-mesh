// Agent Mesh — production live dashboard (issue #31).
//
// Pure read-only observer. Consumes the SSE bridge served by the dashboard
// daemon (internal/dashboard) at GET /events: data-only frames, JSON,
// discriminated by "type":
//
//   {"type":"roster","agents":[RegistryRecord, ...]}   authoritative presence
//   {"type":"event","envelope":{schemaVersion,kind,id,from,to,subject,ts,payload}}
//
// Invariants honoured here (docs/decisions/DECISIONS.md):
//   - One authority per fact: presence is ALWAYS rebuilt from the latest
//     roster frame and held claims from the latest claims-KV snapshot frame,
//     never from accumulated UI counters or event-derived state. On
//     reconnect the next frames rebuild both wholesale.
//   - Never fake data: views derived from the event log (notes, tickets)
//     only show envelopes this page actually observed, and say so in their
//     empty states. Ticket / expert panels are placeholders until P2/P3
//     emit real traffic — they render nothing invented.
//   - Read-only: this script issues no mutating request; its only server
//     interaction is the EventSource GET.
"use strict";

/* ------------------------------------------------------------------------- *
 * Wire contract (mirrors internal/envelope — never invent kinds).
 * ------------------------------------------------------------------------- */

// Envelope kinds from internal/envelope/envelope.go — exactly the wire's
// knownKinds set, nothing invented (an unknown kind is rejected at the
// publish edge and dropped by the SSE bridge, so it could never reach this
// page anyway). Unknown kinds fold into "other" so a newer meshd never
// breaks this page.
const KINDS = [
  "register", "leave", "heartbeat", "status", "announce",
  "claim", "ask", "answer", "note", "ticket",
  "job", "task", "triage", "worker", "fleet",
];

const KIND_COLOR = {
  register: "#38ffa3",
  leave: "#f66f7d",
  heartbeat: "#3a6b8a",
  status: "#21e6ff",
  announce: "#6aa8ff",
  claim: "#ffb31f",
  ask: "#f4b942",
  answer: "#38ffa3",
  note: "#9b8cff",
  ticket: "#a78bfa",
  job: "#68f5b8",
  task: "#7dd3fc",
  triage: "#f4b942",
  worker: "#38ffa3",
  fleet: "#ffb31f",
  other: "#8a98aa",
};

const MAX_EVENTS = 500; // raw log ring buffer
const MAX_NOTES = 100;
const MAX_TICKETS = 100;
const MAX_AGENT_NODES = 8; // stage cap; the Agents panel always lists everyone
const MAX_PACKETS = 40;
const ROSTER_STALE_MS = 3500; // server pushes a roster frame every 1s

/* ------------------------------------------------------------------------- *
 * State.
 * ------------------------------------------------------------------------- */

const state = {
  connected: false,
  everConnected: false,
  claimsSnapshotted: false, // first authoritative claims frame has arrived
  agents: [], // latest roster frame verbatim — the one presence authority
  rosterAt: 0, // ms clock of the last roster frame
  events: [], // raw envelope log, oldest first
  totalEvents: 0, // observed-since-connect tally (display only, never authoritative)
  claims: new Map(), // claim key -> {holder, path, repo, ts}
  notes: [], // newest first
  tickets: new Map(), // ticket id -> {ticket, from, route, q, state, answer, answeredBy, ts}
  filter: { kinds: new Set(), subject: "", heartbeats: false },
  openEvents: new Set(), // env ids whose <details> the reader has expanded
  // P3: populated from authoritative snapshot frames (jobs/tasks/workers/triage/fleet).
  // One authority per fact: these are replaced wholesale from each server frame —
  // never accumulated from UI counters.
  jobs: new Map(),      // job id -> {id, repo, source, title, state, ts}
  tasks: new Map(),     // task id -> {id, job, role, title, state, ts}
  workers: [],          // newest last; bounded ring from the server
  triages: [],          // newest last; bounded ring from the server
  fleet: null,          // null until first KindFleet observed; {state, code, spentUSD, budgetUSD, ts}
  jobsSnapshotted: false,   // first authoritative jobs frame has arrived
  tasksSnapshotted: false,  // first authoritative tasks frame has arrived
  // Claude Code session telemetry (proxied GET /api/claude-sessions).
  claudeSessions: [],
  claudeSessionsError: null,
  claudeSessionsAt: 0,
};

function byId(id) {
  return document.getElementById(id);
}

function esc(value) {
  return String(value ?? "").replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;",
  }[ch]));
}

function kindColor(kind) {
  return KIND_COLOR[kind] || KIND_COLOR.other;
}

function chipKind(kind) {
  return KINDS.includes(kind) ? kind : "other";
}

// Derive mesh agent name from Claude session_id (hooks/claude-code/mesh-session-start.sh).
function meshNameFromSessionId(sessionId) {
  if (!sessionId) return null;
  const short = String(sessionId).replace(/[^A-Za-z0-9]/g, "").slice(0, 8);
  return short ? "cc-" + short : null;
}

function meshAgentForSession(session) {
  const meshName = meshNameFromSessionId(session && session.session_id);
  if (!meshName) return null;
  return state.agents.find((a) => {
    const card = a.card || {};
    return card.name === meshName || card.id === meshName;
  }) || null;
}

function sessionForMeshAgent(agent) {
  const card = agent && agent.card || {};
  const meshName = card.name || card.id || "";
  if (!meshName.startsWith("cc-")) return null;
  return state.claudeSessions.find((s) => meshNameFromSessionId(s.session_id) === meshName) || null;
}

function formatMB(mb) {
  if (mb == null || Number.isNaN(mb)) return "—";
  return mb >= 1024 ? (mb / 1024).toFixed(1) + " GB" : mb.toFixed(0) + " MB";
}

function timeText(value) {
  if (!value) return "-";
  const t = new Date(value);
  if (Number.isNaN(t.getTime())) return "-";
  return t.toLocaleTimeString();
}

// issueURL converts a sourceRef like "owner/repo#N" to a GitHub issue URL.
// Returns null if the ref is absent or doesn't match the expected pattern.
function issueURL(ref) {
  if (!ref) return null;
  const m = ref.match(/^([^/\s#]+\/[^/\s#]+)#(\d+)$/);
  if (!m) return null;
  return "https://github.com/" + m[1] + "/issues/" + m[2];
}

function ageText(value) {
  if (!value) return "-";
  const t = new Date(value).getTime();
  if (Number.isNaN(t)) return "-";
  const diff = Math.max(0, Date.now() - t);
  if (diff < 1000) return "now";
  const s = Math.round(diff / 1000);
  if (s < 60) return s + "s";
  const m = Math.round(s / 60);
  if (m < 60) return m + "m";
  return Math.round(m / 60) + "h";
}

function eventLabel(env) {
  const p = env.payload || {};
  switch (env.kind) {
    case "status": return p.text || "";
    case "announce": return p.intent || "";
    case "claim": return ((p.result || "claim") + " " + (p.path || "")).trim();
    case "ask": return p.q || p.ticket || "";
    case "answer": return p.answer || p.ticket || "";
    case "note": return p.decision || "";
    case "leave": return p.reason ? (p.id || "") + " (" + p.reason + ")" : p.id || "";
    case "heartbeat": return p.status || "";
    case "job": return (p.state ? p.state + " " : "") + (p.title || p.id || "");
    case "task": return (p.state ? p.state + " " : "") + (p.title || p.id || "");
    case "triage": return (p.result || "") + (p.tasks ? " (" + p.tasks + " tasks)" : "");
    case "worker": return (p.result || "") + (p.costUSD ? " $" + p.costUSD.toFixed(4) : "");
    case "fleet": return p.state || "";
    case "ticket": return p.state || p.ticket || "";
    default: return "";
  }
}

/* ------------------------------------------------------------------------- *
 * Ingestion.
 * ------------------------------------------------------------------------- */

function onRoster(agents) {
  // Roster frames are authoritative for presence: replace wholesale,
  // never merge with event-derived guesses.
  state.agents = Array.isArray(agents) ? agents : [];
  state.rosterAt = Date.now();
  syncStageNodes();
  scheduleRender();
}

// onClaims replaces the held-claims view wholesale from the dashboard's
// authoritative claims-KV snapshot frame (one source of truth: the claims
// bucket; this view is never derived from claim/leave envelopes).
function onClaims(held) {
  if (!Array.isArray(held)) return;
  state.claims.clear();
  for (const h of held) {
    if (!h || !h.path) continue;
    state.claims.set(claimKey(h), {
      holder: h.agent || "",
      path: h.path,
      repo: h.repo || "",
      ts: h.ts,
    });
  }
  state.claimsSnapshotted = true;
  scheduleRender();
}

// onJobs replaces the jobs view wholesale from the server's authoritative
// snapshot frame. Jobs are keyed by id; the server sends the complete current
// map on every change, so local state never drifts.
function onJobs(jobs) {
  if (!Array.isArray(jobs)) return;
  state.jobs.clear();
  for (const j of jobs) {
    if (!j || !j.id) continue;
    state.jobs.set(j.id, j);
  }
  state.jobsSnapshotted = true;
  scheduleRender();
}

// onTasks replaces the tasks view wholesale from the server's authoritative
// snapshot frame.
function onTasks(tasks) {
  if (!Array.isArray(tasks)) return;
  state.tasks.clear();
  for (const t of tasks) {
    if (!t || !t.id) continue;
    state.tasks.set(t.id, t);
  }
  state.tasksSnapshotted = true;
  scheduleRender();
}

// onWorkers replaces the workers ring from the server's authoritative frame.
function onWorkers(workers) {
  if (!Array.isArray(workers)) return;
  state.workers = workers.slice();
  scheduleRender();
}

// onTriage replaces the triage ring from the server's authoritative frame.
function onTriage(triages) {
  if (!Array.isArray(triages)) return;
  state.triages = triages.slice();
  scheduleRender();
}

// onFleet stores the latest fleet-state from the server's authoritative frame.
function onFleet(fleet) {
  if (!fleet || typeof fleet !== "object") return;
  state.fleet = fleet;
  scheduleRender();
}

function onEnvelope(env) {
  if (!env || typeof env !== "object" || !env.kind) return;
  state.events.push(env);
  if (state.events.length > MAX_EVENTS) state.events.shift();
  state.totalEvents++;
  deriveFromEnvelope(env);
  animateEnvelope(env);
  scheduleRender();
}

// Claim keys join the ClaimPayload's (repo, path) with a NUL so two claims
// on the same file collapse to one row here too.
function claimKey(p) {
  return (p.repo || "") + "\u0000" + (p.path || "");
}

function deriveFromEnvelope(env) {
  const p = env.payload || {};
  switch (env.kind) {
    // Claim and leave envelopes render in the event stream and animate on
    // the stage, but the held-claims view is NOT derived from them: the
    // dashboard pushes an authoritative claims-KV snapshot frame
    // (type:"claims") and onClaims replaces the view wholesale. One
    // authority per fact.
    case "note":
      state.notes.unshift({ from: env.from || "", decision: p.decision || "", repo: p.repo || "", ts: env.ts });
      if (state.notes.length > MAX_NOTES) state.notes.pop();
      break;
    case "ask": {
      const id = p.ticket || env.id;
      state.tickets.set(id, {
        ticket: id,
        from: env.from || "",
        route: p.to || env.to || p.role || "open",
        q: p.q || "",
        state: "open",
        answer: "",
        answeredBy: "",
        ts: env.ts,
      });
      trimTickets();
      break;
    }
    case "answer": {
      const id = p.ticket || env.id;
      const t = state.tickets.get(id) || { ticket: id, from: "", route: "", q: "", ts: env.ts };
      t.state = "answered";
      t.answer = p.answer || "";
      t.answeredBy = env.from || "";
      state.tickets.set(id, t);
      trimTickets();
      break;
    }
  }
}

function trimTickets() {
  while (state.tickets.size > MAX_TICKETS) {
    const oldest = state.tickets.keys().next().value;
    state.tickets.delete(oldest);
  }
}

/* ------------------------------------------------------------------------- *
 * Connection. EventSource auto-reconnects; when the server closes the stream
 * for good (readyState CLOSED) we re-dial with a backoff. After any drop the
 * next roster frame rebuilds presence, and the event-derived claims view is
 * reset because claim and leave traffic during the gap may have been missed.
 * ------------------------------------------------------------------------- */

function setConnected(connected, text) {
  state.connected = connected;
  byId("connDot").className = "dot " + (connected ? "live" : state.everConnected ? "down" : "");
  byId("connText").textContent = text;
}

function connect() {
  const es = new EventSource("/events");

  es.onopen = () => {
    // Reconnects self-heal: the next roster frame is authoritative for
    // presence and the next claims frame replaces the held-claims view
    // wholesale, so nothing here needs resetting.
    state.everConnected = true;
    setConnected(true, "live");
    scheduleRender();
  };

  es.onmessage = (event) => {
    let msg;
    try {
      msg = JSON.parse(event.data);
    } catch {
      return; // not a frame we understand; never guess
    }
    if (msg.type === "roster") onRoster(msg.agents);
    else if (msg.type === "event") onEnvelope(msg.envelope);
    else if (msg.type === "claims") onClaims(msg.claims);
    else if (msg.type === "jobs") onJobs(msg.jobs);
    else if (msg.type === "tasks") onTasks(msg.tasks);
    else if (msg.type === "workers") onWorkers(msg.workers);
    else if (msg.type === "triage") onTriage(msg.triages);
    else if (msg.type === "fleet") onFleet(msg.fleet);
  };

  es.onerror = () => {
    setConnected(false, "reconnecting");
    scheduleRender();
    if (es.readyState === EventSource.CLOSED) {
      es.close();
      setTimeout(connect, 2000);
    }
  };
}

/* ------------------------------------------------------------------------- *
 * Stage: the bus topology, ported from docs/mockups/dashboard-bus.html but
 * driven by real frames. Agent nodes come from the roster (top side); the
 * coordinator (which embeds the bus) and this observer sit below.
 * ------------------------------------------------------------------------- */

const STAGE_W = 1280;
const BUS_TOP = 430;
const BUS_BOT = 474;
const BUS_MID = 452;
const TOP_Y = 240;
const BOT_Y = 614;
const SVGNS = "http://www.w3.org/2000/svg";

const AGENT_COLORS = ["#21e6ff", "#a78bfa", "#f472b6", "#ffb31f", "#38ffa3", "#7dd3fc", "#f66f7d", "#6aa8ff"];

const ROLE_GLYPHS = { builder: "◆", reviewer: "●", expert: "★", worker: "⬡", observer: "▣" };

const INFRA_NODES = [
  { key: "@coordinator", name: "coordinator", role: "bus · control plane", glyph: "✺", color: "#38ffa3", x: 440, hub: true },
  { key: "@observer", name: "dashboard", role: "observer · this page", glyph: "▣", color: "#7dd3fc", x: 840, hub: false },
];

const stageNodes = new Map(); // key -> {key,x,y,side,anchorY,busEdge,el,color}
const nodeMsgs = new Map(); // key -> last label shown on the card
let stageSignature = "";
const packets = [];
let packetLoopRunning = false;

function agentX(i, n) {
  if (n <= 1) return 640;
  return 240 + Math.round((800 * i) / (n - 1));
}

function agentGlyph(card) {
  return ROLE_GLYPHS[String(card.role || "").toLowerCase()] || "◆";
}

// resolveNodeKey maps an envelope "from"/"to" id to a stage node, if shown.
function resolveNodeKey(id) {
  if (!id) return null;
  if (stageNodes.has(id)) return id;
  if (id === "coordinator") return "@coordinator";
  return null;
}

function syncStageNodes() {
  const shown = state.agents.slice(0, MAX_AGENT_NODES);
  const overflow = state.agents.length - shown.length;
  const signature = shown.map((a) => (a.card && a.card.id) + ":" + a.state).join("|");
  const chip = byId("overflowChip");
  if (overflow > 0) {
    chip.hidden = false;
    chip.textContent = "+" + overflow + " more agent" + (overflow === 1 ? "" : "s") + " (see Agents panel)";
  } else {
    chip.hidden = true;
  }
  if (signature === stageSignature) return;
  stageSignature = signature;

  const layer = byId("nodeLayer");
  const staticRoutes = byId("staticRoutes");
  layer.textContent = "";
  staticRoutes.textContent = "";
  stageNodes.clear();

  const defs = [];
  shown.forEach((agent, i) => {
    const card = agent.card || {};
    // Past four agents, stagger the top side into two rows so the 172px
    // cards never overlap horizontally.
    const y = shown.length > 4 ? (i % 2 === 0 ? TOP_Y - 60 : TOP_Y + 60) : TOP_Y;
    defs.push({
      key: card.id || card.name || "agent-" + i,
      name: card.name || card.id || "?",
      role: card.role || "-",
      glyph: agentGlyph(card),
      color: AGENT_COLORS[i % AGENT_COLORS.length],
      x: agentX(i, shown.length),
      y,
      side: "top",
      away: agent.state === "away",
      hub: false,
    });
  });
  for (const infra of INFRA_NODES) {
    defs.push({ ...infra, y: BOT_Y, side: "bot", away: false });
  }

  for (const def of defs) {
    const el = document.createElement("div");
    el.className = "node" + (def.hub ? " hub" : "") + (def.away ? " away" : "");
    el.style.left = def.x + "px";
    el.style.top = def.y + "px";
    const msg = nodeMsgs.get(def.key) || "idle";
    el.innerHTML = '<div class="top">' +
      '<div class="glyph" style="color:' + def.color + ";border-color:" + def.color + "55;background:" + def.color + '1a">' + esc(def.glyph) + "</div>" +
      '<div><div class="name">' + esc(def.name) + '</div><div class="role">' + esc(def.role) + "</div></div>" +
      "</div>" +
      '<div class="stat"><span class="ndot" style="background:' + def.color + ";box-shadow:0 0 8px " + def.color + '"></span><span class="msg">' + esc(msg) + "</span></div>";
    byId("nodeLayer").appendChild(el);
    if (def.side === "top") el.dataset.drillAgent = def.key;

    const node = {
      key: def.key,
      x: def.x,
      y: def.y,
      side: def.side,
      anchorY: def.side === "top" ? def.y + 50 : def.y - 50,
      busEdge: def.side === "top" ? BUS_TOP : BUS_BOT,
      el,
      color: def.color,
    };
    stageNodes.set(def.key, node);

    // Faint connector node -> bus, with a bus-stop dot.
    const line = document.createElementNS(SVGNS, "path");
    line.setAttribute("d", "M " + node.x + " " + node.anchorY + " L " + node.x + " " + node.busEdge);
    line.setAttribute("stroke", "rgba(120,180,255,.14)");
    line.setAttribute("stroke-width", "2");
    line.setAttribute("fill", "none");
    staticRoutes.appendChild(line);
    const stop = document.createElementNS(SVGNS, "circle");
    stop.setAttribute("cx", node.x);
    stop.setAttribute("cy", node.busEdge);
    stop.setAttribute("r", "4");
    stop.setAttribute("fill", "#21e6ff");
    stop.setAttribute("opacity", ".7");
    staticRoutes.appendChild(stop);
  }
}

function flashNode(key, kind, text) {
  const node = stageNodes.get(key);
  if (!node) return;
  if (text) {
    nodeMsgs.set(key, text);
    const msgEl = node.el.querySelector(".msg");
    if (msgEl) msgEl.textContent = text;
  }
  const dot = node.el.querySelector(".ndot");
  if (dot) {
    dot.style.background = kindColor(kind);
    dot.style.boxShadow = "0 0 8px " + kindColor(kind);
  }
  node.el.classList.add("flash");
  setTimeout(() => node.el.classList.remove("flash"), 500);
}

function routePath(from, to) {
  if (to) {
    return "M " + from.x + " " + from.anchorY +
      " L " + from.x + " " + BUS_MID +
      " L " + to.x + " " + BUS_MID +
      " L " + to.x + " " + to.anchorY;
  }
  // Broadcast: ride to the centre of the bus and fade there.
  return "M " + from.x + " " + from.anchorY +
    " L " + from.x + " " + BUS_MID +
    " L 640 " + BUS_MID;
}

function animateEnvelope(env) {
  const label = eventLabel(env) || env.kind;
  if (env.kind === "heartbeat" && !state.filter.heartbeats) {
    // Heartbeats every 5s per agent would drown the stage: just blink.
    flashNode(resolveNodeKey(env.from), env.kind, null);
    return;
  }
  const fromKey = resolveNodeKey(env.from);
  if (!fromKey) return; // beyond the stage cap or already evicted; log still shows it
  const toKey = resolveNodeKey(env.to);
  flashNode(fromKey, env.kind, label);
  if (packets.length >= MAX_PACKETS) return;
  spawnPacket(stageNodes.get(fromKey), toKey ? stageNodes.get(toKey) : null, env.kind, toKey, label);
}

function spawnPacket(from, to, kind, toKey, label) {
  const layer = byId("packetLayer");
  const d = routePath(from, to);

  const path = document.createElementNS(SVGNS, "path");
  path.setAttribute("d", d);
  path.setAttribute("fill", "none");
  path.setAttribute("stroke", "none");
  layer.appendChild(path);
  const len = path.getTotalLength();

  const trail = document.createElementNS(SVGNS, "path");
  trail.setAttribute("d", d);
  trail.setAttribute("fill", "none");
  trail.setAttribute("stroke", kindColor(kind));
  trail.setAttribute("stroke-width", "2.5");
  trail.setAttribute("stroke-linecap", "round");
  trail.setAttribute("opacity", "0");
  trail.setAttribute("stroke-dasharray", "60 " + len);
  layer.appendChild(trail);

  const dot = document.createElementNS(SVGNS, "circle");
  dot.setAttribute("r", "5");
  dot.setAttribute("fill", kindColor(kind));
  dot.setAttribute("filter", "url(#pktglow)");
  layer.appendChild(dot);

  packets.push({ path, trail, dot, len, t: 0, speed: 0.011 + Math.random() * 0.004, kind, toKey, label, arrived: false });
  if (!packetLoopRunning) {
    packetLoopRunning = true;
    requestAnimationFrame(tickPackets);
  }
}

function tickPackets() {
  for (let i = packets.length - 1; i >= 0; i--) {
    const p = packets[i];
    p.t += p.speed;
    if (p.t >= 1) {
      if (!p.arrived && p.toKey) {
        p.arrived = true;
        flashNode(p.toKey, p.kind, p.label);
      }
      p.path.remove();
      p.trail.remove();
      p.dot.remove();
      packets.splice(i, 1);
      continue;
    }
    const pt = p.path.getPointAtLength(p.t * p.len);
    p.dot.setAttribute("cx", pt.x);
    p.dot.setAttribute("cy", pt.y);
    const off = Math.max(0, p.t * p.len - 60);
    p.trail.setAttribute("stroke-dashoffset", String(-off));
    p.trail.setAttribute("opacity", String(p.t < 0.06 ? (p.t / 0.06) * 0.8 : 0.8 * (1 - Math.max(0, p.t - 0.85) / 0.15)));
  }
  if (packets.length) {
    requestAnimationFrame(tickPackets);
  } else {
    packetLoopRunning = false;
  }
}

function fitStage() {
  const wrap = byId("stagewrap");
  const scale = Math.min(wrap.clientWidth / (STAGE_W + 20), wrap.clientHeight / 740);
  byId("stage").style.transform = "scale(" + Math.min(1, scale) + ")";
}

/* ------------------------------------------------------------------------- *
 * Panels.
 * ------------------------------------------------------------------------- */

let renderQueued = false;

function scheduleRender() {
  if (renderQueued) return;
  renderQueued = true;
  requestAnimationFrame(() => {
    renderQueued = false;
    render();
  });
}

function presenceCounts() {
  // Recomputed from the latest roster frame every time — the roster is the
  // one authority for presence; UI counters are never trusted for this.
  let live = 0;
  let away = 0;
  for (const a of state.agents) {
    if (a.state === "live") live++;
    else if (a.state === "away") away++;
  }
  return { live, away, total: state.agents.length };
}

function renderSummary() {
  const { live, away, total } = presenceCounts();
  byId("liveMetric").textContent = String(live);
  byId("awayMetric").textContent = String(away);
  byId("claimMetric").textContent = String(state.claims.size);
  byId("noteMetric").textContent = String(state.notes.length);
  byId("eventMetric").textContent = String(state.totalEvents);
  const jobMetric = byId("jobMetric");
  if (jobMetric) jobMetric.textContent = String(state.jobs.size);
  const taskMetric = byId("taskMetric");
  if (taskMetric) taskMetric.textContent = String(state.tasks.size);

  const pill = byId("presencePill");
  if (!total) {
    pill.textContent = "empty";
    pill.className = "pill";
  } else {
    pill.textContent = live + " live" + (away ? ", " + away + " away" : "");
    pill.className = "pill " + (live ? "green" : "amber");
  }

  byId("claimsPill").textContent = state.claims.size + " held";
  byId("claimsPill").className = "pill " + (state.claims.size ? "amber" : "");
  byId("notesPill").textContent = state.notes.length + " note" + (state.notes.length === 1 ? "" : "s");
  byId("notesPill").className = "pill " + (state.notes.length ? "violet" : "");
  byId("streamPill").textContent = state.totalEvents ? state.totalEvents + " observed" : "idle";
  byId("streamPill").className = "pill " + (state.connected ? "green" : "amber");

  if (state.connected && state.rosterAt && Date.now() - state.rosterAt > ROSTER_STALE_MS) {
    setConnected(true, "live · roster stale " + ageText(state.rosterAt));
  } else if (state.connected) {
    setConnected(true, "live");
  }
}

function renderAgents() {
  const list = byId("agentList");
  if (!state.agents.length) {
    list.innerHTML = '<div class="empty">No agents registered.<br>The roster snapshot refreshes every second from the registry.</div>';
    return;
  }
  list.innerHTML = state.agents.map((agent) => {
    const card = agent.card || {};
    const pillClass = agent.state === "live" ? "green" : agent.state === "away" ? "amber" : "rose";
    const meta = [card.role || "-", card.repo || "", "seen " + ageText(agent.lastSeen) + " ago"].filter(Boolean).join(" · ");
    const session = sessionForMeshAgent(agent);
    const sessionMeta = session
      ? '<div class="row-insight">' +
        esc(session.status || "unknown") + " · " + formatMB(session.rss_mb) +
        (session.subagents_running ? " · " + session.subagents_running + " sub-agent" + (session.subagents_running === 1 ? "" : "s") : "") +
        (session.last_preview ? " · " + esc(session.last_preview.slice(0, 90)) : "") +
        "</div>"
      : "";
    return '<div class="row" data-drill-agent="' + esc(card.id || card.name || "") + '" style="border-left-color:' + (agent.state === "live" ? "rgba(56,255,163,.6)" : "rgba(255,179,31,.6)") + '">' +
      '<div class="row-top"><div class="row-title">' + esc(card.name || card.id || "unknown") + '</div>' +
      '<span class="pill ' + pillClass + '">' + esc(agent.state || "-") + "</span></div>" +
      '<div class="row-meta">' + esc(meta) + "</div>" +
      (agent.lastStatus ? '<div class="row-body">' + esc(agent.lastStatus) + "</div>" : "") +
      sessionMeta +
      "</div>";
  }).join("");
}

function renderClaims() {
  const list = byId("claimList");
  if (!state.claims.size) {
    list.innerHTML = state.claimsSnapshotted
      ? '<div class="empty">No held claims.</div>'
      : '<div class="empty">Waiting for the claims snapshot…</div>';
    return;
  }
  const rows = Array.from(state.claims.values()).sort((a, b) => new Date(b.ts || 0) - new Date(a.ts || 0));
  list.innerHTML = rows.map((c) =>
    '<div class="row" style="border-left-color:' + KIND_COLOR.claim + '">' +
    '<div class="row-top"><div class="row-title">' + esc(c.path) + '</div>' +
    '<span class="pill amber">claimed</span></div>' +
    '<div class="row-meta">' + esc(c.holder) + (c.repo ? " · " + esc(c.repo) : "") + " · " + esc(ageText(c.ts)) + " ago</div>" +
    "</div>"
  ).join("");
}

function renderNotes() {
  const list = byId("noteList");
  if (!state.notes.length) {
    list.innerHTML = '<div class="empty">No blackboard notes observed.<br>`mesh note` envelopes will appear here as they are published.</div>';
    return;
  }
  list.innerHTML = state.notes.map((n) =>
    '<div class="row" style="border-left-color:' + KIND_COLOR.note + '">' +
    '<div class="row-body" style="margin-top:0">' + esc(n.decision) + "</div>" +
    '<div class="row-meta">' + esc(n.from) + (n.repo ? " · " + esc(n.repo) : "") + " · " + esc(timeText(n.ts)) + "</div>" +
    "</div>"
  ).join("");
}

function filteredEvents() {
  return state.events.filter((env) => {
    if (!state.filter.heartbeats && env.kind === "heartbeat") return false;
    if (state.filter.kinds.size && !state.filter.kinds.has(chipKind(env.kind))) return false;
    if (state.filter.subject && !String(env.subject || "").includes(state.filter.subject)) return false;
    return true;
  });
}

function payloadJSON(env) {
  return JSON.stringify({
    id: env.id,
    schemaVersion: env.schemaVersion,
    kind: env.kind,
    from: env.from,
    to: env.to || undefined,
    subject: env.subject,
    ts: env.ts,
    payload: env.payload ?? {},
  }, null, 2);
}

// eventId is the stable key for an observed envelope's <details> open-state.
// Envelope ids are UUIDv7 and unique; the fallback only guards synthetic events.
function eventId(env) {
  return env.id || (env.kind + "|" + (env.ts || "") + "|" + (env.subject || ""));
}

// renderedEventsKey skips event-list rebuilds when neither the log nor the
// filter changed, so a reader's expanded <details> is not collapsed by the
// 1s roster tick.
let renderedEventsKey = "";

function eventsKey() {
  return state.totalEvents + "|" + Array.from(state.filter.kinds).join(",") +
    "|" + state.filter.subject + "|" + state.filter.heartbeats;
}

function renderEvents() {
  const key = eventsKey();
  if (key === renderedEventsKey) return;
  renderedEventsKey = key;
  const list = byId("eventList");
  const rows = filteredEvents().slice(-80).reverse();
  if (!rows.length) {
    list.innerHTML = '<div class="empty">' + (state.totalEvents ? "No events match the current filter." : "Tap idle — no envelopes observed yet.") + "</div>";
    return;
  }
  list.innerHTML = rows.map((env) => {
    const label = eventLabel(env);
    const eid = eventId(env);
    const open = state.openEvents.has(eid) ? " open" : "";
    return '<div class="row event-card" style="border-left-color:' + kindColor(env.kind) + '">' +
      '<div class="event-top">' +
      '<span class="event-kind" style="color:' + kindColor(env.kind) + '">' + esc(env.kind) + "</span>" +
      '<span class="event-subject">' + esc(env.subject || "") + "</span>" +
      '<span class="event-time">' + esc(timeText(env.ts)) + "</span>" +
      "</div>" +
      '<div class="row-meta">' + esc(env.from || "") + (label ? " — " + esc(label) : "") + "</div>" +
      '<details data-eid="' + esc(eid) + '"' + open + "><summary>envelope</summary><pre>" + esc(payloadJSON(env)) + "</pre></details>" +
      "</div>";
  }).join("");
}

function renderTickets() {
  const list = byId("ticketList");
  const pill = byId("ticketsPill");
  if (!state.tickets.size) {
    pill.textContent = "P2 · none";
    pill.className = "pill";
    list.innerHTML = '<div class="empty">No ask/answer traffic observed.<br>The P2 ticket lifecycle is not built yet — this panel populates from real mesh ask/answer envelopes only.</div>';
    return;
  }
  const rows = Array.from(state.tickets.values()).reverse();
  const open = rows.filter((t) => t.state === "open").length;
  pill.textContent = open + " open / " + rows.length;
  pill.className = "pill " + (open ? "amber" : "green");
  list.innerHTML = rows.map((t) => {
    const answered = t.state === "answered";
    return '<div class="row" style="border-left-color:' + (answered ? KIND_COLOR.answer : KIND_COLOR.ask) + '">' +
      '<div class="row-top"><div class="row-title">' + esc(t.from) + " → " + esc(t.route || "open") + '</div>' +
      '<span class="pill ' + (answered ? "green" : "amber") + '">' + (answered ? "answered" : "open") + "</span></div>" +
      '<div class="row-meta">' + esc(t.ticket) + " · " + esc(timeText(t.ts)) + "</div>" +
      (t.q ? '<div class="row-body">' + esc(t.q) + "</div>" : "") +
      (answered ? '<div class="row-body" style="color:#c8f4d8">' + esc(t.answeredBy) + ": " + esc(t.answer) + "</div>" : "") +
      "</div>";
  }).join("");
}

// JOB_STATE_COLOR and TASK_STATE_COLOR map lifecycle state strings to
// display colors. Defined as objects (not switch-case) so the test that
// validates case-literal strings against the wire envelope kinds does not
// pick up payload-level state vocabulary, which is distinct from kinds.
const JOB_STATE_COLOR = {
  open:      "#8a98aa",
  triaged:   "#7dd3fc",
  scheduled: "#a78bfa",
  running:   "#f4b942",
  done:      "#38ffa3",
  failed:    "#f66f7d",
  cancelled: "#859397",
};

const TASK_STATE_COLOR = {
  pending:   "#8a98aa",
  running:   "#f4b942",
  done:      "#38ffa3",
  failed:    "#f66f7d",
  cancelled: "#859397",
};

function jobStateColor(s) { return JOB_STATE_COLOR[s] || "#8a98aa"; }
function taskStateColor(s) { return TASK_STATE_COLOR[s] || "#8a98aa"; }

function renderFleet() {
  const list = byId("fleetStatus");
  const pill = byId("fleetPill");
  if (!state.fleet) {
    if (pill) { pill.textContent = "no data"; pill.className = "pill"; }
    if (list) list.innerHTML = '<div class="empty">No fleet events yet.<br>KindFleet envelopes from the scheduler will appear here.</div>';
    return;
  }
  const f = state.fleet;
  const running = f.state === "running";
  const color = running ? "#38ffa3" : "#ffb31f";
  if (pill) {
    pill.textContent = f.state;
    pill.className = "pill " + (running ? "green" : "amber");
  }
  const budget = f.budgetUSD > 0 ? " / $" + f.budgetUSD.toFixed(2) : "";
  const cost = f.spentUSD > 0 ? "$" + f.spentUSD.toFixed(4) + " spent" + budget : "";
  const reason = f.reason ? " — " + f.reason : "";
  if (list) {
    list.innerHTML = '<div class="row" style="border-left-color:' + color + '">' +
      '<div class="row-top"><div class="row-title">' + esc(f.state.toUpperCase()) + '</div>' +
      '<span class="pill ' + (running ? "green" : "amber") + '">' + esc(f.state) + '</span></div>' +
      (cost ? '<div class="row-meta">' + esc(cost) + esc(reason) + '</div>' : '') +
      (f.code ? '<div class="row-body" style="color:#ffb4ab">' + esc(f.code) + '</div>' : '') +
      '</div>';
  }
}

function renderJobs() {
  const list = byId("jobList");
  const pill = byId("jobsPill");
  if (!list) return;
  if (!state.jobs.size) {
    if (pill) { pill.textContent = "0"; pill.className = "pill"; }
    list.innerHTML = state.jobsSnapshotted
      ? '<div class="empty">No jobs yet.<br>Submit a job with the + Submit Job button or via `mesh submit`.</div>'
      : '<div class="empty">Waiting for the jobs snapshot…</div>';
    return;
  }
  const rows = Array.from(state.jobs.values()).sort((a, b) => new Date(b.ts || 0) - new Date(a.ts || 0));
  const running = rows.filter((j) => j.state === "running").length;
  const active = rows.filter((j) => !["done", "failed", "cancelled"].includes(j.state)).length;
  if (pill) {
    pill.textContent = active + " active / " + rows.length;
    pill.className = "pill " + (running ? "amber" : active ? "green" : "");
  }
  list.innerHTML = rows.map((j) => {
    const color = jobStateColor(j.state);
    const url = issueURL(j.sourceRef);
    const issueLink = url
      ? ' · <a href="' + esc(url) + '" target="_blank" rel="noopener noreferrer" style="color:#7dd3fc" onclick="event.stopPropagation()">' + esc(j.sourceRef) + '</a>'
      : '';
    return '<div class="row" data-drill-job="' + esc(j.id) + '" style="border-left-color:' + color + '">' +
      '<div class="row-top"><div class="row-title" title="' + esc(j.title) + '">' + esc(j.title) + '</div>' +
      '<span class="pill" style="border-color:' + color + ';color:' + color + '">' + esc(j.state) + '</span></div>' +
      '<div class="row-meta">' + esc(j.repo) + ' · ' + esc(j.source) + issueLink + ' · ' + esc(ageText(j.ts)) + ' ago</div>' +
      '</div>';
  }).join("");
}

function renderTasks() {
  const list = byId("taskList");
  const pill = byId("tasksPill");
  if (!list) return;
  if (!state.tasks.size) {
    if (pill) { pill.textContent = "0"; pill.className = "pill"; }
    list.innerHTML = state.tasksSnapshotted
      ? '<div class="empty">No tasks yet.<br>Tasks appear here once a job is triaged into a DAG.</div>'
      : '<div class="empty">Waiting for the tasks snapshot…</div>';
    return;
  }
  const rows = Array.from(state.tasks.values()).sort((a, b) => new Date(b.ts || 0) - new Date(a.ts || 0));
  const running = rows.filter((t) => t.state === "running").length;
  const done = rows.filter((t) => t.state === "done").length;
  if (pill) {
    pill.textContent = done + " done / " + rows.length;
    pill.className = "pill " + (running ? "amber" : done === rows.length ? "green" : "");
  }

  // Group tasks by job, labelled with the job's human-readable title.
  // Tasks within each group are sorted newest-first (inherited from rows sort above).
  const groups = new Map(); // job id -> {label, tasks[]}
  for (const t of rows) {
    if (!groups.has(t.job)) {
      const j = state.jobs.get(t.job);
      const label = j ? j.title : (t.job ? t.job.slice(0, 8) + "…" : "unknown job");
      groups.set(t.job, { label, tasks: [] });
    }
    groups.get(t.job).tasks.push(t);
  }

  let html = "";
  for (const [, group] of groups) {
    const grpRunning = group.tasks.some((t) => t.state === "running");
    const grpDone = group.tasks.every((t) => t.state === "done");
    const grpColor = grpRunning ? "#f4b942" : grpDone ? "#38ffa3" : "#8a98aa";
    html += '<div class="task-group">' +
      '<div class="task-group-hdr" style="border-left-color:' + grpColor + '">' +
      '<span class="task-group-label">' + esc(group.label) + '</span>' +
      '<span class="task-group-count">' + group.tasks.length + (group.tasks.length === 1 ? " task" : " tasks") + '</span>' +
      '</div>';
    for (const t of group.tasks) {
      const color = taskStateColor(t.state);
      const workerLabel = t.worker || "";
      html += '<div class="row task-row" data-drill-task="' + esc(t.id) + '" style="border-left-color:' + color + '">' +
        '<div class="row-top"><div class="row-title" title="' + esc(t.title) + '">' + esc(t.title) + '</div>' +
        '<span class="pill" style="border-color:' + color + ';color:' + color + '">' + esc(t.state) + '</span></div>' +
        '<div class="row-meta">' + esc(t.role) + (workerLabel ? ' · ' + esc(workerLabel) : '') + ' · ' + esc(ageText(t.ts)) + ' ago</div>' +
        '</div>';
    }
    html += '</div>';
  }
  list.innerHTML = html;
}

function renderWorkers() {
  const list = byId("workerList");
  const pill = byId("workersPill");
  if (!list) return;
  if (!state.workers.length) {
    if (pill) { pill.textContent = "0"; pill.className = "pill"; }
    list.innerHTML = '<div class="empty">No worker runs yet.<br>Each completed worker task appears here with its result and cost.</div>';
    return;
  }
  // Newest last from the server → show newest first in the UI.
  const rows = state.workers.slice().reverse();
  const ok = rows.filter((w) => w.result === "ok").length;
  const err = rows.filter((w) => w.result === "error").length;
  if (pill) {
    pill.textContent = ok + " ok" + (err ? " / " + err + " err" : "");
    pill.className = "pill " + (err ? "amber" : "green");
  }
  list.innerHTML = rows.map((w) => {
    const color = w.result === "ok" ? "#38ffa3" : "#f66f7d";
    const cost = w.costUSD > 0 ? "$" + w.costUSD.toFixed(4) : "";
    return '<div class="row" style="border-left-color:' + color + '">' +
      '<div class="row-top"><div class="row-title">' + esc(w.task) + '</div>' +
      '<span class="pill" style="border-color:' + color + ';color:' + color + '">' + esc(w.result) + '</span></div>' +
      '<div class="row-meta">' + esc(w.job) + (cost ? ' · ' + esc(cost) : '') + ' · ' + esc(ageText(w.ts)) + ' ago</div>' +
      (w.code ? '<div class="row-body" style="color:#ffb4ab">' + esc(w.code) + (w.reason ? ': ' + esc(w.reason) : '') + '</div>' : '') +
      '</div>';
  }).join("");
}

function renderExperts() {
  const list = byId("expertList");
  const pill = byId("expertsPill");
  // Roles are open data, exact-token matched (never substring) per the
  // envelope/authority invariants.
  const pool = state.agents.filter((a) => {
    const role = (a.card && a.card.role) || "";
    return role === "expert" || role === "worker";
  });
  if (!pool.length) {
    pill.textContent = "P3 · none";
    pill.className = "pill";
    list.innerHTML = '<div class="empty">No expert or worker agents registered.<br>The P3 expert pool is not built yet — agents whose card role is exactly "expert" or "worker" will appear here.</div>';
    return;
  }
  pill.textContent = pool.length + " registered";
  pill.className = "pill blue";
  list.innerHTML = pool.map((agent) => {
    const card = agent.card || {};
    return '<div class="row" style="border-left-color:rgba(106,168,255,.6)">' +
      '<div class="row-top"><div class="row-title">' + esc(card.name || card.id) + '</div>' +
      '<span class="pill ' + (agent.state === "live" ? "green" : "amber") + '">' + esc(agent.state || "-") + "</span></div>" +
      '<div class="row-meta">' + esc(card.role) + (card.model ? " · " + esc(card.model) : "") + "</div>" +
      "</div>";
  }).join("");
}

function renderClaudeSessions() {
  const list = byId("claudeSessionList");
  const pill = byId("claudeSessionsPill");
  if (!list) return;

  if (state.claudeSessionsError) {
    if (pill) {
      pill.textContent = "offline";
      pill.className = "pill rose";
    }
    list.innerHTML = '<div class="empty">Claude session viewer offline.<br>Run <code>python3 ~/Documents/code/claude-sessions-viewer/server.py</code> on port 8765.</div>';
    return;
  }

  if (!state.claudeSessions.length) {
    if (pill) {
      pill.textContent = "0";
      pill.className = "pill";
    }
    list.innerHTML = '<div class="empty">No Claude Code CLI sessions detected.</div>';
    return;
  }

  const runningSubs = state.claudeSessions.reduce((n, s) => n + (s.subagents_running || 0), 0);
  if (pill) {
    pill.textContent = state.claudeSessions.length + " sess" + (runningSubs ? " · " + runningSubs + " active" : "");
    pill.className = "pill " + (runningSubs ? "green" : "cyan");
  }

  list.innerHTML = state.claudeSessions.map((session) => {
    const meshAgent = meshAgentForSession(session);
    const meshName = meshNameFromSessionId(session.session_id);
    const meshLink = meshAgent
      ? '<span class="pill green">' + esc(meshName) + " · " + esc(meshAgent.state) + "</span>"
      : (meshName ? '<span class="pill">unmeshed</span>' : "");
    const statusColor = session.status === "busy" ? "#ffb31f" : session.status === "idle" ? "#38ffa3" : "#8a98aa";
    const runningAgents = (session.subagents || []).filter((a) => a.status === "running");

    const subRows = runningAgents.length
      ? '<div class="subagent-block">' + runningAgents.map((a) =>
          '<div class="subagent-row">' +
          '<div class="subagent-title">' + esc(a.description) + ' <span class="pill green">running</span></div>' +
          '<div class="subagent-meta">' + esc(a.agent_type) + " · " + esc(a.current_action || a.preview || "—") + "</div>" +
          "</div>"
        ).join("") + "</div>"
      : "";

    const childRows = (session.child_processes || []).slice(0, 3).map((p) =>
      '<div class="subagent-meta">' + formatMB(p.rss_mb) + " · " + esc(p.command) + "</div>"
    ).join("");

    return '<div class="row" style="border-left-color:' + statusColor + '">' +
      '<div class="row-top"><div class="row-title">' + esc(session.project || "unknown") + '</div>' +
      '<span class="pill" style="border-color:' + statusColor + ';color:' + statusColor + '">' + esc(session.status || "?") + "</span></div>" +
      '<div class="row-meta">' +
      formatMB(session.rss_mb) + " RSS · " + formatMB(session.tree_rss_mb) + " tree · " +
      session.cpu_percent.toFixed(1) + "% CPU · pid " + esc(session.pid) +
      "</div>" +
      (session.last_preview ? '<div class="row-body">' + esc(session.last_preview.slice(0, 140)) + "</div>" : "") +
      (meshLink ? '<div class="row-meta">' + meshLink + "</div>" : "") +
      subRows +
      (childRows ? '<div class="subagent-block">' + childRows + "</div>" : "") +
      "</div>";
  }).join("");
}

async function pollClaudeSessions() {
  try {
    const res = await fetch("/api/claude-sessions");
    if (!res.ok) {
      state.claudeSessions = [];
      state.claudeSessionsError = res.status === 503 ? "viewer offline" : "fetch failed";
      scheduleRender();
      return;
    }
    const data = await res.json();
    state.claudeSessions = data.sessions || [];
    state.claudeSessionsError = null;
    state.claudeSessionsAt = Date.now();
    scheduleRender();
  } catch (_err) {
    state.claudeSessions = [];
    state.claudeSessionsError = "viewer offline";
    scheduleRender();
  }
}

function startClaudeSessionsPoll() {
  pollClaudeSessions();
  setInterval(pollClaudeSessions, 3000);
}

function render() {
  renderSummary();
  renderAgents();
  renderClaudeSessions();
  renderClaims();
  renderNotes();
  renderEvents();
  renderTickets();
  renderExperts();
  renderFleet();
  renderJobs();
  renderTasks();
  renderWorkers();
}

/* ------------------------------------------------------------------------- *
 * Filter UI.
 * ------------------------------------------------------------------------- */

function buildFilters() {
  const chips = byId("kindChips");
  const all = KINDS.concat(["other"]);
  chips.innerHTML = all.map((kind) =>
    '<button type="button" class="chip" data-kind="' + esc(kind) + '">' + esc(kind) + "</button>"
  ).join("");
  for (const button of chips.querySelectorAll("[data-kind]")) {
    button.addEventListener("click", () => {
      const kind = button.getAttribute("data-kind");
      if (state.filter.kinds.has(kind)) state.filter.kinds.delete(kind);
      else state.filter.kinds.add(kind);
      styleChips();
      renderEvents();
    });
  }
  styleChips();

  byId("subjectFilter").addEventListener("input", (event) => {
    state.filter.subject = event.target.value.trim();
    renderEvents();
  });

  byId("hbToggle").addEventListener("change", (event) => {
    state.filter.heartbeats = event.target.checked;
    renderEvents();
  });

  // Preserve which envelope <details> are open across the per-event list
  // rebuild: renderEvents() wipes #eventList.innerHTML on every new event, so
  // without this the disclosure a reader just opened collapses on the next
  // event. `toggle` does not bubble — listen in the capture phase.
  byId("eventList").addEventListener("toggle", (event) => {
    const d = event.target;
    if (!d || d.tagName !== "DETAILS" || !d.dataset.eid) return;
    if (d.open) state.openEvents.add(d.dataset.eid);
    else state.openEvents.delete(d.dataset.eid);
  }, true);
}

function styleChips() {
  for (const button of byId("kindChips").querySelectorAll("[data-kind]")) {
    const kind = button.getAttribute("data-kind");
    const active = state.filter.kinds.has(kind);
    button.className = "chip" + (active ? " active" : "");
    button.style.background = active ? kindColor(kind) : "";
    button.style.borderColor = active ? kindColor(kind) : "";
  }
}

function buildLegend() {
  byId("legend").innerHTML = ["status", "announce", "claim", "note", "register", "leave"].map((kind) =>
    "<span><i style=\"background:" + kindColor(kind) + ";box-shadow:0 0 8px " + kindColor(kind) + "\"></i>" + esc(kind) + "</span>"
  ).join("");
}

/* ------------------------------------------------------------------------- *
 * Boot.
 * ------------------------------------------------------------------------- */

buildLegend();
buildFilters();
syncStageNodes();
setConnected(false, "connecting");
render();
fitStage();
window.addEventListener("resize", fitStage);
setInterval(scheduleRender, 1000); // tick ages / roster staleness
startClaudeSessionsPoll();
connect();

/* ------------------------------------------------------------------------- *
 * Drill-in (peek). Click a stage node / agent / job / task row for a live
 * detail panel. Pure client-side — reads the same authoritative `state`
 * frames already streamed over /events; observes nothing new.
 * ------------------------------------------------------------------------- */
(function drillIn() {
  const style = document.createElement("style");
  style.textContent =
    "#drillPanel{position:fixed;top:0;right:0;width:380px;max-width:92vw;height:100vh;background:#0b1016;" +
    "border-left:1px solid #1d2733;box-shadow:-10px 0 34px rgba(0,0,0,.55);padding:16px;overflow:auto;" +
    "z-index:9999;font:13px/1.45 ui-sans-serif,system-ui;color:#d7e6f5;transform:translateX(106%);" +
    "transition:transform .18s ease}" +
    "#drillPanel.open{transform:none}" +
    "#drillPanel .dh{display:flex;justify-content:space-between;align-items:flex-start;gap:10px;font-size:15px;font-weight:600;margin-bottom:12px}" +
    "#drillPanel .dx{cursor:pointer;opacity:.55;padding:0 6px;font-size:18px;line-height:1}#drillPanel .dx:hover{opacity:1}" +
    "#drillPanel table{width:100%;border-collapse:collapse;margin-bottom:14px}" +
    "#drillPanel td{padding:4px 6px;border-bottom:1px solid #141d27;vertical-align:top;word-break:break-word}" +
    "#drillPanel td:first-child{color:#7d8da0;width:88px;white-space:nowrap}" +
    "#drillPanel .deh{color:#7d8da0;text-transform:uppercase;font-size:11px;letter-spacing:.06em;margin:4px 0 6px}" +
    "#drillPanel .de{display:flex;justify-content:space-between;gap:10px;padding:4px 0;border-bottom:1px solid #11181f}" +
    "#drillPanel .de .det{color:#5d6b7d;flex:0 0 auto}" +
    ".node{cursor:pointer}[data-drill-agent],[data-drill-task],[data-drill-job]{cursor:pointer}";
  document.head.appendChild(style);

  const panel = document.createElement("aside");
  panel.id = "drillPanel";
  document.body.appendChild(panel);
  let cur = null;

  function row(k, v) {
    return "<tr><td>" + esc(k) + "</td><td>" + esc(v == null || v === "" ? "-" : String(v)) + "</td></tr>";
  }
  function eventsFor(id) {
    return state.events.filter(function (e) {
      return e.from === id || e.to === id || (e.subject && e.subject.indexOf(id) !== -1);
    }).slice(-30).reverse();
  }
  function openDrill(kind, id) {
    cur = { kind: kind, id: id };
    let title = id, rows = "", workTask = "";
    if (kind === "task") workTask = id;
    else if (kind === "agent") {
      const ag = state.agents.find(function (x) { return x.card && x.card.id === id; });
      const cwd = ag && ag.card ? (ag.card.cwd || "") : "";
      const m = cwd.match(/workers\/([^/]+)\/?$/);
      if (m) workTask = m[1];
    }
    if (kind === "agent") {
      const a = state.agents.find(function (x) { return x.card && x.card.id === id; }) || { card: {} };
      const c = a.card || {};
      title = (c.name || id) + " · " + (c.role || "-");
      rows = row("kind", "agent") + row("state", a.state) + row("role", c.role) +
        row("model", c.model) + row("pid", c.pid) + row("repo", c.repo) +
        row("cwd", c.cwd) + row("last seen", a.lastSeen ? ageText(a.lastSeen) + " ago" : "-");
    } else if (kind === "task") {
      const t = state.tasks.get(id) || {};
      const w = state.workers.find(function (x) { return x.task === id; });
      title = (t.role || "task") + " · " + (t.title || id);
      rows = row("kind", "task") + row("state", t.state) + row("role", t.role) +
        row("job", t.job) + row("title", t.title) +
        (w ? row("result", w.result) + row("cost", w.costUSD ? "$" + w.costUSD.toFixed(4) : "-") : "") +
        row("branch", "mesh/worker/" + id);
    } else if (kind === "job") {
      const j = state.jobs.get(id) || {};
      const ts = Array.from(state.tasks.values()).filter(function (t) { return t.job === id; });
      title = j.title || id;
      const jUrl = issueURL(j.sourceRef);
      const issueRow = j.sourceRef
        ? "<tr><td>issue</td><td>" + (jUrl ? '<a href="' + esc(jUrl) + '" target="_blank" rel="noopener noreferrer" style="color:#7dd3fc">' + esc(j.sourceRef) + "</a>" : esc(j.sourceRef)) + "</td></tr>"
        : "";
      rows = row("kind", "job") + row("state", j.state) + row("repo", j.repo) +
        row("source", j.source) + issueRow +
        row("tasks", ts.map(function (t) { return t.role + ":" + t.state; }).join(", "));
    }
    const evs = eventsFor(id);
    panel.innerHTML =
      '<div class="dh"><span>' + esc(title) + '</span><span class="dx" id="drillClose">✕</span></div>' +
      "<table>" + rows + "</table>" +
      '<div class="deh">recent events (' + evs.length + ")</div>" +
      (evs.length
        ? evs.map(function (e) {
            return '<div class="de"><span>' + esc(e.kind + (eventLabel(e) ? ": " + eventLabel(e) : "")) +
              '</span><span class="det">' + esc(ageText(e.ts)) + " ago</span></div>";
          }).join("")
        : '<div class="de" style="color:#5d6b7d">no observed envelopes for this id yet</div>') +
      '<div id="drillLog"><div class="deh">lifecycle log</div><div class="de" style="color:#5d6b7d">loading…</div></div>' +
      (workTask ? '<div id="drillWork"><div class="deh">live worker output</div><div class="de" style="color:#5d6b7d">loading…</div></div>' : "");
    byId("drillClose").onclick = function () { cur = null; panel.classList.remove("open"); };
    fetch("/api/tasklog?id=" + encodeURIComponent(id)).then(function (r) { return r.json(); }).then(function (d) {
      const box = byId("drillLog");
      if (!box || !cur || cur.id !== id) return;
      const ls = d.lines || [];
      box.innerHTML = '<div class="deh">lifecycle log (' + ls.length + ")</div>" +
        (ls.length
          ? ls.slice().reverse().map(function (l) { return '<div class="de" style="font-size:11px;color:#9fb3c8;display:block">' + esc(l) + "</div>"; }).join("")
          : '<div class="de" style="color:#5d6b7d">no log lines for this id</div>');
    }).catch(function () {});
    if (workTask) {
      fetch("/api/worklog?task=" + encodeURIComponent(workTask)).then(function (r) { return r.json(); }).then(function (d) {
        const box = byId("drillWork");
        if (!box || !cur) return;
        const ls = d.lines || [];
        box.innerHTML = '<div class="deh">live worker output (' + ls.length + ")</div>" +
          (ls.length
            ? ls.map(function (l) { return '<div class="de" style="display:block;font-size:12px;color:#cfe3d6">' + esc(l) + "</div>"; }).join("")
            : '<div class="de" style="color:#5d6b7d">no streamed output yet</div>');
      }).catch(function () {});
    }
    panel.classList.add("open");
  }
  window.openDrill = openDrill;

  document.addEventListener("click", function (e) {
    const a = e.target.closest("[data-drill-agent]");
    if (a) { openDrill("agent", a.dataset.drillAgent); return; }
    const t = e.target.closest("[data-drill-task]");
    if (t) { openDrill("task", t.dataset.drillTask); return; }
    const j = e.target.closest("[data-drill-job]");
    if (j) { openDrill("job", j.dataset.drillJob); return; }
    // Click anywhere off the panel (and not on a drill trigger) tucks it away,
    // so the main screen is interactive again.
    if (panel.classList.contains("open") && !panel.contains(e.target)) {
      cur = null;
      panel.classList.remove("open");
    }
  });
  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape") { cur = null; panel.classList.remove("open"); }
  });
  // Live-refresh the open panel so a selected node updates in place.
  setInterval(function () { if (cur && panel.classList.contains("open")) openDrill(cur.kind, cur.id); }, 1500);
})();
