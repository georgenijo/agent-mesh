// settings.js — v1 fleet settings screen (settings.html).
//
// Reads GET /api/settings (three-column state: effective / staged / default),
// renders one row per knob grouped by operator workflow, and stages changes via
// POST /api/settings — the single settings authority, token-gated exactly like
// the job write path (jobform.js). No credential/API-key field ever.
"use strict";

(function () {

// Write-API token (loopback-only, same origin), fetched once like jobform.js.
let writeToken = null;
function loadWriteToken() {
  fetch("/api/write-token")
    .then(r => r.ok ? r.json() : Promise.reject(r.status))
    .then(data => { writeToken = data.token || ""; })
    .catch(() => { writeToken = ""; })
    .finally(syncSubmit);
}

// Per-field coercion type.
const FIELD_TYPE = {
  budgetUSD: "float", maxWorkers: "int", reDispatchBackoff: "str",
  workerCLI: "str", workerModel: "model", plannerCLI: "str", plannerModel: "model",
  expertCLI: "str", workerTimeout: "str", triageTimeout: "str", triageBackoff: "str",
  triageMaxAttempts: "int", reviewRole: "role", reviewPoolSize: "int", reviewRetries: "int",
  reviewTimeout: "str", keepWorktrees: "enum:on-failure,always,never", autoExperts: "bool",
  auditFanout: "bool", expertIdleTTL: "str", jobsAddr: "str",
  heartbeatInterval: "str", awayAfter: "str", evictAfter: "str", claimTTL: "str",
  dashboardAddr: "str", observeAddr: "str",
};

// Sections top-to-bottom by mid-run need.
const SECTIONS = [
  { title: "Budget & Throughput", hint: "Live-raising the budget avoids resetting spend-to-date.",
    fields: ["budgetUSD", "maxWorkers", "reDispatchBackoff", "workerTimeout"] },
  { title: "Review Gating", hint: "Review role accepts any live role token (free-text, exact match).",
    fields: ["reviewRole", "reviewPoolSize", "reviewRetries", "reviewTimeout"] },
  { title: "Experts", hint: "Auto-experts arms unattended spawning on EVERY coordinator sharing this MESH_DIR.",
    fields: ["autoExperts", "expertIdleTTL"] },
  { title: "Models & CLIs", hint: "Worker/planner models are three-state: Unset · CLI-default (empty) · Pinned.",
    fields: ["workerCLI", "workerModel", "plannerCLI", "plannerModel", "expertCLI"],
    readonly: [
      { label: "Expert model", note: "not pinnable yet — plumbing gap (EnvExpertModel + runtime.Options.Model)" },
      { label: "Reviewer model", note: "not pinnable yet — same plumbing gap as expert model" },
    ] },
  { title: "Triage", hint: "",
    fields: ["triageTimeout", "triageBackoff", "triageMaxAttempts"] },
  { title: "Worktrees & Audit", hint: "",
    fields: ["keepWorktrees", "auditFanout"] },
];
const ADVANCED = { title: "Presence & Network", hint: "Restart-fleet: every daemon must restart. Validated by the shared config invariants.",
  fields: ["heartbeatInterval", "awayAfter", "evictAfter", "claimTTL", "dashboardAddr", "observeAddr"] };

// Live state from the last GET/SSE.
let state = { meta: [], metaByField: {}, effective: null, staged: null, defaults: {}, stagedRev: 0, envOverridden: [], lastRejection: null };
// initial rendered value per field, for change detection.
let initial = {};

const sectionsEl = document.getElementById("setSections");
const submitEl = document.getElementById("setSubmit");
const statusEl = document.getElementById("setStatus");
const divergeEl = document.getElementById("divergeBanner");
const envBannerEl = document.getElementById("envBanner");
const armModal = document.getElementById("armModal");
const armBody = document.getElementById("armModalBody");

function load() {
  fetch("/api/settings")
    .then(r => r.ok ? r.json() : Promise.reject(r.status))
    .then(applyState)
    .catch(() => setStatus("Failed to load settings.", "err"));
}

function applyState(s) {
  state.meta = s.meta || [];
  state.metaByField = {};
  state.meta.forEach(m => { state.metaByField[m.field] = m; });
  state.effective = s.effective || null;
  state.staged = s.staged || null;
  state.defaults = s.defaults || {};
  state.stagedRev = s.stagedRev || 0;
  state.envOverridden = s.envOverridden || [];
  state.lastRejection = s.lastRejection || null;
  render();
}

function envLocked(field) {
  const m = state.metaByField[field];
  return m && state.envOverridden.indexOf(m.envName) !== -1;
}

// esc HTML-escapes a value before it is interpolated into innerHTML. Settings
// values are operator/coordinator-controlled free text (workerModel, jobsAddr,
// …) and several string knobs are not charset-validated in settings.ValidateRecord,
// so an unescaped value is a stored-XSS vector: a payload survives to storage,
// self-propagates via the coordinator's effective-settings broadcast, and would
// execute in the dashboard origin. Mirror app.js's esc() (settings.html does not
// load app.js, so this IIFE carries its own copy).
function esc(value) {
  return String(value == null ? "" : value).replace(/[&<>"']/g, (ch) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;",
  }[ch]));
}

function colVal(src, field) {
  if (!src) return "—";
  const v = src[field];
  if (v === undefined || v === null) return "—";
  if (v === "") return "«empty»";
  return esc(String(v));
}

function stagedRaw(field) {
  // undefined = not staged; may be "" (empty) which is meaningful.
  if (!state.staged) return undefined;
  return state.staged[field];
}

function render() {
  sectionsEl.innerHTML = "";
  initial = {};
  SECTIONS.forEach(sec => sectionsEl.appendChild(renderSection(sec)));
  // Advanced collapsed.
  const det = document.createElement("details");
  det.className = "set-advanced";
  const sum = document.createElement("summary");
  sum.textContent = "Advanced — " + ADVANCED.title;
  det.appendChild(sum);
  det.appendChild(renderSection(ADVANCED));
  sectionsEl.appendChild(det);

  renderBanners();
  syncSubmit();
}

function renderSection(sec) {
  const wrap = document.createElement("section");
  wrap.className = "set-section";
  const h = document.createElement("h2");
  h.textContent = sec.title;
  wrap.appendChild(h);
  if (sec.hint) { const p = document.createElement("p"); p.className = "hint"; p.textContent = sec.hint; wrap.appendChild(p); }

  const scroll = document.createElement("div");
  scroll.className = "set-scroll";
  const table = document.createElement("table");
  table.className = "set-table";
  table.innerHTML = "<thead><tr><th>Knob</th><th>Effective</th><th>Staged</th><th>Default</th><th>Value</th></tr></thead>";
  const tb = document.createElement("tbody");
  sec.fields.forEach(f => tb.appendChild(renderRow(f)));
  (sec.readonly || []).forEach(r => tb.appendChild(renderReadonlyRow(r)));
  table.appendChild(tb);
  scroll.appendChild(table);
  wrap.appendChild(scroll);
  return wrap;
}

function badges(field) {
  const m = state.metaByField[field];
  if (!m) return "";
  let out = '<span class="badge ' + m.applyClass + '">' + applyLabel(m.applyClass) + "</span>";
  if (m.arming) out += '<span class="badge arm">arm</span>';
  return out;
}
function applyLabel(c) {
  if (c === "hot") return "applies now";
  if (c === "restart-coordinator") return "restart";
  return "fleet restart";
}

function renderRow(field) {
  const tr = document.createElement("tr");
  const lock = envLocked(field);
  const m = state.metaByField[field] || {};
  const lockIcon = lock ? ' <span class="envlock" title="pinned by env var ' + esc(m.envName) + '">🔒</span>' : "";
  tr.innerHTML =
    '<td><div class="set-label">' + esc(m.label || field) + lockIcon + "</div>" + badges(field) + "</td>" +
    '<td class="set-col eff">' + colVal(state.effective, field) + "</td>" +
    '<td class="set-col">' + colVal(state.staged, field) + "</td>" +
    '<td class="set-col">' + colVal(state.defaults, field) + "</td>";
  const td = document.createElement("td");
  td.appendChild(renderControl(field, lock));
  tr.appendChild(td);
  return tr;
}

function renderReadonlyRow(r) {
  const tr = document.createElement("tr");
  tr.innerHTML =
    '<td><div class="set-label">' + r.label + "</div></td>" +
    '<td class="set-col">—</td><td class="set-col">—</td><td class="set-col">—</td>' +
    '<td><input class="set-input" disabled placeholder="' + r.note + '"><div class="set-row-note">' + r.note + "</div></td>";
  return tr;
}

function renderControl(field, lock) {
  const type = FIELD_TYPE[field] || "str";
  const container = document.createElement("div");
  if (type === "bool") {
    const sel = mkSelect(field, ["", "on", "off"], boolInitial(field), lock);
    initial[field] = sel.value;
    container.appendChild(sel);
  } else if (type.indexOf("enum:") === 0) {
    const opts = [""].concat(type.slice(5).split(","));
    const cur = pickInitial(field);
    const sel = mkSelect(field, opts, cur, lock);
    initial[field] = sel.value;
    container.appendChild(sel);
  } else if (type === "model") {
    container.appendChild(renderModelControl(field, lock));
  } else {
    const inp = document.createElement("input");
    inp.className = "set-input";
    inp.id = "f-" + field;
    inp.value = pickInitial(field);
    inp.disabled = lock;
    inp.addEventListener("input", () => { validateField(field, inp); syncSubmit(); });
    initial[field] = inp.value;
    container.appendChild(inp);
  }
  return container;
}

// Three-state model control: mode (unset|cli-default|pinned) + text.
function renderModelControl(field, lock) {
  const box = document.createElement("div");
  const raw = stagedRaw(field);
  let mode = "unset", pinned = "";
  if (raw !== undefined) { if (raw === "") mode = "cli-default"; else { mode = "pinned"; pinned = raw; } }
  const sel = document.createElement("select");
  sel.className = "set-select mode-pill";
  sel.id = "m-" + field;
  [["unset", "Unset (fall through)"], ["cli-default", "CLI default (empty)"], ["pinned", "Pinned"]].forEach(o => {
    const opt = document.createElement("option"); opt.value = o[0]; opt.textContent = o[1]; sel.appendChild(opt);
  });
  sel.value = mode;
  sel.disabled = lock;
  const inp = document.createElement("input");
  inp.className = "set-input";
  inp.id = "f-" + field;
  inp.value = pinned;
  inp.placeholder = "e.g. opus";
  inp.style.marginTop = "5px";
  inp.disabled = lock || mode !== "pinned";
  sel.addEventListener("change", () => { inp.disabled = lock || sel.value !== "pinned"; syncSubmit(); });
  inp.addEventListener("input", syncSubmit);
  initial["m-" + field] = mode;
  initial["f-" + field] = pinned;
  box.appendChild(sel);
  box.appendChild(inp);
  return box;
}

function mkSelect(field, opts, val, lock) {
  const sel = document.createElement("select");
  sel.className = "set-select";
  sel.id = "f-" + field;
  opts.forEach(o => { const opt = document.createElement("option"); opt.value = o; opt.textContent = o === "" ? "(unchanged)" : o; sel.appendChild(opt); });
  sel.value = val;
  sel.disabled = lock;
  sel.addEventListener("change", syncSubmit);
  return sel;
}

function boolInitial(field) {
  const raw = stagedRaw(field);
  if (raw === true) return "on";
  if (raw === false) return "off";
  return "";
}

// Non-model, non-bool initial value: staged if present, else "".
function pickInitial(field) {
  const raw = stagedRaw(field);
  if (raw === undefined || raw === null) return "";
  return String(raw);
}

function validateField(field, inp) {
  const type = FIELD_TYPE[field] || "str";
  let ok = true;
  const v = inp.value.trim();
  if (v === "") { inp.classList.remove("invalid"); return; }
  if (type === "int") ok = /^-?\d+$/.test(v);
  else if (type === "float") ok = /^-?\d+(\.\d+)?$/.test(v);
  else if (type === "role") ok = /^[A-Za-z0-9_-]{1,48}$/.test(v);
  inp.classList.toggle("invalid", !ok);
}

// Collect the changed fields into a record + list of changed field keys.
function collect() {
  const rec = {};
  const changed = [];
  Object.keys(FIELD_TYPE).forEach(field => {
    if (envLocked(field)) return;
    const type = FIELD_TYPE[field];
    if (type === "model") {
      const mode = (document.getElementById("m-" + field) || {}).value;
      const txt = ((document.getElementById("f-" + field) || {}).value || "").trim();
      const im = initial["m-" + field], it = initial["f-" + field];
      if (mode === im && (mode !== "pinned" || txt === it)) return; // unchanged
      changed.push(field);
      if (mode === "unset") return;              // omit → fall through
      rec[field] = mode === "cli-default" ? "" : txt;
      return;
    }
    const el = document.getElementById("f-" + field);
    if (!el) return;
    const cur = (el.value || "").trim();
    if (cur === (initial[field] || "")) return;  // unchanged
    if (type === "bool") { if (cur === "") return; changed.push(field); rec[field] = (cur === "on"); return; }
    if (type.indexOf("enum:") === 0) { if (cur === "") return; changed.push(field); rec[field] = cur; return; }
    if (cur === "") return; // clearing a value is a no-op stage in v1
    changed.push(field);
    if (type === "int") rec[field] = parseInt(cur, 10);
    else if (type === "float") rec[field] = parseFloat(cur);
    else rec[field] = cur;
  });
  return { rec, changed };
}

function armingIn(changed) {
  return changed.filter(f => state.metaByField[f] && state.metaByField[f].arming);
}

function syncSubmit() {
  if (!submitEl) return;
  const { changed } = collect();
  submitEl.disabled = writeToken === null || changed.length === 0;
}

function setStatus(msg, cls) { statusEl.textContent = msg; statusEl.className = cls || ""; }

function renderBanners() {
  // Divergence: any staged value that differs from effective.
  let diverge = [];
  if (state.staged && state.effective) {
    Object.keys(FIELD_TYPE).forEach(f => {
      const s = state.staged[f];
      if (s === undefined || s === null) return;
      if (String(s) !== String(state.effective[f])) diverge.push(f);
    });
  }
  if (diverge.length) {
    divergeEl.textContent = "Staged differs from effective for: " + diverge.join(", ") +
      " — restart-class changes need `mesh ops down && mesh up` to take effect.";
    divergeEl.classList.remove("hidden");
  } else {
    divergeEl.classList.add("hidden");
  }
  if (state.envOverridden.length) {
    envBannerEl.textContent = "Env-pinned (staged values cannot override): " + state.envOverridden.join(", ");
    envBannerEl.classList.remove("hidden");
  } else {
    envBannerEl.classList.add("hidden");
  }
  if (state.lastRejection && state.lastRejection.message) {
    setStatus("Last write rejected: " + state.lastRejection.message, "err");
  }
}

function doPost(rec, changed, confirm) {
  if (!writeToken) { setStatus("Write token unavailable — is the dashboard running?", "err"); return; }
  const body = Object.assign({ stagedRev: state.stagedRev }, rec);
  if (confirm) body.confirm = true;
  submitEl.disabled = true;
  setStatus("Staging…");
  fetch("/api/settings", {
    method: "POST",
    headers: { "Content-Type": "application/json", "Authorization": "Bearer " + writeToken },
    body: JSON.stringify(body),
  }).then(async resp => {
    const data = await resp.json().catch(() => ({}));
    if (resp.status === 201) {
      setStatus("Staged ✓ (rev " + data.rev + ")", "ok");
      load();
      return;
    }
    if (resp.status === 409 && data.error === "confirmation_required") {
      showArmModal(data.arming || [], rec, changed);
      return;
    }
    if (resp.status === 409) {
      setStatus("Conflict: " + (data.message || "stale — reloading") , "err");
      load();
      return;
    }
    setStatus("Error: " + (data.message || data.error || ("HTTP " + resp.status)), "err");
  }).catch(err => setStatus("Network error: " + err.message, "err"))
    .finally(syncSubmit);
}

function showArmModal(arming, rec, changed) {
  armBody.innerHTML = "Arming <strong>" + arming.join(", ") + "</strong> arms real subscription spend on every coordinator sharing this MESH_DIR, or opens a network port. This is opt-in and audited. Proceed?";
  armModal.hidden = false;
  document.getElementById("armConfirm").onclick = function () { armModal.hidden = true; doPost(rec, changed, true); };
  document.getElementById("armCancel").onclick = function () { armModal.hidden = true; setStatus("Cancelled.", ""); syncSubmit(); };
}

document.getElementById("setForm").addEventListener("submit", e => {
  e.preventDefault();
  const { rec, changed } = collect();
  if (changed.length === 0) return;
  const arming = armingIn(changed);
  if (arming.length) { showArmModal(arming, rec, changed); return; }
  doPost(rec, changed, false);
});
document.getElementById("setReset").addEventListener("click", () => { setStatus(""); load(); });

// Live cross-tab reflection: an edit elsewhere (or a live hot-apply) refreshes
// this page via the shared SSE settings frame.
try {
  const es = new EventSource("/events");
  es.onmessage = ev => {
    let msg; try { msg = JSON.parse(ev.data); } catch (_) { return; }
    if (msg.type === "settings" && msg.settings) applyState(msg.settings);
  };
} catch (_) { /* SSE optional */ }

loadWriteToken();
load();

})();
