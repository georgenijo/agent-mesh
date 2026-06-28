// jobform.js — P4 job submit form (issue #47).
//
// This is the ONLY script that sends a mutating request to the dashboard
// server. All other scripts (app.js, enhance.js) are read-only observers.
//
// Security posture: the write API requires a local bearer token written to
// MESH_DIR/dashboard.token on start. The form fetches the token from
// GET /api/write-token (loopback-only, same origin) and presents it as
// Authorization: Bearer <token> on POST /api/jobs. No cross-origin, no
// persistent cookie, no long-lived credential.
"use strict";

(function () {

// Write-API token, fetched once on load from the dashboard server.
// null = not yet fetched; "" = fetch failed (form will show an error).
let writeToken = null;

// Fetch the write-API token from the server. Retried once on failure;
// the form disables submit until it resolves.
function loadWriteToken() {
  fetch("/api/write-token")
    .then(r => r.ok ? r.json() : Promise.reject(r.status))
    .then(data => { writeToken = data.token || ""; })
    .catch(() => { writeToken = ""; })
    .finally(syncSubmitBtn);
}

loadWriteToken();

// -- DOM refs --

const overlay = document.getElementById("jobFormOverlay");
const openBtn  = document.getElementById("submitJobBtn");
const closeBtn = document.getElementById("jfClose");
const form     = document.getElementById("jobForm");
const repoEl   = document.getElementById("jfRepo");
const titleEl  = document.getElementById("jfTitle2");
const bodyEl   = document.getElementById("jfBody");
const submitEl = document.getElementById("jfSubmit");
const statusEl = document.getElementById("jfStatus");

// Populate the repo <select> from GET /api/repos. Called when the form opens
// so the list is always fresh. On error or empty list a disabled placeholder
// option makes the failure visible without blocking the rest of the form.
function loadRepos() {
  if (!repoEl) return;
  fetch("/api/repos")
    .then(r => r.ok ? r.json() : Promise.reject(r.status))
    .then(data => {
      const repos = (data && data.repos) || [];
      // Clear existing options.
      repoEl.innerHTML = "";
      if (repos.length === 0) {
        const opt = document.createElement("option");
        opt.value = "";
        opt.disabled = true;
        opt.selected = true;
        opt.textContent = "No repos found (MESH_REPOS_DIR not configured)";
        repoEl.appendChild(opt);
        return;
      }
      const placeholder = document.createElement("option");
      placeholder.value = "";
      placeholder.disabled = true;
      placeholder.selected = true;
      placeholder.textContent = "Select a repo…";
      repoEl.appendChild(placeholder);
      repos.forEach(function(r) {
        const opt = document.createElement("option");
        opt.value = r.name;
        opt.textContent = r.name;
        repoEl.appendChild(opt);
      });
      // Auto-select the only option when there is exactly one repo.
      if (repos.length === 1) {
        repoEl.value = repos[0].name;
      }
    })
    .catch(function() {
      repoEl.innerHTML = "";
      const opt = document.createElement("option");
      opt.value = "";
      opt.disabled = true;
      opt.selected = true;
      opt.textContent = "Failed to load repos";
      repoEl.appendChild(opt);
    });
}

function openForm() {
  if (!overlay) return;
  overlay.hidden = false;
  loadRepos();
  if (repoEl) repoEl.focus();
}

function closeForm() {
  if (!overlay) return;
  overlay.hidden = true;
  clearStatus();
}

function setStatus(msg, cls) {
  if (!statusEl) return;
  statusEl.textContent = msg;
  statusEl.className = "jf-status" + (cls ? " " + cls : "");
}

function clearStatus() {
  if (statusEl) { statusEl.textContent = ""; statusEl.className = "jf-status"; }
}

function syncSubmitBtn() {
  if (!submitEl) return;
  // Disabled while token is loading (null) or if fetch failed ("").
  submitEl.disabled = writeToken === null;
}

// Wire button / overlay / keyboard.
if (openBtn)  openBtn.addEventListener("click", openForm);
if (closeBtn) closeBtn.addEventListener("click", closeForm);
if (overlay) {
  // Click outside the sheet to close.
  overlay.addEventListener("click", (e) => {
    if (e.target === overlay) closeForm();
  });
}
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && overlay && !overlay.hidden) closeForm();
});

// Form submission.
if (form) {
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    if (!submitEl || submitEl.disabled) return;

    const repo  = (repoEl  && repoEl.value.trim())  || "";
    const title = (titleEl && titleEl.value.trim()) || "";
    const body  = (bodyEl  && bodyEl.value.trim())  || "";

    if (!repo)  { setStatus("Please select a repo.", "err"); repoEl && repoEl.focus(); return; }
    if (!title) { setStatus("Title is required.", "err"); titleEl && titleEl.focus(); return; }

    if (!writeToken) {
      setStatus("Write token unavailable — is the dashboard running?", "err");
      return;
    }

    submitEl.disabled = true;
    setStatus("Submitting…");

    let result;
    try {
      const resp = await fetch("/api/jobs", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": "Bearer " + writeToken,
        },
        body: JSON.stringify({ repo, title, body }),
      });
      result = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        const msg = result.message || result.error || ("HTTP " + resp.status);
        setStatus("Error: " + msg, "err");
        return;
      }
    } catch (err) {
      setStatus("Network error: " + err.message, "err");
      return;
    } finally {
      submitEl.disabled = false;
    }

    // Success: reset form and show confirmation.
    form.reset();
    const jobID = result.job ? result.job.slice(0, 8) + "…" : "";
    setStatus("Submitted" + (jobID ? " · " + jobID : "") + " ✓", "ok");
    setTimeout(closeForm, 1800);
  });
}

})();
