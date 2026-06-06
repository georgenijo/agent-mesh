#!/usr/bin/env bash
# mesh-claim-guard.sh — Claude Code PreToolUse hook: take a CAS claim on the
# target file before any mutating tool call (Edit/Write/MultiEdit/NotebookEdit).
#
# Contract (P1, advisory-guard spirit — see hooks/claude-code/README.md):
#   - claim won  (mesh exit 0) → exit 0: the edit proceeds, we hold the path.
#   - claim lost (mesh exit 6) → exit 2 + one stderr line naming the owner.
#     Exit 2 is the only blocking exit in Claude Code's hook protocol; the
#     stderr line is fed back to the model so it can coordinate instead of
#     colliding on the same file.
#   - not joined (mesh exit 5) → exit 0 silently: this session is not on the
#     mesh, so the hook must be a perfect no-op.
#   - everything else (non-edit tools, parse trouble, mesh/python3 missing,
#     bus down) → exit 0. Fail-open is deliberate: a coordination aid must
#     never brick editing on a machine where the mesh is absent or broken.
#
# Identity: the hook claims as the agent named by $MESH_SOCKET (this session's
# own sidecar socket). It MUST be set. Without it, `mesh` would fall back to
# "the single socket under $MESH_DIR/agents" and silently claim as whatever
# agent happens to be the only one on the machine — so a Claude Code session
# that never joined would take claims under someone else's identity. Rather
# than guess, the hook no-ops when $MESH_SOCKET is unset (a session not on the
# mesh has no socket to point at). Export it from the same shell that ran
# `mesh join` before launching the agent.
#
# Claims taken here are NOT auto-released by this hook (P1 limitation):
# release happens via `mesh release`, `mesh leave`, or coordinator reclaim
# when the holder's presence lease expires.
set -euo pipefail

# Fail-open guards first, before touching stdin: no python3 (needed to parse
# the hook JSON) or no mesh binary means this machine cannot participate —
# allow the edit.
command -v python3 >/dev/null 2>&1 || exit 0
command -v mesh >/dev/null 2>&1 || exit 0

# No explicit session socket → no-op. Claiming via the single-socket fallback
# would take the claim as the wrong agent (see "Identity" above).
[ -n "${MESH_SOCKET:-}" ] || exit 0

# One embedded parser, two modes, so all JSON handling lives in one place:
#   pre  — stdin: PreToolUse JSON → prints the absolute target path iff the
#          tool mutates a file (NotebookEdit carries notebook_path, the rest
#          file_path); prints nothing otherwise.
#   lost — stdin: `mesh claim --json` stdout → prints "<owner>\t<since>".
# Any parse trouble prints nothing and exits 0 — the shell side treats an
# empty result as "stand down", never as an error.
# (`read -d ''` reaches EOF without its NUL delimiter, hence `|| true`.)
read -r -d '' PARSER <<'PY' || true
import json, os, sys

mode = sys.argv[1] if len(sys.argv) > 1 else "pre"
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
if not isinstance(data, dict):
    sys.exit(0)

if mode == "pre":
    tool = data.get("tool_name")
    tool_input = data.get("tool_input")
    if not isinstance(tool_input, dict):
        sys.exit(0)
    if tool in ("Edit", "Write", "MultiEdit"):
        path = tool_input.get("file_path")
    elif tool == "NotebookEdit":
        path = tool_input.get("notebook_path")
    else:
        sys.exit(0)
    if not isinstance(path, str) or not path.strip():
        sys.exit(0)
    cwd = data.get("cwd")
    if not os.path.isabs(path) and isinstance(cwd, str) and cwd:
        path = os.path.join(cwd, path)
    print(path)
else:  # lost
    owner = data.get("owner") or "unknown"
    since = data.get("since") or "unknown"
    print("%s\t%s" % (owner, since))
PY

# The hook's stdin (the PreToolUse JSON) flows straight into python3 in one
# pass — never buffered in the shell, because a Write payload carries the
# whole file body and can be large.
path="$(python3 -c "$PARSER" pre 2>/dev/null)" || exit 0
[ -n "$path" ] || exit 0

# Take the claim. --repo only when the caller pins one via MESH_REPO;
# otherwise the sidecar defaults the repo from the agent card.
rc=0
if [ -n "${MESH_REPO:-}" ]; then
    out="$(mesh claim "$path" --repo "$MESH_REPO" --json 2>/dev/null)" || rc=$?
else
    out="$(mesh claim "$path" --json 2>/dev/null)" || rc=$?
fi

case "$rc" in
0)
    # Claimed: this agent holds the path; let the tool call through.
    exit 0
    ;;
6)
    # Lost the race: another agent legitimately holds this path. Name the
    # owner so the model can coordinate rather than guess; degrade to
    # "unknown" if the JSON is missing or malformed — still block, because
    # exit 6 alone is authoritative for "someone else holds it".
    owner="unknown"
    since="unknown"
    if parsed="$(printf '%s' "$out" | python3 -c "$PARSER" lost 2>/dev/null)" && [ -n "$parsed" ]; then
        owner="${parsed%%$'\t'*}"
        since="${parsed#*$'\t'}"
    fi
    printf 'mesh: %s is claimed by %s since %s — coordinate before editing (mesh who / mesh release)\n' \
        "$path" "$owner" "$since" >&2
    exit 2
    ;;
*)
    # 5 = not joined (silent no-op). Anything else — usage, transport error,
    # bus down — fails open: never block an edit over mesh trouble.
    exit 0
    ;;
esac
