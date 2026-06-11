#!/usr/bin/env bash
# mesh-inbox-drain.sh — Claude Code Stop hook: when this agent has accepted
# mesh asks pending, feed them back to the model so it answers them before
# finishing the turn. The responder side of the P2 loop: the asker never
# blocks; the responder drains its inbox between turns.
#
# Protocol: a Stop hook's plain stdout is shown in the transcript but is NOT
# given to the model — so to make the model act on pending asks, the hook
# must emit {"decision":"block","reason":"..."} on stdout; the reason is fed
# to the model and the turn continues. Two guards keep this from looping or
# nagging:
#   - stop_hook_active true (the model is already continuing because of a
#     Stop hook) → never block again; print to the transcript only.
#   - empty inbox → exit 0 silently.
# The hook never fabricates answers and never marks a ticket handled; the
# model must explicitly run `mesh answer <ticket> "..."`.
#
# Identity: explicit $MESH_SOCKET wins; otherwise derived from the
# session_id in the hook JSON ("cc-<sid8>" — the name mesh-session-start.sh
# joined under). No socket on disk → this session never joined → no-op.
#
# Fail-open: missing python3/mesh, unparseable stdin, not joined, bus down
# → exit 0 silently.
set -euo pipefail

command -v python3 >/dev/null 2>&1 || exit 0
command -v mesh >/dev/null 2>&1 || exit 0

# Parse the Stop JSON → two lines: the derived agent name (may be empty)
# and stop_hook_active ("true"/"false").
read -r -d '' STDIN_PARSER <<'PY' || true
import json, re, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
if not isinstance(data, dict):
    sys.exit(0)
sid = data.get("session_id")
short = re.sub(r"[^A-Za-z0-9]", "", sid)[:8] if isinstance(sid, str) else ""
print("cc-" + short if short else "")
print("true" if data.get("stop_hook_active") else "false")
PY

parsed="$(python3 -c "$STDIN_PARSER" 2>/dev/null)" || exit 0
parsed="${parsed//$'\r'/}"
agent="${parsed%%$'\n'*}"
stop_active="${parsed#*$'\n'}"

if [ -z "${MESH_SOCKET:-}" ]; then
    [ -n "$agent" ] || exit 0
    derived="${MESH_DIR:-$HOME/.mesh}/agents/$agent.sock"
    [ -S "$derived" ] || exit 0
    export MESH_SOCKET="$derived"
fi

rc=0
out="$(mesh inbox --json 2>/dev/null)" || rc=$?
[ "$rc" -eq 0 ] || exit 0

# Render the pending-ask block from `mesh inbox --json`. Mode "block" wraps
# it in the Stop-hook decision JSON (fed to the model); mode "plain" prints
# it bare (transcript only — used when stop_hook_active forbids another
# block). Empty inbox prints nothing in both modes.
read -r -d '' RENDER <<'PY' || true
import json, sys
mode = sys.argv[1] if len(sys.argv) > 1 else "block"
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
items = data.get("items") if isinstance(data, dict) else None
if not isinstance(items, list) or not items:
    sys.exit(0)
lines = ["mesh inbox: pending asks addressed to this agent — answer them now:"]
for item in items:
    ticket = item.get("ticket", "")
    sender = item.get("from", "")
    question = item.get("question", "")
    ctx = item.get("context", "")
    lines.append(f"- {ticket} from {sender}: {question}")
    if ctx:
        lines.append(f"  context: {ctx}")
lines.append('Answer each with: mesh answer <ticket> "<answer>"')
text = "\n".join(lines)
if mode == "block":
    print(json.dumps({"decision": "block", "reason": text}))
else:
    print(text)
PY

if [ "$stop_active" = "true" ]; then
    # Already continuing because of a Stop hook: never block again
    # (prevents an answer-loop); leave a transcript note only.
    printf '%s' "$out" | python3 -c "$RENDER" plain || true
else
    printf '%s' "$out" | python3 -c "$RENDER" block || true
fi
exit 0
