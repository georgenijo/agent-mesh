#!/usr/bin/env bash
# mesh-inbox-drain.sh - Codex Stop hook: continue the turn when accepted mesh
# asks are pending for this agent.
#
# Codex Stop hooks require JSON stdout. Empty stdout is success/no-op.
set -euo pipefail

command -v python3 >/dev/null 2>&1 || exit 0
command -v mesh >/dev/null 2>&1 || exit 0

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
print("cx-" + short if short else "")
print("true" if data.get("stop_hook_active") else "false")
PY

parsed="$(python3 -c "$STDIN_PARSER" 2>/dev/null)" || exit 0
parsed="${parsed//$'\r'/}"
agent="${parsed%%$'\n'*}"
stop_active="${parsed#*$'\n'}"

[ "$stop_active" != "true" ] || exit 0

if [ -z "${MESH_SOCKET:-}" ]; then
    [ -n "$agent" ] || exit 0
    derived="${MESH_DIR:-$HOME/.mesh}/agents/$agent.sock"
    [ -S "$derived" ] || exit 0
    export MESH_SOCKET="$derived"
fi

rc=0
out="$(mesh inbox --json 2>/dev/null)" || rc=$?
[ "$rc" -eq 0 ] || exit 0

read -r -d '' RENDER <<'PY' || true
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
items = data.get("items") if isinstance(data, dict) else None
if not isinstance(items, list) or not items:
    sys.exit(0)
lines = ["mesh inbox: pending asks addressed to this Codex agent - answer them now:"]
for item in items:
    ticket = item.get("ticket", "")
    sender = item.get("from", "")
    question = item.get("question", "")
    ctx = item.get("context", "")
    lines.append(f"- {ticket} from {sender}: {question}")
    if ctx:
        lines.append(f"  context: {ctx}")
lines.append('Answer each with: mesh answer <ticket> "<answer>"')
print(json.dumps({"decision": "block", "reason": "\n".join(lines)}))
PY

printf '%s' "$out" | python3 -c "$RENDER" || true
exit 0
