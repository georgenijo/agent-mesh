#!/usr/bin/env bash
# mesh-inbox-drain.sh — Claude Code Stop hook: surface accepted mesh asks
# between turns. It never answers automatically; it only prints deterministic
# pending-work context for the next model turn.
set -euo pipefail

command -v python3 >/dev/null 2>&1 || exit 0
command -v mesh >/dev/null 2>&1 || exit 0
[ -n "${MESH_SOCKET:-}" ] || exit 0

rc=0
out="$(mesh inbox --json 2>/dev/null)" || rc=$?
case "$rc" in
0) ;;
5) exit 0 ;;
*) exit 0 ;;
esac

read -r -d '' PARSER <<'PY' || true
import json, sys
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
items = data.get("items")
if not isinstance(items, list) or not items:
    sys.exit(0)
print("mesh inbox: pending asks")
for item in items:
    ticket = item.get("ticket", "")
    sender = item.get("from", "")
    question = item.get("question", "")
    ctx = item.get("context", "")
    print(f"- {ticket} from {sender}: {question}")
    if ctx:
        print(f"  context: {ctx}")
print("Reply with: mesh answer <ticket> \"<answer>\"")
PY

printf '%s' "$out" | python3 -c "$PARSER" || true
exit 0
