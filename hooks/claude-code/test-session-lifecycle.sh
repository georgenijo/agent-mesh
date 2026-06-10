#!/usr/bin/env bash
# test-session-lifecycle.sh — self-contained checks for the session
# lifecycle hooks (mesh-session-start.sh, mesh-session-end.sh) and the
# Stop-hook inbox drain (mesh-inbox-drain.sh).
#
# Same harness style as test-claim-guard.sh: a stubbed `mesh` on PATH
# (canned output + exit code per subcommand, driven by env; argv + the
# MESH_SOCKET it ran under logged for assertions), sample hook JSON piped
# in, plain bash asserts. Needs only bash + python3.
set -u

HERE="$(cd "$(dirname "$0")" && pwd)"
START="$HERE/mesh-session-start.sh"
END="$HERE/mesh-session-end.sh"
DRAIN="$HERE/mesh-inbox-drain.sh"
BASH_BIN="$(command -v bash)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

STUB="$TMP/bin"
mkdir -p "$STUB"
export MESH_STUB_LOG="$TMP/mesh-args.log"

# Stub mesh: logs every invocation; join/leave honour MESH_STUB_JOIN /
# MESH_STUB_LEAVE exit codes; who prints a tiny roster; inbox prints
# $MESH_STUB_INBOX (default: one pending ask).
cat >"$STUB/mesh" <<'SH'
#!/usr/bin/env bash
printf '%s | socket=%s\n' "$*" "${MESH_SOCKET:-}" >>"${MESH_STUB_LOG:-/dev/null}"
case "$1" in
join)
    exit "${MESH_STUB_JOIN:-0}" ;;
leave)
    exit "${MESH_STUB_LEAVE:-0}" ;;
announce)
    exit "${MESH_STUB_ANNOUNCE:-0}" ;;
who)
    echo "NAME   ROLE     STATE"
    echo "alpha  builder  live"
    exit 0 ;;
inbox)
    if [ -n "${MESH_STUB_INBOX+x}" ]; then
        printf '%s' "$MESH_STUB_INBOX"
    else
        printf '%s' '{"items":[{"ticket":"t-1","from":"alpha","question":"is the bus fix merged?","context":"PR 41"}]}'
    fi
    exit "${MESH_STUB_INBOX_RC:-0}" ;;
*)
    exit 0 ;;
esac
SH
chmod +x "$STUB/mesh"

SID="abcd1234-ef56-7890-abcd-ef1234567890"   # derives agent cc-abcd1234
START_JSON='{"session_id":"'"$SID"'","cwd":"/tmp/demo","hook_event_name":"SessionStart","source":"startup"}'
END_JSON='{"session_id":"'"$SID"'","cwd":"/tmp/demo","hook_event_name":"SessionEnd","reason":"exit"}'
STOP_JSON='{"session_id":"'"$SID"'","cwd":"/tmp/demo","hook_event_name":"Stop","stop_hook_active":false}'
STOP_ACTIVE_JSON='{"session_id":"'"$SID"'","cwd":"/tmp/demo","hook_event_name":"Stop","stop_hook_active":true}'

pass=0
fail=0

assert_eq() { # <name> <want> <got>
    if [ "$2" = "$3" ]; then
        echo "PASS  $1"; pass=$((pass + 1))
    else
        echo "FAIL  $1 (want: $2, got: $3)"; fail=$((fail + 1))
    fi
}

assert_contains() { # <name> <needle> <file>
    if grep -qF -- "$2" "$3" 2>/dev/null; then
        echo "PASS  $1"; pass=$((pass + 1))
    else
        echo "FAIL  $1 (missing: $2 in: $(cat "$3" 2>/dev/null))"; fail=$((fail + 1))
    fi
}

assert_empty_file() { # <name> <file>
    if [ ! -s "$2" ]; then
        echo "PASS  $1"; pass=$((pass + 1))
    else
        echo "FAIL  $1 (expected empty, got: $(cat "$2"))"; fail=$((fail + 1))
    fi
}

# run <hook> <json> [extra env as VAR=VAL ...]: sets $rc, $TMP/out, $TMP/err.
run() {
    local hook="$1" json="$2"; shift 2
    rc=0
    printf '%s' "$json" | env -u MESH_SOCKET PATH="$STUB:$PATH" "$@" \
        "$BASH_BIN" "$hook" >"$TMP/out" 2>"$TMP/err" || rc=$?
}

mksock() { # <path> — create a real unix socket
    mkdir -p "$(dirname "$1")"
    perl -MIO::Socket::UNIX -e 'IO::Socket::UNIX->new(Local => $ARGV[0], Listen => 1) or die $!' "$1"
}

# ════ mesh-session-start.sh ════════════════════════════════════════════════

# Joins under the derived session name with the default role.
: >"$MESH_STUB_LOG"
run "$START" "$START_JSON"
assert_eq "start: exit 0" 0 "$rc"
assert_contains "start: joins as cc-<sid8>" "join --name cc-abcd1234 --role builder" "$MESH_STUB_LOG"
assert_contains "start: announces startup" "announce claude-code session started" "$MESH_STUB_LOG"
assert_contains "start: context names the agent" 'agent "cc-abcd1234"' "$TMP/out"
assert_contains "start: context includes roster" "alpha  builder  live" "$TMP/out"

# MESH_ROLE / MESH_REPO are forwarded.
: >"$MESH_STUB_LOG"
run "$START" "$START_JSON" MESH_ROLE=reviewer MESH_REPO=myrepo
assert_eq "start: role+repo exit 0" 0 "$rc"
assert_contains "start: --role forwarded" "--role reviewer" "$MESH_STUB_LOG"
assert_contains "start: --repo forwarded" "--repo myrepo" "$MESH_STUB_LOG"
assert_contains "start: announce --repo forwarded" "announce claude-code session started --repo myrepo" "$MESH_STUB_LOG"

# Join refused → silent fail-open: no context block, exit 0.
run "$START" "$START_JSON" MESH_STUB_JOIN=1
assert_eq "start: join refused -> exit 0" 0 "$rc"
assert_empty_file "start: join refused -> no context" "$TMP/out"

# Garbage stdin / missing session_id → no-op, mesh never invoked.
: >"$MESH_STUB_LOG"
run "$START" 'not json'
assert_eq "start: garbage stdin -> exit 0" 0 "$rc"
assert_empty_file "start: garbage stdin -> mesh not invoked" "$MESH_STUB_LOG"

: >"$MESH_STUB_LOG"
run "$START" '{"cwd":"/tmp/demo"}'
assert_eq "start: no session_id -> exit 0" 0 "$rc"
assert_empty_file "start: no session_id -> mesh not invoked" "$MESH_STUB_LOG"

# ════ mesh-session-end.sh ══════════════════════════════════════════════════

# Socket present → leaves via the derived socket.
MESH_DIR_T="$TMP/meshdir"
mksock "$MESH_DIR_T/agents/cc-abcd1234.sock"
: >"$MESH_STUB_LOG"
run "$END" "$END_JSON" MESH_DIR="$MESH_DIR_T"
assert_eq "end: exit 0" 0 "$rc"
assert_contains "end: leaves with reason" "leave --reason claude-code session ended" "$MESH_STUB_LOG"
assert_contains "end: leaves via session socket" "socket=$MESH_DIR_T/agents/cc-abcd1234.sock" "$MESH_STUB_LOG"

# No socket → session never joined → mesh never invoked.
: >"$MESH_STUB_LOG"
run "$END" "$END_JSON" MESH_DIR="$TMP/empty-meshdir"
assert_eq "end: no socket -> exit 0" 0 "$rc"
assert_empty_file "end: no socket -> mesh not invoked" "$MESH_STUB_LOG"

# leave failing is still fail-open.
run "$END" "$END_JSON" MESH_DIR="$MESH_DIR_T" MESH_STUB_LEAVE=1
assert_eq "end: leave failed -> exit 0" 0 "$rc"

# ════ mesh-inbox-drain.sh ══════════════════════════════════════════════════

# Pending asks, stop_hook_active=false → decision:block JSON on stdout.
run "$DRAIN" "$STOP_JSON" MESH_DIR="$MESH_DIR_T"
assert_eq "drain: exit 0" 0 "$rc"
assert_contains "drain: emits decision block" '"decision": "block"' "$TMP/out"
assert_contains "drain: reason carries ticket" "t-1 from alpha" "$TMP/out"
assert_contains "drain: reason carries answer hint" "mesh answer <ticket>" "$TMP/out"

# stop_hook_active=true → plain text only (never block twice in a row).
run "$DRAIN" "$STOP_ACTIVE_JSON" MESH_DIR="$MESH_DIR_T"
assert_eq "drain: stop_hook_active exit 0" 0 "$rc"
assert_contains "drain: stop_hook_active still lists asks" "t-1 from alpha" "$TMP/out"
rc=0; grep -qF '"decision"' "$TMP/out" && rc=1
assert_eq "drain: stop_hook_active never blocks" 0 "$rc"

# Empty inbox → no output at all.
run "$DRAIN" "$STOP_JSON" MESH_DIR="$MESH_DIR_T" MESH_STUB_INBOX='{"items":[]}'
assert_eq "drain: empty inbox exit 0" 0 "$rc"
assert_empty_file "drain: empty inbox silent" "$TMP/out"

# inbox failing (e.g. not joined) → fail-open, silent.
run "$DRAIN" "$STOP_JSON" MESH_DIR="$MESH_DIR_T" MESH_STUB_INBOX_RC=5
assert_eq "drain: inbox rc5 exit 0" 0 "$rc"
assert_empty_file "drain: inbox rc5 silent" "$TMP/out"

# No socket → session never joined → mesh never invoked.
: >"$MESH_STUB_LOG"
run "$DRAIN" "$STOP_JSON" MESH_DIR="$TMP/empty-meshdir"
assert_eq "drain: no socket -> exit 0" 0 "$rc"
assert_empty_file "drain: no socket -> mesh not invoked" "$MESH_STUB_LOG"

# ════ summary ══════════════════════════════════════════════════════════════
echo
echo "$pass passed, $fail failed"
if [ "$fail" -gt 0 ]; then
    echo "FAIL"
    exit 1
fi
echo "PASS"
exit 0
