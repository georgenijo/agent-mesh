#!/bin/bash
# plan-cli.sh — adapter from plan-tool.sh's file-output to the stdout contract
# the mesh plan-step (triage.runPlanStep) expects.
#
# The mesh plan-step captures the plan from stdout; plan-tool.sh in the sibling
# planning-tool repo writes its output to a file and prints a
# PLAN_ARTIFACT=<path> line. This script bridges the two contracts: it runs
# plan-tool.sh with the given args, detects the PLAN_ARTIFACT line, and cats
# that file to stdout so the mesh can capture it.
#
# Required: PLAN_TOOL must point at planning-tool/plan-tool.sh (absolute path
# or resolvable from PATH). Example:
#   export PLAN_TOOL=/Users/george/Documents/Code/planning-tool/plan-tool.sh
#
# Usage (called by the mesh coordinator):
#   plan-cli.sh <ticketfile> --repo <repopath> [--model <model>]
#
# Bash 3.2 compatible (macOS default shell).

set -e

if [ -z "$PLAN_TOOL" ]; then
    echo "plan-cli.sh: PLAN_TOOL is not set; must point at planning-tool/plan-tool.sh" >&2
    exit 1
fi

if [ ! -x "$PLAN_TOOL" ]; then
    echo "plan-cli.sh: PLAN_TOOL=$PLAN_TOOL is not executable" >&2
    exit 1
fi

# Run plan-tool.sh; capture its combined output to detect PLAN_ARTIFACT.
tmpout=$(mktemp)
trap 'rm -f "$tmpout"' EXIT

"$PLAN_TOOL" "$@" >"$tmpout" 2>&1
status=$?

# If the tool printed PLAN_ARTIFACT=<path>, cat that file to stdout.
artifact=""
while IFS= read -r line; do
    case "$line" in
        PLAN_ARTIFACT=*)
            artifact="${line#PLAN_ARTIFACT=}"
            ;;
    esac
done <"$tmpout"

if [ -n "$artifact" ]; then
    if [ ! -f "$artifact" ]; then
        echo "plan-cli.sh: PLAN_ARTIFACT=$artifact not found" >&2
        exit 1
    fi
    cat "$artifact"
else
    # No PLAN_ARTIFACT line: pass the raw stdout through (fallback for tools
    # that already write the plan directly to stdout).
    cat "$tmpout"
fi

exit $status
