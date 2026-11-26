#!/usr/bin/env bash
# Prove the drill can fail.
#
# A crash test that has only ever passed is a test that cannot fail: the usual
# reason a spool drill passes is that it never actually entered the window, or
# that its assertion is compatible with the bug it was written to catch. So each
# sabotage below runs the SAME drill, with the SAME boundary row, against a
# pipeline that is broken in one specific way -- and the drill must fail, and
# fail for the stated reason. The intact pipeline runs first at the same
# boundary, so a sabotage "failure" cannot be the drill failing at everything.
#
#   make sabotage
set -uo pipefail

cd "$(dirname "$0")/.." || exit 1
INSPECT="python3 examples/spool/inspect.py"
PIPELINE="python3 examples/spool/pipeline.py"
LOG=$(mktemp)
trap 'rm -f "$LOG"' EXIT

status=0

# must_pass <boundary> [pipeline flags]
must_pass() {
    local boundary="$1" flags="${2:-}"
    if python3 drill/drill.py --pipeline "$PIPELINE $flags" --inspect "$INSPECT" --only "$boundary" >"$LOG" 2>&1; then
        printf '  ok    intact spool passes %-30s\n' "$boundary"
    else
        printf '  BROKEN drill FAILED the intact spool at %s -- fix the drill first\n' "$boundary"
        sed 's/^/        /' "$LOG"
        status=1
    fi
}

# must_fail <boundary> <flags> <expected substring> <what the sabotage is>
must_fail() {
    local boundary="$1" flags="$2" expected="$3" what="$4"
    if python3 drill/drill.py --pipeline "$PIPELINE $flags" --inspect "$INSPECT" --only "$boundary" --deadline 8 >"$LOG" 2>&1; then
        printf '  BROKEN drill PASSED a sabotaged spool at %-22s (%s)\n' "$boundary" "$what"
        sed 's/^/        /' "$LOG"
        status=1
        return
    fi
    if grep -qF -- "$expected" "$LOG"; then
        printf '  ok    drilled %-38s -> %s\n' "$what" "$expected"
    else
        printf '  BROKEN %s failed, but not for the stated reason (wanted %q)\n' "$what" "$expected"
        sed 's/^/        /' "$LOG"
        status=1
    fi
}

echo "the drill must pass a spool that is intact:"
must_pass post-emit-pre-batch
must_pass post-batch-pre-send
must_pass post-batch-pre-send@mid-write
must_pass post-send-pre-ack
must_pass post-ack-pre-cursor

echo
echo "the drill must fail a spool that is broken:"
must_fail post-batch-pre-send --row-first \
    "has no staged bytes" \
    "receipt committed before its bytes"
must_fail post-batch-pre-send@mid-write --publish-direct \
    "does not hash to its own name" \
    "bundle published before it was whole"
must_fail post-batch-pre-send --forget-outbox \
    "the lake's merged export holds offsets" \
    "restart forgets the outbox"
must_fail post-send-pre-ack --ack-early \
    "was not reached" \
    "ack committed before the lake answered"

echo
if [ "$status" -eq 0 ]; then
    echo "sabotage: every broken spool was caught, and the intact one passed. The drill can fail."
else
    echo "sabotage: FAILED (see above)"
fi
exit "$status"
