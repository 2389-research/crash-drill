#!/usr/bin/env bash
# One command: drill the reference spool. Pass a pipeline flag to drill a sabotage:
#
#   ./examples/spool/run.sh                  # intact: all boundaries must pass
#   ./examples/spool/run.sh --row-first      # broken: the drill must fail
set -euo pipefail
cd "$(dirname "$0")/../.." || exit 1

exec python3 drill/drill.py \
    --pipeline "python3 examples/spool/pipeline.py $*" \
    --inspect "python3 examples/spool/inspect.py"
