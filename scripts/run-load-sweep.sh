#!/usr/bin/env bash
set -Eeuo pipefail
out=$(realpath -m -- "${1:?usage: run-load-sweep.sh OUTPUT_DIR STAGE...}")
shift
(($# > 0)) || { echo "Supply at least one stage" >&2; exit 2; }
cd "$(dirname "$0")/.."
mkdir -p "$out"
runner_error=0
for key in "$@"; do
    [[ "$key" =~ ^([1-9][0-9]*)(-r[1-9][0-9]*)?$ ]] || { echo "Invalid stage label: $key" >&2; exit 2; }
    n=${BASH_REMATCH[1]}
    mkdir "$out/$key"
    printf 'Running %s on a fresh Compose project\n' "$key"
    rc=0
    LOAD_START_DEVICES="$n" LOAD_MAX_DEVICES="$n" LOAD_STAGE_SECONDS=12 LOAD_OUTPUT_DIR="$out/$key" \
        make load >"$out/$key/run.log" 2>&1 || rc=$?
    printf '%s %s\n' "$key" "$rc" >>"$out/exit-status.txt"
    if ((rc)); then
        if [[ ! -s "$out/$key/results.json" ]] || ! python3 -c 'import json,sys; sys.exit(any(s["passed"] for s in json.load(open(sys.argv[1]))["stages"]))' "$out/$key/results.json"; then
            echo "Runner error: see $out/$key/run.log" >&2
            runner_error=1
        fi
    fi
done
exit "$runner_error"
