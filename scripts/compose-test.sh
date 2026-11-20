#!/usr/bin/env bash
set -Eeuo pipefail
cd "$(dirname "$0")/.."
mode=${1:?expected null, smoke, chaos or load}
case "$mode" in null|smoke|chaos|load) ;; *) exit 2 ;; esac
if [[ "$mode" == chaos ]]; then export TEST_WAL_MAX_BYTES=12288; fi
# Test runs use live device traffic; the dedicated year fixture is seeded by
# the bench so the demo devices stay independent from chart verification.
export SIM_BACKFILL_DAYS=0
project="monitoring-${mode}-$(date +%s)-$$"
compose=(docker compose -p "$project" -f docker-compose.yml -f docker-compose.test.yml)
dc() { "${compose[@]}" "$@"; }
probe() { dc exec -T bench /tmp/bench "$@"; }
stats_pid=
traffic_pid=
cleanup() {
    local status=$?
    trap - EXIT
    if [[ -n "$stats_pid" ]]; then kill "$stats_pid" 2>/dev/null || true; wait "$stats_pid" 2>/dev/null || true; fi
    if [[ -n "$traffic_pid" ]]; then kill "$traffic_pid" 2>/dev/null || true; wait "$traffic_pid" 2>/dev/null || true; fi
    if ((status)); then dc logs --no-color --tail=12 >&2 || true; fi
    # Only this invocation's newly created fixture volumes belong to this project.
    dc down --volumes --remove-orphans || status=1
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

echo "Docker test project: $project"
echo "Test frontend (available until this command finishes): http://localhost:${TEST_UI_PORT:-18080}/"
if [[ "$mode" == null ]]; then
    dc build ingest1 nginx
    dc up -d --no-build --wait --wait-timeout 240 nginx consumer1 consumer2 consumer3 simulator
    dc run --rm test-null
    exit
fi
dc build ingest1 nginx bench
dc up -d --no-build --wait --wait-timeout 240 nginx consumer1 consumer2 consumer3 bench simulator
base=$(probe now)

broker_outage() {
    dc stop -t 0 redpanda
    # Keep this scenario focused on the ingest WAL instead of proxy retries
    # while all brokers are deliberately offline.
    probe send 101 "$base" 60 1s http://ingest1:8081/v1/ingest
    probe absent 101
    dc up -d --no-deps --wait --wait-timeout 120 redpanda
    probe verify 101 "$base" 60
    echo "Broker outage: 60 acknowledged, 0 lost, 0 duplicate database rows."
}
ingest_outage() {
    probe send 102 "$base" 40 250ms http://nginx/v1/ingest /tmp/traffic-started &
    traffic_pid=$!
    local seen=false
    for _ in {1..40}; do
        if dc exec -T bench test -f /tmp/traffic-started; then seen=true; break; fi
        sleep .1
    done
    [[ "$seen" == true ]]
    dc stop -t 0 ingest1
    wait "$traffic_pid"
    traffic_pid=
    probe send 102 "$base" 40 0s http://nginx/v1/ingest
    probe verify 102 "$base" 40
    dc up -d --no-deps --wait --wait-timeout 120 ingest1
    dc restart nginx
    echo "Ingest stopped during traffic: 40 unique samples after resending all 40."
}
wal_full() {
    dc stop -t 0 redpanda
    probe fill 103 "$base" http://ingest1:8081/v1/ingest
    probe absent 103
    dc up -d --no-deps --wait --wait-timeout 120 redpanda
    probe verify-fill 103 "$base"
}
database_outage() {
    dc stop -t 0 timescaledb
    [[ "$(dc exec -T bench curl -sS -o /dev/null -w '%{http_code}' http://api:8082/healthz)" == 503 ]]
    [[ "$(dc exec -T bench curl -sS -o /dev/null -w '%{http_code}' http://consumer1:8081/healthz)" == 503 ]]
    probe send 104 "$base" 50 20ms http://nginx/v1/ingest
    dc up -d --no-deps --wait --wait-timeout 120 timescaledb
    probe verify 104 "$base" 50
    dc exec -T bench curl -fsS --retry 3 --retry-delay 1 http://api:8082/healthz
    dc exec -T bench curl -fsS --retry 3 --retry-delay 1 http://consumer1:8081/healthz
    echo "Database outage: all 50 acknowledged samples caught up."
}

case "$mode" in
smoke)
    probe smoke "$base"
    probe year
    dc stop -t 5 simulator
    broker_outage
    ;;
chaos)
    dc stop -t 5 simulator
    broker_outage
    ingest_outage
    wal_full
    database_outage
    ;;
load)
    probe smoke "$base"
    out=${LOAD_OUTPUT_DIR:-docs/loadtest}
    mkdir -p "$out"
    # Sample only this project's service containers. No metric endpoint is needed.
    mapfile -t ids < <(dc ps -q timescaledb redpanda ingest1 ingest2 ingest3 consumer1 consumer2 consumer3 api nginx bench)
    (while :; do
        docker stats --no-stream --format '{{json .}}' "${ids[@]}"
        sleep 1
    done) > "$out/resources.jsonl" &
    stats_pid=$!
    docker info --format '{{.NCPU}} logical CPUs, {{.MemTotal}} bytes RAM, {{.Architecture}}, {{.OperatingSystem}}' > "$out/host.txt"
    set +e
    probe load "${LOAD_START_DEVICES:-100}" "${LOAD_MAX_DEVICES:-10000}" "${LOAD_STAGE_SECONDS:-12}" > "$out/results.json"
    status=$?
    set -e
    kill "$stats_pid" 2>/dev/null || true
    wait "$stats_pid" 2>/dev/null || true
    stats_pid=
    if [[ -s "$out/results.json" && -s "$out/resources.jsonl" ]]; then
        for graph in write-rate chart-latency resources; do
            { cat "$out/results.json"; cat "$out/resources.jsonl"; } | probe report "$graph" > "$out/$graph.html"
        done
    else
        echo "Load test did not produce results and resource samples" >&2
        exit 1
    fi
    cat "$out/results.json"
    exit "$status"
    ;;
esac
