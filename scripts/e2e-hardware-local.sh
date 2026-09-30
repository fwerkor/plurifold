#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

BACKEND=${1:-}
DEVICE_INDEX=${PLURIFOLD_DEVICE_INDEX:-0}
BASE_PORT=${PLURIFOLD_HARDWARE_E2E_PORT_BASE:-19480}
COORD_PORT=$BASE_PORT
AGENT_PORT=$((BASE_PORT + 1))
COORD="http://127.0.0.1:${COORD_PORT}"
AGENT="http://127.0.0.1:${AGENT_PORT}"
TMP=$(mktemp -d)
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$TMP"
}
trap cleanup EXIT

case "$BACKEND" in
  cuda)
    FEATURE=cuda
    ARTIFACT="$ROOT/examples/executors/cuda-driver-smoke.py"
    ARGS=()
    ;;
  cann)
    FEATURE=cann
    ARTIFACT="$ROOT/examples/executors/ascend-torch-smoke.py"
    ARGS=(--argument "$DEVICE_INDEX")
    ;;
  *)
    echo "usage: $0 {cuda|cann}" >&2
    exit 2
    ;;
esac

cargo build --release -p plurifold-agent -p plurifold-coordinator -p plurifold-cli --quiet

./target/release/plurifold-coordinator \
  --bind "127.0.0.1:${COORD_PORT}" \
  --membership-ttl-ms 5000 \
  --execution-ttl-ms 15000 \
  --maintenance-interval-ms 200 \
  >"$TMP/coordinator.log" 2>&1 &
PIDS+=("$!")

for _ in $(seq 1 100); do
  curl -fsS "$COORD/healthz" >/dev/null 2>&1 && break
  sleep 0.05
done

./target/release/plurifold-agent run \
  --name "hardware-${BACKEND}" \
  --coordinator "$COORD" \
  --bind "127.0.0.1:${AGENT_PORT}" \
  --advertise "$AGENT" \
  --store-dir "$TMP/store" \
  --exec-root "$ROOT/examples/executors" \
  --heartbeat-interval-ms 500 \
  --poll-interval-ms 100 \
  --probe-interval-ms 5000 \
  >"$TMP/agent.log" 2>&1 &
PIDS+=("$!")

for _ in $(seq 1 100); do
  RESOURCES=$(./target/release/plurifold resources --coordinator "$COORD" 2>/dev/null || true)
  printf '%s' "$RESOURCES" | grep -q "\"$FEATURE\"" && break
  sleep 0.05
done
printf '%s' "${RESOURCES:-}" | grep -q "\"$FEATURE\""

TASK=$(./target/release/plurifold submit \
  --coordinator "$COORD" \
  --artifact "native:$ARTIFACT" \
  --require-feature executor:native \
  --require-feature "$FEATURE" \
  "${ARGS[@]}" \
  --compute-ms 100)

./target/release/plurifold wait --coordinator "$COORD" --task "$TASK" --timeout-s 30 >/dev/null
grep -R -q '"verified":true' "$TMP/store/sha256"

echo "hardware-e2e: ok backend=$BACKEND task=$TASK"
