#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

BASE_PORT=${MOSAIC_E2E_PORT_BASE:-19180}
COORD_PORT=$BASE_PORT
A_PORT=$((BASE_PORT + 1))
B_PORT=$((BASE_PORT + 2))
COORD="http://127.0.0.1:${COORD_PORT}"
A_URL="http://127.0.0.1:${A_PORT}"
B_URL="http://127.0.0.1:${B_PORT}"
TMP=$(mktemp -d)
PIDS=()

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$TMP"
}
trap cleanup EXIT

wait_http() {
  local url=$1
  for _ in $(seq 1 100); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.05
  done
  echo "timed out waiting for $url" >&2
  return 1
}

json_resource_id() {
  local name=$1
  python3 -c 'import json,sys
name=sys.argv[1]
data=json.load(sys.stdin)
for item in data["resources"]:
    if f"agent-name:{name}" in item["descriptor"]["features"]:
        print(item["descriptor"]["id"])
        break
else:
    raise SystemExit(1)' "$name"
}

json_running_resource() {
  python3 -c 'import json,sys
data=json.load(sys.stdin)
status=data["status"]
if isinstance(status, dict) and "Running" in status:
    print(status["Running"]["resource_id"])
'
}

cargo build --workspace --quiet

./target/debug/mosaic-coordinator \
  --bind "127.0.0.1:${COORD_PORT}" \
  --membership-ttl-ms 800 \
  --execution-ttl-ms 1200 \
  --maintenance-interval-ms 50 \
  >"$TMP/coordinator.log" 2>&1 &
COORD_PID=$!
PIDS+=("$COORD_PID")
wait_http "$COORD/healthz"

./target/debug/mosaic-agent run \
  --name worker-a \
  --performance 2 \
  --coordinator "$COORD" \
  --bind "127.0.0.1:${A_PORT}" \
  --advertise "$A_URL" \
  --store-dir "$TMP/a" \
  --heartbeat-interval-ms 150 \
  --poll-interval-ms 50 \
  >"$TMP/worker-a.log" 2>&1 &
A_PID=$!
PIDS+=("$A_PID")

./target/debug/mosaic-agent run \
  --name worker-b \
  --performance 1 \
  --coordinator "$COORD" \
  --bind "127.0.0.1:${B_PORT}" \
  --advertise "$B_URL" \
  --store-dir "$TMP/b" \
  --heartbeat-interval-ms 150 \
  --poll-interval-ms 50 \
  >"$TMP/worker-b.log" 2>&1 &
B_PID=$!
PIDS+=("$B_PID")

wait_http "$A_URL/healthz"
wait_http "$B_URL/healthz"

for _ in $(seq 1 100); do
  RESOURCES=$(./target/debug/mosaic-cli resources --coordinator "$COORD")
  COUNT=$(printf '%s' "$RESOURCES" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["resources"]))')
  if [[ "$COUNT" == "2" ]]; then
    break
  fi
  sleep 0.05
done
[[ "${COUNT:-0}" == "2" ]]
A_ID=$(printf '%s' "$RESOURCES" | json_resource_id worker-a)
B_ID=$(printf '%s' "$RESOURCES" | json_resource_id worker-b)

./target/debug/mosaic-cli link \
  --coordinator "$COORD" \
  --from "$A_ID" --to "$B_ID" \
  --rtt-ms 80 --bandwidth-mbps 100

printf 'hello-' >"$TMP/input-a"
printf 'fabric\n' >"$TMP/input-b"
OBJ_A=$(./target/debug/mosaic-cli put --coordinator "$COORD" --agent "$A_URL" --file "$TMP/input-a")
OBJ_B=$(./target/debug/mosaic-cli put --coordinator "$COORD" --agent "$A_URL" --file "$TMP/input-b")
TRANSFER_TASK=$(./target/debug/mosaic-cli submit \
  --coordinator "$COORD" \
  --artifact builtin:concat \
  --input "$OBJ_A" --input "$OBJ_B" \
  --require-feature agent-name:worker-b \
  --compute-ms 2000)
./target/debug/mosaic-cli wait --coordinator "$COORD" --task "$TRANSFER_TASK" --timeout-s 10 >/dev/null

EXPECTED=$(printf 'hello-fabric\n' | sha256sum | awk '{print $1}')
[[ -f "$TMP/b/sha256/$EXPECTED" ]]
BLOB_COUNT=$(find "$TMP/b/sha256" -type f | wc -l)
[[ "$BLOB_COUNT" -ge 3 ]]

echo "peer-transfer: ok (worker-b cached $BLOB_COUNT blobs)"

FAILURE_TASK=$(./target/debug/mosaic-cli submit \
  --coordinator "$COORD" \
  --artifact builtin:sleep \
  --argument 3000 \
  --compute-ms 3000)

RUNNING_RESOURCE=""
for _ in $(seq 1 100); do
  VIEW=$(./target/debug/mosaic-cli status --coordinator "$COORD" --task "$FAILURE_TASK")
  RUNNING_RESOURCE=$(printf '%s' "$VIEW" | json_running_resource)
  if [[ -n "$RUNNING_RESOURCE" ]]; then
    break
  fi
  sleep 0.05
done
[[ -n "$RUNNING_RESOURCE" ]]

if [[ "$RUNNING_RESOURCE" == "$A_ID" ]]; then
  kill "$A_PID"
  A_PID=0
elif [[ "$RUNNING_RESOURCE" == "$B_ID" ]]; then
  kill "$B_PID"
  B_PID=0
else
  echo "task leased to unknown resource $RUNNING_RESOURCE" >&2
  exit 1
fi

./target/debug/mosaic-cli wait --coordinator "$COORD" --task "$FAILURE_TASK" --timeout-s 12 >/dev/null

for _ in $(seq 1 50); do
  ACTIVE=$(./target/debug/mosaic-cli resources --coordinator "$COORD" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["resources"]))')
  if [[ "$ACTIVE" == "1" ]]; then
    break
  fi
  sleep 0.05
done
[[ "${ACTIVE:-0}" == "1" ]]

echo "failure-recovery: ok (lost $RUNNING_RESOURCE, task completed on survivor)"
