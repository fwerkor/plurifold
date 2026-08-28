#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

BASE_PORT=${PLURIFOLD_E2E_PORT_BASE:-19180}
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

./target/debug/plurifold-coordinator \
  --bind "127.0.0.1:${COORD_PORT}" \
  --membership-ttl-ms 800 \
  --execution-ttl-ms 1200 \
  --maintenance-interval-ms 50 \
  >"$TMP/coordinator.log" 2>&1 &
COORD_PID=$!
PIDS+=("$COORD_PID")
wait_http "$COORD/healthz"

./target/debug/plurifold-agent run \
  --name worker-a \
  --performance 2 \
  --feature demo:role-left \
  --coordinator "$COORD" \
  --bind "127.0.0.1:${A_PORT}" \
  --advertise "$A_URL" \
  --store-dir "$TMP/a" \
  --heartbeat-interval-ms 150 \
  --poll-interval-ms 50 \
  >"$TMP/worker-a.log" 2>&1 &
A_PID=$!
PIDS+=("$A_PID")

./target/debug/plurifold-agent run \
  --name worker-b \
  --performance 1 \
  --feature demo:role-right \
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
  RESOURCES=$(./target/debug/plurifold resources --coordinator "$COORD")
  COUNT=$(printf '%s' "$RESOURCES" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["resources"]))')
  if [[ "$COUNT" == "2" ]]; then
    break
  fi
  sleep 0.05
done
[[ "${COUNT:-0}" == "2" ]]
A_ID=$(printf '%s' "$RESOURCES" | json_resource_id worker-a)
B_ID=$(printf '%s' "$RESOURCES" | json_resource_id worker-b)

./target/debug/plurifold link \
  --coordinator "$COORD" \
  --from "$A_ID" --to "$B_ID" \
  --rtt-ms 80 --bandwidth-mbps 100

printf 'hello-' >"$TMP/input-a"
printf 'fabric\n' >"$TMP/input-b"
OBJ_A=$(./target/debug/plurifold put --coordinator "$COORD" --agent "$A_URL" --file "$TMP/input-a")
OBJ_B=$(./target/debug/plurifold put --coordinator "$COORD" --agent "$A_URL" --file "$TMP/input-b")
TRANSFER_TASK=$(./target/debug/plurifold submit \
  --coordinator "$COORD" \
  --artifact builtin:concat \
  --input "$OBJ_A" --input "$OBJ_B" \
  --require-feature agent-name:worker-b \
  --compute-ms 2000)
./target/debug/plurifold wait --coordinator "$COORD" --task "$TRANSFER_TASK" --timeout-s 10 >/dev/null

EXPECTED=$(printf 'hello-fabric\n' | sha256sum | awk '{print $1}')
[[ -f "$TMP/b/sha256/$EXPECTED" ]]
BLOB_COUNT=$(find "$TMP/b/sha256" -type f | wc -l)
[[ "$BLOB_COUNT" -ge 3 ]]

echo "peer-transfer: ok (worker-b cached $BLOB_COUNT blobs)"

cat >"$TMP/cooperative-job.json" <<'JSON'
{
  "roles": [
    {
      "name": "left",
      "task": {
        "artifact": "builtin:sleep",
        "entrypoint": "run",
        "arguments": ["3000"],
        "requirements": {
          "architecture": null,
          "min_memory_bytes": 0,
          "accelerator": null,
          "required_features": ["demo:role-left"]
        },
        "effects": "Pure",
        "cost": {"compute_ms_on_reference": 3000.0, "output_bytes": 4}
      },
      "depends_on": []
    },
    {
      "name": "right",
      "task": {
        "artifact": "builtin:sleep",
        "entrypoint": "run",
        "arguments": ["3000"],
        "requirements": {
          "architecture": null,
          "min_memory_bytes": 0,
          "accelerator": null,
          "required_features": ["demo:role-right"]
        },
        "effects": "Pure",
        "cost": {"compute_ms_on_reference": 3000.0, "output_bytes": 4}
      },
      "depends_on": []
    },
    {
      "name": "join",
      "task": {
        "artifact": "builtin:concat",
        "entrypoint": "run",
        "requirements": {
          "architecture": null,
          "min_memory_bytes": 0,
          "accelerator": null,
          "required_features": ["demo:role-left"]
        },
        "effects": "Pure",
        "cost": {"compute_ms_on_reference": 100.0, "output_bytes": 8}
      },
      "depends_on": ["left", "right"]
    }
  ],
  "outputs": ["join"]
}
JSON

COOPERATIVE_JOB=$(./target/debug/plurifold job submit \
  --coordinator "$COORD" \
  --file "$TMP/cooperative-job.json")

LEFT_TASK=""
RIGHT_TASK=""
for _ in $(seq 1 100); do
  JOB_VIEW=$(./target/debug/plurifold job status --coordinator "$COORD" --job "$COOPERATIVE_JOB")
  LEFT_TASK=$(printf '%s' "$JOB_VIEW" | python3 -c 'import json,sys
name=sys.argv[1]
for role in json.load(sys.stdin)["roles"]:
    if role["name"] == name and isinstance(role["status"], dict) and "Submitted" in role["status"]:
        print(role["status"]["Submitted"])
        break' left)
  RIGHT_TASK=$(printf '%s' "$JOB_VIEW" | python3 -c 'import json,sys
name=sys.argv[1]
for role in json.load(sys.stdin)["roles"]:
    if role["name"] == name and isinstance(role["status"], dict) and "Submitted" in role["status"]:
        print(role["status"]["Submitted"])
        break' right)
  if [[ -n "$LEFT_TASK" && -n "$RIGHT_TASK" ]]; then
    break
  fi
  sleep 0.05
done
[[ -n "$LEFT_TASK" && -n "$RIGHT_TASK" ]]

LEFT_RESOURCE=""
RIGHT_RESOURCE=""
for _ in $(seq 1 100); do
  LEFT_VIEW=$(./target/debug/plurifold status --coordinator "$COORD" --task "$LEFT_TASK")
  RIGHT_VIEW=$(./target/debug/plurifold status --coordinator "$COORD" --task "$RIGHT_TASK")
  LEFT_RESOURCE=$(printf '%s' "$LEFT_VIEW" | json_running_resource)
  RIGHT_RESOURCE=$(printf '%s' "$RIGHT_VIEW" | json_running_resource)
  if [[ -n "$LEFT_RESOURCE" && -n "$RIGHT_RESOURCE" ]]; then
    break
  fi
  sleep 0.05
done
[[ "$LEFT_RESOURCE" == "$A_ID" ]]
[[ "$RIGHT_RESOURCE" == "$B_ID" ]]

./target/debug/plurifold job wait \
  --coordinator "$COORD" \
  --job "$COOPERATIVE_JOB" \
  --timeout-s 12 >/dev/null

COOPERATIVE_EXPECTED=$(printf 'donedone' | sha256sum | awk '{print $1}')
[[ -f "$TMP/a/sha256/$COOPERATIVE_EXPECTED" ]]
echo "cooperative-job: ok (left=$LEFT_RESOURCE, right=$RIGHT_RESOURCE, joined on worker-a)"

FAILURE_TASK=$(./target/debug/plurifold submit \
  --coordinator "$COORD" \
  --artifact builtin:sleep \
  --argument 3000 \
  --compute-ms 3000)

RUNNING_RESOURCE=""
for _ in $(seq 1 100); do
  VIEW=$(./target/debug/plurifold status --coordinator "$COORD" --task "$FAILURE_TASK")
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

./target/debug/plurifold wait --coordinator "$COORD" --task "$FAILURE_TASK" --timeout-s 12 >/dev/null

for _ in $(seq 1 50); do
  ACTIVE=$(./target/debug/plurifold resources --coordinator "$COORD" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["resources"]))')
  if [[ "$ACTIVE" == "1" ]]; then
    break
  fi
  sleep 0.05
done
[[ "${ACTIVE:-0}" == "1" ]]

echo "failure-recovery: ok (lost $RUNNING_RESOURCE, task completed on survivor)"
