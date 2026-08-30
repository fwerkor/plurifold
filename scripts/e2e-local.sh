#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

BASE_PORT=${PLURIFOLD_E2E_PORT_BASE:-19180}
COORD_PORT=$BASE_PORT
A_PORT=$((BASE_PORT + 1))
B_PORT=$((BASE_PORT + 2))
C_PORT=$((BASE_PORT + 3))
COORD="http://127.0.0.1:${COORD_PORT}"
A_URL="http://127.0.0.1:${A_PORT}"
B_URL="http://127.0.0.1:${B_PORT}"
C_URL="http://127.0.0.1:${C_PORT}"
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

json_role_task() {
  local name=$1
  python3 -c 'import json,sys
name=sys.argv[1]
for role in json.load(sys.stdin)["roles"]:
    status=role["status"]
    if role["name"] == name and isinstance(status, dict) and "Submitted" in status:
        print(status["Submitted"])
        break' "$name"
}

write_fusion_job() {
  local file=$1
  local output_bytes=$2
  cat >"$file" <<JSON
{
  "roles": [
    {
      "name": "producer",
      "implementations": [{
        "name": "producer-builtin",
        "task": {
          "artifact": "builtin:sleep",
          "entrypoint": "run",
          "arguments": ["600"],
          "requirements": {
            "architecture": null,
            "min_memory_bytes": 0,
            "accelerator": null,
            "required_features": ["fusion:producer"]
          },
          "effects": "Pure",
          "cost": {"compute_ms_on_reference": 100.0, "output_bytes": $output_bytes}
        }
      }],
      "depends_on": []
    },
    {
      "name": "consumer",
      "implementations": [{
        "name": "consumer-builtin",
        "task": {
          "artifact": "builtin:identity",
          "entrypoint": "run",
          "requirements": {
            "architecture": null,
            "min_memory_bytes": 0,
            "accelerator": null,
            "required_features": ["fusion:consumer"]
          },
          "effects": "Pure",
          "cost": {"compute_ms_on_reference": 1000.0, "output_bytes": 4}
        }
      }],
      "depends_on": ["producer"]
    }
  ],
  "outputs": ["consumer"]
}
JSON
}

write_chain_fusion_job() {
  local file=$1
  local middle_output_bytes=$2
  local final_compute_ms=$3
  cat >"$file" <<JSON
{
  "roles": [
    {
      "name": "producer",
      "implementations": [{
        "name": "producer-builtin",
        "task": {
          "artifact": "builtin:sleep",
          "entrypoint": "run",
          "arguments": ["600"],
          "requirements": {
            "architecture": null,
            "min_memory_bytes": 0,
            "accelerator": null,
            "required_features": ["fusion:producer"]
          },
          "effects": "Pure",
          "cost": {"compute_ms_on_reference": 100.0, "output_bytes": 17179869184}
        }
      }],
      "depends_on": []
    },
    {
      "name": "middle",
      "implementations": [{
        "name": "middle-builtin",
        "task": {
          "artifact": "builtin:identity",
          "entrypoint": "run",
          "requirements": {
            "architecture": null,
            "min_memory_bytes": 0,
            "accelerator": null,
            "required_features": ["fusion:middle"]
          },
          "effects": "Pure",
          "cost": {"compute_ms_on_reference": 100.0, "output_bytes": $middle_output_bytes}
        }
      }],
      "depends_on": ["producer"]
    },
    {
      "name": "consumer",
      "implementations": [{
        "name": "consumer-builtin",
        "task": {
          "artifact": "builtin:echo",
          "entrypoint": "run",
          "arguments": ["chain-final"],
          "requirements": {
            "architecture": null,
            "min_memory_bytes": 0,
            "accelerator": null,
            "required_features": ["fusion:consumer"]
          },
          "effects": "Pure",
          "cost": {"compute_ms_on_reference": $final_compute_ms, "output_bytes": 4}
        }
      }],
      "depends_on": ["middle"]
    }
  ],
  "outputs": ["consumer"]
}
JSON
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
  --feature fusion:producer \
  --feature fusion:middle \
  --feature fusion:consumer \
  --coordinator "$COORD" \
  --bind "127.0.0.1:${A_PORT}" \
  --advertise "$A_URL" \
  --store-dir "$TMP/a" \
  --heartbeat-interval-ms 150 \
  --poll-interval-ms 50 \
  --probe-interval-ms 100 \
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
  --probe-interval-ms 100 \
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
  LEFT_TASK=$(printf '%s' "$JOB_VIEW" | json_role_task left)
  RIGHT_TASK=$(printf '%s' "$JOB_VIEW" | json_role_task right)
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

printf 'auto-left|' >"$TMP/auto-left"
printf 'auto-right\n' >"$TMP/auto-right"
AUTO_LEFT_OBJ=$(./target/debug/plurifold put --coordinator "$COORD" --agent "$A_URL" --file "$TMP/auto-left")
AUTO_RIGHT_OBJ=$(./target/debug/plurifold put --coordinator "$COORD" --agent "$B_URL" --file "$TMP/auto-right")

cat >"$TMP/logical-job.json" <<JSON
{
  "roles": [
    {
      "name": "left",
      "implementations": [
        {
          "name": "left-native",
          "task": {
            "artifact": "builtin:sleep",
            "entrypoint": "run",
            "arguments": ["1500"],
            "inputs": ["$AUTO_LEFT_OBJ"],
            "requirements": {
              "architecture": null,
              "min_memory_bytes": 0,
              "accelerator": null,
              "required_features": ["demo:role-left"]
            },
            "effects": "Pure",
            "cost": {"compute_ms_on_reference": 2000.0, "output_bytes": 104857600}
          }
        },
        {
          "name": "left-fallback",
          "task": {
            "artifact": "builtin:sleep",
            "entrypoint": "run",
            "arguments": ["1500"],
            "inputs": ["$AUTO_LEFT_OBJ"],
            "requirements": {
              "architecture": null,
              "min_memory_bytes": 0,
              "accelerator": null,
              "required_features": ["demo:role-right"]
            },
            "effects": "Pure",
            "cost": {"compute_ms_on_reference": 12000.0, "output_bytes": 104857600}
          }
        }
      ],
      "depends_on": []
    },
    {
      "name": "right",
      "implementations": [
        {
          "name": "right-native",
          "task": {
            "artifact": "builtin:sleep",
            "entrypoint": "run",
            "arguments": ["1500"],
            "inputs": ["$AUTO_RIGHT_OBJ"],
            "requirements": {
              "architecture": null,
              "min_memory_bytes": 0,
              "accelerator": null,
              "required_features": ["demo:role-right"]
            },
            "effects": "Pure",
            "cost": {"compute_ms_on_reference": 2000.0, "output_bytes": 1048576}
          }
        },
        {
          "name": "right-fallback",
          "task": {
            "artifact": "builtin:sleep",
            "entrypoint": "run",
            "arguments": ["1500"],
            "inputs": ["$AUTO_RIGHT_OBJ"],
            "requirements": {
              "architecture": null,
              "min_memory_bytes": 0,
              "accelerator": null,
              "required_features": ["demo:role-left"]
            },
            "effects": "Pure",
            "cost": {"compute_ms_on_reference": 12000.0, "output_bytes": 1048576}
          }
        }
      ],
      "depends_on": []
    },
    {
      "name": "join",
      "implementations": [
        {
          "name": "join-left",
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
            "cost": {"compute_ms_on_reference": 100.0, "output_bytes": 21}
          }
        },
        {
          "name": "join-right",
          "task": {
            "artifact": "builtin:concat",
            "entrypoint": "run",
            "requirements": {
              "architecture": null,
              "min_memory_bytes": 0,
              "accelerator": null,
              "required_features": ["demo:role-right"]
            },
            "effects": "Pure",
            "cost": {"compute_ms_on_reference": 100.0, "output_bytes": 21}
          }
        },
        {
          "name": "join-fast",
          "task": {
            "artifact": "builtin:concat",
            "entrypoint": "run",
            "requirements": {
              "architecture": null,
              "min_memory_bytes": 0,
              "accelerator": null,
              "required_features": ["demo:join-fast"]
            },
            "effects": "Pure",
            "cost": {"compute_ms_on_reference": 100.0, "output_bytes": 21}
          }
        }
      ],
      "depends_on": ["left", "right"]
    }
  ],
  "outputs": ["join"]
}
JSON

AUTO_PLAN=$(./target/debug/plurifold job plan --coordinator "$COORD" --file "$TMP/logical-job.json")
printf '%s' "$AUTO_PLAN" | python3 -c 'import json,sys
data=json.load(sys.stdin)
expected={"left":("left-native",sys.argv[1]),"right":("right-native",sys.argv[2]),"join":("join-left",sys.argv[1])}
seen={role["name"]:(role["implementation"],role["placement"]["resource_id"]) for role in data["roles"]}
assert seen == expected, (seen, expected)
join=next(role for role in data["roles"] if role["name"] == "join")
assert join["placement"]["input_transfer_ms"] > 0
' "$A_ID" "$B_ID"

AUTO_JOB=$(./target/debug/plurifold job auto-submit --coordinator "$COORD" --file "$TMP/logical-job.json")
AUTO_LEFT_TASK=""
AUTO_RIGHT_TASK=""
for _ in $(seq 1 100); do
  AUTO_VIEW=$(./target/debug/plurifold job status --coordinator "$COORD" --job "$AUTO_JOB")
  AUTO_LEFT_TASK=$(printf '%s' "$AUTO_VIEW" | json_role_task left)
  AUTO_RIGHT_TASK=$(printf '%s' "$AUTO_VIEW" | json_role_task right)
  if [[ -n "$AUTO_LEFT_TASK" && -n "$AUTO_RIGHT_TASK" ]]; then
    break
  fi
  sleep 0.05
done
[[ -n "$AUTO_LEFT_TASK" && -n "$AUTO_RIGHT_TASK" ]]

AUTO_LEFT_RESOURCE=""
AUTO_RIGHT_RESOURCE=""
for _ in $(seq 1 100); do
  AUTO_LEFT_VIEW=$(./target/debug/plurifold status --coordinator "$COORD" --task "$AUTO_LEFT_TASK")
  AUTO_RIGHT_VIEW=$(./target/debug/plurifold status --coordinator "$COORD" --task "$AUTO_RIGHT_TASK")
  AUTO_LEFT_RESOURCE=$(printf '%s' "$AUTO_LEFT_VIEW" | json_running_resource)
  AUTO_RIGHT_RESOURCE=$(printf '%s' "$AUTO_RIGHT_VIEW" | json_running_resource)
  if [[ -n "$AUTO_LEFT_RESOURCE" && -n "$AUTO_RIGHT_RESOURCE" ]]; then
    break
  fi
  sleep 0.05
done
[[ "$AUTO_LEFT_RESOURCE" == "$A_ID" ]]
[[ "$AUTO_RIGHT_RESOURCE" == "$B_ID" ]]

./target/debug/plurifold-agent run \
  --name worker-c \
  --performance 20 \
  --feature demo:join-fast \
  --feature fusion:middle \
  --feature fusion:consumer \
  --coordinator "$COORD" \
  --bind "127.0.0.1:${C_PORT}" \
  --advertise "$C_URL" \
  --store-dir "$TMP/c" \
  --heartbeat-interval-ms 150 \
  --poll-interval-ms 50 \
  --probe-interval-ms 100 \
  >"$TMP/worker-c.log" 2>&1 &
C_PID=$!
PIDS+=("$C_PID")
wait_http "$C_URL/healthz"

for _ in $(seq 1 100); do
  RESOURCES=$(./target/debug/plurifold resources --coordinator "$COORD")
  COUNT=$(printf '%s' "$RESOURCES" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["resources"]))')
  if [[ "$COUNT" == "3" ]]; then
    break
  fi
  sleep 0.05
done
[[ "${COUNT:-0}" == "3" ]]
C_ID=$(printf '%s' "$RESOURCES" | json_resource_id worker-c)

./target/debug/plurifold job wait --coordinator "$COORD" --job "$AUTO_JOB" --timeout-s 10 >/dev/null
AUTO_EXPECTED=$(printf 'auto-left|auto-right\n' | sha256sum | awk '{print $1}')
[[ -f "$TMP/c/sha256/$AUTO_EXPECTED" ]]
AUTO_FINAL_VIEW=$(./target/debug/plurifold job status --coordinator "$COORD" --job "$AUTO_JOB")
printf '%s' "$AUTO_FINAL_VIEW" | python3 -c 'import json,sys
data=json.load(sys.stdin)
join=next(role for role in data["roles"] if role["name"] == "join")
assert join["implementation"] == "join-fast", join
assert join["planned_resource"] == sys.argv[1], join
' "$C_ID"
echo "dynamic-replan: ok (automatic topology; preview join-left->$A_ID, ready-time join-fast->$C_ID after hot join)"

write_fusion_job "$TMP/fusion-low.json" 1
LOW_FUSION_JOB=$(./target/debug/plurifold job auto-submit \
  --coordinator "$COORD" --file "$TMP/fusion-low.json")
LOW_FUSION_TASK=""
for _ in $(seq 1 100); do
  LOW_FUSION_VIEW=$(./target/debug/plurifold job status \
    --coordinator "$COORD" --job "$LOW_FUSION_JOB")
  LOW_FUSION_TASK=$(printf '%s' "$LOW_FUSION_VIEW" | json_role_task producer)
  if [[ -n "$LOW_FUSION_TASK" ]]; then
    break
  fi
  sleep 0.02
done
[[ -n "$LOW_FUSION_TASK" ]]
printf '%s' "$LOW_FUSION_VIEW" | python3 -c 'import json,sys
data=json.load(sys.stdin)
roles={role["name"]:role for role in data["roles"]}
assert roles["producer"]["fusion"] is None, roles["producer"]
assert roles["consumer"]["status"] == "Waiting", roles["consumer"]
'
LOW_FUSION_TASK_VIEW=$(./target/debug/plurifold status \
  --coordinator "$COORD" --task "$LOW_FUSION_TASK")
printf '%s' "$LOW_FUSION_TASK_VIEW" | python3 -c 'import json,sys
task=json.load(sys.stdin)["task"]
assert "pipeline" not in task, task
'
./target/debug/plurifold job wait \
  --coordinator "$COORD" --job "$LOW_FUSION_JOB" --timeout-s 10 >/dev/null
echo "graph-granularity: low-cost edge kept separate"

write_fusion_job "$TMP/fusion-high.json" 17179869184
HIGH_FUSION_JOB=$(./target/debug/plurifold job auto-submit \
  --coordinator "$COORD" --file "$TMP/fusion-high.json")
HIGH_PRODUCER_TASK=""
HIGH_CONSUMER_TASK=""
for _ in $(seq 1 100); do
  HIGH_FUSION_VIEW=$(./target/debug/plurifold job status \
    --coordinator "$COORD" --job "$HIGH_FUSION_JOB")
  HIGH_PRODUCER_TASK=$(printf '%s' "$HIGH_FUSION_VIEW" | json_role_task producer)
  HIGH_CONSUMER_TASK=$(printf '%s' "$HIGH_FUSION_VIEW" | json_role_task consumer)
  if [[ -n "$HIGH_PRODUCER_TASK" && -n "$HIGH_CONSUMER_TASK" ]]; then
    break
  fi
  sleep 0.02
done
[[ -n "$HIGH_PRODUCER_TASK" && "$HIGH_PRODUCER_TASK" == "$HIGH_CONSUMER_TASK" ]]
printf '%s' "$HIGH_FUSION_VIEW" | python3 -c 'import json,sys
data=json.load(sys.stdin)
roles={role["name"]:role for role in data["roles"]}
producer=roles["producer"]
consumer=roles["consumer"]
assert producer["fusion"]["chain_roles"] == ["producer", "consumer"], producer
assert producer["fusion"]["stage_index"] == 0, producer
assert consumer["fusion"]["stage_index"] == 1, consumer
assert producer["fusion"]["estimated_avoided_transfer_ms"] > 20, producer
assert producer["fusion"]["estimated_vs_separate_ms"] >= 0, producer
assert producer["planned_resource"] == sys.argv[1], producer
assert consumer["status"] == producer["status"], (producer, consumer)
' "$A_ID"
HIGH_FUSION_TASK_VIEW=$(./target/debug/plurifold status \
  --coordinator "$COORD" --task "$HIGH_PRODUCER_TASK")
printf '%s' "$HIGH_FUSION_TASK_VIEW" | python3 -c 'import json,sys
pipeline=json.load(sys.stdin)["task"]["pipeline"]
assert len(pipeline["stages"]) == 2, pipeline
'
./target/debug/plurifold job wait \
  --coordinator "$COORD" --job "$HIGH_FUSION_JOB" --timeout-s 10 >/dev/null
FUSION_EXPECTED=$(printf 'done' | sha256sum | awk '{print $1}')
[[ -f "$TMP/a/sha256/$FUSION_EXPECTED" ]]
echo "graph-fusion: high-cost edge fused into one task on worker-a"

write_chain_fusion_job "$TMP/chain-fusion-full.json" 17179869184 100
CHAIN_FUSION_JOB=$(./target/debug/plurifold job auto-submit \
  --coordinator "$COORD" --file "$TMP/chain-fusion-full.json")
CHAIN_PRODUCER_TASK=""
CHAIN_MIDDLE_TASK=""
CHAIN_CONSUMER_TASK=""
for _ in $(seq 1 100); do
  CHAIN_FUSION_VIEW=$(./target/debug/plurifold job status \
    --coordinator "$COORD" --job "$CHAIN_FUSION_JOB")
  CHAIN_PRODUCER_TASK=$(printf '%s' "$CHAIN_FUSION_VIEW" | json_role_task producer)
  CHAIN_MIDDLE_TASK=$(printf '%s' "$CHAIN_FUSION_VIEW" | json_role_task middle)
  CHAIN_CONSUMER_TASK=$(printf '%s' "$CHAIN_FUSION_VIEW" | json_role_task consumer)
  if [[ -n "$CHAIN_PRODUCER_TASK" && -n "$CHAIN_MIDDLE_TASK" && -n "$CHAIN_CONSUMER_TASK" ]]; then
    break
  fi
  sleep 0.02
done
[[ -n "$CHAIN_PRODUCER_TASK" ]]
[[ "$CHAIN_PRODUCER_TASK" == "$CHAIN_MIDDLE_TASK" ]]
[[ "$CHAIN_PRODUCER_TASK" == "$CHAIN_CONSUMER_TASK" ]]
printf '%s' "$CHAIN_FUSION_VIEW" | python3 -c 'import json,sys
data=json.load(sys.stdin)
roles={role["name"]:role for role in data["roles"]}
expected=["producer", "middle", "consumer"]
for index,name in enumerate(expected):
    fusion=roles[name]["fusion"]
    assert fusion["chain_roles"] == expected, (name, fusion)
    assert fusion["stage_index"] == index, (name, fusion)
    assert roles[name]["planned_resource"] == sys.argv[1], roles[name]
' "$A_ID"
CHAIN_FUSION_TASK_VIEW=$(./target/debug/plurifold status \
  --coordinator "$COORD" --task "$CHAIN_PRODUCER_TASK")
printf '%s' "$CHAIN_FUSION_TASK_VIEW" | python3 -c 'import json,sys
pipeline=json.load(sys.stdin)["task"]["pipeline"]
assert len(pipeline["stages"]) == 3, pipeline
'
./target/debug/plurifold job wait \
  --coordinator "$COORD" --job "$CHAIN_FUSION_JOB" --timeout-s 10 >/dev/null
CHAIN_EXPECTED=$(printf 'chain-final' | sha256sum | awk '{print $1}')
[[ -f "$TMP/a/sha256/$CHAIN_EXPECTED" ]]
echo "graph-chain-fusion: three-stage chain fused into one task on worker-a"

write_chain_fusion_job "$TMP/chain-fusion-prefix.json" 1 10000
PREFIX_FUSION_JOB=$(./target/debug/plurifold job auto-submit \
  --coordinator "$COORD" --file "$TMP/chain-fusion-prefix.json")
PREFIX_PRODUCER_TASK=""
PREFIX_MIDDLE_TASK=""
for _ in $(seq 1 100); do
  PREFIX_FUSION_VIEW=$(./target/debug/plurifold job status \
    --coordinator "$COORD" --job "$PREFIX_FUSION_JOB")
  PREFIX_PRODUCER_TASK=$(printf '%s' "$PREFIX_FUSION_VIEW" | json_role_task producer)
  PREFIX_MIDDLE_TASK=$(printf '%s' "$PREFIX_FUSION_VIEW" | json_role_task middle)
  if [[ -n "$PREFIX_PRODUCER_TASK" && -n "$PREFIX_MIDDLE_TASK" ]]; then
    break
  fi
  sleep 0.02
done
[[ -n "$PREFIX_PRODUCER_TASK" && "$PREFIX_PRODUCER_TASK" == "$PREFIX_MIDDLE_TASK" ]]
printf '%s' "$PREFIX_FUSION_VIEW" | python3 -c 'import json,sys
data=json.load(sys.stdin)
roles={role["name"]:role for role in data["roles"]}
expected=["producer", "middle"]
assert roles["producer"]["fusion"]["chain_roles"] == expected, roles["producer"]
assert roles["middle"]["fusion"]["chain_roles"] == expected, roles["middle"]
assert roles["consumer"]["status"] == "Waiting", roles["consumer"]
'
PREFIX_FUSION_TASK_VIEW=$(./target/debug/plurifold status \
  --coordinator "$COORD" --task "$PREFIX_PRODUCER_TASK")
printf '%s' "$PREFIX_FUSION_TASK_VIEW" | python3 -c 'import json,sys
pipeline=json.load(sys.stdin)["task"]["pipeline"]
assert len(pipeline["stages"]) == 2, pipeline
'
./target/debug/plurifold job wait \
  --coordinator "$COORD" --job "$PREFIX_FUSION_JOB" --timeout-s 10 >/dev/null
echo "graph-chain-prefix: expensive tail kept outside the two-stage fused prefix"

kill "$C_PID"
C_PID=0
for _ in $(seq 1 50); do
  ACTIVE=$(./target/debug/plurifold resources --coordinator "$COORD" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["resources"]))')
  if [[ "$ACTIVE" == "2" ]]; then
    break
  fi
  sleep 0.05
done
[[ "${ACTIVE:-0}" == "2" ]]

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
