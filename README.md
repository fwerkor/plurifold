# Plurifold

Plurifold is an experimental runtime for **elastic heterogeneous computing across weakly connected resources**. It targets CPUs, GPUs, NPUs, accelerators, edge devices, workstations, clusters, and cloud instances that may differ in architecture and performance, may be separated by WAN links, and may join or leave while an application is running.

The central rule is:

> **Location-transparent programming, topology-aware execution.**

Plurifold does not pretend that a 150 ms WAN link is shared memory. Applications describe tasks, objects, state, and execution constraints. The runtime keeps network topology, data locality, accelerator capabilities, failure risk, and startup cost as first-class scheduling inputs.

## Why another runtime?

Existing systems solve important parts of this problem: Legion provides a data-centric programming model, StarPU schedules heterogeneous tasks and data movement, Ray provides tasks/actors/objects and fault tolerance, Charm++ supports migratable objects, Globus Compute spans administrative domains, and WASI provides a portable component ABI. Plurifold explores the missing composition of these ideas for **highly heterogeneous, dynamically changing, weakly connected compute**.

The research hypothesis is that useful global scheduling requires more than placement. A WAN-aware runtime should be able to adapt **computation granularity** by batching, fusing, replicating, migrating, and checkpointing work according to communication/computation ratio.

## Core abstractions

- **Cooperative Job** — one logical objective decomposed into dependency-linked roles that may run concurrently on complementary resources.
- **Task** — one schedulable execution unit with declared inputs, requirements, retry semantics, arguments, and cost hints.
- **Object** — immutable/versioned data identified independently of its current physical location.
- **Resource** — a leased description of compute, memory, accelerators, runtimes, network position, and reliability.
- **Actor** — a logical stateful service whose state can be checkpointed and rebound to another resource (planned after the task/object core).
- **Stream** — long-lived flow with backpressure and topology-aware placement (planned).
- **Collective** — an explicit locality-sensitive primitive, never silently stretched across unsuitable WAN links (planned).

## Architecture

```text
Applications / libraries
        |
Programming model: Cooperative Job / Task / Object / Actor / Stream / Collective
        |
Logical planner
  role implementations | predicted placement | intermediate transfer
        |
Dynamic graph runtime
  dependencies | retry | fusion | batching | checkpoint
        |
Topology-aware scheduler
  compute | locality | RTT | bandwidth | startup | risk | cost
        |
Versioned object space + execution leases
        |
Resource fabric
 CPU | CUDA | ROCm | CANN/NPU | TPU | FPGA | WASI | native
        |
 laptop | workstation | datacenter | HPC | cloud | edge | WAN
```

## Repository status

The repository now contains a **v0.11 networked research prototype**, not only a design scaffold:

- typed Resource, Task, Object, Topology, membership-lease, and execution-lease models;
- a topology-aware placement cost model;
- a real HTTP/JSON coordinator control plane;
- hot-pluggable agents with membership heartbeats and epoch-based reincarnation safety;
- renewable work leases for long-running tasks;
- cooperative jobs whose independent roles can execute concurrently on different resources and whose dependent roles consume predecessor outputs;
- a logical-job planner that previews per-role/per-shard implementation and resource choices using capabilities, compute cost, input locality, predicted intermediate transfers, topology, and predicted resource availability;
- fixed and automatic sharded logical roles: numeric `shards: N` preserves explicit independent contributions, while auto mode can choose a byte-range shard count from current resources and topology;
- deterministic fan-in: shard outputs are exposed to downstream roles in shard-index order even when child Tasks complete out of order;
- concrete byte-range and record-aligned partitioning for auto roles: applications/domain libraries may supply explicit record offsets, child Tasks receive complete record spans, scheduler transfer cost uses the corresponding byte ranges, and remote agents fetch only those ranges over HTTP;
- bounded automatic parallelism: the planner evaluates candidate counts, heterogeneous implementation/resource choices, declared per-child overhead, and a minimum modeled makespan gain before adding shards;
- topology-aware hierarchical reduction for sharded roles: adjacent outputs within a declared RTT locality threshold are reduced first, then remaining ordered partials are reduced across domains; reducer Tasks use the same lease/CAS/retry path as ordinary Tasks;
- dynamic ready-time replanning: `auto-submit` keeps implementation alternatives live and chooses a role's concrete Task only after its real predecessor Objects exist, using the then-current resources and topology;
- automatic peer topology discovery: agents measure RTT plus bounded practical HTTP throughput, report reachability to the coordinator, refresh links periodically, and withdraw automatic links when probes fail;
- topology-driven graph coarsening for safe linear logical-role chains: the runtime discovers a maximal single-successor/single-dependency chain and can fuse a cost-optimal Pure builtin-family prefix into one multi-stage `TaskPipeline`;
- in-memory fused-stage handoff: intermediate stage bytes stay inside one agent execution lease instead of becoming coordinator-visible/CAS-published Objects; only the final fused stage publishes an Object, which can then feed an unfused suffix;
- explainable fusion decisions in job status, including the ordered fusion group (`chain_roles`), each role's `stage_index`, estimated avoided transfer time, and modeled end-to-end delta versus fully separate execution;
- a SHA-256 content-addressed local object cache;
- direct agent-to-agent HTTP object transfer with digest verification and replica registration;
- replay-safe retry after worker loss, with non-replay-safe tasks entering `Uncertain`;
- a small builtin executor (`identity`, `concat`, `echo`, `sleep`, plus shard-observation test operations) used to exercise the runtime without hiding unimplemented portability behind a fake generic executor;
- a CLI for object publication, task/cooperative-job submission, planner preview/auto-submit, resource inspection, and manual topology overrides;
- a multi-process E2E test that verifies direct peer transfer, explicit cooperative execution, hot-join-driven replanning, fixed fan-out, automatic byte-range fan-out, record-aligned A/B/C sharding with local-first two-level reduction and exact numeric result, graph fusion/coarsening, cross-resource joins, and takeover after worker loss.

Still intentionally missing: authentication/TLS, durable coordinator state, native/WASI sandboxed executors, accelerator adapters, automatic record-index discovery, tensor-aware partitioners, commutative/quorum/streaming reductions, arbitrary general-DAG fusion/batching, speculative replication/migration, and production-grade observability.

## Quick start

Run all unit tests and the real multi-process smoke test:

```bash
cargo test --workspace
./scripts/e2e-local.sh
```

Inspect the original topology-aware scheduling demo:

```bash
cargo run -p plurifold-cli -- demo
```

For manual networked use, start a coordinator and one or more agents:

```bash
cargo run -p plurifold-coordinator -- --bind 127.0.0.1:8080
cargo run -p plurifold-agent -- run \
  --name worker-a \
  --coordinator http://127.0.0.1:8080 \
  --bind 127.0.0.1:8081 \
  --advertise http://127.0.0.1:8081
```

The defaults bind to loopback. The current control plane is unauthenticated, so do not expose it to an untrusted network.

Agents automatically discover other advertised data endpoints. Every unordered peer pair has one probing agent; it periodically takes three lightweight RTT samples and one bounded throughput sample. The default probe interval is 30 seconds and can be changed with `--probe-interval-ms`. Automatic discovery requires the advertised agent endpoints to be mutually routable; if a probe becomes unreachable, that automatic link is removed. `plurifold link` remains available as an explicit operator override and is not overwritten by automatic measurements.

Cooperative jobs are submitted as JSON role graphs:

```bash
cargo run -p plurifold-cli -- job submit \
  --coordinator http://127.0.0.1:8080 \
  --file examples/cooperative-job.json
```

Roles without dependencies become schedulable together. A dependent role is materialized only after all predecessors finish, and their output Object IDs are appended to its inputs.

For automatic implementation/placement planning, submit a logical role graph whose roles contain alternative Task templates:

```bash
cargo run -p plurifold-cli -- job plan \
  --coordinator http://127.0.0.1:8080 \
  --file examples/logical-job.json

cargo run -p plurifold-cli -- job auto-submit \
  --coordinator http://127.0.0.1:8080 \
  --file examples/logical-job.json
```

A sharded role uses the same command surface:

```bash
cargo run -p plurifold-cli -- job auto-submit \
  --coordinator http://127.0.0.1:8080 \
  --file examples/sharded-job.json
```

`job plan` is a snapshot preview. `job auto-submit` instead stores the `LogicalJobSpec` and replans work when dependencies actually complete. A ready role with no currently feasible implementation stays `Ready` and is retried as resources/topology change.

A logical role may still set `shards: N` (default `1`). That fixed form preserves the previous numeric-shard contract: N independent contributions receive the same logical inputs, `TaskShard { index, count }` identifies each contribution, and cost hints are per child.

For partitionable work, v0.11 accepts an automatic policy. Raw bytes use the existing form:

```json
"shards": {
  "mode": "auto",
  "max_shards": 8,
  "partition": {"kind": "byte_range", "input": 0},
  "per_shard_overhead_ms": 2.0,
  "min_gain_ratio": 0.05
}
```

The partition input must be an explicit `TaskTemplate.inputs` Object and the same Object across implementation alternatives. In auto mode the template compute/output hints describe the single-shard total; Plurifold scales them by the declared work partition, adds per-child overhead, evaluates shard counts from 1 through the configured bound, and only accepts additional parallelism when modeled incremental role makespan improves enough. Concrete partitions are visible in `job plan` and live job status.

For record-oriented inputs, the application or a domain library may provide an explicit immutable boundary index:

```json
"partition": {
  "kind": "records",
  "input": 0,
  "offsets": [0, 5, 11, 18]
}
```

`offsets` contains every record start plus the final Object length. Plurifold partitions by **record count**, while the generated child `TaskShard` also carries the exact byte offset/length needed for transport. The referenced Object must already be published, offsets must start at 0, be strictly increasing, and end at its byte size. Plurifold does not parse CSV/JSONL or invent record boundaries itself.

Remote agents issue HTTP Range requests and verify a SHA-256 digest for the returned range; partial bytes are never registered as a full Object replica. A local agent that already owns the complete Object slices it locally.

A sharded role can also declare an ordered-associative reduction:

```json
"reduction": {
  "name": "sum-u64",
  "task": {
    "artifact": "builtin:sum-u64",
    "entrypoint": "run",
    "requirements": {
      "architecture": null,
      "min_memory_bytes": 0,
      "accelerator": null,
      "required_features": []
    },
    "effects": "Pure",
    "cost": {"compute_ms_on_reference": 10.0, "output_bytes": 8}
  },
  "max_fan_in": 4,
  "locality_rtt_ms": 2.0
}
```

After all shard outputs exist, Plurifold keeps their shard-index order, forms contiguous pairwise-local output groups whose replicas are within the locality RTT threshold, and materializes those reducer Tasks first. If a level has no local reduction opportunity, it falls back to ordered cross-domain groups up to `max_fan_in`, repeating until one Object remains. Reducers must be `Pure`, declare no static inputs, and be ordered-associative because the runtime may change parentheses but never input order. An expired reducer lease is retried without rerunning completed shards.

This still is not arbitrary semantic understanding: tensor axes, automatic record-index construction, commutative reordering, quorum reduction, and source-program decomposition remain outside the v0.11 claim.

For unsharded roles, v0.11 retains adaptive linear-chain fusion. Fixed multi-shard and auto-sharded roles are explicit graph boundaries and are not absorbed into a fused `TaskPipeline`. Predicted resources remain advisory rather than hard bindings, and lease-time scheduling is authoritative.

## Design invariants

1. No global `world_size` exists.
2. Membership is lease-based; node loss must not require a global barrier.
3. Scheduling is data- and topology-aware, not merely load-aware.
4. Automatic retry is allowed only when task effect semantics make it safe.
5. Objects are logical identities; physical replicas are replaceable locations.
6. Fine-grained collectives are valid only inside a suitable latency domain.
7. The runtime may change graph granularity, but not application-visible semantics.
8. Hardware-specific execution remains backend-native; portability is provided at the task/state ABI boundary.
9. Coordinator state, not a worker-provided timestamp, is authoritative for execution-lease validity.

## Documents

- [Architecture](docs/architecture.md)
- [Programming model](docs/programming-model.md)
- [Scheduler](docs/scheduler.md)
- [Failure and elasticity model](docs/failure-model.md)
- [Protocol](docs/protocol.md)
- [Security and trust model](docs/security.md)
- [Related systems](docs/related-work.md)
- [Research plan](docs/research-plan.md)
- [Roadmap](docs/roadmap.md)

## License

Apache-2.0.
