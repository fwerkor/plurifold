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

The repository now contains a **v0.3 networked research prototype**, not only a design scaffold:

- typed Resource, Task, Object, Topology, membership-lease, and execution-lease models;
- a topology-aware placement cost model;
- a real HTTP/JSON coordinator control plane;
- hot-pluggable agents with membership heartbeats and epoch-based reincarnation safety;
- renewable work leases for long-running tasks;
- cooperative jobs whose independent roles can execute concurrently on different resources and whose dependent roles consume predecessor outputs;
- a SHA-256 content-addressed local object cache;
- direct agent-to-agent HTTP object transfer with digest verification and replica registration;
- replay-safe retry after worker loss, with non-replay-safe tasks entering `Uncertain`;
- a small builtin executor (`identity`, `concat`, `echo`, `sleep`) used to exercise the runtime without hiding unimplemented portability behind a fake generic executor;
- a CLI for object publication, task and cooperative-job submission/status, resource inspection, and topology links;
- a multi-process E2E test that verifies direct peer transfer, two-resource cooperative execution with a join role, and takeover after worker loss.

Still intentionally missing: authentication/TLS, durable coordinator state, active RTT/bandwidth probing, native/WASI sandboxed executors, accelerator adapters, graph rewriting, and production-grade observability.

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

Cooperative jobs are submitted as JSON role graphs:

```bash
cargo run -p plurifold-cli -- job submit \
  --coordinator http://127.0.0.1:8080 \
  --file examples/cooperative-job.json
```

Roles without dependencies become schedulable together. A dependent role is materialized only after all predecessors finish, and their output Object IDs are appended to its inputs.

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
