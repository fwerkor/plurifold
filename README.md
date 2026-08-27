# Mosaic Fabric

Mosaic Fabric is an experimental runtime for **elastic heterogeneous computing across weakly connected resources**. It targets a setting where CPUs, GPUs, NPUs, accelerators, edge devices, workstations, clusters, and cloud instances may differ in architecture and performance, may be separated by WAN links, and may join or leave while an application is running.

The central rule is:

> **Location-transparent programming, topology-aware execution.**

Mosaic does not pretend that a 150 ms WAN link is shared memory. Applications describe tasks, objects, state, and execution constraints. The runtime keeps network topology, data locality, accelerator capabilities, failure risk, and startup cost as first-class scheduling inputs.

## Why another runtime?

Existing systems solve important parts of this problem: Legion provides a data-centric programming model, StarPU schedules heterogeneous tasks and data movement, Ray provides tasks/actors/objects and fault tolerance, Charm++ supports migratable objects, Globus Compute spans administrative domains, and WASI provides a portable component ABI. Mosaic explores the missing composition of these ideas for **highly heterogeneous, dynamically changing, weakly connected compute**.

The initial research hypothesis is that useful global scheduling requires more than placement. A WAN-aware runtime should be able to adapt **computation granularity** by batching, fusing, replicating, migrating, and checkpointing work according to communication/computation ratio.

## Core abstractions

- **Task** — a schedulable computation with declared inputs, outputs, requirements, retry semantics, and cost hints.
- **Object** — immutable/versioned data identified independently of its current physical location.
- **Resource** — a leased description of compute, memory, accelerators, runtimes, network position, and reliability.
- **Actor** — a logical stateful service whose state can be checkpointed and rebound to another resource (planned after the task/object core).
- **Stream** — long-lived flow with backpressure and topology-aware placement (planned).
- **Collective** — an explicit locality-sensitive primitive, never silently stretched across unsuitable WAN links (planned).

## Architecture

```text
Applications / libraries
        |
Programming model: Task / Object / Actor / Stream / Collective
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

This repository currently contains the **v0 design scaffold and a compilable control-plane prototype**:

- typed resource, task, object, topology, and lease models;
- a topology-aware placement cost model;
- an in-memory elastic Fabric runtime that supports resource join/leave;
- explicit retry/effect semantics;
- a graph-fusion advisor for coarse WAN execution;
- a protocol sketch for coordinator/agent communication;
- an agent capability descriptor and CLI demo;
- unit tests and CI.

It deliberately does **not** yet contain a production RPC layer, distributed object store, accelerator executors, or security boundary. Those are staged in the roadmap rather than hidden behind premature abstractions.

## Quick start

```bash
cargo test --workspace
cargo run -p mosaic-cli -- demo
cargo run -p mosaic-agent -- describe --id workstation-a --memory-gib 64 --performance 8
```

## Design invariants

1. No global `world_size` exists.
2. Membership is lease-based; node loss must not require a global barrier.
3. Scheduling is data- and topology-aware, not merely load-aware.
4. Automatic retry is allowed only when task effect semantics make it safe.
5. Objects are logical identities; physical replicas are replaceable locations.
6. Fine-grained collectives are valid only inside a suitable latency domain.
7. The runtime may change graph granularity, but not application-visible semantics.
8. Hardware-specific execution remains backend-native; portability is provided at the task/state ABI boundary.

## Documents

- [Architecture](docs/architecture.md)
- [Programming model](docs/programming-model.md)
- [Scheduler](docs/scheduler.md)
- [Failure and elasticity model](docs/failure-model.md)
- [Protocol sketch](docs/protocol.md)
- [Security and trust model](docs/security.md)
- [Related systems](docs/related-work.md)
- [Research plan](docs/research-plan.md)
- [Roadmap](docs/roadmap.md)

## License

Apache-2.0.
