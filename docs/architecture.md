# Architecture

## 1. Problem statement

Plurifold targets a compute pool in which resources may have different ISAs, accelerator vendors, memory capacities, software stacks, performance, trust/reliability characteristics, and network distance. The pool is elastic: a machine can appear or disappear without forming or rebuilding a global process group.

The system is not a distributed shared-memory machine and not a WAN collective library. It is a **logical compute fabric** that maps coarse computations and data to physical resources while preserving topology as an execution concern.

## 2. Layering

### Application layer

Applications express computations through stable logical handles. Domain libraries may build richer models on top: scientific workflows, rendering, compilation, search, simulation, batch inference, and eventually asynchronous AI training.

### Programming model

The base model is a dynamic dataflow graph:

`Task(inputs: Object...) -> Object...`

Task and Object are the minimal portable units. Actor, Stream, and Collective are explicit extensions because they have different failure and placement semantics.

### Graph runtime

The runtime tracks dependencies and may perform semantics-preserving transformations:

- task batching;
- producer/consumer fusion;
- speculative replication;
- checkpoint insertion;
- migration at checkpoint boundaries;
- placement pinning for communication-heavy subgraphs.

Graph transformation is intentionally separate from the placement scheduler. This keeps the design testable: one component changes the graph; the other maps the resulting graph to resources.

### Scheduler

The scheduler predicts end-to-end cost rather than raw compute time:

`score = compute + input transfer + output pressure + queue + startup + failure risk + policy cost`

A faster accelerator can therefore lose to a slower local CPU when transferring inputs would dominate execution.

### Object space

Objects are immutable or explicitly versioned. A logical object may have multiple physical replicas. Object metadata contains size, content digest, encoding, and locations. The scheduler consumes this metadata; executors consume the bytes.

This design avoids requiring a single globally mounted filesystem and makes lineage-based reconstruction possible.

### Membership and leases

Resources register a descriptor and receive a membership lease. Heartbeats renew it. A missing heartbeat removes the resource from future placement without globally stopping other resources.

Task execution also uses leases. A coordinator may reschedule expired work if and only if retry semantics allow it. Late results carry an execution attempt identity and are rejected or deduplicated deterministically.

### Executors

Plurifold does not force hardware through one low-level instruction set. Executors are backend-native. A task artifact may eventually be:

- a WASI Component for portable CPU work;
- an OCI/native artifact;
- a Python environment;
- a backend-specific accelerator package;
- a domain-specific compiled graph.

The portable boundary is the task invocation/state ABI, not tensor kernels.

## 3. Latency domains

The topology model classifies links by observed RTT/bandwidth rather than administrative labels. Approximate default domains are:

| Class | Typical RTT | Intended primitives |
|---|---:|---|
| L0 | < 10 us | device/local accelerator operations |
| L1 | < 200 us | host/rack fine-grained coordination |
| L2 | < 2 ms | datacenter collectives and distributed memory patterns |
| L3 | < 20 ms | coarse RPC/pipeline stages |
| L4 | < 200 ms | task/object transfer, asynchronous state exchange |
| L5 | >= 200 ms | batch, replication, disconnected-tolerant work |

These thresholds are policy defaults, not correctness rules. Runtime measurements can override them.

## 4. Control plane vs data plane

The control plane exchanges small metadata: leases, descriptors, task specs, object metadata, heartbeats, and completion records. The data plane transfers object bytes directly or through pluggable storage/relay mechanisms.

Keeping the planes separate prevents the coordinator from becoming an unavoidable bulk-data bottleneck and allows future peer transfer or object-store backends.

## 5. Key invariants

### No fixed membership

There is no global rank assignment and no assumption that a job starts with a known final resource set.

### Explicit side effects

A pure or idempotent task may be replayed. A task with non-idempotent external side effects cannot be transparently retried. This property is part of the task specification, not an executor guess.

### Explicit communication sensitivity

A collective or tightly coupled subgraph can declare co-location constraints. The runtime should reject impossible placements instead of masking physical limits.

### Monotonic object publication

A completed object is immutable. New state creates a new logical version. This makes caching, replication, deduplication, and lineage reasoning simpler.

## 6. What Plurifold intentionally does not promise

- transparent POSIX shared memory across WAN;
- arbitrary migration of an instruction-level process at any instant;
- exactly-once external side effects without cooperation from the external system;
- efficient tensor/model parallelism across high-RTT links;
- automatic portability of vendor-specific kernels.

These exclusions are important: the abstraction should expose a useful common model without making physically impossible guarantees.
