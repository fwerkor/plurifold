# Architecture

## 1. Problem statement

Plurifold targets a compute pool in which resources may have different ISAs, accelerator vendors, memory capacities, software stacks, performance, trust/reliability characteristics, and network distance. The pool is elastic: a machine can appear or disappear without forming or rebuilding a global process group.

The system is not a distributed shared-memory machine and not a WAN collective library. It is a **logical compute fabric** that maps coarse computations and data to physical resources while preserving topology as an execution concern.

## 2. Layering

### Application layer

Applications express computations through stable logical handles. Domain libraries may build richer models on top: scientific workflows, rendering, compilation, search, simulation, batch inference, and eventually asynchronous AI training.

### Programming model

The base execution model is a dynamic dataflow graph:

`Task(inputs: Object...) -> Object...`

Task and Object are the minimal portable execution units. A Cooperative Job adds a logical objective above them: it names multiple role Tasks, allows independent roles to run concurrently on complementary resources, and releases dependent roles when predecessor Objects become available. Actor, Stream, and Collective remain explicit extensions because they have different failure and placement semantics.

```text
Cooperative Job
   ├─ role A -> Task -> Object(s)
   ├─ role B -> Task -> Object(s)
   └─ role C depends on A,B -> Task -> result Object(s)
```

### Logical planner

A `LogicalJobSpec` keeps the role/dependency graph explicit, allows each role to advertise multiple Task implementations, and may request multiple independent shard contributions. A v0.10 snapshot `CooperativePlan` reports predicted placements per shard rather than compiling one fixed `CooperativeJobSpec`: different shards may choose different implementations/resources, and live execution keeps those alternatives dynamic.

For each ready role it scores implementation/resource pairs with the ordinary topology-aware scheduler. Numeric `shards: N` keeps the fixed child count. An auto role declares one explicit input as byte-range partitionable and a maximum count. The planner evaluates candidate counts, builds concrete ranges, scales total compute/output hints by range fraction plus declared child overhead, and tracks predicted Resource availability while selecting heterogeneous implementations and placements. A larger count is adopted only when the modeled incremental role makespan improves by the configured threshold. Predicted predecessor outputs remain temporary planning Objects at their selected Resources, and all preview placements are advisory.

For `auto-submit`, a role is concretized only when its predecessors have completed. Fixed multi-shard roles materialize the declared children. Auto roles recompute the best count from current Resources, topology, Object size/locations, and implementation alternatives, then materialize ordinary child Tasks carrying concrete byte ranges. Scheduler transfer estimates use the range length. Remote Agents request only their ranges and verify range digests; a local full replica is sliced locally. Dependent roles wait for every child and consume outputs in deterministic shard-index order. Lease, retry, CAS publication, and uncertainty semantics remain ordinary Task semantics.

### Graph runtime

The runtime tracks dependencies and may perform semantics-preserving transformations. v0.10 retains look-ahead coarsening from one producer/consumer pair to a variable-length safe linear chain. Before a ready logical role is submitted, the runtime follows sole-consumer edges while every next role depends only on its predecessor and no interior role is a declared job output. It then evaluates each fusible prefix rather than forcing the maximal chain into one task.

The placement model is used end to end. A stage-wise dynamic program estimates the best fully separate execution of the maximal chain. For each candidate fused prefix, Plurifold scores the same-resource `TaskPipeline` and then predicts the best separate placement of any remaining suffix. The selected prefix maximizes modeled whole-chain savings, with avoided transfer and longer chains used only as tie-breakers. A locally attractive fusion is therefore rejected when it would make the later suffix more expensive overall.

Sharded roles form explicit coarsening boundaries: v0.10 does not fuse a `shards > 1` role into a linear `TaskPipeline`. The fusion safety envelope otherwise remains strict: every fused role must expose only Pure builtin-family implementations, interior fused roles cannot be declared job outputs or fan out, each next fused role must depend only on its predecessor, and the selected implementations' resource requirements must be representable by one combined requirement set. A non-fusible tail may remain outside an otherwise valid fused prefix. One fused chain keeps one normal execution lease; internal stage bytes stay in agent memory, and only the last fused stage publishes an Object. Job status preserves every logical role and records the ordered fusion group and stage index.

Other graph transformations remain research work:

- task batching;
- arbitrary general-DAG fusion and batching;
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

Cooperative roles do not share one giant execution lease. Each materialized role remains an ordinary Task with an independent lease and placement decision. This lets one logical job survive partial worker loss without imposing a global barrier on unrelated roles.

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

In v0.10 the link graph is maintained from active peer probes rather than requiring topology to be injected manually. Agents measure peer RTT and bounded practical HTTP throughput in the background without blocking heartbeats or work polling. Failed probes withdraw automatic links. Explicit operator links remain authoritative overrides. The same measured links now also drive the first graph-coarsening decision. This still assumes the advertised peer endpoints are directly routable; relay/NAT traversal is a separate transport problem.

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
