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

A `LogicalJobSpec` keeps the role/dependency graph explicit but allows each role to advertise multiple Task implementations. A snapshot planner can preview a concrete `CooperativeJobSpec`, but v0.7 logical execution keeps the alternatives live instead of treating that preview as the execution contract.

For each ready role it scores implementation/resource pairs with the ordinary topology-aware scheduler. Predicted predecessor outputs are represented as temporary planning objects located at the chosen predecessor resources, with size derived from the selected implementation's output-size hint. This lets a downstream choice account for the communication cost created by upstream choices instead of selecting every role independently.

The preview planner also tracks predicted resource availability so independent roles can be spread across idle resources rather than accidentally serialized on one worker. Its placement output is advisory.

For `auto-submit`, a role is normally concretized only when all of its predecessors have actually completed. At that point the runtime scores its implementation alternatives against the current schedulable resources, current topology, and the real published predecessor Object locations/sizes. A role with satisfied dependencies but no feasible implementation enters `Ready`; agent polling retries ready roles, so a later resource hot join can make progress without resubmitting the logical job. The resulting Task then follows the ordinary scheduler and execution-lease path.

### Graph runtime

The runtime tracks dependencies and may perform semantics-preserving transformations. v0.7 implements the first production transformation: look-ahead fusion of a two-role linear chain. Before a logical producer is submitted, the runtime may inspect its sole consumer, compare separate execution paths against a same-resource coarse task, and materialize one `TaskPipeline` when measured transfer cost makes the edge communication-sensitive and the fused task is not worse under the current model.

The v0.7 safety envelope is intentionally strict: both roles must expose only Pure builtin-family implementations; the producer cannot be an externally visible job output or have another consumer; the consumer must depend only on that producer; and their resource requirements must be representable by one combined requirement set. A fused task retains one normal execution lease. Its intermediate bytes are passed between pipeline stages in agent memory and are not published as an Object; only the final consumer output is published. Job status keeps both logical roles and records the fusion relationship and cost estimates.

Other graph transformations remain research work:

- task batching;
- N-stage/general-DAG producer/consumer fusion;
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

In v0.7 the link graph is maintained from active peer probes rather than requiring topology to be injected manually. Agents measure peer RTT and bounded practical HTTP throughput in the background without blocking heartbeats or work polling. Failed probes withdraw automatic links. Explicit operator links remain authoritative overrides. The same measured links now also drive the first graph-coarsening decision. This still assumes the advertised peer endpoints are directly routable; relay/NAT traversal is a separate transport problem.

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
