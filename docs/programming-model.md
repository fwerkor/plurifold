# Programming model

## Task

A Task is one schedulable execution unit. It declares:

- artifact/entrypoint identity;
- input Object IDs;
- resource requirements;
- cost hints;
- effect/retry semantics;
- optional affinity/anti-affinity hints;
- optional checkpoint capability.

Tasks remain one-shot work. Dynamic graph construction is allowed: a completed task may make additional tasks runnable.

## Cooperative Job

A Cooperative Job is one logical objective implemented by multiple named roles. Each role contains a Task template and a dependency list. Independent roles become schedulable together and can be placed on different resources according to their individual capability requirements. A dependent role is materialized only after every predecessor completes; predecessor output Object IDs are appended to the dependent Task's inputs in dependency order.

For example:

```text
preprocess (CPU) ─┐
                  ├─ aggregate (large-memory CPU) -> result
simulate (GPU) ───┘
```

The job is the logical unit seen by the application, while each Task retains its own execution lease, retry semantics, placement decision, and output objects. If an `Exclusive` role loses an ambiguous execution attempt, the containing job becomes `Uncertain`. Pure or idempotent role tasks keep the existing replay behavior.

The v0.9 implementation has a `LogicalJobSpec` one level above `CooperativeJobSpec`. A logical role keeps the same dependency boundary, may offer multiple named Task implementations, and may request `shards: N` independent contributions (default `1`). `job plan` predicts one implementation/resource choice per shard using the same placement cost model as ordinary Tasks. It tracks predicted resource availability so shards can run concurrently when capacity exists or reuse Resources serially when N exceeds currently useful parallelism. Downstream preview uses one predicted Object per predecessor shard and each implementation's `output_bytes` hint to estimate fan-in transfer cost.

`job auto-submit` does not execute that preview as a frozen compilation. The runtime retains the original implementation alternatives. When a logical role's dependencies actually complete, an unsharded role materializes one concrete Task; a sharded role materializes N ordinary child Tasks carrying `TaskShard { index, count }`. Each child receives the same logical Object inputs, so the application/executor—not Plurifold—defines how the shard context maps to real data/work. A sharded role completes only after all child Tasks complete, and its downstream Object list is ordered by shard index regardless of completion order. If no implementation is feasible, the role remains `Ready`. The selected resources remain advisory; every child goes through normal lease-time scheduling.

This creates two separate decisions: **implementation selection at role/shard readiness** and **resource placement at execution lease time**. The former may change from the earlier preview after hot joins or topology/locality changes; the latter may still move a child Task to another compatible Resource. If a replay-safe shard loses its lease it returns to `Pending` without invalidating completed sibling shards. If an unreplayable shard becomes `Uncertain`, the whole logical role and job become `Uncertain`.

`auto-submit` may make a third decision before role materialization: **graph coarsening**. v0.9 follows a maximal safe linear chain beginning at the ready role, stopping at fan-out, fan-in, a declared intermediate job output, or another graph-shape boundary. It evaluates every consecutive Pure builtin-family prefix of length at least two whose selected requirements can coexist on one Resource. The chosen prefix is not simply the longest one: the runtime compares the projected end-to-end cost of the whole chain, including the best separately placed suffix, against fully separate execution.

A selected prefix becomes one multi-stage `TaskPipeline`. All fused logical roles refer to the same Task ID while it runs. Intermediate fused stages complete with no published Object; the last fused stage publishes the pipeline output. If later roles remain outside the prefix, that output is the normal Object input for the suffix. Thus a three-role chain may become one three-stage task, or a two-stage task followed by one ordinary Task, depending on topology and compute costs.

Job status exposes the coarsening decision rather than hiding it. Each fused role reports the same ordered `chain_roles`, its own `stage_index`, the estimated avoided cross-resource transfer time, and the modeled end-to-end delta versus fully separate execution. A zero modeled delta means the runtime chose the coarser task on a cost tie; it is not reported as measured wall-clock speedup.

Role decomposition and shard semantics are still explicit. Plurifold does not infer arbitrary program boundaries, automatically partition input bytes/tensors, choose a shard count, or split one instruction-level computation across unrelated devices. v0.9 supports an explicit fan-out/fan-in pattern through sharded roles, but still does not fuse arbitrary fan-out/fan-in subgraphs or rewrite a general DAG. Domain libraries or future graph compilers can generate `LogicalJobSpec` while preserving the same execution protocol.

## Object

An Object has a stable logical ID and metadata independent of placement. Physical locations are replicas, not identities.

Object properties include:

- byte size;
- optional content digest;
- encoding/media type;
- producing task/attempt lineage;
- replica locations.

Objects are immutable after publication. Mutable application state is represented as a sequence of versions.

## Resource

A Resource is a leased capability advertisement, not a permanent machine record. It describes what can be scheduled there now:

- architecture and OS;
- CPU cores and memory;
- accelerators and device memory;
- executor/runtime capabilities;
- normalized performance hint;
- reliability/failure estimate;
- queue pressure;
- topology links.

## Effects and retries

The task effect model is intentionally small:

- `Pure`: no externally visible side effects; freely replayable.
- `Idempotent`: effects are safe to repeat using an application-provided idempotency key or equivalent protocol.
- `Exclusive`: effects are not replay-safe; the runtime must not automatically create a second execution after an uncertain failure.

This avoids claiming exactly-once execution in a distributed system when only at-least-once task dispatch is actually available.

## Actor (planned)

An Actor will be a stable logical identity with state represented by versioned objects and a serialized message order. Migration occurs at explicit state checkpoints. The design goal is logical mobility rather than arbitrary process-memory migration.

## Stream (planned)

Streams will add flow control, bounded buffering, and placement pressure. A stream edge is a stronger locality signal than an immutable object edge because repeated transfer cost can dominate.

## Collective (planned)

Collectives are explicit, topology-constrained operations. They are never automatically expanded from a low-latency island to a WAN-wide group merely because more resources become available.

## Portable execution ABI

Plurifold separates the **control/data model** from the **artifact execution ABI**. WASI 0.3 / the WebAssembly Component Model is a strong candidate for portable CPU-side tasks because it offers typed cross-language composition and native async primitives. Accelerator-heavy tasks will often remain backend-specific; the scheduler can select among multiple implementations of one logical task.
