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

The v0.4 implementation adds a `LogicalJobSpec` one level above `CooperativeJobSpec`. A logical role keeps the same semantic dependency boundary but may offer multiple named Task implementations, for example CPU, CUDA, or CANN variants. The planner evaluates the currently schedulable resources and chooses one implementation per role using the same placement cost model used by ordinary Tasks. For downstream roles it also predicts predecessor output locations and uses each implementation's `output_bytes` hint to estimate intermediate transfer cost.

The resulting `CooperativePlan` contains the compiled `CooperativeJobSpec`, selected implementation names, predicted resources, cost breakdowns, and an estimated makespan. Predicted resources explain the snapshot plan; they are not hard bindings. When the compiled job executes, each role still goes through normal lease-time scheduling so current membership and data locality remain authoritative.

Role decomposition itself is still explicit. Plurifold does not infer arbitrary program boundaries from source code or split one instruction-level computation across unrelated devices. Domain libraries or future graph compilers can generate `LogicalJobSpec` while preserving the same execution protocol.

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
