# Topology-aware scheduler

## Objective

For task `t` on resource `r`, Plurifold estimates:

`C(t,r) = Ccompute + Cinput + Cqueue + Cstartup + Crisk + Cpolicy`

The first prototype implements compute, input transfer, startup/queue hints, and a simple risk penalty. Later work should learn these terms online.

## Compute estimate

A Task carries a normalized compute estimate. A Resource advertises a normalized performance score. The initial estimator uses:

`compute_ms = task.compute_ms / resource.performance_score`

This is intentionally a placeholder for per-task/per-backend performance models.

## Input transfer estimate

For every input object not already present on the destination, the scheduler chooses the cheapest known source replica and estimates:

`transfer = RTT + bytes / bandwidth`

This immediately gives the desired behavior for weak links: compute-heavy/small-input work may move; data-heavy/light-compute work stays near data.

## Reliability

A resource advertises an estimated failure probability over the task horizon. The prototype turns this into a time penalty. A production scheduler should model expected wasted work, checkpoint interval, task replay safety, and resource price separately.

## Constraints before scoring

A resource is excluded if it cannot satisfy hard requirements such as:

- minimum host memory;
- architecture;
- accelerator kind/count/memory;
- required executor feature;
- explicit affinity/domain constraint.

Hard compatibility should never be represented only as a large score.

## Adaptive graph granularity

Placement alone is insufficient on WANs. Plurifold therefore treats graph granularity as a scheduler/runtime concern rather than a fixed application constant.

v0.7 wires the existing `FusionAdvisor` into logical-job materialization for one conservative case: a Pure producer with a single Pure consumer. The runtime looks ahead before submitting the producer, estimates separate producer/consumer paths using the normal placement model, identifies the best cross-resource path, and evaluates the intermediate's transfer time from the measured `LinkProfile`.

If the cross-resource edge exceeds the advisor threshold and a same-resource `TaskPipeline` is no worse than the best separate modeled path, the runtime may choose the pipeline. Exact cost ties prefer the coarser execution because it removes an intermediate Task/Object boundary; Plurifold does not invent an unmeasured fixed CAS/control-plane savings constant. Status therefore distinguishes `estimated_avoided_transfer_ms` from `estimated_vs_separate_ms`.

The initial `FusionAdvisor` still uses a simple threshold based on intermediate bytes, RTT, and bandwidth. General N-stage fusion, batching, splitting, and replication remain research work; the path is to replace the heuristic with benchmark-driven or learned transformation costs while preserving application-visible semantics.

## Non-regression property for elastic resources

A useful design target is:

> Adding an optional remote resource should never make an already schedulable workload slower solely because the scheduler insists on using it.

This is not automatically guaranteed by all online schedulers, so it should become an explicit experimental property: new resources are opportunities, not mandatory participants.
