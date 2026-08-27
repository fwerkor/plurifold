# Topology-aware scheduler

## Objective

For task `t` on resource `r`, Mosaic estimates:

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

Placement alone is insufficient on WANs. Mosaic therefore treats graph rewriting as a first-class future mechanism.

For producer `A` and consumer `B`, if their intermediate object is large relative to the compute between WAN crossings, it may be better to fuse them into one placement unit. Conversely, compute-heavy independent work may be split and replicated.

The initial `FusionAdvisor` uses a simple threshold based on intermediate bytes, RTT, and bandwidth. The research path is to replace this with measured/learned cost models while preserving deterministic semantics.

## Non-regression property for elastic resources

A useful design target is:

> Adding an optional remote resource should never make an already schedulable workload slower solely because the scheduler insists on using it.

This is not automatically guaranteed by all online schedulers, so it should become an explicit experimental property: new resources are opportunities, not mandatory participants.
