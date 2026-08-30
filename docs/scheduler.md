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

v0.8 applies `FusionAdvisor` across safe linear logical-role chains. The runtime first discovers the maximal graph-safe chain and uses a stage-wise dynamic program to estimate fully separate implementation/resource choices without enumerating every resource path exponentially. Each cross-resource transition uses the normal placement model plus the measured `LinkProfile`; the advisor marks communication-sensitive edges using intermediate size, RTT, and bandwidth.

The runtime then evaluates each fusible prefix of length at least two. For a prefix candidate it selects compatible stage implementations, scores the same-resource `TaskPipeline`, and—when a suffix remains—projects the suffix's best separate placements starting from the fused output location. A candidate is accepted only when this whole-chain projected cost is no worse than fully separate execution. Prefixes are ranked by modeled end-to-end savings, then avoided transfer time, then length. This allows a three-stage fusion when useful while stopping at two stages when forcing the tail onto the fused resource would cost more.

`estimated_avoided_transfer_ms` remains an estimate of communication removed by the chosen coarse region, while `estimated_vs_separate_ms` is the modeled whole-chain delta. The initial `FusionAdvisor` still uses a simple transfer-time threshold; general-DAG fusion, batching, splitting, and replication remain research work. The path is to replace or calibrate the heuristic with benchmark-driven or learned transformation costs while preserving application-visible semantics.

## Non-regression property for elastic resources

A useful design target is:

> Adding an optional remote resource should never make an already schedulable workload slower solely because the scheduler insists on using it.

This is not automatically guaranteed by all online schedulers, so it should become an explicit experimental property: new resources are opportunities, not mandatory participants.
