# Related systems and the intended gap

Mosaic is deliberately assembled around a gap between several mature lines of work rather than claiming that its individual primitives are new.

## Legion

Legion is a data-centric parallel programming system for distributed heterogeneous architectures. Its separation of application correctness from mapping/placement is directly aligned with Mosaic's location-transparent programming goal.

Mosaic's research emphasis differs in treating **weak WAN links, open-ended membership, and computation-granularity adaptation** as first-class conditions rather than assuming a relatively stable HPC execution environment.

Reference: https://legion.stanford.edu/

## StarPU

StarPU provides task scheduling, dependency tracking, performance models, and data movement across heterogeneous CPUs/accelerators, including cluster communication. Its codelet/performance-model approach is a strong precedent for selecting among implementations on different devices.

Mosaic extends the problem outward: resources may live in separate latency domains and administrative sites, so the runtime must be willing to avoid a resource entirely, coarsen a graph before crossing a WAN, or treat a remote node as opportunistic rather than as a mandatory cluster member.

Reference: https://starpu.gitlabpages.inria.fr/

## Ray

Ray's Task/Actor/Object primitives and lineage/fault-tolerance model are close to the programming ergonomics Mosaic wants. Mosaic differs primarily in execution assumptions: topology and bulk-data transfer are part of the scheduling model, and membership is intended to span weakly connected, highly heterogeneous resources rather than form one conventional cluster abstraction.

Reference: https://docs.ray.io/

## Charm++

Charm++ demonstrates the value of migratable logical objects, overdecomposition, dynamic load balancing, and checkpoint/restart. Mosaic borrows the idea that logical placement should not be permanently tied to one processor, while moving migration boundaries upward to explicit state/object checkpoints that remain practical across different architectures.

Reference: https://charmplusplus.org/

## Globus Compute

Globus Compute demonstrates a practical federated execution model spanning laptops, clusters, clouds, and supercomputers. It is a strong reference for crossing site and administrative boundaries.

Mosaic aims below the function-delivery layer: it wants a runtime-owned object space, topology-aware placement model, and graph transformations that jointly optimize computation and data movement.

Reference: https://www.globus.org/compute

## WASI / WebAssembly Component Model

WASI is not a scheduler, but it is a promising portable execution ABI. WASI 0.3 added native asynchronous functions, futures, and streams to the Component Model in 2026, which makes it particularly interesting for portable CPU-side Mosaic tasks.

Mosaic should not force accelerators through WASI where vendor-native execution is better. The design therefore allows multiple artifacts/implementations behind one logical task contract.

References:

- https://wasi.dev/
- https://wasi.dev/releases/wasi-p3

## Research gap

The intended contribution is the composition:

`heterogeneous hardware + WAN topology + dynamic membership + versioned data + adaptive graph granularity`

A convincing Mosaic result must show that this composition produces properties that placement-only or cluster-oriented systems do not naturally provide: disruption-free hot membership changes, useful exploitation of stranded remote compute, and lower WAN cost through semantics-preserving graph adaptation.
