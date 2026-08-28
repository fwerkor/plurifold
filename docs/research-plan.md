# Research plan

## Thesis

A useful runtime for weakly connected heterogeneous compute must jointly reason about **computation, data, topology, elasticity, and graph granularity**. Treating all resources as members of one synchronous cluster is the wrong abstraction.

## Research questions

### RQ1 — Can topology-aware placement turn stranded heterogeneous resources into useful throughput?

Compare Plurifold against locality-oblivious and compute-only schedulers over controlled CPU/GPU/NPU mixtures and WAN emulation. Measure makespan, useful compute utilization, transferred bytes, idle time, and scheduler overhead.

### RQ2 — Does adaptive graph granularity outperform placement-only scheduling under high RTT?

Evaluate batching/fusion/splitting as RTT, bandwidth, object size, and task compute intensity vary. The important boundary is when remote execution becomes profitable.

### RQ3 — Can lease-based elasticity make membership changes non-disruptive?

Inject node joins, graceful leaves, crashes, and partitions while workflows run. Measure unaffected-task slowdown, recovery time, recomputation, and lost work.

### RQ4 — How portable can one task/state ABI be across architectures?

Use portable WASI components where practical and backend-specific accelerator implementations where necessary. Evaluate the amount of application code that remains unchanged across x86-64, ARM64, CUDA, and NPU backends.

### RQ5 — Can resource addition be monotonic in performance?

Test the policy property that adding an optional weak/slow resource does not degrade baseline makespan. Identify workload classes where exploration or migration violates this and design safeguards.

## Experimental workloads

Use workloads with different communication/computation ratios instead of only AI:

- embarrassingly parallel Monte Carlo;
- video/render tiles;
- distributed compilation;
- scientific parameter sweeps;
- ETL/object transforms;
- graph pipelines with large intermediates;
- batch inference;
- asynchronous/local-update AI training as a stress case.

## Baselines / intellectual neighbors

The design should be compared conceptually and experimentally with systems representing different slices of the problem: Legion, StarPU, Ray, Charm++, Parsl/Dask-style workflow runtimes, Globus Compute, and appropriate cluster schedulers. WASI Component Model should be evaluated as an execution ABI rather than treated as a scheduling baseline.

## Expected contribution boundary

The paper-worthy contribution should not be "yet another task scheduler." The stronger claim is a runtime architecture where **latency-domain-aware graph transformation and elastic resource leases** make heterogeneous WAN resources composable without pretending they are a low-latency cluster.
