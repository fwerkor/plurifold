# Roadmap

## Phase 0 — Executable design scaffold

- [x] Task/Object/Resource/Topology types
- [x] lease-oriented membership model
- [x] topology-aware placement cost model
- [x] in-memory elastic Fabric prototype
- [x] graph-fusion heuristic
- [x] protocol sketch
- [x] CI and unit tests

## Phase 1 — Real multi-node task execution

- coordinator RPC service;
- agent heartbeat and work leases;
- local native/WASI executor;
- content-addressed object cache;
- direct HTTP object transfer;
- retry/deduplication protocol;
- benchmark-driven resource performance models.

## Phase 2 — Topology and graph adaptation

- active RTT/bandwidth probing;
- peer link graph and latency domains;
- automatic batching/fusion;
- speculative replication;
- checkpoint-aware migration;
- policy simulator and trace replay.

## Phase 3 — Heterogeneous accelerators

- CUDA executor adapter;
- ROCm executor adapter;
- Ascend/CANN executor adapter;
- multi-implementation task selection;
- site-local collective groups.

## Phase 4 — Stateful primitives

- Actor checkpoints and rebinding;
- Streams with backpressure;
- explicit Collectives constrained to latency domains;
- decentralized/federated coordinator research.

## Phase 5 — Evaluation

- WAN emulation and real multi-site deployment;
- failure injection;
- CPU/GPU/NPU mixtures;
- non-AI workloads plus asynchronous AI training;
- baselines and ablation studies.
