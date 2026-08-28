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

- [x] coordinator HTTP/JSON service
- [x] agent registration, heartbeat, work polling, and completion
- [x] execution-lease renewal for long tasks
- [x] resource epoch protection against stale incarnations
- [x] content-addressed object cache
- [x] direct HTTP object transfer with digest verification
- [x] replica registration after peer fetch
- [x] replay-safe retry after worker loss
- [x] multi-process failure-injection E2E
- [ ] stable agent identity across process restarts
- [ ] native/WASI sandboxed executor
- [ ] durable coordinator state and restart recovery
- [ ] authentication/TLS and trust-domain policy enforcement
- [ ] benchmark-driven resource performance models
- [ ] real routed multi-host validation (the current development containers do not expose a mutually reachable address)

## Phase 2 — Topology and graph adaptation

- [ ] active RTT/bandwidth probing
- [ ] peer link graph maintenance and latency domains from live measurements
- [ ] automatic batching/fusion
- [ ] speculative replication
- [ ] checkpoint-aware migration
- [ ] policy simulator and trace replay

## Phase 3 — Heterogeneous accelerators

- [ ] CUDA executor adapter
- [ ] ROCm executor adapter
- [ ] Ascend/CANN executor adapter
- [ ] multi-implementation task selection
- [ ] site-local collective groups

## Phase 4 — Stateful primitives

- [ ] Actor checkpoints and rebinding
- [ ] Streams with backpressure
- [ ] explicit Collectives constrained to latency domains
- [ ] decentralized/federated coordinator research

## Phase 5 — Evaluation

- [ ] WAN emulation and real multi-site deployment
- [ ] failure injection at scale
- [ ] CPU/GPU/NPU mixtures
- [ ] non-AI workloads plus asynchronous AI training
- [ ] baselines and ablation studies
