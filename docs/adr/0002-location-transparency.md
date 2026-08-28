# ADR 0002: Location-transparent programming, topology-aware execution

Status: Accepted

Application correctness must not depend on fixed machine identities unless an explicit placement constraint says so. Execution performance, however, must account for physical topology.

Plurifold therefore does not expose WAN links as fake shared memory and does not silently construct global collectives. RTT, bandwidth, data locality, startup cost, and resource reliability are scheduler inputs.
