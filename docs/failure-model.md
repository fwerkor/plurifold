# Failure and elasticity model

## Resource lifecycle

A Resource cycles through:

`discovered -> active -> draining/expired -> absent`

Membership is controlled by leases. No global barrier or rank rebuild is needed when a lease expires.

## Task execution lifecycle

A logical Task may have multiple Execution Attempts over time. Each attempt has a unique execution ID and lease deadline.

- If an attempt completes before expiry, its outputs can be committed.
- A late duplicate completion is deduplicated against the logical task state.
- An expired `Pure` or `Idempotent` attempt may be retried elsewhere.
- An `Exclusive` task enters an uncertain state after loss; operator/application reconciliation is required before replay.

## Object loss

Object replicas can disappear with resources. Recovery choices are, in order:

1. use another verified replica;
2. fetch from durable backing storage;
3. reconstruct through producer lineage if replay is safe;
4. declare the object unavailable.

The object identity does not change merely because a replica moves.

## Hot join

A joining resource is capability-probed and benchmarked, then becomes eligible for new work. Running tasks are not automatically reshuffled merely to use it; migration must have a predicted benefit greater than checkpoint/transfer cost.

## Hot leave

A graceful leave marks the resource draining and stops new leases. An abrupt leave is detected by membership lease expiry. Other resources continue without reconfiguration.

## Partitions

A network partition is treated as temporary loss of lease authority. The control plane prefers safety over accepting ambiguous duplicate commits. Future decentralized coordinator designs may use scoped authorities, but v0 assumes one logical coordinator authority.
