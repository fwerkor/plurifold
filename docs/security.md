# Security and trust model

Plurifold's target environment can cross administrative and network boundaries, so security cannot be treated as an RPC implementation detail. The v0 code does not yet enforce these mechanisms, but the architecture reserves the following boundaries.

## Identities and authenticated membership

A production coordinator must authenticate agents before issuing membership leases. Resource IDs are logical names; cryptographic identity should be bound to a certificate/key or external workload identity. Re-registration creates a new epoch so a stale process cannot continue acting as the current incarnation.

## Artifact integrity

Task artifacts and immutable objects should be content-addressed or carry cryptographic digests. An executor verifies artifacts before launch; consumers verify object replicas after transport. Transport location is never proof of identity.

## Capability-scoped execution

Portable tasks should receive explicit capabilities rather than ambient host authority. WASI's capability-oriented model is attractive for sandboxable CPU tasks. Native/OCI and accelerator executors require equivalent policy around filesystem, network, devices, and secrets.

## Secrets

Secrets must not be ordinary Objects that can be replicated according to performance policy. They require separate scoped handles with resource/trust constraints and short lifetimes. A task placement that cannot satisfy the secret's trust policy is incompatible, regardless of performance.

## Trust-aware scheduling

Resource requirements should eventually include a trust domain/attestation policy. Data classifications may similarly restrict allowed locations. This belongs in the hard-constraint phase of scheduling, before cost scoring.

## Malicious resources

Lease and digest checks protect against stale or corrupted results but do not prove that a malicious worker performed a computation correctly. Workloads requiring Byzantine resistance need domain-specific verification, replication/voting, trusted execution, or proofs. Plurifold does not claim generic Byzantine-safe computation.

## Coordinator authority

The first implementation assumes one logical coordinator authority. High availability can replicate that authority later, but split-brain task commit must be prevented. Execution IDs, epochs, and conditional object publication are designed so they can eventually be backed by a consensus/transactional metadata store.
