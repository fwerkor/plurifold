# Protocol

Plurifold v0.8 uses a deliberately small **HTTP/JSON control plane** (protocol constant `API_VERSION = 4`). The Rust request/response types in `plurifold-protocol` are authoritative for the current prototype. `proto/plurifold.proto` remains a transport-neutral design sketch for a possible future binary RPC encoding; it is not generated into the build.

## Membership

1. An agent advertises `ResourceDescriptor + data_endpoint` to `POST /v1/resources/register`.
2. The coordinator returns a `MembershipLease` containing the logical resource ID, incarnation epoch, and lease deadline.
3. The agent renews membership through `POST /v1/resources/heartbeat`.
4. Expiry removes the Resource from future placement without rebuilding a global process group.
5. Re-registering the same logical Resource increments its epoch; old execution attempts from previous incarnations cannot commit.

The current prototype generates a fresh Resource ID when an agent process starts. Stable resource identity across process restarts can be layered on later without changing the epoch rule.

## Work leasing

Agents pull work through `POST /v1/work/poll`. An assignment contains:

- the complete `TaskSpec`;
- a unique `ExecutionId`;
- the Resource ID and Resource epoch that own the attempt;
- an execution deadline;
- resolved input object metadata and direct replica URLs.

Long work renews its lease with `POST /v1/work/renew`. The coordinator's current `TaskStatus::Running` lease is authoritative; a client cannot extend validity by inventing a later timestamp.

Completion uses `POST /v1/work/complete`. The coordinator accepts the outputs only when the execution attempt is still current, the resource incarnation is active, and the execution has not expired.

If a lease expires, replay-safe work returns to `Pending`. An `Exclusive` task instead becomes `Uncertain` and requires application/operator reconciliation.

`TaskSpec` now has an optional runtime-generated `pipeline`. A `TaskPipeline` is an ordered list of stages. Stage inputs explicitly bind either to an external Task input index or to the immediately previous stage output. Ordinary submitted Tasks leave this field absent. The v0.8 agent uses it for fused builtin-family work: all external Objects are materialized once, stage intermediates remain in memory, and only the final bytes are committed as the Task output.

## Cooperative jobs

`POST /v1/jobs` submits a `CooperativeJobSpec`. Root roles are immediately materialized as ordinary Tasks. When a role completes, the coordinator records its output Object IDs and materializes any roles whose dependencies are now satisfied. Those downstream Tasks use predecessor outputs as normal object inputs, so cross-resource edges reuse the same replica discovery and direct peer-transfer path as standalone Tasks.

`GET /v1/jobs/{id}` returns either the fixed `CooperativeJobSpec` or live `LogicalJobSpec`, plus each role's state. Dynamically selected roles also report the chosen implementation, advisory resource, ready-time estimated cost, and optional fusion metadata. In API 4, fusion metadata contains the ordered `chain_roles`, the role's `stage_index`, `estimated_avoided_transfer_ms`, and `estimated_vs_separate_ms`. Every role in a fused prefix references the same Task ID. A job completes after all roles complete; its result is the concatenated output Object IDs of the roles named in the job definition. An unreplayable role entering `Uncertain` propagates that state to the whole job.

## Logical planning

`POST /v1/jobs/plan` accepts a `LogicalJobSpec` whose roles contain alternative Task implementations. It returns a `CooperativePlan` with one selected implementation per role, predicted resource placements, placement-cost breakdowns, an estimated makespan, and the compiled `CooperativeJobSpec`.

`POST /v1/jobs/auto` accepts the same `LogicalJobSpec` but stores the logical definition rather than freezing the preview. Its response includes the Job ID and an optional `initial_plan`; the preview is absent when the current snapshot has no complete feasible plan, but the logical job can still be accepted. Each role is implementation-selected when its real dependencies complete. If no implementation is feasible then, the role remains `Ready`; later agent polling retries it against current membership, topology, and Object replicas. This is what allows a hot-joined resource to change a downstream implementation choice after submission.

Before a currently ready logical role is materialized, v0.8 may discover a maximal safe linear chain and replace a cost-selected prefix with one multi-stage `TaskPipeline`. This is an execution-time transformation, not part of the `job plan` snapshot contract. Chain growth stops at fan-out, fan-in, a declared interior job output, or another non-linear boundary. Only a consecutive Pure builtin-family prefix is eligible for fusion; a non-fusible tail can remain separate. The runtime compares each candidate prefix using projected whole-chain cost, so it can select two stages, three stages, or more rather than always taking the maximum. Internal fused stages do not publish Objects. The last fused stage does publish its output, which is the job result when the prefix reaches an output role or the normal predecessor Object when a suffix remains.

## Object metadata and data plane

Object metadata is published to `POST /v1/objects/publish`. When an agent fetches an existing object into its local cache, it records the new physical replica through `POST /v1/objects/replica`.

Bulk object bytes do **not** pass through the coordinator. In v0.8 each agent exposes:

- `POST /v1/objects` — stage bytes into the local SHA-256 CAS and create logical object metadata;
- `GET /v1/blobs/{sha256}` — serve immutable bytes directly to another agent.
- `GET /v1/probe/{bytes}` — serve a bounded synthetic payload for peer RTT/throughput measurement (maximum 1 MiB).

The receiving agent recomputes the SHA-256 digest before using the object. The transport can therefore be replaced later by HTTP range transfer, object storage, RDMA/site-local paths, or relays without changing logical object identity.

## Topology

Agents periodically reuse `GET /v1/resources` to discover peer data endpoints. One agent per unordered peer pair performs three empty-response RTT probes and a 256 KiB payload probe, then reports the observed reachability through `POST /v1/topology/measurement`. Reports carry the reporter Resource ID and epoch; stale or inactive reporters are rejected. Successful reports install/update the measured `LinkProfile`; an `Unreachable` report withdraws the automatic link so scheduling does not keep using stale reachability.

`POST /v1/topology/link` remains an explicit operator override. A manual link is authoritative for that Resource pair until one endpoint expires; automatic measurements neither overwrite nor withdraw it.

## Current endpoint summary

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | liveness |
| `POST /v1/resources/register` | join/rejoin fabric |
| `POST /v1/resources/heartbeat` | renew membership |
| `GET /v1/resources` | inspect active resources |
| `POST /v1/work/poll` | request work |
| `POST /v1/work/renew` | renew execution lease |
| `POST /v1/work/complete` | conditionally commit outputs |
| `POST /v1/objects/publish` | publish object metadata |
| `POST /v1/objects/replica` | add cached replica location |
| `POST /v1/tasks` | submit task |
| `GET /v1/tasks/{id}` | inspect task state |
| `POST /v1/jobs` | submit cooperative role graph |
| `POST /v1/jobs/plan` | preview implementation choices and predicted placements for a logical job |
| `POST /v1/jobs/auto` | submit a logical job for ready-time implementation replanning |
| `GET /v1/jobs/{id}` | inspect cooperative job and role states |
| `POST /v1/topology/link` | set a manual link override |
| `POST /v1/topology/measurement` | report automatic peer reachability/RTT/bandwidth |

## Compatibility and security

Resource capabilities and task features use extensible string identifiers where premature closed enums would block new accelerator/runtime types. Wire-version negotiation and backward compatibility are not implemented yet.

The v0.8 service is **unauthenticated** and defaults to loopback. Cross-trust-domain authentication, authorization, TLS, signed artifacts, and secret locality are design requirements in `security.md`, not claims of the current implementation.
