# Protocol

Plurifold v0.2 uses a deliberately small **HTTP/JSON control plane**. The Rust request/response types in `plurifold-protocol` are authoritative for the current prototype. `proto/plurifold.proto` remains a transport-neutral design sketch for a possible future binary RPC encoding; it is not generated into the build.

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

## Object metadata and data plane

Object metadata is published to `POST /v1/objects/publish`. When an agent fetches an existing object into its local cache, it records the new physical replica through `POST /v1/objects/replica`.

Bulk bytes do **not** pass through the coordinator. In v0.2 each agent exposes:

- `POST /v1/objects` — stage bytes into the local SHA-256 CAS and create logical object metadata;
- `GET /v1/blobs/{sha256}` — serve immutable bytes directly to another agent.

The receiving agent recomputes the SHA-256 digest before using the object. The transport can therefore be replaced later by HTTP range transfer, object storage, RDMA/site-local paths, or relays without changing logical object identity.

## Topology

The prototype accepts measured links at `POST /v1/topology/link`. Links carry RTT and bandwidth. Active measurement is intentionally a later phase; scheduling never invents reachability for a missing link.

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
| `POST /v1/topology/link` | update measured link |

## Compatibility and security

Resource capabilities and task features use extensible string identifiers where premature closed enums would block new accelerator/runtime types. Wire-version negotiation and backward compatibility are not implemented yet.

The v0.2 service is **unauthenticated** and defaults to loopback. Cross-trust-domain authentication, authorization, TLS, signed artifacts, and secret locality are design requirements in `security.md`, not claims of the current implementation.
