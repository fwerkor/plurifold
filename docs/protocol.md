# Protocol sketch

The wire protocol is intentionally small and lease-oriented. `proto/mosaic.proto` records the initial RPC shape but is not yet compiled into the Rust build.

## Membership

1. Agent sends `RegisterResource(ResourceDescriptor)`.
2. Coordinator returns `membership_id`, `epoch`, and lease deadline.
3. Agent renews with heartbeats carrying current queue/load observations.
4. Expiry removes the Resource from placement candidates.

The epoch prevents an old incarnation of the same resource identity from publishing results after a restart.

## Work leasing

1. Agent requests or receives a Task lease.
2. Lease includes logical task ID, unique execution ID, resource epoch, and deadline.
3. Agent resolves input objects through object metadata/data-plane URLs.
4. Agent executes using a selected local executor.
5. Agent stages outputs and commits metadata conditionally on the execution lease.

## Data plane

Bulk object bytes should not be forced through the coordinator. The metadata protocol can return one or more transfer endpoints, allowing:

- peer-to-peer transfer;
- object storage;
- HTTP/range transport;
- site-local cache;
- domain-specific high-speed paths.

Content digests verify replicas regardless of transport.

## Compatibility

Protocol messages are versioned. Resource capabilities and task features use extensible string identifiers where premature closed enums would block new accelerator/runtime types.
