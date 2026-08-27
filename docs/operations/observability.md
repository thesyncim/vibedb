# Observe a distributed cluster

The distributed runtime is experimental and unreleased. Its first shipped
operator counter surface is the gateway `metrics` request. Send it over the
same mutually authenticated client listener used for data requests:

```vibejson
{"op":"metrics"}
```

The authenticated principal must have `topology` capability. The request is
closed: SQL, parameters, request identities, classes, and result budgets are
rejected rather than ignored. Development plaintext may use it only on the
explicit loopback-only listener.

The response is one canonical direct-`vibejson` object:

```vibejson
{"metrics":{"route_single":0,"route_targeted":0,"route_scatter":0,"route_empty":0,"shards_fanned":0,"rows_returned":0,"bytes_returned":0,"retries":0,"scatter_all_shards":0,"scatter_unknown_route":0}}
```

All values are monotonically increasing `uint64` counters for the current
gateway process incarnation. A scrape collector should retain the process
identity externally, detect a decrease as a restart, and derive rates outside
the serving path. The encoder walks a fixed field table directly; it does not
build maps, strings, or a generic JSON tree.

## Current coverage

| Stage | Available evidence | Shipped operator surface |
| --- | --- | --- |
| Routing and fan-out | route class, shards fanned, scatter reason, returned rows/bytes, stale-route retries | `{"op":"metrics"}` |
| Proposal and apply | `multiraft.Progress` reports proposal count/bytes and applied index spans | Fixed authenticated shard-control metrics frame |
| Quorum and leadership | `raftmember.RuntimeStatus` and RF3 replica observation report leader, term, and progress | Used by authenticated replica control; no general metrics endpoint |
| Snapshot transfer | RF3 owner reports completed snapshot application; transfer service and repository expose connection, resident/in-flight byte, chunk, artifact, and disk-byte cuts | Snapshot-apply count is in the shard-control frame; transfer/repository gauges are not yet exported |
| Checkpoint and WAL | durable publications, state certificates, WAL bounds, and retention witnesses exist | No general metrics endpoint |
| Split and move | replicated operation records and controller observations expose durable phase progress | Used by controllers; no general metrics endpoint |

`internal/servicemetrics.Client` retrieves the fixed 96-byte RF3 owner snapshot
over the mutually authenticated shard-control traffic class. The peer must
have `topology` capability. Its response carries proposal commands/bytes,
applied entries, durably persisted Ready steps, finished snapshot applies,
completed `ReadIndex` outcomes, and owner-loop faults, followed by a digest.
The service has no string labels and no unbounded response.

The gateway endpoint does not yet aggregate these per-shard frames; an operator
must preserve the endpoint/node identity beside each sample. Do not infer the
remaining stage counters from request latency. In particular, a
gateway completion cannot distinguish quorum, durable apply, and response
settlement time without a shard-origin observation. The next safe extension is
a bounded gateway collector that preserves group identity and separately
exports checkpoint, WAL-retention, transfer-repository, split, and move cuts.

## Performance contract

Observability must remain downstream of correctness and off the hot path:

- mutation paths update fixed-width atomics or already-required progress
  records only;
- public encoding reads one bounded snapshot and uses direct `vibejson`;
- group, node, and operation identities remain fixed-width values rather than
  unbounded labels;
- collectors bound concurrency, response bytes, and scrape time;
- telemetry failure never authorizes routing, membership, split, move, cleanup,
  or acknowledgement.
