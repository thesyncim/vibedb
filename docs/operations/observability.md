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

The response is one canonical direct-`vibejson` object. A replicated gateway
adds `distributed_metrics` beside its process-local routing counters:

```vibejson
{"metrics":{"route_single":0,"route_targeted":0,"route_scatter":0,"route_empty":0,"shards_fanned":0,"rows_returned":0,"bytes_returned":0,"retries":0,"scatter_all_shards":0,"scatter_unknown_route":0}}
```

`distributed_metrics` contains a saturating aggregate, an `overflow` flag, and
one bounded `members` array. A member entry binds the complete RF3 group key,
member ID, node ID, collection read/fault counts, and the exact counter cut.
Node-aggregate entries are marked with `node_aggregate:true` and carry service
stages that are physical-process rather than per-group values. The fixed
catalog route directory bounds the array. Clients must not merge entries by a
string label or infer a missing group.

When replica-move or split controllers are configured, `controller_metrics`
reports pass, discovered, advanced or triggered, completed, fault, cumulative
duration, and maximum-duration counters for those gateway loops. These are
process-incarnation counters. Replicated operation records remain the recovery
and topology authority.

Values are fixed-width `uint64` counters and gauges. Event counters increase
within the source process incarnation, while live WAL bytes, resident bytes,
and in-flight work can decrease normally. A scrape collector should retain
source identity externally and distinguish a counter reset from a gauge
change. Derive rates outside the serving path. The encoder walks a fixed field
table directly. It does not build maps, strings, or a generic JSON tree.

## Current coverage

| Stage | Available evidence | Shipped operator surface |
| --- | --- | --- |
| Routing and fan-out | route class, shards fanned, scatter reason, returned rows/bytes, stale-route retries | `{"op":"metrics"}` |
| Proposal, quorum, and apply | proposal commands/bytes, authoritative commit-index advancements and committed entries, applied entries, persisted Ready steps, completed ReadIndex operations, and Raft faults | Per-group member entries and saturating aggregate |
| Snapshot apply and transfer | finished snapshot applies, transfer chunks/bytes, and current resident transfer bytes | Per-group apply counter plus node-aggregate transfer counters |
| Checkpoint and WAL | checkpoint applied index, logical/physical checkpoint counts, barrier syncs, WAL live bytes, entries, and syncs | Node-aggregate entries and saturating aggregate |
| Replica action | authenticated requests, completions, and faults | Node-aggregate entries and saturating aggregate |
| Split control | authenticated requests, completions, and faults | Node-aggregate entries and saturating aggregate |
| Target bootstrap | requests, chunks/bytes, completions, faults, and current resident bytes/in-flight work | Node-aggregate entries and saturating aggregate |
| Split and move controller | passes, discovered work, advanced/triggered work, completions, faults, cumulative duration, and maximum duration | Gateway `controller_metrics` object |
| Live backup | requests, faults, logical artifact bytes, and snapshot scan bytes | Node-aggregate entries and saturating aggregate |
| Leadership and exact catalog operation phase | Runtime and replicated operation observations exist | Not exported by this counter surface |

`internal/servicemetrics.Client` sends a fixed 80-byte request and retrieves a
fixed 408-byte RF3 snapshot
over the mutually authenticated shard-control traffic class. The peer must
have `topology` capability. A group request returns the exact group/member cut.
A zero group request returns the node-aggregate service stages. The response
has no string labels and no unbounded field.

The gateway fixes the complete group/member and unique-node directory at
startup. A bounded worker set refreshes it over authenticated shard control.
Slow nodes consume only their worker and configured deadline. Readers use a
lock-free seqlock snapshot, saturating arithmetic, and direct `vibejson`
encoding. Collection failure increments an explicit fault counter and never
becomes topology authority.

Refreshes do not create a simultaneous cluster-wide observation cut. Aggregate
indexes and counts are telemetry sums, not a quorum certificate or a common
applied position. Use exact per-group observations for operational diagnosis.

Do not infer absent stage timing from request latency. A completion counter does
not expose quorum duration, apply duration, or response-settlement duration.
`ready_persisted` counts durable Ready handling. It is not a quorum counter.
`quorum_commit_advancements` and `committed_entries` observe authoritative
commit-index movement, but do not measure quorum latency or committed bytes.
The surface also does not export latency histograms, total network bytes,
device writes, the exact replicated split or move phase, controller queue
depth, catalog age, or leader identity.

## Performance contract

Observability must remain downstream of correctness and off the hot path:

- Mutation paths update fixed-width atomics or already-required progress
  records only.
- Public encoding reads one bounded snapshot and uses direct `vibejson`.
- Group, node, and operation identities remain fixed-width values rather than
  unbounded labels.
- Collectors bound concurrency, response bytes, and scrape time.
- Telemetry failure never authorizes routing, membership, split, move, cleanup,
  or acknowledgement.

Live backup reports logical artifact bytes separately from snapshot scan bytes.
The current deterministic hash-before-stream design reads the pinned image
twice, so the scan counter advances by exactly 2× encoded artifact bytes. This
is explicit read amplification. The repository does not create a second
artifact disk copy. Any future one-pass framing change must preserve the exact
header/hash contract and update this evidence rather than hiding the scan.
