# Operate replica lifecycle

This page describes the current RF3 replica-move path: enroll one cold target,
bootstrap it as a learner, promote it, transfer leadership when required,
publish routing changes, remove the retiring voter, and finalize the move.

The runtime is unreleased and experimental. The controller is deliberately
fail-closed: reachability loss alone never authorizes removal.

## How replacement advances

One move progresses through durable, idempotent evidence boundaries:

1. Persist an exact move intent in the replicated catalog group.
2. Install the transition grant on the serving voters and enrolled target.
3. Add the target as a learner.
4. Export a snapshot from a distinct healthy source voter.
5. Transfer, verify, and install the snapshot on the cold target.
6. Wait for learner catch-up, then promote the target to voter.
7. Transfer leadership if the retiring member is leader.
8. Propose the ownership transition and publish catalog generation G+1.
9. Obtain a catalog-drain certificate from the configured gateway roster.
10. Remove the retiring voter.
11. Publish the exact post-removal catalog fence at G+2 and drain it.
12. Retire the source identity, finalize the transition grant, and complete the
    journaled operation.

The gateway move controller reopens the replicated journal on every pass. A
gateway restart resumes the next unproved action instead of replaying a
process-local plan.

## Before you begin

Prepare all of the following:

- A stable serving RF3 group.
- A fourth node identity, certificate, empty retained SQL identity, apply
  identity, WAL key, and static bootstrap snapshot.
- An `enrolled_target` descriptor in each serving member manifest.
- A cold-bootstrap manifest on the target.
- A canonical gateway replica-control manifest.
- Catalog metadata containing each replica's peer, native, and control
  endpoint. The cold-bootstrap manifest carries the snapshot data endpoint.
- A gateway policy entry with `membership` and `topology` capabilities.
- A replicated failure certificate or an already-journaled move intent. A
  local timeout or failed TCP probe is not removal authority.

The target is not a serving fourth voter merely because it appears in a
manifest. It remains outside the serving RF3 until the membership protocol
adds and promotes it.

## Enroll the target in member manifests

Append `enrolled_target` after the `members` array. The exact field order is
mandatory.

```vibejson
"enrolled_target": {
  "member_id": 4,
  "node_id": "14000000000000000000000000000000",
  "store_id": "24000000000000000000000000000000",
  "node_incarnation": 1,
  "peer_address": "127.0.0.1:7414",
  "native_address": "127.0.0.1:7514",
  "snapshot_address": "127.0.0.1:7614",
  "control_address": "127.0.0.1:7714"
}
```

The member and node IDs must not duplicate a serving member. All four target
addresses must be distinct. The serving members retain exactly three entries
in `members` throughout enrollment.

## Prepare the cold learner

Create this strict, ordered manifest on the target:

```vibejson
{
  "member_manifest": "/srv/vibedb/member-4/member.vibejson",
  "control_listener": "127.0.0.1:7714",
  "source_node": "13000000000000000000000000000000",
  "source_snapshot_address": "127.0.0.1:7613",
  "repository_path": "/srv/vibedb/member-4/bootstrap-artifacts",
  "cursor_path": "/srv/vibedb/member-4/bootstrap.cursor",
  "journal_path": "/srv/vibedb/member-4/bootstrap-journal",
  "static_bootstrap_path": "/srv/vibedb/member-4/static-bootstrap.pb",
  "max_artifact_bytes": 1073741824
}
```

`member_manifest` points to the target's ordinary RF3 manifest. That manifest
still lists the original three voters and the same enrolled target. Its local
retained SQL identity must bind member 4 and the target store ID.

Start the cold target before the controller reaches snapshot bootstrap:

```bash
./bin/vibedb-shard bootstrap-rf3 -manifest ./member-4-bootstrap.vibejson
```

If the target WAL does not exist, the command listens only on the bootstrap
control address. It accepts one authorized snapshot, verifies and installs the
artifact, opens the learner in Multi-Raft, then transitions into the ordinary
`serve-rf3` path. Native data ingress remains fenced until promotion and catalog
publication authorize the target. If the WAL already exists, the command
reopens through `serve-rf3` directly.

Keep the cursor, journal, repository, WAL, and SQL paths distinct. The source
node must be one of the three serving members and must match the source snapshot
endpoint. A failed or retiring member is not a valid snapshot donor.

## Configure the gateway move controller

The gateway replica-control manifest is canonical output from `vibejson.Marshal`:
it must contain no whitespace or trailing newline, and arrays must be sorted by
their binary identities. This example is intentionally one line.

```vibejson
{"generation":1,"local_gateway":{"node":"01000000000000000000000000000000","incarnation":1,"control_address":"127.0.0.1:7101"},"tls":{"certificate":"./secrets/gateway-cert.pem","key":"./secrets/gateway-key.pem","roots":"./secrets/cluster-roots.pem","identity_oid":"1.3.6.1.4.1.32473.1.1","authorization_policy":"./secrets/authorization-policy.vibejson"},"bounds":{"max_connections":32,"max_handshakes":8,"max_concurrent_drains":4,"controller_interval_millis":100,"read_timeout_millis":1000,"write_timeout_millis":1000},"shard_endpoints":[{"node":"11000000000000000000000000000000","control_address":"127.0.0.1:7711"},{"node":"12000000000000000000000000000000","control_address":"127.0.0.1:7712"},{"node":"13000000000000000000000000000000","control_address":"127.0.0.1:7713"},{"node":"14000000000000000000000000000000","control_address":"127.0.0.1:7714"}],"gateway_endpoints":[{"node":"01000000000000000000000000000000","incarnation":1,"control_address":"127.0.0.1:7101"}],"candidates":[{"member":4,"node":"14000000000000000000000000000000","store":"24000000000000000000000000000000","node_incarnation":1,"endpoint":"member-4-control","load":0}]}
```

Start the normal authenticated gateway with the additional flag:

```bash
./bin/vibedb-gateway serve \
  -catalog ./cluster.vibejson \
  -catalog-relation 1 \
  -catalog-session-journal ./state/gateway-catalog-session \
  -catalog-client-id 21000000000000000000000000000000 \
  -catalog-retry-home 2200000000000000 \
  -listen 127.0.0.1:7400 \
  -tls-certificate ./secrets/gateway-cert.pem \
  -tls-key ./secrets/gateway-key.pem \
  -tls-roots ./secrets/cluster-roots.pem \
  -tls-identity-oid 1.3.6.1.4.1.32473.1.1 \
  -authorization-policy ./secrets/authorization-policy.vibejson \
  -replica-control-manifest ./replica-control.vibejson \
  -shard-peer 127.0.0.1:7511=11000000000000000000000000000000 \
  -shard-peer 127.0.0.1:7512=12000000000000000000000000000000 \
  -shard-peer 127.0.0.1:7513=13000000000000000000000000000000
```

The manifest TLS paths must exactly equal the corresponding gateway flags.
Every listed gateway identity needs `topology`. The local gateway also needs
`membership`. Catalog control endpoints must match the manifest inventory.

## Trigger and observe a replacement

The move executor consumes exact move intents from the replicated catalog
journal. It does not accept an unauthenticated CLI request. On each configured
controller interval, the gateway polls the three authenticated shard-control
endpoints. It accepts a health cut only when the current leader answers and a
quorum agrees on leader, term, commit index, and replica-set version.

When only a quorum agrees, the absent or nonagreeing member becomes the one
suspect. The gateway publishes the observation through an exact CAS in the
catalog Raft group. Three consecutive replicated failure revisions produce the
certificate that can authorize replacement. A full three-member agreement
clears prior suspicion. Process-local elapsed time and failed pings cannot
create removal authority.

The certified scheduler selects a configured candidate, binds the certificate
and placement evidence into an exact move intent, persists that intent, and
hands it to the same resumable move controller. A gateway restart reopens the
health revision and move journals before it advances either workflow.

Watch the gateway's standard error stream. It reports controller passes that
advance or complete operations:

```text
gateway: replica move controller advanced 1/1 move(s), completed 0
```

Shard startup logs report member, replica-set version, and listener addresses.
Authenticated controller observations carry current leader, term, commit, and
applied state, but the command has no public status formatter for them.
`vibedb-gateway inspect -catalog ./cluster.vibejson`
inspects the bootstrap file only. It is not a live status command for the
authoritative replicated head.

There is currently no public CLI for manually creating a move intent, listing
move-journal records, or transferring leadership on demand. Those actions are
available through the authenticated controller path after an authorized intent
exists. Do not edit the catalog file or action journals to force a transition.

## Test failure and recovery

Use a disposable prepared cluster. Use the authenticated replica-control
observation protocol in the test harness to identify the current leader, then
stop it with `SIGKILL`. The command does not provide a public status CLI. The
two remaining voters should elect a leader and continue quorum-backed
operations. Restart the stopped process with the exact same manifest and
artifacts.

Exercise at least these cuts:

- Stop the leader before proposal admission.
- Stop it after sending a request but before receiving a response. Retry the
  exact request bytes and request ID.
- Isolate the former leader and verify it refuses a linearizable read.
- Restart a follower and wait for its applied index to catch the leader.
- Interrupt cold bootstrap, restart `bootstrap-rf3` with the same manifest,
  and verify the journal resumes the same descriptor.
- Interrupt the gateway during each replica-move action and verify the next
  process resumes the replicated move journal.

Never delete a WAL, bootstrap journal, action journal, snapshot cursor, or
source repository to make a retry succeed. Preserve the first error and inspect
identity mismatches before changing any artifact.

The current process test restarts the source repository and completes a real
transfer through the authenticated snapshot listener into the bootstrap
receiver. The complete replica-replacement sequence does not yet have one
external multi-process qualification gate across every action above.

## Metrics and troubleshooting

The current commands expose bounded internal counters and structured stderr
messages, but no Prometheus or HTTP metrics endpoint. Process-level resource
metrics must come from the host or test harness. Do not scrape logs as a stable
API.

Check these symptoms first:

| Symptom | Check |
| --- | --- |
| Manifest rejected before listen | Field order, duplicate fields, absolute clean paths, roster sort order, and file size. |
| TLS handshake rejected | CA roots, critical identity OID, trust domain, exact node ID, traffic class, and policy generation. |
| WAL rejected on reopen | Exact five WAL bounds, key ID, and 32 raw key bytes. |
| Target never leaves cold bootstrap | Source node/address match, topology capability, static bootstrap identity, artifact bound, and retained bootstrap journal. |
| Membership action rejected | Transition grant installed on all required peers, exact replica-set version, current leader/term, and target identity. |
| Move waits at catalog drain | Every configured gateway must acknowledge the exact generation and catalog digest. |
| Removed source still appears routable | Wait for the G+2 post-removal catalog fence and its cluster drain. Do not retire early. |
| Linearizable read returns `NotLeader` | Retry through the gateway so leader routing and the current serving fence are re-resolved. |
| Transaction outcome is unknown | Retry the exact request ID and bytes. Never invent a new ID for the same logical mutation. |

Important operational gaps remain:

- No cold-learner artifact provisioner or RF3 repair command.
- No public move, leader-transfer, or live-status CLI.
- No public metrics endpoint or alert bundle.
- No distributed DDL rollout command.
- No backup/restore operating contract for a live RF3 cluster.
- No mixed-build rolling wire- or disk-format upgrade or migration policy.
  The exact same-build pre-release restart boundary is documented in
  [Unreleased compatibility and rolling restarts](unreleased-compatibility.md).
- External multi-process crash and partition coverage is not exhaustive.
- The runtime has no release or production-support contract.

Use [Distributed feature state](../distributed-feature-state.md) as the
evidence matrix for what is implemented, integrated, shipped, and fault-tested.
