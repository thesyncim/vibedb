# Unreleased compatibility and rolling restarts

VibeDB has no released API, wire, or disk format and no migration ladder. Pin a
tested commit. A numeric internal sentinel such as `format 0` identifies only
the decoder in that source tree. It is not a promise that two commits can share
data or run in one cluster.

The only operationally qualified restart is a same-binary restart: every
process comes back with the identical artifact and its unchanged durable root.
A mixed-build rolling upgrade, downgrade, or in-place data migration is not
supported.

## What production code checks today

Authenticated internal connections exchange a fixed build preface before
application frames for these traffic classes:

- ordinary Raft and snapshot transport.
- shard native, SQL, and control transport.
- gateway control transport.

The preface requires exact wire and disk grammar identities and mutual required
capabilities. A mismatch closes that internal connection. External gateway
client connections do not exchange this build preface. Their own TLS,
authorization, and request codecs remain separate boundaries.

Durable formats also validate their own magic, internal version fields,
checksums, sealed geometry, and identity records. Those checks detect malformed
or unsupported images. They do not provide cross-commit migration.

## Important current gap: disk adoption is not globally wired

`internal/buildgate` contains a fixed disk-identity codec and an
inspect-authorize-mutate permit API. Its tests prove that the API refuses a
different grammar before calling a mutation hook. Production startup code does
not currently call this shared disk-adoption boundary for the durable embedded
store, Raft WAL, SQL catalog, or request ledger.

Therefore do not rely on startup to reject every incompatible image before all
repair or rewrite paths. The previous documentation claimed that universal
enforcement. The source does not currently provide it. Until each durable root
uses the common gate, the operator boundary is stricter than the code boundary:
reuse data only with the identical tested artifact, keep backups, and qualify a
copy before changing commits.

## Schema rollout is not a binary upgrade

An RF3 table/relation schema rollout is a replicated data transition inside one
build grammar. Shards prepare exact relation bundles, catalog authority
publishes one certified target generation, and restart reopens the exact active
bundle. This machinery does not make different binaries compatible and does not
provide general SQL DDL.

## Same-binary restart procedure

1. Record the source revision and cryptographic artifact digest for every
   process.
2. Back up the data and retain the original artifact.
3. Restart one RF3 member at a time with that identical artifact and unchanged
   durable root.
4. Wait for the member to rejoin and catch up before restarting the next member.
   Keep quorum throughout.
5. Restart a gateway with its own unchanged session, issuer, and ACK journals.
6. Stop on any peer-build, durable-format, identity, capability, or catalog
   refusal. Do not bypass a check or copy individual files between roots.

The checked-in Kubernetes qualification exercises this same-build restart over
one catalog group, one request-ledger group, one data group, and one gateway.
It does not prove mixed-build upgrades or format migration.

## Source-backed checks

- `internal/rafttransport/identity.go` installs the internal build preface.
- `internal/buildgate.TestUnreleasedRollingRestartBoundary` unit-tests profile
  matching and the standalone disk-adoption API. It is not an end-to-end
  storage-startup test.
- `gateway.TestUnreleasedSchemaRolloutRollingRestartBoundary` and
  `internal/replicatedstate.TestMachineSchemaTransitionFencesOldBundleAndReopensExactTarget`
  test the same-build schema-transition boundary.
- `deploy/kubernetes/qualify-kind.sh` exercises the shipped same-binary RF3
  rolling-restart lane.

These checks define a pre-release development boundary, not a release support
policy.
