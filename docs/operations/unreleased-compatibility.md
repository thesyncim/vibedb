# Unreleased compatibility and rolling restarts

VibeDB has no released wire or disk format. It therefore has no mixed-build
rolling-upgrade, downgrade, or migration promise yet. Do not interpret format
sentinels as public version numbers.

The supported pre-release operational boundary is narrower:

- A rolling **restart** is allowed only when every restarted process uses the
  exact same tested build grammar as the remaining processes and its durable
  image.
- A rolling **binary upgrade** across different grammar identities is not
  supported, even when both commits call their internal image "format 0".
- Back up data before changing commits. Keep the producer build available until
  the replacement cluster has been qualified independently.

## What fails closed

Internal Raft, snapshot, shard-native, shard-SQL, and shard-control connections
exchange a fixed authenticated build preface before application frames. Peer
admission requires exact wire and disk grammar identities plus mutual required
capabilities. A mismatch closes the connection; it is not negotiated.

A durable image carries its exact disk identity. Startup inspects that identity
before mutation or repair and issues a permit only for the current grammar and
available required capabilities. There is no compatibility decoder or implicit
migration.

These two checks intentionally reject a mixed-build rolling deployment before
it can partially serve or rewrite state.

## Schema-generation rollout is different

A table/relation schema rollout does not relax binary compatibility. It is a
replicated data transition inside one exact build contract:

1. Every affected RF3 group proves the exact old and new schema generation,
   relation-manifest digest, installation digest, and schema-rollout contract
   digest.
2. The catalog records one bounded prepared operation.
3. Activation publishes the exact certified target catalog generation.
4. A restarted controller resumes from replicated operation and catalog state.

Mixed schema-install contracts, partially old/new shard cuts, stale old bundles,
and a different activation target fail closed. After the Raft-ordered schema
transition, a shard reopens only with the exact target generation, manifest,
apply contract, membership witness, authorization digest, and catalog-CAS
digest.

## Operator procedure for a same-build rolling restart

1. Record the exact source revision and artifact digest used by every node.
2. Confirm the replacement process uses that identical artifact and unchanged
   durable roots.
3. Restart one member at a time and wait for it to rejoin and catch up before
   moving to the next member.
4. Keep quorum throughout the restart and verify leader routing after each
   member returns.
5. Stop if peer admission reports a build mismatch or disk adoption reports a
   grammar/capability mismatch. Do not bypass the gate or copy files into a new
   format.

The qualification tests are
`internal/buildgate.TestUnreleasedRollingRestartBoundary`,
`gateway.TestUnreleasedSchemaRolloutRollingRestartBoundary`, and
`internal/replicatedstate.TestMachineSchemaTransitionFencesOldBundleAndReopensExactTarget`.
They prove the current pre-release boundary; they are not a release support
policy.
