# Request authority across replica membership changes

Adding or removing a replica does not change a transaction's logical table,
schema, range ownership, or request identity. Newly built durable SQL programs
therefore use explicit membership-stable data and route-session command
classes. A command from an earlier nonzero replica-set version can execute on
the same logical authority at a later membership version. A command from a
future version is rejected.

The gateway still obtains the current leader's exact physical serving fence.
Cluster and shard incarnations, allocation, policy, protection, ownership,
schema, relation manifest, routing, and route generation remain checked. This
is not permission to replay a command against a different table incarnation
or to bypass a topology/schema rollout.

## Durable compatibility

The existing protocol-program digest seals the command mode using a separate
hash domain. The plan and command envelopes do not grow. The execution-pin
binding includes this digest. Transaction authority witnesses omit only the
physical membership coordinate and use a distinct hash domain.

Retained legacy plans keep their original command class and exact-membership
semantics. Retrying never changes client, transaction, or request identities,
or rewrites an already staged command. This change does not by itself repair
a legacy request already stranded by a membership change.

Membership-stable transaction completions retain the original command's
membership metadata. Historical step completion may describe a later durable
transaction-control revision; it is not necessarily the original reply's
applied index. Replaying that step does not execute its mutation again.

All readers of the new command classes must be upgraded before enabling new
SQL requests. Older binaries cannot decode these classes; do not roll back
to them after new commands have been durably appended.

## Focused evidence

- `TestDurableRequestAdmissionContinuesAcrossLearnerAddition` covers route-gate
  admission, retained-command retry, catalog publication, and session cleanup.
- `TestMembershipStableTransactionFinishesAcrossLostResponsesAndReopen`
  changes membership before each transaction stage, loses each reply, reopens
  durable storage, checks row visibility, and retries historical settlement.
- `TestDurableRequestDistributedRunnerResumesProtocolCuts` exercises committed
  and aborted recovery cuts in both legacy and membership-stable modes.
- `TestMembershipStableTransactionRecoveryRetainsLogicalAndServingFences`
  checks leader-only recovery and rejects other authority changes.
- The hot-shard external process test exercises real owners and transport.

These tests are correctness evidence, not a comparative performance result.
