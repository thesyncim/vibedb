# Raft WAL

`internal/raftstore` provides one encrypted, preallocated, single-writer WAL
for a Raft member. `vibedb-shard prepare-rf3` creates an initial WAL for one
member, and `vibedb-shard serve-rf3` opens one externally prepared WAL for each
local group member in its process manifest with the exact retained identity,
key, recovery epoch, and sealed bounds. Serving never invents or repairs a
missing WAL. Certified split-child and restore lifecycles have separate bounded
WAL creation paths.

## Identity

The immutable identity binds these fields:

- Cluster and cluster incarnation
- Distribution and shard
- Allocation generation and shard incarnation
- Group and member
- Store

The SQL binding also retains a separate SQL log ID. A replicated reopen must
match both identities.

## Default capacity

| Resource | Default |
| --- | ---: |
| File capacity | 256 MiB |
| Maximum record | 80 MiB |
| Record count | 65,536 |
| Entry count | 1,048,576 |
| Live-log bytes | 128 MiB |

The absolute file maximum is 4 GiB.

## Current image

Two authenticated 4 KiB current slots record adjacent generations. WAL records
form an authenticated digest chain from an immutable snapshot base.

Recovery can select the remaining authenticated slot when the inactive slot is
torn. The member enters quarantine before SQL binding after this fallback.

An unknown persistence outcome accepts only an exact retry. This rule prevents
a caller from replacing an uncertain record with different data.

The active WAL is append-only. Compaction creates an offline generation. It
does not rewrite the active file in place.

## Generation family

Every WAL is born with one mandatory authenticated 4 KiB family manifest; an
absent manifest is corruption, not a compatibility path. Its two fixed slots
start with the same source authority and thereafter admit only the adjacent
semantic sequence `source -> selecting 1 -> active 1 -> selecting 2 -> active
2 ...`. A missing/invalid peer or a pair that does not encode one legal
transition quarantines serving.

The offline builder streams the exact authenticated source cut into one
deterministic preallocated stage and retains only the suffix above the certified
SQL checkpoint. It holds no source descriptor between calls. Candidate build,
family creation, and initial WAL creation use deterministic stage witnesses.
One persistent lock derived only from the logical WAL leaf serializes initial
creation across every caller identity; a separate persistent per-family build
lease serializes later generation construction. A
clean source Open only proves an optional same-inode creation witness and pays
no directory barrier; first selection unconditionally settles that witness
before the source can be reclaimed. Selection also removes a same-inode
candidate stage under the candidate writer lock before publishing authority.
Selecting and active recovery reject a surviving creation alias. Repeated
crashes therefore cannot accumulate full-size construction files.

Selection returns the fixed-width family/generation/binding identity atomically
with its persistence result. Even an outcome-unknown family-slot write can
therefore fence SQL without a post-return query or a race with WAL close. The
source and SQL apply remain non-serving while the family is selecting.

Activation orders the exact SQL snapshot install and checkpoint before logical
WAL replacement, an unconditional parent-directory barrier, namespace proof,
the authenticated active family slot, and a final logical-name proof after that
authority is durable. Only then does raftstore mint an opaque completion
capability for that exact generation. A failed final proof keeps the same
handle fenced and retries without repeating SQL settlement. A zero, stale,
foreign, or replayed capability cannot release SQL. The old full WAL loses its
final namespace link only inside this ordered protocol, and later generations
bind the preceding generation digest.

`serve-rf3` configures the automatic generation driver for every opened local
group member. Its ordinary cadence is ten minutes of logical RF3 ticks. A hard
WAL-capacity refusal can trigger the same certified replacement earlier in an
empty input window, so a busy group is not required to survive until the next
periodic pass. Cut capture, validation, selection, and activation stay on the
serialized runtime lane; only immutable candidate construction uses one bounded
worker. An idle group whose certified base cannot advance does not manufacture
a new generation.

## Staged child base

A split child can start from a certified snapshot base whose index is newer
than one. `BindingForNewWAL` validates an intended immutable member identity
and derives its SQL binding without allocating a provisional WAL. This binding
does not grant serving authority.

After SQL child activation, `CreateStagedChildWAL` checks the live apply cut,
artifact manifest, snapshot-base state, planned SQL binding, and final WAL
identity. It creates one preallocated WAL from that base. It does not mint a
node incarnation. `AdoptRuntime` remains the ownership transfer that constructs
the Raft node.

## Replicated SQL binding

`BindReplicatedShardStore` permanently converts a prepared local SQL root. The
root must have these properties:

- No live sessions
- Exactly one user table
- No schema
- No index
- No view
- No prior materialization
- No prior local serving fence
- No transaction marker

After the binding, direct SQL DML and DDL are fenced. Replicated apply owns the
mutation path.

Identity cannot distinguish a byte-identical copied root by itself. Deployment
controls must protect physical root ownership.

## Implementation references

- `internal/raftstore/types.go`, `store.go`, and `recovery.go`
- `internal/raftstore/family.go`, `family_codec.go`, and
  `generation_activate.go`
- `sql/driver/replicated_store.go`
- `internal/raftstore/store_test.go`
- `sql/driver/replicated_store_test.go`
- `internal/raftmember/binding.go` and `staged_child.go`
- `internal/raftmember/generation_driver.go` and
  `generation_driver_test.go`
- `cmd/vibedb-shard/prepare_rf3.go`, `serve_rf3.go`, and
  `adopt_restored_rf3.go`
- `cmd/vibedb-shard/wal_pressure_process_test.go` and
  `wal_retention_process_qualification_test.go`
