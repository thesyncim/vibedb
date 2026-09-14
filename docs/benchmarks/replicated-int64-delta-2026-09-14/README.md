# Replicated integer delta writes

JID1 adds an apply-time mutation for the narrow, common SQL shape
`UPDATE table SET integer_column = integer_column + $1 WHERE primary_key = $2`.
The gateway sends the column name and the bound signed integer delta instead of
reading the committed row, materializing a full replacement document, and
putting that document in the replicated command. `vibejson` parses the row with
zero-copy options at apply time and rewrites only the selected top-level value;
unchanged keys and values retain their original JSON bytes. The existing
digest-guarded materialization path remains the fallback for every unsupported
statement shape.

The candidate was measured as the working tree on top of
`df5c0637375a89e0b51a48ba76961521cca309bb` (PR #236), so the source revision
is shared with the baseline but the candidate includes the uncommitted JID1
files shown in this change. The benchmark uses the same durable settings,
schema, fixture, and client workload for both builds.

## Matched SQL results

The runner used one Linux arm64 container with 12 CPUs and 24 GiB, CRDB
v26.3.1 at its pinned image digest, three CRDB nodes, and VibeDB RF3 (three
catalog, three ledger, and three data nodes). Each engine was stopped before
the other engine started; the runner removed its container and volume after
each campaign. Both engines used 8,192 rows, `update_existing`, eight clients,
5,000 warmups, 50,000 measured operations, and three repetitions. Every trial
returned `verified=true` with zero operation errors.

VibeDB-first, 50,000-operation trials:

| build | trial 1 | trial 2 | trial 3 | median |
| --- | ---: | ---: | ---: | ---: |
| baseline | 812.603 | 587.746 | 611.104 | 611.104 |
| JID1 | 1,713.544 | 1,790.761 | 1,272.699 | 1,713.544 |

The median is 2.80× higher (1,592.3 versus 670.5 ops/s by arithmetic mean).
The corresponding per-trial latency was:

| build/trial | p50 | p95 | p99 (ms) |
| --- | ---: | ---: | ---: |
| baseline/1 | 7.620 | 23.904 | 43.583 |
| baseline/2 | 9.427 | 37.381 | 68.525 |
| baseline/3 | 8.207 | 35.717 | 72.099 |
| JID1/1 | 3.782 | 11.382 | 17.459 |
| JID1/2 | 3.681 | 11.191 | 17.128 |
| JID1/3 | 4.591 | 16.080 | 30.554 |

The reverse CRDB-first campaign retained the full variability rather than
discarding an unfavorable trial. JID1 VibeDB measured 1,958.173, 1,625.242,
and 625.467 ops/s (median 1,625.242); the CRDB controls measured 1,426.796,
1,032.807, and 2,255.373 ops/s. The 625.467 trial had a 175.913 ms p99.
This confirms that the single-host fixture has periodic stalls and that the
VibeDB-first median is the useful paired signal, rather than evidence for a
stable 10× end-to-end result. The matched CRDB control moved about 1.10× in
median between the VibeDB-first baseline and candidate campaigns.

The smaller command also reduced the measured VibeDB fixture footprint: the
baseline `/data/vibe` was about 1,225,112 KiB with roughly 260–263 MiB node
logs, while JID1 was about 1,060,800 KiB with roughly 206–208 MiB node logs.
No durability or quorum setting was changed; this is reduced replicated
proposal/write amplification.

Reproduction command (run once from the candidate worktree and once from a
clean checkout of `df5c0637375a89e0b51a48ba76961521cca309bb`):

```sh
PATH=/private/tmp/vibedb-go-shim:$PATH \
CODEX_AGENT_ID=write_validation \
/Users/thesyncim/.codex/bin/project-env python3 scripts/bench/run-crdb-sql-comparison.py /new/evidence/path \
  --node-log --rows 8192 --operations 50000 --scans 5000 \
  --warmup 5000 --repetitions 3 --clients 8 \
  --workloads update_existing --order vibedb-first --timeout 30m
```

The reverse-order check uses `--order crdb-first` with the same arguments.

## What the optimization supports

JID1 is selected only for one direct, primary-key point UPDATE with a single
top-level declared JSON `INTEGER` assignment. The expression must be an exact
closed `column + integer` or `column - integer` operation, including a bound
integer parameter; primary-key updates, indexed-table mutations, RETURNING,
multiple assignments, nested paths, and other expressions use the existing
path. SQL integer aliases collapse to the repository's JSON integer type and
use exact signed int64 arithmetic.

At apply time a missing target row remains a zero-row update. A missing or JSON
null current value propagates JSON null, and normal schema validation preserves
NOT NULL behavior. Overflow and invalid stored values produce the existing
deterministic invalid-document result while the Raft log advances. A repeated
request reuses the original JID1 command and does not increment twice; gateway
replanning after restart reconstructs that same command instead of creating a
new preimage-dependent digest. Tests also cover preservation of unrelated
fields, concurrent same-key increments, and reopen/replay.

JID1 is mutation kind 10 and is included in the authenticated apply-contract
digest. An existing durable cluster must upgrade all replicas and complete the
repository's authenticated apply-contract/schema transition before enabling
JID1. An old binary rejects the new descriptor; this is an explicit deployment
compatibility requirement, not an assumed mixed-version capability. Existing
mutation kinds and their replay behavior are unchanged.

## Remaining write cost

A diagnostic profiled run of the candidate (`1,024` measured operations,
`1,024` rows, eight clients) recorded about 5.5–6.6% of samples in file
`fsync`, 4.9% in `storeio.dataSync`, 2.0% in Raft `Fdatasync`, and about 2% in
checkpoint/apply work. These are the remaining durable replication and storage
barriers; this change does not overlap or remove them. The measured result
therefore supports a material 2.80× improvement for this workload, not the
10× goal across all writes.

## Validation

All Go commands used `/Users/thesyncim/.codex/bin/project-env`:

```sh
CODEX_AGENT_ID=write_validation /Users/thesyncim/.codex/bin/project-env go test ./gateway ./internal/replication ./internal/replicatedstate -count=1 -timeout=20m
CODEX_AGENT_ID=write_validation /Users/thesyncim/.codex/bin/project-env go test -race ./gateway -run 'TestPreparedDirectIntegerUpdate|TestDurableSQLSingleTargetFastPathSkipsLedgerAndReplaysExactly|TestReplicatedDirectMutationIsOneProposalWithCrossGatewayExactRetry|TestReplicatedDirectInt64DeltaConcurrentSameKeyIncrements' -count=1 -timeout=15m
CODEX_AGENT_ID=write_validation /Users/thesyncim/.codex/bin/project-env go test -race ./internal/replication ./internal/replicatedstate -run 'TestJSONInt64Delta|TestMaterializeJSONInt64Delta|TestGolden|TestApplyContract|TestTransition' -count=1 -timeout=15m
```

The first command passed gateway (84.413s), replication (0.520s), and
replicatedstate (104.734s). Both race-focused commands passed, as did the
dedicated concurrent same-key test in normal and race modes. The SQL-path
tests prove JID1 was decoded from the actual direct proposal and that replay
uses JID1 again; planner-only coverage is not the performance evidence.
