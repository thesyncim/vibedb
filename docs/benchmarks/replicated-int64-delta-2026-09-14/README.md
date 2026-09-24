# Replicated integer delta writes

JID1 adds an apply-time mutation for the narrow, common SQL shape
`UPDATE table SET integer_column = integer_column + $1 WHERE primary_key = $2`.
The gateway sends the column name and the bound signed integer delta instead of
reading the committed row, materializing a full replacement document, and
putting that document in the replicated command. JID2 extends the same
operation to two or more independent integer self-deltas in one UPDATE; the
apply point parses the source row once and evaluates every assignment against
that original row. `vibejson` parses with zero-copy options at apply time and
unchanged keys and values retain their original JSON bytes. The existing
digest-guarded materialization path remains the fallback for every unsupported
statement shape.

The JID1 production candidate is `aa25f6e0d` on top of the clean PR #236
revision `df5c0637375a89e0b51a48ba76961521cca309bb`; the clean baseline is
that latter revision. The long production trials were first measured from the
equivalent candidate working tree before the JID1 commit, and the
retry-to-completion trials used `aa25f6e0d`. The JID2 extension was measured
from `825620ffd` plus the working-tree patch; its exact source bytes and the
matching harness-only baseline are recorded in each campaign's
`source.patch`. Every comparison uses the same durable settings, schema,
fixture, and client workload for both builds.

## Matched SQL results

The runner used one Linux arm64 container with 12 CPUs and 24 GiB, CRDB
v26.3.1 at its pinned image digest, three CRDB nodes, and VibeDB RF3 (three
catalog, three ledger, and three data logical Raft members). With `--node-log`,
those nine VibeDB members run in three `vibedb-shard serve-node` physical
processes, each with its embedded frontend; the gateway is not a ninth
process. Each engine was stopped before the other engine started; the runner
removed its container and volume after each campaign. Both engines used 8,192
rows, `update_existing`, eight clients, 5,000 warmups, 50,000 measured
operations, and three repetitions. Every trial returned `verified=true` with
zero operation errors.

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

## Multi-field integer updates

JID2 uses a bounded descriptor for one primary-key point UPDATE containing
independent assignments such as `SET score=score+1,counter=counter+1`. The
benchmark adds `counter` only for this workload, initializes both counters to
the same deterministic starting value, and checks both columns after every
trial. The expected values come from successful writes, independently of the
final database read; because both fields receive the same delta, the verifier
uses that one expected-value array for both columns. The apply path uses one
`vibejson.ParseOptions(... ZeroCopy:true)` pass over the source object, builds
both replacement values before publishing the row, and retains JID1 for the
single-assignment case.

The matched campaign used source `825620ffd` plus the JID2 working-tree patch
(the candidate `source.patch` records the exact bytes), and a detached
`825620ffd` baseline with only the benchmark harness extension. Both used
8,192 rows, 1,000 warmups, 10,000 measured C8 operations, three repetitions,
RF3/durable settings, and zero transient-retry allowance. Every trial had
10,000 successes, zero errors, `verified=true`, and matching score/counter
checks against the successful-write oracle.

VibeDB-first:

| build | trial 1 | trial 2 | trial 3 | median |
| --- | ---: | ---: | ---: | ---: |
| baseline | 722.899 | 716.363 | 730.154 | 722.899 |
| JID2 | 1,559.206 | 1,645.414 | 1,530.012 | 1,559.206 |
| CRDB baseline control | 1,431.411 | 1,307.878 | 2,465.307 | 1,431.411 |
| CRDB JID2 control | 2,804.449 | 2,853.142 | 2,701.033 | 2,804.449 |

The first-order VibeDB medians differ by 2.16×, but the CRDB controls moved
substantially between campaigns, so this is recorded as an observed
between-campaign result rather than a stable uniform improvement. A balanced
CRDB-first confirmation retained the control difference and did not reproduce
the gain:

| build | trial 1 | trial 2 | trial 3 | median |
| --- | ---: | ---: | ---: | ---: |
| baseline | 1,439.094 | 1,404.685 | 1,394.432 | 1,404.685 |
| JID2 | 1,522.508 | 1,321.003 | 1,358.147 | 1,358.147 |
| CRDB baseline control | 2,764.953 | 2,854.663 | 2,882.876 | 2,854.663 |
| CRDB JID2 control | 2,952.786 | 2,898.551 | 2,857.256 | 2,898.551 |

Across the reverse pair, JID2 was 0.97× the baseline median. The extension is
therefore useful for avoiding the multi-field preimage path under contention,
but this campaign establishes no general JID2 throughput gain and does not
support a universal 10× claim.

The smaller command also reduced the measured VibeDB fixture footprint: the
baseline `/data/vibe` was about 1,225,112 KiB with roughly 260–263 MiB node
logs, while JID1 was about 1,060,800 KiB with roughly 206–208 MiB node logs.
No durability or quorum setting was changed; this is reduced replicated
proposal/write amplification.

Reproduction command (run once from the candidate worktree and once from a
clean checkout of `df5c0637375a89e0b51a48ba76961521cca309bb`):

```sh
PATH=/private/tmp/vibedb-go-shim:$PATH \
python3 scripts/bench/run-crdb-sql-comparison.py /new/evidence/path \
  --node-log --rows 8192 --operations 50000 --scans 5000 \
  --warmup 5000 --repetitions 3 --clients 8 \
  --workloads update_existing --order vibedb-first --timeout 30m
```

The reverse-order check uses `--order crdb-first` with the same arguments.

## High-contention point increments

The checked-in `rf3-sqlbench` harness has an explicit `update_hot` workload.
All eight clients increment the same existing primary-key row. The operation
stream uses no retries by default: `successful_ops_per_second` counts only
completed operations and `Errors` remains visible. A separate atomic success
count updates the final verification oracle after the concurrent phase, so a
failed transaction cannot be counted as a write.

The 10,000-operation no-retry confirmation used 8,192 rows, 1,000 warmups,
eight clients, three repetitions, and the same RF3/durability settings as the
uniform run. The VibeDB-first order completed all candidate trials, while the
baseline stopped after its second trial encountered one SQLSTATE 40001:

| engine/build | trial 1 | trial 2 | trial 3 | completed median |
| --- | ---: | ---: | ---: | ---: |
| VibeDB baseline | 334.166 | 315.332 (9,999; 1 error) | — | — |
| VibeDB JID1 | 2,015.431 | 2,222.487 | 2,408.554 | 2,222.487 |
| CRDB baseline control | 774.119 | 427.343 | 771.947 | 771.947 |
| CRDB JID1 control | 650.415 | 451.504 | 388.999 | 451.504 |

The reverse CRDB-first order retained the same behavior: JID1 VibeDB measured
1,782.836, 2,035.283, and 2,605.994 ops/s, while the baseline stopped after
175.279 ops/s with 9,999 successes and one 40001. Its CRDB controls measured
316.877, 374.488, and 392.032 ops/s; JID1's controls measured 678.928,
772.920, and 795.762 ops/s. The baseline failure was a transaction conflict,
and the verification pass itself succeeded; `verified=false` reflected the
nonzero error count.

For a fair completed-write comparison, `--retry-transient` retries only
SQLSTATE 40001 and 40P01, up to 64 extra attempts with a bounded 100 µs to
2 ms delay. The same policy is applied to both engines, and the report keeps
`successful_ops`, `attempts`, `transient_retries`, `errors`, and the
`expected_hot_scores`/`observed_hot_scores` fields. The 10,000-operation,
1,000-warmup, three-repetition VibeDB-first run completed every logical write
with matching score and counter checks against the successful-write oracle:

| engine/build | trial 1 | trial 2 | trial 3 | median |
| --- | ---: | ---: | ---: | ---: |
| VibeDB baseline | 102.274 (10,002/2) | 101.365 (10,001/1) | 135.598 (10,000/0) | 102.274 |
| VibeDB JID1 | 1,884.631 (10,000/0) | 2,187.210 (10,000/0) | 1,924.273 (10,000/0) | 1,924.273 |
| CRDB baseline control | 187.220 (10,000/0) | 242.400 (10,000/0) | 224.184 (10,000/0) | 224.184 |
| CRDB JID1 control | 223.540 (10,000/0) | 319.309 (10,000/0) | 226.191 (10,000/0) | 226.191 |

Each parenthesized pair is `attempts/transient_retries`; every row had zero
final errors, 10,000 successful logical operations, `verified=true`, and an
expected score equal to the independently read score (11,000, 22,000, and
33,000 after the three repetitions). The JID1 median is 18.81× the baseline
median for this fixed-key, retry-to-completion workload. Its per-trial
p50/p95/p99 latencies were 3.384/9.767/15.666 ms, 2.708/10.859/20.824 ms,
and 2.735/15.866/23.212 ms; baseline p50/p95/p99 were
31.635/304.787/640.846 ms, 32.638/278.151/572.551 ms, and
24.279/204.828/443.473 ms. The CRDB controls were stable in this matched
run (baseline median 224.184, candidate median 226.191 ops/s).

The earlier 2,000-operation pilot was 21.36× (2,128.621 versus 99.635
ops/s), but the longer run is the headline because it reports every logical
write, retries, and an independent final counter. The uniform workload remains
the broader end-to-end comparison at 2.80× median throughput. These results
support a scoped 10× improvement for eligible hot integer point updates, not a
universal 10× claim across arbitrary writes.

The retry-mode campaigns were reproduced with:

```sh
PATH=/private/tmp/vibedb-go-shim:$PATH \
python3 scripts/bench/run-crdb-sql-comparison.py /new/evidence/path \
  --node-log --rows 8192 --operations 10000 --scans 1000 \
  --warmup 1000 --repetitions 3 --clients 8 \
  --workloads update_hot --retry-transient --order vibedb-first --timeout 30m
```

The corresponding two-counter contention workload was `update_multi_hot`.
With 8,192 rows, 1,000 warmups, 10,000 measured C8 operations, three
repetitions, and the same bounded retry-to-completion policy, all trials
completed 10,000 logical writes with matching score/counter checks against the
successful-write oracle. The report's archived hot fields contain the expected
and observed `score` values; the full verification query checks `counter` too
using the same expected-value array because both fields start and advance
identically:

| engine/build | trial 1 | trial 2 | trial 3 | median |
| --- | ---: | ---: | ---: | ---: |
| VibeDB baseline | 246.113 | 250.049 | 114.359 | 246.113 |
| VibeDB JID2 | 1,561.807 | 1,551.537 | 1,508.218 | 1,551.537 |
| CRDB baseline control | 224.038 | 167.138 | 210.529 | 210.529 |
| CRDB JID2 control | 537.224 | 577.857 | 555.838 | 555.838 |

Baseline attempts/retries were 10,003/3, 10,001/1, and 10,004/4; JID2 had
10,000/0 on every trial. The observed JID2 median is 6.30× the baseline
median for this fixed-key two-counter workload. CRDB controls also drifted
between the two campaigns, so this is a scoped contention result rather than
a general engine comparison. The existing single-field JID1 hot workload's
separate 18.81× result remains the strongest contention result in this
campaign.

## Runtime allocation diagnostic

The default runner leaves Go's process setting unchanged. A temporary wrapper
ran the same 20,000-operation C8 workload with `GOMAXPROCS=4` for each VibeDB
physical process, each CRDB process, and the benchmark client. The three VibeDB
and three CRDB process counts make this an equal 12-process-CPU allocation;
the wrapper did not change production code or durability settings.

The two VibeDB baseline trials at that setting were 2,066.183 and 2,023.425
ops/s; the JID1 trials were 1,956.066 and 1,872.762 ops/s. Their two-trial
midpoints were 2,044.804 and 1,914.414 ops/s, so this short pair did not show
an additional JID1 gain under the tuned setting. The matching CRDB controls
were 1,894.317 and 1,856.834 ops/s for the baseline campaign and 4,244.673
and 4,040.010 for the JID1 campaign. A same-size default baseline control
measured 1,173.590 and 1,031.736 ops/s (midpoint 1,102.663), which is a useful
oversubscription diagnostic but is too short and variable to turn into a
universal deployment claim. The JID1 SQL result above therefore retains the
unchanged-default, long-run comparison.

## What the optimization supports

JID1 is selected for one direct, primary-key point UPDATE with a single
top-level declared JSON `INTEGER` assignment. JID2 is selected for two or more
distinct assignments of the same closed form. Each expression must be an
exact `column + integer` or `column - integer` operation, including a bound
integer parameter; primary-key assignments, maintained global indexes,
RETURNING, duplicate targets, cross-field RHS references, nested paths, and
other expressions use the existing path. Local index maintenance still runs
through the normal apply pipeline. SQL integer aliases collapse to the
repository's JSON integer type and carry signed int64 deltas. The stored value
and result retain the repository's exact arbitrary-width JSON integer
semantics, including values beyond int64; only exponent-free JSON integer
spellings are accepted, so `1.0` and `1e0` remain on the existing invalid-value
path just as they do for a materialized `INTEGER` update.

At apply time a missing target row remains a zero-row update. A missing or JSON
null current value propagates JSON null, and normal schema validation preserves
NOT NULL behavior. Malformed or non-integer stored values produce the existing
deterministic invalid-document result while the Raft log advances; valid
arithmetic results that exceed int64 remain exact until the normal
document-size bound is reached. A repeated request reuses the original JID1 or
JID2 command and does not increment twice; gateway replanning after restart
reconstructs that same command instead of creating a new preimage-dependent
digest. Tests cover simultaneous original-row evaluation, duplicate and
malformed descriptors, preservation of unrelated fields, concurrent same-key
single- and multi-field increments, and reopen/replay.

JID2 is selected only when every assignment is a distinct top-level declared
JSON `INTEGER` column with a closed self-delta expression. Cross-field RHS
dependencies, duplicate targets, primary-key assignments, maintained global
indexes, RETURNING, nested paths, and unsupported arithmetic remain on the
existing path.

JID1 and JID2 are mutation kind 10 and are included in the authenticated
apply-contract digest. `Machine.Open` compares the persisted contract digest
with the local prepared contract, so a cluster cannot emit or apply either
descriptor under an old contract. An existing durable cluster must upgrade all
replicas and complete the repository's authenticated apply-contract/schema
transition before enabling these descriptors; that transition binds the old
and new contract digests to the membership witness, authorization digest, and
catalog CAS. An old binary rejects the new descriptor. Existing mutation kinds
and their replay behavior are unchanged.

## Remaining write cost

A diagnostic profiled run of the candidate (`1,024` measured operations,
`1,024` rows, eight clients) recorded about 5.5–6.6% of samples in file
`fsync`, 4.9% in `storeio.dataSync`, 2.0% in Raft `Fdatasync`, and about 2% in
checkpoint/apply work. These are the remaining durable replication and storage
barriers; this change does not overlap or remove them. The measured result
therefore supports a material 2.80× improvement for this workload, not the
10× goal across all writes.

## Validation

All Go commands ran through a private environment wrapper that selects the Go toolchain:

```sh
go test ./gateway ./internal/replication ./internal/replicatedstate -count=1 -timeout=20m
go test -race ./gateway -run 'TestPreparedDirectIntegerUpdate|TestDurableSQLSingleTargetFastPathSkipsLedgerAndReplaysExactly|TestReplicatedDirectMutationIsOneProposalWithCrossGatewayExactRetry|TestReplicatedDirectInt64DeltaConcurrentSameKeyIncrements' -count=1 -timeout=15m
go test -race ./internal/replication ./internal/replicatedstate -run 'TestJSONInt64Delta|TestMaterializeJSONInt64Delta|TestGolden|TestApplyContract|TestTransition' -count=1 -timeout=15m
cd integration/pgclient && go test ./cmd/rf3-sqlbench -count=1 -timeout=15m
```

The first command passed gateway (84.413s), replication (0.520s), and
replicatedstate (104.734s) before the JID2 extension. The focused JID2 suite,
including malformed framing, simultaneous application, exact wide integers,
reopen/replay, and the direct executor, passed after the extension; the race
suite also covered concurrent same-key JID1 and JID2 increments. The SQL
benchmark package test passed after adding the two-counter workloads and
score/counter verification against the successful-write oracle. The SQL-path tests prove JID2 was
decoded from the actual direct proposal with zero preimage reads; planner-only
coverage is not the performance evidence.
