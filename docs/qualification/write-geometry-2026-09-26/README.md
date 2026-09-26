# Runtime write geometry and read control, 2026-09-26

These are shared-host diagnostics for storage changes in PR #253, measured against
main `f9a9627a513a1599bc00bdaf59f4eb93470d9fbf`. The measured leaf implementation is
included in `94accf7d3276b9235ea6f9c347d222aeeb1523e0`. They do not demonstrate the
requested 10–100× write improvement or an RF3/network throughput improvement.

## Method

The host was an Apple M4 Max running Darwin/arm64 and Go 1.27.0 with
`GOEXPERIMENT=simd`. Other applications remained active. Builds and runs used
`/Users/thesyncim/.codex/bin/project-env` and the shared project Go cache.

Each write operation inserts 131,072 rows in 2,048 batches of 64 through SQL
`ReplicatedApply`. Its three durable members are system state, the user relation,
and transition capture. Row and command preparation, apply, and the final full
checkpoint are timed. Proposal admission, network traffic and Raft replication
are excluded. Rows and completion records are verified outside the timer.

The baseline overlays main's changed storage implementation into the same
benchmark scaffold. The benchmark source is identical in both builds. Three
pairs alternate execution order for each workload. File footprint is sampled
every 64 batches outside timing; an observed peak is not a guaranteed maximum.
[Source and binary provenance](metadata.json) and [all write measurements](write-runs.json)
are included; private host process inventories are omitted.

## Write results

Medians of three independent fixed-size runs:

| Metric | Main, shared | Leaf, shared | Main, varied | Leaf, varied |
| --- | ---: | ---: | ---: | ---: |
| Apply and final checkpoint | 28.237 s | 5.419 s | 24.699 s | 20.451 s |
| Speedup over main | — | 5.21× | — | 1.21× |
| Leaf splits | 627 | 31 | 511 | 390 |
| Group barriers | 2,516 | 132 | 2,052 | 1,564 |
| User device bytes | 134,651,904 | 7,200,768 | 167,862,272 | 146,350,080 |
| Final allocated file bytes | 42,487,808 | 37,789,696 | 73,195,520 | 63,688,704 |
| Timed allocated bytes | 308,750,168 | 207,953,088 | 502,282,064 | 261,160,496 |

Both workloads have 256-byte payloads. The shared case draws from eight spellings;
the varied case uses distinct generated payloads. Unindexed leaves now use the
existing compact format's 4,096-row capacity within the unchanged 64 KiB byte
bound. Maintained exact and tin indexes retain their 256-slot geometry. No
memory-reservation or retirement limits were increased.

## Read control

The separate runtime read benchmark builds and checkpoints the same 131,072-row
corpora through 64-row apply batches. It validates every row, precomputes a full
deterministic permutation, and reuses a destination sized for the largest
canonical row. Setup, warming, correctness checks and footprint measurement are
outside timing. Each operation reads every row once via `Collection.AppendRaw`.

The initial one-pass comparisons were noisy: a varied-data slowdown in one pair
reversed when execution order was reversed. The longer control below uses 50
passes over each immutable corpus. These are repeated reads of one prepared
store per corpus, not 50 independent end-to-end trials. Values are diagnostic;
[the full counters](read-runs.json) are retained.

| Corpus and order | Main ns/read | Leaf ns/read |
| --- | ---: | ---: |
| Varied, sequential | 458.0 | 441.0 |
| Varied, permuted | 761.4 | 698.4 |
| Shared, sequential | 490.8 | 273.9 |
| Shared, permuted | 624.3 | 324.2 |

Every timed case reported zero allocated bytes, allocations, cache misses, page
reads and evictions. User-collection resident bytes fell from 35,356,672 to
25,698,304 for varied data, and from 4,878,336 to 495,616 for shared data. These are
collection cache statistics, not total process RSS. The longer comparison did
not reproduce the initial slowdown; it is not a general latency guarantee.

## Validation and limits

Compact-prefix byte/row-boundary oracles, mixed mutations, held snapshots,
reopening, exact/tin index queries, structural certificate failure recovery,
overlay pressure and free-log spill were exercised. A broad storeio/durable/SQL
run exposed split-dependent test fixtures, which were repaired and rerun with
focused race and SIMD-disabled checks; the original broad run was not itself
an unconditional pass. All 43 non-optional CI checks passed on `94accf7d3`.

A bounded conditional-journal buffering experiment was reviewed and tested at
the primitive level, but was kept out of the branch after shared-workload
medians changed only from 5.372 s to 5.337 s. It combined three realistic
17,920-byte records per 64 KiB write without establishing a material overall
gain. Its durable integration crash qualification was not completed.

## Scalar alphabet packer follow-up

The alphabet writer now accumulates width-0–6 character codes in a bit reservoir
instead of calling the generic bit writer for each character. A frozen copy of
the prior scalar algorithm checks complete encoded bytes across widths, partial
tails, empty values, affixes, restart boundaries and reused scratch. Alphabet
selection, decoding, representation sizes and checkpoint policy are unchanged.

The final shared-host kernel run used a normally selected 64-symbol alphabet
with 130 rows of 256-byte payloads. Three samples per implementation measured
median 57.711 µs for the scalar reference and 20.718 µs for the reservoir
(2.79×), with zero bytes and allocations per operation. One reservoir sample was
47.365 µs, demonstrating the noise on this host. [Raw kernel samples](alphabet-kernel.txt)
and [source hashes and public-operation controls](alphabet-metadata.json) are included.

This is a kernel improvement, not an additional demonstrated application
throughput gain. The single public write pair was effectively flat: shared
5.320 s → 5.340 s, varied 18.753 s → 18.577 s. Encoded-size oracles and the
public controls preserve the space bounds, disk footprint and barrier counts.

Full storeio SIMD and SIMD-disabled suites, focused race coverage and vet
passed. An earlier concurrent run hit a page-cache queue-depth assertion;
isolated and final reruns passed. The byte-parity test runs in race builds too.
The strict warm-zero-allocation assertion runs without race instrumentation;
a race-only control requires the optimized and reference writers to have equal
allocation counts (observed two each at width zero and four each at width six).

## Reproduction

Run the write benchmarks separately with a fixed single operation:

```sh
CODEX_AGENT_ID=write-geometry GOEXPERIMENT=simd /Users/thesyncim/.codex/bin/project-env go test ./sql/driver -run '^$' -bench '^BenchmarkReplicatedApplyBatch64(SequentialInsert|CompactSharedPayload)$' -benchtime=1x -count=3 -benchmem
```

For the runtime-built warm read control:

```sh
CODEX_AGENT_ID=write-geometry-read GOEXPERIMENT=simd /Users/thesyncim/.codex/bin/project-env go test ./sql/driver -run '^$' -bench '^BenchmarkReplicatedApplyBatch64RuntimeWarmPointRead$' -benchtime=50x -count=1 -benchmem
```

A comparison must use identical benchmark source on both sides and alternate
prebuilt binaries; a single command's repetition order does not do that.
