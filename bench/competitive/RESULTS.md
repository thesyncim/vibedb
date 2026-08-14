# Competitive results

> **Clean published snapshot (2026-08-14).** The mixed-throughput and
> concurrency tables were regenerated from engine commit
> `8c1142322c96cfe8f99d04cd04683a5d827e6710` on an Apple M4 Max, with engines
> run serially in isolated child processes.
> Each of the eleven TSVs independently records that full Git commit,
> `git-dirty=false`, an empty `git-status`, the empty tracked-diff SHA-256
> `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`, all
> three publishability flags as `true`, and zero pressure-forced checkpoints.
> The nested benchmark module does not carry Go `vcs.revision` or
> `vcs.modified` build fields, so binary identity is pinned instead by exact
> SHA-256: `b6e2b719d962787e42d0335d19a7d93c900c7fa162116c94a5a3c1d2115f8a18`
> for `mixed` and
> `856da49919a3cc65ac18944cfeb859c2bc91f0fe89c89f861f484adb95706bee`
> for `mixedsuite`.

The local publication artifact directory was
`/private/tmp/vibedb-publish-8c11423`. It is an ephemeral host path that is not
checked into Git or available as a repository download. Its TSV manifest is:

| TSV basename | SHA-256 |
| --- | --- |
| `mixed-single-ycsb-a-c1.tsv` | `c1d412e5f58eb01204d50e8e884c9f52689be1d6458af4b3b46aea64e35b9418` |
| `mixed-single-ycsb-b-c1.tsv` | `a67dad7aa59b093c1ca4dfd374445883ce785f2ff1dac051adc2ff00c8006ca7` |
| `mixed-single-ycsb-f-c1.tsv` | `8babf0e1695d9fbfdb4115e8af5149a46ea332e9cad2ca2e532e91c208f4ed72` |
| `mixed-single-churn-c1.tsv` | `b828beb463bee9d6a87609f66fcc55d91da80dfe5013d2130ba2e52220aafa4e` |
| `mixed-single-scan-c1.tsv` | `be8e084d824c07a8bf0b4fe88cb8520bd91c45a8790646aae5295285b631baea` |
| `mixed-concurrent-write-c1.tsv` | `f4b7609d902768b5e4391595410633a4df93967b3eb98b06188adecb52713caf` |
| `mixed-concurrent-write-c8.tsv` | `7b0a342bef6a75d54cf40fd66c24ee7ff08bb843c2b10f9a238e8cc8594b3f93` |
| `mixed-concurrent-write-c32.tsv` | `f5cb98fe01bddfddf8082af089176f6c3649ba7f8436107c6b3f543f80e468e1` |
| `mixed-concurrent-churn-c1.tsv` | `04ab8d25419be83addcdf8d67f2f88bcab3adb7ae81704bd8a03454cc2eb153c` |
| `mixed-concurrent-churn-c8.tsv` | `99391c09758ce12dae0377a6de2714dbc8819fe8497f0335fcb2b9a5d4c64805` |
| `mixed-concurrent-churn-c32.tsv` | `89013a6f9fa8075893118a1c2f3b6b593c4d93088455f80f628029af3c9277d5` |

The final clean micro-gate rerun retained these raw logs in the same local
artifact directory; no `space.txt` footprint rerun is claimed:

| raw micro-gate log | SHA-256 |
| --- | --- |
| `leaf-fold.txt` | `e3ca349ad81b9db70a6682633d302863e28e45095b94479107083526b9f7750f` |
| `scan.txt` | `0c7e94b2f7425ec0f896bf61ace2dfd47d7a02b1853b178ebb4748d264b5b802` |

This clean snapshot reverses the old compact-default regression headline.
VibeDB is **1.26–1.47× Badger** on four of the five single-client mixed lanes;
scan mix remains behind at **0.93×**. The checkpoint-tail regression is much
smaller but not eliminated: VibeDB checkpoint p99 is 0.816–1.372 ms across the
five lanes, still 8.74–13.45× Badger. The 100% replacement lane scales modestly
and remains ahead at 32 clients, while mixed churn still loses throughput badly
at 8 and 32 clients. The local CPU/scan gates were also rerun cleanly at the
same commit. The older bulk-footprint and sustained-churn-disk sections were not
rerun and retain explicit historical provenance below.

## Provenance and protocol

- Machine: Apple M4 Max (16 logical CPUs, 64 GB), macOS / Darwin 25.3.0, APFS,
  Go 1.26.0.
- Competitors: bbolt 1.5.0, Badger 4.9.5, Pebble 1.1.5, modernc SQLite
  1.54.0.
- Corpus: 10,000 documents for throughput and 100,000 for churn-disk and
  footprint; low cardinality unless a table says otherwise.
- Throughput shape: 2,000 warmup operations, 20,000 measured operations,
  buffered-visible durability, and a CP64 acknowledged-mutation threshold.
- Each throughput cell is the median of ten recorded repetitions. Engines run
  in isolated child processes, with deterministic Latin-square ordering and
  one unrecorded conditioning pass per engine.
- Footprint cells are one isolated run and are **apparent / allocated MiB**
  derived from the harness's exact byte columns.
- Every suite records the commit, dirty bit, binary hash, effective options,
  corpus shape, engine order, repetitions, and pressure-forced checkpoints.
  All eleven suites report `maximum-forced-checkpoints=0`,
  `publishable-suite=true`, `publishable-checkpoint-cadence=true`, and
  `publishable-repetition-count=true`, as well as the clean Git-root evidence
  and identical binary hashes listed above.
- Correctness is checked outside timed intervals: corpus shape, operation
  trace, final key/value state, and complete consumption of returned scan
  bytes.

## Durability lanes

Results are never averaged across durability promises. The current snapshot
publishes the cross-engine **buffered-visible** lane: writes become visible
immediately and become durable at each scheduled checkpoint.

The harness also distinguishes **ordinary-sync** from **power-safe**. On this
Darwin host only vibedb (`DurabilitySync`) and SQLite can make the strongest
power-loss promise natively; bbolt, Badger, and Pebble stop at ordinary fsync
and fail closed when asked to enter the power-safe lane.

## Single-client mixed workloads

Total operations per second, median of ten. The leading engine per row is bold.

| workload | vibedb | Badger | SQLite | Pebble | bbolt | vibedb / Badger |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| YCSB-A: 50% read, 50% update | **374,293** | 296,759 | 98,928.5 | 25,975 | 21,623 | **1.26×** |
| YCSB-B: 95% read, 5% update | **1,396,465.5** | 1,024,235.5 | 301,115.5 | 222,450 | 208,415 | **1.36×** |
| YCSB-F: 50% read, 50% read-modify-write | **387,114** | 264,666 | 87,362 | 27,068.5 | 23,272.5 | **1.46×** |
| Churn: 70% read, 25% update, 5% delete+restore | **563,800** | 383,864 | 127,748.5 | 35,623.5 | 30,745 | **1.47×** |
| Scan mix: 79.9% read, 15% update, 5% delete+restore, 0.1% full scan | 255,356 | **274,536.5** | 113,481 | 46,365 | 40,163 | 0.93× |

VibeDB leads Badger by 26–47% on YCSB-A, YCSB-B, YCSB-F, and churn. Scan mix
is the exception: its full ordered scan remains slower, leaving aggregate
throughput 7% behind Badger even though its point mutations are fast.

### vibedb operation latency

Median of the ten run-level percentiles, microseconds:

| workload | operation p50 / p95 / p99 | checkpoint p50 / p95 / p99 |
| --- | ---: | ---: |
| YCSB-A | read 0.167 / 1.0205 / 1.1665; update 2.3335 / 3.8335 / 31.792 | 30.167 / 169.604 / 1,226.625 |
| YCSB-B | read 0.125 / 0.8955 / 1.0205; update 2.458 / 3.896 / 62.313 | 193.417 / 230.771 / 1,099.2915 |
| YCSB-F | read 0.167 / 1.0205 / 1.166; RMW 2.583 / 4.5 / 33.437 | 31.271 / 283.271 / 815.6035 |
| Churn | read 0.125 / 1 / 1.1455; update 2.416 / 4.3125 / 31.187; delete+restore 2.25 / 4.75 / 39.687 | 29.5 / 273.458 / 1,012.8335 |
| Scan mix | read 0.208 / 1 / 1.166; update 2.417 / 4.3125 / 33.5415; delete+restore 2.375 / 5 / 46.375; ordered scan 2,430.833 / 2,579.583 / 2,691.667 | 33.4165 / 266.125 / 1,371.521 |

Point-read p50 remains 0.125–0.208 µs and update p50 is 2.3335–2.458 µs.
Checkpoint p99 is now 0.816–1.372 ms, more than an order of magnitude below the
stale snapshot, but Badger is still materially better at that tail: its five
checkpoint-p99 medians are 81.8545–115.771 µs, making VibeDB 8.74–13.45× slower
there. In scan mix,
VibeDB's ordered-scan p50/p99 is 2.431/2.692 ms versus Badger's 1.541/1.755 ms.

## Concurrent replacement and churn

Total operations per second, median of ten. `write` is 100% existing-key
replacement; `churn` has the same 70/25/5 read/update/delete+restore mix as
above. The leading engine per row is bold.

| workload | clients | vibedb | Badger | SQLite | Pebble | bbolt | vibedb / Badger |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| write | 1 | **237,524** | 165,319.5 | 56,455 | 13,067 | 11,157 | **1.44×** |
| write | 8 | **257,265.5** | 239,677 | 50,095 | 13,356 | 10,993 | **1.07×** |
| write | 32 | **269,722** | 252,644 | 46,708 | 13,332.5 | 10,800 | **1.07×** |
| churn | 1 | **542,512.5** | 370,904.5 | 129,581 | 35,827.5 | 31,679 | **1.46×** |
| churn | 8 | 201,188 | **535,117.5** | 101,348.5 | 35,982 | 23,910.5 | 0.38× |
| churn | 32 | 127,940 | **522,805.5** | 103,350 | 36,047 | 17,279.5 | 0.24× |

Replacement throughput rises 8.3% at eight clients and 13.6% at 32 clients
relative to one client; it stays 1.07× Badger at both larger counts. Mixed
churn exposes a separate unresolved bottleneck: VibeDB falls to 37.1% and 23.6%
of its one-client rate at 8 and 32 clients, while Badger scales up. The result is
0.38× and 0.24× Badger. The replacement recovery must not be generalized to
claim that concurrent delete/restore scaling is solved.

### Write-lane resource envelope

These are medians from the 100% replacement, one-client TSV after the measured
10,000-document workload. Disk cells are **apparent / allocated filesystem
MiB**; `heap`, runtime-resident memory, and peak RSS have different scopes and
must not be combined. This is not a replacement for the 100,000-document bulk
or sustained-churn space tables below.

| engine | disk MiB, apparent / allocated | heap / runtime MiB | peak RSS MiB |
| --- | ---: | ---: | ---: |
| VibeDB | 3.2 / 2.9 | 45.6 / 55.7 | 51.9 |
| Badger | 257.0 / 9.1 | 86.1 / 96.55 | 90.9 |
| SQLite | 2.8 / 2.8 | 2.5 / 11.25 | 41.8 |
| Pebble | 3.5 / 3.6 | 2.8 / 11.75 | 45.0 |
| bbolt | 16.1 / 16.1 | 2.5 / 11.8 | 43.55 |

VibeDB's apparent image is 80.3× smaller than Badger's in this lane and its
allocated blocks are 3.14× smaller. It is 0.4 MiB larger than SQLite by
apparent size while using 0.1 MiB more allocated blocks.

## Disk under sustained churn

> **Historical; not re-run in the 2026-08-14 pass.** The tables in this section
> retain their 2026-08-01 provenance from clean commit
> `7fe67691dd889a34951682d2522661c7741d8720`, which predates compact primary
> storage becoming the default. The `cmd/churndisk` matrix (five engines × two
> cardinalities × two profiles, each a sustained 200,000-mutation run) exceeds
> the wall-time budget of this pass and was deferred. Because the vibedb rows
> below reflect the pre-compact-default online-churn representation, they are
> **stale relative to the current default** and are kept only to avoid deleting
> published data; treat every vibedb cell here as pending a compact-default
> re-measurement. Partial VibeDB-only churn-disk probes were run at
> `c1dea2b` (saved in the old raw logs; not published as table cells because the
> cross-engine matrix was not re-run). Those historical diagnostics measured a
> compact-default online image well below these pre-compact rows — for example
> high-cardinality
> intrinsic online 22.376 / 18.168 MiB (23,462,912 / 19,050,496 bytes) versus the
> 54.841 / 36.070 shown here, and low-cardinality online 5.497 / 5.426 MiB versus
> 22.075 / 16.020 — so the retained vibedb numbers are conservative: they
> overstate its space use and understate its compactness; they are not current
> cross-engine evidence.

`cmd/churndisk` keeps 100,000 documents live through 200,000 acknowledged
state changes. Eighty percent of random choices are one-change replacements;
the rest are indivisible delete+reinsert pairs. Checkpoint and sampling
cadences are mutation thresholds, so a pair may cross one by a single change.

Cells are **apparent / allocated MiB**. `online` is measured immediately after
the workload and all requested CP64 checkpoints. `offline` is a separate
out-of-place `durable.Repack` result and is not required to bound online growth.

### Intrinsic representation (2026-08-01 / 7fe6769, pre-compact-default — not re-run)

Optional SST compression is disabled.

| engine | low online | low offline | high online | high offline |
| --- | ---: | ---: | ---: | ---: |
| vibedb | **22.075 / 16.020** | **9.001 / 9.520** | 54.841 / 36.070 | **18.767 / 19.520** |
| SQLite | 28.109 / 28.109 | 26.234 / 26.234 | **28.109 / 28.109** | 26.234 / 26.234 |
| bbolt | 45.750 / 45.750 | 45.750 / 45.750 | 45.750 / 45.750 | 45.750 / 45.750 |
| Pebble (median of 3) | 93.864 / 95.141 | 103.447 / 99.859 | 93.863 / 95.320 | 103.445 / 95.703 |
| Badger | 314.769 / 72.641 | 314.769 / 72.641 | 314.770 / 72.645 | 314.770 / 72.645 |

### Production-compressed LSM control (2026-08-01 / 7fe6769, pre-compact-default — not re-run)

Pebble and Badger use the pinned releases' Snappy SST configuration. vibedb,
bbolt, and SQLite have no corresponding profile switch, so their rows are the
same measurement as above.

| engine | low online | low offline | high online | high offline |
| --- | ---: | ---: | ---: | ---: |
| vibedb | **22.075 / 16.020** | **9.001 / 9.520** | 54.841 / 36.070 | **18.767 / 19.520** |
| SQLite | 28.109 / 28.109 | 26.234 / 26.234 | **28.109 / 28.109** | 26.234 / 26.234 |
| bbolt | 45.750 / 45.750 | 45.750 / 45.750 | 45.750 / 45.750 | 45.750 / 45.750 |
| Pebble, Snappy (median of 3) | 79.133 / 81.129 | 58.445 / 54.203 | 84.244 / 86.055 | 76.311 / 70.730 |
| Badger, Snappy | 273.948 / 31.820 | 273.948 / 31.820 | 279.414 / 37.285 | 279.414 / 37.285 |

These pre-compact online rows are retained for continuity only. They do not
support a present-tense space ranking for the current engine; a clean
compact-default churn-disk matrix remains a separate publication requirement.

## Bulk footprint

> **Retained exact-byte snapshot; not re-run in the 2026-08-14 pass.** These
> cells retain their 2026-08-03 `c1dea2b` provenance and the disclosed-dirty
> status of that older run. The subsequent performance work did not change the
> storage format and byte-parity tests remain green, but that is not presented
> as a new footprint measurement.

The low- and high-cardinality corpora have identical shape and length:
24,881,153 JSON bytes (23.729 MiB) plus 1,200,000 key bytes (1.144 MiB), or
26,081,153 key-inclusive logical bytes (24.873 MiB), for 100,000 documents.
Their JSON-only gzip-9 sizes are 1.837 MiB and 8.041 MiB, respectively (measured
1,925,945 and 8,431,529 bytes by `footprint -corpus-stats`, the harness's
JSON-only entropy control; both corpora hold 24,881,153 identical JSON bytes and
differ only in value entropy). Cells below are one isolated run and are
**apparent / allocated MiB**. VibeDB's ordinary buffered unified-bulk images are
immutable here, so their lazy recovery journal does not yet exist and these rows
are the complete footprint; the point-put rows have mutated and include the
sibling. Only the vibedb rows changed since 2026-08-01: the unified-bulk image
is measured at 0.973 / 6.609 MiB (resolving the earlier hand-edit), and the
point-put build fell from 16.341 / 28.606 to 4.122 / 11.821 MiB apparent.

### Intrinsic representation

| engine | low cardinality | high cardinality |
| --- | ---: | ---: |
| vibedb unified bulk, immutable | **0.973 / 0.973** | **6.609 / 6.609** |
| vibedb point-put build | 4.122 / 4.203 | 11.821 / 10.133 |
| SQLite | 28.109 / 28.109 | 28.109 / 28.109 |
| bbolt | 45.750 / 29.734 | 45.750 / 29.734 |
| Pebble | 50.611 / 50.664 | 50.611 / 50.664 |
| Badger | 257.000 / 26.621 | 257.000 / 26.621 |

### Production-compressed LSM control

| engine | low cardinality | high cardinality |
| --- | ---: | ---: |
| vibedb unified bulk, immutable | **0.973 / 0.973** | **6.609 / 6.609** |
| vibedb point-put build | 4.122 / 4.203 | 11.821 / 11.008 |
| SQLite | 28.109 / 28.109 | 28.109 / 28.109 |
| bbolt | 45.750 / 29.734 | 45.750 / 29.734 |
| Pebble, Snappy | 33.978 / 34.000 | 40.993 / 41.027 |
| Badger, Snappy configured | 257.000 / 26.621 | 257.000 / 26.621 |

Badger's bulk corpus is still resident in its configured mutable table, so the
bulk row has no meaningful compressed SST payload; the churn table is the
materialized production-compressed comparison. Vibedb's compactness is
structural: repeated canonical JSON skeletons and scalar spellings are shared
within a leaf, while uncommon shapes stay verbatim. It is not a generic gzip
comparison and does not require a second storage mode. The high-cardinality
point-put apparent image (11.821 MiB) exceeds the immutable unified-bulk image
(6.609 MiB) because it has mutated and carries its bounded sibling journal.

## Current CPU and scan gates

These five-sample, one-second Go microbenchmark medians were run from a fresh
detached worktree at exact commit `8c11423`, with an empty Git status, on the
same Apple M4 Max. They are local regression gates, not cross-engine database
results. Every row reports 0 B and 0 allocations.

| gate | median | allocation |
| --- | ---: | ---: |
| stable native checkpoint leaf patch | **1.852 µs** | 0 B, 0 allocs |
| generic render/plan/encode of that leaf | 255.438 µs | 0 B, 0 allocs |
| ordered scan, 100k three-scalar documents, first byte | 75.63 ns/document | 0 B, 0 allocs |
| ordered scan, same corpus, all bytes | 89.08 ns/document | 0 B, 0 allocs |
| competitive canonical render, low cardinality | 160.7 ns/document | 0 B, 0 allocs |
| competitive canonical render, high cardinality | 340.9 ns/document | 0 B, 0 allocs |
| competitive all-byte scan, low cardinality | 235.2 ns/document | 0 B, 0 allocs |
| competitive all-byte scan, high cardinality | 415.5 ns/document | 0 B, 0 allocs |
| masked scan, one occupied row per live posting tile | 579.0 ns/selected document | 0 B, 0 allocs |

The native patch is about 137.9× faster than generic render/plan/encode. The
competitive all-byte rows consume 248.8 returned bytes per document, equivalent
to about 1.058 GB/s at low cardinality and 599 MB/s at high. The masked row
records 0.2502 page pins per selected document and zero cache misses. The
end-to-end scan-mix result above remains authoritative for the database ranking.

## Publishing rules

A replacement snapshot must:

1. name the exact commit, dirty bit, machine, OS, Go, and competitor versions;
2. run timed engines in isolated processes and publish repeated-sample medians;
3. validate equivalent corpus, trace, final state, and returned scan bytes;
4. match durability, checkpoint cadence, workload shape, and client count;
5. include requested checkpoint stalls in elapsed time;
6. report apparent and allocated bytes for both cardinalities;
7. label online foreground state separately from every offline maintenance
   hook;
8. name the storage profile and effective compression for every disk row; and
9. keep database results, microbenchmarks, and projections separate.

The 2026-08-14 timing refresh satisfies rules 1–5 and 9 for the sections it
replaces. All eleven TSVs carry the same clean Git-root evidence and exact
binary hashes. The nested benchmark module does not expose Go VCS build fields;
their absence is not silently converted into a clean stamp. This pass is not a
replacement footprint or churn-disk snapshot under rules 6–8; those older
sections are explicitly excluded from the refresh and retain their own
provenance. The complete harness contract is in the
[competitive benchmark guide](README.md).

## Reproduction

From `bench/competitive`, with the repository checked out at
`8c1142322c96cfe8f99d04cd04683a5d827e6710`. Build and write artifacts outside
the worktree. A faithful refresh requires all eleven complete five-engine TSVs;
a diagnostic A/B pair or one workload cannot replace this snapshot.

```sh
go test . \
  -run '^(TestFullEquivalence|TestFullEquivalenceIndexedDurable|TestCorpusVariantsAreShapeMatched)$' \
  -count=1 -timeout=60m
go test ./cmd/... -count=1 -timeout=60m

test -z "$(git status --porcelain=v1 --untracked-files=normal)"
publication_commit=$(git rev-parse HEAD)
publication_dir=$(mktemp -d /tmp/vibedb-publish.XXXXXX)
publication_engines=vibedb,bbolt,badger,pebble,sqlite

go build -trimpath -o "$publication_dir/mixed" ./cmd/mixed
go build -trimpath -o "$publication_dir/mixedsuite" ./cmd/mixedsuite

for workload in ycsb-a ycsb-b ycsb-f churn scan; do
  "$publication_dir/mixedsuite" -mixed-bin="$publication_dir/mixed" \
    -engines="$publication_engines" \
    -workload="$workload" -durability=buffered-visible \
    -clients=1 -checkpoint-mutations=64 -repetitions=10 \
    -output="$publication_dir/mixed-single-${workload}-c1.tsv"
done

for workload in write churn; do
  for clients in 1 8 32; do
    "$publication_dir/mixedsuite" -mixed-bin="$publication_dir/mixed" \
      -engines="$publication_engines" \
      -workload="$workload" -durability=buffered-visible \
      -clients="$clients" -checkpoint-mutations=64 -repetitions=10 \
      -output="$publication_dir/mixed-concurrent-${workload}-c${clients}.tsv"
  done
done

go build -trimpath -o /tmp/vibedb-churndisk ./cmd/churndisk
go build -trimpath -o /tmp/vibedb-footprint ./cmd/footprint

/tmp/vibedb-churndisk -engine=<engine> -cardinality=<low|high> \
  -storage-profile=<intrinsic|production>
/tmp/vibedb-footprint -engine=<engine> -cardinality=<low|high> \
  -storage-profile=<intrinsic|production>
# vibedb point-put build image adds -putloop.
```

Run timing lanes serially, without competing benchmark engines. The exact
micro-gate commands are listed in the [benchmark guide](README.md).
