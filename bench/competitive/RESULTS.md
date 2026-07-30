# Competitive results

> **Current published snapshot.** The buffered-visible tables were measured on
> 2026-07-31 host-local time from clean engine commit
> `0a593f513663b66e631853d1352edf1a74b7a8a6`. Disk and footprint were
> regenerated from clean benchmark-verification commit
> `3fbcd8a8c79f076adce682a121131b1fbc11fe54`, which changes the harness and
> tests but not the measured engine.

Every number in this file comes from `cmd/mixedsuite`, `cmd/churndisk`, and
`cmd/footprint` runs on the benchmark host, with command templates in
[Reproduction](#reproduction). Mixed-workload numbers are medians of 10
recorded repetitions in isolated child processes with deterministic
Latin-square engine ordering and one discarded conditioning pass per engine.
Footprint and non-Pebble churn cells are one isolated invocation per
engine/cardinality/profile, not timing medians. Pebble churn cells are medians
of three clean runs because compaction scheduling moves its post-maintenance
image by a few MiB. Historical tables remain in Git history; they are never
mixed with current numbers.

- Machine: Apple M4 Max (16 cores, 64 GB), macOS 26.3.1 / Darwin 25.3.0,
  APFS. Go 1.26.0.
- Competitors: bbolt 1.5.0, Badger 4.9.5, Pebble 1.1.5, modernc SQLite 1.54.0.
- Within each current mixed table, all engine rows use the same clean commit,
  binaries, corpus, and run protocol. Historical and disk tables name their
  own provenance.
- Corpus: 10,000 documents (mixed workloads) / 100,000 (churn-disk,
  footprint), ~250 B JSON each; low cardinality unless stated.
- Mixed workload: 2,000 warmup operations, 20,000 measured operations,
  one client.
- Checkpoint cadence: CP64 acknowledged-mutation threshold, all engines.
- Five mixed suites recorded 12.6–14.0 GiB free on the APFS Data volume. Their
  raw TSVs record filesystem state, binary hashes, Latin order, and every
  repetition.

## Durability lanes

The comparison never averages across guarantee classes. An engine appears in
a lane only when it can natively make that lane's promise; requesting an
unsupported lane fails closed.

- **buffered-visible** — mutations visible immediately; durability at each
  scheduled checkpoint. All five engines.
- **ordinary-sync** — every mutation durably acknowledged at ordinary
  fsync/msync class before returning.
- **power-safe** — survives power loss, not just process death: the
  strongest native fence (F_FULLFSYNC on Darwin). Only vibedb
  (`DurabilitySync`) and SQLite can make this promise natively on this
  platform; bbolt/Badger/Pebble stop at plain fsync and are excluded by
  fail-closed design. Pebble is NOT power-safe on Darwin.

## Mixed workloads, buffered-visible (total ops/s, median of 10)

| workload | vibedb | bbolt | Pebble | SQLite | Badger |
|---|---:|---:|---:|---:|---:|
| ycsb-a | 278,553 | 20,904 | 23,122 | 114,072 | **313,682** |
| ycsb-b | **1,697,367** | 211,765 | 227,437 | 326,279 | 1,092,690 |
| ycsb-f | 269,938 | 20,343 | 22,122 | 100,899 | **289,320** |
| churn | **445,005** | 27,458 | 31,725 | 147,831 | 401,636 |
| scan-mix | 276,420 | 37,924 | 41,321 | 131,560 | **309,899** |

vibedb is 2.1-5.2x SQLite. It leads the table on YCSB-B by 55% and churn
by 10.8%; it trails Badger by 11.2%, 6.7%, and 10.8% on YCSB-A, YCSB-F,
and scan-mix, respectively. Relative to the previous clean vibedb publication,
the five medians are 1.11–1.82× as high (11.4–82.3% higher); that is descriptive
commit-to-commit progress, not a causal A/B because the filesystem state and
competitor rows also moved.

The vibedb median point read is 0.333-0.334 µs, update is 0.916-0.958 µs,
delete+restore is 1.25 µs, and a full 10k-document ordered scan inside the
scan mix is 2.15 ms. Checkpoint p50 is 28.2-34.3 µs. Median checkpoint p99
ranges from 38 µs on read-heavy YCSB-B to 1.03 ms in scan-mix; every run has
zero pressure-forced checkpoints.

## Historical ordinary-sync snapshot (clean `d714d63`, median of 10)

This lane has not been rerun on `0a593f5`; it is retained for durability
provenance and must not be mixed into the current buffered-visible ranking.

| workload | vibedb | bbolt | Pebble | SQLite | Badger |
|---|---|---|---|---|---|
| ycsb-a | 10,138 | 210 | 440 | 28,790 | **58,358** |
| ycsb-b | 177,810 | 2,336 | 4,678 | 181,062 | **410,408** |
| ycsb-f | 9,860 | 214 | 427 | 34,737 | **59,638** |
| churn | 18,016 | 298 | 654 | 51,866 | **81,976** |
| scan-mix | 18,380 | 402 | 863 | 62,592 | **99,676** |

Badger leads this lane. Vibedb is close to SQLite on read-heavy ycsb-b, but
trails it by 2.8-3.5x on the other mixes. bbolt and Pebble remain much slower
when every mutation pays their synchronous commit path.

## Historical power-safe snapshot (clean `d714d63`, median of 10)

This lane likewise predates the current buffered-visible and unified-format
publication.

| workload | vibedb | SQLite |
|---|---|---|
| ycsb-a | **394** | 382 |
| ycsb-b | **4,175** | 3,728 |
| ycsb-f | **436** | 392 |
| churn | **607** | 518 |
| scan-mix | **829** | 765 |

Both engines sit on the ~4 ms F_FULLFSYNC device floor per fenced mutation.
The `d714d63` single-fence vibedb path leads SQLite by 3-17% across all five
workloads; vibedb's measured mutation median is 4.03-4.12 ms.

## Disk under sustained churn (200k mutations, fixed 100k-doc live set)

`cmd/churndisk`: 200,000 acknowledged state changes over a fixed live set.
Eighty percent of random choices are one-change replacements; the rest are
indivisible two-change delete+reinsert pairs. Checkpoint and sample cadences are
thresholds, so a pair may cross one by a single change. The harness verifies
every final key and byte before invoking each engine's maintenance hook. Cells
are **apparent / allocated MiB**, using the sample counts stated above.

### Intrinsic representation (optional SST compression disabled)

| engine | low pre-maintenance | low after maintenance | high pre-maintenance | high after maintenance |
|---|---:|---:|---:|---:|
| vibedb | **15.48 / 16.02** | **7.50 / 8.02** | 37.00 / 37.02 | **17.27 / 18.02** |
| SQLite | 28.11 / 28.11 | 26.23 / 26.23 | **28.11 / 28.11** | 26.23 / 26.23 |
| bbolt | 45.75 / 45.75 | 45.75 / 45.75 | 45.75 / 45.75 | 45.75 / 45.75 |
| Pebble (median of 3) | 93.86 / 94.96 | 105.46 / 100.21 | 93.86 / 95.28 | 105.46 / 100.49 |
| Badger | 314.77 / 72.66 | 314.77 / 72.66 | 314.77 / 72.66 | 314.77 / 72.66 |

vibedb's sizes plateau early and remain flat through 200,000 acknowledged
state changes with zero forced checkpoints. Its gap between pre-maintenance and
the repacked image is reusable high-water capacity, not continuing growth. Its
maintenance floor is an offline out-of-place `durable.Repack`; the benchmark
removes the source pair only after a successful build. A production
crash-atomic pair cutover is explicitly outside this measurement.

### Production-compressed LSM control

Vibedb has no optional block-codec switch, so its row is the same intrinsic
measurement. Pebble and Badger explicitly enable the pinned versions' Snappy
SST compression.

| engine | low pre-maintenance | low after maintenance | high pre-maintenance | high after maintenance |
|---|---:|---:|---:|---:|
| vibedb (no-op profile) | **15.48 / 16.02** | **7.50 / 8.02** | **37.00 / 37.02** | **17.27 / 18.02** |
| Pebble (Snappy, median of 3) | 77.22 / 78.48 | 58.57 / 55.25 | 84.24 / 85.48 | 78.32 / 75.12 |
| Badger (Snappy) | 273.93 / 31.80 | 273.93 / 31.80 | 279.41 / 37.30 | 279.41 / 37.30 |

After each engine's maintenance hook, Snappy Pebble uses 7.81× as many
apparent bytes as vibedb on the low-cardinality corpus and 4.54× as many on
the high-cardinality control. Pebble's intrinsic hook grows its image in this
runs, so “maintenance floor” is reserved for rows where the hook actually
establishes one.

## Bulk footprint (100k docs, both corpus cardinalities)

The low-cardinality corpus is ~92% redundant; the high-cardinality variant
is shape- and length-identical with near-unique values. The pair isolates each
format's sensitivity to value entropy at fixed shape and length.

### Intrinsic representation

Cells include vibedb's paired 1 MiB-capacity recovery journal plus its 1 KiB
dual header and are **apparent / allocated MiB** from one isolated
run.

| engine | low card | high card |
|---|---:|---:|
| vibedb unified pair | **7.5 / 8.0** | **17.3 / 18.0** |
| SQLite | 28.1 / 28.1 | 28.1 / 28.1 |
| bbolt | 45.8 / 29.7 | 45.8 / 29.7 |
| Pebble | 50.6 / 50.7 | 50.6 / 50.7 |
| Badger | 257.0 / 26.6 | 257.0 / 26.6 |

### Production-compressed LSM control

| engine | low card | high card |
|---|---:|---:|
| vibedb unified pair (no-op profile) | **7.5 / 8.0** | **17.3 / 18.0** |
| Pebble (Snappy) | 34.0 / 34.0 | 41.0 / 41.0 |
| Badger (Snappy configured) | 257.0 / 26.6 | 257.0 / 26.6 |

Badger's 100k bulk corpus remains in its configured 64 MiB mutable table, so
this pre-close bulk row contains no meaningful compressed SST payload; the
churn table above is the materialized compressed comparison.

The shape-matched corpus is 23.73 MiB raw. Its gzip-9 size is 1.84 MiB at low
cardinality and 8.04 MiB at high cardinality. Vibedb is smallest in both
variants, including against optional Snappy compression.

The compactness is structural rather than a generic gzip claim: class-5 stores
each repeated canonical JSON skeleton once per leaf, dictionaries repeated
value spellings, and encodes each row as typed hole tokens inside a succinct
ordered envelope. Shapes that do not save bytes remain trivial rows, so the
format does not require a second selectable storage mode. The paired journal,
keys, page metadata, and update headroom explain why the database is larger
than gzip-9 while still smaller than every compared database image.

## Unified scan development gates

These class-5 microbenchmarks were measured on the Apple M4 Max during the
2026-07-31 host-local unification work. They are kept separate from the isolated
cross-engine workload tables above.

| gate | result |
|---|---:|
| ordered scan, 100k three-scalar documents | **24.58 ns/document**, 0 allocs |
| competitive ~250 B scan, low / high cardinality | **88.07 / 89.48 ns/document**, 0 allocs |
| masked scan, 1 / 4 / 16 selected rows per leaf | **163 ns / 443–448 ns / 1.47 µs** |
| dense 153-row mask | **10.88–11.10 µs**, within 2% of sequential |

Sparse masks decode and render only selected stable slots; masks at or above
the measured 75% density crossover use the sequential class-5 drain.

The native plan-stable checkpoint patch is **2.12–2.13 µs**, zero allocations,
versus **240–242 µs** for a complete render/replan/encode. In the current
five-engine CP64 workloads, whole-checkpoint p50 is **28.2–34.3 µs**. The
ordinary filesystem lane now keeps the same strength when recycling its
journal header instead of accidentally upgrading a scheduled physical drain
to Darwin `F_FULLFSYNC`; power-safe mode retains `F_FULLFSYNC`. Median
checkpoint p99 is **38 µs–1.03 ms**, depending on workload, and every current
run reports zero forced persistence checkpoints.

## Reproduction

```sh
cd bench/competitive
go test -run 'TestFullEquivalence|TestCorpusVariantsAreShapeMatched' \
  -count=1 -timeout=60m .
go build -o /tmp/vibedb-mixed ./cmd/mixed
go build -o /tmp/vibedb-mixedsuite ./cmd/mixedsuite

# current five-engine publication lane (template):
/tmp/vibedb-mixedsuite -mixed-bin=/tmp/vibedb-mixed \
  -workload=<ycsb-a|ycsb-b|ycsb-f|churn|scan> \
  -durability=buffered-visible \
  -checkpoint-mutations=64 -repetitions=10 -output=<file>

# Historical ordinary-sync and power-safe commands remain available, but must
# be rerun as complete isolated suites before those tables are advanced.
/tmp/vibedb-mixedsuite -mixed-bin=/tmp/vibedb-mixed \
  -engines=vibejson-durable,sqlite \
  -workload=<ycsb-a|ycsb-b|ycsb-f|churn|scan> \
  -durability=power-safe -checkpoint-mutations=64 \
  -repetitions=10 -output=<file>

go build -o /tmp/vibedb-churndisk ./cmd/churndisk
go build -o /tmp/vibedb-footprint ./cmd/footprint

/tmp/vibedb-churndisk -engine=<engine> -cardinality=<low|high> \
  -storage-profile=<intrinsic|production>
/tmp/vibedb-footprint -engine=<engine> -cardinality=<low|high> \
  -storage-profile=<intrinsic|production>
```
