# Competitive results

> **Current published snapshot.** These tables were measured on 2026-07-30
> from clean commit `d714d63e1c48fc7c8e3021cf27675712d08a04fa`.

Every number in this file comes from `cmd/mixedsuite`, `cmd/churndisk`, and
`cmd/footprint` runs on a quiet machine, with the exact commands in
[Reproduction](#reproduction). Mixed-workload numbers are medians of 10
recorded repetitions in isolated child processes with deterministic
Latin-square engine ordering and one discarded conditioning pass per engine.
Historical tables remain in Git history; they are never mixed with current
numbers.

- Machine: Apple M4 Max (16 cores, 64 GB), macOS 26.3.1 / Darwin 25.3.0,
  APFS. Go 1.26.0.
- Competitors: bbolt 1.5.0, Badger 4.9.5, Pebble 1.1.5, modernc SQLite 1.54.0.
- All engine rows: the same clean commit, binaries, corpus, and run.
- Corpus: 10,000 documents (mixed workloads) / 100,000 (churn-disk,
  footprint), ~250 B JSON each; low cardinality unless stated.
- Mixed workload: 2,000 warmup operations, 20,000 measured operations,
  one client.
- Checkpoint cadence: every 64 acknowledged mutations, all engines.

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
|---|---|---|---|---|---|
| ycsb-a | 182,590 | 21,323 | 24,512 | 94,134 | **264,284** |
| ycsb-b | **1,105,691** | 207,516 | 238,010 | 275,950 | 865,332 |
| ycsb-f | 171,480 | 22,234 | 24,658 | 83,182 | **236,196** |
| churn | 244,114 | 31,252 | 37,072 | 118,515 | **328,884** |
| scan-mix | **248,194** | 42,120 | 48,068 | 107,244 | 230,531 |

vibedb is 1.9-4.0x SQLite, wins ycsb-b and scan-mix overall, and trails
Badger on the write-heavier ycsb-a, ycsb-f, and churn mixes. Its median point
read is 0.25-0.42 µs, update is 4.8-4.9 µs, delete+restore is 9.8 µs, and a
full 10k-document ordered scan is 1.01 ms.

## Mixed workloads, ordinary-sync (total ops/s, median of 10)

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

## Mixed workloads, power-safe (total ops/s, median of 10)

| workload | vibedb | SQLite |
|---|---|---|
| ycsb-a | **394** | 382 |
| ycsb-b | **4,175** | 3,728 |
| ycsb-f | **436** | 392 |
| churn | **607** | 518 |
| scan-mix | **829** | 765 |

Both engines sit on the ~4 ms F_FULLFSYNC device floor per fenced mutation.
The current single-fence vibedb path leads SQLite by 3-17% across all five
workloads; vibedb's measured mutation median is 4.03-4.12 ms.

## Disk under sustained churn (200k mutations, fixed 100k-doc live set)

`cmd/churndisk`: steady-state bytes while 80% of mutations replace
uniformly random keys, checkpoint every 64, then each engine's own
maintenance floor. Cells are **apparent / allocated MiB**.

| engine | steady state | after maintenance floor | maintenance required |
|---|---|---|---|
| vibedb | **35.1 / 35.4**, flat | 35.1 / 35.4 | flush; representation unchanged |
| SQLite | 28.1 / 28.1 | **26.2 / 26.2** | WAL truncate + VACUUM |
| bbolt | 45.8 / 45.8, flat | 45.8 / 45.8 | none available |
| Pebble | 93.9 / 96.1 | 103.4 / 98.1 | full-range compaction |
| Badger | 314.8 / 72.6 | 314.8 / 72.6 | flatten + value-log GC |

## Bulk footprint (100k docs, both corpus cardinalities)

The low-cardinality corpus is ~92% redundant; the high-cardinality variant
is shape- and length-identical with near-unique values, so the difference
between an engine's two columns is exactly how much of its compactness came
from the corpus rather than the format.

Cells are **apparent / allocated MiB**.

| engine | low card | high card |
|---|---|---|
| vibedb compact primary graph | **7.8 / 8.0** | **17.6 / 18.0** |
| vibedb verbatim primary graph | 28.1 / 29.0 | 28.1 / 28.1 |
| SQLite | 28.1 / 28.1 | 28.1 / 28.1 |
| bbolt | 45.8 / 29.7 | 45.8 / 29.7 |
| Pebble | 50.6 / 50.7 | 50.6 / 50.7 |
| Badger | 257.0 / 26.6 | 257.0 / 26.6 |

The shape-matched corpus is 23.73 MiB raw. Its gzip-9 size is 1.84 MiB at low
cardinality and 8.04 MiB at high cardinality. Compact primary-graph storage is
smallest in both variants. Verbatim storage preserves the source JSON bytes
and stays at SQLite's apparent-size parity.

The auxiliary replay-through-`Put` footprint did not produce a row: inserting
100,000 new keys from an empty store reached the current primary macro-tablet
split limit in both cardinalities. Bulk load and the fixed-live-set churn run
completed; this limitation is recorded rather than conflated with either path.

## Reproduction

```sh
cd bench/competitive
go test -run 'TestFullEquivalence|TestCorpusVariantsAreShapeMatched' \
  -count=1 -timeout=60m .
go build -o /tmp/vibedb-mixed ./cmd/mixed
go build -o /tmp/vibedb-mixedsuite ./cmd/mixedsuite

# five-engine lanes:
/tmp/vibedb-mixedsuite -mixed-bin=/tmp/vibedb-mixed \
  -workload=<ycsb-a|ycsb-b|ycsb-f|churn|scan> \
  -durability=<buffered-visible|ordinary-sync> \
  -checkpoint-mutations=64 -repetitions=10 -output=<file>

# power-safe lane (only the two engines that can promise it):
/tmp/vibedb-mixedsuite -mixed-bin=/tmp/vibedb-mixed \
  -engines=vibejson-durable,sqlite \
  -workload=<ycsb-a|ycsb-b|ycsb-f|churn|scan> \
  -durability=power-safe -checkpoint-mutations=64 \
  -repetitions=10 -output=<file>

go build -o /tmp/vibedb-churndisk ./cmd/churndisk
go build -o /tmp/vibedb-footprint ./cmd/footprint

/tmp/vibedb-churndisk -engine=<engine> -cardinality=low
/tmp/vibedb-footprint -engine=<engine> -cardinality=<low|high>
/tmp/vibedb-footprint -engine=vibejson-durable \
  -compact -cardinality=<low|high>
```
