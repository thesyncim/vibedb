# Competitive results

> **Published snapshot, not current-head results.** These tables were measured
> on 2026-07-28 at the commits named below. Later work changed the primary
> graph, synchronous journal, compact leaves, and exact-index path. The numbers
> remain the latest complete cross-engine run until the suite is refreshed.

Every number in this file comes from `cmd/mixedsuite`, `cmd/churndisk`, and
`cmd/footprint` runs on a quiet machine, with the exact commands in
[Reproduction](#reproduction). Mixed-workload numbers are medians of 10
recorded repetitions in isolated child processes with deterministic
Latin-square engine ordering and one discarded conditioning pass per engine.
Historical tables remain in Git history; they are never mixed with current
numbers.

- Machine: Apple M4 Max, Darwin 25.3.0, APFS. Go 1.26.5.
- Competitors: bbolt 1.5.0, Badger 4.9.5, Pebble 1.1.5, modernc SQLite 1.54.0.
- Competitor rows: commit `8472066` (five-engine suite, 2026-07-28).
- vibedb rows: commit `b506f2c` — the ordered primary graph (routed
  splits/merges, capacity-relative structural gates, recovery journal),
  measured in a paired vibedb+SQLite run the same day. The paired SQLite
  medians replicated the five-engine run within ~7% on every workload,
  which bounds cross-run drift.
- Corpus: 10,000 documents (mixed workloads) / 100,000 (churn-disk,
  footprint), ~250 B JSON each; low cardinality unless stated.
- Checkpoint cadence: every 64 acknowledged mutations, all engines.

## Durability lanes

The comparison never averages across guarantee classes. An engine appears in
a lane only when it can natively make that lane's promise; requesting an
unsupported lane fails closed.

- **buffered-visible** — mutations visible immediately; durability at each
  scheduled checkpoint. All five engines.
- **ordinary-sync** — every mutation durably acknowledged at ordinary
  fsync/msync class before returning. bbolt/Badger/Pebble sync modes,
  SQLite's synchronous WAL commit, vibedb buffered-visible + recovery
  journal (one 512 B redo record appended and synced per mutation;
  acknowledgement counters assert the journal engages).
- **power-safe** — survives power loss, not just process death: the
  strongest native fence (F_FULLFSYNC on Darwin). Only vibedb
  (`DurabilitySync`) and SQLite can make this promise natively on this
  platform; bbolt/Badger/Pebble stop at plain fsync and are excluded by
  fail-closed design. Pebble is NOT power-safe on Darwin.

## Mixed workloads, buffered-visible (total ops/s, median of 10)

| workload | vibedb | bbolt | Pebble | SQLite | Badger |
|---|---|---|---|---|---|
| ycsb-a | **195,351** | 21,896 | 24,390 | 104,263 | 241,812 |
| ycsb-b | **1,010,312** | 205,009 | 239,836 | 280,248 | 893,563 |
| ycsb-f | **179,986** | 21,975 | 23,021 | 86,000 | 231,770 |
| churn | **265,137** | 30,577 | 33,041 | 127,557 | 316,547 |
| scan-mix | **265,362** | 40,617 | 42,071 | 112,967 | 229,686 |

vibedb leads every engine except Badger on every workload (1.9-3.6x SQLite,
8-45x bbolt/Pebble), beats Badger on ycsb-b and scan-mix, and trails it on
ycsb-a/f and churn. Badger's remaining wins are priced below: 257 MiB of
disk and ~86 MiB of heap for a corpus every other engine stores in 3-50 MiB.

vibedb in-workload latency medians (churn/scan): point read 0.33-0.38 µs
(the fastest of the five engines inside these workloads), update 4.7 µs,
delete+restore 9.7 µs, checkpoint 330-355 µs, full 10k-doc ordered scan
988 µs (SQLite: 2,496 µs).

## Mixed workloads, ordinary-sync (total ops/s, median of 10)

| workload | vibedb | bbolt | Pebble | SQLite | Badger |
|---|---|---|---|---|---|
| ycsb-a | 17,949 | 226 | 460 | 27,871 | 59,728 |
| ycsb-b | 176,264 | 2,247 | 4,506 | 202,783 | 391,289 |
| ycsb-f | 18,519 | 222 | 433 | 32,560 | 56,333 |
| churn | 17,860 | 311 | 635 | 49,881 | 82,157 |
| scan-mix | 21,176 | 439 | 913 | 62,369 | 94,931 |

**The measured vibedb build loses this lane to SQLite and Badger.** The journal
acknowledgement costs 32.5 µs per mutation (append + ordinary sync of the
dedicated journal file); SQLite's fsync-class WAL commit is cheaper per
operation on this platform, and Badger group-commits its value log. bbolt
and Pebble pay a full fsync-fenced commit per mutation and are 40-140x
behind everyone. An earlier draft of this table showed vibedb winning the
lane; those rows were measured with the journal silently disengaged (the
legacy chunk layout has no journal append path), were withdrawn, and the
engine now fails closed if a journal is requested on a layout that cannot
feed it. Single-fence journal acknowledgement landed after this measurement;
its current competitive standing is deliberately unclaimed until the complete
lane is rerun.

## Mixed workloads, power-safe (total ops/s, median of 10)

| workload | vibedb | SQLite |
|---|---|---|
| ycsb-a | 378 | 431 |
| ycsb-b | 3,663 | 3,904 |
| ycsb-f | 370 | 402 |
| churn | 533 | 589 |
| scan-mix | 757 | 828 |

Both engines sit on the ~4-5 ms F_FULLFSYNC device floor per fenced
mutation; the measured vibedb build trails by roughly 6-13% because its sync
lane paid two full fences per commit (update p50 5.01 ms) against SQLite's one
(4.09 ms). The journal's single-fence power-safe acknowledgement landed after
this table; an isolated store-level result does not replace the competitive
lane.

## Disk under sustained churn (200k mutations, fixed 100k-doc live set)

`cmd/churndisk`: steady-state allocated bytes while 80% of mutations replace
uniformly random keys, checkpoint every 64, then each engine's own
maintenance floor.

| engine | steady state | after maintenance floor | maintenance required |
|---|---|---|---|
| vibedb | **36.0 MiB, flat** | identical | none — the mutation representation is the steady state |
| SQLite | ~28 MiB | 26.2 MiB | wal_checkpoint(TRUNCATE) + full VACUUM |
| bbolt | 45.8 MiB, flat | identical | none available (no online compaction) |
| Pebble | 94-103 MiB, climbing | 96.3 MiB | manual full-range compaction |
| Badger | 315 MiB apparent | 72.7 MiB | Flatten + value-log GC to ErrNoRewrite |

vibedb also completes the 200k-mutation run 2.6x faster than the legacy
layout did (6.5 vs 17.3 minutes wall clock at identical settings).

## Bulk footprint (100k docs, both corpus cardinalities, MiB on disk)

The low-cardinality corpus is ~92% redundant; the high-cardinality variant
is shape- and length-identical with near-unique values, so the difference
between an engine's two columns is exactly how much of its compactness came
from the corpus rather than the format.

| engine | low card | high card |
|---|---|---|
| vibedb compact (chunk layout) | **13.9** | **26.1** |
| vibedb verbatim (primary graph) | 28.1 | 28.1 |
| SQLite | 28.1 | 28.1 |
| bbolt | 45.8 | 45.8 |
| Pebble | 50.6 | 50.6 |
| Badger | 257.0 | 257.0 |

In this snapshot vibedb compact is the smallest at both cardinalities —
smaller than the raw corpus itself at low cardinality. Verbatim stores the
exact JSON bytes, byte-for-byte recoverable, at SQLite parity. The compact row
measured the since-deleted chunk layout; primary-graph compact and unified
leaves require a new cross-engine footprint run. The representations are never
conflated.

## Reproduction

```
cd bench/competitive
go build -o /tmp/vibe-mixed ./cmd/mixed
go build -o /tmp/vibe-mixedsuite ./cmd/mixedsuite

# five-engine lanes:
/tmp/vibe-mixedsuite -mixed-bin=/tmp/vibe-mixed \
  -workload=<ycsb-a|ycsb-b|ycsb-f|churn|scan> \
  -durability=<buffered-visible|ordinary-sync> -repetitions=10 -output=<file>

# power-safe lane (only the two engines that can promise it):
/tmp/vibe-mixedsuite -mixed-bin=/tmp/vibe-mixed \
  -engines=vibejson-durable,sqlite \
  -workload=<...> -durability=power-safe -repetitions=10 -output=<file>

go run ./cmd/churndisk -engine=<engine>
go run ./cmd/footprint -engine=<engine> -cardinality=<low|high> [-compact]
```
