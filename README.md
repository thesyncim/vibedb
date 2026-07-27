# vibedb

vibedb is an embedded JSON document database written in Go. It provides a
mutable in-memory store, a bounded-residency durable store, immutable O(1)
snapshots, exact JSON indexes, ordered scans, and a typed query engine. Durable
mutations publish complete copy-on-write generations: readers do not reconcile
memtables, tombstones, or version chains.

The project is pre-v1. Public APIs and the on-disk format may change without a
migration path.

## Measured baseline

These are medians from the checked-in Apple M4 Max baseline: Go 1.26.0,
darwin/arm64, 100,000 documents, three isolated process runs. They describe
the current default durable store unless a row says otherwise.

| Measurement | Result |
| --- | ---: |
| Random point read | 1,162 ns, 0 B / 0 alloc |
| Ordered iteration | 7.546 ns/document, 0 B / 0 alloc |
| Ordered all-bytes scan | 79.64 ns/document, 0 B / 0 alloc |
| Exact indexed filter, 945 matches | 36.108 µs |
| Verbatim bulk file, 23.73 MiB raw JSON | 32.2 MiB allocated |
| Explicit compact bulk, low / high cardinality | 13.9 / 26.1 MiB allocated |
| Power-safe mixed workloads vs comparable SQLite | 6.3–14.7% lower throughput |

The full tables, commits, corpus definitions, caveats, and reproduction
commands are in [competitive results](bench/competitive/RESULTS.md). Compact
bulk is a separate representation, not the mutable default.

## Quickstart

```go
file, err := os.Create("example.vdb")
if err != nil { log.Fatal(err) }
defer file.Close()

db, err := durable.Create(file, durable.Options{})
if err != nil { log.Fatal(err) }
defer db.Close()

if _, err = db.Put("user:1",
	[]byte(`{"name":"Ada","active":true}`)); err != nil {
	log.Fatal(err)
}
snapshot, err := db.Snapshot()
if err != nil { log.Fatal(err) }
defer snapshot.Close()

q := query.Select(query.Path("name")).
	Where(query.Cmp("active", query.Eq, true))
result, err := q.Run(query.FromFile(snapshot))
if err != nil { log.Fatal(err) }
fmt.Println(result.RowCount) // 1
```

Imports are `os`, `log`, `fmt`,
`github.com/thesyncim/vibedb/store/durable`, and
`github.com/thesyncim/vibedb/query`. The caller owns the `*os.File`; keep it
open until `Close` returns.

## Durability at a glance

| Option | Success means | Reader visibility | Crash window |
| --- | --- | --- | --- |
| `DurabilityBufferedVisible` | accepted into bounded canonical COW staging | immediate | acknowledged changes after the last successful `Flush` may be lost |
| `DurabilityAsyncVisible` | accepted by the bounded background committer | immediate | acknowledged generations not yet reported by `DurableGeneration` may be lost |
| `DurabilitySync` (zero value) | data and alternate root crossed the platform's power-safe barriers | after the barriers | recovery selects the complete old or new generation |

For buffered mode, `CheckpointPowerSafe` is the zero-value `Flush` strength;
`CheckpointFilesystem` explicitly selects an ordinary filesystem boundary.
See [the durability contract](docs/durability.md) before choosing a weaker
mode.

## Start here

- [Architecture](docs/architecture.md): representation invariants and the
  read, write, checkpoint, and snapshot paths.
- [Store API](docs/store.md): current heap and durable surfaces.
- [Durability](docs/durability.md): acknowledgement, crash, recovery, and
  platform-sync contracts.
- [On-disk format](docs/format.md): current byte-level format authority.
- [Performance](docs/performance.md): measured tables and benchmark honesty
  rules.
- [Design documents](docs/design/): promotion specifications and future work.
- [Contributing](CONTRIBUTING.md): tests, benchmarks, and documentation rules.

The repository was extracted from
[vibejson](https://github.com/thesyncim/vibejson) on 2026-07-27; that
repository carries the earlier design and measurement history.
