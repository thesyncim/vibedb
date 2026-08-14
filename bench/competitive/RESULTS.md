# Competitive results

This page contains the latest published competitive snapshot, not a rolling
claim about `main`.

## Snapshot identity

- Engine commit: `8c1142322c96cfe8f99d04cd04683a5d827e6710`
- Date: 2026-08-14
- Host: Apple M4 Max, 16 logical CPUs, 64 GB, Darwin 25.3.0, APFS
- Go: 1.26.0
- Competitors: bbolt 1.5.0, Badger 4.9.5, Pebble 1.1.5, modernc SQLite 1.54.0
- Durability: buffered-visible, CP64 scheduled checkpoints
- Corpus: 10,000 documents, 2,000 warmup operations, 20,000 measured
  operations
- Repetitions: ten recorded Latin-square repetitions after one discarded
  conditioning pass; engines run in isolated child processes

Every suite recorded a clean Git root, the full commit, an empty tracked diff,
all publication flags true, and zero pressure-forced checkpoints. The nested
benchmark module had no Go VCS build fields, so the binaries were pinned by
SHA-256:

| binary | SHA-256 |
| --- | --- |
| `mixed` | `b6e2b719d962787e42d0335d19a7d93c900c7fa162116c94a5a3c1d2115f8a18` |
| `mixedsuite` | `856da49919a3cc65ac18944cfeb859c2bc91f0fe89c89f861f484adb95706bee` |

The original TSV and micro-gate logs lived in the ephemeral local directory
`/private/tmp/vibedb-publish-8c11423`; they are not repository downloads. Their
recorded hashes preserve provenance:

| artifact | SHA-256 |
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
| `leaf-fold.txt` | `e3ca349ad81b9db70a6682633d302863e28e45095b94479107083526b9f7750f` |
| `scan.txt` | `0c7e94b2f7425ec0f896bf61ace2dfd47d7a02b1853b178ebb4748d264b5b802` |

No bulk-footprint or sustained-churn-disk result is claimed by this snapshot.
Older measurements were removed from current product documentation; rerun the
current harness before publishing those surfaces again.

## Single-client mixed workloads

Median total operations per second. The leading engine is bold.

| workload | VibeDB | Badger | SQLite | Pebble | bbolt | VibeDB / Badger |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| YCSB-A: 50% read, 50% update | **374,293** | 296,759 | 98,928.5 | 25,975 | 21,623 | **1.26×** |
| YCSB-B: 95% read, 5% update | **1,396,465.5** | 1,024,235.5 | 301,115.5 | 222,450 | 208,415 | **1.36×** |
| YCSB-F: 50% read, 50% RMW | **387,114** | 264,666 | 87,362 | 27,068.5 | 23,272.5 | **1.46×** |
| Churn: 70% read, 25% update, 5% delete+restore | **563,800** | 383,864 | 127,748.5 | 35,623.5 | 30,745 | **1.47×** |
| Scan mix: 79.9% read, 15% update, 5% delete+restore, 0.1% full scan | 255,356 | **274,536.5** | 113,481 | 46,365 | 40,163 | 0.93× |

VibeDB leads Badger on the four point/update-heavy lanes. Scan mix remains
behind because the ordered full scan is slower.

### VibeDB latency

Each cell is the median of ten run-level percentiles in microseconds, not a
pooled quantile.

| workload | operation p50 / p95 / p99 | checkpoint p50 / p95 / p99 |
| --- | ---: | ---: |
| YCSB-A | read 0.167 / 1.0205 / 1.1665; update 2.3335 / 3.8335 / 31.792 | 30.167 / 169.604 / 1,226.625 |
| YCSB-B | read 0.125 / 0.8955 / 1.0205; update 2.458 / 3.896 / 62.313 | 193.417 / 230.771 / 1,099.2915 |
| YCSB-F | read 0.167 / 1.0205 / 1.166; RMW 2.583 / 4.5 / 33.437 | 31.271 / 283.271 / 815.6035 |
| Churn | read 0.125 / 1 / 1.1455; update 2.416 / 4.3125 / 31.187; delete+restore 2.25 / 4.75 / 39.687 | 29.5 / 273.458 / 1,012.8335 |
| Scan mix | read 0.208 / 1 / 1.166; update 2.417 / 4.3125 / 33.5415; delete+restore 2.375 / 5 / 46.375; scan 2,430.833 / 2,579.583 / 2,691.667 | 33.4165 / 266.125 / 1,371.521 |

Checkpoint p99 is 0.816–1.372 ms, still 8.74–13.45× Badger's corresponding
81.8545–115.771 µs. Scan-mix ordered-scan p50/p99 is 2.431/2.692 ms versus
Badger's 1.541/1.755 ms.

## Concurrent replacement and churn

Median total operations per second. At each client count all engines receive
the same deterministic trace. Across client counts, each worker owns a
different disjoint corpus shard and independently seeded Zipf stream, so these
rows are not a pure same-trace scaling curve.

| workload | clients | VibeDB | Badger | SQLite | Pebble | bbolt | VibeDB / Badger |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| write | 1 | **237,524** | 165,319.5 | 56,455 | 13,067 | 11,157 | **1.44×** |
| write | 8 | **257,265.5** | 239,677 | 50,095 | 13,356 | 10,993 | **1.07×** |
| write | 32 | **269,722** | 252,644 | 46,708 | 13,332.5 | 10,800 | **1.07×** |
| churn | 1 | **542,512.5** | 370,904.5 | 129,581 | 35,827.5 | 31,679 | **1.46×** |
| churn | 8 | 201,188 | **535,117.5** | 101,348.5 | 35,982 | 23,910.5 | 0.38× |
| churn | 32 | 127,940 | **522,805.5** | 103,350 | 36,047 | 17,279.5 | 0.24× |

Replacement remains ahead of Badger at 8 and 32 clients. Mixed churn is the
clear weakness: VibeDB falls to 37.1% and 23.6% of its one-client rate while
Badger rises. This snapshot does not claim concurrent delete/restore scaling is
solved.

### One-client write resource envelope

Post-workload medians for the 10,000-document replacement lane. Disk is
apparent / allocated filesystem MiB; heap, runtime-resident memory, and RSS
have different scopes.

| engine | disk MiB | heap / runtime MiB | peak RSS MiB |
| --- | ---: | ---: | ---: |
| VibeDB | 3.2 / 2.9 | 45.6 / 55.7 | 51.9 |
| Badger | 257.0 / 9.1 | 86.1 / 96.55 | 90.9 |
| SQLite | 2.8 / 2.8 | 2.5 / 11.25 | 41.8 |
| Pebble | 3.5 / 3.6 | 2.8 / 11.75 | 45.0 |
| bbolt | 16.1 / 16.1 | 2.5 / 11.8 | 43.55 |

This is not a bulk-load or long-running churn footprint comparison.

## CPU and scan gates

Five-sample, one-second medians from a clean detached worktree at the same
commit. Every row reports zero bytes and zero allocations per operation.

| gate | median |
| --- | ---: |
| native checkpoint leaf patch | 1.852 µs |
| generic render/plan/encode | 255.438 µs |
| ordered scan, 100k documents, first byte | 75.63 ns/document |
| ordered scan, same corpus, all bytes | 89.08 ns/document |
| competitive canonical render, low/high cardinality | 160.7 / 340.9 ns/document |
| competitive all-byte scan, low/high cardinality | 235.2 / 415.5 ns/document |
| sparse masked scan | 579.0 ns/selected document |

The end-to-end mixed tables remain authoritative for cross-engine claims.

## Publication rules

A replacement snapshot must use one exact clean commit and publish all eleven
five-engine TSVs. It must run engines serially, report repeated-sample medians,
verify equivalent corpus/trace/final state/returned scan bytes, include
scheduled checkpoints inside elapsed time, name durability and checkpoint
cadence, disclose apparent and allocated disk bytes separately, and keep
microbenchmarks distinct from database results.

## Reproduction

From `bench/competitive` at the named commit:

```sh
go test . -run '^(TestFullEquivalence|TestFullEquivalenceIndexedDurable|TestCorpusVariantsAreShapeMatched)$' -count=1 -timeout=60m
go test ./cmd/... -count=1 -timeout=60m
test -z "$(git status --porcelain=v1 --untracked-files=normal)"

publication_dir=$(mktemp -d /tmp/vibedb-publish.XXXXXX)
engines=vibedb,bbolt,badger,pebble,sqlite
go build -trimpath -o "$publication_dir/mixed" ./cmd/mixed
go build -trimpath -o "$publication_dir/mixedsuite" ./cmd/mixedsuite

for workload in ycsb-a ycsb-b ycsb-f churn scan; do
  "$publication_dir/mixedsuite" -mixed-bin="$publication_dir/mixed" \
    -engines="$engines" -workload="$workload" \
    -durability=buffered-visible -clients=1 \
    -checkpoint-mutations=64 -repetitions=10 \
    -output="$publication_dir/mixed-single-${workload}-c1.tsv"
done

for workload in write churn; do
  for clients in 1 8 32; do
    "$publication_dir/mixedsuite" -mixed-bin="$publication_dir/mixed" \
      -engines="$engines" -workload="$workload" \
      -durability=buffered-visible -clients="$clients" \
      -checkpoint-mutations=64 -repetitions=10 \
      -output="$publication_dir/mixed-concurrent-${workload}-c${clients}.tsv"
  done
done
```

See [the harness guide](README.md) for workload, durability, footprint, and
micro-gate commands.
