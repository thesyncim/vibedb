# Performance

Performance claims are commit-pinned measurements, not properties of the API
and not automatically claims about current `main`.

The latest complete competitive publication is commit
`8c1142322c96cfe8f99d04cd04683a5d827e6710`, measured on 2026-08-14 on an Apple
M4 Max with Go 1.26.0. The exact protocol, all five-engine tables, artifact
hashes, dependency versions, and reproduction commands are in
[Competitive results](../bench/competitive/RESULTS.md).

## Published summary

Single-client buffered-visible throughput, median of ten isolated repetitions:

| workload | VibeDB | Badger | VibeDB / Badger |
| --- | ---: | ---: | ---: |
| YCSB-A | 374,293 | 296,759 | 1.26× |
| YCSB-B | 1,396,465.5 | 1,024,235.5 | 1.36× |
| YCSB-F | 387,114 | 264,666 | 1.46× |
| churn | 563,800 | 383,864 | 1.47× |
| scan mix | 255,356 | 274,536.5 | 0.93× |

The update-heavy result is strong at one client. The remaining single-client
weakness is ordered scanning under mutation: VibeDB's scan-mix ordered-scan
p50/p99 is 2.431/2.692 ms versus Badger's 1.541/1.755 ms.

Checkpoint p99 is 0.816–1.372 ms across the five lanes, still
8.74–13.45× Badger's corresponding tail. Requested checkpoints are included in
elapsed throughput.

## Concurrency

| workload | clients | VibeDB | Badger | VibeDB / Badger |
| --- | ---: | ---: | ---: | ---: |
| replacement | 1 | 237,524 | 165,319.5 | 1.44× |
| replacement | 8 | 257,265.5 | 239,677 | 1.07× |
| replacement | 32 | 269,722 | 252,644 | 1.07× |
| churn | 1 | 542,512.5 | 370,904.5 | 1.46× |
| churn | 8 | 201,188 | 535,117.5 | 0.38× |
| churn | 32 | 127,940 | 522,805.5 | 0.24× |

At a fixed client count every engine receives the same deterministic trace.
Across client counts, workers use disjoint corpus shards and independently
seeded Zipf streams, so the rows include a changing hot set and locality rather
than a pure same-trace scale-up.

Replacement stays ahead at 32 clients. Concurrent delete/restore churn is the
largest published throughput weakness and is not presented as solved.

## Resource envelope

In the one-client, 10,000-document replacement lane, VibeDB ended at
3.2/2.9 MiB apparent/allocated disk, 45.6/55.7 MiB Go heap/runtime-resident
memory, and 51.9 MiB peak RSS. Badger ended at 257.0/9.1 MiB disk,
86.1/96.55 MiB heap/runtime, and 90.9 MiB RSS.

These scopes are different and must not be added together. This lane is not a
bulk-load or sustained-churn footprint comparison. No current bulk/churn-disk
publication is claimed; older measurements were removed rather than carried
forward as current product evidence.

## Current design implications

- Warm compiled paths are designed to reuse bounded workspaces and target zero
  allocations; tests and microbenchmarks pin individual lanes.
- Deferred overlays, journals, caches, publisher queues, transaction state, and
  retired extents have fixed capacities. Pressure becomes foreground work or a
  typed refusal, not unbounded background debt.
- Stronger durability is a different benchmark lane. Do not compare a
  per-mutation power-safe acknowledgement with buffered visibility or an
  ordinary fsync mode.
- Multi-collection durable commit costs `K+1` syncs for `K` participants. That
  is the current protocol, not a hidden one-sync claim.
- No background compactor means checkpoint and fold tails are part of the
  foreground latency contract and must remain inside measured time.

## Running benchmarks

The competitive harness lives in its own module so competitor dependencies do
not enter the product module:

```sh
cd bench/competitive
go test ./... -count=1
```

Use the complete publication loop in
[Competitive results](../bench/competitive/RESULTS.md) for cross-engine claims.
Use [Benchmark coverage](../bench/competitive/COVERAGE.md) to see which
correctness evidence backs each lane. Use [Allocation regression gate](../bench/gate/README.md)
for local A/B allocation checks.

## Reading results

- Compare only equal durability, checkpoint cadence, workload, corpus, and
  client count.
- Throughput cells are repeated-sample medians; percentile cells are medians of
  run-level percentiles unless stated otherwise.
- Apparent bytes, allocated filesystem bytes, Go heap, runtime-resident memory,
  and RSS answer different questions.
- A microbenchmark explains one implementation path; it cannot replace an
  end-to-end database result.
- A result belongs to its exact commit. Rerun the full matrix before describing
  a newer commit as faster, smaller, or more scalable.
