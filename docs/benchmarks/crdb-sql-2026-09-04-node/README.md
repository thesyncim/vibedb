# Shared-node log: matched SQL comparison

Clean VibeDB source `58d302ef`, Go 1.27 with `GOEXPERIMENT=simd`. Both engines
finished with zero client errors and complete table verification. The validator
checked all 120,000 raw latency samples. VibeDB remains slower on every measured
workload: **0.331–0.578× CRDB**, versus the goal of at least 2×.

[Throughput medians](summary.md), [latency percentiles](latencies.md),
[environment and binary hashes](manifest.json), and compressed raw reports and
server logs are retained here. `sha256.json` covers the collected evidence.

## Method

Same matrix as the earlier pipelined comparison: RF3, 8,192 rows with a 256-byte
payload, point hits/misses, ordered 64-row ranges, grouped scans, and existing-row
updates. C1/C8, 1,000 warmups, 2,000 measured operations per workload/client count,
three repetitions. Both engines run sequentially, VibeDB first, within the same
Linux arm64 container capped at 12 CPU and 24 GiB with a named disk volume.
CRDB is v26.3.1 pinned by the manifest's image digest. Inter-node TLS is enabled;
SQL uses loopback trust/plaintext for both engines. No durability setting was
disabled. VibeDB uses nine shard processes plus a gateway; CRDB uses three nodes.

This is a single-host warm-cache autocommit comparison. It does not qualify
interactive serializable SQL parity, multi-machine scaling, fault behavior,
contention, rebalance or concurrent schema evolution. The full goal remains open.
No build, test or profile processing overlapped either timed engine run.

Reproduce:

```sh
python3 scripts/bench/run-crdb-sql-comparison.py /new/evidence/path \
  --node-log --rows 8192 --operations 2000 --scans 2000 \
  --warmup 1000 --repetitions 3 --clients 1,8
```

## Interpretation

C8 grouped scans reach 1,925.5 ops/s versus CRDB 3,330.5 (0.578×). C8 updates
reach 2,581.9 versus 5,898.9 (0.438×); update trial rates vary from 2,026.7 to
2,731.4 and their p99 values span 11.6–16.0 ms. All trials remain in the results.

The earlier pipelined run's ratios were 0.409× for C8 grouped scans and 0.582×
for C8 updates. Thus relative performance improved for that scan and regressed
for concurrent updates. Both engines' absolute rates changed substantially
between runs. Do not attribute the rise in VibeDB ops/s alone to the node log.

## Allocated space

Untimed `du -sk` reports 1,399,680 KiB (1.335 GiB) for this VibeDB fixture.
File-level accounting moments later reports 1,399,484 KiB. The nine shared node
logs total 959,784 KiB, and no per-group `member.wal` appears. Catalog/ledger node
logs use about 97.5 MiB each; the data node logs use about 117.3 MiB each, including
retained history. Initial allocation and data-dependent growth remain distinct.

Earlier diagnostic accounting with the same row count reported 3,585,228 KiB
(3.419 GiB), including twelve roughly 256 MiB per-group WAL reservations. The
observed fixture total is about 61% smaller; the diagnostic workload had a
different operation count, so this is not a steady-state amplification ratio.
The structural reduction replaces per-group reservations with shared node logs.

CRDB's three roots total 4,375,880 KiB here, including 3,145,740 KiB of emergency
ballast. Those files count as allocation but have a different purpose from
VibeDB's log reserves. These small fixtures do not establish superior engine data
density. Sustained updates, deletes, history retention and reclamation need their
own measured campaign.

## Smoke result retained

The preceding `smoke/` run used 128 rows, four warmups and only 16 updates. Both
engines verified successfully, but VibeDB's first ten measured updates took
19–55 ms and its last six took 1.1–1.8 ms. No cause has been established. Its
54.2 ops/s update result is not a steady-state estimate and has not been deleted.
The full run used the unchanged baseline's 1,000 warmups. Cold behavior remains a
separate performance issue to diagnose.
