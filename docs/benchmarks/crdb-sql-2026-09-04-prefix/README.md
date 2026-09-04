# Compact prefix CPU change: matched SQL comparison

Clean source `a37b8471`, Go 1.27 with SIMD experiment enabled. Both clients
completed, full table verification passed, and the raw validator checked all
120,000 samples. VibeDB remains slower on every workload: **0.330–0.595× CRDB**.

[Throughput medians](summary.md), [latency percentiles](latencies.md), the complete
raw reports, server/client logs and environment manifest are retained here.
The only production change from the preceding node-log comparison is the
compact scalar prefix kernel. Durability barriers and encoded formats are
unchanged. The [node-log process fault campaign](../../qualification/node-fault-2026-09-04/README.md)
also passes with this code.

## Method

Unchanged single-host RF3 matrix: 8,192 rows, 256-byte payload, C1/C8,
1,000 warmups, 2,000 operations per trial, three repetitions. VibeDB first,
CRDB second, in the same Linux arm64 container limited to 12 CPU and 24 GiB
with a named disk volume. CRDB v26.3.1 is pinned by image digest. Inter-node
TLS; SQL loopback trust/plaintext on both engines. Nine VibeDB shard processes
plus gateway versus three CRDB processes. No build, test or profile processing
overlaps the timed runs. See `manifest.json` for binaries and platform details.

```sh
python3 scripts/bench/run-crdb-sql-comparison.py /new/evidence/path \
  --node-log --rows 8192 --operations 2000 --scans 2000 \
  --warmup 1000 --repetitions 3 --clients 1,8
```

## What changed

VibeDB update medians are 658.2 ops/s C1 and 2,807.0 C8, compared with 621.0 and
2,581.9 in the preceding node-log run. Relative to the CRDB measured in each
run, the ratios move from 0.501×/0.438× to 0.554×/0.463×. Read rates are broadly
similar. This is a modest observed improvement, not the microbenchmark's 3–4×
long-prefix speedup transferred to database throughput.

The update tails are worse: C8 p99 spans 16.3–20.4 ms, compared with 11.6–16.0 ms
previously. Its third trial falls to 1,862.3 ops/s after 2,850.9 and 2,807.0.
The C1 second trial also has a 14.1 ms p99. These samples and slower trials are
retained; median throughput alone does not establish an overall latency win.
Further profiling must span sustained mutations and rotation/checkpoint work;
the short prior profile did not establish a cause for this throughput cliff.

The representative workload matrix, comparable interactive transaction semantics,
space amplification through reclamation, multi-machine scaling and nonblocking
schema evolution remain incomplete. No performance-goal acceptance gate passes.
