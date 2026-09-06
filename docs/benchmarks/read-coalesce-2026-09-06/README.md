# Read coalescing: merged PR 194 performance

PR #194 merged as `5f9ac3c0`. This RF3 ABBA comparison completed **96 trials and 300,000 timed operations, with zero errors and every trial verified**. The candidate tree equals the merge tree. The sole CI failure was `io_uring_setup: cannot allocate memory` in unchanged storeio tests; the failed job passed on rerun before merge.

The results are mixed. Range-read throughput increased in both run orders. Single-client updates slowed in both orders. Eight-client point reads and mixed traffic changed direction between orders. This is not evidence of an across-the-board improvement, and no CockroachDB comparison was run.

## Method

Three physical Linux ARM64 processes, RF3, four logical tables with 8,192 rows each, one client endpoint, uniform distribution, 12-CPU/24-GiB container limits. Authority caching was disabled in both revisions. Both use the same frozen client and Go 1.27 builds. Baseline `38409c7a` already includes buffer reuse and bounded owner scheduling; candidate `e8cf5617` adds only PR 194.

Order: baseline A1, candidate B1, candidate B2, baseline A2, each in a fresh container/volume. Each workload/client cell has three repetitions per arm: 4,000 operations for point/update/mixed, 500 scans for range-32, and 500 warmup operations. Tables below use medians of six trial rates and medians of six per-trial p99 values, not a pooled latency percentile.

No competing container starts/execs were recorded and binary hashes stayed unchanged. Host resource contention was not excluded and leader placement was not pinned across fresh clusters. Range trials are short at these rates. These are descriptive observations from one ABBA campaign, not confidence-bounded estimates of causal speedup. The runtime diagnostic does not export ReadIndexShared, so this campaign does not quantify the actual sharing rate.

## Observed medians

| Workload | Clients | Before ops/s | After ops/s | Change | Before p99 ms | After p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| mixed_uniform | 1 | 603.7 | 1,261.2 | +108.9% | 9.634 | 4.884 |
| mixed_uniform | 8 | 895.4 | 1,473.2 | +64.5% | 48.584 | 33.484 |
| point_hit | 1 | 3,649.2 | 3,952.2 | +8.3% | 0.404 | 0.361 |
| point_hit | 8 | 18,783.1 | 19,627.9 | +4.5% | 0.697 | 0.679 |
| range_32 | 1 | 3,589.0 | 3,773.8 | +5.2% | 0.408 | 0.394 |
| range_32 | 8 | 17,722.6 | 19,283.9 | +8.8% | 0.717 | 0.680 |
| update_existing | 1 | 748.6 | 564.5 | -24.6% | 6.442 | 8.362 |
| update_existing | 8 | 1,167.1 | 1,164.9 | -0.2% | 20.446 | 27.029 |

## Order sensitivity

Each ratio below compares the median of three trials in a candidate arm with its adjacent baseline arm. These paired medians can differ materially from the pooled-six median above.

| Workload | Clients | B1/A1 change | B2/A2 change |
|---|---:|---:|---:|
| mixed_uniform | 1 | +112.4% | +105.4% |
| mixed_uniform | 8 | -7.1% | +137.2% |
| point_hit | 1 | +14.0% | +4.3% |
| point_hit | 8 | -4.4% | +6.8% |
| range_32 | 1 | +4.4% | +6.1% |
| range_32 | 8 | +12.2% | +8.0% |
| update_existing | 1 | -16.3% | -4.2% |
| update_existing | 8 | -0.5% | +7.5% |

Single-client mixed traffic improved in both pairs, but this does not establish why; internal requests and background activity differ from a lone point read. Eight-client mixed throughput varies strongly even within a single revision. Single-client update regression needs isolated follow-up before describing this optimization as a general win.

## Retained evidence

[Summary](summary.json), [build metadata](build.json), [summarizer](summarize.py), [raw evidence archive](evidence.tar.gz), and [checksums](SHA256SUMS). The archive contains manifests, frozen hashes, controller sources and commands, client reports with per-operation samples, diagnostics, container event logs and topology snapshots. Binaries and published database volumes are omitted. Internal arm label `scheduler` means the PR 194 candidate, not an owner-fairness-only build.
