Guarded point updates measured **1.946× and 1.997× baseline throughput at C8**, with p99 latency falling to **0.489× and 0.461×** baseline. Cluster append barriers per update fell by about 49% in both orders. Candidate throughput remains **0.633× and 0.645× CockroachDB** for these updates. This is promising bounded progress; the full performance goal and promotion gates remain unmet.

Before is `f2df4fac525fa0f73ff2e1b558f167e1e598f6c5`; after is `7b8efb88436dba5452ef4ab46e9c3d72f242adb1`. The source difference contains three gateway production files, four test files and the guarded-update plan. The earlier compact batch storage experiment was reverted before this candidate and is excluded from these timings. The retained change prepares eligible exact-key updates from a committed preimage and checks the complete row digest atomically at apply. Public reads remain linearizable; missing rows and stale evaluation errors fall back before admission. Focused SIMD normal/race tests cover guard rejection, exact replay, fallback and unchanged public reads.

Actual binaries prove **Go 1.27 with `GOEXPERIMENT=simd`, Linux/ARM64**, correct clean revisions and ELF64 AArch64 output for both VibeDB builds and the shared client. CockroachDB 26.3.1 is the pinned stock Linux/ARM64 build on the same Docker VM and kernel. Each fixture uses RF3, three serving processes, one SQL endpoint, fresh storage and an aggregate **12 CPU / 24 GiB** ceiling including the client, with swap disabled. The retained control script sets SIMD explicitly and rejects missing SIMD or mismatched OS/architecture in binary build metadata. This establishes experimental build selection, not SIMD execution in every workload or kernel.

The independent audit validates **120 trials, 153,600 latency samples and 480 diagnostic snapshots, with zero SQL errors**. Workloads are point hits/misses, range scans, grouping and updates at C1/C8, using one table, 1,024 rows and two repetitions in each of two fixture orders. No earlier campaign contributes samples.

| Metric | Before-first order | After-first order |
|---|---:|---:|
| Overall throughput, after/before geomean | 1.7905× | 0.8896× |
| Overall throughput, after/CRDB geomean | 0.5774× | 0.4949× |
| C8 update throughput, after/before | 1.9464× | 1.9967× |
| C8 update p99, after/before | 0.4886× | 0.4610× |
| C8 update throughput, after/CRDB | 0.6330× | 0.6454× |
| C1 update throughput, after/before | 5.1868× | 0.7858× |
| Cells losing over 5% throughput | 0 | 5 |
| Cells increasing p99 over 10% | 0 | 7 |

There is **no stable overall gain**. The first baseline has severe stalls: median C1 update throughput is 121.4 ops/s with 107.95 ms p99, and unchanged read workloads also vary sharply. A retained term 2→3 transition occurs during its read phase, but bounded logs cannot establish the cause of all stalls. Reads are local in both arms of the first order; the reverse order has local candidate reads and remote baseline reads. Shared-host activity, placement differences and short scan samples further limit attribution. All unfavorable trials remain included, and exact speedup magnitude cannot be attributed solely to the code change. Zero SQL errors does not imply error-free runtime counters; warnings and native service failures remain disclosed in the audit.

This is a short single-host diagnostic. It does not qualify independent-machine scalability, failure recovery, sustained checkpoint performance, interactive transaction parity or the ≥2× CockroachDB objective. Both CRDB fixtures required one forced process shutdown after measurement; that cleanup provides no recovery evidence. The reviewed guarded-update implementation remains on the redesign branch while these qualification gaps remain open.

See [the full independent audit](audit-summary.md), [structured audit](audit-summary.json) and [runtime/build evidence](environment.json). Extract `raw-trials.tar.gz` beside this file to resolve the audit's raw-source links. All **491 entries** and the **2,042,254-byte archive** were SHA256-verified against `sha256.json`. It contains six reports, 480 referenced diagnostic snapshots, exact control scripts and the source patch. It excludes databases, node manifests, keys, certificates and server logs.
