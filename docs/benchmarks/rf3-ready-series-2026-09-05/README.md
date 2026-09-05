# RF3 Ready-series comparison

This matched run measures the bounded same-group Ready-series storage path and
the owner-side `ReadIndex` deferral fix. It is a valid improvement at C8, but it
does not meet the CockroachDB performance goal and is not a full promotion
matrix.

| Clients | Baseline ops/s | Candidate ops/s | CRDB ops/s | Candidate / baseline | Candidate / CRDB |
|---:|---:|---:|---:|---:|---:|
| 1 | 1,277.5 | 1,251.6 | 2,140.6 | 0.980x | 0.585x |
| 8 | 1,731.4 | 2,091.8 | 10,439.6 | 1.208x | 0.200x |

The C8 improvement repeats in both execution orders: 1.162x with the baseline
first and 1.202x with the candidate first. C1 changes by 1.009x and 0.940x in
the two orders. Across both orders, C1 p99 rises from 4.25 ms to 4.75 ms. At C8,
p50 rises from 2.43 ms to 3.00 ms while p95 falls from 14.23 ms to 12.75 ms and
p99 falls from 27.59 ms to 20.69 ms. The mixed workload's C8 point-read median
rises from 0.77 ms to 1.94 ms while update median falls from 6.35 ms to 3.73 ms.
The next iteration must address that read/write scheduling tradeoff.

All 96,000 measured operations completed with zero client errors and every
trial passed full verification. The earlier candidate stopped on C8 read
refusals; the final owner fix retains the exact read and correlation context
across a pending Ready boundary instead of exposing a transient lifecycle state
as unavailability.

## Method

- Baseline: `daa75505be055509d7442eb6da7caf04fddb94b8`
- Candidate: `445c834d9315ae46ca9c9c74418207fc3b4fb192`
- Fixed client: `eb3e26b465ed881ae0f2695fb9facad0320b2ee7`
- CRDB: v26.3.1, image digest recorded in `manifest.json`
- Linux/arm64 Docker for every engine, capped at 12 CPUs and 24 GiB
- Go 1.27.1, `GOOS=linux`, `GOARCH=arm64`, `CGO_ENABLED=0`, and
  `GOEXPERIMENT=simd`; the runner rejects binaries missing these build settings
- Three physical serving processes, RF3, one SQL endpoint
- 8,192 rows, 256-byte payload, 500 warmups, 4,000 measured operations per
  trial, two repetitions, C1 and C8, `mixed_uniform`
- Both orders: baseline/candidate/CRDB and CRDB/candidate/baseline

No durability option was disabled. VibeDB acknowledgements remain behind the
same quorum, durable apply, and result-settlement fences. The benchmark runs
engines sequentially; no build or test load overlaps a timed arm.

The candidate versus baseline patch is retained in `before-after.patch`.
`manifest.json` records source state, commands, images, platform, limits,
binary hashes, and verified SIMD build metadata. `analysis.json` contains every
trial, latency summary, and diagnostic delta. The six compressed raw reports
retain all latency samples and signal-acknowledged diagnostics. The copied
controls are the exact runner and validator used by the evidence directory.

## Ready and storage evidence

At C8, the candidate groups about 1.42–1.50 logical Ready batches per observed
append barrier. It records roughly 1,530–1,570 proposal batches for about 1,960
measured commands, or only about 1.25–1.29 commands per leader proposal batch.
This confirms that storage-side series grouping works while leader proposal
arrival remains too sparse. Three durability-backpressure attempts were retried
in each of two C8 repetitions; no Ready wave failed and no client operation
failed.

The candidate's captured stopped store artifacts contain about 273 KiB of
logical file bytes. CRDB's reliable live store captures, excluding nested
diagnostic log directories, contain 60.6–62.5 MiB. The resulting median ratio is
225.2x. The engines were captured at different lifecycle stages because those
are the reliable artifacts produced by this harness, so this is raw footprint
evidence rather than a steady-state space-amplification result. Sustained
updates, deletes, history retention, compaction, and a synchronized capture
stage still need a dedicated space campaign.

## Scope

This run covers one warm-cache mixed point workload on one physical machine. It
does not establish multi-machine scaling, contention behavior, failover,
rebalance, schema evolution, or full SQL parity. VibeDB remains about 1.7x slower
than CRDB at C1 and 5.0x slower at C8 in this cell. The broader goal remains
open.

Reproduce with the retained control:

```sh
python3 controls/run-fused-livepoint-canary.py /new/evidence/path \
  --baseline-ref daa75505be055509d7442eb6da7caf04fddb94b8 \
  --candidate-ref 445c834d9315ae46ca9c9c74418207fc3b4fb192
```
