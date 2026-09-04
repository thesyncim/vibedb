# Diagnostic profile and storage accounting

Clean profiled source `4bac98e5`; 8192 rows, C1/C8, 1024 point/update operations,
256 scan operations, 64 warmups, one repetition. Profiling was enabled. These
timings are diagnostic and must not be compared as benchmark results.
Both clients completed successfully. Process IDs are recorded; data member 3
(pid 283) served the SQL read regions. Flight traces had no unmatched edges
for the reported regions. CPU samples include startup and the mixed workload.

5,822 read attempts: mean admission 0.000373 ms, quorum/cut 0.226124 ms,
SQL execution 0.784897 ms. Medians: quorum/cut 0.193664 ms, execution 0.037760 ms.
These aggregates mix point queries, scans and untimed verification and are not
a per-workload latency decomposition. There were three fewer encoded responses
than attempts, consistent with tier escalation before response publication.

2,304 write regions: mean preparation 0.492457 ms, direct execution 2.212054 ms,
and total write 2.884037 ms. Only three initialization/fallback outbox saves.
Nested region times must not be added to parent totals.

The data leader CPU capture had 9.08 seconds of samples: Segment.Append had
1.31 seconds cumulative, including document building/indexing; parseDecimal
accounted for 0.30 seconds flat. These are mixed-workload samples, not a claim
that all scan cost is parsing. The raw profile supports deeper analysis.

The file-level du output reports 3,585,228 KiB for VibeDB. Twelve member.wal
files each reserve approximately 256 MiB, totaling about 3 GiB. This is fixed
per-member allocation, not live-data amplification. CRDB's three emergency
ballast files also total approximately 3 GiB; retain them in allocated-space
reporting and separate their purpose when comparing engine data density.
A small warm fixture cannot establish steady-state space efficiency.

Reproduce with run-crdb-sql-comparison.py --profile --rows 8192
--operations 1024 --scans 256 --warmup 64 --repetitions 1. Decode traces with
Go 1.27 trace -d=parsed and summarize-go-trace-regions.py. The SHA256 file
covers the original uncompressed trace and CPU files.
