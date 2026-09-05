# Read-authority fault qualification evidence

Status: **incomplete diagnostic; no qualification pass and no performance claim**.

This record covers candidate-only Linux RF3 fixture runs. They are separate from
the planned VibeDB/CockroachDB comparison in
[`assessment-plan.md`](assessment-plan.md). The fixture is three physical-node
processes hosting four RF3 groups on one Docker host, with one loopback SQL
frontend. The run uses the pinned image
`golang:1.27-bookworm@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b`,
`GOEXPERIMENT=simd`,
`GOCACHE=/private/tmp/vibedb-horizontal-gocache`, and an aggregate Docker limit
of 12 CPUs and 24 GiB (swap disabled).

The integrated source under test was
`e3b3e53e04a268d6d5a76ffab5fa14366bec255f`. It was a dirty working tree
containing the reviewed production changes plus the qualification runner and
plan. The runner recorded the same source revision and patch hash
`54d9fd02b60f92dd7b44749644f52422d64f33eb6513b1f4f9a8f77f0a8d020b` before and
after each run. The first two runs below predate the final runner provenance
fix; their raw source records are retained and their limitations are stated.

## Repository archive

Compact repository copies of the initial startup failure, successful short
smoke, e3 Linux runtime tests, and both long fault attempts are in
[`qualification/README.md`](qualification/README.md). The archive catalog is
[`qualification/qualification-manifest.json`](qualification/qualification-manifest.json)
and its tarball checksums are in [`qualification/SHA256SUMS`](qualification/SHA256SUMS).
Each retained run has a source and runtime manifest. The d898 filtered archive
also has a companion per-file provenance record with exact source and binary
identity. Compiled binaries and Docker volumes are intentionally omitted; the
d898 record retains their hashes and sizes. The initial startup smoke's missing
untracked-source snapshot remains explicitly marked as incomplete. The later
d898 diagnostic and its derived offline validation are
documented in [`qualification/authority-fault-diagnostic-d898.md`](qualification/authority-fault-diagnostic-d898.md);
that report preserves the original failed manifest and makes no qualification
or performance claim.

## Retained runs

`/private/tmp/vibedb-horizontal-authority-smoke-20260905T145118Z` is the failed
startup smoke. Its source was revision
`bcfce760cb420c2f51d2c38f97e93d9e96031964` with a dirty production working tree;
the snapshot recorded tracked changes but omitted untracked production Go
files. The server exited with status 1 before authority hits or rounds were
proved. Treat its source provenance as incomplete. The startup log is at
`candidate/candidate-server.log` and the captured command is in
`control-commands.jsonl`.

`/private/tmp/vibedb-horizontal-authority-smoke-20260905T150902Z` is the
successful correctness smoke at the earlier immutable candidate revision
`91d1fd119b8c227c5356cd0f4cac9f0937407303`. It ran `point_hit,point_miss` with
3 nodes, 4 groups, rows 64, warmup 1000, 2000 operations, 200 scans, one
repetition, and clients 1 and 8. The strict oracle checked 8000 samples and
the client exited 0. The authority-hit delta was 8000, lifetime hits were
11996, and the retained diagnostic counters showed four rounds. This revision
predates frozen `M` and is smoke-only; it is not campaign `S` and had no fault
injection or restart qualification.

`/private/tmp/vibedb-horizontal-authority-diagnostic-20260905T162305Z` is the
later clean no-fault diagnostic at `2c887f4a20180f15be353a8492e27067e0a0b19d`.
It ran all four workloads (`point_hit,point_miss,mixed_read_update,mixed_uniform`)
with 60,000 operations, 500 scans, warmup 1000, clients 1 and 8, and three
physical nodes. The strict workload oracle completed 480,000 samples with zero
errors and client/server exit 0. The separate per-group sidecar had 536
complete 21-member cycles but 11,256 authentication/protocol sampling errors;
those cuts are invalid for causal analysis. This was a no-fault run, so it
provides no pause, restart, fault-qualification, or CRDB comparison result.
Its compact repository archive is
[`authority-diagnostic-2c887f4a.tar.gz`](qualification/archives/authority-diagnostic-2c887f4a.tar.gz).

`/private/tmp/vibedb-horizontal-authority-fault-diagnostic-d898-20260905T191703Z`
is the later full diagnostic at clean source revision `d898f062...`. It ran
the same 60,000-operation, four-workload matrix at clients 1 and 8, completed
480,000 oracle samples with zero errors, paused one process, and restarted it
from the same volume. The retained post-CONT latch was revalidated offline
with exact integer nanosecond ordering after the original parser rejected its
valid nine-digit fractional timestamp. The original manifest remains
`incomplete-or-failed` with hash
`eefdcce6af86a6e473bd36378d1b008266f87d59193a0d5171656fc4e514355c`; the
derived report has `qualification_claimed=false`. The archive, provenance, and
checksums are linked from [`qualification/README.md`](qualification/README.md).
The configured 5-second grant and approximately 6.11-second restart quarantine
are elapsed-clock settings under the ±10% clock-rate assumption, and the run
did not expose a quarantine boolean.

`/private/tmp/vibedb-horizontal-authority-fault-20260905T173000Z` is the first
long qualification attempt at `e3b3e53e...`. It used 60000 operations, 500
scans, warmup 1000, clients 1 and 8, and
`point_hit,point_miss,mixed_read_update,mixed_uniform`. The strict point-hit and
point-miss trials passed. Before any fault signal was sent,
`mixed_read_update` warmup failed with `gateway: replicated shard has no
reachable leader` and `gateway: no authenticated replica reported itself as
leader`; the server log also contains 154 `stale serving fence` messages and
1096 `entry at index ... missing from unstable log` messages. The report is
`status=failed`, `client_exit_code=1`, `samples_checked=240000`, and the
verification error is retained verbatim in `candidate/report.json`. This is a
real pre-fault qualification failure, independent of the runner's original
60-second measuring-state wait bug. Authority diagnostics still recorded
243992 lifetime hits and 48 rounds, so fast-path use and renewal amortization
were observed, but those counters cannot turn the failed strict workload into
a pass. The run container was `vibedb-fused-15806a097b05` and its volume was
`vibedb-fused-15806a097b05-data`.

`/private/tmp/vibedb-horizontal-authority-fault-20260905T181500Z` is an
interrupted retry after the measuring-state wait fix. It intentionally used a
different bounded workload set (`point_hit,point_miss,mixed_uniform`) and
40000 operations, so it cannot qualify or refute the `mixed_read_update`
failure above. Point-hit and point-miss completed cleanly; the client was
interrupted while `mixed_uniform` was preparing and its report remains
`status=incomplete` with `verification_error=benchmark did not finish`.

The retry did execute the requested process pause before it was stopped. The
target was candidate shard PID 273, node identity
`206b03f87943e2d875fa68f4d92f264d`, selected from fresh acknowledged
`authority_read_hits` deltas (98→279, 268→673, and 160→375 for the three
physical nodes). The event records `SIGSTOP`/`SIGCONT` through Docker with an
observed 7.0500745-second interval against the 5-second grant. This is a
process pause only, not a network partition. The runner explicitly records
`native_current_group_leader_probed=false` and `current_holder_claim=false`;
the selection proves recent fast-path activity, not a current per-group
holder. No wrong-result, stale-result, fallback, or recovery claim is made
because the strict mixed workload did not finish. SIGKILL/restart and recovery
were not reached. The retry container was `vibedb-fused-d632e9f869b2` and its
volume was `vibedb-fused-d632e9f869b2-data`; both were cleaned up by the
fixture, and no run processes remain.

For both long attempts the four candidate binary hashes were identical:

```text
candidate-vibedb         303ae167475a8d6ee360b4798b22de92fcffdabc1abcecac8db78fe80a37448f
candidate-vibedb-gateway 76deb96af94f2d186baaad71d39c97feb96afa346ceb9910ecf15cba84ab9187
candidate-vibedb-shard   651d3969ec78aad4b9d640f3cbeb628da9d1e504d789967c4a4a478f17cfc860
rf3-sqlbench             8f0557bef96e26648a7c6a6ebb3041244b5c820bead492728658b6982150e631
```

The exact redacted client and server argv for each long attempt are in its
`manifest.json`. The client shape was:

```text
/bench/rf3-sqlbench -engine vibedb -url postgresql://[redacted]@127.0.0.1:5432/vibedb?sslmode=disable -urls postgresql://[redacted]@127.0.0.1:5432/vibedb?sslmode=disable -rows 64 -operations <N> -scans 500 -warmup 1000 -repetitions 1 -clients 1,8 -tables rf3_sql_group,rf3_sql_group_01,rf3_sql_group_02,rf3_sql_group_03 -workloads <workloads> -group-distribution uniform -skew-percent 80 -physical-nodes 3 -output /evidence/report.json -require-existing-tables -diagnostic-targets /evidence/diagnostic-targets.json -phase run -recovery-oracle /evidence/client-oracle.json
```

The principal raw files are retained under each long-run directory:

```text
manifest.json
control-commands.jsonl
candidate/candidate-server.log
candidate/client.log
candidate/report.json
candidate/raw/report.json
candidate/run.json
candidate/diagnostics/
candidate/published/ready/
candidate/published/after/
candidate/published/stopped/
fault/pre-pause-*.txt and fault/pre-pause-inventory.json
fault/selection-*.json
fault/post-pause-*.txt and fault/post-pause-inventory.json
```

The first long run has no `fault/` pause records because the strict workload
failed before the old wait elapsed. The retry has complete pre/post pause
inventories, signal events, selection snapshots, and per-node diagnostics, but
no restart records. These paths preserve startup failures, node and gateway
logs, exact control commands, group incarnations, oracle output, and source and
binary provenance for diagnosis.

The cgroup `stats-before`/`stats-after` and Docker resource files are whole-run
snapshots that include setup and seeding. They are aggregate campaign-resource
observations, not per-query CPU or peak-RSS measurements. No throughput result
from these diagnostics is comparable with CockroachDB or suitable for a
performance claim. Further fault loads remain blocked pending diagnosis of the
pre-fault mixed workload failure and an explicitly reviewed source or
instrumentation change.
