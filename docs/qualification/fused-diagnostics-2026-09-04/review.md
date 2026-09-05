Diagnostics Sol review frozen — 2026-09-04

Owned diff ready for root commit, with no staging or commit by this reviewer:

- cmd/vibedb-shard/rf3_diagnostics.go
- cmd/vibedb-shard/serve_rf3.go
- cmd/vibedb-shard/rf3_diagnostics_resource_test.go (new)
- cmd/vibedb-shard/rf3_diagnostics_resource_linux_test.go (new, separately authorized by root)

The final collector adds detached ResourceStats overlay counters, fixed histogram buckets, gauges and explicit availability/coverage/failure counts to the existing flat SIGUSR1 record. Existing JSON keys, latest-file path and atomic replacement behavior remain. No new endpoint, exporter, collection Snapshot, Checkpoint or Flush was added. The existing diagnostics file publication still writes/syncs its latest evidence file; collecting storage counters adds no storage I/O.

Reviewed and confirmed Luna's corrections: expected groups come from the current manifest/native-authority union, with deduplication; retired startup groups are not reintroduced. Current schema handles override prepared/adopted predecessor handles, including nil/closed current handles that must fail closed. Closed/failed inventory marks resources unavailable. Relation count is validated before indexing. All sums saturate and overflow makes resources unavailable. Inventory and schema locks are released before ResourceStats enters the apply/database/collection locks. JSON contains only detached bounded scalars/fixed histograms, with no retained error strings or group labels.

Sol added the production counter-lifetime comment and two real regressions:

- TestRF3DiagnosticRealCollectionSamplingPreservesPendingOverlay bulk-builds a real buffered durable collection, retains pending overlay records, samples through the collector, and compares the complete durable.Stats value before/after. Fold/materialization counters and histograms, publication/durable generations, dirty gauges, page-read/device/journal counters all remain unchanged. A subsequent real mutation does not change the retained diagnostic totals.
- TestRF3DiagnosticRealApplyCurrentSchemaAndDynamicCoverage persists and applies a real schema transition in a shared node log, recovers generation 2 through openRF3RetainedApply, and keeps the closed prepared generation 1 in both fallback provider lists. The collector selects the current generation, fails closed while a native group lacks its provider, includes a second real apply through the schema map used by reload, and compares complete ResourceStats plus checkpoint-group DurabilityStats before/after collection. Closed inventory and concurrent current-apply closure are covered without stale fallback.

Validation used an isolated copy of frozen source /private/tmp/vibedb-catalog-miss-fixed-6402842c with only these four files overlaid. Unreviewed storage/gateway WIP was excluded. Focused Linux normal PASS 0.851s; focused Linux race PASS 2.799s. Named real-regression PASS appears in both logs, with no SKIP. git diff --check passes for the tracked owned source diff. Tested copies were checked byte-for-byte against the final owned files.

Evidence:

- /private/tmp/fused-diagnostics-sol-linux-command.txt — exact source preparation, Docker image ID, platform, mounts, volume, env and both commands
- /private/tmp/fused-diagnostics-sol-linux-normal.log — verbose normal output
- /private/tmp/fused-diagnostics-sol-linux-race.log — verbose race output
- /private/tmp/fused-diagnostics-sol-sha256.txt — four source hashes and both log hashes
- /private/tmp/vibedb-diagnostics-sol-review-20260904 — retained isolated tested source

Practical limits: resource counters sum currently open collection generations, so schema replacement/group retirement can reset them within one PID; interval comparisons require unchanged generation/inventory. This focused test exercises real schema recovery and reload's provider publication topology, not a new real-process SIGHUP campaign. The pending overlay proof uses the existing buffered collection path; the frozen checkpoint-group implementation materializes batches, so it cannot qualify the separate proposed checkpoint-batch overlay candidate. No benchmark, performance claim, quorum/fault expansion or durability change is part of this review. All test/build activity has stopped.
