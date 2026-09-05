# Storage fold evidence without materialization

The existing SIGUSR1 diagnostic now includes storage fold counters, histogram,
current overlay gauges and explicit availability/coverage/failure counts.
It samples detached `ResourceStats` across the live group union and selects
current schema handles. Missing or closed providers, malformed counts and
arithmetic overflow make the evidence unavailable. No storage Snapshot,
Checkpoint or Flush is added. The existing diagnostic-file write/sync remains.

Independent Linux ARM64 Go 1.27 SIMD tests pass: normal 0.851s, race 2.799s.
They ran from frozen `6402842c` plus only the four reviewed diagnostics files,
excluding the unfinished storage experiment. Both real regressions ran without
skips: sampling preserved a pending overlay and all storage/I/O counters, and
real schema recovery selected generation 2 over a closed generation 1 while
checking dynamic provider coverage and concurrent closure.

`commands.txt` records the image identity, frozen source, environment and exact
invocations. The complete verbose logs, source hashes and review are retained;
`evidence-sha256.json` covers those evidence files. No database files, manifests
or credentials are included.

Counters sum currently open collection generations; group or schema changes
can reset them within one PID. Comparisons require a stable inventory and must
reject incomplete coverage or decreases. Node aggregates do not identify a
particular table's fold work. This is measurement support, not performance,
process-level reload or batch-overlay qualification.
