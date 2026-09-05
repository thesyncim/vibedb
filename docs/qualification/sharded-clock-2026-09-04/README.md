# Sharded transaction clock correctness review

Incoming main `ea8dfc0f` adds striped transaction registration, per-collection
history lookup, reduced commit allocations, and cached collection handles.
The corrected main source is `82ea6abfcf51de01745a99609d5ffb0cbbb828d0`.
The same fix is on the fused-node branch at `4eb0cafb`.

`incoming-main-regressions.log` was produced from an archive of incoming main
with three deterministic scheduling hooks and the regression tests added,
without the logic fixes. Eight new regressions and the restored saturation
transition test fail. Three failures show a transaction committing after a
conflicting write because quiescence, directory overflow, or a delayed revision
record erased its conflict history. Other failures expose a pruning hint ahead
of registration, wrapped holder counts/revisions, and an escaped finished state
retaining its database handle.

The correction keeps striped registration and concurrent disjoint work. A
shared coordination gate prevents history records and registrations from
crossing exclusive resets; validation checks the conservative guards again
before accepting. Registration samples revisions inside its stripe. Overflow
covers all reserved revisions, counters saturate permanently, and finished
states clear cached handles. The allocation reductions remain.

Validation used Go 1.27 on Darwin arm64:

- `fixed-scoped-race.log`: `go test -race . ./store -count=1` on the redesign
  checkout; root and store pass.
- `fixed-root-final-race.log`: root race rerun after the final saturation
  pruning correction; passes.
- `main-clean-root-race.log`: `go test -race . -count=1` on the clean isolated
  main checkout at the corrected source above; passes in 13.125 seconds.

The retained logs are ordinary test output. `sha256.json` hashes their exact
bytes. These tests establish the covered transaction invariants; they do not
measure throughput or qualify fused-node distributed failure/recovery behavior.
