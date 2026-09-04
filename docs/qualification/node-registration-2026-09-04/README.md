# Shared-node registration qualification

Fresh groups can be prepared while their node owns its log with
`prepare-node-group-rf3 -manifest PATH`. Reload verifies the SQL identity,
bootstrap publication, roster and canonical snapshot before submitting durable
registration through the shared append sequencer. Existing descriptors must
match the binding; retry never resets a newer log or incarnation.

`cluster dev --node-log` prepares fresh nodes and uses this path for newly created
tables. The CRDB comparison runner accepts the same flag. This commit has not yet
completed the end-to-end benchmark smoke run; no performance gain is claimed.

## Checks

Go 1.27 on macOS arm64: full `go test` passed for `internal/raftstore`,
`internal/raftmember`, `cmd/vibedb`, and `cmd/vibedb-shard` (23.974 s, 11.239 s,
5.191 s and 7.921 s respectively).

Linux arm64 on a named Docker volume, `TMPDIR=/data`, 8 CPU cap, Debian
bookworm-slim digest `88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171`:

- Initial shared-log preparation and runtime ownership: pass (1.47 s).
- Three serving instances start with two RF3 groups each, prepare/register a
  third group through live reload, then all stop/restart and elect leaders:
  pass (8.77 s). These are instances in one test process, not three machines.
- Seventeen initial groups crossing the creation-wave bound: pass (3.14 s).
- Foreign SQL/bootstrap rejection, durable registration and retry preserving
  newer history/incarnation: pass (0.07 s).
- Restart with 128 appended entries and a 64-entry segment index bound:
  pass (0.44 s). The node capacity format now distinguishes per-segment bounds
  from total history; legacy immutable-base capacity checks remain strict.
- All three prepared/committed/applied schema recovery boundaries: pass (0.72 s).

The last three cases are in `rf3_node_registration_linux_test.go` and
`rf3_node_schema_recovery_linux_test.go`; serving cases are in
`prepare_node_rf3_linux_test.go`. No Linux test above was skipped.

Process-crash cuts during live admission, acknowledged-write fault histories,
node-log replica movement/splitting and sustained reclamation remain unqualified.
No performance-goal acceptance gate is complete.
