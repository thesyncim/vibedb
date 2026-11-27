# Testing strategy

[Documentation](../README.md) / [Developer guide](README.md)

Choose the cheapest test layer that observes the contract you are changing, and
satisfy the [evidence table](../../CONTRIBUTING.md#match-evidence-to-the-change).

## Test layers

| Layer | Proves | Where |
| --- | --- | --- |
| Unit & table tests | Codec grammar, validation, planner, bounded queues | Every package |
| Differential oracles | SIMD vs portable, heap vs durable, Vitess golden vectors | `storeio`, `query`, `raftstore`, `x/vitessroute` |
| Fuzz targets | Malformed-input rejection & valid round-trips (73 `Fuzz*` files) | `query`, `replicatedstate`, `storeio`, `sql` (run locally; seed corpora run in CI) |
| Crash-point & fault devices | Atomic recovery across every write/barrier/sync + reopen | `storeio` (`FaultDevice`, `FaultJournal`), `store/durable`, `clusterrestore` |
| Deterministic Raft sim | Replayable Raft faults without wall time | `internal/raftsim`, `raftmodel`, `raftstore` |
| `testing/synctest` bubbles | Virtual-time retries, backoffs, leases without `time.Sleep` | `raftservice`, `gatewayruntime`, `gateway`, `cmd/vibedb-shard` |
| Allocation & race / `checkptr` | Zero/bounded allocs (`testing.AllocsPerRun`, `bench/gate`); race-free & safe `unsafe` | Storage, distributed, LATERAL race lanes; `packed-simd` |
| External-process qualifications | Shipped binaries under `SIGKILL`/`SIGSTOP`/TCP proxies (`rf3PeerProxy`) with resource bounds | `internal/gatewayruntime`, `cmd/vibedb-shard` (Linux CI gates reject skips) |
| Kubernetes & PG clients | Kind rolling restarts; `pgx`/`lib/pq`/`psql` & PG 18.6 corpus | `deploy/kubernetes`, `integration/pgclient`, `integration/pgcompat` |

## Core testing rules

- **Crash points:** Any new disk write or barrier needs a `FaultDevice` /
  `FaultJournal` crash point and a full reopen assertion.
- **Ambiguous distributed outcomes:** Inject reply loss and leader partition,
  then assert that retrying the exact identity returns the original outcome and
  a fresh request is not treated as a retry.
- **No `time.Sleep` stabilization:** Wait on observable conditions with a
  deadline, or move timer logic into a `testing/synctest` bubble.
- **Fuzzing locally:** Run `GOEXPERIMENT=simd go test -run '^$' -fuzz '^FuzzName$' -fuzztime 60s ./pkg`
  for new decoders and commit any crash reproducer under `testdata/fuzz/`.
- **Qualifications fail closed:** CI verifies the test exists (`go test -list`),
  passes `count` times with zero skips, and meets all summary/TSV bounds.
  Overrides (e.g. `VIBEDB_SCALE_*`) drop the `Strict` flag so relaxed runs
  never pose as qualification passes ([qualification workflows](../qualification/README.md#standalone-workflows)).

## Source map

- [internal/storeio/device_fault.go](../../internal/storeio/device_fault.go) · [recovery_journal_fault.go](../../internal/storeio/recovery_journal_fault.go)
- [internal/raftsim/doc.go](../../internal/raftsim/doc.go) · [internal/gatewayruntime/rf3_peer_proxy_test.go](../../internal/gatewayruntime/rf3_peer_proxy_test.go)
