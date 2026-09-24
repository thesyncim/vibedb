# Testing strategy

[Documentation](../README.md) / [Developer guide](README.md)

VibeDB's tests go from pure-function unit tests to ten-process fault
qualifications that CI runs on dedicated jobs. Choose the cheapest layer that
can observe the contract you are changing. Then add the layers the
[evidence table](../../CONTRIBUTING.md#match-evidence-to-the-change) requires.

## Layers

| Layer | What it proves | Where it lives | How it runs |
| --- | --- | --- | --- |
| Unit and table tests | Codec grammar, validation, planner decisions, bounded-queue arithmetic | Next to the code in every package | Unit shards on x86-64 and arm64 |
| Differential oracles | An optimized path equals a simple one: SIMD vs portable, heap vs durable, VibeDB vs a reference implementation | For example `internal/raftstore/seglog_differential_test.go`, the `query` recursive-CTE fuzz differentials, `x/vitessroute/differential_test.go`; 66 test files reference differential checks | Unit shards; SIMD job repeats with `nosimd` and AVX2 disabled |
| Fuzz targets | Decoders reject every malformed input and round-trip every valid one | 73 files define `Fuzz*` targets, mostly `query`, `internal/replicatedstate`, `internal/storeio`, `sql` | Seed corpora run as ordinary tests; nothing fuzzes continuously in CI |
| Crash-point and fault-device tests | On-disk state after a crash at each write or barrier recovers to exactly the old or the new generation | `internal/storeio` (`FaultDevice`, `FaultJournal`), `store/durable`, `internal/clusterrestore` (`FaultPoint`) | Unit shards; storage race lane |
| Deterministic Raft simulation | Raft integration under replayable schedules and faults, without wall time | `internal/raftsim`, `internal/raftmodel`, `internal/raftstore` integration tests | Core shard |
| `testing/synctest` bubbles | Timer, retry, and backoff behavior with virtual time | `internal/raftservice`, `internal/gatewayruntime`, `gateway`, `cmd/vibedb-shard` | Unit shards |
| Allocation budgets | Hot paths stay allocation-free or within a stated budget | `testing.AllocsPerRun` in 385 test files; `bench/gate` for curated benchmarks | Unit shards; `bench-gate` workflow |
| Race and checkptr | No data race; unsafe pointer arithmetic stays within bounds | Selected packages | Storage, distributed, and LATERAL race lanes; `packed-simd` checkptr |
| In-process multi-group | Raft groups, owners, and transports in one test binary with injected partitions and response loss | `internal/raftservice`, `internal/multiraft`, `gateway` | Unit shards; some gated with `VIBEDB_DURABLE_SQL_RF3_E2E` |
| External-process qualifications | Shipped `vibedb`, `vibedb-shard`, and `vibedb-gateway` binaries under `SIGKILL`, `SIGSTOP`, TCP partitions, and restarts, with resource bounds | Mostly `internal/gatewayruntime` and `cmd/vibedb-shard`, Linux-only | Environment-gated; dedicated jobs reject skips |
| Kubernetes | Rendered manifests, image, and rolling restart on Kind | `deploy/kubernetes/qualify-kind.sh` | `ci` Kubernetes job |
| Compatibility corpora | PostgreSQL clients and the upstream regression corpus | `integration/pgclient`, `integration/pgcompat` | `ci` recovery job; nightly `postgresql-compatibility` |

## Durable storage: crash-point enumeration

`storeio.FaultDevice` models the commit protocol exactly: data pages, a
barrier, the alternate root, and a final barrier. A clean probe pass
(`FaultNone`) records the write sequence. Each crash point is then replayed
(`FaultAfterDataWrite`, `FaultAfterBarrier`, `FaultAfterRootWrite`,
`FaultAfterFinalSync`, `FaultTornRoot`, `FaultDropDataThenApply`, and three
`ENOSPC` variants). The test reopens the image and asserts which generation
won. The recovery journal has the same structure through `FaultJournal`.

When you add a write or barrier, add its crash point and a reopen assertion.
A test that only checks the happy path, or that checks recovery without
reopening, does not meet the persistence evidence bar.

## Distributed code: identity and ambiguity

Most distributed defects surface as a retry that changes identity or a
response that is lost after the command commits. In-process tests inject
response loss and leader partitions and then check two things:

- the retry reuses the original request identity and bytes and gets the
  original outcome; and
- a fresh request is not treated as a retry.

See the retry rules in [coding conventions](conventions.md#exact-once-identity).

External-process tests reach the shipped binaries through a TCP proxy per peer
link (`rf3PeerProxy` in `internal/gatewayruntime`). They block or delay links,
send `SIGKILL` and `SIGSTOP`, restart processes on retained state, and replace
gateways. Each qualification prints one summary line of `key=value` facts,
such as `leader_kill=true`, `p99=…`, and `rss_growth=…`. Its workflow requires
every fact to be present, so a scenario cannot silently drop out.

## Virtual time with synctest

`testing/synctest` (`synctest.Test`, `synctest.Wait`) runs a test in a bubble
whose clock advances only when every goroutine is blocked. Use it for retry
schedules, backoff, lease expiry, and "eventually" loops that would otherwise
need real sleeps. Code under test must not block on real I/O inside the bubble;
network and disk tests stay outside it. Examples:
`internal/raftservice/schema_generation_delivery_test.go` and
`internal/gatewayruntime/seamless_scale_retry_test.go`.

Do not add `time.Sleep` to "stabilize" a test. Wait for an explicit condition
with a deadline. Otherwise, move the timing logic into a bubble.

## Fuzzing

Fuzz targets double as regression tests: `go test` runs their seed corpus.
The only checked-in generated corpus is `sql/testdata/fuzz`. To fuzz locally:

```sh
GOEXPERIMENT=simd go test -run '^$' -fuzz '^FuzzOpenCommand$' -fuzztime 60s ./internal/replication
```

A crash writes a reproducer under the package's `testdata/fuzz/<Target>/`.
Commit the reproducer with the fix. No workflow runs coverage-guided fuzzing,
so a new decoder needs a fuzz target and a local fuzzing session before review.

## Qualification tests

A qualification is a named, repeatable validation workflow with fixed
thresholds. It fails closed. All of the following must hold:

- The test must exist (`go test -list`), pass the requested number of times
  (often `count=3`), and emit no `skip`.
- Its summary line or TSV evidence must contain every required fact within
  the stated bound.
- Raw evidence is uploaded whether the run passes or fails, usually with
  30-day retention. The `Dev hot-shard split qualification` workflow is an
  exception: it uploads nothing and keeps only the job log.

Performance overrides turn the result into a diagnostic. For example, any
`VIBEDB_SCALE_*` variable clears the seamless-scale `Strict` flag. A relaxed
bound must never silently become acceptance. See the
[qualification index](../qualification/README.md) for every workflow.

## Known weaknesses

- **Linux-only coverage.** Strict allocation, `/proc` accounting, and most
  process qualifications run only on Linux. Only the `Native RF3 SQL and
  restart` job exercises macOS in CI.
- **Shared runners.** Latency and throughput bounds in CI are set for noisy,
  shared four-core runners. They catch stalls and gross regressions, not
  small performance changes. Product latency targets (the seamless-scale
  `dedicated` profile) need controlled hardware that CI does not have.
- **Short qualification history.** Several workflows are new. For example,
  seamless scale first passed on `main` on 2026-09-23. A pass is evidence
  for that revision and scenario only.
- **No continuous fuzzing, and single-host chaos.** Faults are injected by
  proxies and signals on one host. There is no multi-host network, disk-fault,
  or clock-skew testing beyond the
  [clock-fault matrix](../qualification/README.md#qualification-workflows),
  which declares `live_process_utc_step not_injected`.
- **Nested modules.** No workflow runs the `x/vitessroute` tests.

## Source map

- [internal/storeio/device_fault.go](../../internal/storeio/device_fault.go), [internal/storeio/recovery_journal_fault.go](../../internal/storeio/recovery_journal_fault.go)
- [internal/raftsim/doc.go](../../internal/raftsim/doc.go)
- [internal/gatewayruntime/rf3_peer_proxy_test.go](../../internal/gatewayruntime/rf3_peer_proxy_test.go)
- [internal/gatewayruntime/seamless_scale_evidence_test.go](../../internal/gatewayruntime/seamless_scale_evidence_test.go)
- [scripts/ci/clock-fault-matrix.sh](../../scripts/ci/clock-fault-matrix.sh)
