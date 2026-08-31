# Performance evidence

> [!CAUTION]
> VibeDB is unreleased; APIs, commands, wire/disk formats, and results may change
> at any commit. Use one exact source/docs pair and disposable data. This
> repository publishes no competitive result: a benchmark command, coverage
> table, CI artifact, or validated inventory is not a speed, cost, memory,
> storage, or scaling claim.

Use this page to choose the right measurement and to keep a result
reproducible.

## Choose the evidence

| Question | Tool | What it establishes |
| --- | --- | --- |
| Did a hot path allocate more? | `go run ./bench/gate` | Same-machine `allocs/op` and `B/op` comparison to a Git base; never gates time |
| How does one embedded workload behave? | `bench/competitive/cmd/mixed` | One isolated engine/process workload with latency, throughput, memory, and disk observations |
| Are repeated mixed runs comparable? | `cmd/mixedsuite` | Deterministic execution order; its current summary TSV is not machine-safe (see below) |
| What is the file/RSS footprint? | `cmd/footprint` | Apparent bytes, allocated blocks, Go memory, RSS, and corpus metadata |
| Does a live set grow on disk? | `cmd/churndisk` | Bounded churn/storage samples; nonstandard shapes are diagnostic |
| Where does one host/workload saturate? | `cmd/saturation` | Host-specific plateau search, not universal capacity |
| What does a held snapshot cost? | `cmd/snapshotpressure` | Matched control versus pinned-snapshot pressure |
| What is SQL interface overhead? | `cmd/sqlsurface` | VibeDB/SQLite embedded SQL or VibeDB pgwire shapes; not a PostgreSQL server comparison |
| How do open/cold/recovery paths behave? | `cmd/lifecycle` | Isolated lifecycle stages with explicit timer boundaries |
| Can the corpus exceed RAM safely? | `cmd/outofram` | Qualification of streaming/bounds, not a load-speed result |
| Does RF3 survive the defined faults? | `go run ./bench/rf3chaos` | Canonical external-fault evidence for one exact build and scenario |

`bench/competitive/COVERAGE.md` is generated from a 38-cell harness manifest.
“Implemented” there means that an executable harness and oracle exist. It does
not mean the product surface is complete or that the cell has been measured.

## Allocation gate

```bash
go run ./bench/gate
go run ./bench/gate -base <commit>
```

The curated gate rejects any gated `allocs/op` increase and a `B/op` increase
greater than five percent. It neither reads, reports, nor gates `ns/op`. The
local default compares with the merge base of `main`; pull-request CI supplies
the exact PR base.

### Known `mixedsuite` output defect

The current summary header declares fewer grouping columns than each summary
row emits. The runner's execution order is deterministic, but downstream tools
must not treat its summary TSV as a stable machine-readable schema until that
defect is fixed. Raw per-run evidence remains the safer input.

## Publication requirements

A result intended for comparison must record:

- exact commit, dirty state, toolchain, dependency lockfiles, and command;
- host CPU, memory, OS, architecture, filesystem, device, mount/controller, and
  power/durability assumptions;
- engine versions, configuration, compression/storage profile, cache budget,
  indexes, and acknowledgement contract;
- corpus generator/digest, count, document shape, cardinalities, seed, and
  logical bytes;
- clients, operations, mix, warmup, conditioning, checkpoints, and timer
  boundaries;
- per-run raw rows, repetitions, failure/skip rows, and summary method;
- for RF3: process/node/group/replica/shard topology and which byte/latency/RSS
  cuts were actually observed.

Keep each summary row linked to immutable raw evidence. Never remove slow or
failed repetitions without a predeclared exclusion rule.

The publication validator checks artifact inventory, exact revision, selected
metadata, and canonical receipt digests. It assumes a trusted, immutable
evidence directory while validation runs: files are reopened for hashing,
symlinks are followed, and receipt publication is not a directory-fsync
protocol. The receipt is therefore neither tamper-proof nor proof that a crash
made the directory entry durable. It also does **not** independently prove every
runner flag, workload equivalence, hardware fairness, or a comparative
conclusion. Qualification mode uses a smaller CI-only inventory and cannot be
promoted to a publication result.

## Comparison rules

- Match acknowledgement durability before comparing write latency.
- Name cache/accounting differences. For example, bbolt maps the complete
  database and cannot honor the nominal engine cache; the SQLite adapter uses
  one open connection.
- Plain KV engines do not execute exact-index lanes. Only compare a lane that
  both engines implement with equivalent semantics.
- A setting named `production` in the harness only selects an optional
  compression profile; it says nothing about product maturity.
- `BenchmarkScan` touches one byte per document and cannot support a
  bytes-throughput claim; use full-byte scan evidence.
- Do not use single-group in-process RF3 latency as horizontal scaling evidence.
- Do not translate whole-harness elapsed time into election, failover, recovery,
  or foreground latency when the protocol does not expose those cuts.

## Current result state

[`bench/competitive/RESULTS.md`](../bench/competitive/RESULTS.md) intentionally
contains no number: there is no checked-in immutable evidence bundle or
validated competitive summary. CI uploads claim-free raw qualification
artifacts for limited retention; it does not publish a result registry.

## Source map

- `bench/gate/main.go`
- `bench/competitive/internal/coverage/manifest.go`
- `bench/competitive/cmd/*`
- `bench/competitive/cmd/publishcheck`
- `bench/rf3chaos/main.go`
- `scripts/bench/run-ci-competitive-evidence.sh`
