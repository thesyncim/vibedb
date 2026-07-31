# Contributing

vibedb changes must preserve storage correctness, snapshot isolation,
durability, bounded memory, and format validation before improving a benchmark.
The repository targets Go 1.26 and is unreleased. Format changes edit version
0 in place, and every format or API break must leave the current tree
internally consistent.

## Start focused

Run the smallest package and test that exercise the change while iterating:

```sh
go test ./internal/storeio -run 'TestName' -count=1
go test ./store/durable -run 'TestName' -count=1
go test ./query -run 'TestName' -count=1
```

Use `-count=1` when cached success would hide the run. Add `-race` for
concurrency, snapshot, cache, combiner, or lease changes. Storage failures need
the existing fault-injection and reopen oracles, not only a successful happy
path.

## Before handing off

The local equivalent of the main CI lane is:

```sh
go build ./...
go vet ./...
go test -timeout=25m ./...
go test -race -timeout=25m \
  -run 'Primary|BufferedInplace|Committer|PageCache|WriteTransaction' \
  ./internal/storeio/ ./store/durable/
git diff --check
```

CI runs these checks on linux/amd64 and linux/arm64. Platform-specific I/O
changes also require the affected platform: Darwin durability work must prove
the `F_FULLFSYNC`/`F_BARRIERFSYNC` path; Linux direct-I/O or io_uring work must
prove fallback and required-mode behavior.

The nested competitive harness is a separate module and is not covered by
`go test ./...` at the root:

```sh
cd bench/competitive
go test -run 'TestFullEquivalence|TestCorpusVariantsAreShapeMatched' \
  -count=1 -timeout=60m .
```

Do not add competitor dependencies to the root `go.mod`.

## Test discipline

Add the smallest permanent test that proves the contract:

- Codec changes need byte-exact golden images, checksum-valid semantic
  corruption cases, reserved-zero validation, and truncation bounds.
- COW or publication changes need failure injection before and after each
  write/barrier, reopen, and previous-generation fallback.
- Canonical materialization needs every capsule/data/root crash cut and
  idempotent second recovery.
- Snapshot or reclamation changes need held-old-generation tests, bounded
  pressure, release/retry, and race coverage.
- Index changes need hash-collision candidates, exact-value rechecks, aliases,
  mutation maintenance, and scan differentials.
- Optimized read/write routes need allocation assertions and a structural
  oracle such as page acquisitions, device bytes, or route selection.
- Query changes need heap/durable differential coverage and reusable-workspace
  tests where the API promises a warmed allocation boundary.

A benchmark is not a correctness test. Keep a deterministic oracle beside each
new measured phase.

## Storage-format changes

[docs/format.md](docs/format.md) is the readable specification; codecs are
authoritative. Change them together. Preserve or regenerate byte-exact fixtures
under `internal/storeio/testdata/format0`, and make obsolete or malformed
development layouts fail closed. Do not add compatibility branches.

Update [docs/architecture.md](docs/architecture.md) when a representation or
root graph changes, and [docs/durability.md](docs/durability.md) when an
acknowledgement, checkpoint, platform barrier, or recovery window changes.

## Benchmark discipline

Follow [docs/performance.md](docs/performance.md). In particular:

- compare the same commit pair, compiler, CPU, OS, filesystem, corpus, seed,
  duration, writer count, and durability lane;
- report time, bytes, allocations, device I/O, and tail latency relevant to the
  changed path;
- use repeated isolated samples and medians;
- run correctness before performance;
- label every value measured, projected, or a gate;
- never replace a database-level table with an isolated primitive result.

Cross-engine figures have one authoritative home:
[bench/competitive/RESULTS.md](bench/competitive/RESULTS.md). Record the exact
commit, dirty state, machine, Go version, corpus variant, mode, and sampling
method there.

## Adding a research phase

A phase is a bounded experiment that may graduate into the sole production
path. Add it in this order:

1. State the idea, current status, and invariant it must preserve in one design
   document under `docs/design/`.
2. Define explicit correctness, read-path, space, allocation, and latency
   gates before implementing the lab.
3. Build the smallest isolated codec or routing experiment under
   `internal/storeio`, with differential and corruption tests.
4. Record every measured number with commit, machine, input shape, repetition,
   and units. Keep projections labeled.
5. Integrate reads first when the phase changes representation; stop if the
   standing read gates fail.
6. Integrate mutation and recovery paths, then run the crash, snapshot, churn,
   and whole-file matrices.
7. Promote one representation and remove obsolete paths. Do not leave a
   permanent reader-visible fallback or overlay to rescue a failed gate.

After a decision, rewrite the design record to describe only the current path
and remaining work. Delete rejected experiment code, dead plans, and stale
benchmark claims.

## Ownership and unsafe code

The caller owns files and explicit snapshot leases. Returned borrowed bytes
must not outlive their documented owner. Keep Go pointers out of external
memory, do not retain pointers as `uintptr`, and preserve pointer-free durable
and mmap-backed layouts.

Review [UNSAFE.md](UNSAFE.md) before changing an unsafe scope and update it only
when the inventory or its links actually change. Preserve
[docs/provenance.md](docs/provenance.md) and its evidence requirements for
externally derived algorithms or source.

## Documentation links

Use relative in-repository links and run the repository link sanity check
described in the task or review. A moved design document requires updating
every inbound link in the same change.
