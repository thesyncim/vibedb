# Build and test

[Documentation](../README.md) / [Developer guide](README.md)

This page covers building VibeDB, running its tests the way CI does, and
reproducing a Linux-only failure from macOS. Run every command from the
repository root unless a step says otherwise.

## Prerequisites

| Tool | Needed for | Notes |
| --- | --- | --- |
| Go 1.27 | Everything | `go.mod` declares `go 1.27`; CI pins `1.27`. |
| `GOEXPERIMENT=simd` | Every build that should match CI | Make targets and `scripts/ci/*.sh` set it; raw `go` commands do not. `go.mod` cannot enable a compiler experiment. |
| Linux on ext4 | Strict-allocation durability tests and most process qualifications | See [Linux-only tests](#linux-only-tests). |
| `jq`, GNU coreutils | Replaying a workflow's evidence validator | Workflow scripts use `jq` and `stat -c`. |
| Docker | Reproducing Linux lanes from macOS; `psql` client lane; Kind lane | Optional. |
| Python 3 | CI script self-tests and the docs checker | The docs checker needs the packages in `scripts/docs/requirements.txt`. |

Use the standard Go build cache (`go env GOCACHE`). Do not point `GOCACHE` at a
per-task or in-worktree directory.

## Make targets

The Makefile exports `GOEXPERIMENT` (default `simd`) to every recipe.

| Target | Runs | Variables |
| --- | --- | --- |
| `make build` | `go build $(PACKAGES)` | `PACKAGES` (default `./...`) |
| `make test` | `go test $(TEST_FLAGS) $(PACKAGES)` | `PACKAGES`, `TEST_FLAGS` (default empty) |
| `make vet` | `go vet $(PACKAGES)` | `PACKAGES` |
| `make bench` | `make info`, then a small public-operation benchmark set at `-cpu=1` | `BENCH_PACKAGES`, `BENCH`, `BENCHTIME` (250ms), `COUNT` (1) |
| `make info` | `go version` and `go env -json GOEXPERIMENT GOOS GOARCH GOAMD64` | none |
| `make docs-check` | `scripts/docs/check.py` | `PYTHON` (default `python3`) |

Override the compiler with `GO=/path/to/go`. Select the portable build with
`GOEXPERIMENT=nosimd`:

```sh
make info
make test PACKAGES=./distribution TEST_FLAGS='-run ^TestName$ -count=1'
make test GOEXPERIMENT=nosimd PACKAGES=./distribution TEST_FLAGS='-short -count=1'
```

A plain `make test` runs the whole root module serially per package with the
default 10-minute per-binary timeout. The durable, SQL, and process packages are
long; prefer `TEST_FLAGS='-p=1 -timeout=25m'` for a full local run, or use the
CI shards below.

Nested modules are not covered by `./...`. Run them from their own directory,
as listed in [Contributing](../../CONTRIBUTING.md#nested-modules).

## The CI workflow at a glance

The main `ci` workflow runs on every pull request and every push to `main`.
Recent `main` runs have 28 jobs:

| Job | What it runs | Runners |
| --- | --- | --- |
| `unit (…, durable \| durable-churn \| durable-large-cache \| process \| core)` | `scripts/ci/test-shard.sh <shard>` | `ubuntu-latest` (x86-64) and `ubuntu-24.04-arm` |
| `unit (…, sql)` | `scripts/ci/test-shard.sh sql` | both |
| `race storage (…)` | `scripts/ci/storage-race.sh all` | both |
| `race distributed` | `go test -race -p=4` over `gateway`, `internal/multiraft`, `internal/snapshottransfer`, `internal/raftservice`, `internal/replicatedstate` | x86-64, 35-minute limit |
| `SIMD planner and query (…)` | `nosimd` parity, AVX2-disabled fallback (x86-64 only), portable JSON and placement | both |
| `cross-compile (…)` | `go test -run '^$' -exec=true ./...` for `linux/386`, `windows/386`, `windows/amd64`, `windows/arm64`, plus `integration/pgclient` | x86-64 |
| `Native RF3 SQL and restart (…)` | Portable sidecar, ownership, restart, and three-process replica tests | `ubuntu-latest` and `macos-latest` |
| `repository contracts` | actionlint, CI script self-tests, stale generated files, Kubernetes manifest determinism, restore activation cuts, focused LATERAL race tests | x86-64 |
| `RF3 read-authority default and protocol` | Read-authority selection, physical preparation, protocol, and live-enrollment tests | x86-64 |
| `Recovery, replica replacement and PostgreSQL clients` | Durable SQL RF3 recovery (`count=3`), automatic replica replacement (`count=3`), pgx/lib/pq, `psql` 18.4 | x86-64 |
| `Hot-shard and transport qualification` | Write-driven hot-shard move, `bench/rf3chaos`, authenticated transport faults, RF3 quorum cuts | x86-64 |
| `Kubernetes RF3 serving and restart qualification` | `deploy/kubernetes/qualify-kind.sh` on Kind v0.32.0 | x86-64, 45-minute limit |

Separate qualification workflows are listed in the
[qualification index](../qualification/README.md#qualification-workflows).

Things to know about CI results:

- The `ci` concurrency group cancels superseded runs for the same pull request
  or branch. A `cancelled` conclusion usually means a newer push, not a failure.
- GitHub-hosted runners are shared four-core machines, and noisy neighbors move
  absolute latency. Process qualifications' latency bounds are set to pass on
  these runners. The seamless-scale qualification has a separate
  `shared-runner` profile for this reason (see the
  [qualification index](../qualification/README.md#seamless-scale-profiles)).
  Wall-clock time also depends on repository-wide runner queueing; see the
  [CI performance record](../ci-performance.md).
- In the recorded `main` run for `47ac5ab23` (2026-09-23), the slowest jobs were
  `race storage (ubuntu-latest)` (about 8 minutes), `race distributed` (about
  5.5 minutes), and `unit (ubuntu-latest, core)` (about 4.5 minutes, including
  full `go vet ./...`).

## Reproduce a unit shard

`scripts/ci/test-shard.sh` discovers packages with `go list ./...` on each run
and assigns each package to exactly one shard:

| Shard | Packages | Package workers | Filter |
| --- | --- | ---: | --- |
| `durable` | `store/durable` | 1 | skips the two pressure qualifications |
| `durable-churn` | `store/durable` | 1 | only `TestFilePrimaryChurnQualification` |
| `durable-large-cache` | `store/durable` | 1 | only `TestFilePrimaryLargerThanCacheQualification` |
| `sql` | `query`, `store`, `sql/driver`, `pgwire` | 4 | none |
| `process` | `cmd/vibedb-gateway`, `cmd/vibedb-shard`, `internal/gatewayruntime` | 2 | none |
| `core` | everything else (85 packages at this revision), then `go vet ./...` | 4 | none |

```sh
scripts/ci/test-shard.sh process --list
scripts/ci/test-shard.sh sql
```

The script emits `go test -json` output with `-timeout=25m`. The `core` shard runs
vet even when tests fail and exits with the test status first. The shard
partition and the storage-race selectors have their own self-tests:

```sh
python3 -m unittest discover -s scripts/ci -p '*_test.py'
```

## Race lanes

| Lane | Local command |
| --- | --- |
| Storage race (both architectures in CI) | `scripts/ci/storage-race.sh all`, or one of `storeio`, `durable-heavy`, `durable-rest` |
| Distributed race | `GOEXPERIMENT=simd go test -race -p=4 -count=1 -timeout=30m ./gateway ./internal/multiraft ./internal/snapshottransfer ./internal/raftservice ./internal/replicatedstate` |
| LATERAL race | `GOEXPERIMENT=simd go test -race -p=3 -count=1 -timeout=10m -run 'Lateral' ./query ./sql/driver ./pgwire` |
| Packed SIMD checkptr | The `packed-simd` workflow adds `-race -gcflags=all=-d=checkptr=2` for `internal/storeio` SIMD tests |

The storage lane selects tests matching
`Primary|CompactRankAffine|BufferedInplace|Committer|PageCache|WriteTransaction`
and skips every `Qualification` test. Qualification corpora are multi-hundred-MiB
geometry gates. Under `-race` on ARM they can use the whole time budget.
The distributed race lane also excludes external-process corpora; those run
unskipped in their own Linux jobs.

Race instrumentation multiplies memory use. On a laptop, run one race package
at a time.

## SIMD and portable builds

VibeDB uses the Go 1.27 `simd` experiment for packed column kernels, checksums,
text folding, and Bloom insertion. Each kernel has a portable fallback, and CI
checks both:

| Mode | How CI selects it |
| --- | --- |
| Native SIMD | `GOEXPERIMENT=simd` (global default) |
| Portable build | `GOEXPERIMENT=nosimd` in the SIMD job |
| SIMD build without AVX2 | `GOAMD64=v1 GOEXPERIMENT=simd GODEBUG=cpu.avx2=off` on x86-64 |
| Required native AVX2 | `VIBEDB_TEST_REQUIRE_AVX2=1` in the x86-64 `durable` shard and `packed-simd` workflow; the dispatch tests fail instead of passing on the fallback |

Apple silicon and `ubuntu-24.04-arm` use the NEON paths. The AVX2 paths run
only on the x86-64 runners, so an arm64 laptop cannot reproduce an AVX2-only
failure. Use an x86-64 Linux machine for that.

See [SIMD](../simd.md) for the kernel inventory.

## Cross-compilation

```sh
CGO_ENABLED=0 GOOS=windows GOARCH=386 GOEXPERIMENT=simd go test -run '^$' -exec=true ./...
```

`-exec=true` compiles every test binary without running it. The 32-bit targets
catch integer-width, alignment, and build-tag errors that 64-bit hosts miss.

## Linux-only tests

At this revision, 55 test files carry `//go:build linux`. Most are in
`internal/gatewayruntime`, `cmd/vibedb-shard`, `store/durable`, and
`internal/storeio`. Others need Linux at run time:

- **Strict physical allocation.** `storeio.StrictlyAllocateFile` proves private
  physical backing with `fallocate` plus `FALLOC_FL_UNSHARE_RANGE`. It accepts
  `EOPNOTSUPP` only on ext4, and returns `ErrStrictAllocationUnsupported` on
  every non-Linux platform. Durable RF3 tests skip when the proof is
  unavailable. Their qualification gate turns that skip into a failure. CI
  jobs preflight `fallocate` on the runner's temporary directory for this
  reason.
- **`/proc` and block accounting.** RSS, file-descriptor, and physical-byte
  bounds read `/proc/self` and `stat` block counts.
- **GNU tools in scripts.** `scripts/ci/clock-fault-matrix.sh` and the workflow
  validators use `stat -c`, which BSD `stat` on macOS rejects.

A filtered run of a build-tagged test on macOS can pass without running
anything:

```text
$ GOEXPERIMENT=simd go test -run '^TestGatewayDurableRF3ExternalProcessRecovery$' ./internal/gatewayruntime
ok  	github.com/thesyncim/vibedb/internal/gatewayruntime	1.039s [no tests to run]
```

The workflows guard against this. Before the real run, they check that the test
exists with `go test -list '^Name$' <pkg>`. Afterwards, they count `pass` events
in the `-json` stream and reject any `skip`. Apply the same checks to local
evidence.

## Process and qualification gates

Expensive process tests call `t.Skip` unless an environment gate is set. The gate
also turns a missing prerequisite into a failure where the test would otherwise
skip it:

| Gate | Test | Package |
| --- | --- | --- |
| `VIBEDB_DURABLE_SQL_RF3_E2E=1` | `TestTwoGatewayDurableSQLRF3RecoversTerminalAndAckAcrossLeaderPartitions`, `…UnfinishedWaveWithDefaultPinSpan` | `internal/raftservice` |
| `VIBEDB_RF3_QUORUM_QUALIFICATION=1` | `TestRF3AllThreeVoterQuorumCutsFailClosedOrCommit` | `internal/raftservice` |
| `VIBEDB_REPLICA_REPLACEMENT_E2E=1` (+ `…_EVIDENCE=dir`) | `TestGatewayAutomaticReplicaReplacementProcesses` | `internal/gatewayruntime` |
| `VIBEDB_HOT_SHARD_MUTATION_E2E=1` | `TestGatewayHotShardMutationProcesses` | `internal/gatewayruntime` |
| `VIBEDB_DURABLE_RF3_PROCESS_E2E=1` | `TestGatewayDurableRF3ExternalProcessRecovery` | `internal/gatewayruntime` |
| `VIBEDB_READ_BATCH_RF3_PROCESS_E2E=1` | `TestGatewayReadBatchRF3ExternalProcessChaos` | `internal/gatewayruntime` |
| `VIBEDB_DURABLE_RF3_MULTIRELATION_E2E=1` | `TestGatewayDurableRF3MultiRelationChaosProcess` | `internal/gatewayruntime` |
| `VIBEDB_FUSED_RF3_PROCESS_E2E=1` | `TestFusedRF3NodeProcessQualification` | `internal/gatewayruntime` |
| `VIBEDB_SEAMLESS_SCALE_E2E=1` (+ `…_PROFILE`, `…_EVIDENCE`, `…_FAILURE`) | `TestSeamlessScaleInOutProcessQualification` | `internal/gatewayruntime` |
| `VIBEDB_DEV_HOT_SPLIT_CUSTOM_TABLE_E2E=1` | `TestGatewayCustomTableDevPressureCompletesReplicatedSplitAndRestart` | `internal/gatewayruntime` |
| `VIBEDB_WAL_RETENTION_E2E=1` (+ `…_EVIDENCE=dir`) | `TestServeRF3WALRetentionCrashQualification` | `cmd/vibedb-shard` |
| `VIBEDB_RESTORE_RF3_PROCESS_E2E=1` (+ `VIBEDB_RESTORE_GATEWAY_BINARY`) | `TestRestoredRF3ExternalProcessServingAndFailover` | `cmd/vibedb-shard` |
| `VIBEDB_NODE_SPACE_E2E=1` | `TestServeRF3NodeSpaceQualification` (no workflow; driven by `scripts/bench/run-node-space-comparison.py`) | `cmd/vibedb-shard` |
| `VIBEDB_RESTORE_ACTIVATION_E2E=1` | `TestActivationExternalProcessRecoversEveryPublicationCutWithinBounds` | `internal/clusterrestore` |
| `VIBEDB_AUTH_TRANSPORT_PROCESS_E2E=1` | `TestAuthenticatedGatewayShardProcessPartitionRotationAndDeputyFaults` | `gateway` |
| `VIBEDB_APPLY_10M=1` (+ `VIBEDB_P01_EVIDENCE_PATH`) | `TestMachineApplyPointUpdateTenMillionQualification` | `internal/replicatedstate` |
| `VIBEDB_STRUCTURAL_10M=1` | `TestFilePrimaryStructuralChurnTenMillionQualification` (no workflow) | `store/durable` |
| `VIBEDB_TEST_PSQL=1` | `TestPSQLClient18` (digest-pinned `psql` image) | `integration/pgclient` |

Variables such as `VIBEDB_RF3_PROCESS_*`, `VIBEDB_RF3_COMMAND_HELPER`, and
`…_CHILD` or `…_HELPER` are set by a parent test for its re-executed child
process. Do not set them by hand.

`VIBEDB_PHYSICAL_TEST_SHARD_BINARY` points `cmd/vibedb` physical-cluster tests at
a prebuilt `vibedb-shard`. Most other process tests build the shipped binaries
themselves with `go build`, so the first run in a cold cache is slow.

Run a gated test with `-count=1`. Otherwise a cached pass is reused, and it
proves nothing about your change:

```sh
VIBEDB_HOT_SHARD_MUTATION_E2E=1 GOEXPERIMENT=simd \
  go test -count=1 -timeout=6m -run '^TestGatewayHotShardMutationProcesses$' -v ./internal/gatewayruntime
```

## Reproduce Linux CI from macOS

The `golang:1.27.0` image reproduces the Ubuntu jobs closely enough for
correctness work. Keep the Go module cache, build cache, and test temporary
directory on named Docker volumes. Named volumes are ext4 in Docker's Linux VM,
so strict allocation works on them. A bind-mounted macOS directory is not ext4.

```sh
docker run --rm \
  -v "$PWD":/src:ro -w /src \
  -e GOEXPERIMENT=simd -e GOFLAGS=-buildvcs=false -e TMPDIR=/work \
  -v vibedb-gomod:/go/pkg/mod \
  -v vibedb-gocache:/root/.cache/go-build \
  -v vibedb-work:/work \
  -e VIBEDB_HOT_SHARD_MUTATION_E2E=1 \
  golang:1.27.0 \
  go test -count=1 -timeout=6m -run '^TestGatewayHotShardMutationProcesses$' -v ./internal/gatewayruntime
```

- `-buildvcs=false` is needed because the source is mounted read-only, and a
  Git worktree's `.git` file points outside the mount. Process tests run
  `go build` on the shipped commands, and that build fails when it cannot read
  VCS data.
- To run a shard, replace the final command with
  `scripts/ci/test-shard.sh durable-large-cache` (or another shard). The image
  has `bash`.
- Workflow validators need `jq`. Install it in the container
  (`apt-get update && apt-get install -y jq`) or validate the `-json` output on
  the host.
- On Apple silicon, the container is linux/arm64, which matches
  `ubuntu-24.04-arm`. For the x86-64 jobs and AVX2 paths, use an x86-64 host;
  emulation is too slow for process timing gates.
- The VM's CPU and memory limits differ from a four-core hosted runner. A
  timing-bound failure that reproduces only in the container is still evidence,
  but compare it with the job's recorded `uname.txt` and limits first.

On a 16-CPU Docker VM with warm caches, the hot-shard command above finished in
about 100 seconds, including builds (70.7 seconds of test time).

## Allocation gate

```sh
go run ./bench/gate
go run ./bench/gate -base <commit> -keep
```

The gate builds the base revision in a temporary detached worktree and compares
`allocs/op` and `B/op`; it never gates `ns/op`. Pull-request CI compares the
test-merge commit against the exact base SHA. See the
[gate README](../../bench/gate/README.md).

## Before you push

```sh
git diff --check
go vet ./...
python3 -m unittest discover -s scripts/ci -p '*_test.py'
go generate ./internal/buildgate ./internal/featurestate && git diff --exit-code
```

The last command matches the stale-generated-file check in the
`repository contracts` job. When you change documentation, also run
`make docs-check` (see the [documentation style](../STYLE.md#check-a-documentation-change)).

## Source map

- [Makefile](../../Makefile)
- [scripts/ci/test-shard.sh](../../scripts/ci/test-shard.sh), [scripts/ci/storage-race.sh](../../scripts/ci/storage-race.sh), [scripts/ci/clock-fault-matrix.sh](../../scripts/ci/clock-fault-matrix.sh)
- [.github/workflows/ci.yml](../../.github/workflows/ci.yml) and [.github/actions/setup-go-ci/action.yml](../../.github/actions/setup-go-ci/action.yml)
- [internal/storeio/strict_allocation_linux.go](../../internal/storeio/strict_allocation_linux.go)
