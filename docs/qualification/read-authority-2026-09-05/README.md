# Quorum read authority qualification — 2026-09-05

[Qualification index](../README.md)

This record retains intermediate validation for the explicit quorum-promise
read authority implementation. It contains no throughput claim. SQL integration,
production policy wiring, and end-to-end qualification were not complete at
this checkpoint.

## Source and scope

The protocol core was committed as `ef8f3c4b`; adversarial clock-schedule tests
as `7ee6be2c`; authenticated transport and election gates as `e3dd0f1d`.
The following merge `fa01b92d` incorporates main `dc6304e0`, including the
separate fix for ordinary traffic queued to a removed voter.

The logs cover intermediate working copies leading to those commits, rather
than a benchmark of immutable release binaries. File checksums are recorded in
[checksums.json](checksums.json). Failed precursors remain alongside subsequent
runs. No test result establishes the broader 2× CockroachDB objective.

## Validation limits and results

Native package and race suites passed, but strict physical-allocation runtime
fixtures can skip on the macOS host. These passes must not be interpreted as
execution of every enabled-runtime case.

Linux/arm64 test binaries ran in the pinned runtime image
`sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b`
with an anonymous Docker volume mounted at `/tmp`. This permits the actual
strict-allocation fixtures to execute. The initial enabled-runtime Linux run
failed because its fixture omitted the authenticated source identity. The
corrected fixture uses `StepAuthorityMessageFrom`; later runs also align the
policy test with the conservative immutable-per-runtime policy contract.

The final runtime regression log has no skips and covers election gates,
clock faults, independent leader incarnation, term/configuration invalidation,
live-promise policy protection, acquisition expiry, overlapping renewal, and
configuration-pending regressions. Full Linux package runs for raftmodel,
raftmember, rafttransport, multiraft, and raftservice passed at the recorded
intermediate gate checkpoint.

Standalone protocol tests additionally compare drift, delay, renewal, and
restart deadlines against independent arbitrary-precision arithmetic. These
are bounded deterministic tests, not a formal proof or an RF3 process-failure
qualification of the complete SQL feature.

The focused SQL owner validation log
`vibedb-horizontal-authority-sql-owner-linux.log` was built at `e680bdf7` for
`./internal/raftservice` with `GOEXPERIMENT=simd`, `GOOS=linux`,
`GOARCH=arm64`, and `CGO_ENABLED=0`. It ran in the pinned image above with an
anonymous `/tmp` volume and
`-test.run 'Test(Owner.*Authority|OwnerLinearizablePointCut.*)' -test.count=1 -test.v`.
All selected cases passed with no skips. These are focused fake-host SQL owner
tests; they do not constitute production RF3 integration qualification and
make no performance claim.

## Shardservice Linux harness evidence

The three `vibedb-horizontal-authority-sql-shardservice-linux*.log` files
retain the shardservice harness attempts for commit `e680bdf7`. The test
binary was built for Linux arm64 with `GOOS=linux`, `GOARCH=arm64`,
`CGO_ENABLED=0`, and `GOEXPERIMENT=simd`, then run in the pinned image
`sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b`
with an anonymous `/tmp` volume.

The first run and the run with the fixture volume both failed at
`TestShardWireV1GoldenVectors` because the process could not resolve the
relative file `testdata/shard_wire_v1_vectors.txt`. The fixture volume did not
make that relative path visible from the process working directory. The
corrected run set `--workdir /`, bound `shardservice/testdata` to `/testdata`,
executed `/runtime.test`, and used `-test.count=1 -test.timeout=5m`; its log is
`PASS`.

The corrected log is intentionally concise and does not report package-level
skip details. The three copied logs and their byte counts and SHA-256 digests
are listed in [checksums.json](checksums.json).
