# Raft owner and WAL pressure qualification — 2026-09-06

The production change in this PR stops fresh timer work from indefinitely extending a pending control drain. It retains async completion service and resumes ticks after the control executes. The deterministic control test fails without the change and verifies the Ready boundary, completion wake, timer resumption, and ingress accounting.

Main advanced to `ba6c4888` during qualification. PR #172 already fixed legacy pipelined WAL pressure by allowing an uncaptured Ready to wait while a fully settled durable cut is compacted. A separate admission experiment was discarded. The additional WAL regression test covers a proposal arriving before the last outstanding append completion is consumed, then verifies repeated compaction, exact-retry completion, and stored contents. This test fails against `8f52fc5b` (before #172), leaving an uncaptured Ready with no WAL headroom; it passes three repetitions on `856b27a6` (main plus the control fix).

## Recorded checks

- `wal-overlap-before.log`: expected failure with the final test against pre-#172 code.
- `wal-overlap-after.log`: both pipelined pressure tests, three repetitions, pass.
- `race.log`: focused pressure and owner regression tests pass under the race detector.
- `owner-host-full.log`: complete raftservice and multiraft suites pass on current main plus the control fix.
- `linux-pressure.log`: three production-process pressure/restart trials; two pass, one loses its serving leader between discovery and proposal at sequence 2. This remains a failed process qualification, not a passing gate.

Go commands use the project environment wrapper. Process qualification uses Linux ARM64, Go 1.27, `GOEXPERIMENT=simd`, CGO disabled, and runtime image `sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b`. Host contention was not excluded; these checks establish behavior, not comparative performance.

## Performance limits

Earlier RF3 mixed-load qualification on `16bc587f` and `8f52fc5b` lost leader availability. The control-fix run also returned two outcome-unknown writes and subsequently failed its final oracle comparison; unknown writes may have committed, so that comparison alone does not establish corruption. A pending-tick-budget experiment also failed sustained qualification and is excluded. A commit-hint scheduling experiment passed storage tests but failed RF3 setup and is excluded.

No end-to-end speedup or CockroachDB lead is claimed. Sustained mixed-load availability and matched performance qualification remain unresolved.
