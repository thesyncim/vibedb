# RF3 external fault evidence

`rf3chaos` runs the shipped three-process RF3 fault harness outside the Go test
driver and writes one canonical TSV evidence file. Each repetition exercises:

- isolation of the elected process with `SIGSTOP` and election by the quorum;
- rejection of a stale linearizable-read fence by the resumed former leader;
- leader `SIGKILL` before a proposal and successful post-election proposal;
- a request/response `SIGKILL` race followed by byte-identical recovery;
- an applied mutation whose native response is deliberately never read,
  followed by leader kill and byte-identical recovery from the surviving
  quorum;
- restart and catch-up of killed replicas;
- two asymmetric leader-to-follower partition/restart/heal loops through
  test-owned directional TCP relays, with interrupted/rejected connections;
- four complete 64-caller request-waiter waves, admission/refusal totals,
  post-wave capacity reuse, and a hard aggregate child-RSS growth bound;
- a hard physical WAL-allocation growth bound; and
- durable acknowledgement survival after all three replicas have restarted.

Run at least nine isolated repetitions for publication:

```bash
go run ./bench/rf3chaos \
  -output "$(pwd)/rf3-chaos.tsv" \
  -runs 9 \
  -timeout 5m
```

The output path must be absolute and must not exist. The runner builds the
exact working tree once, hashes that test binary, then starts a fresh test
process for every repetition. The test process starts three real
`vibedb-shard serve-rf3` OS-process children. A skipped test is a failed
qualification, even when the test binary exits successfully.

The physical WAL-allocation and process-RSS prerequisites are Linux-only.
Darwin cannot prove strict recovery-journal allocation or read the required
per-child `/proc` RSS cuts, so the harness skips there and `rf3chaos`
intentionally returns nonzero after preserving a failed TSV row. A Darwin run
is useful only to confirm that limitation; it is not RF3 fault qualification.
The directional proxy, `SIGSTOP`, `SIGCONT`, and `SIGKILL` controls also fail
the exact test if they cannot be created or exercised. The Linux CI job runs
one mandatory repetition through this runner, so an unsupported filesystem, a
skip, an absent exact test, a missing synced qualification artifact, or any
failed bound fails closed.

The schema-v2 report retains the commit and dirty state, test binary digest,
exact-test and exact-qualification proof, process exit, timeout state, total
output bytes, output digest, and whole-harness elapsed time. Every raw row also
contains the three observed kill/response cuts, asymmetric loop and rejected
connection counts, waiter wave/call/completion/refusal/reuse counts, WAL
baseline/final/growth/bound bytes, and waiter-phase RSS baseline/peak/growth/
bound bytes. It deliberately does not label whole-harness time as
leader-election, failover, recovery, or foreground latency. The black-box
protocol still does not expose those individual durations.

The runner returns nonzero when any repetition fails, after preserving the raw
report. Keep the corresponding full test logs separately when investigating a
failure; the canonical report authenticates them by SHA-256 but does not embed
unbounded log bytes.
