# RF3 external fault evidence

`rf3chaos` runs the shipped three-process RF3 fault harness outside the Go test
driver and writes one canonical TSV evidence file. Each repetition exercises:

- isolation of the elected process with `SIGSTOP` and election by the quorum;
- rejection of a stale linearizable-read fence by the resumed former leader;
- leader `SIGKILL` before a proposal and successful post-election proposal;
- a request/response `SIGKILL` race followed by byte-identical recovery;
- restart and catch-up of the killed replica; and
- bounded request-waiter admission and reuse.

Run at least nine isolated repetitions for publication:

```bash
go run ./bench/rf3chaos \
  -output "$(pwd)/rf3-chaos.tsv" \
  -runs 9 \
  -timeout 3m
```

The output path must be absolute and must not exist. The runner builds the
exact working tree once, hashes that test binary, then starts a fresh test
process for every repetition. The test process starts three real
`vibedb-shard serve-rf3` OS-process children. A skipped test is a failed
qualification, even when the test binary exits successfully.

The report retains the commit and dirty state, test binary digest, exact-test
proof, process exit, timeout state, total output bytes, output digest, and
whole-harness elapsed time. It deliberately does not label whole-harness time
as leader-election, failover, recovery, or foreground latency. The current
black-box protocol does not expose those individual cuts.

The runner returns nonzero when any repetition fails, after preserving the raw
report. Keep the corresponding full test logs separately when investigating a
failure; the canonical report authenticates them by SHA-256 but does not embed
unbounded log bytes.
