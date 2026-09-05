# RF3 external fault qualification

[Performance guide](../../docs/performance.md)

> **Development status:** This is a checked-in development qualification harness,
> not a production fault-injection service or a stable release contract. RF3
> behavior, limits, report schema, and commands can change or break at any commit.

`rf3chaos` runs one exact Go qualification test outside the normal test driver.
Each repetition launches three real `vibedb-shard serve-rf3` child processes and
writes a canonical TSV row.

## Requirements

- Linux with `/proc` process RSS and process-I/O accounting
- working `SIGSTOP`, `SIGCONT`, and `SIGKILL`
- a filesystem on which the qualification's WAL-allocation checks work
- enough time and local resources for three shard processes per repetition

Darwin cannot prove the required RSS and physical WAL-allocation bounds. The
underlying test skips there, and this runner converts that skip into a failed
qualification row and a nonzero exit.

## Run it

From the repository root:

```bash
go run ./bench/rf3chaos \
  -output "$(pwd)/rf3-chaos.tsv" \
  -runs 9 \
  -timeout 5m
```

The output path must be absolute and must not already exist. `-runs` must be
between 1 and 1,024, and `-timeout` must be positive. One repetition is useful
for development feedback; a publication candidate requires at least nine
isolated repetitions and the full competitive evidence protocol.

The runner builds and hashes the exact worktree's test binary once, then starts
a fresh test process for every repetition. It writes and syncs the report even
when a repetition fails, and returns nonzero after recording all attempted rows.

## What one passing repetition checks

- quorum election while the elected process is stopped;
- rejection of a stale linearizable-read fence after that process resumes;
- leader kill before proposal, followed by successful post-election proposal;
- lost-response and unread-response kill races with byte-identical recovery;
- restart and catch-up of killed replicas;
- two asymmetric leader-to-follower partition/restart/heal loops through
  directional test proxies;
- four waves of 64 request waiters, refusal accounting, and capacity reuse;
- bounded aggregate child-RSS growth during waiter pressure;
- bounded physical WAL allocation growth; and
- survival of a durable acknowledgement after all replicas restart.

A skipped test, missing exact test marker, missing synced qualification artifact,
timeout, failed bound, or unsuccessful child exit fails the row.

## What it does not prove

- The recorded elapsed value covers the whole harness. It is not isolated
  election, failover, recovery, or foreground latency.
- The harness does not vary node count, replica factor, group count, placement,
  storage device, or network topology.
- It does not exercise a gateway process or establish horizontal scaling,
  split/rebalance behavior, rolling upgrades, or production readiness.
- Linux process-I/O and filesystem allocation counters are not media-write
  measurements.
- A standalone run may record a dirty tree; publication validation rejects one.

The TSV records revision and dirty state, Go/OS/architecture, exact test and
qualification markers, binary and output digests, process/timeout state, fault
counters, waiter counts, and WAL/RSS baselines, peaks, growth, and bounds. The
canonical report retains only bounded log material; preserve full logs separately
when diagnosing a failure.

Core CI runs one repetition, PR competitive qualification runs three, and neither
is an endorsed performance result. See
[the benchmark evidence levels](../competitive/README.md#evidence-levels).
