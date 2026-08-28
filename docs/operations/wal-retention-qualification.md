# RF3 WAL retention qualification

Status: mandatory Linux pull-request gate.

The qualification runs the shipped `vibedb-shard serve-rf3` path as three
authenticated operating-system processes. It is deliberately separate from
the in-memory Raft and WAL unit suites.

Each of three cycles writes a bounded 1.5 MiB live key set, waits for every
replica to replace its logical WAL inode with an authenticated checkpoint-based
generation, kills a different process with `SIGKILL`, restarts it from the same
disk state, catches it up, and performs a linearizable read of the acknowledged
value. After all three process lifetimes have changed, replaying the first
acknowledged command must return the durable retired outcome.

The gate records canonical TSV, not JSON, and rejects any skipped run. Three
independent runs must satisfy all of these bounds:

- exactly three generation-and-crash cycles and nine observed WAL replacements;
- no more than 1 MiB additional physically allocated WAL space above the
  initial fixed reservation;
- retained WAL growth no greater than 250 permille of the final replicated live
  document bytes (overwritten history is excluded from the denominator);
- no more than 128 MiB aggregate process RSS growth and 24 aggregate file
  descriptors of growth;
- foreground write p99 no greater than 5 seconds and maximum no greater than
  15 seconds under the maintenance and crash workload;
- bounded duplicate waiter waves complete and return their capacity before the
  next fresh proposal.

The production maintenance cadence remains ten minutes. Only the test binary,
when launched with the qualification environment, compresses that cadence to
eight logical ticks. This keeps CI finite without exposing a serving flag that
could accidentally trade write amplification for an unrealistically aggressive
compaction schedule.

Run the exact Linux test locally with:

```bash
evidence="$(mktemp -d)/evidence"
VIBEDB_WAL_RETENTION_E2E=1 \
VIBEDB_WAL_RETENTION_EVIDENCE="${evidence}" \
go test -count=3 -timeout=24m \
  -run '^TestServeRF3WALRetentionCrashQualification$' \
  ./cmd/vibedb-shard
```

The test requires `/proc` RSS and descriptor accounting plus strict physical
allocation support. Darwin runs skip by design; the Linux workflow treats a
skip, absent evidence, extra/missing evidence files, or any violated bound as a
failure.
