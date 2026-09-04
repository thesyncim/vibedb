# Initial shared-node serving qualification

These are correctness/composition tests, not performance measurements. The code
under this commit adds `prepare-node-rf3 -manifest PATH`. Fresh preparation writes
one node log plus multiple SQL roots into a private directory, then publishes
that directory with a rename and parent-directory sync. `serve-rf3` selects the
node owner from the generated manifest's `node_log` field. It never opens the
individual groups' legacy WAL paths in this mode.

A node owns one bounded append sequencer and one checkpoint coordinator.
Individual runtimes retain separate group/member identities and SQL claims.
Incarnation allocation uses the same sequencer as appends. Schema quiescence
drains that group's checkpoint work before closing its SQL generation; it keeps
the shared coordinator and log alive.

## Evidence

Go 1.27, Linux arm64 Docker, 12 CPU / 24 GiB, named Docker volume mounted at
`/data`, with `TMPDIR=/data`. The volume supplies real filesystem allocation;
these tests were not run on overlayfs or tmpfs. Raw output: [tests.log](tests.log).

- Two initial SQL groups share one log; neither has a `member.wal` file.
  Two complete physical-log reopen cycles advance each member incarnation.
  Quiescing and reinstalling a SQL handle retains its Raft runtime. Closing
  one runtime leaves the neighboring group usable.
- Seventeen groups cross the initial log-creation wave bound. Additional
  descriptors and snapshots are registered inside the unpublished node root.
  Every SQL group opens after publication. Preparing over that existing root
  is rejected.
- Three server instances, two RF3 groups each, run the normal authenticated
  serving path. Each group elects a leader; all servers stop, reopen, and elect
  leaders again. This test uses three server instances in one test process,
  not three machines or a process-crash fault campaign.
- Node-log schema recovery preserves uncommitted preparation, finishes committed
  publication, and accepts a durable applied checkpoint ahead of its asynchronous
  commit-only HardState. Each case repeats physical-log recovery.
- Existing legacy preparation and the shipped process fault harness still pass.
  The harness reported cuts 1/1/1, 256/256 waiter completions, zero waiter refusals,
  zero WAL growth and acknowledged-outcome code 4. **That fault harness still
  uses legacy per-range logs; it does not qualify node-log fault behavior.**

Host tests also passed: `go test ./internal/raftmember ./cmd/vibedb-shard -count=1`.

## Preparation interface and remaining work

The canonical preparation document is `prepareRF3NodeManifest`:
`root`, `node_log`, and `groups`. Each group uses the existing RF3 member
preparation fields, with its root exactly `ROOT/group-N`. All groups share the
same local node, RF3 roster, listeners, TLS and control grants. Node log format
is 1, its path is `ROOT/node-log`, and its key input is copied to `ROOT/node-key`.
`options` encodes `raftstore.NodeStoreOptions`; zero values select its defaults.
Existing roots are rejected. No legacy-data migration is performed.

The group metadata retains legacy WAL geometry for existing split/replica
preparation templates. This does not allocate or authorize a per-range WAL in
node mode. Removing those legacy template dependencies is still part of the
redesign.

Hot registration of freshly prepared groups into a live node, split/replica
movement on node logs, interrupted admission, sustained checkpoint reclamation,
concurrent DDL under load and acknowledged-write crash testing remain unqualified.
The benchmark/dev topology still uses legacy preparation. There is no new CRDB
comparison, throughput gain, data-density claim or horizontal-scaling result in
this qualification. No performance-goal acceptance gate is complete.
