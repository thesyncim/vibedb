# Shared-node shipped-process fault qualification

The existing shipped-command fault history now also runs against fresh node-log
preparation. Three OS processes serve one RF3 group, each with a node-owned log,
using the real TLS/native protocol. No per-group WAL is created. This tests the
same runtime used by `cluster dev --node-log`; it does not substitute an in-memory
Raft simulator or disable a durability setting.

Go 1.27, Linux arm64 Docker, 12 CPU / 24 GiB, named volume mounted at `/data`,
`TMPDIR=/data`. Raw results before the codec optimization are in `baseline.log`;
final results are in `tests.log`. Neither Linux campaign was skipped.

Both final campaigns pass: legacy 6.28 s, shared node log 5.05 s. Each reports:

- Kills before request, racing admission/response, and after applied lost reply:
  one each. Exact retries settle after failover and replica recovery.
- Two asymmetric peer-partition loops, with ten rejected connections proving
  the cuts acted on traffic. Healed replicas catch up.
- Four waiter waves: all 256 calls complete, zero refusals. Later calls reuse
  resources rather than relying on a one-time allocation.
- Acknowledged retry retirement survives every replica restarting (outcome 4).
- Zero observed log-allocation growth in this small campaign. Node accounting
  walks the full node-log tree, including segments, reserves, catalogs and
  checkpoints; SQL roots are outside that measurement.

Waiter RSS growth is 43,786,240 bytes for legacy and 52,969,472 for the node log,
within the unchanged harness bound. These runtime durations are correctness-test
execution times, not a performance comparison.

Host shard-command tests pass (4.650 s); compact-codec/primary-leaf tests pass
(3.159 s). The CPU-only prefix optimization leaves log barriers and encoded
scalar formats unchanged.

This is one group per node. Shared multi-group startup/reload has separate tests;
this campaign does not prove independence under simultaneous multi-group load.
It also does not prove power-loss behavior of the device, forced segment rotation,
sustained reclamation, interrupted group registration, node movement, serializable
interactive SQL or schema backfill under failures. Those goal requirements remain.
