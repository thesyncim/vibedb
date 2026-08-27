# Distributed clock and suspend qualification

Status: **unreleased qualification gate**

VibeDB does not require synchronized UTC for RF3 log order, quorum-confirmed
reads, execution-pin takeover, or transaction recovery. Those protocols use
Raft term/index/applied-index order and bounded replicated recovery pulses.
VibeDB also does not claim a global MVCC timestamp or external consistency
across groups. TLS certificate validity remains explicitly dependent on each
process's UTC clock.

The pull-request workflow
[`clock-fault-matrix.yml`](../../.github/workflows/clock-fault-matrix.yml) runs
one bounded evidence matrix. It composes existing production-path tests rather
than adding a test-only clock to serving code:

| Fault | Boundary exercised | Required result |
| --- | --- | --- |
| Independent forward/backward UTC steps | Separate injected clocks on both ends of a TLS 1.3 peer handshake | Opposite in-window steps authenticate; either peer outside X.509 validity fails closed. |
| Logical recovery stall and restart | Ordered transaction recovery pulses | Restart cannot skip pulses or convert the durable outcome. |
| Leader isolation and re-election | Two independently led RF3 data groups | Hidden commit retry is byte-exact; the isolated former leader cannot propose, read linearly, or perform recovery reads. |
| `SIGSTOP` / `SIGCONT` | Real RF3 child processes and authenticated peer transport | A stopped follower resumes and catches up; a stopped leader loses authority and refuses linearizable reads after resume. |
| Foreground request during a fault | Native session write across response loss and leader death | The operation settles exactly once within the test's explicit 20-second ceiling. |
| Shipped-command pressure | `vibedb-shard serve-rf3`, 64 concurrent waiters, partitions, kill/restart, and TLS | Admission remains bounded, waiter capacity is returned, WAL/RSS growth stay bounded, and acknowledged results survive restart. |

The evidence directory contains one bounded JSONL stream per gate, the shipped
RF3 qualification TSV, build and platform identity, and a compact matrix TSV.
Skipped tests fail the workflow; a green job therefore cannot silently become
a platform skip.

Run the same gate locally on Linux:

```sh
export VIBEDB_CLOCK_FAULT_EVIDENCE="$(mktemp -d)/evidence"
./scripts/ci/clock-fault-matrix.sh
```

## Honest limits

The test injects independent UTC values at the TLS verification boundary. It
does not change the host clock of live database processes: doing that in a
shared CI runner would affect unrelated jobs and Go's monotonic timers. Real
process suspension is tested with OS signals instead. This qualifies protocol
safety and bounded recovery under those exact faults; it is not a claim about
arbitrary kernel, hypervisor, or time-service failures.

Operators still need clock monitoring and certificate renewal margin. A wrong
UTC value can reject a valid certificate or accept one outside the operator's
intended real-time window. It cannot grant Raft leadership or advance a
transaction recovery pulse, but loss of authenticated connectivity can make a
quorum unavailable.

The semantic model and remaining obligations are documented in
[`distributed-clock-model.md`](../design/distributed-clock-model.md).
