# Direct channel cancellation

RF3 SQL previously registered an AfterFunc callback and then started a driver watcher goroutine with two stop/join channels for the same execution. CancelFlag can now observe the request Done channel directly at its existing bounded checkpoints. The driver retains its original context and skips the duplicate watcher only for an exact channel match. The point request owns its flag storage and detaches the channel on return.

On Apple M4 Max / Go 1.27, BenchmarkReplicatedPointReadCancellation (three one-second repetitions, including cell verification) measured:

| Execution | ns/op range | B/op | allocs/op |
|---|---:|---:|---:|
| Duplicate bridges | 4988–5167 | 480 | 7 |
| Direct channel | 3116–3143 | 0 | 0 |

Both arms use the same current binary and warmed caller-owned lease, cursor and preboxed arguments. Context construction is outside the timer; per-execution cancellation setup is inside it. These are local driver measurements, not end-to-end RF3 throughput or whole-request allocation claims. The pre-change bridge on the previous flag representation measured 464 B/op and 7 allocs/op; adding channel storage explains the current duplicate arm's extra 16 bytes.

Regression coverage includes zero warm hit/miss allocations with a cancellable context, matching and mismatching channels, catalog deadline waits, executor cancellation/reuse, explicit atomic cancellation, concurrent workers, closed-channel lifetime, and segment/snapshot/durable scan backends. A closed bound channel cannot be cleared by Reset or Take; rebinding requires a quiescent flag. Unbound flags keep their original reusable atomic signal.
