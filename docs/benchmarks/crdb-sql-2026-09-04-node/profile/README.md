# Shared-node diagnostic profile

Clean source `57001b45`, Go 1.27/Linux arm64, shared node logs, 8,192 rows,
1,024 point/update operations, 256 scans, 64 warmups, C1/C8, one repetition.
Both clients verified successfully. Profiling is enabled, so these timings
are diagnostic and must not be treated as a benchmark improvement.

Data member 1 (pid 267) served the read regions; the retained process inventory
maps all profiles. Its 8.73-second capture has 5.79 seconds of CPU samples.
Document construction/indexing remains material: `Segment.Append` is 0.78 seconds
cumulative, `buildDoc` 0.60, `vibejson.buildIndexOptions` 0.56. The follower profiles
also show common-prefix compression work (0.13 seconds flat in member 2).
These mixed captures include startup, reads, writes and verification.

The 5,822 matched read regions average 0.1522 ms for quorum/cut and 0.4751 ms for
execution. Their medians are 0.1303 and 0.0236 ms. Three attempts did not reach
response encoding, consistent with bounded workspace escalation. No reported
region has an unmatched edge. These aggregates are not point-read decomposition.

The gateway's 2,304 writes average 0.4387 ms preparation, 1.7951 ms direct
execution, and 2.3697 ms total. Nested regions must not be added to parent totals.
The trace's data-leader syscall delay includes about 0.96 seconds in the node
log's fdatasync path and 0.66 seconds in fsync across other paths. These are
summed diagnostic syscall durations, not exclusive per-request latency.

A source audit found that the node path also syncs commit-only notifications.
A proposed buffering optimization was rejected by automatic approval review
because of acknowledged-write recovery risk. It was not applied. All node-log
barriers remain intact; the accepted CPU optimization preserves stored bytes.
