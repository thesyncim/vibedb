# Catalog visibility across physical frontends

[Documentation](README.md) / [Research records](design/research.md)

**Record scope:** This page retains a dated proposal or investigation. Its
revision-specific findings and future work are not the current operating guide.
See [architecture](architecture.md) and [operations](operations/README.md).

Frozen baseline `7dc21395110fa79b90b13dde7848a3bc13090d6d` reproduces two
failures in the real Linux/ARM64 process qualification with Go 1.27 SIMD:
physical3 acknowledges writes through the owner but native frontend 2 cannot
resolve `fused_alpha`; physical6 rejects PostgreSQL preparation of that table
with `ErrTableNotPlaced`. Both fail before crash injection. This is a serving
correctness gap, not evidence of data loss after a crash.

Secondary frontends deliberately run as control participants without autonomous
membership or health controllers. They have an authenticated catalog refresh
source, but it is currently reached only after a shard rejects a known route.
Locally absent tables fail before reaching a shard, so a secondary frontend can
remain on its startup catalog indefinitely. PostgreSQL also validates routing
during preparation, before the executor's dispatch retry boundary.

The implementation must refresh on this specific pre-dispatch metadata miss.
Use the configured authenticated RF3 catalog authority and existing checked
publication, lineage and generation-drain rules. Reacquire an immutable catalog
generation and re-prepare or resolve the original request once. Coalesce
concurrent refreshes where supported. An existing table's successful lookup
must retain its current path without another catalog quorum read or periodic
polling.

An absent table must be distinguished from an invalid key, foreign read
position, unsupported SQL, mixed route, resource refusal or authorization
failure. Those errors do not authorize generic replay. If a refresh yields no
usable newer catalog, retain the missing-table refusal and fail closed on
refresh errors. Each actual dispatch owns one generation; no partial result
may escape from an attempt that is retried.

Cover PostgreSQL read and write preparation, ordinary distributed SQL reads,
and byte-native point/batch/scatter reads. Inspect direct SQL pre-admission
planning as well. Already admitted writes and durable prepared recipes must
retain their exact identity and mutation bytes; metadata refresh must not
silently replan an unknown outcome. Participant-only frontends must not gain
membership authority or start autonomous controllers.

Validation requires stale and refreshed catalogs, bounded missing-table
refusal, unchanged malformed-input/position/route errors, no data dispatch
against an absent table, and no refresh on successful existing-table access.
After focused normal/race tests and independent review, run the real physical3
and physical6 qualification through every frontend and retained-root restart.
Keep the failing baseline and any newly exposed failures visible. Passing this
gate alone does not establish transaction parity, independent-machine scale
or the performance objective.
