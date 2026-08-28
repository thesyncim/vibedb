# Online schema shadow build and replay

`BuildReplicatedSchemaDDLShadow` and `ReplayReplicatedSchemaDDLShadow` are
replica-local background build primitives. They are not SQL completion or
activation APIs. The PostgreSQL coordinator must not expose them as completed
online DDL until bounded cutover, immutable certification, and cleanup are wired.

## Copy and catch-up

A build first validates its SQL against the current catalog, starts the durable
capture stream, and pins one immutable data cut. Capture may have started at an
earlier cut: `SourceCapture.CursorAt` resolves the exact snapshot publication to
its capture-chain position using at most one retained-record read. It checks
binding, schema, manifest, term, entry digest, and data-chain digest.

The `.schema-ddl-shadow` journal reserves fresh target storage identities before
any target files are created. Copying streams bounded row batches outside the
serving lock. CREATE/DROP INDEX builds new physical indexes using the target
collection's normal storage machinery. A completed copy records its snapshot
and replay cursors durably. Neither the serving catalog nor source WAL/session
identities change.

Replay consumes a caller-bounded number of entries, stopping no later than the
head observed at call entry. Each relation uses one bounded mutation batch.
Each target row must match either the authenticated before witness or the exact
after value from a prior partially completed attempt. Target updates maintain
the new local indexes. The journal cursor advances only after all relation
effects of that entry are durable. This ordering allows restart between base
and global-index commits without losing or duplicating effects.

TRUNCATE builds empty target relations. Catch-up advances the source cursor but
does not copy intervening writes into those empty targets: all source writes
before the eventual truncate cut belong to the generation being truncated.
Activation must still fence and compare that exact cut.

## Recovery and ownership

One nonblocking shard-local file lock serializes build/replay with the offline
DDL builder. It is not a source write lock. An incomplete copy restarts from a
fresh captured snapshot, reusing only its reserved, unprepared files. A ready
copy is reused across reopening. Corrupt journals, mismatched operations/SQL,
changed source catalogs, aborted capture, and before-witness mismatches fail
the build without modifying serving data.

The ordinary target-certification path refuses retained mutable shadows.
`PreflightReplicatedSchemaTarget` can instead audit a ready shadow under its
exclusive shard-local DDL lock. It binds the exact source catalog and applied
cut, capture cursor, and opaque durable identities of the opened target files.
The returned handle owns those files until `Close`; it holds no source write
lock. Source writes can continue, but any advance makes the handle stale.

After acquiring the distributed write fence, the coordinator can call the
handle's `Prepare`. It rechecks the source cut and target identities and
prepares checkpoint membership without reopening or rescanning target rows.
A stale handle must be closed before catch-up and a fresh audit. Preparation
still performs checkpoint/journal and metadata I/O: it is not a zero-cost or
zero-pause operation. Continuous writes can invalidate each audit, so this
primitive alone does not guarantee cutover progress under sustained load.

Prepared target files cannot be removed or replayed into. A caught-up cursor
or audited handle is only historical evidence; neither establishes a current
distributed write fence. After process replacement the target must be audited
again outside that fence. After closing the target files, `OpenActivatedApply`
can pass the retained opaque image audit into first target activation. It
compares the reopened files' exact durable identities before reusing their
canonical content and global-placement roots, without scanning user or index
rows again. A changed image, foreign binding, zero proof, or non-schema state
fails instead of falling back to a scan. The committed command, checkpoint
membership, catalog CAS, index declarations, and system/session rows still
undergo their ordinary checks.

This audit is process-local, contains no open files or row buffers, and does
not authorize activation by itself. File reopen and full system/session
validation remain. The RF3 shard installer retains this closed audit after
staging and uses it during activation; a replacement process without an audit
uses the existing full-validation recovery path. Command construction can
still re-audit prepared targets, and the PostgreSQL online coordinator is not
yet wired. This is not yet an end-to-end bounded online cutover.

## Costs and remaining work

Copying is proportional to source rows, and capture/replay is proportional to
intervening mutations. Replay currently persists one cursor per source entry;
this is not a zero-cost or measured throughput claim. Source writes retain
their existing bounded capture-abort behavior when storage/retention budgets
are exhausted. Target opening and certification costs must remain outside the
final write fence. Bounded cutover, successful/aborted artifact reclamation,
and PostgreSQL coordinator integration remain required.
