# Durable ownership and lifetime rules

This is a contributor reference for `store/durable`. These rules are part of
the current implementation contract, but the project is unreleased: pin an
exact commit and expect API and disk-format breaks.

The package separates three kinds of ownership:

| Resource | Owner | Release point |
| --- | --- | --- |
| Primary file passed to `Create` or `Open` | Caller | After collection `Close` completes |
| Files opened by `OpenDatabase` | `Database` | Successful terminal `Database.Close` |
| Published generation | Collection engine | Retirement after all leases close |
| Collection snapshot lease | `Snapshot` | `Snapshot.Close` |
| Database snapshot leases | `DatabaseSnapshot` | `DatabaseSnapshot.Close` |
| Batch staging storage | Collection/database session | Reused after callback returns |
| Caller key/value arguments | Caller | May reuse after the method returns |

## Caller-owned primary files

`Create(file, options)` and `Open(file, options)` borrow `file`; they do not
close it. The caller must keep the descriptor open for the collection's whole
lifetime and close it only after the collection has completed `Close`.

The borrow is exclusive. While the collection is live, the caller must not:

- read, write, seek, or truncate through the descriptor;
- take a competing lock;
- rename or unlink the backing file;
- hand the descriptor to a second engine.

The engine can own additional internal descriptors, including recovery-journal
handles. It closes those during its own terminal close. This does not transfer
ownership of the caller's primary descriptor.

`OpenDatabase(directory, options)` is different. The database opens its own
collection, journal, and transaction-marker descriptors and owns them. The
caller owns only the directory path and must not mutate those files behind the
database.

## Input byte slices

Point writes and batch methods borrow key/value slices for the duration of the
call and copy any bytes they retain. A successful return therefore permits the
caller to reuse or mutate its input buffers.

The same rule applies when a write is staged inside `Update`: staging must not
retain caller aliases beyond the callback. A rejected call publishes nothing
for that operation.

Read callbacks are the inverse. Slices passed to `GetRaw`, `RangeRaw`, and
similar visitors may alias immutable page/cache/scratch storage. Treat them as
read-only and copy any bytes that must outlive the callback.

Do not infer string ownership rules from byte APIs. Keep new byte-native APIs
explicit about whether an argument or result is borrowed, transferred, or
copied.

## Published generations

Readers load an immutable published state. Writers build replacement state and
publish it under the collection's publication gates. Copy-on-write publication
does not make the underlying file immutable: retired extents can later be
reused after every lease that can see them is gone.

Never retain a page-cache view, decoded leaf view, slot view, or borrowed value
past the lease or callback that protects it. A Go reference alone is not a
generation lease.

`Generation()` identifies the current physical topology generation. It is not
an application revision, transaction ID, or proof that document content
changed.

## Snapshots

A `Snapshot` owns one generation lease plus reusable traversal scratch. It is a
single-consumer object: do not issue concurrent operations through the same
snapshot. Call `Close` when finished; repeated close calls are harmless.

A `DatabaseSnapshot` owns one lease for each non-empty captured collection. It
also retains reusable entry/name storage. It is single-consumer and must be
closed even if the caller reads only one member.

An unclosed snapshot can pin retired extents. Once retirement reaches configured
bounds, writes or new snapshot acquisition may return backpressure. This is a
resource-lifetime failure, not automatic garbage collection.

Closing a snapshot invalidates all borrowed views produced through it. An empty
collection may have no underlying collection snapshot, but its catalog entry is
still part of a database-wide cut.

## Update callbacks

`Collection.Update` lends one reusable `WriteBatch` to the callback. The batch
is valid only until the callback returns. Do not save it, call it later, or use
it concurrently.

`Database.Update` similarly lends one transaction object with per-collection
batches. The database's collection catalog is fixed for the callback; DDL must
not be attempted from inside it.

The engine copies staged key/value inputs. Callback success only asks the engine
to commit; the commit may still fail. Callback error abandons staged mutations.
After an unknown-outcome persistence error, recover from the durable decision
state instead of reusing a captured batch object.

Session and workspace APIs retain scratch specifically to amortize allocation.
They are single-consumer objects unless their type documentation explicitly
says otherwise. Release or close them through their owning facade.

## Close and retry

`Close` can fail before ownership is fully released. Do not close a borrowed
primary descriptor, delete a directory, or reuse an engine merely because one
close attempt returned.

Use `CloseCompleted()` where provided to distinguish a terminal close from a
retryable failure. Retry `Close` according to the error contract. Only after
terminal completion may the caller close its borrowed primary file or mutate
the database directory.

No operation should silently convert a failed close into descriptor transfer.
Tests that inject close/checkpoint failures must assert both the returned error
and the ownership state.

## Contributor checklist

- State ownership in every exported API that accepts an `*os.File` or `[]byte`.
- Pair every generation view with an explicit lease or bounded callback.
- Keep callbacks synchronous; never retain visitor arguments.
- Keep snapshot and session concurrency rules explicit.
- Make failure paths release only resources actually acquired.
- Preserve retryable ownership after non-terminal `Close` failures.
- Test aliasing by mutating caller input immediately after return.
- Test retired-pressure behavior with a deliberately held snapshot.
- Do not describe cache or overlay budgets as a total process-memory cap.

## Source map

- `store/durable/durable.go` — package-level file and snapshot ownership
- `store/durable/store_file.go` — collection construction and close lifecycle
- `store/durable/store_file_batch.go` — batch lifetime and copied inputs
- `store/durable/store_file_operations.go` — generation leases and borrowed reads
- `store/durable/store_database.go` — database descriptor ownership and close
- `store/durable/store_database_snapshot.go` — coherent multi-collection leases
- `store/durable/store_database_txn.go` — transaction callback lifetime
- `store/durable/facade_close_external_test.go` — borrowed-file close behavior
- `store/durable/store_file_admission_test.go` — lease-driven retirement pressure
