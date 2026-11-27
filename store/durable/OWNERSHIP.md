# Durable ownership and lifetime rules

[Storage guide](../../docs/store.md)

Contributor ownership contract for `store/durable`.

## Ownership matrix

| Resource | Owner | Release point |
| --- | --- | --- |
| Primary `*os.File` passed to `Create`/`Open` | Caller (exclusive borrow) | Only after `CloseCompleted(col.Close())` is true |
| Files opened by `OpenDatabase` | `Database` | Terminal `Database.Close()` |
| Published generation | Engine | Retired once all snapshot leases close |
| `Snapshot` / `DatabaseSnapshot` | Caller (single-consumer) | `Close()` (unclosed leases pin retired extents and block writes) |
| `WriteBatch` in `Update` callback | Engine (reusable) | Invalid once callback returns; never retain or use concurrently |
| Input key/value `[]byte` | Caller | Engine copies retained bytes; caller may reuse after return |
| Read callback `[]byte` (`GetRaw`, `RangeRaw`) | Engine (borrowed) | Read-only, valid only inside callback; copy to retain |

## Key rules

- **Exclusive file borrow:** Never read, write, seek, truncate, lock, rename,
  or unlink a borrowed primary file while the collection is open.
- **No Go-GC lifetime:** A Go reference to a page/slot view is not a generation
  lease; never retain borrowed views past the callback or `Snapshot.Close()`.
- **Retryable close:** `Close()` may fail while snapshots or locks remain open.
  Use `CloseCompleted()` to distinguish retryable teardown from terminal close;
  never close the borrowed `*os.File` until terminal completion.
- **No DDL inside `Database.Update`:** The catalog is fixed for the callback.
  After `ErrCommitOutcomeUnknown`, close and reopen to recover from `txn.vtm`.

## Contributor checklist

- Document borrow/copy ownership on every exported `*os.File` or `[]byte` API.
- Test aliasing by mutating input slices immediately after return, and test
  retirement backpressure with a held snapshot.
- Keep failure paths releasing only resources actually acquired.

## Source map

- [store/durable/durable.go](durable.go) · [store_file.go](store_file.go) · [store_file_batch.go](store_file_batch.go) · [store_database.go](store_database.go) · [store_database_txn.go](store_database_txn.go)
- Tests: [facade_close_external_test.go](facade_close_external_test.go) · [store_file_admission_test.go](store_file_admission_test.go)
