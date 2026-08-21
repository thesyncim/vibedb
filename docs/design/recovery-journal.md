# Recovery journal

The recovery journal provides bounded redo for synchronous mutation and
conditional participant records for durable database transactions.

## Identity

The primary root stores a journal ID. The journal header stores the related
store ID and journal ID. Open requires an exact cross-binding.

Do not copy, rename, restore, or replace a primary without its related journal.

## Synchronous point mutation

The current synchronous path uses this order:

1. Append the redo record.
2. Sync the record.
3. Apply and publish the visible generation.

Thus, successful synchronous visibility follows durability.

## Conditional batch

A multi-collection participant journal can contain a conditional batch. The
record binds marker ID, marker epoch, transaction ID, generation, store ID, and
journal ID.

The record is not visible until the decision log contains the matching commit.
A valid decision log with no matching decision means presumed abort. Missing
`txn.vtm`, or a mismatched or missing participant, fails closed.

A standalone collection cannot resolve a conditional record. It returns
`ErrCollectionInDoubt` and must be opened through the complete database.

## Torn tail and corruption

Recovery can discard or truncate a torn final append according to the record
grammar. Corruption before the tail fails closed.

An append or sync error poisons the writer. Reopen must replay and reconcile
the journal before new mutation.

## Capacity

An elastic journal can recycle its valid region after the related state is
rooted. A sealed journal has a fixed strictly allocated record region. Sealed
operation is Linux-only and synchronous.

The sealed size must admit the largest possible conditional batch for its
configured transaction geometry.

## Implementation references

- `internal/storeio/recovery_journal.go`
- `store/durable/store_file_journal.go`
- `store/durable/store_database_txn.go`
- `internal/storeio/txn_marker.go`
