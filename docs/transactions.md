# Transactions

> [!CAUTION]
> VibeDB is unreleased development software. Any commit may break APIs, disk
> formats, or wire behavior. Build and operate one exact tested commit only.
> Do not entrust irreplaceable data to VibeDB.

Use root `vibedb` for serializable application transactions over named
collections. `store` and `store/durable` are lower-level engine primitives with
different lifecycle and commit contracts.

## Choose an API

| Need | API | Contract |
| --- | --- | --- |
| Read and write application data | `(*vibedb.Database).Update` | Serializable; commits on a nil callback result |
| Read one coherent database cut | `(*vibedb.Database).View` | Read-only; mutations return `ErrTxReadOnly` |
| Control commit and rollback | `Begin` / `BeginReadOnly` | Caller owns the `Tx` lifetime |
| Atomically publish heap-engine states | `store.UpdateCollections` | Visibility atomic to `store.Database.Snapshot`; no durability |
| Batch one durable collection | `(*durable.Collection).Update` | One logical failure-atomic publication |
| Commit several durable collections | `(*durable.Database).Update` | Conditional prepares plus one durable decision |

## Run a root transaction

```go
err := db.Update(func(tx *vibedb.Tx) error {
	accounts := tx.Collection("accounts")
	audit := tx.Collection("audit")

	if _, err := accounts.Put(
		"account:1",
		[]byte(`{"balance":90}`),
	); err != nil {
		return err
	}
	_, err := audit.Put(
		"entry:1",
		[]byte(`{"account":"account:1","delta":-10}`),
	)
	return err
})
```

`Update` rolls back when the callback returns an error. It also rolls back
before propagating a panic. It does not retry automatically.

Use `Begin` when commit belongs outside a callback:

```go
tx, err := db.Begin()
if err != nil {
	return err
}
defer tx.Rollback()

if _, err := tx.Collection("jobs").Put("job:7", body); err != nil {
	return err
}
return tx.Commit()
```

After `Commit` or `Rollback`, the transaction and every escaped `TxCollection`
return `ErrTxDone`. `Rollback` after commit is safe and returns nil.

## What a transaction sees

A transaction reads an immutable database cut captured at `Begin`, plus its
own staged overlay. `Get`, `Range`, and `Run` all provide read-your-writes.
Uncommitted changes are not visible outside the transaction.

The root transaction is serializable, not snapshot isolation. Commit validates:

- point reads, including reads of absent keys;
- ABA changes to a key;
- write existence used to compute `Put` and `Delete` results;
- collection-wide scans and queries, including phantoms; and
- lazy collection creation races.

Disjoint-key transactions can both commit. A transaction that loses required
revision history can fail conservatively even when no current value differs.
Treat `ErrTxConflict` as a normal whole-transaction retry signal.

## Retry conflicts

Retry the complete closure against a fresh cut. Never reuse a finished `Tx`.

```go
for attempt := 0; ; attempt++ {
	err := db.Update(applyTransfer)
	if !errors.Is(err, vibedb.ErrTxConflict) {
		return err
	}
	if attempt == 7 {
		return err
	}
	time.Sleep(time.Duration(1<<attempt) * time.Millisecond)
}
```

Choose a bounded retry policy appropriate for the request. VibeDB supplies no
default backoff, deadline, or retry count.

## Admission limits

The default root limits are fixed and checked before publication.

| Scope | Resource | Default maximum |
| --- | --- | ---: |
| Per dirty collection | Distinct staged keys | 64 |
| Per dirty collection | Staged key and value bytes | 16,793,600 |
| Whole transaction | Dirty collections | 16 |
| Whole transaction | Distinct staged keys | 256 |
| Whole transaction | Staged key and value bytes | 67,174,400 |
| Whole transaction | Exact read keys | 4,096 |
| Whole transaction | Retained exact-key bytes | 1 MiB |
| Whole transaction | Collections with read dependencies | 128 |

The exact-read counters are transaction-wide. Reaching either bound escalates
the current collection to a coarse dependency; it is not a per-collection
allowance. Exceeding a non-escalating bound returns `ErrTxTooLarge`.

Configure whole-transaction write bounds with
`AdvancedOptions.TxnLimits`. Per-collection bounds come from each engine
collection and are not changed by `TxnLimits`.

## Profile support

| Root profile | One dirty collection | Two or more dirty collections |
| --- | --- | --- |
| `Durable` | Supported | Supported; crash-atomic decision protocol |
| `Buffered` | Supported | Refused with `ErrTxUnsupportedLane` |
| `Memory` | Supported | Supported; visibility atomic, no crash guarantee |

Only dirty collections select the commit path. An empty or read-only commit
does not create lazy storage.

## Publication and generations

The facade serializes transaction validation and publication. It holds the
participant fences in name order, so a direct write cannot slip between
validation and publication. Direct writes to unrelated collections remain
independent.

For a durable single-collection batch, rows and exact-index postings form one
logical failure-atomic publication. Preparing a batch can first publish a
content-equivalent topology generation. If later validation or persistence
fails, logical rows can remain unchanged while `Generation may advance`.
Never use generation equality as the only proof that a failed batch changed
nothing.

## Durable multi-collection commit

With two or more dirty durable collections, the low-level protocol is:

1. append one conditional prepare to each participant journal;
2. synchronize all `K` participant journals;
3. append and synchronize one decision in `txn.vtm`; and
4. publish all participant states while holding every snapshot gate.

The decision is the commit point. The protocol uses `K+1` synchronization
operations; publication after the durable decision is designed to be infallible.

An append or synchronization error can return `ErrCommitOutcomeUnknown`; it does
not prove abort. Close and reopen the poisoned, complete database directory to
resolve the all-or-none result before retrying.

Low-level `durable.UpdateCollections` requires explicit nonzero `TxnLimits` for
two or more dirty collections. `durable.Database.Update` supplies defaults; one
dirty collection bypasses the cross-collection protocol and limits.

`durable.Database.Update` holds the catalog read lock through its callback.
Do not run collection DDL from that callback. The heap
`store.Database.Update` has a different contract: it copies the catalog first
and releases the catalog lock before invoking the callback.

## Heap publication

`store.UpdateCollections` stages all participant batches before locking. It
then locks named participants in sorted order, plans every next state, and
publishes the state pointers only after all fallible work succeeds.

A concurrent `store.Database.Snapshot` sees every participant before or after
the commit. Independent single-collection snapshots can observe different sides
of the pointer-flip sequence. There is no persistence or crash-recovery
dimension in this API.

## Errors and nesting

Use `errors.Is` with:

- `ErrTxConflict` for serialization conflicts;
- `ErrTxTooLarge` for bounded admission refusal;
- `ErrTxDone` for a finished transaction;
- `ErrTxReadOnly` for a mutation in `View` or `BeginReadOnly`;
- `ErrTxUnsupportedLane` for an unsupported durability profile;
- `ErrCommitOutcomeUnknown` for an ambiguous durable decision; and
- `ErrTxNested` for same-goroutine reentry through `Update` or `View`.

Native transactions do not implement savepoints. `ErrTxNested` describes
closure-helper reentry; it is not a general statement about every manual
`Begin` pattern. A `Tx` is single-consumer and must not be copied after first
use.

## Non-guarantees

- A conflict does not retry itself.
- A successful visibility-atomic heap commit is not durable.
- `Buffered` does not support facade transactions that dirty several collections.
- A database snapshot is a local coherent visibility cut, not a durable or distributed timestamp.
- A persistence error does not necessarily mean the last durable transaction aborted.

## Source map

- Facade transaction API: `vibedb_txn.go`, `vibedb.go`
- Serializable anomaly tests: `vibedb_txn_serializable_test.go`
- Profile and retry tests: `vibedb_txn_test.go`, `capability_matrix_facade_test.go`
- Heap atomic publication: `store/store_database_txn.go`
- Durable decision protocol: `store/durable/store_database_txn.go`
- Durable recovery: `store/durable/store_database_txn_recovery.go`
- Crash matrix: `store/durable/store_database_txn_crash_test.go`
