# Multi-table transactions

The native facade, durable database, `database/sql`, and pgwire expose
multi-table transaction paths. They share storage primitives but have different
entry-point rules.

## Native facade

`Database.Update` and `Begin` create serializable read-write transactions.
`View` and `BeginReadOnly` create coherent read-only cuts.

The facade defaults to 16 dirty collections, 256 staged documents, and
67,174,400 staged key-plus-value bytes. It returns `ErrTxConflict` for a
validated dependency conflict and `ErrTxTooLarge` for a limit breach.

The memory profile publishes participant state pointers while it holds all
writers. The durable profile uses participant journals and a decision log. The
buffered facade refuses two or more dirty collections because its selected
lane has no participant journal protocol.

## Durable protocol

One dirty collection uses the ordinary collection update. Two or more dirty
collections use this sequence:

1. Append conditional records to participant journals.
2. Sync each participant journal.
3. Append and sync the decision in `txn.vtm`.
4. Publish every participant under the database snapshot gates.

The decision is the commit point. A valid decision log with no matching
decision means presumed abort. Missing `txn.vtm` or a required participant
fails closed. A committed decision causes roll-forward during reopen.

## SQL entry points

The SQL driver maps transaction isolation levels to read-cut and dependency
behavior. It supports savepoints. Native facade transactions do not.

Pgwire also maintains protocol transaction status. A failed transaction accepts
rollback control. `ROLLBACK TO SAVEPOINT` can return it to an active state.

## Observation rule

A coherent database snapshot cannot observe a partial multi-table commit.
Independent reads from separate collection handles are not one coherent
observation and can see different publication instants.

## Unknown outcome

An ambiguous decision append or sync can return an unknown-outcome error.
Every participant remains all-or-none after recovery. Close and reopen before
you decide whether to retry application work.

## Implementation references

- `vibedb_txn.go`
- `store/store_database_txn.go`
- `store/durable/store_database_txn.go`
- `sql/driver/tx.go` and `savepoint.go`
