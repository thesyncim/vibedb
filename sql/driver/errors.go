package driver

import (
	"errors"
	"fmt"
)

var (
	// ErrTableNotFound reports a statement naming a table absent from the SQL
	// catalog. Errors wrap this sentinel and retain the table name in their text.
	ErrTableNotFound = errors.New("vibedb: SQL table does not exist")
	// ErrTableExists reports CREATE TABLE without IF NOT EXISTS naming an
	// existing catalog entry.
	ErrTableExists = errors.New("vibedb: SQL table already exists")
	// ErrIndexExists reports CREATE INDEX without IF NOT EXISTS naming an
	// existing index.
	ErrIndexExists = errors.New("vibedb: SQL index already exists")
	// ErrIndexNotFound reports a storage replacement request naming an index
	// absent from the table's cataloged exact-index definitions.
	ErrIndexNotFound = errors.New("vibedb: SQL index does not exist")
	// ErrIndexAmbiguous reports unqualified DROP INDEX when the same table-local
	// index name exists on more than one table.
	ErrIndexAmbiguous = errors.New("vibedb: SQL index name is ambiguous")
	// ErrIndexBuildInProgress reports a second CREATE INDEX on a table whose
	// online build has not reached its atomic catalog cutover.
	ErrIndexBuildInProgress = errors.New("vibedb: SQL index build is already in progress")
	// ErrDuplicatePrimaryKey reports an INSERT whose document-derived primary
	// key already exists, or appears twice in one VALUES batch. INSERT never
	// replaces; UPDATE is the explicit replacement operation.
	ErrDuplicatePrimaryKey = errors.New("vibedb: INSERT primary key already exists")
	// ErrUpdatePrimaryKey reports a whole-document UPDATE whose replacement
	// derives a different primary key from a selected row. One constant
	// replacement document therefore cannot update several distinct primary
	// keys in a single statement; use an explicit transaction with one
	// replacement document per key.
	ErrUpdatePrimaryKey = errors.New("vibedb: UPDATE cannot change a document's primary key")
	// ErrReadOnlyTransaction reports DML or DDL attempted in a read-only
	// transaction.
	ErrReadOnlyTransaction = errors.New("vibedb: mutation attempted in a read-only transaction")
	// ErrDDLInTransaction reports CREATE TABLE or CREATE INDEX attempted between
	// BEGIN and COMMIT/ROLLBACK.
	ErrDDLInTransaction = errors.New("vibedb: schema definitions cannot run inside a transaction")
	// ErrUnsupportedIsolation reports a requested transaction isolation level
	// other than the default or snapshot isolation.
	ErrUnsupportedIsolation = errors.New("vibedb: unsupported transaction isolation level")
	// ErrTransactionTooLarge reports a transaction whose distinct write keys
	// exceed the collection's atomic WriteBatch reservation.
	ErrTransactionTooLarge = errors.New("vibedb: transaction exceeds the collection write-batch bound")
	// ErrTransactionUnsupportedLane reports an ordered-primary durability lane
	// that cannot publish a transactional batch.
	ErrTransactionUnsupportedLane = errors.New("vibedb: transaction is unsupported by the table's durability lane")
	// ErrTransactionConflict reports a first-committer-wins conflict. Nothing
	// from the transaction was published.
	ErrTransactionConflict = errors.New("vibedb: transaction conflict")
	// ErrJoinMaterializationTooLarge reports a JOIN whose durable inputs cannot
	// be expanded through the heap fan-out executor within the query memory
	// budget. No query execution has begun when this is returned.
	ErrJoinMaterializationTooLarge = errors.New("vibedb: join materialization exceeds the query memory bound")
	// ErrCatalogTooLarge reports a SQL catalog whose prospective or on-disk
	// encoding exceeds the bounded metadata budget. The catalog is rejected
	// before an unbounded read or JSON encoding allocation.
	ErrCatalogTooLarge = errors.New("vibedb: SQL catalog exceeds the metadata size bound")
	// ErrTooManyTables reports a catalog whose table count would exceed the
	// database's bounded metadata and eagerly opened durable-handle budget.
	ErrTooManyTables = errors.New("vibedb: SQL catalog exceeds the table-count bound")
	// ErrTooManyRetiredTables bounds old storage incarnations whose active
	// snapshots prevent immediate descriptor release and physical cleanup.
	ErrTooManyRetiredTables = errors.New("vibedb: too many SQL table incarnations are retiring")
	// ErrTooManyStorageFiles bounds recovery work in the private SQL table
	// directory before any durable collection handles are opened.
	ErrTooManyStorageFiles = errors.New("vibedb: SQL table storage recovery exceeds its entry bound")
	// ErrArgumentsTooLarge reports one execution whose aggregate string,
	// exact-number, and document argument payload exceeds the connection's
	// bounded binding workspace.
	ErrArgumentsTooLarge = errors.New("vibedb: SQL arguments exceed the execution payload bound")
)

// tableDependencyError preserves the source location of a physical relation
// across prepare-time validation and execution-time catalog revalidation. It
// wraps ErrTableNotFound so database/sql callers retain errors.Is semantics;
// protocol adapters may use Position without matching message text.
type tableDependencyError struct {
	name        string
	pos         int
	transaction bool
}

func (e *tableDependencyError) Error() string {
	if e.transaction {
		return fmt.Sprintf(
			"%v: %q was not present when the transaction began",
			ErrTableNotFound, e.name,
		)
	}
	return fmt.Sprintf("%v: %q", ErrTableNotFound, e.name)
}

func (e *tableDependencyError) Unwrap() error { return ErrTableNotFound }

// Position returns the zero-based SQL byte offset of the missing relation.
func (e *tableDependencyError) Position() int { return e.pos }

func missingTableDependency(name string, pos int, transaction bool) error {
	return &tableDependencyError{name: name, pos: pos, transaction: transaction}
}
