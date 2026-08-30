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
	// ErrColumnExists reports ALTER TABLE ADD COLUMN without IF NOT EXISTS
	// naming a path already present in the table's declared schema.
	ErrColumnExists = errors.New("vibedb: SQL column already exists")
	// ErrViewNotFound reports a statement naming an ordinary view absent from
	// the durable SQL catalog.
	ErrViewNotFound = errors.New("vibedb: SQL view does not exist")
	// ErrViewExists reports CREATE VIEW naming an existing relation.
	ErrViewExists = errors.New("vibedb: SQL view already exists")
	// ErrViewChanged reports execution of a prepared plan after its view was
	// dropped or replaced. The statement must be prepared again.
	ErrViewChanged = errors.New("vibedb: prepared SQL view definition changed")
	// ErrDependentObjects reports RESTRICT preventing a table or view removal.
	ErrDependentObjects = errors.New("vibedb: dependent SQL views exist")
	// ErrDuplicateViewColumn reports a view whose durable relation schema would
	// contain an ambiguous duplicate output name.
	ErrDuplicateViewColumn = errors.New("vibedb: duplicate SQL view output column")
	// ErrWrongObjectType reports relation DDL naming an existing relation of a
	// different kind, such as DROP VIEW applied to a table. IF EXISTS does not
	// suppress this error because the named object exists.
	ErrWrongObjectType = errors.New("vibedb: wrong SQL relation object type")
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
	// ErrDDLInTransaction reports schema or storage-definition DDL attempted
	// between BEGIN and COMMIT/ROLLBACK.
	ErrDDLInTransaction = errors.New("vibedb: schema definitions cannot run inside a transaction")
	// ErrUnsupportedIsolation reports a requested transaction isolation level
	// other than Read Committed, Repeatable Read/Snapshot, or Serializable.
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
	// ErrTooManySavepoints reports that a transaction exceeded the bounded
	// SAVEPOINT nesting depth (64).
	ErrTooManySavepoints = errors.New("vibedb: too many SAVEPOINT marks")
	// ErrSavepointNotFound reports RELEASE or ROLLBACK TO naming a mark that
	// is not on the transaction's savepoint stack.
	ErrSavepointNotFound = errors.New("vibedb: SAVEPOINT does not exist")
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
	// ErrTooManyViews bounds the durable view graph and reopen validation work.
	ErrTooManyViews = errors.New("vibedb: SQL catalog exceeds the view-count bound")
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
	// ErrCrossShardWrite reports a multi-row INSERT whose rows do not all route
	// to one physical shard. The current contract dispatches each write to a
	// single shard, so a cross-shard batch is refused before any participant
	// receives work.
	ErrCrossShardWrite = errors.New("vibedb: cross-shard write unsupported")
	// ErrShardKeyImmutable reports a whole-document UPDATE whose replacement would
	// move a row to a different shard by changing a shard-key column. A shard-key
	// move is an explicit delete-plus-insert workflow, not an UPDATE.
	ErrShardKeyImmutable = errors.New("vibedb: shard key is immutable")
	// ErrScatterWrite reports a distributed UPDATE or DELETE whose predicate does
	// not resolve to exactly one physical shard. The current contract rejects
	// unknown, targeted multi-shard, and scatter writes before dispatch.
	ErrScatterWrite = errors.New("vibedb: write does not resolve to a single shard")
	// ErrShardKeyNotLocal reports a placed table whose primary or unique key does
	// not contain every shard-key column. Per-shard uniqueness would otherwise
	// stop being global uniqueness, so the placement is refused when it is bound.
	ErrShardKeyNotLocal = errors.New("vibedb: unique or primary key does not contain every shard-key column")
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

type viewDependencyError struct {
	name        string
	pos         int
	transaction bool
}

func (e *viewDependencyError) Error() string {
	if e.transaction {
		return fmt.Sprintf(
			"%v: %q was not present when the transaction began",
			ErrViewChanged, e.name,
		)
	}
	return fmt.Sprintf("%v: %q", ErrViewChanged, e.name)
}

func (e *viewDependencyError) Unwrap() error { return ErrViewChanged }

func (e *viewDependencyError) Position() int { return e.pos }

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
