package driver

import "errors"

var (
	// ErrIndexedTableReadOnly reports a mutation against a table whose exact
	// posting index is currently build-and-read.
	ErrIndexedTableReadOnly = errors.New("vibedb: indexed tables are build-and-read; mutation waits for mutable exact postings")
	// ErrTransactionIndexedTable reports the engine gate that prevents a
	// transactional batch from maintaining an indexed primary collection.
	ErrTransactionIndexedTable = errors.New("vibedb: transaction cannot mutate an indexed table")
	// ErrTransactionTooLarge reports a transaction whose distinct write keys
	// exceed the collection's atomic WriteBatch reservation.
	ErrTransactionTooLarge = errors.New("vibedb: transaction exceeds the collection write-batch bound")
	// ErrTransactionUnsupportedLane reports an ordered-primary durability lane
	// that cannot publish a transactional batch.
	ErrTransactionUnsupportedLane = errors.New("vibedb: transaction is unsupported by the table's durability lane")
	// ErrTransactionConflict reports a first-committer-wins conflict. Nothing
	// from the transaction was published.
	ErrTransactionConflict = errors.New("vibedb: transaction conflict")
)
