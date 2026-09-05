// Package conformance owns the executable database-capability manifest.
//
// The manifest deliberately lives outside the storage and SQL packages: their
// black-box tests consume it, while the documentation golden test renders the
// same rows. A capability therefore cannot be added to the public table without
// also becoming an executable test case at every named entry point.
//
//go:generate go run ./cmd/capabilitygen -out ../../docs/capabilities.md
package conformance

import (
	"fmt"
	"strings"
)

type EntryPoint string

const (
	Native      EntryPoint = "native"
	DatabaseSQL EntryPoint = "database/sql"
	PGWire      EntryPoint = "pgwire"
)

type Indexing string

const (
	Unindexed Indexing = "unindexed"
	Indexed   Indexing = "indexed"
)

type Tables string

const (
	OneTable       Tables = "one table"
	MultipleTables Tables = "multiple tables"
)

type Transaction string

const (
	Autocommit Transaction = "autocommit"
	Explicit   Transaction = "explicit transaction"
	Savepoints Transaction = "explicit transaction with savepoints"
)

type Keys string

const (
	OneKey       Keys = "one key"
	MultipleKeys Keys = "multiple keys"
)

type Operation string

const (
	Insert Operation = "insert"
	Update Operation = "update"
	Delete Operation = "delete"
	Mixed  Operation = "mixed"
)

var AllOperations = []Operation{Insert, Update, Delete, Mixed}

var PointSQLOperations = []Operation{Insert, Update, Delete}

var MultiRowSQLOperations = []Operation{Insert, Delete}

type Result string

const (
	Success          Result = "success"
	DocumentedError  Result = "documented error"
	IndependentCalls Result = "success; separate publications"
)

// Lane is one externally distinct acknowledgement/publication contract. The
// backend and direct-I/O choices are physical implementations of these same
// contracts and are qualified separately; checkpoint strength is included
// because it changes the crash promise.
type Lane string

const (
	SyncJournal                Lane = "sync-journal / power-safe"
	BufferedVolatilePowerSafe  Lane = "buffered-volatile / power-safe"
	BufferedVolatileFilesystem Lane = "buffered-volatile / filesystem"
	BufferedJournalPowerSafe   Lane = "buffered-journal / power-safe"
	BufferedJournalFilesystem  Lane = "buffered-journal / filesystem"
	AsyncCOW                   Lane = "async COW / power-safe"
	SyncChainFence             Lane = "sync chain-fence / power-safe"
	SQLDefaultSyncJournal      Lane = "fixed SQL default: sync-journal / power-safe"
)

var NativeLanes = []Lane{
	SyncJournal,
	BufferedVolatilePowerSafe,
	BufferedVolatileFilesystem,
	BufferedJournalPowerSafe,
	BufferedJournalFilesystem,
	AsyncCOW,
	SyncChainFence,
}

var DeferredBatchLanes = []Lane{
	SyncJournal,
	BufferedVolatilePowerSafe,
	BufferedVolatileFilesystem,
	BufferedJournalPowerSafe,
	BufferedJournalFilesystem,
}

var FenceBatchErrorLanes = []Lane{AsyncCOW, SyncChainFence}

// DatabaseTxnLanes are the journal-backed lanes that accept a multi-collection
// database transaction. Acknowledgement follows K prepare syncs plus the
// decision sync; buffered-journal commits are crash-atomic with durability
// preceding visibility, stronger than that lane's single-collection contract.
var DatabaseTxnLanes = []Lane{
	SyncJournal,
	BufferedJournalPowerSafe,
	BufferedJournalFilesystem,
}

// DatabaseTxnErrorLanes refuse multi-collection database transactions with
// durable.ErrDatabaseTransactionUnsupportedLane.
var DatabaseTxnErrorLanes = []Lane{
	BufferedVolatilePowerSafe,
	BufferedVolatileFilesystem,
	AsyncCOW,
	SyncChainFence,
}

// Case is one row in the public capability table. Tests expand Tables, Keys,
// Operations, and Lanes into named subtests. Rollback means every successful
// atomic expansion must also execute a rejected sibling and prove that no
// target collection published rows or postings.
type Case struct {
	ID          string
	Entry       EntryPoint
	Indexing    Indexing
	Tables      []Tables
	Transaction Transaction
	Keys        []Keys
	Operations  []Operation
	Lanes       []Lane
	Result      Result
	Atomic      bool
	Rollback    bool
	Error       string
	Note        string
}

// Cases states the implementation contract that exists today. Indexed and
// unindexed Collection.Update batches share the deferred-canonical lanes; the
// two committer-fence lanes still fail closed with the typed lane error.
// Changing a row necessarily changes both the executable adapters and the
// rendered public table.
var Cases = []Case{
	{
		ID: "native-point-unindexed", Entry: Native, Indexing: Unindexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: PointSQLOperations,
		Lanes: NativeLanes, Result: Success, Atomic: true, Rollback: true,
		Note: "one Put/Delete call is one publication",
	},
	{
		ID: "native-point-indexed", Entry: Native, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: PointSQLOperations,
		Lanes: NativeLanes, Result: Success, Atomic: true, Rollback: true,
		Note: "the primary row and exact postings publish together",
	},
	{
		ID: "native-point-mixed-one-unindexed", Entry: Native, Indexing: Unindexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: []Operation{Mixed},
		Lanes: NativeLanes, Result: IndependentCalls,
		Note: "mixed point operations on one key are separate publications; use Collection.Update for an atomic group",
	},
	{
		ID: "native-point-mixed-one-indexed", Entry: Native, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: []Operation{Mixed},
		Lanes: NativeLanes, Result: IndependentCalls,
		Note: "each point publication maintains exact postings; use Collection.Update for an atomic group",
	},
	{
		ID: "native-point-multiple-unindexed", Entry: Native, Indexing: Unindexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: AllOperations,
		Lanes: NativeLanes, Result: IndependentCalls,
		Note: "separate point calls are not a transaction",
	},
	{
		ID: "native-point-multiple-indexed", Entry: Native, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: AllOperations,
		Lanes: NativeLanes, Result: IndependentCalls,
		Note: "separate point calls each maintain exact postings but are not atomic as a group",
	},
	{
		ID: "native-batch-unindexed", Entry: Native, Indexing: Unindexed,
		Tables:      []Tables{OneTable},
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: DeferredBatchLanes, Result: Success, Atomic: true, Rollback: true,
		Note: "Collection.Update publishes one logical failure-atomic cut; topology preparation may advance Generation without changing rows",
	},
	{
		ID: "native-batch-fence-lane", Entry: Native, Indexing: Unindexed,
		Tables:      []Tables{OneTable},
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: FenceBatchErrorLanes, Result: DocumentedError,
		Error: "durable.ErrPrimaryBatchUnsupportedLane",
	},
	{
		ID: "native-batch-indexed", Entry: Native, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: DeferredBatchLanes, Result: Success, Atomic: true, Rollback: true,
		Note: "primary rows and exact postings publish in the same logical batch cut; topology preparation may advance Generation without changing either",
	},
	{
		ID: "native-batch-indexed-fence-lane", Entry: Native, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: FenceBatchErrorLanes, Result: DocumentedError,
		Error: "durable.ErrPrimaryBatchUnsupportedLane",
	},
	{
		ID: "native-database-txn-unindexed", Entry: Native, Indexing: Unindexed,
		Tables:      []Tables{MultipleTables},
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: DatabaseTxnLanes, Result: Success, Atomic: true, Rollback: true,
		Note: "Database.Update / (*vibedb.Database).Update: sync-journal and buffered-journal are crash-atomic after K prepare syncs + decision sync; buffered-journal acknowledgement follows its durability fence; Memory profile is visibility-atomic with no crash dimension",
	},
	{
		ID: "native-database-txn-indexed", Entry: Native, Indexing: Indexed,
		Tables:      []Tables{MultipleTables},
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: DatabaseTxnLanes, Result: Success, Atomic: true, Rollback: true,
		Note: "primary rows and exact postings publish together across every participant; sync-journal and buffered-journal are crash-atomic after K prepare syncs + decision sync",
	},
	{
		ID: "native-database-txn-unsupported-lane", Entry: Native, Indexing: Unindexed,
		Tables:      []Tables{MultipleTables},
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: DatabaseTxnErrorLanes, Result: DocumentedError,
		Error: "durable.ErrDatabaseTransactionUnsupportedLane",
		Note:  "buffered-volatile, async-COW, and sync chain-fence refuse multi-collection commits; the facade Buffered profile maps to the same typed refusal",
	},
	{
		ID: "database-sql-autocommit-unindexed-one", Entry: DatabaseSQL, Indexing: Unindexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: PointSQLOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "database-sql-autocommit-unindexed-multiple", Entry: DatabaseSQL, Indexing: Unindexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: MultiRowSQLOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "one multi-row INSERT or matching DELETE",
	},
	{
		ID: "database-sql-autocommit-unindexed-multiple-update-error", Entry: DatabaseSQL, Indexing: Unindexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: []Operation{Update},
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: DocumentedError,
		Error: "driver.ErrUpdatePrimaryKey",
		Note:  "one constant whole-document replacement cannot preserve several distinct primary keys; use BEGIN with one replacement per key",
	},
	{
		ID: "database-sql-autocommit-unindexed-mixed-error", Entry: DatabaseSQL, Indexing: Unindexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{OneKey, MultipleKeys}, Operations: []Operation{Mixed},
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: DocumentedError,
		Error: "*sql.ParseError (only one statement may be parsed at a time)",
		Note:  "N/A as one database/sql Exec: it accepts one statement; use BEGIN for an atomic mixed group",
	},
	{
		ID: "database-sql-autocommit-indexed-one", Entry: DatabaseSQL, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: PointSQLOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "database-sql-autocommit-indexed-multiple", Entry: DatabaseSQL, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: MultiRowSQLOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "one multi-row INSERT or matching DELETE",
	},
	{
		ID: "database-sql-autocommit-indexed-multiple-update-error", Entry: DatabaseSQL, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: []Operation{Update},
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: DocumentedError,
		Error: "driver.ErrUpdatePrimaryKey",
		Note:  "one constant whole-document replacement cannot preserve several distinct primary keys; use BEGIN with one replacement per key",
	},
	{
		ID: "database-sql-autocommit-indexed-mixed-error", Entry: DatabaseSQL, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{OneKey, MultipleKeys}, Operations: []Operation{Mixed},
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: DocumentedError,
		Error: "*sql.ParseError (only one statement may be parsed at a time)",
		Note:  "N/A as one database/sql Exec: it accepts one statement; use BEGIN for an atomic mixed group",
	},
	{
		ID: "database-sql-transaction-unindexed", Entry: DatabaseSQL, Indexing: Unindexed,
		Tables:      []Tables{OneTable},
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "database-sql-transaction-indexed-one", Entry: DatabaseSQL, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Explicit, Keys: []Keys{OneKey}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "database-sql-transaction-indexed-multiple", Entry: DatabaseSQL, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Explicit, Keys: []Keys{MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "database-sql-transaction-multi-table-unindexed", Entry: DatabaseSQL, Indexing: Unindexed,
		Tables:      []Tables{MultipleTables},
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "fixed SQL default sync-journal: crash-atomic after K prepare syncs + decision sync",
	},
	{
		ID: "database-sql-transaction-multi-table-indexed", Entry: DatabaseSQL, Indexing: Indexed,
		Tables:      []Tables{MultipleTables},
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "fixed SQL default sync-journal: crash-atomic after K prepare syncs + decision sync",
	},
	{
		ID: "database-sql-transaction-savepoint", Entry: DatabaseSQL, Indexing: Unindexed,
		Tables:      []Tables{OneTable, MultipleTables},
		Transaction: Savepoints, Keys: []Keys{OneKey}, Operations: []Operation{Mixed},
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true,
		Note: "SAVEPOINT / ROLLBACK TO / RELEASE restore staged overlays without publishing",
	},
	{
		ID: "pgwire-autocommit-unindexed", Entry: PGWire, Indexing: Unindexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "one simple-query message uses the shared SQL transaction overlay",
	},
	{
		ID: "pgwire-autocommit-indexed-one", Entry: PGWire, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "one simple-query message uses the shared SQL transaction overlay",
	},
	{
		ID: "pgwire-autocommit-indexed-multiple", Entry: PGWire, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "multi-key update and mixed use several statements in one implicit simple-query transaction",
	},
	{
		ID: "pgwire-transaction-unindexed", Entry: PGWire, Indexing: Unindexed,
		Tables:      []Tables{OneTable},
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "pgwire-transaction-indexed-one", Entry: PGWire, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Explicit, Keys: []Keys{OneKey}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "pgwire-transaction-indexed-multiple", Entry: PGWire, Indexing: Indexed,
		Tables:      []Tables{OneTable},
		Transaction: Explicit, Keys: []Keys{MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "pgwire-transaction-multi-table-unindexed", Entry: PGWire, Indexing: Unindexed,
		Tables:      []Tables{MultipleTables},
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "fixed SQL default sync-journal: crash-atomic after K prepare syncs + decision sync",
	},
	{
		ID: "pgwire-transaction-multi-table-indexed", Entry: PGWire, Indexing: Indexed,
		Tables:      []Tables{MultipleTables},
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "fixed SQL default sync-journal: crash-atomic after K prepare syncs + decision sync",
	},
	{
		ID: "pgwire-transaction-savepoint", Entry: PGWire, Indexing: Unindexed,
		Tables:      []Tables{OneTable, MultipleTables},
		Transaction: Savepoints, Keys: []Keys{OneKey}, Operations: []Operation{Mixed},
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true,
		Note: "SAVEPOINT / ROLLBACK TO / RELEASE; ROLLBACK TO recovers a failed transaction to status T",
	},
	{
		ID: "pgwire-transaction-serialization-failure", Entry: PGWire, Indexing: Unindexed,
		Tables:      []Tables{MultipleTables},
		Transaction: Explicit, Keys: []Keys{OneKey}, Operations: []Operation{Update},
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: DocumentedError,
		Error: "SQLSTATE 40001",
		Note:  "first-committer-wins conflict on a multi-table write set surfaces as serialization_failure; no participant publishes",
	},
}

func CasesFor(entry EntryPoint) []Case {
	out := make([]Case, 0, len(Cases))
	for _, c := range Cases {
		if c.Entry == entry {
			out = append(out, c)
		}
	}
	return out
}

// RenderMarkdown is the canonical public capability table. It is intentionally
// compact: each executable row expands its line-broken table, key, operation,
// and lane cells into individual subtests.
func RenderMarkdown() string {
	var b strings.Builder
	b.WriteString("| Executable case | Entry point | Indexing | Tables | Transaction | Keys | Operations | Durability / publication | Result | Atomic unit |\n")
	b.WriteString("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, c := range Cases {
		result := string(c.Result)
		if c.Error != "" {
			result += ": `" + c.Error + "`"
		}
		if c.Note != "" {
			result += "; " + c.Note
		}
		atomic := "no"
		if c.Atomic {
			atomic = "yes"
			if c.Rollback {
				atomic += "; rejected sibling rolls back all participants"
			}
		}
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			c.ID, c.Entry, c.Indexing, join(c.Tables), c.Transaction, join(c.Keys),
			join(c.Operations), join(c.Lanes), result, atomic,
		)
	}
	return b.String()
}

func join[T ~string](values []T) string {
	parts := make([]string, len(values))
	for i := range values {
		parts[i] = string(values[i])
	}
	return strings.Join(parts, "<br>")
}
