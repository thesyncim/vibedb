// Package conformance owns the executable database-capability manifest.
//
// The manifest deliberately lives outside the storage and SQL packages: their
// black-box tests consume it, while the documentation golden test renders the
// same rows. A capability therefore cannot be added to the public table without
// also becoming an executable test case at every named entry point.
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

type Transaction string

const (
	Autocommit Transaction = "autocommit"
	Explicit   Transaction = "explicit transaction"
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

// Case is one row in the public capability table. Tests expand Operations,
// Keys, and Lanes into named subtests. Rollback means every successful atomic
// expansion must also execute a rejected sibling and prove that it published
// neither primary rows nor exact-index changes.
type Case struct {
	ID          string
	Entry       EntryPoint
	Indexing    Indexing
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
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: PointSQLOperations,
		Lanes: NativeLanes, Result: Success, Atomic: true, Rollback: true,
		Note: "one Put/Delete call is one publication",
	},
	{
		ID: "native-point-indexed", Entry: Native, Indexing: Indexed,
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: PointSQLOperations,
		Lanes: NativeLanes, Result: Success, Atomic: true, Rollback: true,
		Note: "the primary row and exact postings publish together",
	},
	{
		ID: "native-point-mixed-one-unindexed", Entry: Native, Indexing: Unindexed,
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: []Operation{Mixed},
		Lanes: NativeLanes, Result: IndependentCalls,
		Note: "mixed point operations on one key are separate publications; use Collection.Update for an atomic group",
	},
	{
		ID: "native-point-mixed-one-indexed", Entry: Native, Indexing: Indexed,
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: []Operation{Mixed},
		Lanes: NativeLanes, Result: IndependentCalls,
		Note: "each point publication maintains exact postings; use Collection.Update for an atomic group",
	},
	{
		ID: "native-point-multiple-unindexed", Entry: Native, Indexing: Unindexed,
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: AllOperations,
		Lanes: NativeLanes, Result: IndependentCalls,
		Note: "separate point calls are not a transaction",
	},
	{
		ID: "native-point-multiple-indexed", Entry: Native, Indexing: Indexed,
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: AllOperations,
		Lanes: NativeLanes, Result: IndependentCalls,
		Note: "separate point calls each maintain exact postings but are not atomic as a group",
	},
	{
		ID: "native-batch-unindexed", Entry: Native, Indexing: Unindexed,
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: DeferredBatchLanes, Result: Success, Atomic: true, Rollback: true,
		Note: "Collection.Update publishes one logical failure-atomic cut; topology preparation may advance Generation without changing rows",
	},
	{
		ID: "native-batch-fence-lane", Entry: Native, Indexing: Unindexed,
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: FenceBatchErrorLanes, Result: DocumentedError,
		Error: "durable.ErrPrimaryBatchUnsupportedLane",
	},
	{
		ID: "native-batch-indexed", Entry: Native, Indexing: Indexed,
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: DeferredBatchLanes, Result: Success, Atomic: true, Rollback: true,
		Note: "primary rows and exact postings publish in the same logical batch cut; topology preparation may advance Generation without changing either",
	},
	{
		ID: "native-batch-indexed-fence-lane", Entry: Native, Indexing: Indexed,
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: FenceBatchErrorLanes, Result: DocumentedError,
		Error: "durable.ErrPrimaryBatchUnsupportedLane",
	},
	{
		ID: "database-sql-autocommit-unindexed-one", Entry: DatabaseSQL, Indexing: Unindexed,
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: PointSQLOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "database-sql-autocommit-unindexed-multiple", Entry: DatabaseSQL, Indexing: Unindexed,
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: MultiRowSQLOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "one multi-row INSERT or matching DELETE",
	},
	{
		ID: "database-sql-autocommit-unindexed-multiple-update-error", Entry: DatabaseSQL, Indexing: Unindexed,
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: []Operation{Update},
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: DocumentedError,
		Error: "driver.ErrUpdatePrimaryKey",
		Note:  "one constant whole-document replacement cannot preserve several distinct primary keys; use BEGIN with one replacement per key",
	},
	{
		ID: "database-sql-autocommit-unindexed-mixed-error", Entry: DatabaseSQL, Indexing: Unindexed,
		Transaction: Autocommit, Keys: []Keys{OneKey, MultipleKeys}, Operations: []Operation{Mixed},
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: DocumentedError,
		Error: "*sql.ParseError (only one statement may be parsed at a time)",
		Note:  "N/A as one database/sql Exec: it accepts one statement; use BEGIN for an atomic mixed group",
	},
	{
		ID: "database-sql-autocommit-indexed-one", Entry: DatabaseSQL, Indexing: Indexed,
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: PointSQLOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "database-sql-autocommit-indexed-multiple", Entry: DatabaseSQL, Indexing: Indexed,
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: MultiRowSQLOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "one multi-row INSERT or matching DELETE",
	},
	{
		ID: "database-sql-autocommit-indexed-multiple-update-error", Entry: DatabaseSQL, Indexing: Indexed,
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: []Operation{Update},
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: DocumentedError,
		Error: "driver.ErrUpdatePrimaryKey",
		Note:  "one constant whole-document replacement cannot preserve several distinct primary keys; use BEGIN with one replacement per key",
	},
	{
		ID: "database-sql-autocommit-indexed-mixed-error", Entry: DatabaseSQL, Indexing: Indexed,
		Transaction: Autocommit, Keys: []Keys{OneKey, MultipleKeys}, Operations: []Operation{Mixed},
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: DocumentedError,
		Error: "*sql.ParseError (only one statement may be parsed at a time)",
		Note:  "N/A as one database/sql Exec: it accepts one statement; use BEGIN for an atomic mixed group",
	},
	{
		ID: "database-sql-transaction-unindexed", Entry: DatabaseSQL, Indexing: Unindexed,
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "database-sql-transaction-indexed-one", Entry: DatabaseSQL, Indexing: Indexed,
		Transaction: Explicit, Keys: []Keys{OneKey}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "database-sql-transaction-indexed-multiple", Entry: DatabaseSQL, Indexing: Indexed,
		Transaction: Explicit, Keys: []Keys{MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "pgwire-autocommit-unindexed", Entry: PGWire, Indexing: Unindexed,
		Transaction: Autocommit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "one simple-query message uses the shared SQL transaction overlay",
	},
	{
		ID: "pgwire-autocommit-indexed-one", Entry: PGWire, Indexing: Indexed,
		Transaction: Autocommit, Keys: []Keys{OneKey}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "one simple-query message uses the shared SQL transaction overlay",
	},
	{
		ID: "pgwire-autocommit-indexed-multiple", Entry: PGWire, Indexing: Indexed,
		Transaction: Autocommit, Keys: []Keys{MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
		Note: "multi-key update and mixed use several statements in one implicit simple-query transaction",
	},
	{
		ID: "pgwire-transaction-unindexed", Entry: PGWire, Indexing: Unindexed,
		Transaction: Explicit, Keys: []Keys{OneKey, MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "pgwire-transaction-indexed-one", Entry: PGWire, Indexing: Indexed,
		Transaction: Explicit, Keys: []Keys{OneKey}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
	},
	{
		ID: "pgwire-transaction-indexed-multiple", Entry: PGWire, Indexing: Indexed,
		Transaction: Explicit, Keys: []Keys{MultipleKeys}, Operations: AllOperations,
		Lanes: []Lane{SQLDefaultSyncJournal}, Result: Success, Atomic: true, Rollback: true,
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
// compact: each executable row expands its line-broken key, operation, and lane
// cells into individual subtests.
func RenderMarkdown() string {
	var b strings.Builder
	b.WriteString("| Executable case | Entry point | Indexing | Transaction | Keys | Operations | Durability / publication | Result | Atomic unit |\n")
	b.WriteString("| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
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
				atomic += "; rejected sibling rolls back"
			}
		}
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			c.ID, c.Entry, c.Indexing, c.Transaction, join(c.Keys),
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
