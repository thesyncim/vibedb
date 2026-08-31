package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"unicode/utf8"
	"unsafe"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	vibejsonsimd "github.com/thesyncim/vibejson/simd"
	"github.com/thesyncim/vibejson/x/byteview"
	"github.com/thesyncim/vibejson/x/scanner"
)

var errNoLastInsertID = errors.New("vibedb: LastInsertId is unavailable; primary keys come from JSON documents")

// insertSelectPlan is the driver-owned catalog binding for the independently
// compiled source of INSERT ... SELECT. The query.Statement itself is owned
// and released by DMLStatement; this sidecar retains only immutable routing
// metadata computed once at prepare time.
type insertSelectPlan struct {
	statement    *query.Statement
	tree         *sqlast.SelectStmt
	dependencies []physicalDependency
	catalogJoin  bool
}

const insertSelectIntermediateResource = "INSERT SELECT source, atomic staging, and RETURNING"

type insertSelectIntermediateBudget struct {
	limit int64
	used  int64
}

func newInsertSelectIntermediateBudget(
	options query.ExecOptions,
	sourceBytes int64,
) (insertSelectIntermediateBudget, error) {
	limit := options.IntermediateBytes
	switch {
	case limit < -1:
		return insertSelectIntermediateBudget{}, fmt.Errorf(
			"query: IntermediateBytes must be -1, zero, or positive, got %d",
			limit,
		)
	case limit == 0:
		limit = query.DefaultIntermediateBytes
	}
	budget := insertSelectIntermediateBudget{limit: limit}
	if err := budget.admit(sourceBytes); err != nil {
		return insertSelectIntermediateBudget{}, err
	}
	return budget, nil
}

func (b *insertSelectIntermediateBudget) admit(bytes int64) error {
	if bytes <= 0 {
		return nil
	}
	required := b.used + bytes
	if required < b.used {
		required = math.MaxInt64
	}
	if b.limit >= 0 && required > b.limit {
		return &query.IntermediateBudgetError{
			Resource: insertSelectIntermediateResource,
			Bytes:    required,
			Limit:    b.limit,
		}
	}
	b.used = required
	return nil
}

func (b *insertSelectIntermediateBudget) release(bytes int64) {
	if bytes <= 0 {
		return
	}
	b.used -= bytes
	if b.used < 0 {
		b.used = 0
	}
}

func insertSelectStagedRowBytes(
	document []byte,
	key string,
	tape []vibejson.IndexEntry,
) int64 {
	return insertSelectStagedRowBytesFor(
		document, key, tape, int64(unsafe.Sizeof(seedDocument{})),
	)
}

func insertSelectStagedRowBytesFor(
	document []byte,
	key string,
	tape []vibejson.IndexEntry,
	recordBytes int64,
) int64 {
	bytes := insertSelectSaturatedAdd(int64(len(document)), int64(len(key)))
	// pointDocs rebuilds and retains one equivalent classic tape per staged
	// document. The validation tape supplies its exact entry cardinality.
	tapeBytes := insertSelectSaturatedProduct(
		len(tape), int64(unsafe.Sizeof(vibejson.IndexEntry{})),
	)
	bytes = insertSelectSaturatedAdd(bytes, tapeBytes)
	// The logical staging account includes the retained publication record and
	// document handle in addition to their variable-width bytes. This is
	// conservative for compact/shape-taped Segment representations and keeps
	// admission independent of whichever representation Segment selects.
	bytes = insertSelectSaturatedAdd(bytes, recordBytes)
	bytes = insertSelectSaturatedAdd(
		bytes, int64(unsafe.Sizeof(vibejson.Index{})),
	)
	return bytes
}

func insertSelectSaturatedAdd(left, right int64) int64 {
	if left < 0 || right < 0 || left > math.MaxInt64-right {
		return math.MaxInt64
	}
	return left + right
}

func insertSelectSaturatedProduct(count int, width int64) int64 {
	if count < 0 || width < 0 || (width != 0 && int64(count) > math.MaxInt64/width) {
		return math.MaxInt64
	}
	return int64(count) * width
}

// runInsertSelectReturning executes RETURNING inside the same logical
// IntermediateBytes account as the source and atomic staging. RETURNING
// invalidates the source root Result, so only that root is released; nested
// source spools and every staged document remain live until publication.
func runInsertSelectReturning(
	returning *query.Statement,
	exec *query.Exec,
	source query.Source,
	budget *insertSelectIntermediateBudget,
	sourceRootBytes int64,
) (query.Cursor, error) {
	if returning == nil || exec == nil || budget == nil {
		return query.Cursor{}, errors.New(
			"vibedb: internal INSERT SELECT RETURNING account is incomplete",
		)
	}
	if sourceRootBytes < 0 || sourceRootBytes > budget.used {
		return query.Cursor{}, fmt.Errorf(
			"vibedb: internal INSERT SELECT source root charge %d exceeds live charge %d",
			sourceRootBytes, budget.used,
		)
	}
	budget.release(sourceRootBytes)
	base := budget.used
	remaining := int64(-1)
	if budget.limit >= 0 {
		remaining = budget.limit - base
	}

	options := exec.Options
	childLimit := remaining
	if childLimit == 0 {
		// Zero selects query's default limit. Use the smallest real limit instead
		// so RunIntermediateInto still invalidates the source Result, arbitrates
		// ResultRows/ResultBytes normally, and reports the exact first rejected
		// RETURNING shape. A valid SQL RETURNING list always retains more than one
		// byte; the parent admission below remains the fail-closed backstop.
		childLimit = 1
	}
	exec.Options.IntermediateBytes = childLimit
	cursor, retained, err := returning.RunIntermediateInto(exec, source, nil)
	exec.Options = options
	if err != nil {
		var intermediate *query.IntermediateBudgetError
		if errors.As(err, &intermediate) {
			return query.Cursor{}, &query.IntermediateBudgetError{
				Resource: insertSelectIntermediateResource,
				Bytes: insertSelectSaturatedAdd(
					base, intermediate.Bytes,
				),
				Limit: budget.limit,
			}
		}
		return query.Cursor{}, err
	}
	// The child just proved this admission against the exact remainder. Keep
	// the parent account honest for the cancellation/publication tail.
	if err := budget.admit(retained); err != nil {
		return query.Cursor{}, err
	}
	return cursor, nil
}

type result struct{ affected int64 }

var _ sqldriver.Result = result{}

func (result) LastInsertId() (int64, error) { return 0, errNoLastInsertID }
func (r result) RowsAffected() (int64, error) {
	return r.affected, nil
}

func (c *conn) execMutation(statement *query.DMLStatement, args []any) (sqldriver.Result, error) {
	return c.execMutationContext(backgroundContext, statement, args)
}

func (c *conn) execMutationContext(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
) (sqldriver.Result, error) {
	return c.execMutationCoreContext(ctx, statement, args, nil)
}

func (c *conn) execPreparedMutationContext(
	ctx context.Context,
	prepared *stmt,
	args []any,
) (sqldriver.Result, error) {
	if prepared == nil {
		return nil, errors.New("vibedb: internal prepared mutation is nil")
	}
	return c.execMutationCoreContext(ctx, prepared.mutation, args, prepared)
}

func (c *conn) execMutationCoreContext(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	prepared *stmt,
) (sqldriver.Result, error) {
	if err := c.requireDirectWriteAllowed(); err != nil {
		return nil, err
	}
	ctx = withCooperativeCancellation(ctx, c.exec.Options.Cancel)
	d := c.db
	if statement.Kind() == query.DDLCreateIndex {
		return d.createIndexContext(ctx, statement)
	}
	if err := lockContext(ctx, &d.mu); err != nil {
		return nil, err
	}
	defer d.mu.Unlock()
	if d.closed {
		return nil, sqldriver.ErrBadConn
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	if err := d.settleCatalogLocked(); err != nil {
		return nil, err
	}
	if prepared != nil {
		if err := prepared.validateViewDependenciesLocked(); err != nil {
			return nil, err
		}
	}
	if err := d.validateViewTableTargetLocked(statement.Tree()); err != nil {
		return nil, err
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	switch statement.Kind() {
	case query.DDLCreateTable:
		return d.createTableLockedContext(ctx, statement)
	case query.DDLAlterTable:
		if err := d.alterTableAddColumnStorageLockedContext(ctx, statement); err != nil {
			return nil, err
		}
		return result{}, nil
	case query.DDLDropTable:
		return d.dropTableLockedContext(ctx, statement)
	case query.DDLTruncate:
		if err := d.truncateTableStorageLockedContext(
			ctx, statement.Tree().Truncate.Table,
		); err != nil {
			return nil, err
		}
		return result{}, nil
	case query.DDLDropIndex:
		tableName, found, err := d.resolveDropIndexLocked(statement.Tree().DropIndex)
		if err != nil {
			return nil, err
		}
		if !found {
			return result{}, nil
		}
		if err := d.dropIndexStorageLockedContext(
			ctx, tableName, statement.Tree().DropIndex.Name,
		); err != nil {
			return nil, err
		}
		return result{}, nil
	}
	t, ok := d.tables[statement.Collection()]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, statement.Collection())
	}
	switch statement.Kind() {
	case query.DMLInsert:
		var source *insertSelectPlan
		if prepared != nil {
			source = prepared.insertSource
		}
		return c.insertLocked(ctx, statement, args, t, source, nil, nil)
	case query.DMLUpdate:
		return c.updateLocked(ctx, statement, args, t)
	case query.DMLDelete:
		return c.deleteLocked(ctx, statement, args, t)
	}
	return nil, fmt.Errorf("vibedb: unsupported statement %s", statement.Kind())
}

func (d *database) createTableLocked(statement *query.DMLStatement) (sqldriver.Result, error) {
	return d.createTableLockedContext(backgroundContext, statement)
}

// dropTableLockedContext publishes the namespace removal before attempting
// physical cleanup. That ordering is the essential durability rule: a crash
// after the catalog rename may leave an unreachable table file, but a crash
// after deleting the file must never leave the catalog claiming the table
// still exists. Existing snapshots are retained in retired until their
// collection can close, so DROP remains safe with cursors that outlive it.
func (d *database) dropTableLockedContext(
	ctx context.Context,
	statement *query.DMLStatement,
) (sqldriver.Result, error) {
	drop := statement.Tree().DropTable
	name := drop.Table
	t, exists := d.tables[name]
	if !exists {
		if drop.IfExists {
			return result{}, nil
		}
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, name)
	}
	if dependent := d.firstDependentViewLocked(name, false); dependent != "" {
		return nil, fmt.Errorf(
			"%w: table %q is required by view %q; DROP dependent views first",
			ErrDependentObjects, name, dependent,
		)
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	retire := t.collection != nil || t.file != nil
	if retire {
		if err := d.checkRetirementCapacityLocked(1); err != nil {
			return nil, err
		}
	}
	detached := false
	if t.collection != nil {
		if d.txnLog == nil {
			return nil, errors.New("vibedb: database transaction log is not open")
		}
		if err := d.txnLog.DetachCollection(t.collection); err != nil {
			return nil, fmt.Errorf(
				"vibedb: detach dropped table %q from transaction log: %w",
				name, err,
			)
		}
		detached = true
	}
	previousPending := d.catalogWritePending
	retiredBefore := len(d.retired)
	meta := d.catalog.Tables[name]
	storagePath := d.tablePathForMeta(meta)
	delete(d.catalog.Tables, name)
	delete(d.tables, name)
	if retire {
		d.retired = append(d.retired, retiredTable{
			name: name, path: storagePath,
			journal: durable.RecoveryJournalPath(storagePath),
			file:    t.file, collection: t.collection,
		})
	}
	published, err := d.persistCatalogLocked()
	if err != nil {
		if !published {
			d.catalog.Tables[name] = meta
			d.tables[name] = t
			d.retired = d.retired[:retiredBefore]
			d.catalogWritePending = previousPending
			if detached {
				adoptErr := d.txnLog.AdoptCollection(t.collection)
				if adoptErr != nil {
					d.retainTxnReattachLocked(t.collection)
				}
				err = errors.Join(err, adoptErr)
			}
		} else {
			d.advanceLayoutEpochLocked()
		}
		return nil, err
	}
	d.advanceLayoutEpochLocked()
	if err := d.settleDroppedTablesLocked(); err != nil {
		return nil, err
	}
	return result{}, nil
}

func (d *database) createTableLockedContext(
	ctx context.Context,
	statement *query.DMLStatement,
) (sqldriver.Result, error) {
	definition, err := statement.LowerTable()
	if err != nil {
		return nil, err
	}
	if err := validateCatalogTableName(definition.Name); err != nil {
		return nil, fmt.Errorf("vibedb: CREATE TABLE: %w", err)
	}
	if _, exists := d.tables[definition.Name]; exists {
		if definition.IfNotExists {
			return result{}, nil
		}
		return nil, fmt.Errorf("%w: %q", ErrTableExists, definition.Name)
	}
	if _, exists := d.catalog.Views[definition.Name]; exists {
		if definition.IfNotExists {
			return result{}, nil
		}
		return nil, fmt.Errorf(
			"%w: relation %q is an ordinary view", ErrTableExists, definition.Name,
		)
	}
	if err := checkCatalogTableCount(len(d.tables) + 1); err != nil {
		return nil, fmt.Errorf("vibedb: CREATE TABLE %q: %w",
			definition.Name, err)
	}
	if len(definition.PrimaryKey) != 1 {
		return nil, fmt.Errorf("vibedb: CREATE TABLE requires exactly one PRIMARY KEY JSON path")
	}
	path := statement.Tree().CreateTable.PrimaryKey[0]
	pointer := string(path.AppendPointer(nil))
	if err := d.cluster.validatePlacementLocality(definition.Name, pointer); err != nil {
		return nil, err
	}
	primary, err := vibejson.CompilePointer(pointer)
	if err != nil {
		return nil, fmt.Errorf("vibedb: compile primary-key path %q: %w", pointer, err)
	}
	if err := validatePrimarySchema(pointer, definition.Schema); err != nil {
		return nil, fmt.Errorf(
			"vibedb: CREATE TABLE %q primary key: %w",
			definition.Name, err,
		)
	}
	storage, err := d.newStorageIdentityLocked()
	if err != nil {
		return nil, err
	}
	meta := &tableMeta{
		PrimaryKey: pointer,
		Schema:     schemaMetaFrom(definition.Schema),
		Storage:    storage,
	}
	candidate := &table{
		meta: meta, schema: definition.Schema, primary: primary,
	}
	if err := durable.ValidateOptions(durableOptions(candidate)); err != nil {
		return nil, fmt.Errorf(
			"vibedb: CREATE TABLE %q cannot be represented durably: %w",
			definition.Name, err)
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	d.catalog.Tables[definition.Name] = meta
	d.tables[definition.Name] = candidate
	if published, err := d.persistCatalogLocked(); err != nil {
		if !published {
			delete(d.catalog.Tables, definition.Name)
			delete(d.tables, definition.Name)
		} else {
			d.advanceLayoutEpochLocked()
		}
		return nil, err
	}
	d.advanceLayoutEpochLocked()
	return result{}, nil
}

func (d *database) createIndexLocked(statement *query.DMLStatement) (sqldriver.Result, error) {
	return d.createIndexLockedContext(backgroundContext, statement)
}

// createIndexContext keeps the catalog mutex only around validation and the
// final metadata publication. A materialized table delegates the expensive
// scan to durable.Collection.CreateIndexContext, whose optimistic leaf
// reconciliation leaves concurrent SQL reads and writes running.
func (d *database) createIndexContext(
	ctx context.Context,
	statement *query.DMLStatement,
) (sqldriver.Result, error) {
	if err := lockContext(ctx, &d.mu); err != nil {
		return nil, err
	}
	if d.closed {
		d.mu.Unlock()
		return nil, sqldriver.ErrBadConn
	}
	if d.catalog.ReplicatedShardStore != nil {
		d.mu.Unlock()
		return nil, ErrDirectWriteFenced
	}
	if err := d.settleCatalogLocked(); err != nil {
		d.mu.Unlock()
		return nil, err
	}
	if err := d.validateViewTableTargetLocked(statement.Tree()); err != nil {
		d.mu.Unlock()
		return nil, err
	}
	definition, err := statement.LowerIndex()
	if err != nil {
		d.mu.Unlock()
		return nil, err
	}
	if _, err := store.CompileExactIndex(definition.Definition); err != nil {
		d.mu.Unlock()
		return nil, fmt.Errorf("vibedb: CREATE INDEX: %w", err)
	}
	t, exists := d.tables[definition.Table]
	if !exists {
		d.mu.Unlock()
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, definition.Table)
	}
	if definition.Unique {
		if err := d.cluster.validateUniqueIndexLocality(
			definition.Table, definition.Definition.Name,
			definition.Definition.Paths,
		); err != nil {
			d.mu.Unlock()
			return nil, err
		}
	}
	for _, index := range t.meta.Indexes {
		if index.Name == definition.Definition.Name {
			d.mu.Unlock()
			if definition.IfNotExists {
				return result{}, nil
			}
			return nil, fmt.Errorf("%w: %q", ErrIndexExists, index.Name)
		}
	}
	if definition.Unique && t.collection != nil {
		defer d.mu.Unlock()
		return d.createUniqueIndexMaterializedLockedContext(
			ctx, definition, t,
		)
	}
	if t.collection == nil {
		defer d.mu.Unlock()
		return d.createIndexLockedContext(ctx, statement)
	}
	collection := t.collection
	if err := contextCheckpoint(ctx); err != nil {
		d.mu.Unlock()
		return nil, err
	}
	d.mu.Unlock()

	_, err = collection.CreateIndexContext(ctx, definition.Definition)
	if err != nil {
		// CreateIndexContext observes cancellation only before its atomic
		// publication transaction, so a cancellation result proves no index was
		// committed. Return it before reacquiring the catalog lock: another DDL
		// may hold that lock while replacing this table, and cancellation must not
		// block again or be rewritten as a later serialization conflict.
		if cancelErr := contextCancellationError(ctx); cancelErr != nil &&
			errors.Is(err, cancelErr) {
			return nil, err
		}
	}
	// Catalog DDL may replace the table while the durable online build is
	// running without the catalog mutex. Check the exact object identity before
	// interpreting any durable result: retirement can deliberately close the
	// old collection, and that implementation detail must surface as a logical
	// serialization conflict rather than ErrClosed. A second identity check is
	// still required under the publication lock below because replacement can
	// race this observation.
	d.mu.RLock()
	current, exists := d.tables[definition.Table]
	d.mu.RUnlock()
	if !exists || current != t {
		return nil, fmt.Errorf(
			"%w: table %q storage changed during CREATE INDEX",
			ErrTransactionConflict, definition.Table,
		)
	}
	if errors.Is(err, durable.ErrCommitOutcomeUnknown) {
		// The durable root may already expose the new index. Mirror whatever can
		// be observed, retain a pending SQL-catalog repair even if the poisoned
		// collection cannot be snapshotted, and fence the retained/unknown layout
		// before returning the durability error.
		d.mu.Lock()
		if current, exists := d.tables[definition.Table]; !exists || current != t {
			d.mu.Unlock()
			return nil, fmt.Errorf(
				"%w: table %q storage changed during CREATE INDEX",
				ErrTransactionConflict, definition.Table,
			)
		}
		_, _ = syncTableIndexMeta(t)
		d.catalogWritePending = true
		d.advanceLayoutEpochLocked()
		d.mu.Unlock()
		return nil, err
	}
	if errors.Is(err, store.ErrIndexExists) && definition.IfNotExists {
		err = nil
	}
	if errors.Is(err, store.ErrIndexExists) {
		return nil, fmt.Errorf(
			"%w: %q", ErrIndexExists, definition.Definition.Name,
		)
	}
	if errors.Is(err, durable.ErrIndexBuildInProgress) {
		return nil, fmt.Errorf(
			"%w: table %q", ErrIndexBuildInProgress, definition.Table,
		)
	}
	if err != nil {
		return nil, err
	}

	if err := lockContext(ctx, &d.mu); err != nil {
		// The durable catalog is already authoritative for this exact storage
		// incarnation. A canceled SQL metadata mirror is repaired on reopen or by
		// the next catalog settlement. If catalog DDL replaced the incarnation
		// while the online build ran, the committed index belongs only to retired
		// storage and must never be acknowledged as part of the current table.
		d.mu.Lock()
		if current, exists := d.tables[definition.Table]; !exists || current != t {
			d.mu.Unlock()
			return nil, fmt.Errorf(
				"%w: table %q storage changed during CREATE INDEX",
				ErrTransactionConflict, definition.Table,
			)
		}
		_, _ = syncTableIndexMeta(t)
		d.catalogWritePending = true
		d.advanceLayoutEpochLocked()
		d.mu.Unlock()
		return nil, fmt.Errorf(
			"%w: durable index committed before SQL catalog cancellation: %v",
			durable.ErrCommitOutcomeUnknown, err,
		)
	}
	defer d.mu.Unlock()
	if current, exists := d.tables[definition.Table]; !exists || current != t {
		return nil, fmt.Errorf(
			"%w: table %q storage changed during CREATE INDEX",
			ErrTransactionConflict, definition.Table,
		)
	}
	// The durable index publication is already visible through snapshots of
	// this exact table object, even if refreshing or persisting its SQL mirror
	// fails below.
	d.advanceLayoutEpochLocked()
	if _, syncErr := syncTableIndexMeta(t); syncErr != nil {
		d.catalogWritePending = true
		return nil, fmt.Errorf(
			"%w: durable index committed before SQL catalog refresh: %v",
			durable.ErrCommitOutcomeUnknown, syncErr,
		)
	}
	published, persistErr := d.persistCatalogLocked()
	if persistErr != nil {
		d.catalogWritePending = !published
		if published {
			return nil, persistErr
		}
		return nil, fmt.Errorf(
			"%w: durable index committed before SQL catalog publication: %v",
			durable.ErrCommitOutcomeUnknown, persistErr,
		)
	}
	return result{}, nil
}

// createUniqueIndexMaterializedLockedContext holds db.mu across the durable
// build and SQL-catalog publication. Until the uniqueness bit is visible in
// the in-memory layout, that fence prevents an ordinary SQL writer from
// inserting a duplicate after durable validation but before metadata install.
// The durable alias also persists Unique, so crash recovery cannot downgrade a
// successfully published constraint when the SQL mirror trails it.
func (d *database) createUniqueIndexMaterializedLockedContext(
	ctx context.Context,
	definition query.IndexDefinition,
	t *table,
) (sqldriver.Result, error) {
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	_, err := t.collection.CreateUniqueIndexContext(ctx, definition.Definition)
	if err != nil {
		if errors.Is(err, store.ErrUniqueIndexViolation) {
			return nil, fmt.Errorf(
				"%w: index %q found duplicate existing values",
				ErrUniqueConstraint, definition.Definition.Name,
			)
		}
		if errors.Is(err, store.ErrIndexExists) && definition.IfNotExists {
			return result{}, nil
		}
		if errors.Is(err, store.ErrIndexExists) {
			return nil, fmt.Errorf(
				"%w: %q", ErrIndexExists, definition.Definition.Name,
			)
		}
		if errors.Is(err, durable.ErrIndexBuildInProgress) {
			return nil, fmt.Errorf(
				"%w: table %q", ErrIndexBuildInProgress, definition.Table,
			)
		}
		if errors.Is(err, durable.ErrCommitOutcomeUnknown) {
			// The durable page catalog is authoritative for the Unique bit. Mirror
			// whatever is observable and retain a pending SQL-catalog repair.
			_, _ = syncTableIndexMeta(t)
			d.catalogWritePending = true
			d.advanceLayoutEpochLocked()
			return nil, err
		}
		return nil, err
	}

	// A nil durable result proves publication. Install the in-memory constraint
	// even if reading the durable catalog or persisting its SQL mirror fails;
	// subsequent SQL writes must remain fenced by the new invariant.
	t.meta.Indexes = append(t.meta.Indexes, indexMeta{
		Name:   definition.Definition.Name,
		Paths:  append([]string(nil), definition.Definition.Paths...),
		Unique: true,
	})
	if _, syncErr := syncTableIndexMeta(t); syncErr != nil {
		d.catalogWritePending = true
		d.advanceLayoutEpochLocked()
		return nil, fmt.Errorf(
			"%w: durable unique index committed before SQL catalog refresh: %v",
			durable.ErrCommitOutcomeUnknown, syncErr,
		)
	}
	d.advanceLayoutEpochLocked()
	published, persistErr := d.persistCatalogLocked()
	if persistErr != nil {
		d.catalogWritePending = !published
		if published {
			return nil, persistErr
		}
		return nil, fmt.Errorf(
			"%w: durable unique index committed before SQL catalog publication: %v",
			durable.ErrCommitOutcomeUnknown, persistErr,
		)
	}
	return result{}, nil
}

func (d *database) createIndexLockedContext(
	ctx context.Context,
	statement *query.DMLStatement,
) (sqldriver.Result, error) {
	if d.catalog.ReplicatedShardStore != nil {
		return nil, ErrDirectWriteFenced
	}
	if err := d.validateViewTableTargetLocked(statement.Tree()); err != nil {
		return nil, err
	}
	definition, err := statement.LowerIndex()
	if err != nil {
		return nil, err
	}
	if _, err := store.CompileExactIndex(definition.Definition); err != nil {
		return nil, fmt.Errorf("vibedb: CREATE INDEX: %w", err)
	}
	t, exists := d.tables[definition.Table]
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, definition.Table)
	}
	if definition.Unique {
		if err := d.cluster.validateUniqueIndexLocality(
			definition.Table, definition.Definition.Name,
			definition.Definition.Paths,
		); err != nil {
			return nil, err
		}
	}
	for _, index := range t.meta.Indexes {
		if index.Name == definition.Definition.Name {
			if definition.IfNotExists {
				return result{}, nil
			}
			return nil, fmt.Errorf("%w: %q", ErrIndexExists, index.Name)
		}
	}
	if t.collection != nil {
		return nil, errors.New(
			"vibedb: materialized CREATE INDEX reached the catalog-only path",
		)
	}
	proposed := append([]indexMeta(nil), t.meta.Indexes...)
	proposed = append(proposed, indexMeta{
		Name:   definition.Definition.Name,
		Paths:  append([]string(nil), definition.Definition.Paths...),
		Unique: definition.Unique,
	})
	candidateMeta := *t.meta
	candidateMeta.Indexes = proposed
	if err := durable.ValidateOptions(durableOptions(&table{
		meta: &candidateMeta, schema: t.schema,
	})); err != nil {
		return nil, fmt.Errorf(
			"vibedb: CREATE INDEX %q cannot be represented durably: %w",
			definition.Definition.Name, err)
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	previous := t.meta.Indexes
	t.meta.Indexes = proposed
	if published, err := d.persistCatalogLocked(); err != nil {
		if !published {
			t.meta.Indexes = previous
		} else {
			d.advanceLayoutEpochLocked()
		}
		return nil, err
	}
	d.advanceLayoutEpochLocked()
	return result{}, nil
}

func (c *conn) insertLocked(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	t *table,
	source *insertSelectPlan,
	returning *query.Statement,
	returned *query.Cursor,
) (sqldriver.Result, error) {
	tree := statement.Tree().Insert
	limits, err := tableMutationLimits(t)
	if err != nil {
		return nil, err
	}
	if source == nil && len(tree.Rows) > limits.MaxBatchDocuments {
		return nil, fmt.Errorf(
			"vibedb: INSERT has %d rows, durable batch limit is %d: %w",
			len(tree.Rows), limits.MaxBatchDocuments, durable.ErrBatchTooLarge,
		)
	}
	if source != nil {
		return c.insertSelectLocked(
			ctx, statement, args, t, limits, source, returning, returned,
		)
	}
	conflictUpdate := tree.OnConflictUpdate
	if conflictUpdate != nil && len(conflictUpdate.Assignments) != 0 {
		if err := validateUpsertColumnAssignments(
			tree.Table, t.meta, conflictUpdate.Assignments,
		); err != nil {
			return nil, err
		}
		if err := validateColumnAssignmentBindings(
			conflictUpdate.Assignments, args,
		); err != nil {
			return nil, err
		}
		if err := statement.ValidateConflictUpdateExpressionBindings(args); err != nil {
			return nil, err
		}
	}

	// Resolve and validate every candidate before deciding which conflict
	// branch it takes. Besides matching SQL constraint order, this lets routing
	// preflight the authored INSERT batch rather than a set of post-images whose
	// conflict updates could conceal a cross-shard candidate.
	candidates := make([]seedDocument, 0, len(tree.Rows))
	var seen map[string]struct{}
	if len(tree.Rows) > 1 {
		seen = make(map[string]struct{}, len(tree.Rows))
	}
	candidateBytes := 0
	cancellable := contextCanCancel(ctx)
	for i := range tree.Rows {
		if cancellable {
			if err := contextCheckpoint(ctx); err != nil {
				return nil, err
			}
		}
		document, key, err := resolveInsertRow(
			statement, tree, &tree.Rows[i], args,
			t.meta.PrimaryKey, t.primary, limits,
		)
		if err != nil {
			return nil, err
		}
		if tree.HasConflictAction() {
			if err := validateDocument(
				t.schema, document, limits.MaxDocumentBytes, &c.insertTape,
			); err != nil {
				return nil, err
			}
		}
		if _, duplicate := seen[key]; duplicate {
			if tree.OnConflictDoNothing {
				continue
			}
			if conflictUpdate != nil {
				return nil, fmt.Errorf(
					"%w: candidate key %q appears twice in one VALUES batch",
					ErrUpsertCardinality, key,
				)
			}
			return nil, fmt.Errorf(
				"%w: %q appears twice in one VALUES batch",
				ErrDuplicatePrimaryKey, key,
			)
		}
		if seen != nil {
			seen[key] = struct{}{}
		}
		if len(key) > limits.MaxBatchBytes-candidateBytes ||
			len(document) > limits.MaxBatchBytes-candidateBytes-len(key) {
			return nil, fmt.Errorf(
				"vibedb: INSERT exceeds the %d-byte durable batch limit: %w",
				limits.MaxBatchBytes, durable.ErrBatchTooLarge,
			)
		}
		candidateBytes += len(key) + len(document)
		candidates = append(candidates, seedDocument{key: key, document: document})
	}
	if err := c.routeInsertSeeds(tree.Table, candidates); err != nil {
		return nil, err
	}

	seeds := candidates
	if tree.HasConflictAction() && t.collection != nil {
		snapshot, snapshotErr := t.collection.Snapshot()
		if snapshotErr != nil {
			return nil, snapshotErr
		}
		defer snapshot.Close()

		seeds = candidates[:0]
		scratch := c.pointRaw[:0]
		defer func() { c.pointRaw = scratch[:0] }()
		stagedBytes := 0
		for i := range candidates {
			if cancellable {
				if err := contextCheckpoint(ctx); err != nil {
					return nil, err
				}
			}
			candidate := candidates[i]
			var found bool
			scratch, found, err = snapshot.AppendRaw(
				scratch[:0], []byte(candidate.key),
			)
			if err != nil {
				return nil, err
			}
			if found && tree.OnConflictDoNothing {
				continue
			}
			finalDocument := candidate.document
			if found && conflictUpdate != nil {
				if !conflictUpdate.WholeDocument() {
					finalDocument, err = materializeConflictColumnAssignments(
						statement, &c.exec, scratch, candidate.document,
						conflictUpdate.Assignments, args, limits.MaxDocumentBytes,
					)
					if err != nil {
						return nil, err
					}
				}
				if err := validateDocument(
					t.schema, finalDocument, limits.MaxDocumentBytes,
					&c.insertTape,
				); err != nil {
					return nil, err
				}
				// Keep UPDATE's shard-move error precedence even though primary-key
				// locality currently makes an unchanged key sufficient in placed
				// tables. The explicit check preserves that invariant if placement
				// grows beyond the present single-path key contract.
				if err := c.checkUpsertShardKeyImmutable(
					tree.Table, scratch, finalDocument,
				); err != nil {
					return nil, err
				}
				finalKey, err := documentKey(
					finalDocument, t.meta.PrimaryKey, t.primary,
					limits.MaxKeyBytes,
				)
				if err != nil {
					return nil, err
				}
				if finalKey != candidate.key {
					return nil, fmt.Errorf(
						"%w: conflict replacement key %q does not match candidate key %q",
						ErrUpdatePrimaryKey, finalKey, candidate.key,
					)
				}
			}
			if len(candidate.key) > limits.MaxBatchBytes-stagedBytes ||
				len(finalDocument) > limits.MaxBatchBytes-stagedBytes-len(candidate.key) {
				return nil, fmt.Errorf(
					"vibedb: INSERT conflict action exceeds the %d-byte mutation batch limit for table %q: %w",
					limits.MaxBatchBytes, tree.Table, durable.ErrBatchTooLarge,
				)
			}
			seeds = append(seeds, seedDocument{
				key: candidate.key, document: finalDocument,
			})
			stagedBytes += len(candidate.key) + len(finalDocument)
		}
	}
	if err := validateTableUniquePostimages(ctx, t, seeds, nil); err != nil {
		return nil, err
	}
	if returning != nil {
		if returned == nil {
			return nil, errors.New("vibedb: internal RETURNING cursor is nil")
		}
		c.pointDocs.Reset()
		for i := range seeds {
			if _, err := c.pointDocs.Append(seeds[i].document); err != nil {
				return nil, err
			}
		}
		cursor, err := returning.RunInto(
			&c.exec, query.FromSegment(&c.pointDocs), nil,
		)
		if err != nil {
			return nil, err
		}
		*returned = cursor
	}
	if t.collection == nil {
		if err := contextCheckpoint(ctx); err != nil {
			return nil, err
		}
		_, err := c.db.materializeLocked(tree.Table, seeds)
		if t.collection != nil {
			// materializeLocked retains the live collection when the file was
			// published but its namespace fence could not be acknowledged.
			// Such an unknown outcome is still a logical write for every
			// transaction that began before it.
			t.conflicts.recordSeeds(seeds)
		}
		if err != nil {
			return nil, err
		}
		return result{affected: int64(len(seeds))}, nil
	}
	if !tree.HasConflictAction() {
		if err := c.rejectExistingSeeds(ctx, t.collection, seeds); err != nil {
			return nil, err
		}
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	beforeGeneration := t.collection.Generation()
	err = putSeedsAtomic(t.collection, seeds)
	if collectionMutationPublished(t.collection, beforeGeneration, err) {
		t.conflicts.recordSeeds(seeds)
	}
	if err != nil {
		return nil, err
	}
	return result{affected: int64(len(seeds))}, nil
}

// insertSelectLocked is the two-phase INSERT ... SELECT executor. The source
// plan is fully materialized from a coherent pre-statement snapshot first.
// Every surviving document, primary key, schema check, conflict decision,
// routing decision, RETURNING row, and cancellation point is then resolved in
// connection-owned staging before the one durable publication primitive runs.
func (c *conn) insertSelectLocked(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	t *table,
	limits durable.Options,
	source *insertSelectPlan,
	returning *query.Statement,
	returned *query.Cursor,
) (sqldriver.Result, error) {
	cursor, sourceRetained, err := c.runInsertSourceLocked(ctx, source, args)
	if err != nil {
		return nil, err
	}
	intermediate, err := newInsertSelectIntermediateBudget(
		c.exec.Options, sourceRetained,
	)
	if err != nil {
		return nil, err
	}
	sourceRootBytes := c.exec.Result.RetainedBytes()
	tree := statement.Tree().Insert
	placement := c.clusterBinding(tree.Table)
	clear(c.insertSeeds)
	seeds := c.insertSeeds[:0]
	c.insertKeyRaw = c.insertKeyRaw[:0]
	if c.insertSeen == nil {
		c.insertSeen = make(map[string]struct{})
	} else {
		clear(c.insertSeen)
	}
	defer func() {
		clear(seeds)
		c.insertSeeds = seeds[:0]
		clear(c.insertSeen)
	}()
	c.pointDocs.Reset()
	conflictScratch := c.pointRaw[:0]
	stagedBytes := 0
	cancellable := ctx.Done() != nil
	row := 0
	for {
		next, nextErr := cursor.NextWithCancel(c.exec.Options.Cancel)
		if nextErr != nil {
			c.pointRaw = conflictScratch
			return nil, nextErr
		}
		if !next {
			break
		}
		if cancellable {
			if err := contextCheckpoint(ctx); err != nil {
				c.pointRaw = conflictScratch
				return nil, err
			}
		}
		cell := cursor.Cell(0)
		if cell.Kind() != query.TypeJSON {
			c.pointRaw = conflictScratch
			return nil, &query.InsertSelectShapeError{
				Pos: insertSelectOutputPosition(tree), Columns: 1,
				Type: cell.Kind(), Row: row,
			}
		}
		document := cell.Payload()
		if err := validateDocumentWithIntermediateBudget(
			t.schema, document, limits.MaxDocumentBytes, &c.insertTape,
			&intermediate,
		); err != nil {
			c.pointRaw = conflictScratch
			return nil, err
		}
		keyStart := len(c.insertKeyRaw)
		var key string
		var keyCharge int64
		c.insertKeyRaw, key, keyCharge, err = appendDocumentKeyBudgeted(
			c.insertKeyRaw, document, t.meta.PrimaryKey,
			t.primary, limits.MaxKeyBytes, &intermediate,
		)
		if err != nil {
			c.pointRaw = conflictScratch
			return nil, err
		}
		if _, duplicate := c.insertSeen[key]; duplicate {
			c.insertKeyRaw = c.insertKeyRaw[:keyStart]
			intermediate.release(keyCharge)
			if tree.OnConflictDoNothing {
				row++
				continue
			}
			c.pointRaw = conflictScratch
			return nil, fmt.Errorf(
				"%w: %q appears twice in one SELECT source",
				ErrDuplicatePrimaryKey, key,
			)
		}
		c.insertSeen[key] = struct{}{}
		if tree.OnConflictDoNothing && t.collection != nil {
			var found bool
			found, err = t.collection.ContainsKey([]byte(key))
			if err != nil {
				c.pointRaw = conflictScratch
				return nil, err
			}
			if found {
				row++
				continue
			}
		}
		if len(seeds) >= limits.MaxBatchDocuments {
			c.pointRaw = conflictScratch
			return nil, fmt.Errorf(
				"vibedb: INSERT SELECT admits more than %d documents: %w",
				limits.MaxBatchDocuments, durable.ErrBatchTooLarge,
			)
		}
		if len(key) > limits.MaxBatchBytes-stagedBytes ||
			len(document) > limits.MaxBatchBytes-stagedBytes-len(key) {
			c.pointRaw = conflictScratch
			return nil, fmt.Errorf(
				"vibedb: INSERT SELECT exceeds the %d-byte durable batch limit: %w",
				limits.MaxBatchBytes, durable.ErrBatchTooLarge,
			)
		}
		if err := intermediate.admit(insertSelectStagedRowBytesFor(
			document, key, c.insertTape,
			int64(unsafe.Sizeof(seedDocument{})),
		)); err != nil {
			c.pointRaw = conflictScratch
			return nil, err
		}
		ordinal, appendErr := c.pointDocs.Append(document)
		if appendErr != nil {
			c.pointRaw = conflictScratch
			return nil, appendErr
		}
		owned := c.pointDocs.DocAt(ordinal).Src
		// A published key is also retained by the transaction-conflict clock.
		// Detach it from the connection's reusable packed scratch before either
		// storage or that clock can outlive this execution.
		seeds = append(seeds, seedDocument{
			key: strings.Clone(key), document: owned,
		})
		stagedBytes += len(key) + len(document)
		row++
	}
	c.pointRaw = conflictScratch
	if !tree.OnConflictDoNothing && t.collection != nil {
		if err := c.rejectExistingSeeds(ctx, t.collection, seeds); err != nil {
			return nil, err
		}
	}
	if err := c.routeInsertSeedsWithBinding(placement, seeds); err != nil {
		return nil, err
	}
	if err := validateTableUniquePostimages(ctx, t, seeds, nil); err != nil {
		return nil, err
	}
	if returning != nil {
		if returned == nil {
			return nil, errors.New("vibedb: internal RETURNING cursor is nil")
		}
		resultCursor, err := runInsertSelectReturning(
			returning, &c.exec, query.FromSegment(&c.pointDocs),
			&intermediate, sourceRootBytes,
		)
		if err != nil {
			return nil, err
		}
		*returned = resultCursor
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	if len(seeds) == 0 {
		return result{}, nil
	}
	if t.collection == nil {
		_, err := c.db.materializeLocked(tree.Table, seeds)
		if t.collection != nil {
			t.conflicts.recordSeeds(seeds)
		}
		if err != nil {
			return nil, err
		}
		return result{affected: int64(len(seeds))}, nil
	}
	beforeGeneration := t.collection.Generation()
	err = putSeedsAtomic(t.collection, seeds)
	if collectionMutationPublished(t.collection, beforeGeneration, err) {
		t.conflicts.recordSeeds(seeds)
	}
	if err != nil {
		return nil, err
	}
	return result{affected: int64(len(seeds))}, nil
}

func insertSelectOutputPosition(insert *sqlast.InsertStmt) int {
	if insert != nil && insert.Source != nil && len(insert.Source.Columns) != 0 {
		return insert.Source.Columns[0].Pos
	}
	if insert != nil {
		return insert.SourcePos
	}
	return 0
}

func (c *conn) runInsertSourceLocked(
	ctx context.Context,
	plan *insertSelectPlan,
	args []any,
) (query.Cursor, int64, error) {
	if plan == nil || plan.statement == nil {
		return query.Cursor{}, 0, errors.New(
			"vibedb: INSERT SELECT has no prepared source plan",
		)
	}
	statement := plan.statement
	if sourceIndependentStatement(statement) {
		return statement.RunIntermediateInto(&c.exec, query.Source{}, args)
	}
	requiresCatalog := statement.RequiresCatalog()
	if requiresCatalog {
		clear(c.joinCatalog)
		collections := c.joinCatalog[:0]
		for _, dependency := range plan.dependencies {
			table := c.db.tables[dependency.name]
			if table == nil {
				c.releaseJoinCatalog(collections)
				return query.Cursor{}, 0, missingTableDependency(
					dependency.name, dependency.pos, false,
				)
			}
			collections = append(collections, durable.NamedCollection{
				Name: dependency.name, Collection: table.collection,
			})
		}
		if err := durable.SnapshotCollectionsIntoContext(
			ctx, &c.joinSnapshot, collections,
		); err != nil {
			c.releaseJoinCatalog(collections)
			return query.Cursor{}, 0, err
		}
		c.releaseJoinCatalog(collections)
		var (
			cursor   query.Cursor
			retained int64
			err      error
		)
		if statement.UsesDirectCatalogExecution() {
			cursor, retained, err = statement.RunIntermediateInto(
				&c.exec,
				query.FromFileDatabase(c.joinSnapshot, statement.Collection()),
				args,
			)
		} else {
			var source query.Source
			source, err = c.materializeDurableJoinSource(
				ctx, &c.joinSnapshot, statement.Collection(), plan.dependencies,
			)
			if err == nil {
				cursor, retained, err = statement.RunIntermediateInto(
					&c.exec, source, args,
				)
			}
		}
		closeErr := c.joinSnapshot.Close()
		if err != nil || closeErr != nil {
			return query.Cursor{}, 0, errors.Join(err, closeErr)
		}
		return cursor, retained, nil
	}
	table := c.db.tables[statement.Collection()]
	if table == nil {
		return query.Cursor{}, 0, missingTableDependency(
			statement.Collection(),
			insertSourceDependencyPosition(plan, statement.Collection()), false,
		)
	}
	if table.collection == nil {
		return statement.RunIntermediateInto(
			&c.exec, query.FromSnapshot(store.Snapshot{}), args,
		)
	}
	if err := table.collection.SnapshotInto(&c.insertSnapshot); err != nil {
		return query.Cursor{}, 0, err
	}
	cursor, retained, runErr := statement.RunIntermediateInto(
		&c.exec, query.FromFile(&c.insertSnapshot), args,
	)
	closeErr := c.insertSnapshot.Close()
	if runErr != nil || closeErr != nil {
		return query.Cursor{}, 0, errors.Join(runErr, closeErr)
	}
	return cursor, retained, nil
}

func insertSourceDependencyPosition(plan *insertSelectPlan, name string) int {
	if plan != nil {
		for i := range plan.dependencies {
			if plan.dependencies[i].name == name {
				return plan.dependencies[i].pos
			}
		}
		if plan.tree != nil && len(plan.tree.From) != 0 {
			return plan.tree.From[0].Pos
		}
	}
	return 0
}

// mutationReturningContext executes a mutation and materializes its RETURNING
// projection before publication. A projection or result-budget failure leaves
// storage untouched, and the successful path performs no storage reread for
// INSERT/UPDATE; DELETE captures the old documents before removing them.
func (c *conn) mutationReturningContext(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	returning *query.Statement,
) (query.Cursor, error) {
	return c.mutationReturningCoreContext(
		ctx, statement, args, returning, nil,
	)
}

func (c *conn) preparedMutationReturningContext(
	ctx context.Context,
	prepared *stmt,
	args []any,
) (query.Cursor, error) {
	if prepared == nil {
		return query.Cursor{}, errors.New("vibedb: internal prepared mutation is nil")
	}
	return c.mutationReturningCoreContext(
		ctx, prepared.mutation, args, prepared.query, prepared,
	)
}

func (c *conn) mutationReturningCoreContext(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	returning *query.Statement,
	prepared *stmt,
) (query.Cursor, error) {
	var cursor query.Cursor
	if err := c.requireDirectWriteAllowed(); err != nil {
		return cursor, err
	}
	d := c.db
	if err := lockContext(ctx, &d.mu); err != nil {
		return cursor, err
	}
	defer d.mu.Unlock()
	if d.closed {
		return cursor, sqldriver.ErrBadConn
	}
	if err := contextCheckpoint(ctx); err != nil {
		return cursor, err
	}
	if err := d.settleCatalogLocked(); err != nil {
		return cursor, err
	}
	if prepared != nil {
		if err := prepared.validateViewDependenciesLocked(); err != nil {
			return cursor, err
		}
	}
	if err := d.validateViewTableTargetLocked(statement.Tree()); err != nil {
		return cursor, err
	}
	if returning == nil {
		return cursor, errors.New(
			"vibedb: internal returning execution requires a RETURNING projection",
		)
	}
	t, ok := d.tables[statement.Collection()]
	if !ok {
		return cursor, fmt.Errorf(
			"%w: %q", ErrTableNotFound, statement.Collection(),
		)
	}
	var err error
	switch statement.Kind() {
	case query.DMLInsert:
		var source *insertSelectPlan
		if prepared != nil {
			source = prepared.insertSource
		}
		_, err = c.insertLocked(
			ctx, statement, args, t, source, returning, &cursor,
		)
	case query.DMLUpdate:
		_, err = c.updateLockedReturning(ctx, statement, args, t, returning, &cursor)
	case query.DMLDelete:
		_, err = c.deleteLockedReturning(ctx, statement, args, t, returning, &cursor)
	default:
		err = fmt.Errorf("vibedb: %s does not support RETURNING", statement.Kind())
	}
	if err != nil {
		return query.Cursor{}, err
	}
	return cursor, nil
}

func (c *conn) rejectExistingSeeds(
	ctx context.Context,
	collection *durable.Collection,
	seeds []seedDocument,
) error {
	cancellable := ctx.Done() != nil
	for _, seed := range seeds {
		if cancellable {
			if err := contextCheckpoint(ctx); err != nil {
				return err
			}
		}
		found, err := collection.ContainsKey([]byte(seed.key))
		if err != nil {
			return err
		}
		if found {
			return fmt.Errorf("%w: %q", ErrDuplicatePrimaryKey, seed.key)
		}
	}
	return nil
}

func putSeedsAtomic(collection *durable.Collection, seeds []seedDocument) error {
	switch len(seeds) {
	case 0:
		return nil
	case 1:
		_, err := collection.Put([]byte(seeds[0].key), seeds[0].document)
		return mapDurableUniqueConstraintError(err)
	}
	return mapDurableUniqueConstraintError(collection.Update(func(batch *durable.WriteBatch) error {
		for _, seed := range seeds {
			if err := batch.Put([]byte(seed.key), seed.document); err != nil {
				return err
			}
		}
		return nil
	}))
}

func resolveInsertRow(
	statement *query.DMLStatement,
	insert *sqlast.InsertStmt,
	row *sqlast.InsertRow,
	args []any,
	primaryKey string,
	primary vibejson.CompiledPointer,
	limits durable.Options,
) ([]byte, string, error) {
	var document []byte
	var err error
	if len(insert.Columns) == 0 {
		document, err = operandDocument(statement, row.Values[0], args)
	} else {
		document, err = encodeFlatInsertDocument(
			statement.InsertFlatFieldOrdinals(), statement.InsertFlatKeyJSONBytes(),
			insert, row, args,
			limits.MaxDocumentBytes,
		)
	}
	if err != nil {
		return nil, "", err
	}
	if len(document) > limits.MaxDocumentBytes {
		return nil, "", durable.ErrDocumentTooLarge
	}
	key, err := documentKey(
		document, primaryKey, primary, limits.MaxKeyBytes,
	)
	return document, key, err
}

type flatInsertDocument struct {
	insert   *sqlast.InsertStmt
	row      *sqlast.InsertRow
	args     []any
	ordinals []uint32
	err      error
}

var flatInsertEncoder = compileFlatInsertEncoder()

var flatInsertDocumentPool = sync.Pool{
	New: func() any { return new(flatInsertDocument) },
}

func compileFlatInsertEncoder() vibejson.Encoder[flatInsertDocument] {
	encoder, err := vibejson.CompileEncoder[flatInsertDocument](vibejson.EncoderOptions{})
	if err != nil {
		panic(fmt.Sprintf("vibedb: compile flat INSERT encoder: %v", err))
	}
	return encoder
}

// MarshalVibeJSON writes the already-compiled key order directly through
// vibejson. It deliberately emits one syntactically valid null after a scalar
// error so hook-validation builds still see a valid object; the caller rejects
// the output through d.err and never publishes it.
func (d *flatInsertDocument) MarshalVibeJSON(
	w vibejson.TrustedAppender,
) vibejson.TrustedAppender {
	w = w.RawByteUnchecked('{')
	for output, ordinal := range d.ordinals {
		if output != 0 {
			w = w.RawByteUnchecked(',')
		}
		column := d.insert.Columns[ordinal]
		w = w.String(column.Segments[0].Key).RawByteUnchecked(':')
		if d.err != nil {
			w = w.Null()
			continue
		}
		value, err := flatInsertOperandValue(d.row.Values[ordinal], d.args)
		if err == nil {
			w, err = appendFlatInsertScalar(w, value)
		}
		if err != nil {
			d.err = err
			w = w.Null()
		}
	}
	return w.RawByteUnchecked('}')
}

func encodeFlatInsertDocument(
	ordinals []uint32,
	keyJSONBytes uint64,
	insert *sqlast.InsertStmt,
	row *sqlast.InsertRow,
	args []any,
	maxDocumentBytes int,
) ([]byte, error) {
	if len(ordinals) == 0 || len(insert.Columns) != len(row.Values) {
		return nil, errors.New("vibedb: internal flat INSERT shape is incomplete")
	}
	if maxDocumentBytes < 0 {
		return nil, durable.ErrDocumentTooLarge
	}
	maxBytes := uint64(maxDocumentBytes)
	encodedBytes := uint64(2)                      // opening and closing object braces
	structuralBytes := uint64(len(insert.Columns)) // one colon per field
	if len(insert.Columns) > 1 {
		structuralBytes += uint64(len(insert.Columns) - 1) // commas
	}
	for index := range insert.Columns {
		value, err := flatInsertOperandValue(row.Values[index], args)
		if err != nil {
			return nil, err
		}
		scalarBytes, err := flatScalarEncodedCapacity(value, maxBytes)
		if err != nil {
			return nil, err
		}
		if !addFlatEncodedBytes(&encodedBytes, scalarBytes, maxBytes) {
			return nil, durable.ErrDocumentTooLarge
		}
	}
	// Inspect operands before rejecting immutable key/structure overhead so an
	// unbound or malformed scalar retains binding-error priority.
	if !addFlatEncodedBytes(&encodedBytes, keyJSONBytes, maxBytes) ||
		!addFlatEncodedBytes(&encodedBytes, structuralBytes, maxBytes) {
		return nil, durable.ErrDocumentTooLarge
	}
	document := flatInsertDocumentPool.Get().(*flatInsertDocument)
	*document = flatInsertDocument{
		insert: insert, row: row, args: args, ordinals: ordinals,
	}
	encoded, err := flatInsertEncoder.AppendJSON(
		make([]byte, 0, int(encodedBytes)), document,
	)
	documentErr := document.err
	releaseFlatInsertDocument(document)
	if documentErr != nil {
		return nil, documentErr
	}
	return encoded, err
}

func releaseFlatInsertDocument(document *flatInsertDocument) {
	*document = flatInsertDocument{}
	flatInsertDocumentPool.Put(document)
}

func flatScalarEncodedCapacity(value any, limit uint64) (uint64, error) {
	var encoded uint64
	switch value := value.(type) {
	case nil:
		encoded = 4
	case bool:
		if value {
			encoded = 4
		} else {
			encoded = 5
		}
	case int:
		encoded = flatIntBytes(int64(value))
	case int8:
		encoded = flatIntBytes(int64(value))
	case int16:
		encoded = flatIntBytes(int64(value))
	case int32:
		encoded = flatIntBytes(int64(value))
	case int64:
		encoded = flatIntBytes(value)
	case uint:
		encoded = flatUintBytes(uint64(value))
	case uint8:
		encoded = flatUintBytes(uint64(value))
	case uint16:
		encoded = flatUintBytes(uint64(value))
	case uint32:
		encoded = flatUintBytes(uint64(value))
	case uint64:
		encoded = flatUintBytes(value)
	case float32:
		return flatFloat64EncodedBytes(float64(value), limit)
	case float64:
		return flatFloat64EncodedBytes(value, limit)
	case string:
		return flatJSONStringEncodedBytes(value, limit)
	case []byte:
		return flatJSONStringEncodedBytes(byteview.String(value), limit)
	case query.Number:
		encoded = uint64(len(value))
	case vibejson.RawValue:
		raw, ok := value.NumberBytes()
		if !ok {
			return 0, errors.New("vibedb: raw scalar parameter must be a JSON number")
		}
		encoded = uint64(len(raw))
	case *bool:
		if *value {
			encoded = 4
		} else {
			encoded = 5
		}
	case *int64:
		encoded = flatIntBytes(*value)
	case *float64:
		return flatFloat64EncodedBytes(*value, limit)
	case *string:
		return flatJSONStringEncodedBytes(*value, limit)
	case *query.Number:
		encoded = uint64(len(*value))
	default:
		return 0, fmt.Errorf("vibedb: %T is not a JSON scalar driver value", value)
	}
	if encoded > limit {
		return 0, durable.ErrDocumentTooLarge
	}
	return encoded, nil
}

func flatFloat64EncodedBytes(value float64, limit uint64) (uint64, error) {
	var scratch [32]byte
	encoded, ok := vibejsonsimd.AppendFloat64(scratch[:0], value)
	if !ok {
		return 0, errors.New("vibedb: numeric parameters must be finite JSON numbers")
	}
	if uint64(len(encoded)) > limit {
		return 0, durable.ErrDocumentTooLarge
	}
	return uint64(len(encoded)), nil
}

func addFlatEncodedBytes(total *uint64, additional, limit uint64) bool {
	if *total > limit || additional > limit-*total {
		return false
	}
	*total += additional
	return true
}

// flatJSONStringEncodedBytes validates UTF-8 and returns the exact byte count
// emitted by TrustedAppender.String under HTML-safe JSON escaping. One scanner
// pass therefore replaces a separate validation pass and
// a conservative capacity guess.
func flatJSONStringEncodedBytes(value string, limit uint64) (uint64, error) {
	encoded := uint64(2)
	tooLarge := false
	if !addFlatEncodedBytes(&encoded, uint64(len(value)), limit) {
		tooLarge = true
	}
	raw := byteview.Bytes(value)
	for cursor := 0; cursor < len(raw); {
		special := scanner.IndexHTMLStringSpecial(raw, cursor)
		if special == len(raw) {
			break
		}
		if raw[special] < utf8.RuneSelf {
			extra := uint64(1)
			switch raw[special] {
			case '\b', '\f', '\n', '\r', '\t', '"', '\\':
			default:
				extra = 5
			}
			if !tooLarge && !addFlatEncodedBytes(&encoded, extra, limit) {
				tooLarge = true
			}
			cursor = special + 1
			continue
		}
		r, width := utf8.DecodeRune(raw[special:])
		if r == utf8.RuneError && width == 1 {
			return 0, errors.New("vibedb: a flat INSERT string must be valid UTF-8")
		}
		if (r == '\u2028' || r == '\u2029') && !tooLarge &&
			!addFlatEncodedBytes(&encoded, 3, limit) {
			tooLarge = true
		}
		cursor = special + width
	}
	if tooLarge {
		return 0, durable.ErrDocumentTooLarge
	}
	return encoded, nil
}

func flatIntBytes(value int64) uint64 {
	if value >= 0 {
		return flatUintBytes(uint64(value))
	}
	return 1 + flatUintBytes(uint64(-(value+1))+1)
}

func flatUintBytes(value uint64) uint64 {
	encoded := uint64(1)
	for value >= 10 {
		value /= 10
		encoded++
	}
	return encoded
}

func appendFlatInsertScalar(
	w vibejson.TrustedAppender,
	value any,
) (vibejson.TrustedAppender, error) {
	switch value := value.(type) {
	case nil:
		return w.Null(), nil
	case bool:
		return w.Bool(value), nil
	case int:
		return w.Int(int64(value)), nil
	case int8:
		return w.Int(int64(value)), nil
	case int16:
		return w.Int(int64(value)), nil
	case int32:
		return w.Int(int64(value)), nil
	case int64:
		return w.Int(value), nil
	case uint:
		return w.Uint(uint64(value)), nil
	case uint8:
		return w.Uint(uint64(value)), nil
	case uint16:
		return w.Uint(uint64(value)), nil
	case uint32:
		return w.Uint(uint64(value)), nil
	case uint64:
		return w.Uint(value), nil
	case float32:
		// Preserve the old flat-INSERT rule: widen first, then choose the
		// shortest exact float64 spelling.
		return w.Float64(float64(value)), nil
	case float64:
		return w.Float64(value), nil
	case string:
		return w.String(value), nil
	case []byte:
		return w.String(byteview.String(value)), nil
	case query.Number:
		return appendFlatInsertNumber(w, byteview.Bytes(string(value)))
	case vibejson.RawValue:
		// RawValue is the zero-copy exact-number carrier accepted by the SQL
		// operand boundary. The former map encoder accidentally serialized its
		// exported Src field as base64 instead of honoring that scalar contract.
		raw, ok := value.NumberBytes()
		if !ok {
			return w, errors.New("vibedb: raw scalar parameter must be a JSON number")
		}
		return w.RawBytesUnchecked(raw), nil
	case *bool:
		return w.Bool(*value), nil
	case *int64:
		return w.Int(*value), nil
	case *float64:
		return w.Float64(*value), nil
	case *string:
		return w.String(*value), nil
	case *query.Number:
		return appendFlatInsertNumber(w, byteview.Bytes(string(*value)))
	default:
		return w, fmt.Errorf("vibedb: %T is not a JSON scalar driver value", value)
	}
}

func appendFlatInsertNumber(
	w vibejson.TrustedAppender,
	raw []byte,
) (vibejson.TrustedAppender, error) {
	if _, ok := (vibejson.RawValue{Src: raw}).NumberBytes(); !ok {
		return w, fmt.Errorf("vibedb: invalid JSON number %q", raw)
	}
	return w.RawBytesUnchecked(raw), nil
}

func tableMutationLimits(t *table) (durable.Options, error) {
	if t.collection != nil {
		return durable.Options{
			MaxKeyBytes:       t.collection.MaxKeyBytes(),
			MaxDocumentBytes:  t.collection.MaxDocumentBytes(),
			MaxBatchDocuments: t.collection.MaxBatchDocuments(),
			MaxBatchBytes:     t.collection.MaxBatchBytes(),
		}, nil
	}
	return durable.NormalizeOptions(durableOptions(t))
}

func operandDocument(statement *query.DMLStatement, operand sqlast.Operand, args []any) ([]byte, error) {
	if operand.Kind == sqlast.OperandParam && operand.Ordinal < len(args) {
		switch value := args[operand.Ordinal].(type) {
		case *string:
			if value == nil {
				return nil, errors.New(
					"vibedb: a document cannot be bound from a nil *string")
			}
			return byteview.Bytes(*value), nil
		case *[]byte:
			if value == nil {
				return nil, errors.New(
					"vibedb: a document cannot be bound from a nil *[]byte")
			}
			return *value, nil
		}
	}
	return statement.Document(operand, args)
}

func operandValue(operand sqlast.Operand, args []any) (any, error) {
	return operandValueWithUTF8(operand, args, true)
}

func flatInsertOperandValue(operand sqlast.Operand, args []any) (any, error) {
	return operandValueWithUTF8(operand, args, false)
}

func operandValueWithUTF8(
	operand sqlast.Operand,
	args []any,
	validateUTF8 bool,
) (any, error) {
	switch operand.Kind {
	case sqlast.OperandParam:
		if operand.Ordinal >= len(args) {
			return nil, fmt.Errorf("vibedb: placeholder %d was not bound", operand.Ordinal+1)
		}
		arg := args[operand.Ordinal]
		switch value := arg.(type) {
		case nil, bool,
			int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64:
			// Return the caller's existing interface box. Returning the
			// type-switched local would allocate a second box for every warm
			// predicate bind.
			return arg, nil
		case float32:
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return nil, errors.New(
					"vibedb: numeric parameters must be finite JSON numbers")
			}
			return arg, nil
		case float64:
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, errors.New(
					"vibedb: numeric parameters must be finite JSON numbers")
			}
			return arg, nil
		case string:
			if validateUTF8 && !utf8.ValidString(value) {
				return nil, errors.New(
					"vibedb: a flat INSERT string must be valid UTF-8")
			}
			return arg, nil
		case query.Number:
			return arg, nil
		case vibejson.RawValue:
			if _, ok := value.NumberBytes(); !ok {
				return nil, errors.New(
					"vibedb: raw scalar parameter must be a JSON number")
			}
			return arg, nil
		case []byte:
			if validateUTF8 && !utf8.Valid(value) {
				return nil, errors.New(
					"vibedb: a flat INSERT string must be valid UTF-8")
			}
			return arg, nil
		case *bool:
			if value == nil {
				return nil, nil
			}
			return arg, nil
		case *int64:
			if value == nil {
				return nil, nil
			}
			return arg, nil
		case *float64:
			if value == nil {
				return nil, nil
			}
			if math.IsNaN(*value) || math.IsInf(*value, 0) {
				return nil, errors.New(
					"vibedb: numeric parameters must be finite JSON numbers")
			}
			return arg, nil
		case *string:
			if value == nil {
				return nil, nil
			}
			if validateUTF8 && !utf8.ValidString(*value) {
				return nil, errors.New(
					"vibedb: a flat INSERT string must be valid UTF-8")
			}
			return arg, nil
		case *query.Number:
			if value == nil {
				return nil, nil
			}
			return arg, nil
		default:
			return nil, fmt.Errorf("vibedb: %T is not a JSON scalar driver value", value)
		}
	case sqlast.OperandString:
		if validateUTF8 && !utf8.ValidString(operand.Text) {
			return nil, errors.New(
				"vibedb: a flat INSERT string must be valid UTF-8")
		}
		return operand.Text, nil
	case sqlast.OperandBool:
		return operand.Bool, nil
	case sqlast.OperandNumber:
		return query.Number(operand.Text), nil
	case sqlast.OperandNull:
		return nil, nil
	}
	return nil, errors.New("vibedb: flat INSERT values must be JSON scalars or placeholders")
}

func documentKey(
	document []byte,
	pointer string,
	primary vibejson.CompiledPointer,
	maxKeyBytes int,
) (string, error) {
	value, found, err := primary.GetRaw(document)
	if err != nil {
		return "", fmt.Errorf("vibedb: invalid JSON document: %w", err)
	}
	if !found {
		return "", fmt.Errorf("vibedb: primary key path %q is missing", pointer)
	}
	if value.IsNull() {
		return "", fmt.Errorf("vibedb: primary key path %q is null", pointer)
	}
	switch value.Kind() {
	case jsondoc.String:
		raw := value.Bytes()
		size, ok := orderedkey.JSONStringEncodedSize(raw)
		if !ok {
			return "", fmt.Errorf(
				"vibedb: primary key path %q has an invalid JSON string",
				pointer,
			)
		}
		if size > maxKeyBytes {
			return "", durable.ErrKeyTooLarge
		}
		key, _ := orderedkey.AppendJSONString(
			nil, raw, orderedkey.Ascending,
		)
		return string(key), nil
	case jsondoc.Bool:
		if maxKeyBytes < 1 {
			return "", durable.ErrKeyTooLarge
		}
		boolean, _ := value.Bool()
		key, _ := orderedkey.AppendBool(nil, boolean, orderedkey.Ascending)
		return string(key), nil
	case jsondoc.Number:
		number, _ := value.NumberBytes()
		size, ok := orderedkey.NumberEncodedSize(number)
		if !ok {
			return "", fmt.Errorf(
				"vibedb: primary key path %q is not a valid JSON number",
				pointer,
			)
		}
		if size > maxKeyBytes {
			return "", durable.ErrKeyTooLarge
		}
		key, _ := orderedkey.AppendNumber(nil, number, orderedkey.Ascending)
		return string(key), nil
	default:
		return "", fmt.Errorf("vibedb: primary key path %q must hold a JSON string, number, or bool", pointer)
	}
}

// appendDocumentKey is documentKey with caller-owned packed storage. It is the
// INSERT SELECT hot form: after one comparable execution, keys and their map
// views are rebuilt without per-row heap allocation. The returned string is a
// read-only view into returned and remains valid until that byte arena is
// reused.
func appendDocumentKey(
	dst []byte,
	document []byte,
	pointer string,
	primary vibejson.CompiledPointer,
	maxKeyBytes int,
) ([]byte, string, error) {
	value, found, err := primary.GetRaw(document)
	if err != nil {
		return dst, "", fmt.Errorf("vibedb: invalid JSON document: %w", err)
	}
	if !found {
		return dst, "", fmt.Errorf("vibedb: primary key path %q is missing", pointer)
	}
	if value.IsNull() {
		return dst, "", fmt.Errorf("vibedb: primary key path %q is null", pointer)
	}
	start := len(dst)
	switch value.Kind() {
	case jsondoc.String:
		raw := value.Bytes()
		size, ok := orderedkey.JSONStringEncodedSize(raw)
		if !ok {
			return dst, "", fmt.Errorf(
				"vibedb: primary key path %q has an invalid JSON string", pointer,
			)
		}
		if size > maxKeyBytes {
			return dst, "", durable.ErrKeyTooLarge
		}
		dst, _ = orderedkey.AppendJSONString(dst, raw, orderedkey.Ascending)
	case jsondoc.Bool:
		if maxKeyBytes < 1 {
			return dst, "", durable.ErrKeyTooLarge
		}
		boolean, _ := value.Bool()
		dst, _ = orderedkey.AppendBool(dst, boolean, orderedkey.Ascending)
	case jsondoc.Number:
		number, _ := value.NumberBytes()
		size, ok := orderedkey.NumberEncodedSize(number)
		if !ok {
			return dst, "", fmt.Errorf(
				"vibedb: primary key path %q is not a valid JSON number", pointer,
			)
		}
		if size > maxKeyBytes {
			return dst, "", durable.ErrKeyTooLarge
		}
		dst, _ = orderedkey.AppendNumber(dst, number, orderedkey.Ascending)
	default:
		return dst, "", fmt.Errorf(
			"vibedb: primary key path %q must hold a JSON string, number, or bool",
			pointer,
		)
	}
	return dst, byteview.String(dst[start:]), nil
}

// appendDocumentKeyBudgeted admits the exact retained key/map footprint before
// growing dst. The caller may release charge and truncate dst when a later
// duplicate check proves this encoded copy is not retained.
func appendDocumentKeyBudgeted(
	dst []byte,
	document []byte,
	pointer string,
	primary vibejson.CompiledPointer,
	maxKeyBytes int,
	budget *insertSelectIntermediateBudget,
) ([]byte, string, int64, error) {
	value, found, err := primary.GetRaw(document)
	if err != nil {
		return dst, "", 0, fmt.Errorf("vibedb: invalid JSON document: %w", err)
	}
	if !found {
		return dst, "", 0, fmt.Errorf("vibedb: primary key path %q is missing", pointer)
	}
	if value.IsNull() {
		return dst, "", 0, fmt.Errorf("vibedb: primary key path %q is null", pointer)
	}
	encoded := 0
	switch value.Kind() {
	case jsondoc.String:
		raw := value.Bytes()
		var ok bool
		encoded, ok = orderedkey.JSONStringEncodedSize(raw)
		if !ok {
			return dst, "", 0, fmt.Errorf(
				"vibedb: primary key path %q has an invalid JSON string", pointer,
			)
		}
	case jsondoc.Bool:
		encoded = 1
	case jsondoc.Number:
		number, _ := value.NumberBytes()
		var ok bool
		encoded, ok = orderedkey.NumberEncodedSize(number)
		if !ok {
			return dst, "", 0, fmt.Errorf(
				"vibedb: primary key path %q is not a valid JSON number", pointer,
			)
		}
	default:
		return dst, "", 0, fmt.Errorf(
			"vibedb: primary key path %q must hold a JSON string, number, or bool",
			pointer,
		)
	}
	if encoded > maxKeyBytes {
		return dst, "", 0, durable.ErrKeyTooLarge
	}
	charge := insertSelectSeenKeyBytesLen(encoded)
	if err := budget.admit(charge); err != nil {
		return dst, "", 0, err
	}
	start := len(dst)
	switch value.Kind() {
	case jsondoc.String:
		dst, _ = orderedkey.AppendJSONString(dst, value.Bytes(), orderedkey.Ascending)
	case jsondoc.Bool:
		boolean, _ := value.Bool()
		dst, _ = orderedkey.AppendBool(dst, boolean, orderedkey.Ascending)
	case jsondoc.Number:
		number, _ := value.NumberBytes()
		dst, _ = orderedkey.AppendNumber(dst, number, orderedkey.Ascending)
	}
	return dst, byteview.String(dst[start:]), charge, nil
}

func insertSelectSeenKeyBytesLen(encoded int) int64 {
	return int64(encoded) + int64(unsafe.Sizeof("")) + 8
}

func validateDocument(
	schema *store.Schema,
	document []byte,
	maxDocumentBytes int,
	tape *[]vibejson.IndexEntry,
) error {
	if len(document) > maxDocumentBytes {
		return durable.ErrDocumentTooLarge
	}
	need := len(document)/8 + 8
	limit := len(document) + 2
	for {
		if cap(*tape) < need {
			*tape = make([]vibejson.IndexEntry, need)
		}
		index, err := vibejson.BuildIndex(
			document, (*tape)[:cap(*tape)],
		)
		if err != jsondoc.ErrIndexFull || cap(*tape) >= limit {
			if err != nil {
				return fmt.Errorf("vibedb: invalid JSON document: %w", err)
			}
			*tape = index.Entries
			if schema == nil {
				return nil
			}
			return schema.ValidateIndex(index)
		}
		need = min(2*cap(*tape), limit)
	}
}

func validateDocumentWithIntermediateBudget(
	schema *store.Schema,
	document []byte,
	maxDocumentBytes int,
	tape *[]vibejson.IndexEntry,
	budget *insertSelectIntermediateBudget,
) error {
	if len(document) > maxDocumentBytes {
		return durable.ErrDocumentTooLarge
	}
	need := len(document)/8 + 8
	limit := len(document) + 2
	entryBytes := int64(unsafe.Sizeof(vibejson.IndexEntry{}))
	for {
		scratch := int64(need) * entryBytes
		if err := budget.admit(scratch); err != nil {
			return err
		}
		if cap(*tape) < need {
			*tape = make([]vibejson.IndexEntry, need)
		}
		work := (*tape)[:need:need]
		index, err := vibejson.BuildIndex(document, work)
		if err == jsondoc.ErrIndexFull && need < limit {
			budget.release(scratch)
			need = min(2*need, limit)
			continue
		}
		if err != nil {
			budget.release(scratch)
			return fmt.Errorf("vibedb: invalid JSON document: %w", err)
		}
		validationErr := error(nil)
		if schema != nil {
			validationErr = schema.ValidateIndex(index)
		}
		*tape = (*tape)[:len(index.Entries)]
		budget.release(scratch)
		return validationErr
	}
}

func (c *conn) updateLocked(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	t *table,
) (sqldriver.Result, error) {
	return c.updateLockedReturning(ctx, statement, args, t, nil, nil)
}

func (c *conn) updateLockedReturning(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	t *table,
	returning *query.Statement,
	returned *query.Cursor,
) (sqldriver.Result, error) {
	if assignments := statement.Tree().Update.Assignments; len(assignments) != 0 {
		return c.updateColumnsLockedReturning(
			ctx, statement, args, t, assignments, returning, returned,
		)
	}
	limits, err := tableMutationLimits(t)
	if err != nil {
		return nil, err
	}
	document, err := operandDocument(
		statement, statement.Tree().Update.Doc, args,
	)
	if err != nil {
		return nil, err
	}
	if len(document) > limits.MaxDocumentBytes {
		return nil, durable.ErrDocumentTooLarge
	}
	if err := c.routeUpdate(statement, args, document); err != nil {
		return nil, err
	}
	keys, err := c.matchingKeysLocked(
		ctx, statement, args, t, limits, len(document))
	if err != nil {
		return nil, err
	}
	newKey, err := documentKey(
		document, t.meta.PrimaryKey, t.primary, limits.MaxKeyBytes,
	)
	if err != nil {
		return nil, err
	}
	for _, key := range keys {
		if key != newKey {
			return nil, fmt.Errorf(
				"%w: replacement key %q does not match selected key %q",
				ErrUpdatePrimaryKey, newKey, key,
			)
		}
	}
	if tableHasUniqueIndexes(t) {
		clear(c.insertSeeds)
		seeds := c.insertSeeds[:0]
		defer func() {
			clear(seeds)
			c.insertSeeds = seeds[:0]
		}()
		for _, key := range keys {
			seeds = append(seeds, seedDocument{key: key, document: document})
		}
		if err := validateTableUniquePostimages(ctx, t, seeds, keys); err != nil {
			return nil, err
		}
	}
	if returning != nil {
		if returned == nil {
			return nil, errors.New("vibedb: internal RETURNING cursor is nil")
		}
		c.pointDocs.Reset()
		for range keys {
			if _, err := c.pointDocs.Append(document); err != nil {
				return nil, err
			}
		}
		cursor, err := returning.RunInto(
			&c.exec, query.FromSegment(&c.pointDocs), nil,
		)
		if err != nil {
			return nil, err
		}
		*returned = cursor
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	if t.collection == nil {
		return result{}, nil
	}
	beforeGeneration := t.collection.Generation()
	var mutationErr error
	switch len(keys) {
	case 0:
	case 1:
		_, mutationErr = t.collection.Put([]byte(keys[0]), document)
	default:
		mutationErr = t.collection.Update(func(batch *durable.WriteBatch) error {
			for _, key := range keys {
				if err := batch.Put([]byte(key), document); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if collectionMutationPublished(
		t.collection, beforeGeneration, mutationErr,
	) {
		t.conflicts.recordKeys(keys)
	}
	if mutationErr != nil {
		return nil, mapDurableUniqueConstraintError(mutationErr)
	}
	return result{affected: int64(len(keys))}, nil
}

func (c *conn) updateColumnsLockedReturning(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	t *table,
	assignments []sqlast.UpdateAssignment,
	returning *query.Statement,
	returned *query.Cursor,
) (sqldriver.Result, error) {
	limits, err := tableMutationLimits(t)
	if err != nil {
		return nil, err
	}
	if err := validateDeclaredColumnAssignments(
		statement.Collection(), t.meta, assignments,
	); err != nil {
		return nil, err
	}
	if err := validateColumnAssignmentBindings(assignments, args); err != nil {
		return nil, err
	}
	if err := statement.ValidateUpdateExpressionBindings(args); err != nil {
		return nil, err
	}
	// A declared-column replacement is still subject to UPDATE's single-shard
	// predicate rule even when no row matches. Each materialized document below
	// additionally proves that the assignment did not move its shard key.
	if err := c.routeDelete(statement, args); err != nil {
		return nil, err
	}
	keys, err := c.matchingKeysLocked(
		ctx, statement, args, t, limits, 0,
	)
	if err != nil {
		return nil, err
	}

	clear(c.insertSeeds)
	seeds := c.insertSeeds[:0]
	defer func() {
		clear(seeds)
		c.insertSeeds = seeds[:0]
	}()
	if returning != nil {
		if returned == nil {
			return nil, errors.New("vibedb: internal RETURNING cursor is nil")
		}
		c.pointDocs.Reset()
	}
	if len(keys) != 0 {
		if t.collection == nil {
			return nil, errors.New(
				"vibedb: selected UPDATE keys have no materialized collection",
			)
		}
		snapshot, snapshotErr := t.collection.Snapshot()
		if snapshotErr != nil {
			return nil, snapshotErr
		}
		defer snapshot.Close()

		scratch := c.pointRaw[:0]
		defer func() { c.pointRaw = scratch[:0] }()
		stagedBytes := 0
		cancellable := contextCanCancel(ctx)
		for _, key := range keys {
			if cancellable {
				if err := contextCheckpoint(ctx); err != nil {
					return nil, err
				}
			}
			var found bool
			scratch, found, err = snapshot.AppendRaw(
				scratch[:0], []byte(key),
			)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, ErrTransactionConflict
			}
			document, err := materializeColumnAssignments(
				statement, &c.exec, scratch, assignments, args,
				limits.MaxDocumentBytes,
			)
			if err != nil {
				return nil, err
			}
			if err := validateDocument(
				t.schema, document, limits.MaxDocumentBytes, &c.insertTape,
			); err != nil {
				return nil, err
			}
			if err := c.routeUpdate(statement, args, document); err != nil {
				return nil, err
			}
			newKey, err := documentKey(
				document, t.meta.PrimaryKey, t.primary, limits.MaxKeyBytes,
			)
			if err != nil {
				return nil, err
			}
			if key != newKey {
				return nil, fmt.Errorf(
					"%w: replacement key %q does not match selected key %q",
					ErrUpdatePrimaryKey, newKey, key,
				)
			}
			if len(key) > limits.MaxBatchBytes-stagedBytes ||
				len(document) > limits.MaxBatchBytes-stagedBytes-len(key) {
				return nil, fmt.Errorf(
					"vibedb: UPDATE exceeds the %d-byte mutation batch limit for table %q: %w",
					limits.MaxBatchBytes, statement.Collection(), durable.ErrBatchTooLarge,
				)
			}
			seeds = append(seeds, seedDocument{key: key, document: document})
			stagedBytes += len(key) + len(document)
			if returning != nil {
				if _, err := c.pointDocs.Append(document); err != nil {
					return nil, err
				}
			}
		}
	}
	if err := validateTableUniquePostimages(ctx, t, seeds, keys); err != nil {
		return nil, err
	}
	if returning != nil {
		cursor, err := returning.RunInto(
			&c.exec, query.FromSegment(&c.pointDocs), nil,
		)
		if err != nil {
			return nil, err
		}
		*returned = cursor
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	if len(seeds) == 0 {
		return result{}, nil
	}
	beforeGeneration := t.collection.Generation()
	mutationErr := putSeedsAtomic(t.collection, seeds)
	if collectionMutationPublished(
		t.collection, beforeGeneration, mutationErr,
	) {
		t.conflicts.recordKeys(keys)
	}
	if mutationErr != nil {
		return nil, mutationErr
	}
	return result{affected: int64(len(seeds))}, nil
}

func (c *conn) deleteLocked(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	t *table,
) (sqldriver.Result, error) {
	return c.deleteLockedReturning(ctx, statement, args, t, nil, nil)
}

func (c *conn) deleteLockedReturning(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	t *table,
	returning *query.Statement,
	returned *query.Cursor,
) (sqldriver.Result, error) {
	limits, err := tableMutationLimits(t)
	if err != nil {
		return nil, err
	}
	if err := c.routeDelete(statement, args); err != nil {
		return nil, err
	}
	keys, err := c.matchingKeysLocked(ctx, statement, args, t, limits, 0)
	if err != nil {
		return nil, err
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	if returning != nil {
		if returned == nil {
			return nil, errors.New("vibedb: internal RETURNING cursor is nil")
		}
		if err := c.loadReturningDocuments(t, keys); err != nil {
			return nil, err
		}
		cursor, err := returning.RunInto(
			&c.exec, query.FromSegment(&c.pointDocs), nil,
		)
		if err != nil {
			return nil, err
		}
		*returned = cursor
	}
	if t.collection == nil {
		return result{}, nil
	}
	beforeGeneration := t.collection.Generation()
	affected := len(keys)
	var mutationErr error
	switch len(keys) {
	case 0:
	case 1:
		var deleted bool
		deleted, mutationErr = t.collection.Delete([]byte(keys[0]))
		if !deleted {
			affected = 0
		}
	default:
		mutationErr = t.collection.Update(func(batch *durable.WriteBatch) error {
			for _, key := range keys {
				if err := batch.Delete([]byte(key)); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if collectionMutationPublished(
		t.collection, beforeGeneration, mutationErr,
	) {
		t.conflicts.recordKeys(keys)
	}
	if mutationErr != nil {
		return nil, mutationErr
	}
	return result{affected: int64(affected)}, nil
}

// loadReturningDocuments captures the pre-delete documents in the same key
// order used by the mutation. The caller holds the catalog mutex, so the
// snapshot cannot race a driver-mediated writer; the durable snapshot still
// keeps the read boundary explicit and makes the borrowed page data safe until
// it has been copied into pointDocs.
func (c *conn) loadReturningDocuments(t *table, keys []string) error {
	c.pointDocs.Reset()
	if t.collection == nil {
		return nil
	}
	snapshot, err := t.collection.Snapshot()
	if err != nil {
		return err
	}
	defer snapshot.Close()
	for _, key := range keys {
		raw, found, err := snapshot.AppendRaw(c.pointRaw[:0], []byte(key))
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		if _, err := c.pointDocs.Append(raw); err != nil {
			return err
		}
	}
	return nil
}

func (c *conn) matchingKeysLocked(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	t *table,
	limits durable.Options,
	valueBytes int,
) ([]string, error) {
	window, err := newMutationWindow(
		statement, args, t.meta.PrimaryKey, limits.MaxBatchDocuments)
	if err != nil {
		return nil, err
	}
	if window.limited && window.limit == 0 {
		c.matchKeys = c.matchKeys[:0]
		return c.matchKeys, nil
	}
	budget := mutationMatchBudget{
		table:        statement.Collection(),
		maxDocuments: limits.MaxBatchDocuments,
		maxBytes:     limits.MaxBatchBytes,
		valueBytes:   valueBytes,
	}
	var where *sqlast.Expr
	switch statement.Tree().Kind {
	case sqlast.KindUpdate:
		if statement.Tree().Update.Filter != nil {
			where = statement.Tree().Update.Filter.Where
		}
	case sqlast.KindDelete:
		if statement.Tree().Delete.Filter != nil {
			where = statement.Tree().Delete.Filter.Where
		}
	}
	keys, point, err := primaryPredicateKeys(
		where, t.meta.PrimaryKey, args, c.pointKeys,
		limits.MaxKeyBytes)
	c.pointKeys = keys
	if point || err != nil {
		if err != nil {
			return nil, err
		}
		if t.collection == nil {
			clear(keys)
			c.pointKeys = keys[:0]
			return c.pointKeys, nil
		}
		present := keys[:0]
		selector := newMutationKeySelector(window, limits.MaxBatchDocuments)
		selector.keys = c.matchKeys[:0]
		scratch := c.pointRaw[:0]
		cancellable := ctx.Done() != nil
		for _, key := range keys {
			if cancellable {
				if err := contextCheckpoint(ctx); err != nil {
					c.pointRaw = scratch
					return nil, err
				}
			}
			var found bool
			scratch, found, err = t.collection.AppendRaw(scratch[:0], []byte(key))
			if err != nil {
				c.pointRaw = scratch
				return nil, err
			}
			if found {
				if window.limited {
					selector.add(key)
				} else {
					if err := budget.admit(len(key)); err != nil {
						c.pointRaw = scratch
						return nil, err
					}
					present = append(present, key)
				}
			}
		}
		clear(keys[len(present):])
		c.pointRaw = scratch
		c.pointKeys = present
		if !window.limited {
			return present, nil
		}
		if err := admitMutationSelection(&budget, selector.keys); err != nil {
			return nil, err
		}
		c.matchKeys = selector.keys
		return c.matchKeys, nil
	}

	clear(c.matchKeys)
	keys = c.matchKeys[:0]
	selector := newMutationKeySelector(window, limits.MaxBatchDocuments)
	selector.keys = keys
	filter, err := statement.Filter(&c.exec, args, func(key, _ []byte) error {
		if window.limited {
			selector.add(string(key))
			return nil
		}
		if err := budget.admit(len(key)); err != nil {
			return err
		}
		keys = append(keys, string(key))
		return nil
	})
	if err != nil {
		return nil, err
	}
	if t.collection == nil {
		if err := filter.Done(); err != nil {
			return nil, err
		}
		if window.limited {
			if err := admitMutationSelection(&budget, selector.keys); err != nil {
				return nil, err
			}
			c.matchKeys = selector.keys
			return c.matchKeys, nil
		}
		c.matchKeys = keys
		return keys, nil
	}
	snapshot, err := t.collection.Snapshot()
	if err != nil {
		return nil, err
	}
	defer snapshot.Close()
	if err := snapshot.RangeRaw(func(key, document []byte) error {
		return filter.Add(key, document)
	}); err != nil {
		return nil, err
	}
	if err := filter.Done(); err != nil {
		return nil, err
	}
	if window.limited {
		if err := admitMutationSelection(&budget, selector.keys); err != nil {
			return nil, err
		}
		c.matchKeys = selector.keys
		return c.matchKeys, nil
	}
	c.matchKeys = keys
	return keys, nil
}

func admitMutationSelection(
	budget *mutationMatchBudget,
	keys []string,
) error {
	for _, key := range keys {
		if err := budget.admit(len(key)); err != nil {
			return err
		}
	}
	return nil
}

// mutationMatchBudget bounds the result of a filtered UPDATE or DELETE while
// the table is still being scanned. The durable batch has the same limits, but
// discovering them only after retaining every matching key would let a
// statement that must fail grow memory with table cardinality.
type mutationMatchBudget struct {
	table        string
	documents    int
	maxDocuments int
	bytes        int
	maxBytes     int
	valueBytes   int
}

func (b *mutationMatchBudget) admit(keyBytes int) error {
	if b.documents >= b.maxDocuments {
		return fmt.Errorf(
			"vibedb: statement matches more than %d documents in table %q: %w",
			b.maxDocuments, b.table, durable.ErrBatchTooLarge,
		)
	}
	if keyBytes > b.maxBytes-b.bytes ||
		b.valueBytes > b.maxBytes-b.bytes-keyBytes {
		return fmt.Errorf(
			"vibedb: statement exceeds the %d-byte mutation batch limit for table %q: %w",
			b.maxBytes, b.table, durable.ErrBatchTooLarge,
		)
	}
	b.documents++
	b.bytes += keyBytes + b.valueBytes
	return nil
}
