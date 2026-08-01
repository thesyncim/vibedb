package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
)

type txMutation struct {
	document []byte
	remove   bool
	existed  bool
}

// stagedTxMutation is one statement-local mutation. existed is resolved for
// every new pending key before any entry is applied, so a failed snapshot read
// cannot leave half of one SQL statement visible in the transaction overlay.
type stagedTxMutation struct {
	key      string
	document []byte
	remove   bool
	existed  bool
}

type txTable struct {
	incarnation      *table
	snapshot         *durable.Snapshot
	pending          map[string]*txMutation
	order            []string
	primaryKey       string
	primary          vibejson.CompiledPointer
	schema           *store.Schema
	limits           durable.Options
	conflicts        *txConflictClock
	conflictRevision uint64
	overlaySource    query.FileOverlaySource
	emptyDocs        store.Segment
	existenceScratch []byte
	validationTape   []vibejson.IndexEntry
	stagedBytes      int
}

// tx owns the generation-leased snapshots captured by BeginTx. Writes are
// limited to one table because one durable Collection.Update is the engine's
// largest atomic publication unit.
type tx struct {
	conn       *conn
	tables     map[string]*txTable
	writeTable string
	readOnly   bool
	done       bool
}

var (
	_ sqldriver.Tx      = (*tx)(nil)
	_ query.FileOverlay = (*txTable)(nil)
)

func (c *conn) beginTx(
	ctx context.Context,
	options sqldriver.TxOptions,
) (*tx, error) {
	switch options.Isolation {
	case 0, 5: // database/sql LevelDefault and LevelSnapshot.
	default:
		return nil, fmt.Errorf(
			"%w %d; transactions provide snapshot isolation",
			ErrUnsupportedIsolation, options.Isolation)
	}
	transaction := &tx{
		conn: c, tables: make(map[string]*txTable), readOnly: options.ReadOnly,
	}
	if err := lockContext(ctx, &c.db.mu); err != nil {
		return nil, err
	}
	defer c.db.mu.Unlock()
	if c.db.closed {
		return nil, sqldriver.ErrBadConn
	}
	for name, table := range c.db.tables {
		if err := contextCheckpoint(ctx); err != nil {
			transaction.releaseSnapshots()
			return nil, err
		}
		limits, err := tableMutationLimits(table)
		if err != nil {
			transaction.releaseSnapshots()
			return nil, err
		}
		state := &txTable{
			incarnation: table,
			pending:     make(map[string]*txMutation),
			primaryKey:  table.meta.PrimaryKey,
			primary:     table.primary,
			schema:      table.schema,
			limits:      limits,
		}
		state.overlaySource = query.NewFileOverlaySource(state)
		if table.collection != nil {
			snapshot, err := table.collection.Snapshot()
			if err != nil {
				transaction.releaseSnapshots()
				return nil, err
			}
			state.snapshot = snapshot
		}
		if !options.ReadOnly {
			state.conflicts = &table.conflicts
		}
		transaction.tables[name] = state
	}
	// Snapshot() itself is synchronous. A cancellation that arrived while the
	// final table was being captured must still be observed before the
	// transaction and its leases become visible to the caller.
	if err := contextCheckpoint(ctx); err != nil {
		transaction.releaseSnapshots()
		return nil, err
	}
	if !options.ReadOnly {
		for _, state := range transaction.tables {
			state.conflictRevision = state.conflicts.begin()
		}
	}
	return transaction, nil
}

func (t *tx) querySource(tableName string) (query.Source, error) {
	if t.done {
		return query.Source{}, errors.New("vibedb: transaction is finished")
	}
	state, ok := t.tables[tableName]
	if !ok {
		return query.Source{}, fmt.Errorf(
			"%w: %q was not present when the transaction began",
			ErrTableNotFound, tableName)
	}
	if state.snapshot != nil {
		if len(state.pending) == 0 {
			return query.FromFile(state.snapshot), nil
		}
		return query.FromFileOverlay(state.snapshot, &state.overlaySource), nil
	}

	// A table without a durable file was empty at BEGIN. Its transaction view
	// therefore consists only of the bounded pending set. Refill one retained
	// Segment rather than allocate a heap Collection on every read.
	state.emptyDocs.Reset()
	for _, key := range state.order {
		mutation := state.pending[key]
		if mutation.remove {
			continue
		}
		if _, err := state.emptyDocs.Append(mutation.document); err != nil {
			return query.Source{}, err
		}
	}
	return query.FromSegment(&state.emptyDocs), nil
}

// Lookup, RangeInserts, RangePresent, and LenDelta implement query.FileOverlay
// directly over the transaction's bounded pending map. Documents remain stable
// for the execution: the connection refuses another statement while rows are
// open.
func (s *txTable) Lookup(key []byte) (value []byte, present, shadowed bool) {
	mutation, ok := s.pending[string(key)]
	if !ok {
		return nil, false, false
	}
	if mutation.remove {
		return nil, false, true
	}
	return mutation.document, true, true
}

func (s *txTable) RangeInserts(visit func(value []byte) error) error {
	for _, key := range s.order {
		mutation := s.pending[key]
		if mutation.existed || mutation.remove {
			continue
		}
		if err := visit(mutation.document); err != nil {
			return err
		}
	}
	return nil
}

func (s *txTable) RangePresent(visit func(value []byte) error) error {
	for _, key := range s.order {
		mutation := s.pending[key]
		if mutation.remove {
			continue
		}
		if err := visit(mutation.document); err != nil {
			return err
		}
	}
	return nil
}

func (s *txTable) LenDelta() int64 {
	var delta int64
	for _, key := range s.order {
		mutation := s.pending[key]
		switch {
		case mutation.existed && mutation.remove:
			delta--
		case !mutation.existed && !mutation.remove:
			delta++
		}
	}
	return delta
}

// appendRaw appends the transaction-visible document for key into dst.
func (s *txTable) appendRaw(dst []byte, key string) ([]byte, bool, error) {
	if mutation, ok := s.pending[key]; ok {
		if mutation.remove {
			return dst, false, nil
		}
		return append(dst, mutation.document...), true, nil
	}
	if s.snapshot == nil {
		return dst, false, nil
	}
	return s.snapshot.AppendRaw(dst, []byte(key))
}

func (t *tx) execMutation(statement *query.DMLStatement, args []any) (sqldriver.Result, error) {
	return t.execMutationCore(statement, args, nil, nil)
}

func (t *tx) execMutationReturning(
	statement *query.DMLStatement,
	args []any,
	returning *query.Statement,
) (query.Cursor, error) {
	var cursor query.Cursor
	_, err := t.execMutationCore(
		statement, args, returning, &cursor,
	)
	return cursor, err
}

func (t *tx) execMutationCore(
	statement *query.DMLStatement,
	args []any,
	returning *query.Statement,
	returned *query.Cursor,
) (sqldriver.Result, error) {
	if t.done {
		return nil, errors.New("vibedb: transaction is finished")
	}
	if t.readOnly {
		return nil, ErrReadOnlyTransaction
	}
	switch statement.Kind() {
	case query.DDLCreateTable, query.DDLCreateIndex, query.DDLDropTable,
		query.DDLTruncate, query.DDLDropIndex:
		return nil, ErrDDLInTransaction
	}
	tableName := statement.Collection()
	state, ok := t.tables[tableName]
	if !ok {
		return nil, fmt.Errorf(
			"%w: %q was not present when the transaction began",
			ErrTableNotFound, tableName)
	}
	t.conn.db.mu.RLock()
	_, exists := t.conn.db.tables[tableName]
	t.conn.db.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("%w: %q", ErrTableNotFound, tableName)
	}
	if t.writeTable != "" && t.writeTable != tableName {
		return nil, fmt.Errorf(
			"vibedb: a transaction writes exactly one table; it already writes %q and cannot also write %q",
			t.writeTable, tableName)
	}
	limits := state.limits

	var staged []stagedTxMutation
	switch statement.Kind() {
	case query.DMLInsert:
		tree := statement.Tree().Insert
		replaceable := 0
		for _, key := range state.order {
			if state.pending[key].remove {
				replaceable++
			}
		}
		remainingDocuments := limits.MaxBatchDocuments -
			len(state.order) + replaceable
		if !tree.OnConflictDoNothing && len(tree.Rows) > remainingDocuments {
			return nil, fmt.Errorf(
				"%w: INSERT has %d rows but table %q has room for at most %d transaction keys: %w",
				ErrTransactionTooLarge, len(tree.Rows), tableName,
				remainingDocuments, durable.ErrBatchTooLarge,
			)
		}
		seen := make(map[string]struct{}, len(tree.Rows))
		scratch := t.conn.pointRaw[:0]
		prospectiveBytes := state.stagedBytes
		for i := range tree.Rows {
			document, key, err := resolveInsertRow(
				statement, tree, &tree.Rows[i], args,
				state.primaryKey, state.primary, limits,
			)
			if err != nil {
				return nil, err
			}
			if err := validateDocument(
				state.schema, document, limits.MaxDocumentBytes,
				&state.validationTape,
			); err != nil {
				return nil, err
			}
			if _, duplicate := seen[key]; duplicate {
				if tree.OnConflictDoNothing {
					continue
				}
				return nil, fmt.Errorf("%w: %q appears twice in one VALUES batch",
					ErrDuplicatePrimaryKey, key)
			}
			seen[key] = struct{}{}
			var found bool
			scratch, found, err = state.appendRaw(scratch[:0], key)
			if err != nil {
				t.conn.pointRaw = scratch
				return nil, err
			}
			if found && tree.OnConflictDoNothing {
				continue
			}
			if found {
				t.conn.pointRaw = scratch
				return nil, fmt.Errorf("%w: %q", ErrDuplicatePrimaryKey, key)
			}
			previous := state.pending[key]
			if previous != nil {
				prospectiveBytes -= len(key) + len(previous.document)
			}
			prospectiveBytes += len(key) + len(document)
			if prospectiveBytes > limits.MaxBatchBytes {
				t.conn.pointRaw = scratch
				return nil, fmt.Errorf(
					"%w: INSERT would stage %d key/document bytes for table %q, limit %d: %w",
					ErrTransactionTooLarge, prospectiveBytes, tableName,
					limits.MaxBatchBytes, durable.ErrBatchTooLarge,
				)
			}
			staged = append(staged, stagedTxMutation{key: key, document: document})
		}
		t.conn.pointRaw = scratch
	case query.DMLUpdate:
		document, err := operandDocument(
			statement, statement.Tree().Update.Doc, args)
		if err != nil {
			return nil, err
		}
		if len(document) > limits.MaxDocumentBytes {
			return nil, durable.ErrDocumentTooLarge
		}
		keys, err := t.conn.matchingKeysTransaction(
			statement, args, state, len(document))
		if err != nil {
			return nil, err
		}
		if len(keys) != 0 {
			if err := validateDocument(
				state.schema, document, limits.MaxDocumentBytes,
				&state.validationTape,
			); err != nil {
				return nil, err
			}
		}
		newKey, err := documentKey(
			document, state.primaryKey, state.primary, limits.MaxKeyBytes,
		)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if key != newKey {
				return nil, errors.New("vibedb: UPDATE cannot change a document's primary key")
			}
			staged = append(staged, stagedTxMutation{key: key, document: document})
		}
	case query.DMLDelete:
		keys, err := t.conn.matchingKeysTransaction(
			statement, args, state, 0)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			staged = append(staged, stagedTxMutation{key: key, remove: true})
		}
	default:
		return nil, fmt.Errorf("vibedb: unsupported transaction statement %s", statement.Kind())
	}

	limit := limits.MaxBatchDocuments
	distinct := len(state.order)
	for _, mutation := range staged {
		if _, present := state.pending[mutation.key]; !present {
			distinct++
		}
	}
	if distinct > limit {
		return nil, fmt.Errorf(
			"%w: statement would bring table %q to %d distinct keys, limit %d: %w",
			ErrTransactionTooLarge, tableName, distinct, limit, durable.ErrBatchTooLarge)
	}
	stagedBytes := state.stagedBytes
	for _, mutation := range staged {
		if previous := state.pending[mutation.key]; previous != nil {
			stagedBytes -= len(mutation.key) + len(previous.document)
		}
		stagedBytes += len(mutation.key) + len(mutation.document)
		if stagedBytes > limits.MaxBatchBytes {
			return nil, fmt.Errorf(
				"%w: statement would stage %d key/document bytes for table %q, limit %d: %w",
				ErrTransactionTooLarge, stagedBytes, tableName,
				limits.MaxBatchBytes, durable.ErrBatchTooLarge,
			)
		}
	}
	if err := t.resolveStagedMutations(state, staged); err != nil {
		return nil, err
	}
	if returning != nil {
		if returned == nil {
			return nil, errors.New(
				"vibedb: internal RETURNING cursor is nil",
			)
		}
		t.conn.pointDocs.Reset()
		for i := range staged {
			var document []byte
			if statement.Kind() == query.DMLDelete {
				raw, found, err := state.appendRaw(t.conn.pointRaw[:0], staged[i].key)
				if err != nil {
					return nil, err
				}
				if !found {
					continue
				}
				document = raw
			} else {
				document = staged[i].document
			}
			if _, err := t.conn.pointDocs.Append(document); err != nil {
				return nil, err
			}
		}
		cursor, err := returning.RunInto(
			&t.conn.exec, query.FromSegment(&t.conn.pointDocs), nil,
		)
		if err != nil {
			return nil, err
		}
		*returned = cursor
	}
	t.applyResolvedMutations(state, staged)
	if len(staged) != 0 {
		t.writeTable = tableName
	}
	return result{affected: int64(len(staged))}, nil
}

// matchingKeysTransaction evaluates a DML predicate directly over the BEGIN
// snapshot plus the bounded pending overlay. Primary-key predicates stay
// point reads; every other predicate streams the merged view once. No heap
// collection proportional to the base table is built.
func (c *conn) matchingKeysTransaction(
	statement *query.DMLStatement,
	args []any,
	state *txTable,
	valueBytes int,
) ([]string, error) {
	window, err := newMutationWindow(
		statement, args, state.primaryKey, state.limits.MaxBatchDocuments)
	if err != nil {
		return nil, err
	}
	if window.limited && window.limit == 0 {
		c.matchKeys = c.matchKeys[:0]
		return c.matchKeys, nil
	}
	budget := transactionMatchBudget{
		table:      statement.Collection(),
		state:      state,
		valueBytes: valueBytes,
		distinct:   len(state.order),
		bytes:      state.stagedBytes,
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
		where, state.primaryKey, args, c.pointKeys,
		state.limits.MaxKeyBytes)
	c.pointKeys = keys
	if point || err != nil {
		if err != nil {
			return nil, err
		}
		present := keys[:0]
		selector := newMutationKeySelector(window, state.limits.MaxBatchDocuments)
		selector.keys = c.matchKeys[:0]
		scratch := c.pointRaw[:0]
		for _, key := range keys {
			var found bool
			scratch, found, err = state.appendRaw(scratch[:0], key)
			if err != nil {
				c.pointRaw = scratch
				return nil, err
			}
			if found {
				if window.limited {
					selector.add(key)
				} else {
					if err := budget.admit(key); err != nil {
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
		if err := admitTransactionSelection(&budget, selector.keys); err != nil {
			return nil, err
		}
		c.matchKeys = selector.keys
		return c.matchKeys, nil
	}

	clear(c.matchKeys)
	keys = c.matchKeys[:0]
	selector := newMutationKeySelector(window, state.limits.MaxBatchDocuments)
	selector.keys = keys
	filter, err := statement.Filter(&c.exec, args, func(key, _ []byte) error {
		owned := string(key)
		if window.limited {
			selector.add(owned)
			return nil
		}
		if err := budget.admit(owned); err != nil {
			return err
		}
		keys = append(keys, owned)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if state.snapshot != nil {
		if err := state.snapshot.RangeRaw(func(key, document []byte) error {
			if mutation, shadowed := state.pending[string(key)]; shadowed {
				if mutation.remove {
					return nil
				}
				return filter.Add(key, mutation.document)
			}
			return filter.Add(key, document)
		}); err != nil {
			return nil, err
		}
	}
	for _, key := range state.order {
		mutation := state.pending[key]
		if mutation.existed || mutation.remove {
			continue
		}
		if err := filter.Add([]byte(key), mutation.document); err != nil {
			return nil, err
		}
	}
	if err := filter.Done(); err != nil {
		return nil, err
	}
	if window.limited {
		if err := admitTransactionSelection(&budget, selector.keys); err != nil {
			return nil, err
		}
		c.matchKeys = selector.keys
		return c.matchKeys, nil
	}
	c.matchKeys = keys
	return keys, nil
}

func admitTransactionSelection(
	budget *transactionMatchBudget,
	keys []string,
) error {
	for _, key := range keys {
		if err := budget.admit(key); err != nil {
			return err
		}
	}
	return nil
}

// transactionMatchBudget applies the transaction's distinct-key and byte
// admission while a filtered mutation is still streaming its BEGIN snapshot.
// Existing pending entries are replacements rather than additional keys, and
// their prior value bytes stop counting exactly as they will in execMutation's
// final admission pass.
type transactionMatchBudget struct {
	table      string
	state      *txTable
	valueBytes int
	distinct   int
	bytes      int
}

func (b *transactionMatchBudget) admit(key string) error {
	limits := b.state.limits
	nextDistinct := b.distinct
	nextBytes := b.bytes
	if previous, present := b.state.pending[key]; present {
		nextBytes -= len(key) + len(previous.document)
	} else {
		nextDistinct++
	}
	if nextDistinct > limits.MaxBatchDocuments {
		return fmt.Errorf(
			"%w: statement would bring table %q above %d distinct transaction keys: %w",
			ErrTransactionTooLarge, b.table, limits.MaxBatchDocuments,
			durable.ErrBatchTooLarge,
		)
	}
	if len(key) > limits.MaxBatchBytes-nextBytes ||
		b.valueBytes > limits.MaxBatchBytes-nextBytes-len(key) {
		return fmt.Errorf(
			"%w: statement would exceed the %d-byte transaction batch limit for table %q: %w",
			ErrTransactionTooLarge, limits.MaxBatchBytes, b.table,
			durable.ErrBatchTooLarge,
		)
	}
	b.distinct = nextDistinct
	b.bytes = nextBytes + len(key) + b.valueBytes
	return nil
}

func (t *tx) stage(state *txTable, key string, document []byte, remove bool) error {
	existed := false
	if entry, present := state.pending[key]; present {
		existed = entry.existed
	} else if state.snapshot != nil {
		raw, found, err := state.snapshot.AppendRaw(
			state.existenceScratch[:0], []byte(key),
		)
		state.existenceScratch = raw[:0]
		if err != nil {
			return err
		}
		existed = found
	}
	t.stageKnown(state, key, document, remove, existed)
	return nil
}

// applyStagedMutations is the transaction statement's two-phase publication.
// Resolve every potentially fallible snapshot lookup before changing pending;
// once that pass succeeds, stageKnown only updates transaction-owned memory.
func (t *tx) applyStagedMutations(
	state *txTable,
	staged []stagedTxMutation,
) error {
	if err := t.resolveStagedMutations(state, staged); err != nil {
		return err
	}
	t.applyResolvedMutations(state, staged)
	return nil
}

func (t *tx) resolveStagedMutations(
	state *txTable,
	staged []stagedTxMutation,
) error {
	for i := range staged {
		mutation := &staged[i]
		if entry, present := state.pending[mutation.key]; present {
			mutation.existed = entry.existed
			continue
		}
		if state.snapshot == nil {
			continue
		}
		raw, found, err := state.snapshot.AppendRaw(
			state.existenceScratch[:0], []byte(mutation.key),
		)
		state.existenceScratch = raw[:0]
		if err != nil {
			return err
		}
		mutation.existed = found
	}
	return nil
}

func (t *tx) applyResolvedMutations(
	state *txTable,
	staged []stagedTxMutation,
) {
	for i := range staged {
		mutation := &staged[i]
		t.stageKnown(
			state, mutation.key, mutation.document,
			mutation.remove, mutation.existed,
		)
	}
}

func (t *tx) stageKnown(
	state *txTable,
	key string,
	document []byte,
	remove, existed bool,
) {
	entry, present := state.pending[key]
	if !present {
		entry = &txMutation{existed: existed}
		state.pending[key] = entry
		state.order = append(state.order, key)
	} else {
		state.stagedBytes -= len(key) + len(entry.document)
	}
	entry.remove = remove
	if remove {
		entry.document = nil
	} else {
		entry.document = append(entry.document[:0], document...)
	}
	state.stagedBytes += len(key) + len(entry.document)
}

func (t *tx) Commit() error {
	if t.done {
		return errors.New("vibedb: transaction is already finished")
	}
	defer t.finish()
	if t.writeTable == "" {
		return nil
	}
	state := t.tables[t.writeTable]
	t.releaseSnapshots()

	t.conn.db.mu.Lock()
	defer t.conn.db.mu.Unlock()
	if err := t.conn.db.settleCatalogLocked(); err != nil {
		return err
	}
	table := t.conn.db.tables[t.writeTable]
	if table == nil || table != state.incarnation {
		return fmt.Errorf(
			"%w: table %q was dropped or replaced after BEGIN",
			ErrTransactionConflict, t.writeTable,
		)
	}
	key, historyOverflow, conflict := state.conflicts.conflict(
		state.conflictRevision, state.order,
	)
	if conflict {
		if historyOverflow {
			return fmt.Errorf(
				"%w: table %q exceeded its bounded conflict history after BEGIN",
				ErrTransactionConflict, t.writeTable,
			)
		}
		return fmt.Errorf(
			"%w: table %q key %q changed after BEGIN",
			ErrTransactionConflict, t.writeTable, key,
		)
	}
	if table.collection == nil {
		seeds := make([]seedDocument, 0, len(state.order))
		for _, key := range state.order {
			entry := state.pending[key]
			if !entry.remove {
				seeds = append(seeds, seedDocument{
					key: key, document: entry.document,
				})
			}
		}
		if len(seeds) == 0 {
			return nil
		}
		_, err := t.conn.db.materializeLocked(t.writeTable, seeds)
		if table.collection != nil {
			table.conflicts.recordTransaction(state)
		}
		return transactionBatchError(err)
	}
	collection := table.collection
	// Validate the write set against the currently committed state BEFORE
	// staging the batch: Collection.Update holds the writer mutex across fn and
	// Snapshot re-acquires it on the deferred-canonical lane, so a conflict
	// check inside fn self-deadlocks. The whole commit runs under db.mu, which
	// serializes every driver-mediated mutation, so a snapshot taken here
	// observes all prior commits and nothing can interleave between check and
	// apply.
	if err := t.validateWriteSet(collection, state); err != nil {
		return err
	}
	beforeGeneration := collection.Generation()
	err := collection.Update(func(batch *durable.WriteBatch) error {
		for _, key := range state.order {
			entry := state.pending[key]
			// INSERT followed by DELETE in one transaction is a net no-op for
			// a key absent at BEGIN. Do not make another transaction conflict
			// with an event that was never published.
			if !entry.existed && entry.remove {
				continue
			}
			if entry.remove {
				if err := batch.Delete([]byte(key)); err != nil {
					return transactionBatchError(err)
				}
			} else if err := batch.Put([]byte(key), entry.document); err != nil {
				return transactionBatchError(err)
			}
		}
		return nil
	})
	if collectionMutationPublished(collection, beforeGeneration, err) {
		table.conflicts.recordTransaction(state)
	}
	return transactionBatchError(err)
}

// validateWriteSet enforces optimistic-concurrency: every key the transaction
// touched must still hold the value it held at BEGIN, otherwise a concurrent
// commit changed it and this transaction must abort. It reads a fresh snapshot
// of the currently committed state; the caller runs under db.mu, so that state
// is stable for the duration of the check and the subsequent apply.
func (t *tx) validateWriteSet(collection *durable.Collection, state *txTable) error {
	live, err := collection.Snapshot()
	if err != nil {
		return err
	}
	defer live.Close()
	var scratch []byte
	for _, key := range state.order {
		entry := state.pending[key]
		current, found, err := live.AppendRaw(scratch[:0], []byte(key))
		if err != nil {
			return err
		}
		scratch = current
		if found != entry.existed {
			return fmt.Errorf(
				"%w: table %q key %q changed after BEGIN",
				ErrTransactionConflict, t.writeTable, key)
		}
	}
	return nil
}

func transactionBatchError(err error) error {
	if errors.Is(err, durable.ErrBatchTooLarge) {
		return fmt.Errorf("%w: %w", ErrTransactionTooLarge, err)
	}
	if errors.Is(err, durable.ErrPrimaryBatchUnsupportedLane) {
		return fmt.Errorf("%w: %w", ErrTransactionUnsupportedLane, err)
	}
	return err
}

func (t *tx) Rollback() error {
	if t.done {
		return nil
	}
	t.finish()
	return nil
}

func (t *tx) finish() {
	t.done = true
	t.releaseSnapshots()
	if t.conn != nil {
		connection := t.conn
		connection.db.mu.Lock()
		for _, state := range t.tables {
			if state.conflicts != nil {
				state.conflicts.finish(state.conflictRevision)
			}
		}
		connection.db.mu.Unlock()
		if connection.tx == t {
			connection.tx = nil
		}
	}
	// A completed transaction is never executable again. Drop the connection
	// and table graph so retaining the driver.Tx value cannot pin the database,
	// staged documents, validation tapes, or overlay high-water storage.
	t.conn = nil
	t.tables = nil
	t.writeTable = ""
}

func (t *tx) releaseSnapshots() {
	for _, table := range t.tables {
		if table.snapshot != nil {
			_ = table.snapshot.Close()
			table.snapshot = nil
		}
	}
}
