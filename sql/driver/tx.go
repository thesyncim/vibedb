package driver

import (
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"slices"
	"unsafe"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/x/byteview"
)

type txMutation struct {
	document         []byte
	remove           bool
	existed          bool
	conflictRevision uint64
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

type insertSelectStageAccount struct {
	budget          insertSelectIntermediateBudget
	sourceRootBytes int64
	active          bool
}

type txTable struct {
	name              string
	incarnation       *table
	snapshot          *durable.Snapshot
	refreshSnapshot   *durable.Snapshot
	refreshCaptured   bool
	pending           map[string]*txMutation
	order             []string
	primaryKey        string
	primary           vibejson.CompiledPointer
	schema            *store.Schema
	limits            durable.Options
	conflicts         *txConflictClock
	conflictRevision  uint64
	statementRevision uint64
	serialRead        bool
	overlaySource     query.FileOverlaySource
	emptyDocs         store.Segment
	existenceScratch  []byte
	validationTape    []vibejson.IndexEntry
	stagedBytes       int
	// highWaterKeys and highWaterBytes track peak overlay occupancy for
	// admission. ROLLBACK TO restores order and stagedBytes but does not lower
	// these watermarks.
	highWaterKeys  int
	highWaterBytes int
	keyChunk       []byte
	keyChunks      [][]byte
}

// tx owns one coherent generation-leased cut. Read Committed replaces its base
// snapshots at statement boundaries; fixed-cut modes retain the BEGIN cut. The
// dirty set is every table with a non-empty overlay; COMMIT validates each
// participant and publishes through Collection.Update or UpdateCollections.
type tx struct {
	conn       *conn
	tables     map[string]*txTable
	views      map[string]*viewMeta
	savepoints []savepointFrame
	readOnly   bool
	isolation  IsolationLevel
	done       bool
	staged     []stagedTxMutation
}

var (
	_ sqldriver.Tx      = (*tx)(nil)
	_ query.FileOverlay = (*txTable)(nil)
)

func (c *conn) beginTx(
	ctx context.Context,
	options sqldriver.TxOptions,
) (*tx, error) {
	isolation, err := runtimeIsolationLevel(options.Isolation)
	if err != nil {
		return nil, err
	}
	transaction := &tx{
		conn: c, tables: make(map[string]*txTable),
		readOnly: options.ReadOnly, isolation: isolation,
	}
	if err := lockContext(ctx, &c.db.mu); err != nil {
		return nil, err
	}
	defer c.db.mu.Unlock()
	if c.db.closed {
		return nil, sqldriver.ErrBadConn
	}
	if len(c.db.catalog.Views) != 0 {
		transaction.views = make(map[string]*viewMeta, len(c.db.catalog.Views))
		for name, meta := range c.db.catalog.Views {
			transaction.views[name] = meta
		}
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
			name:        name,
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
			state.statementRevision = state.conflictRevision
		}
	}
	return transaction, nil
}

func runtimeIsolationLevel(level sqldriver.IsolationLevel) (IsolationLevel, error) {
	switch level {
	case 0, 2: // database/sql LevelDefault and LevelReadCommitted.
		return IsolationReadCommitted, nil
	case 4, 5: // LevelRepeatableRead and LevelSnapshot.
		return IsolationRepeatableRead, nil
	case 6: // LevelSerializable.
		return IsolationSerializable, nil
	default:
		return IsolationDefault, fmt.Errorf(
			"%w %d", ErrUnsupportedIsolation, level)
	}
}

func driverIsolationLevel(level IsolationLevel) (sqldriver.IsolationLevel, error) {
	switch level {
	case IsolationDefault, IsolationReadCommitted:
		return sqldriver.IsolationLevel(2), nil
	case IsolationRepeatableRead:
		return sqldriver.IsolationLevel(4), nil
	case IsolationSerializable:
		return sqldriver.IsolationLevel(6), nil
	default:
		return 0, fmt.Errorf("%w %d", ErrUnsupportedIsolation, level)
	}
}

// refreshStatementCut replaces only the committed base of every transaction
// table. Pending mutations and savepoint marks remain attached to txTable, so
// Read Committed sees a fresh coherent catalog cut without losing its overlay.
// Catalog shape and storage identities are deliberately fail-closed: prepared
// metadata cannot be rebound safely in the middle of a transaction. Every new
// lease is captured into reusable staging first; cancellation or capture
// failure closes staging and leaves the active cut completely untouched.
func (t *tx) refreshStatementCut(ctx context.Context) error {
	if t.isolation != IsolationReadCommitted {
		return nil
	}
	if t.done || t.conn == nil || t.conn.db == nil {
		return errors.New("vibedb: transaction is finished")
	}
	if err := lockContext(ctx, &t.conn.db.mu); err != nil {
		return err
	}
	db := t.conn.db
	if db.closed {
		db.mu.Unlock()
		return sqldriver.ErrBadConn
	}
	if err := db.settleCatalogLocked(); err != nil {
		db.mu.Unlock()
		return err
	}
	if len(db.tables) != len(t.tables) || len(db.catalog.Views) != len(t.views) {
		db.mu.Unlock()
		return fmt.Errorf(
			"%w: catalog shape changed during a READ COMMITTED transaction",
			ErrTransactionConflict,
		)
	}
	for name, state := range t.tables {
		if db.tables[name] != state.incarnation {
			db.mu.Unlock()
			return fmt.Errorf(
				"%w: table %q was dropped or replaced during a READ COMMITTED transaction",
				ErrTransactionConflict, name,
			)
		}
	}
	for name, meta := range t.views {
		if db.catalog.Views[name] != meta {
			db.mu.Unlock()
			return fmt.Errorf(
				"%w: view %q changed during a READ COMMITTED transaction",
				ErrTransactionConflict, name,
			)
		}
	}

	for name, state := range t.tables {
		if err := contextCheckpoint(ctx); err != nil {
			cleanupErr := t.closeRefreshSnapshots()
			db.mu.Unlock()
			return joinRefreshError(err, cleanupErr)
		}
		collection := db.tables[name].collection
		if collection == nil {
			if state.snapshot != nil {
				cleanupErr := t.closeRefreshSnapshots()
				db.mu.Unlock()
				return joinRefreshError(fmt.Errorf(
					"%w: table %q lost its storage incarnation during a READ COMMITTED transaction",
					ErrTransactionConflict, name,
				), cleanupErr)
			}
			continue
		}
		if state.refreshSnapshot == nil {
			state.refreshSnapshot = new(durable.Snapshot)
		}
		if err := collection.SnapshotInto(state.refreshSnapshot); err != nil {
			cleanupErr := t.closeRefreshSnapshots()
			db.mu.Unlock()
			return joinRefreshError(err, cleanupErr)
		}
		state.refreshCaptured = true
	}
	if err := contextCheckpoint(ctx); err != nil {
		cleanupErr := t.closeRefreshSnapshots()
		db.mu.Unlock()
		return joinRefreshError(err, cleanupErr)
	}
	for _, state := range t.tables {
		if !state.refreshCaptured {
			continue
		}
		state.snapshot, state.refreshSnapshot =
			state.refreshSnapshot, state.snapshot
		state.refreshCaptured = false
	}
	// Sample only after the complete cut has installed, while db.mu still
	// excludes publications. New pending keys inherit this exact statement
	// observation; existing pending keys retain their earlier revision.
	for _, state := range t.tables {
		if state.conflicts != nil {
			state.statementRevision = state.conflicts.observe()
		}
	}
	db.mu.Unlock()
	var closeErr error
	for _, state := range t.tables {
		if state.refreshSnapshot == nil {
			continue
		}
		if err := state.refreshSnapshot.Close(); err != nil {
			if closeErr == nil {
				closeErr = err
			} else {
				closeErr = errors.Join(closeErr, err)
			}
		}
	}
	return closeErr
}

func (t *tx) closeRefreshSnapshots() error {
	var closeErr error
	for _, state := range t.tables {
		if !state.refreshCaptured {
			continue
		}
		state.refreshCaptured = false
		if err := state.refreshSnapshot.Close(); err != nil {
			if closeErr == nil {
				closeErr = err
			} else {
				closeErr = errors.Join(closeErr, err)
			}
		}
	}
	return closeErr
}

func joinRefreshError(primary, cleanup error) error {
	if cleanup == nil {
		return primary
	}
	return errors.Join(primary, cleanup)
}

func (t *tx) markSerializableRead(name string) {
	if t.isolation != IsolationSerializable {
		return
	}
	if state := t.tables[name]; state != nil {
		state.serialRead = true
	}
}

func (t *tx) beginQueryStatement(ctx context.Context, statement *stmt) error {
	if err := t.refreshStatementCut(ctx); err != nil {
		return err
	}
	if t.isolation != IsolationSerializable || statement == nil {
		return nil
	}
	for i := range statement.dependencies {
		t.markSerializableRead(statement.dependencies[i].name)
	}
	if statement.query != nil {
		t.markSerializableRead(statement.query.Collection())
	}
	return nil
}

func (t *tx) beginMutationStatement(
	ctx context.Context,
	statement *query.DMLStatement,
	prepared *stmt,
) error {
	if err := t.refreshStatementCut(ctx); err != nil {
		return err
	}
	if t.isolation != IsolationSerializable || statement == nil {
		return nil
	}
	t.markSerializableRead(statement.Collection())
	if prepared != nil {
		for i := range prepared.dependencies {
			t.markSerializableRead(prepared.dependencies[i].name)
		}
	}
	if prepared != nil && prepared.insertSource != nil {
		for i := range prepared.insertSource.dependencies {
			t.markSerializableRead(prepared.insertSource.dependencies[i].name)
		}
		if prepared.insertSource.statement != nil {
			t.markSerializableRead(prepared.insertSource.statement.Collection())
		}
	}
	return nil
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

func (s *txTable) contains(key string) (bool, error) {
	if mutation, ok := s.pending[key]; ok {
		return !mutation.remove, nil
	}
	if s.snapshot == nil {
		return false, nil
	}
	return s.snapshot.ContainsKey([]byte(key))
}

func (t *tx) execMutation(statement *query.DMLStatement, args []any) (sqldriver.Result, error) {
	return t.execMutationContext(backgroundContext, statement, args)
}

func (t *tx) execMutationContext(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
) (sqldriver.Result, error) {
	return t.execMutationCore(ctx, statement, args, nil, nil, nil)
}

func (t *tx) execMutationReturningContext(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	returning *query.Statement,
) (query.Cursor, error) {
	var cursor query.Cursor
	_, err := t.execMutationCore(
		ctx, statement, args, returning, &cursor, nil,
	)
	return cursor, err
}

func (t *tx) execPreparedMutationContext(
	ctx context.Context,
	prepared *stmt,
	args []any,
) (sqldriver.Result, error) {
	if prepared == nil {
		return nil, errors.New("vibedb: internal prepared transaction mutation is nil")
	}
	return t.execMutationCore(
		ctx, prepared.mutation, args, nil, nil, prepared,
	)
}

func (t *tx) execPreparedMutationReturningContext(
	ctx context.Context,
	prepared *stmt,
	args []any,
) (query.Cursor, error) {
	if prepared == nil {
		return query.Cursor{}, errors.New(
			"vibedb: internal prepared transaction mutation is nil",
		)
	}
	var cursor query.Cursor
	_, err := t.execMutationCore(
		ctx, prepared.mutation, args, prepared.query, &cursor, prepared,
	)
	return cursor, err
}

func (t *tx) execMutationCore(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	returning *query.Statement,
	returned *query.Cursor,
	prepared *stmt,
) (sqldriver.Result, error) {
	ctx = withCooperativeCancellation(ctx, t.conn.exec.Options.Cancel)
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	if t.done {
		return nil, errors.New("vibedb: transaction is finished")
	}
	if t.readOnly {
		return nil, ErrReadOnlyTransaction
	}
	if err := t.beginMutationStatement(ctx, statement, prepared); err != nil {
		return nil, err
	}
	if prepared != nil {
		if err := prepared.validateTransactionViewDependencies(); err != nil {
			return nil, err
		}
	}
	switch statement.Kind() {
	case query.DDLCreateTable, query.DDLCreateIndex, query.DDLDropTable,
		query.DDLTruncate, query.DDLDropIndex:
		return nil, ErrDDLInTransaction
	}
	if err := t.validateViewTableTarget(statement.Tree()); err != nil {
		return nil, err
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
	limits := state.limits

	clear(t.staged)
	staged := t.staged[:0]
	var insertSourceAccount insertSelectStageAccount
	defer func() {
		clear(staged)
		t.staged = staged[:0]
	}()
	switch statement.Kind() {
	case query.DMLInsert:
		tree := statement.Tree().Insert
		if prepared != nil && prepared.insertSource != nil {
			var err error
			staged, err = t.stageInsertSelect(
				ctx, statement, args, state, prepared.insertSource, staged,
				&insertSourceAccount,
			)
			if err != nil {
				return nil, err
			}
			break
		}
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
		cancellable := ctx.Done() != nil
		for i := range tree.Rows {
			if cancellable {
				if err := contextCheckpoint(ctx); err != nil {
					t.conn.pointRaw = scratch
					return nil, err
				}
			}
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
		if err := t.conn.routeInsertStaged(tableName, staged); err != nil {
			return nil, err
		}
	case query.DMLUpdate:
		document, err := operandDocument(
			statement, statement.Tree().Update.Doc, args)
		if err != nil {
			return nil, err
		}
		if len(document) > limits.MaxDocumentBytes {
			return nil, durable.ErrDocumentTooLarge
		}
		if err := t.conn.routeUpdate(statement, args, document); err != nil {
			return nil, err
		}
		keys, err := t.conn.matchingKeysTransaction(
			ctx, statement, args, state, len(document))
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
		if err := t.conn.routeDelete(statement, args); err != nil {
			return nil, err
		}
		keys, err := t.conn.matchingKeysTransaction(
			ctx, statement, args, state, 0)
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
	distinct := state.highWaterKeys
	if distinct < len(state.order) {
		distinct = len(state.order)
	}
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
	stagedBytes := state.highWaterBytes
	if stagedBytes < state.stagedBytes {
		stagedBytes = state.stagedBytes
	}
	probeBytes := state.stagedBytes
	for _, mutation := range staged {
		if previous := state.pending[mutation.key]; previous != nil {
			probeBytes -= len(mutation.key) + len(previous.document)
		}
		probeBytes += len(mutation.key) + len(mutation.document)
		if probeBytes > stagedBytes {
			stagedBytes = probeBytes
		}
		if stagedBytes > limits.MaxBatchBytes {
			return nil, fmt.Errorf(
				"%w: statement would stage %d key/document bytes for table %q, limit %d: %w",
				ErrTransactionTooLarge, stagedBytes, tableName,
				limits.MaxBatchBytes, durable.ErrBatchTooLarge,
			)
		}
	}
	if err := t.resolveStagedMutationsContext(ctx, state, staged); err != nil {
		return nil, err
	}
	if returning != nil {
		if returned == nil {
			return nil, errors.New(
				"vibedb: internal RETURNING cursor is nil",
			)
		}
		insertSource := statement.Kind() == query.DMLInsert &&
			prepared != nil && prepared.insertSource != nil
		if !insertSource {
			t.conn.pointDocs.Reset()
		}
		for i := range staged {
			if insertSource {
				break
			}
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
		var cursor query.Cursor
		var err error
		if insertSource {
			if !insertSourceAccount.active {
				return nil, errors.New(
					"vibedb: internal transaction INSERT SELECT account is inactive",
				)
			}
			cursor, err = runInsertSelectReturning(
				returning, &t.conn.exec,
				query.FromSegment(&t.conn.pointDocs),
				&insertSourceAccount.budget,
				insertSourceAccount.sourceRootBytes,
			)
		} else {
			cursor, err = returning.RunInto(
				&t.conn.exec, query.FromSegment(&t.conn.pointDocs), nil,
			)
		}
		if err != nil {
			return nil, err
		}
		*returned = cursor
	}
	if err := contextCheckpoint(ctx); err != nil {
		return nil, err
	}
	t.applyResolvedMutations(state, staged)
	return result{affected: int64(len(staged))}, nil
}

// stageInsertSelect evaluates the source against the transaction view that
// existed at statement entry, then resolves every fallible write decision into
// statement-local storage. applyResolvedMutations remains the only publication
// into the transaction overlay, so an error cannot expose a prefix even to the
// next statement in the same transaction.
func (t *tx) stageInsertSelect(
	ctx context.Context,
	statement *query.DMLStatement,
	args []any,
	state *txTable,
	plan *insertSelectPlan,
	dst []stagedTxMutation,
	account *insertSelectStageAccount,
) ([]stagedTxMutation, error) {
	if account == nil {
		return dst, errors.New(
			"vibedb: internal transaction INSERT SELECT account is nil",
		)
	}
	*account = insertSelectStageAccount{}
	cursor, sourceRetained, err := t.runInsertSource(ctx, plan, args)
	if err != nil {
		return dst, err
	}
	c := t.conn
	intermediate, err := newInsertSelectIntermediateBudget(
		c.exec.Options, sourceRetained,
	)
	if err != nil {
		return dst, err
	}
	sourceRootBytes := c.exec.Result.RetainedBytes()
	c.pointDocs.Reset()
	c.insertKeyRaw = c.insertKeyRaw[:0]
	if c.insertSeen == nil {
		c.insertSeen = make(map[string]struct{})
	} else {
		clear(c.insertSeen)
	}
	defer clear(c.insertSeen)
	scratch := c.pointRaw[:0]
	prospectiveDistinct := len(state.order)
	prospectiveBytes := state.stagedBytes
	insert := statement.Tree().Insert
	placement := c.clusterBinding(insert.Table)
	cancellable := ctx.Done() != nil
	row := 0
	for {
		next, nextErr := cursor.NextWithCancel(c.exec.Options.Cancel)
		if nextErr != nil {
			c.pointRaw = scratch
			return dst, nextErr
		}
		if !next {
			break
		}
		if cancellable {
			if err := contextCheckpoint(ctx); err != nil {
				c.pointRaw = scratch
				return dst, err
			}
		}
		cell := cursor.Cell(0)
		if cell.Kind() != query.TypeJSON {
			c.pointRaw = scratch
			return dst, &query.InsertSelectShapeError{
				Pos: insertSelectOutputPosition(insert), Columns: 1,
				Type: cell.Kind(), Row: row,
			}
		}
		document := cell.Payload()
		if err := validateDocumentWithIntermediateBudget(
			state.schema, document, state.limits.MaxDocumentBytes,
			&state.validationTape, &intermediate,
		); err != nil {
			c.pointRaw = scratch
			return dst, err
		}
		keyStart := len(c.insertKeyRaw)
		var key string
		var keyCharge int64
		c.insertKeyRaw, key, keyCharge, err = appendDocumentKeyBudgeted(
			c.insertKeyRaw, document, state.primaryKey,
			state.primary, state.limits.MaxKeyBytes, &intermediate,
		)
		if err != nil {
			c.pointRaw = scratch
			return dst, err
		}
		if _, duplicate := c.insertSeen[key]; duplicate {
			c.insertKeyRaw = c.insertKeyRaw[:keyStart]
			intermediate.release(keyCharge)
			if insert.OnConflictDoNothing {
				row++
				continue
			}
			c.pointRaw = scratch
			return dst, fmt.Errorf(
				"%w: %q appears twice in one SELECT source",
				ErrDuplicatePrimaryKey, key,
			)
		}
		c.insertSeen[key] = struct{}{}
		var found bool
		found, err = state.contains(key)
		if err != nil {
			c.pointRaw = scratch
			return dst, err
		}
		if found {
			if insert.OnConflictDoNothing {
				row++
				continue
			}
			c.pointRaw = scratch
			return dst, fmt.Errorf("%w: %q", ErrDuplicatePrimaryKey, key)
		}
		nextDistinct := prospectiveDistinct
		nextBytes := prospectiveBytes
		if previous, present := state.pending[key]; present {
			nextBytes -= len(key) + len(previous.document)
		} else {
			nextDistinct++
		}
		if nextDistinct > state.limits.MaxBatchDocuments {
			c.pointRaw = scratch
			return dst, fmt.Errorf(
				"%w: INSERT SELECT would bring table %q above %d transaction keys: %w",
				ErrTransactionTooLarge, insert.Table,
				state.limits.MaxBatchDocuments, durable.ErrBatchTooLarge,
			)
		}
		if len(key) > state.limits.MaxBatchBytes-nextBytes ||
			len(document) > state.limits.MaxBatchBytes-nextBytes-len(key) {
			c.pointRaw = scratch
			return dst, fmt.Errorf(
				"%w: INSERT SELECT exceeds the %d-byte transaction batch limit for table %q: %w",
				ErrTransactionTooLarge, state.limits.MaxBatchBytes,
				insert.Table, durable.ErrBatchTooLarge,
			)
		}
		if err := intermediate.admit(insertSelectStagedRowBytesFor(
			document, key, state.validationTape,
			int64(unsafe.Sizeof(stagedTxMutation{})),
		)); err != nil {
			c.pointRaw = scratch
			return dst, err
		}
		ordinal, appendErr := c.pointDocs.Append(document)
		if appendErr != nil {
			c.pointRaw = scratch
			return dst, appendErr
		}
		dst = append(dst, stagedTxMutation{
			key: key, document: c.pointDocs.DocAt(ordinal).Src,
		})
		prospectiveDistinct = nextDistinct
		prospectiveBytes = nextBytes + len(key) + len(document)
		row++
	}
	c.pointRaw = scratch
	if err := c.routeInsertStagedWithBinding(placement, dst); err != nil {
		return dst, err
	}
	account.budget = intermediate
	account.sourceRootBytes = sourceRootBytes
	account.active = true
	return dst, nil
}

func (t *tx) runInsertSource(
	ctx context.Context,
	plan *insertSelectPlan,
	args []any,
) (query.Cursor, int64, error) {
	if plan == nil || plan.statement == nil {
		return query.Cursor{}, 0, errors.New(
			"vibedb: INSERT SELECT has no prepared transaction source plan",
		)
	}
	statement := plan.statement
	if sourceIndependentStatement(statement) {
		return statement.RunIntermediateInto(&t.conn.exec, query.Source{}, args)
	}
	requiresCatalog := statement.RequiresCatalog()
	if requiresCatalog {
		source, err := t.conn.materializeTransactionJoinSource(
			ctx, t, statement.Collection(), plan.dependencies,
		)
		if err != nil {
			return query.Cursor{}, 0, err
		}
		return statement.RunIntermediateInto(&t.conn.exec, source, args)
	}
	source, err := t.querySource(statement.Collection())
	if err != nil {
		pos := insertSourceDependencyPosition(plan, statement.Collection())
		if errors.Is(err, ErrTableNotFound) {
			return query.Cursor{}, 0, missingTableDependency(
				statement.Collection(), pos, true,
			)
		}
		return query.Cursor{}, 0, err
	}
	return statement.RunIntermediateInto(&t.conn.exec, source, args)
}

// matchingKeysTransaction evaluates a DML predicate directly over the BEGIN
// snapshot plus the bounded pending overlay. Primary-key predicates stay
// point reads; every other predicate streams the merged view once. No heap
// collection proportional to the base table is built.
func (c *conn) matchingKeysTransaction(
	ctx context.Context,
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
		cancellable := ctx.Done() != nil
		for _, key := range keys {
			if cancellable {
				if err := contextCheckpoint(ctx); err != nil {
					c.pointRaw = scratch
					return nil, err
				}
			}
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
	return t.resolveStagedMutationsContext(
		backgroundContext, state, staged,
	)
}

func (t *tx) resolveStagedMutationsContext(
	ctx context.Context,
	state *txTable,
	staged []stagedTxMutation,
) error {
	cancellable := ctx.Done() != nil
	for i := range staged {
		if cancellable {
			if err := contextCheckpoint(ctx); err != nil {
				return err
			}
		}
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
		key = state.ownMutationKey(key)
		entry = &txMutation{
			existed: existed, conflictRevision: state.statementRevision,
		}
		state.pending[key] = entry
		state.order = append(state.order, key)
	} else {
		t.recordSavepointOverwrite(state, key, entry)
		state.stagedBytes -= len(key) + len(entry.document)
	}
	entry.remove = remove
	if remove {
		entry.document = nil
	} else {
		entry.document = append(entry.document[:0], document...)
	}
	state.stagedBytes += len(key) + len(entry.document)
	if len(state.order) > state.highWaterKeys {
		state.highWaterKeys = len(state.order)
	}
	if state.stagedBytes > state.highWaterBytes {
		state.highWaterBytes = state.stagedBytes
	}
}

// ownMutationKey moves an ephemeral statement key into append-only
// transaction storage before it becomes a map key or an order entry. Chunks
// are never overwritten while the transaction is live, satisfying Go's map
// key immutability rule; geometric chunks amortize ownership to a bounded
// number of allocations rather than one allocation per inserted key.
func (s *txTable) ownMutationKey(key string) string {
	if len(key) == 0 {
		return ""
	}
	if cap(s.keyChunk)-len(s.keyChunk) < len(key) {
		if cap(s.keyChunk) != 0 {
			s.keyChunks = append(s.keyChunks, s.keyChunk)
		}
		next := cap(s.keyChunk) * 2
		if next < 4096 {
			next = 4096
		}
		if next < len(key) {
			next = len(key)
		}
		remaining := s.limits.MaxBatchBytes - s.stagedBytes
		if remaining > 0 && next > remaining {
			next = remaining
		}
		if next < len(key) {
			next = len(key)
		}
		s.keyChunk = make([]byte, 0, next)
	}
	start := len(s.keyChunk)
	s.keyChunk = append(s.keyChunk, key...)
	return byteview.String(s.keyChunk[start:len(s.keyChunk):len(s.keyChunk)])
}

type commitDirtyTable struct {
	name  string
	state *txTable
	table *table
}

func (t *tx) Commit() error {
	if t.done {
		return errors.New("vibedb: transaction is already finished")
	}
	defer t.finish()
	dirtyNames := t.dirtyTableNames()
	if len(dirtyNames) == 0 {
		return nil
	}
	t.releaseSnapshots()

	t.conn.db.mu.Lock()
	defer t.conn.db.mu.Unlock()
	if err := t.conn.db.settleCatalogLocked(); err != nil {
		return err
	}
	if t.isolation == IsolationSerializable {
		readNames := make([]string, 0, len(t.tables))
		for name, state := range t.tables {
			if state.serialRead {
				readNames = append(readNames, name)
			}
		}
		slices.Sort(readNames)
		for _, name := range readNames {
			state := t.tables[name]
			table := t.conn.db.tables[name]
			if table == nil || table != state.incarnation {
				return fmt.Errorf(
					"%w: serializable read table %q was dropped or replaced after BEGIN",
					ErrTransactionConflict, name,
				)
			}
			if state.conflicts.changedSince(state.conflictRevision) {
				return fmt.Errorf(
					"%w: serializable read table %q changed after BEGIN",
					ErrTransactionConflict, name,
				)
			}
		}
	}

	dirty := make([]commitDirtyTable, 0, len(dirtyNames))
	for _, name := range dirtyNames {
		state := t.tables[name]
		table := t.conn.db.tables[name]
		if table == nil || table != state.incarnation {
			return fmt.Errorf(
				"%w: table %q was dropped or replaced after BEGIN",
				ErrTransactionConflict, name,
			)
		}
		key, historyOverflow, conflict := t.writeConflict(state)
		if conflict {
			if historyOverflow {
				return fmt.Errorf(
					"%w: table %q exceeded its bounded conflict history after BEGIN",
					ErrTransactionConflict, name,
				)
			}
			return fmt.Errorf(
				"%w: table %q key %q changed after BEGIN",
				ErrTransactionConflict, name, key,
			)
		}
		dirty = append(dirty, commitDirtyTable{name: name, state: state, table: table})
	}

	// Materialize every absent dirty table as an EMPTY durable file first. The
	// staged seeds join the transaction as ordinary batch entries. An aborted
	// or crashed transaction can leave that empty table behind — documented
	// catalog residue.
	for i := range dirty {
		if dirty[i].table.collection != nil {
			continue
		}
		if _, err := t.conn.db.materializeLocked(dirty[i].name, nil); err != nil {
			return transactionBatchError(err)
		}
		dirty[i].table = t.conn.db.tables[dirty[i].name]
		if dirty[i].table == nil || dirty[i].table.collection == nil {
			return fmt.Errorf(
				"vibedb: materialize table %q left no durable collection",
				dirty[i].name,
			)
		}
	}

	for i := range dirty {
		if err := t.validateWriteSet(
			dirty[i].table.collection, dirty[i].name, dirty[i].state,
		); err != nil {
			return err
		}
	}

	if len(dirty) == 1 {
		return t.commitOneTable(dirty[0].name, dirty[0].table, dirty[0].state)
	}
	return t.commitManyTables(dirty)
}

func (t *tx) writeConflict(
	state *txTable,
) (key string, historyOverflow, conflict bool) {
	if t.isolation != IsolationReadCommitted {
		return state.conflicts.conflict(state.conflictRevision, state.order)
	}
	var keys [1]string
	for _, pendingKey := range state.order {
		entry := state.pending[pendingKey]
		if entry == nil {
			continue
		}
		keys[0] = pendingKey
		key, historyOverflow, conflict = state.conflicts.conflict(
			entry.conflictRevision, keys[:],
		)
		if conflict {
			return key, historyOverflow, true
		}
	}
	return "", false, false
}

func (t *tx) dirtyTableNames() []string {
	names := make([]string, 0, len(t.tables))
	for name, state := range t.tables {
		if stateHasDirtyOverlay(state) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

func stateHasDirtyOverlay(state *txTable) bool {
	if state == nil || len(state.order) == 0 {
		return false
	}
	for _, key := range state.order {
		entry := state.pending[key]
		if entry == nil {
			continue
		}
		// INSERT+DELETE of a key absent at BEGIN is a net no-op and must not
		// force a durable participant.
		if !entry.existed && entry.remove {
			continue
		}
		return true
	}
	return false
}

func (t *tx) commitOneTable(name string, table *table, state *txTable) error {
	collection := table.collection
	beforeGeneration := collection.Generation()
	err := collection.Update(func(batch *durable.WriteBatch) error {
		return fillWriteBatchFromTxTable(batch, state)
	})
	if collectionMutationPublished(collection, beforeGeneration, err) {
		table.conflicts.recordTransaction(state)
	}
	return transactionBatchError(err)
}

func (t *tx) commitManyTables(dirty []commitDirtyTable) error {
	if t.conn.db.txnLog == nil {
		return errors.New("vibedb: database transaction log is not open")
	}
	members := make([]durable.NamedCollection, 0, len(t.conn.db.tables))
	for name, table := range t.conn.db.tables {
		if table.collection == nil {
			continue
		}
		members = append(members, durable.NamedCollection{
			Name: name, Collection: table.collection,
		})
	}
	slices.SortFunc(members, func(a, b durable.NamedCollection) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	})
	byName := make(map[string]*txTable, len(dirty))
	for i := range dirty {
		byName[dirty[i].name] = dirty[i].state
	}
	before := make(map[string]uint64, len(dirty))
	for i := range dirty {
		before[dirty[i].name] = dirty[i].table.collection.Generation()
	}
	err := durable.UpdateCollections(
		t.conn.db.txnLog, members, t.conn.db.txnLimits,
		func(batch *durable.DatabaseBatch) error {
			for name, state := range byName {
				wb, batchErr := batch.Collection(name)
				if batchErr != nil {
					return batchErr
				}
				if fillErr := fillWriteBatchFromTxTable(wb, state); fillErr != nil {
					return fillErr
				}
			}
			return nil
		},
	)
	for i := range dirty {
		name := dirty[i].name
		collection := dirty[i].table.collection
		if collectionMutationPublished(collection, before[name], err) {
			dirty[i].table.conflicts.recordTransaction(dirty[i].state)
		}
	}
	return transactionBatchError(err)
}

func fillWriteBatchFromTxTable(batch *durable.WriteBatch, state *txTable) error {
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
}

// validateWriteSet enforces optimistic-concurrency: every key the transaction
// touched must still hold the value it held at BEGIN, otherwise a concurrent
// commit changed it and this transaction must abort. It reads a fresh snapshot
// of the currently committed state; the caller runs under db.mu, so that state
// is stable for the duration of the check and the subsequent apply.
func (t *tx) validateWriteSet(
	collection *durable.Collection, tableName string, state *txTable,
) error {
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
				ErrTransactionConflict, tableName, key)
		}
	}
	return nil
}

func transactionBatchError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, durable.ErrBatchTooLarge) ||
		errors.Is(err, durable.ErrTxnTooLarge) ||
		errors.Is(err, durable.ErrTxnLimitsRequired) {
		return fmt.Errorf("%w: %w", ErrTransactionTooLarge, err)
	}
	if errors.Is(err, durable.ErrPrimaryBatchUnsupportedLane) ||
		errors.Is(err, durable.ErrDatabaseTransactionUnsupportedLane) {
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
	t.views = nil
	t.savepoints = nil
}

func (t *tx) releaseSnapshots() {
	for _, table := range t.tables {
		if table.snapshot != nil {
			_ = table.snapshot.Close()
			table.snapshot = nil
		}
		if table.refreshSnapshot != nil {
			_ = table.refreshSnapshot.Close()
			table.refreshSnapshot = nil
		}
		table.refreshCaptured = false
	}
}
