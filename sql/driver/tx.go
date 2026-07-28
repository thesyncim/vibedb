package driver

import (
	"bytes"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

type txMutation struct {
	document []byte
	before   []byte
	remove   bool
	existed  bool
}

type txTable struct {
	snapshot *durable.Snapshot
	pending  map[string]*txMutation
	order    []string
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

var _ sqldriver.Tx = (*tx)(nil)

func (c *conn) beginTx(options sqldriver.TxOptions) (*tx, error) {
	switch options.Isolation {
	case 0, 5: // database/sql LevelDefault and LevelSnapshot.
	default:
		return nil, fmt.Errorf(
			"vibedb: isolation level %d is unsupported; transactions provide snapshot isolation",
			options.Isolation)
	}
	transaction := &tx{
		conn: c, tables: make(map[string]*txTable), readOnly: options.ReadOnly,
	}
	c.db.mu.RLock()
	defer c.db.mu.RUnlock()
	if c.db.closed {
		return nil, sqldriver.ErrBadConn
	}
	for name, table := range c.db.tables {
		state := &txTable{pending: make(map[string]*txMutation)}
		if table.collection != nil {
			snapshot, err := table.collection.Snapshot()
			if err != nil {
				transaction.releaseSnapshots()
				return nil, err
			}
			state.snapshot = snapshot
		}
		transaction.tables[name] = state
	}
	return transaction, nil
}

func (t *tx) querySource(tableName string) (query.Source, error) {
	if t.done {
		return query.Source{}, errors.New("vibedb: transaction is finished")
	}
	view, err := t.view(tableName)
	if err != nil {
		return query.Source{}, err
	}
	return query.FromSnapshot(view), nil
}

// view materializes the BEGIN snapshot with the transaction's bounded overlay.
// It intentionally makes one full pass so every SELECT and mutation sees exact
// repeatable-read and read-your-writes semantics.
func (t *tx) view(tableName string) (store.Snapshot, error) {
	state, ok := t.tables[tableName]
	if !ok {
		return store.Snapshot{}, fmt.Errorf(
			"vibedb: table %q was not present when the transaction began", tableName)
	}
	heap, err := store.New(store.Options{})
	if err != nil {
		return store.Snapshot{}, err
	}
	if state.snapshot != nil {
		var rangeErr error
		state.snapshot.RangeRaw(func(key, document []byte) error {
			if _, shadowed := state.pending[string(key)]; shadowed {
				return nil
			}
			_, rangeErr = heap.Put(string(key), document)
			return rangeErr
		})
		if rangeErr != nil {
			return store.Snapshot{}, rangeErr
		}
	}
	for _, key := range state.order {
		mutation := state.pending[key]
		if mutation.remove {
			continue
		}
		if _, err := heap.Put(key, mutation.document); err != nil {
			return store.Snapshot{}, err
		}
	}
	return heap.Snapshot()
}

func (t *tx) execMutation(statement *query.DMLStatement, args []any) (sqldriver.Result, error) {
	if t.done {
		return nil, errors.New("vibedb: transaction is finished")
	}
	if t.readOnly {
		return nil, errors.New("vibedb: mutation attempted in a read-only transaction")
	}
	switch statement.Kind() {
	case query.DDLCreateTable, query.DDLCreateIndex:
		return nil, errors.New("vibedb: schema definitions cannot run inside a transaction")
	}
	tableName := statement.Collection()
	t.conn.db.mu.RLock()
	table, exists := t.conn.db.tables[tableName]
	t.conn.db.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("vibedb: table %q does not exist", tableName)
	}
	if len(table.meta.Indexes) != 0 {
		return nil, fmt.Errorf("%w: table %q: %w",
			ErrTransactionIndexedTable, tableName, durable.ErrPrimaryBatchIndexedUnsupported)
	}
	if t.writeTable != "" && t.writeTable != tableName {
		return nil, fmt.Errorf(
			"vibedb: a transaction writes exactly one table; it already writes %q and cannot also write %q",
			t.writeTable, tableName)
	}
	state, ok := t.tables[tableName]
	if !ok {
		return nil, fmt.Errorf(
			"vibedb: table %q was not present when the transaction began", tableName)
	}

	type stagedMutation struct {
		key      string
		document []byte
		remove   bool
	}
	var staged []stagedMutation
	switch statement.Kind() {
	case query.DMLInsert:
		tree := statement.Tree().Insert
		for i := range tree.Rows {
			document, key, err := resolveInsertRow(
				statement, tree, &tree.Rows[i], args, table.meta.PrimaryKey)
			if err != nil {
				return nil, err
			}
			staged = append(staged, stagedMutation{key: key, document: document})
		}
	case query.DMLUpdate:
		document, err := statement.Document(statement.Tree().Update.Doc, args)
		if err != nil {
			return nil, err
		}
		view, err := t.view(tableName)
		if err != nil {
			return nil, err
		}
		keys, err := t.conn.matchingKeysSnapshot(statement, args, table, view)
		if err != nil {
			return nil, err
		}
		newKey, err := documentKey(document, table.meta.PrimaryKey)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if key != newKey {
				return nil, errors.New("vibedb: UPDATE cannot change a document's primary key")
			}
			staged = append(staged, stagedMutation{key: key, document: document})
		}
	case query.DMLDelete:
		view, err := t.view(tableName)
		if err != nil {
			return nil, err
		}
		keys, err := t.conn.matchingKeysSnapshot(statement, args, table, view)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			staged = append(staged, stagedMutation{key: key, remove: true})
		}
	default:
		return nil, fmt.Errorf("vibedb: unsupported transaction statement %s", statement.Kind())
	}

	limit := store.MaxChunkDocuments
	if table.collection != nil {
		limit = table.collection.MaxBatchDocuments()
	}
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
	for _, mutation := range staged {
		if err := t.stage(state, mutation.key, mutation.document, mutation.remove); err != nil {
			return nil, err
		}
	}
	if len(staged) != 0 {
		t.writeTable = tableName
	}
	return result{affected: int64(len(staged))}, nil
}

func (t *tx) stage(state *txTable, key string, document []byte, remove bool) error {
	entry, exists := state.pending[key]
	if !exists {
		entry = &txMutation{}
		if state.snapshot != nil {
			before, found, err := state.snapshot.AppendRaw(nil, key)
			if err != nil {
				return err
			}
			entry.before, entry.existed = before, found
		}
		state.pending[key] = entry
		state.order = append(state.order, key)
	}
	entry.remove = remove
	if remove {
		entry.document = nil
	} else {
		entry.document = append(entry.document[:0], document...)
	}
	return nil
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
	table := t.conn.db.tables[t.writeTable]
	if table == nil {
		return fmt.Errorf("vibedb: table %q no longer exists", t.writeTable)
	}
	if len(table.meta.Indexes) != 0 {
		return fmt.Errorf("%w: table %q: %w",
			ErrTransactionIndexedTable, t.writeTable, durable.ErrPrimaryBatchIndexedUnsupported)
	}
	if table.collection == nil {
		var err error
		table, err = t.conn.db.materializeLocked(t.writeTable, nil)
		if err != nil {
			return err
		}
	}
	collection := table.collection
	err := collection.Update(func(batch *durable.WriteBatch) error {
		live, err := collection.Snapshot()
		if err != nil {
			return err
		}
		defer live.Close()
		var scratch []byte
		for _, key := range state.order {
			entry := state.pending[key]
			current, found, err := live.AppendRaw(scratch[:0], key)
			if err != nil {
				return err
			}
			scratch = current
			if found != entry.existed || (found && !bytes.Equal(current, entry.before)) {
				return fmt.Errorf(
					"%w: table %q key %q changed after BEGIN",
					ErrTransactionConflict, t.writeTable, key)
			}
		}
		for _, key := range state.order {
			entry := state.pending[key]
			if entry.remove {
				if err := batch.Delete(key); err != nil {
					return transactionBatchError(err)
				}
			} else if err := batch.Put(key, entry.document); err != nil {
				return transactionBatchError(err)
			}
		}
		return nil
	})
	return transactionBatchError(err)
}

func transactionBatchError(err error) error {
	if errors.Is(err, durable.ErrBatchTooLarge) {
		return fmt.Errorf("%w: %w", ErrTransactionTooLarge, err)
	}
	if errors.Is(err, durable.ErrPrimaryBatchIndexedUnsupported) {
		return fmt.Errorf("%w: %w", ErrTransactionIndexedTable, err)
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
		t.conn.tx = nil
	}
}

func (t *tx) releaseSnapshots() {
	for _, table := range t.tables {
		if table.snapshot != nil {
			_ = table.snapshot.Close()
			table.snapshot = nil
		}
	}
}
