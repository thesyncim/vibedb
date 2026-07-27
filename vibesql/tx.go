package vibesql

import (
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
)

// Transactions.
//
// # What a transaction is here, exactly
//
// A transaction is one [durable.WriteBatch]: the set of writes one Update
// publishes as one generation. That is the largest atomic unit this engine
// has, and saying so is the whole design. Everything below follows from it.
//
// A transaction therefore:
//
//   - writes exactly one collection. Each durable collection has its own root,
//     its own generation counter, and its own writer lock; there is no operation
//     anywhere in the engine that publishes two collections together. A
//     statement naming a second collection is refused by name rather than
//     written non-atomically.
//   - writes at most Options.MaxBatchDocuments documents (64 by default). The
//     batch is a fixed page reservation, and it is refused whole rather than
//     split, because a batch spanning two commits is not the atomic unit the
//     caller asked for. The limit is checked as statements execute, so the
//     statement that exceeds it is the statement that fails.
//   - requires a durable collection. A heap store.Database has Put and Delete
//     and nothing that publishes several together, so a transaction over one
//     could only be a promise this package could not keep. BeginTx refuses it.
//
// # The isolation guarantee, stated precisely
//
// Against readers: snapshot isolation, and it is the store's, not this
// package's. Begin takes one durable.Snapshot and every SELECT and mutation
// decides against that same generation, overlaid with the transaction's staged
// writes. The snapshot holds a generation lease; the writer's copy-on-write
// publication installs a new state root and cannot reuse the extents that
// lease pins. A concurrent update or insert is therefore invisible for the
// transaction's whole lifetime — repeatable reads and phantom exclusion are
// properties of one retained root, not a best-effort sequence of fresh reads.
// A commit is never partially visible to anyone, because visibility changes
// with a single atomic store of the state pointer.
//
// Against writers: a transaction is snapshot-isolated with first-committer-wins
// conflict detection, which is exactly what its structure can support and no
// more. Its statements execute when the application calls them, against the
// snapshot Begin retained; the writes are held back until Commit. Between the
// two, another writer may publish. So Commit re-reads, under the writer lock,
// every key the transaction wrote, compares it byte for byte against the
// begin-snapshot pre-image, and aborts the whole transaction if any of them
// moved. A transaction that commits therefore wrote from a state that was
// still true when it wrote — which is what a lost update is the absence of.
//
// What that does not give is serializability. Snapshot isolation permits write
// skew and has no predicate conflict detection: a transaction whose WHERE
// matched three begin-snapshot documents continues to see those three, but it
// can commit alongside another that inserted a fourth match because neither
// wrote what the other read. Nothing here claims otherwise, and the alternative
// — holding the collection's writer lock from Begin to Commit — would make an
// application's think time into a global write stall and a forgotten Rollback
// into a deadlock for every other writer in the process.
//
// An autocommit statement is stronger than a transaction of one statement, and
// that is worth knowing rather than surprising. An autocommit statement does
// its whole scan inside the Update callback, under the writer lock, so it is
// serializable against other writers and cannot conflict at all. See write.go.

// ErrConflict reports a transaction that could not commit because a document it
// read was changed by another writer after it read it. Nothing was written; the
// transaction is rolled back and the whole of it may be retried.
var ErrConflict = errors.New("vibesql: transaction conflict")

// A txMutation is one pending write, plus the pre-image the transaction saw.
type txMutation struct {
	doc []byte
	// before is the document the transaction observed under this key, and
	// existed whether it observed one at all. Commit checks both against the
	// live state; see conflict detection above.
	before  []byte
	remove  bool
	existed bool
}

// A tx is one open transaction.
type tx struct {
	conn       *conn
	collection *durable.Collection
	name       string
	// snapshot is the one generation this transaction reads from. It is taken
	// by BeginTx, not lazily by the first statement, so an otherwise idle
	// transaction cannot silently advance past a concurrent commit.
	snapshot *durable.Snapshot
	overlay  query.FileOverlaySource

	// pending holds one entry per key the transaction writes, and order the
	// keys in the order they were first written. The order is kept because a
	// batch deduplicates by key and a map does not iterate deterministically:
	// two runs of the same transaction should stage the same batch, so a
	// failure is reproducible.
	pending map[string]*txMutation
	order   []string
	// delta is the visible row-count difference between pending and snapshot.
	// Maintaining it as writes stage lets the query executor report RowsTotal
	// without walking even the bounded map on every SELECT.
	delta int64

	// limit is the collection's MaxBatchDocuments, read once at Begin. A
	// statement that would push the transaction past it fails where it was
	// written rather than at Commit with a batch to discard.
	limit int

	done bool
}

var _ driver.Tx = (*tx)(nil)

// Begin opens a transaction with the default options.
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx opens a transaction over this connection's collection.
//
// The options are checked rather than ignored. database/sql lets a caller ask
// for an isolation level and for a read-only transaction, and a driver that
// accepted a level it does not implement would be answering the one question
// whose wrong answer is invisible until the data is wrong. Only
// sql.LevelDefault and sql.LevelSnapshot are accepted, because snapshot
// isolation is exactly what this provides. A caller asking for Serializable is
// told so rather than quietly given something weaker, which is the difference
// between a transaction that fails and one that silently permits write skew.
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if err := c.usable(ctx); err != nil {
		return nil, err
	}
	if c.tx != nil {
		return nil, errors.New("vibesql: this connection already has an open transaction")
	}
	if err := checkIsolation(opts); err != nil {
		return nil, err
	}
	if c.src.collection == nil {
		return nil, errors.New(
			"vibesql: this connection reads an in-process store.Database, which has no operation that " +
				"publishes several documents together, so a transaction over it could only promise an " +
				"atomicity it does not have. Transactions require a durable collection — a file DSN, or " +
				"one registered with AttachCollection")
	}
	snapshot, err := c.src.collection.Snapshot()
	if err != nil {
		return nil, err
	}
	t := &tx{
		conn:       c,
		collection: c.src.collection,
		name:       c.src.name,
		snapshot:   snapshot,
		pending:    make(map[string]*txMutation),
		limit:      c.src.collection.MaxBatchDocuments(),
	}
	t.overlay = query.NewFileOverlaySource(t)
	c.tx = t
	return t, nil
}

// checkIsolation refuses the levels this driver does not implement.
func checkIsolation(opts driver.TxOptions) error {
	// The two accepted values are spelled numerically rather than through
	// database/sql's constants, because importing database/sql from a driver
	// implementation for two integers would invert the dependency the
	// driver/driver.Value split exists to keep. sql.LevelDefault is 0 and
	// sql.LevelSnapshot is 5; both are part of database/sql's documented API
	// and cannot change without breaking every driver that switches on them.
	switch opts.Isolation {
	case 0, 5:
	default:
		return fmt.Errorf(
			"vibesql: isolation level %d is not supported: reads take a generation-leased snapshot and "+
				"writes are checked for conflict at commit, which is snapshot isolation and is the only "+
				"level this engine implements", opts.Isolation)
	}
	return nil
}

// exec runs one statement inside the transaction, staging its writes.
//
// The statement executes now, against the snapshot Begin retained and overlaid
// with the transaction's own pending writes, so a transaction reads what it
// has already written without ever admitting a later committed generation. It
// has to execute now: RowsAffected is returned to the application immediately,
// and a driver that deferred the work would have to either guess the count or
// return one it later discovered to be wrong.
func (t *tx) exec(d *query.DMLStatement, args []any, target writeTarget) (result, error) {
	if t.done {
		return result{}, errors.New("vibesql: the transaction is finished")
	}
	// A transaction spans one collection, and today that is enforced one layer
	// down rather than here: a connection resolves to exactly one source, a
	// durable source is exactly one collection, and BeginTx refuses the only
	// multi-collection source there is. So this check cannot fire through the
	// driver as it stands. It is kept because the reason is a property of the
	// engine and not of the connection — each collection has its own root,
	// generation counter, and writer lock, and nothing publishes two of them
	// together — so the day a source holds several, this is the check that has
	// to be here and the message that has to be given.
	if target.file == nil || target.name != t.name {
		return result{}, fmt.Errorf(
			"vibesql: this transaction writes %q and the statement names %q: each collection has its own "+
				"root, generation, and writer lock, and this engine has no operation that publishes two of "+
				"them together, so a transaction spans exactly one collection",
			t.name, target.name)
	}
	mutations, err := t.conn.plan(d, args, readSource{file: t.snapshot, overlay: t})
	if err != nil {
		return result{}, err
	}
	// The batch bound is checked against the transaction's whole staged set,
	// before any of this statement's writes are staged, so a statement that
	// would overflow the batch leaves the transaction exactly as it was and can
	// be retried after a commit.
	staged := len(t.order)
	for _, m := range mutations {
		if _, exists := t.pending[m.key]; !exists {
			staged++
		}
	}
	if staged > t.limit {
		return result{}, fmt.Errorf(
			"vibesql: this statement would bring the transaction to %d documents and the collection "+
				"publishes at most %d in one atomic batch: nothing was staged, and the transaction is "+
				"still usable. Commit and start another, or open the collection with a larger "+
				"Options.MaxBatchDocuments: %w", staged, t.limit, durable.ErrBatchTooLarge)
	}
	for _, m := range mutations {
		if err := t.stage(m); err != nil {
			return result{}, err
		}
	}
	return result{affected: int64(len(mutations))}, nil
}

// stage records one write and, the first time a key is touched, the pre-image
// the transaction observed for it.
//
// The pre-image is recorded once and never updated, which is the point: it is
// what this transaction believed about the key when it decided to write it, and
// the commit check asks whether that belief is still true. Overwriting it with
// the transaction's own later view would make the check compare the world
// against itself.
func (t *tx) stage(m mutation) error {
	entry, exists := t.pending[m.key]
	oldDelta := txMutationDelta(entry)
	if !exists {
		entry = &txMutation{}
		before, found, err := t.snapshot.AppendRaw(nil, m.key)
		if err != nil {
			return err
		}
		entry.before, entry.existed = before, found
		t.pending[m.key] = entry
		t.order = append(t.order, m.key)
	}
	entry.remove = m.remove
	if m.remove {
		entry.doc = nil
		t.delta += txMutationDelta(entry) - oldDelta
		return nil
	}
	// The document is cloned because the caller's []byte argument belongs to
	// database/sql and may be reused as soon as Exec returns, and the commit is
	// arbitrarily later.
	entry.doc = append(entry.doc[:0], m.doc...)
	t.delta += txMutationDelta(entry) - oldDelta
	return nil
}

func txMutationDelta(entry *txMutation) int64 {
	if entry == nil {
		return 0
	}
	switch {
	case entry.existed && entry.remove:
		return -1
	case !entry.existed && !entry.remove:
		return 1
	default:
		return 0
	}
}

// lookup answers what the transaction believes is stored under key, so its own
// statements read its own writes. found is false for a key the transaction has
// deleted.
func (t *tx) lookup(key string) (doc []byte, found, known bool) {
	entry, ok := t.pending[key]
	if !ok {
		return nil, false, false
	}
	if entry.remove {
		return nil, false, true
	}
	return entry.doc, true, true
}

// each visits every key the transaction has staged a document for, so a scan
// can include documents that do not exist in any snapshot yet.
func (t *tx) each(visit func(key string, doc []byte) error) error {
	for _, key := range t.order {
		entry := t.pending[key]
		if entry.remove || entry.existed {
			// A staged replacement of a key that already exists is delivered by
			// the scan, which finds the snapshot's row and substitutes this
			// document for it. Delivering it here as well would show the row
			// twice.
			continue
		}
		if err := visit(key, entry.doc); err != nil {
			return err
		}
	}
	return nil
}

// resolve returns the one source a SELECT inside this transaction must use.
// With no staged writes the durable executor reads the retained snapshot
// directly. Once an overlay exists, FromFileOverlay merges the same base with
// at most the bounded pending set while it scans.
func (t *tx) resolve(collection string, joins bool) (handle, error) {
	if t.done {
		return handle{}, errors.New("vibesql: the transaction is finished")
	}
	if collection != t.name {
		return handle{}, fmt.Errorf(
			"vibesql: this transaction reads the collection %q, and the statement names %q",
			t.name, collection)
	}
	if joins {
		return handle{}, errors.New(
			"vibesql: a JOIN reads two collections from one consistent snapshot, and this " +
				"transaction holds a single durable collection")
	}
	if len(t.pending) == 0 {
		// Deliberately leave handle.file nil: the transaction, not these rows,
		// owns the retained snapshot and releases it at Commit or Rollback.
		return handle{src: query.FromFile(t.snapshot)}, nil
	}
	return handle{src: query.FromFileOverlay(t.snapshot, &t.overlay)}, nil
}

// Lookup implements [query.FileOverlay]. The []byte-to-string conversion in a
// map lookup is allocation-free, and only the overlay source invokes it.
func (t *tx) Lookup(key []byte) (value []byte, present, shadowed bool) {
	entry, ok := t.pending[string(key)]
	if !ok {
		return nil, false, false
	}
	if entry.remove {
		return nil, false, true
	}
	return entry.doc, true, true
}

// RangeInserts implements [query.FileOverlay]. Replacements are emitted in
// their base row's position by Lookup; this tail contains only genuinely new
// visible rows.
func (t *tx) RangeInserts(visit func(value []byte) error) error {
	for _, key := range t.order {
		entry := t.pending[key]
		if entry.existed || entry.remove {
			continue
		}
		if err := visit(entry.doc); err != nil {
			return err
		}
	}
	return nil
}

// RangePresent implements [query.FileOverlay]. Candidate-bounded execution
// suppresses shadowed base rows and exact-evaluates this bounded set once.
func (t *tx) RangePresent(visit func(value []byte) error) error {
	for _, key := range t.order {
		entry := t.pending[key]
		if entry.remove {
			continue
		}
		if err := visit(entry.doc); err != nil {
			return err
		}
	}
	return nil
}

// LenDelta implements [query.FileOverlay].
func (t *tx) LenDelta() int64 { return t.delta }

// Commit publishes the transaction's writes as one generation.
//
// The conflict check runs inside the Update callback, under the writer lock, so
// nothing can change between the check and the publication. It compares the
// exact stored bytes rather than a version counter because the store has no
// per-document version to compare, and because exact bytes are the strongest
// available answer: a document rewritten to the identical spelling is not a
// conflict, which is right, and any real change is one.
func (t *tx) Commit() error {
	if t.done {
		return errors.New("vibesql: the transaction is already finished")
	}
	defer t.finish()
	if len(t.order) == 0 {
		return nil
	}
	// Every written key's exact begin-snapshot pre-image is owned by pending,
	// so the lease has finished its job before the writer lock is entered.
	// Releasing it here both bounds lease lifetime exactly to the transaction's
	// read phase and leaves one-lease configurations able to take the live
	// conflict-check snapshot below.
	t.releaseReadView()
	return t.collection.Update(func(batch *durable.WriteBatch) error {
		snapshot, err := t.collection.Snapshot()
		if err != nil {
			return err
		}
		defer func() { _ = snapshot.Close() }()
		var scratch []byte
		for _, key := range t.order {
			entry := t.pending[key]
			current, found, err := snapshot.AppendRaw(scratch[:0], key)
			if err != nil {
				return err
			}
			scratch = current
			if found != entry.existed || (found && !bytes.Equal(current, entry.before)) {
				return fmt.Errorf(
					"%w: the document %q changed after this transaction read it, so committing would "+
						"overwrite a write this transaction never saw. Nothing was written; retry the "+
						"transaction", ErrConflict, key)
			}
		}
		for _, key := range t.order {
			entry := t.pending[key]
			if entry.remove {
				if err := batch.Delete(key); err != nil {
					return batchError(err, len(t.order), t.limit)
				}
				continue
			}
			if err := batch.Put(key, entry.doc); err != nil {
				return batchError(err, len(t.order), t.limit)
			}
		}
		return nil
	})
}

// Rollback discards the transaction's staged writes. Nothing was published, so
// there is nothing to undo.
func (t *tx) Rollback() error {
	if t.done {
		return nil
	}
	t.finish()
	return nil
}

func (t *tx) finish() {
	t.done = true
	t.releaseReadView()
	t.overlay.Bind(nil)
	if t.conn != nil {
		t.conn.tx = nil
	}
}

func (t *tx) releaseReadView() {
	if t.snapshot != nil {
		_ = t.snapshot.Close()
		t.snapshot = nil
	}
}
