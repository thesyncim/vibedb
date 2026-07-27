package vibesql

import (
	"database/sql/driver"
	"errors"
	"fmt"

	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// The write path: turning a prepared mutation into a set of keyed document
// writes, and applying it.
//
// # Two writers, one of which is atomic
//
// A connection writes to one of two things, and the difference is not
// cosmetic. A durable collection has [durable.Collection.Update], which
// publishes a whole batch as one generation — one document-page rebuild per
// touched chunk, one copy-on-write descent per directory, one state root, one
// fence — under a single writer lock. A heap collection has Put and Delete and
// nothing between them: each is atomic by itself, and a statement touching
// three documents is three publications.
//
// So a statement against a durable collection is failure-atomic and a statement
// against a heap collection is not. That difference is stated everywhere it
// shows — here, in [Conn.BeginTx]'s refusal for a heap source, and in the
// package documentation — rather than smoothed over, because smoothing it over
// would mean offering the same syntax with two different guarantees and naming
// only the stronger one.
//
// What is done about it, rather than merely said: every document a statement
// will write is validated before the first one is written. A heap statement can
// therefore still fail partway on an I/O-shaped error the heap store does not
// have, and cannot fail partway on the error a caller actually produces, which
// is a malformed or schema-violating document.
//
// # Read, then write, inside one lock
//
// A filtered UPDATE or DELETE has to decide which documents match before it can
// write, and the decision has to be made against the state the write applies
// to. For the durable backend both happen inside one Update: the callback takes
// its own snapshot, which loads exactly the state the batch will be applied to,
// because the writer lock Update holds is the lock every other writer needs.
// There is no window between the scan and the write for another writer to move
// a document out from under the statement, so an autocommit statement is
// serializable against every other writer with no conflict detection at all.
//
// An explicit transaction cannot do that — its statements execute when the
// application calls Exec, and the commit happens later — so it takes the other
// road: snapshot isolation with commit-time conflict detection. See tx.go.

// errNoLastInsertId is what LastInsertId answers.
//
// Returning an error rather than zero is the whole point. A document's key is
// an opaque string the caller chose; there is no sequence, no autoincrement,
// and nothing an int64 could stand for. A driver that answered 0 would be
// answering a question it did not understand, and a caller that recorded the
// answer would have recorded a number that means nothing about their data.
var errNoLastInsertId = errors.New(
	"vibesql: LastInsertId has no meaning here: a document's primary key is a caller-supplied " +
		"string, not a generated integer, so there is no id to report")

// A result reports how many documents a statement wrote.
type result struct{ affected int64 }

var _ driver.Result = result{}

// LastInsertId always fails; see [errNoLastInsertId].
func (r result) LastInsertId() (int64, error) { return 0, errNoLastInsertId }

// RowsAffected reports the number of documents the statement created,
// replaced, or removed.
//
// It is the count of documents whose stored state changed, which is the only
// count that is true of both statements and both backends. A DELETE naming
// three keys of which one is absent reports two; an INSERT reports one per row
// because an INSERT onto an existing key is refused rather than counted; an
// UPDATE reports the documents it replaced, and a matched document is always
// replaced because the replacement is unconditional.
func (r result) RowsAffected() (int64, error) { return r.affected, nil }

// A mutation is one pending keyed write.
type mutation struct {
	key    string
	doc    []byte
	remove bool
}

// A writeTarget is the collection a statement writes to, in whichever of the
// two forms this connection's source holds.
//
// It is a struct with two nilable fields rather than an interface because the
// two backends do not have one shape: the durable side's atomicity lives in a
// callback that has no heap counterpart, and an interface would have to either
// omit it — losing the guarantee — or invent a heap batch that publishes
// document by document while claiming otherwise.
type writeTarget struct {
	file *durable.Collection
	heap *store.Collection
	name string
}

// target resolves the collection a mutation names, refusing a statement whose
// collection this connection does not hold.
func (s *source) target(collection string) (writeTarget, error) {
	if s.db != nil {
		c, ok := s.db.Collection(collection)
		if !ok {
			return writeTarget{}, fmt.Errorf(
				"vibesql: this database has no collection %q; create it with CREATE TABLE", collection)
		}
		return writeTarget{heap: c, name: collection}, nil
	}
	if collection != s.name {
		return writeTarget{}, fmt.Errorf(
			"vibesql: this connection writes the collection %q, and the statement names %q",
			s.name, collection)
	}
	return writeTarget{file: s.collection, name: collection}, nil
}

// execWrite runs one prepared mutation to completion and reports what it wrote.
func (c *conn) execWrite(d *query.DMLStatement, args []any) (result, error) {
	target, err := c.src.target(d.Collection())
	if err != nil {
		return result{}, err
	}
	if tx := c.tx; tx != nil {
		return tx.exec(d, args, target)
	}
	if target.file != nil {
		return c.execDurable(d, args, target.file)
	}
	return c.execHeap(d, args, target.heap)
}

// execDurable applies one statement as a single durable generation.
//
// Everything — the scan that decides which documents match, the reads a keyed
// statement performs, and the writes themselves — happens inside the Update
// callback, which runs under the collection's writer lock. That is what makes
// the statement serializable against other writers without any conflict
// detection: no other writer can publish a generation between the decision and
// the write, because there is no moment between them at which the lock is free.
//
// Taking a snapshot inside the callback is safe and is the point. Snapshot
// takes the collection's snapshot gate, which Update does not hold while the
// callback runs, and it loads exactly the state Update will apply the batch to,
// because that state is loaded after the callback returns and under the same
// writer lock.
func (c *conn) execDurable(d *query.DMLStatement, args []any, collection *durable.Collection) (result, error) {
	var out result
	err := collection.Update(func(batch *durable.WriteBatch) error {
		snapshot, err := collection.Snapshot()
		if err != nil {
			return err
		}
		defer func() { _ = snapshot.Close() }()
		mutations, err := c.plan(d, args, readSource{file: snapshot})
		if err != nil {
			return err
		}
		for _, m := range mutations {
			if m.remove {
				if err := batch.Delete(m.key); err != nil {
					return batchError(err, len(mutations), collection.MaxBatchDocuments())
				}
				continue
			}
			if err := batch.Put(m.key, m.doc); err != nil {
				return batchError(err, len(mutations), collection.MaxBatchDocuments())
			}
		}
		out.affected = int64(len(mutations))
		return nil
	})
	if err != nil {
		return result{}, err
	}
	return out, nil
}

// batchError turns the batch bound into a message naming both numbers.
//
// ErrBatchTooLarge is the honest answer and the collection's own; what it
// cannot say is how many documents the statement wanted, which is the number
// that tells a caller whether to narrow the WHERE or to raise the option.
func batchError(err error, wanted, limit int) error {
	if !errors.Is(err, durable.ErrBatchTooLarge) {
		return err
	}
	return fmt.Errorf(
		"vibesql: this statement writes %d documents and the collection publishes at most %d in one "+
			"atomic batch: nothing was written. Narrow the condition, or open the collection with a "+
			"larger Options.MaxBatchDocuments; the statement is not split, because half of a statement "+
			"is not a statement: %w", wanted, limit, err)
}

// execHeap applies one statement to a heap collection, document by document.
//
// Every document is validated before anything is written — see the file
// comment — so the loop below fails only where the heap store itself can fail,
// which after validation is nowhere. It is still not one publication, and the
// package documentation says so.
func (c *conn) execHeap(d *query.DMLStatement, args []any, collection *store.Collection) (result, error) {
	snapshot, err := collection.Snapshot()
	if err != nil {
		return result{}, err
	}
	mutations, err := c.plan(d, args, readSource{heap: snapshot})
	if err != nil {
		return result{}, err
	}
	written := int64(0)
	for _, m := range mutations {
		if m.remove {
			if _, err := collection.Delete(m.key); err != nil {
				return result{affected: written}, err
			}
			written++
			continue
		}
		if _, err := collection.Put(m.key, m.doc); err != nil {
			return result{affected: written}, err
		}
		written++
	}
	return result{affected: written}, nil
}

// A readSource is the state a statement decides from: the snapshot it scans
// and the snapshot it reads individual keys out of, which are always the same
// snapshot.
//
// It is one type over both backends rather than an interface because the two
// differ in exactly two method signatures and in nothing else a caller cares
// about, and because an interface would have boxed a store.Snapshot — a value
// type — once per statement to gain nothing.
type readSource struct {
	file *durable.Snapshot
	heap store.Snapshot
	// overlay is the open transaction whose staged writes the snapshot does not
	// yet hold, or nil outside one. It is what makes a transaction read its own
	// writes: without it, a DELETE following an UPDATE in one transaction would
	// decide against a state its own earlier statement had already changed.
	overlay *tx
}

// get appends the document stored under key to dst, reporting whether the key
// exists.
//
// It exists because three statement shapes need a read the scan does not
// perform: an INSERT has to know whether the key is taken, and a keyed UPDATE
// or DELETE has to know whether the key is there before it can report an honest
// RowsAffected.
func (r readSource) get(dst []byte, key string) ([]byte, bool, error) {
	if r.overlay != nil {
		if doc, found, known := r.overlay.lookup(key); known {
			return append(dst, doc...), found, nil
		}
	}
	if r.file != nil {
		return r.file.AppendRaw(dst, key)
	}
	out, ok := r.heap.AppendRaw(dst, key)
	return out, ok, nil
}

// scan enumerates every document into the filter.
//
// The two backends are enumerated by their own scan primitives rather than
// through a common iterator, because they hand out their bytes differently: the
// durable scan lends page-backed bytes for the duration of one callback and
// wants a caller-owned overflow buffer, and the heap snapshot lends the
// collection's own storage for as long as the snapshot is open. The filter
// copies either, so neither lifetime escapes.
func (c *conn) scan(r readSource, filter *query.Filter) error {
	if r.file != nil {
		scratch, err := r.file.RangeRawBuffer(c.scanScratch, func(key, value []byte) error {
			if r.overlay != nil {
				// A key the transaction has staged is delivered in the state
				// the transaction put it in, not the snapshot's: deleted keys
				// are skipped and replaced ones are substituted. Doing the
				// substitution here rather than after the scan is what keeps
				// the scan a single pass and the row order the snapshot's.
				if doc, found, known := r.overlay.lookup(string(key)); known {
					if !found {
						return nil
					}
					return filter.Add(key, doc)
				}
			}
			return filter.Add(key, value)
		})
		c.scanScratch = scratch
		if err != nil {
			return err
		}
		if r.overlay == nil {
			return nil
		}
		// Documents the transaction created are in no snapshot, so the scan
		// cannot have produced them. They are offered after it, which puts them
		// last in the pass rather than in key order — a difference no statement
		// can observe, because a mutation has no ORDER BY and reports a count.
		return r.overlay.each(func(key string, doc []byte) error {
			c.keyScratch = append(c.keyScratch[:0], key...)
			return filter.Add(c.keyScratch, doc)
		})
	}
	var failure error
	r.heap.Range(func(key string, value vibejson.RawValue) bool {
		// The key is converted through the connection's own buffer rather than
		// with []byte(key), which would allocate once per document scanned —
		// including every document the predicate is about to reject.
		c.keyScratch = append(c.keyScratch[:0], key...)
		if err := filter.Add(c.keyScratch, value.Bytes()); err != nil {
			failure = err
			return false
		}
		return true
	})
	return failure
}

// plan turns a prepared statement and its binding into the exact set of writes
// it performs, without performing any of them.
//
// Separating the decision from the application is what lets the two backends
// share every rule about what a statement means while disagreeing entirely
// about how a write is published, and it is what lets a transaction record the
// same decision for later. It is also what makes "nothing was written" true of
// every failure this layer can raise: a bad key type, an unparseable document,
// a duplicate key, and an over-large batch are all decided here.
func (c *conn) plan(d *query.DMLStatement, args []any, r readSource) ([]mutation, error) {
	c.mutations = c.mutations[:0]
	c.docBuf = c.docBuf[:0]
	tree := d.Tree()
	switch tree.Kind {
	case sqlast.KindInsert:
		return c.planInsert(d, args, r, tree.Insert)
	case sqlast.KindUpdate:
		return c.planUpdate(d, args, r, tree.Update)
	case sqlast.KindDelete:
		return c.planDelete(d, args, r, tree.Delete)
	}
	return nil, fmt.Errorf("vibesql: %s is not a mutation", d.Kind())
}

func (c *conn) planInsert(
	d *query.DMLStatement, args []any, r readSource, stmt *sqlast.InsertStmt,
) ([]mutation, error) {
	for i := range stmt.Rows {
		row := &stmt.Rows[i]
		key, err := d.Key(row.Key, args)
		if err != nil {
			return nil, err
		}
		doc, err := d.Document(row.Doc, args)
		if err != nil {
			return nil, err
		}
		// An INSERT onto an existing key is refused rather than treated as an
		// overwrite. SQL's INSERT means "this row is new", and a store whose
		// Put happens to be an upsert would silently make it mean "this row is
		// new or was something else", which is the one reading that loses data
		// without saying so. A deliberate overwrite has its own spelling.
		for j := range c.mutations {
			if c.mutations[j].key == key {
				return nil, fmt.Errorf(
					"vibesql: this INSERT writes the key %q twice; one statement writes each key once", key)
			}
		}
		var found bool
		if c.docBuf, found, err = r.get(c.docBuf[:0], key); err != nil {
			return nil, err
		}
		if found {
			return nil, fmt.Errorf(
				"vibesql: a document with the key %q already exists; INSERT does not overwrite, "+
					"write UPDATE %s SET %q = ? WHERE %q = %q to replace it",
				key, stmt.Table, sqlast.DocumentColumn, sqlast.KeyColumn, key)
		}
		c.mutations = append(c.mutations, mutation{key: key, doc: doc})
	}
	return c.mutations, nil
}

func (c *conn) planUpdate(
	d *query.DMLStatement, args []any, r readSource, stmt *sqlast.UpdateStmt,
) ([]mutation, error) {
	doc, err := d.Document(stmt.Doc, args)
	if err != nil {
		return nil, err
	}
	if stmt.Keys != nil {
		// A keyed UPDATE replaces only keys that are there. Putting an absent
		// key would turn UPDATE into an insert, which is the mirror of the
		// mistake planInsert refuses, and would make RowsAffected count a
		// document the statement created rather than one it changed.
		keys, err := c.resolveKeys(d, stmt.Keys, args, r)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			c.mutations = append(c.mutations, mutation{key: key, doc: doc})
		}
		return c.mutations, nil
	}
	if err := c.collect(d, args, r, func(key []byte) {
		c.mutations = append(c.mutations, mutation{key: string(key), doc: doc})
	}); err != nil {
		return nil, err
	}
	return c.mutations, nil
}

func (c *conn) planDelete(
	d *query.DMLStatement, args []any, r readSource, stmt *sqlast.DeleteStmt,
) ([]mutation, error) {
	if stmt.Keys != nil {
		keys, err := c.resolveKeys(d, stmt.Keys, args, r)
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			c.mutations = append(c.mutations, mutation{key: key, remove: true})
		}
		return c.mutations, nil
	}
	if err := c.collect(d, args, r, func(key []byte) {
		c.mutations = append(c.mutations, mutation{key: string(key), remove: true})
	}); err != nil {
		return nil, err
	}
	return c.mutations, nil
}

// resolveKeys turns a primary-key condition's operands into the keys that are
// actually present, deduplicated.
//
// The presence check is what makes RowsAffected true: `DELETE FROM t WHERE
// "$key" IN ('a','b')` where only 'a' exists affects one document, and the
// store's own Delete would have reported the same thing one key at a time.
// Deduplication matters for the same reason and for a second one: a batch
// records one mutation per key, so listing a key twice would report two
// affected documents for one write.
func (c *conn) resolveKeys(
	d *query.DMLStatement, operands []sqlast.Operand, args []any, r readSource,
) ([]string, error) {
	c.keys = c.keys[:0]
	for _, operand := range operands {
		key, err := d.Key(operand, args)
		if err != nil {
			return nil, err
		}
		seen := false
		for _, existing := range c.keys {
			if existing == key {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		var found bool
		if c.docBuf, found, err = r.get(c.docBuf[:0], key); err != nil {
			return nil, err
		}
		if found {
			c.keys = append(c.keys, key)
		}
	}
	return c.keys, nil
}

// collect runs the statement's filtered scan, reporting each matching key.
//
// The scan is the connection's own enumeration of the collection fed through
// query's filter, which evaluates the compiled predicate a SELECT with the same
// WHERE would have evaluated. See query/sqldml.go for why the filter is reached
// that way rather than by walking the parse tree.
func (c *conn) collect(d *query.DMLStatement, args []any, r readSource, keep func(key []byte)) error {
	filter, err := d.Filter(&c.exec, args, func(key, _ []byte) error {
		keep(key)
		return nil
	})
	if err != nil {
		return err
	}
	if err := c.scan(r, filter); err != nil {
		return err
	}
	return filter.Done()
}
