package vibesql

import (
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
)

// CREATE TABLE and CREATE INDEX.
//
// # Which backend accepts which statement, and why the other refuses
//
// Both statements are heap-only today, and both refusals are the engine's
// rather than this package's:
//
// CREATE TABLE creates a collection in a catalog. [store.Database] is a
// catalog and has CreateCollection; a durable collection is one file holding
// one collection with no catalog above it, created by the application that
// chose its options. A durable catalog is being built elsewhere and does not
// exist on this branch, so a CREATE TABLE against a file DSN has nowhere to go
// and is refused by name.
//
// CREATE INDEX adds an index to a live collection. The heap [store.Collection]
// implements store.IndexManager — CreateIndex publishes the definition, every
// later write maintains it, and BackfillIndex processes the documents already
// there. A durable collection's indexes are frozen when the file is created and
// it deliberately does not implement IndexManager; online index creation for it
// is being built elsewhere. So CREATE INDEX against a durable collection is
// refused rather than silently made a no-op, which is the one outcome that
// would leave a caller believing they had an index.
//
// Neither refusal is a design position. Both become one line of dispatch when
// the corresponding engine work lands.

// execDefine runs a CREATE TABLE or CREATE INDEX.
//
// It reports zero rows affected, which is the truth: RowsAffected counts
// documents, a definition writes none, and reporting 1 for "one object created"
// would be counting a different thing under the same name.
func (c *conn) execDefine(d *query.DMLStatement) (driver.Result, error) {
	if c.tx != nil {
		// A definition is not part of the write batch a transaction is, so it
		// could only be applied outside it — which would make a rolled-back
		// transaction leave a collection or an index behind. Refusing is the
		// only honest answer while the catalog and the batch are separate
		// things.
		return result{}, errors.New(
			"vibesql: a definition cannot run inside a transaction: a transaction is one document write " +
				"batch, and a catalog change is not part of it, so a rollback could not undo one")
	}
	db := c.src.db
	if db == nil {
		return result{}, fmt.Errorf(
			"vibesql: %s needs an in-process store.Database: a durable collection is one file holding one "+
				"collection with no catalog above it, and its indexes are frozen when the file is created. "+
				"Register a store.Database with Attach to run definitions", d.Kind())
	}
	switch d.Kind() {
	case query.DDLCreateTable:
		return c.createTable(db, d)
	default:
		return c.createIndex(db, d)
	}
}

// createTable creates a collection, with the declared schema if the statement
// gave a column list.
func (c *conn) createTable(db *store.Database, d *query.DMLStatement) (result, error) {
	def, err := d.LowerTable()
	if err != nil {
		return result{}, err
	}
	if _, exists := db.Collection(def.Name); exists {
		if def.IfNotExists {
			return result{}, nil
		}
		return result{}, fmt.Errorf("vibesql: the collection %q already exists", def.Name)
	}
	if _, err := db.CreateCollection(def.Name, store.Options{Schema: def.Schema}); err != nil {
		return result{}, fmt.Errorf("vibesql: CREATE TABLE %s: %w", def.Name, err)
	}
	return result{}, nil
}

// createIndex declares an index and backfills it to completion.
//
// The backfill is run here, in full, rather than left for the application to
// drive. A CREATE INDEX that returned with the index still building would be
// telling the truth about the engine and a lie about the statement: SQL's
// CREATE INDEX means the index is there when it returns, and a caller who then
// ran a query would get the exact scan fallback and conclude the index did
// nothing. BackfillIndex with no chunk limit is the whole collection, which is
// what the statement asked for.
func (c *conn) createIndex(db *store.Database, d *query.DMLStatement) (result, error) {
	def, err := d.LowerIndex()
	if err != nil {
		return result{}, err
	}
	collection, ok := db.Collection(def.Table)
	if !ok {
		return result{}, fmt.Errorf(
			"vibesql: this database has no collection %q to index", def.Table)
	}
	if _, err := collection.CreateIndex(def.Definition); err != nil {
		if def.IfNotExists && errors.Is(err, store.ErrIndexExists) {
			return result{}, nil
		}
		return result{}, fmt.Errorf("vibesql: CREATE INDEX %s: %w", def.Definition.Name, err)
	}
	if _, err := collection.BackfillIndex(def.Definition.Name, 0); err != nil {
		return result{}, fmt.Errorf("vibesql: backfilling index %s: %w", def.Definition.Name, err)
	}
	return result{}, nil
}
