package pgwire

import (
	"fmt"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// Where a session's rows come from.
//
// A Source is a closed set of two shapes, and which one a server was built with
// decides which statements it can run. A heap [store.Database] holds many
// collections and produces a [store.DatabaseSnapshot], the consistent
// multi-collection cut a JOIN needs, so a database-backed server can serve a
// join. A single durable collection is one collection with no catalog, so a
// statement naming any other collection is an undefined table and a JOIN has
// no second side to read.
//
// The interface is closed — its method is unexported — because the set of
// things the query engine can execute against is closed. An open interface here
// would advertise an extension point that [query.Source] does not actually
// have.

// A Source supplies the collections a server's statements read. Construct one
// with [FromDatabase] or [FromCollection].
type Source interface {
	// resolve produces the query source for one statement, or an error naming
	// why this source cannot serve it.
	resolve(collection string, joins bool) (lease, error)
}

// A lease is one execution's resolved [query.Source] plus whatever must be
// released when its rows are done.
//
// The durable backend pins a generation for as long as a snapshot is open,
// which blocks the writer from reusing retired extents, so the snapshot is
// taken per execution and dropped when the portal's rows are exhausted or the
// portal is closed. That is the lifetime durable.Snapshot's own documentation
// asks for, and getting it wrong would not corrupt anything — it would quietly
// stop the writer from ever reclaiming space.
type lease struct {
	src  query.Source
	file *durable.Snapshot
}

func (l *lease) release() {
	if l.file != nil {
		_ = l.file.Close()
		l.file = nil
	}
}

// FromDatabase serves every collection in db. It is the source a statement with
// a JOIN needs, because both sides of a join are read from one
// [store.DatabaseSnapshot] and only a Database produces one.
//
// The database is not copied and not owned; it stays the application's to write
// to, and every statement reads a snapshot taken at the moment it executes.
func FromDatabase(db *store.Database) Source { return &databaseSource{db: db} }

// FromCollection serves one durable collection under the name a FROM clause
// must spell. A statement naming anything else is an undefined table, and a
// statement with a JOIN is refused: one collection file has no catalog and
// therefore no second side to read consistently.
func FromCollection(name string, c *durable.Collection) Source {
	return &collectionSource{name: name, collection: c}
}

type databaseSource struct{ db *store.Database }

func (s *databaseSource) resolve(collection string, _ bool) (lease, error) {
	if s.db == nil {
		return lease{}, newError(sqlstateInternalError, "pgwire: the server has no database")
	}
	catalog := s.db.Snapshot()
	if _, ok := catalog.Collection(collection); !ok {
		return lease{}, newError(sqlstateUndefinedTable,
			fmt.Sprintf("relation %q does not exist", collection))
	}
	return lease{src: query.FromDatabase(catalog, collection)}, nil
}

type collectionSource struct {
	name       string
	collection *durable.Collection
}

func (s *collectionSource) resolve(collection string, joins bool) (lease, error) {
	if collection != s.name {
		return lease{}, newError(sqlstateUndefinedTable,
			fmt.Sprintf("relation %q does not exist", collection)).
			withHint(fmt.Sprintf("this server serves one collection, %q", s.name))
	}
	if joins {
		return lease{}, newError(sqlstateFeatureNotSupported,
			"a JOIN reads two collections from one consistent snapshot, and this server "+
				"holds a single durable collection").
			withHint("serve a store.Database with FromDatabase instead")
	}
	snapshot, err := s.collection.Snapshot()
	if err != nil {
		return lease{}, newError(sqlstateInternalError, err.Error())
	}
	return lease{src: query.FromFile(snapshot), file: snapshot}, nil
}
