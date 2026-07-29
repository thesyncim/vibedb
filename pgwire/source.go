package pgwire

import (
	"context"
	"fmt"

	"github.com/thesyncim/vibedb/query"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// Where a session's rows come from.
//
// A Source is a closed set of three shapes, and which one a server was built
// with decides which statements it can run. A SQL Database provides the typed
// writable catalog/session runtime. A heap [store.Database] provides read-only
// multi-collection snapshots, including JOIN. A single durable collection has
// no catalog, so it is read-only, resolves one name, and has no second JOIN
// side.
//
// The interface is closed — its method is unexported — because the set of
// things the query engine can execute against is closed. An open interface here
// would advertise an extension point that [query.Source] does not actually
// have.

// A Source supplies either a read-only query source or a writable SQL catalog.
// Construct one with [FromDatabase], [FromCollection], or [FromSQLDatabase].
type Source interface {
	// validate rejects a source that cannot ever resolve safely. Keeping it in
	// this closed interface means a future source shape cannot accidentally be
	// admitted by NewServer without defining its construction invariant.
	validate() error
	// resolve produces the query source for one statement, or an error naming
	// why this source cannot serve it.
	resolve(collection string, joins bool) (lease, error)
}

type sqlSessionSource interface {
	openSession(context.Context) (*sqldriver.Session, error)
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

// FromSQLDatabase serves the SQL catalog owned by db. Unlike [FromDatabase],
// this source accepts the repository's bounded SQL DDL, DML, SELECT, indexes,
// and transaction subset over the PostgreSQL protocol.
//
// The Database is borrowed and remains the caller's responsibility. Each
// authenticated PostgreSQL connection owns one independent SQL Session, which
// the server closes on every disconnect path.
func FromSQLDatabase(db *sqldriver.Database) Source {
	return &sqlDatabaseSource{db: db}
}

type databaseSource struct{ db *store.Database }

func (s *databaseSource) validate() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("pgwire: FromDatabase requires a non-nil database")
	}
	return nil
}

func (s *databaseSource) resolve(collection string, _ bool) (lease, error) {
	if s == nil || s.db == nil {
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

func (s *collectionSource) validate() error {
	if s == nil || s.collection == nil {
		return fmt.Errorf("pgwire: FromCollection requires a non-nil collection")
	}
	return nil
}

func (s *collectionSource) resolve(collection string, joins bool) (lease, error) {
	if s == nil || s.collection == nil {
		return lease{}, newError(sqlstateInternalError,
			"pgwire: the server has no durable collection")
	}
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

type sqlDatabaseSource struct{ db *sqldriver.Database }

func (s *sqlDatabaseSource) validate() error {
	if s == nil || s.db == nil {
		return fmt.Errorf("pgwire: FromSQLDatabase requires a non-nil SQL database")
	}
	return nil
}

func (s *sqlDatabaseSource) resolve(string, bool) (lease, error) {
	return lease{}, newError(sqlstateInternalError,
		"pgwire: a SQL catalog query bypassed its typed session")
}

func (s *sqlDatabaseSource) openSession(ctx context.Context) (*sqldriver.Session, error) {
	if s == nil || s.db == nil {
		return nil, sqldriver.ErrDatabaseClosed
	}
	return s.db.NewSession(ctx)
}
