package vibesql

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
)

// Where a connection's rows come from.
//
// The DSN is deliberately not a parameter language. It is one of two things:
//
//	/var/lib/app/users.vj      a durable collection file
//	memory:analytics           a source this process registered with Attach
//
// The file form is the one a caller reaches for first, and it carries a real
// limitation worth stating rather than hiding. A durable collection's options —
// its index catalog above all — are fixed when the file is created, and Open
// refuses a mismatch, so a path alone can only open a collection created with
// default options. A file with declared indexes has to be opened by the
// application that knows its options and handed over with [AttachCollection].
// Inventing a DSN syntax for index definitions would be inventing a second
// spelling for durable.Options, which is the kind of thing that is easy to add
// and impossible to remove.
//
// The memory form is not a convenience. A join is executable only against a
// [store.DatabaseSnapshot], the consistent multi-collection cut, and only a
// heap [store.Database] can produce one; a durable file holds exactly one
// collection and has no catalog. So the shape of the source decides which
// statements a connection can run, and [Attach] is how an application that
// already holds a Database lets SQL read it.

// ErrUnknownSource reports a DSN naming a registered source that does not
// exist.
var ErrUnknownSource = errors.New("vibesql: no source is registered under that name")

// memoryPrefix marks a DSN naming a registered in-process source rather than a
// filesystem path. It is a scheme rather than a query parameter so that adding
// a second scheme later cannot change how an existing DSN parses.
const memoryPrefix = "memory:"

// A source is the collections one DSN resolves to, shared by every connection
// that names it.
//
// It is shared rather than per-connection because a durable collection takes an
// exclusive writer lock on its file: a second Open of the same path fails, and
// database/sql opens a connection whenever its pool decides to. Reference
// counting is at the connector, which database/sql closes with the DB.
type source struct {
	// db is set for a heap database, which is the only source a join can run
	// against.
	db *store.Database
	// collection and name are set for a single durable collection: the handle,
	// and the name a FROM clause has to spell to reach it.
	collection *durable.Collection
	name       string
	// file and path are set only for a collection this package opened, and are
	// what closing it has to undo. An attached collection belongs to the
	// application, which closes it.
	file  *os.File
	path  string
	owned bool

	refs int
}

var (
	registryMu sync.Mutex
	// attached holds the sources an application registered by name.
	attached = map[string]*source{}
	// opened holds the durable collections this package opened, keyed by
	// absolute path, so a second connection to one file shares the first's
	// handle instead of failing on its writer lock.
	opened = map[string]*source{}
)

// Attach registers a heap [store.Database] as the source named by the DSN
// "memory:"+name, and returns that DSN.
//
// This is the source a statement with a JOIN needs: the join reads both sides
// out of one [store.DatabaseSnapshot], the cut that makes snapshot skew across
// a join inexpressible, and only a Database produces one.
//
// The database is not copied and not owned. It stays the application's to write
// to, and every query reads a snapshot taken at the moment it runs. Detach
// removes the registration; open connections keep working, because they hold
// the source directly.
func Attach(name string, db *store.Database) (string, error) {
	if db == nil {
		return "", errors.New("vibesql: Attach was given a nil Database")
	}
	return attach(name, &source{db: db})
}

// AttachCollection registers one durable collection under name, reachable as
// the DSN "memory:"+name and spelled in FROM as name.
//
// It exists because a durable collection's options are frozen at creation and a
// path cannot describe them; an application that opened a collection with
// declared indexes hands the handle over rather than teaching the DSN to
// restate its catalog. The collection stays the application's to write to and
// to close.
func AttachCollection(name string, collection *durable.Collection) (string, error) {
	if collection == nil {
		return "", errors.New("vibesql: AttachCollection was given a nil Collection")
	}
	return attach(name, &source{collection: collection, name: name})
}

func attach(name string, s *source) (string, error) {
	if name == "" || strings.ContainsAny(name, "/\\") {
		return "", fmt.Errorf("vibesql: %q is not a usable source name", name)
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := attached[name]; exists {
		return "", fmt.Errorf("vibesql: a source named %q is already attached", name)
	}
	attached[name] = s
	return memoryPrefix + name, nil
}

// Detach removes a registration made by [Attach] or [AttachCollection],
// reporting whether one was there. Connections already open keep reading their
// source; what Detach ends is the ability to open new ones by that name.
func Detach(name string) bool {
	registryMu.Lock()
	defer registryMu.Unlock()
	_, ok := attached[name]
	delete(attached, name)
	return ok
}

// acquire resolves a DSN to a source and takes a reference on it.
func acquire(dsn string) (*source, error) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if name, ok := strings.CutPrefix(dsn, memoryPrefix); ok {
		s, found := attached[name]
		if !found {
			return nil, fmt.Errorf("%w: %q", ErrUnknownSource, name)
		}
		s.refs++
		return s, nil
	}
	path, err := filepath.Abs(dsn)
	if err != nil {
		return nil, err
	}
	if s, found := opened[path]; found {
		s.refs++
		return s, nil
	}
	s, err := openFile(path)
	if err != nil {
		return nil, err
	}
	s.refs = 1
	opened[path] = s
	return s, nil
}

// openFile opens a durable collection with default options, naming it after
// the file.
func openFile(path string) (*source, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	collection, err := durable.Open(file, durable.Options{})
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf(
			"vibesql: opening %s: %w; a collection created with declared indexes or "+
				"non-default options cannot be opened from a path alone — open it in the "+
				"application and register it with AttachCollection", path, err)
	}
	name := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	return &source{collection: collection, name: name, file: file, path: path, owned: true}, nil
}

// release drops one reference, closing a collection this package opened once
// the last connector using it is closed.
func (s *source) release() error {
	registryMu.Lock()
	defer registryMu.Unlock()
	s.refs--
	if s.refs > 0 || !s.owned {
		return nil
	}
	delete(opened, s.path)
	err := s.collection.Close()
	if closeErr := s.file.Close(); err == nil {
		err = closeErr
	}
	s.collection, s.file = nil, nil
	return err
}

// A handle is one execution's resolved [query.Source] plus whatever has to be
// released afterwards. The durable backend leases a generation for as long as
// a snapshot is open, which blocks the writer from reusing retired extents, so
// the lease is taken per query and dropped when the rows are closed — the
// lifetime durable.Snapshot's own documentation asks for.
type handle struct {
	src  query.Source
	file *durable.Snapshot
}

func (h *handle) close() {
	if h.file != nil {
		_ = h.file.Close()
		h.file = nil
	}
}

// resolve produces the query source for one statement, checking that the
// collection the statement names is one this source holds.
func (s *source) resolve(collection string, joins bool) (handle, error) {
	if s.db != nil {
		catalog := s.db.Snapshot()
		if _, ok := catalog.Collection(collection); !ok {
			return handle{}, fmt.Errorf(
				"vibesql: this database has no collection %q", collection)
		}
		return handle{src: query.FromDatabase(catalog, collection)}, nil
	}
	if collection != s.name {
		return handle{}, fmt.Errorf(
			"vibesql: this connection reads the collection %q, and the statement names %q",
			s.name, collection)
	}
	if joins {
		return handle{}, errors.New(
			"vibesql: a JOIN reads two collections from one consistent snapshot, and this " +
				"connection holds a single durable collection; attach a store.Database instead")
	}
	snapshot, err := s.collection.Snapshot()
	if err != nil {
		return handle{}, err
	}
	return handle{src: query.FromFile(snapshot), file: snapshot}, nil
}
