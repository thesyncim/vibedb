package durable

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"
)

// Multi-collection durable reads.
//
// A durable Collection publishes each new state through one atomic pointer
// guarded by snapshotGate, and Collection.Snapshot takes that gate's read side
// for exactly as long as it needs to load the state pointer and acquire a
// generation lease against it. Taking two such snapshots one after another does
// not compose into a view of the database: a commit on the second collection
// can land between the two acquisitions, and the result is a pair of
// generations that never coexisted. A join across them would resolve
// references against a state its own driving side never saw.
//
// Database.Snapshot closes that window the way store.Database.Snapshot does,
// with the one substitution the durable backend requires. The heap takes every
// collection's writer mutex; here the exclusion needed is against publication
// rather than against mutation in general, and publication is exactly what
// snapshotGate's write side already fences. So the durable analogue takes every
// collection's snapshotGate read side, in collection-name order, and acquires
// one lease per collection while the whole set is held.
//
// Read-locking rather than write-locking is not a weakening. A generation lease
// is what makes a snapshot durable-valid — it pins the extents the reclaimer
// may reuse — and the gate's write side is held across the instant a commit
// swaps the state pointer, so no collection can be between deciding its next
// state and publishing it while any state here is loaded. Concurrent readers
// are not excluded from each other, which is right: two snapshots of the same
// database are both consistent cuts and neither invalidates the other.
//
// The acquisition order is a total order over a set the catalog lock is holding
// still, which is what makes it provably deadlock-free rather than merely
// deadlock-free in practice. It is worth being precise about why the proof is
// needed at all when no writer ever holds two collections' gates: Go's
// sync.RWMutex makes a pending writer block subsequent readers, so a reader
// holding gate A and waiting on gate B behind a writer is a real wait edge, and
// unordered acquisition by two concurrent snapshots would close it into a cycle.
//
// The cost lands entirely on the reader, exactly as on the heap side. Nothing
// is added to the write path: no shared gate to acquire per mutation, no atomic
// beyond the ones a commit already performs, and single-collection
// Collection.Snapshot is untouched.

var (
	// ErrCollectionName reports an empty, invalid UTF-8, separator-bearing, or
	// otherwise unsafe collection name. A durable database maps names onto file
	// names, so the rule is stricter than the heap catalog's: a name that could
	// escape the database directory or collide with the directory's own
	// bookkeeping is refused rather than sanitized.
	ErrCollectionName = errors.New("vibejson: invalid durable collection name")
	// ErrCollectionExists reports duplicate collection creation.
	ErrCollectionExists = errors.New("vibejson: durable collection already exists")
	// ErrDatabaseClosed reports use of a closed Database.
	ErrDatabaseClosed = errors.New("vibejson: durable database is closed")
)

// collectionFileSuffix is the extension a collection's file carries inside a
// database directory. It exists so OpenDatabase can tell a collection from any
// other file a user or an operating system left in the directory, and so the
// mapping from a name to a path is total and reversible.
const collectionFileSuffix = ".vjc"

// A Database is a catalog of durable collections that share one directory,
// with a consistent multi-collection snapshot.
//
// Layout is one file per collection: the database is a directory and a
// collection named "orders" is the file "orders.vjc" inside it. That choice is
// deliberate and its reasoning belongs here, because the alternative — several
// collections sharing one file — is the one a reader will wonder about.
//
// A collection's file is a complete, self-describing store: two superblocks, a
// double state root, its own generation stream, its own free set, and its own
// lease table. Every one of those is per-file by construction. Sharing a file
// would mean a catalog root above the state roots, a generation counter whose
// order is meaningful across collections, a free set arbitrating extents
// between independent writers, and a commit protocol that fsyncs one device
// image on behalf of several. That is a different storage engine, not a catalog
// over this one. One file per collection leaves every collection's root,
// generation, lease, and recovery machinery exactly as it is, and reduces the
// catalog to what a catalog should be: a name-to-handle map plus the lock
// discipline that makes a cross-collection cut well defined.
//
// What one file per collection costs is real and should be stated: N open file
// descriptors and N writer locks rather than one, and N independent fsyncs for
// a logical change spanning collections. It does not cost cross-collection
// atomicity, because a shared file would not have provided that either without
// a cross-collection transaction the engine does not have.
//
// A Database owns the files it opened and closes them in Close.
type Database struct {
	dir string

	mu          sync.RWMutex
	closed      bool
	collections map[string]*databaseEntry

	// order is the collection set in name order, rebuilt on every catalog
	// mutation rather than sorted per snapshot. A snapshot's cost should be the
	// locks it takes and the leases it acquires, not a sort of the catalog, and
	// DDL is rare enough that paying there instead is free. It is also what
	// lets SnapshotInto reach a fully allocation-free steady state.
	order []*databaseEntry
}

// A databaseEntry is one cataloged collection plus the file the Database opened
// for it. The file is kept because a Collection deliberately does not own its
// caller's *os.File lifetime, so something above it must.
type databaseEntry struct {
	name       string
	collection *Collection
	file       *os.File
}

// DatabaseOptions configure a Database itself, as opposed to the collections in
// it. Its zero value is usable.
type DatabaseOptions struct {
	// Options is the per-collection configuration applied to every collection
	// OpenDatabase discovers on disk. Collections created through
	// CreateCollection take their own. It exists because a durable collection's
	// options are frozen into its file — index catalog, chunk geometry, schema
	// presence — and Open validates the file against them, so reopening a
	// directory requires knowing what to validate against.
	Options Options
	// FileMode is the permission bits a newly created collection file gets.
	// Zero means 0o600: a database is a private store by default.
	FileMode os.FileMode
	// DirMode is the permission bits a newly created database directory gets.
	// Zero means 0o700.
	DirMode os.FileMode
}

// OpenDatabase opens dir as a database, creating the directory if it does not
// exist, and opens every collection file already in it under options.Options.
//
// Recovery is per collection and bounded exactly as [Open]'s is: no key,
// document or posting leaf is scanned. A directory holding K collections
// costs K bounded recoveries and K open descriptors.
//
// Every collection in the directory must accept options.Options. A durable
// collection validates its frozen index catalog, chunk geometry, and schema
// presence against the options it is opened with, so a directory whose
// collections were created under different options cannot be reopened as one
// database — and failing loudly here is better than opening the subset that
// happens to match.
func OpenDatabase(dir string, options DatabaseOptions) (*Database, error) {
	if dir == "" {
		return nil, fmt.Errorf("vibejson: durable database requires a directory")
	}
	dirMode := options.DirMode
	if dirMode == 0 {
		dirMode = 0o700
	}
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, err
	}
	items, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	d := &Database{dir: dir, collections: make(map[string]*databaseEntry)}
	for _, item := range items {
		if item.IsDir() {
			continue
		}
		base := item.Name()
		if !strings.HasSuffix(base, collectionFileSuffix) {
			continue
		}
		name := strings.TrimSuffix(base, collectionFileSuffix)
		if !validCollectionName(name) {
			// A file whose stem is not a legal collection name cannot have been
			// written by CreateCollection, so it is not this database's. Opening
			// it would take an exclusive writer lock on a stranger's file.
			continue
		}
		file, openErr := os.OpenFile(filepath.Join(dir, base), os.O_RDWR, 0)
		if openErr != nil {
			_ = d.Close()
			return nil, openErr
		}
		collection, openErr := Open(file, options.Options)
		if openErr != nil {
			_ = file.Close()
			_ = d.Close()
			return nil, fmt.Errorf("vibejson: durable collection %q: %w", name, openErr)
		}
		d.collections[name] = &databaseEntry{name: name, collection: collection, file: file}
	}
	d.reorder()
	return d, nil
}

// CreateCollection creates and catalogs an empty collection named name, backed
// by its own file in the database directory.
//
// options are frozen into the new file exactly as [Create] freezes them. They
// are taken per collection rather than from the Database because two
// collections in one database may legitimately want different index catalogs —
// which is the common case for a join, whose inner side is usually the indexed
// one.
func (d *Database) CreateCollection(name string, options Options) (*Collection, error) {
	if d == nil {
		return nil, ErrDatabaseClosed
	}
	if !validCollectionName(name) {
		return nil, ErrCollectionName
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil, ErrDatabaseClosed
	}
	if _, exists := d.collections[name]; exists {
		return nil, ErrCollectionExists
	}
	path := filepath.Join(d.dir, name+collectionFileSuffix)
	// O_EXCL is what makes the catalog check above authoritative rather than
	// advisory: a file left behind by a previous process is refused here rather
	// than silently adopted and then rejected by Create's empty-file rule under
	// a much less obvious message.
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if os.IsExist(err) {
			return nil, ErrCollectionExists
		}
		return nil, err
	}
	collection, err := Create(file, options)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	entry := &databaseEntry{name: strings.Clone(name), collection: collection, file: file}
	d.collections[entry.name] = entry
	d.reorder()
	return collection, nil
}

// Collection resolves name and reports whether it is currently cataloged.
func (d *Database) Collection(name string) (*Collection, bool) {
	if d == nil {
		return nil, false
	}
	d.mu.RLock()
	entry, ok := d.collections[name]
	d.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return entry.collection, true
}

// DropCollection removes name from the catalog, closes its collection, closes
// the file the Database opened for it, and deletes that file. Dropping a name
// that is not cataloged is not an error.
//
// This differs from [store.Database.DropCollection], which leaves existing
// handles and snapshots valid, and the difference is not an oversight. A heap
// collection dropped from a catalog is still a live Go object the garbage
// collector reclaims once nothing references it; a durable one holds an
// exclusive writer lock, an open descriptor, a page cache, and prefetch
// goroutines, none of which any amount of unreachability releases. Something
// has to close it, and the Database is the only party that knows when the last
// catalog reference goes away.
//
// Open snapshots of the dropped collection must therefore be closed first, the
// same requirement [Collection.Close] already states.
func (d *Database) DropCollection(name string) error {
	if d == nil {
		return ErrDatabaseClosed
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrDatabaseClosed
	}
	entry, ok := d.collections[name]
	if !ok {
		d.mu.Unlock()
		return nil
	}
	delete(d.collections, name)
	d.reorder()
	d.mu.Unlock()

	err := entry.close()
	if removeErr := os.Remove(filepath.Join(d.dir, name+collectionFileSuffix)); err == nil {
		err = removeErr
	}
	return err
}

// Names appends the cataloged collection names to dst in name order.
func (d *Database) Names(dst []string) []string {
	if d == nil {
		return dst
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, entry := range d.order {
		dst = append(dst, entry.name)
	}
	return dst
}

// All iterates the cataloged collections in name order. It holds the catalog
// lock for the walk, so fn must not perform catalog DDL.
func (d *Database) All(fn func(name string, collection *Collection) bool) {
	if d == nil {
		return
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	for _, entry := range d.order {
		if !fn(entry.name, entry.collection) {
			return
		}
	}
}

// Len returns the number of cataloged collections.
func (d *Database) Len() int {
	if d == nil {
		return 0
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return len(d.order)
}

// Dir returns the database directory.
func (d *Database) Dir() string {
	if d == nil {
		return ""
	}
	return d.dir
}

// Close closes every cataloged collection and every file the Database opened,
// and marks the Database unusable. Active snapshots must be closed first.
//
// It reports the first error and still attempts every remaining close, because
// leaving descriptors and writer locks held after a failed shutdown is worse
// than losing the second error message.
func (d *Database) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	entries := d.order
	d.order = nil
	d.collections = nil
	d.mu.Unlock()

	var first error
	for _, entry := range entries {
		if err := entry.close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (e *databaseEntry) close() error {
	err := e.collection.Close()
	if fileErr := e.file.Close(); err == nil {
		err = fileErr
	}
	return err
}

// reorder rebuilds the name-ordered view. The caller holds the write lock.
func (d *Database) reorder() {
	d.order = d.order[:0]
	for _, entry := range d.collections {
		d.order = append(d.order, entry)
	}
	slices.SortFunc(d.order, func(a, b *databaseEntry) int {
		return strings.Compare(a.name, b.name)
	})
}

// validCollectionName reports whether name may be a durable collection.
//
// It is stricter than the heap catalog's rule because a durable name becomes a
// path element. Rejecting separators, the two relative directory names, and any
// name already carrying the collection suffix is what makes the name-to-path
// mapping injective and confined to the database directory; sanitizing instead
// would make two distinct names collide on one file.
func validCollectionName(name string) bool {
	if name == "" || !utf8.ValidString(name) {
		return false
	}
	if name == "." || name == ".." {
		return false
	}
	if strings.HasSuffix(name, collectionFileSuffix) {
		return false
	}
	return !strings.ContainsRune(name, 0) &&
		!strings.ContainsRune(name, '/') &&
		!strings.ContainsRune(name, '\\') &&
		!strings.ContainsRune(name, os.PathSeparator)
}
