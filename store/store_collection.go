package store

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"
)

var (
	// ErrCollectionName reports an empty, invalid UTF-8, or NUL-bearing
	// collection name.
	ErrCollectionName = errors.New(
		"vibedb: invalid collection name",
	)
	// ErrCollectionExists reports duplicate collection creation.
	ErrCollectionExists = errors.New(
		"vibedb: collection already exists",
	)
)

// Name returns the immutable catalog name, or "" for a standalone collection
// that no [Database] published.
func (c *Collection) Name() string {
	if c == nil {
		return ""
	}
	return c.name
}

// CollectionInfo is a detached catalog summary. Its name is immutable owned
// text; it does not retain a collection or snapshot graph.
type CollectionInfo struct {
	Name       string
	Documents  int
	Generation uint64
	Schema     bool
}

// Database is a concurrency-safe catalog of independent JSON collections.
// Its zero value is ready to use. DDL takes the catalog lock; holding a
// collection handle removes that lock and name lookup from every data
// operation. Dropping a name does not invalidate handles or snapshots already
// acquired from that collection.
type Database struct {
	mu          sync.RWMutex
	collections map[string]*Collection
}

// CreateCollection atomically publishes a new empty collection. Its options
// are validated and frozen before catalog publication.
func (d *Database) CreateCollection(
	name string,
	options Options,
) (*Collection, error) {
	if d == nil || !validCollectionName(name) {
		return nil, ErrCollectionName
	}
	collection, err := New(options)
	if err != nil {
		return nil, err
	}
	collection.name = strings.Clone(name)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.collections == nil {
		d.collections = make(map[string]*Collection)
	}
	if _, exists := d.collections[name]; exists {
		return nil, ErrCollectionExists
	}
	d.collections[collection.name] = collection
	return collection, nil
}

// Collection resolves name and reports whether it is currently cataloged.
func (d *Database) Collection(name string) (*Collection, bool) {
	if d == nil {
		return nil, false
	}
	d.mu.RLock()
	collection, ok := d.collections[name]
	d.mu.RUnlock()
	return collection, ok
}

// DropCollection removes name from the catalog. Existing handles and
// snapshots remain valid and continue to own their collection.
func (d *Database) DropCollection(name string) bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.collections[name]; !ok {
		return false
	}
	delete(d.collections, name)
	return true
}

// AppendCollections appends a name-ordered catalog snapshot to dst.
func (d *Database) AppendCollections(
	dst []CollectionInfo,
) []CollectionInfo {
	if d == nil {
		return dst
	}
	d.mu.RLock()
	start := len(dst)
	for _, collection := range d.collections {
		state := collection.state.Load()
		info := CollectionInfo{
			Name:   collection.name,
			Schema: collection.options.Schema != nil,
		}
		if state != nil {
			info.Documents = state.Count
			info.Generation = state.Generation
		}
		dst = append(dst, info)
	}
	d.mu.RUnlock()
	slices.SortFunc(
		dst[start:],
		func(a, b CollectionInfo) int {
			return strings.Compare(a.Name, b.Name)
		},
	)
	return dst
}

func validCollectionName(name string) bool {
	return name != "" && utf8.ValidString(name) &&
		!strings.ContainsRune(name, 0)
}
