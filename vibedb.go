// Package vibedb provides the small, owned-lifecycle API for an embedded JSON
// database. Packages store and store/durable remain available for callers that
// need direct control over storage geometry, snapshots, or query execution.
package vibedb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/txnclock"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

var (
	// ErrClosed reports an operation on a closed database or standalone file
	// collection.
	ErrClosed = errors.New("vibedb: handle is closed")
	// ErrInvalidOptions reports incompatible facade options. The underlying
	// store's validation error is wrapped when a resource bound is invalid.
	ErrInvalidOptions = errors.New("vibedb: invalid options")
	// ErrInvalidCollectionName reports an empty, invalid UTF-8, or longer than
	// MaxCollectionNameBytes logical name.
	ErrInvalidCollectionName = errors.New("vibedb: invalid collection name")
	// ErrManagedCollection reports an attempt to close a collection whose
	// lifecycle belongs to its Database. Close the database instead.
	ErrManagedCollection = errors.New("vibedb: collection is owned by its database")
	// ErrKeyTooLarge reports an empty key or one beyond the selected profile's
	// configured bound.
	ErrKeyTooLarge = errors.New("vibedb: collection key exceeds configured bound")
	// ErrDocumentTooLarge reports an empty document or one beyond the selected
	// profile's configured bound.
	ErrDocumentTooLarge = errors.New("vibedb: collection document exceeds configured bound")
)

const (
	// MaxCollectionNameBytes is the UTF-8 byte limit for a logical collection
	// name. Durable profiles encode names into a portable reversible filename;
	// this bound leaves room for the paired .vjc.rjournal suffix under the
	// 255-byte portable filename-component limit.
	MaxCollectionNameBytes = collectionname.MaxNameBytes
	// The product facade gives Memory the same logical zero-value bounds as the
	// durable engine. Advanced disk profiles may deliberately override them.
	defaultMaxKeyBytes      = 256
	defaultMaxDocumentBytes = 4 << 20
	// Keep common canonicalization buffers warm without allowing one unusual
	// document to pin its full high-water allocation on every collection.
	memoryCanonicalRetainBytes = 64 << 10
)

// Durability is one of the three deliberate product profiles. The zero value
// is Durable so omitted configuration chooses the strongest contract.
type Durability uint8

const (
	// Durable acknowledges a mutation only after its recovery record is
	// power-safely fenced. Reader visibility follows that fence.
	Durable Durability = iota
	// Buffered publishes mutations from bounded memory. Flush and Close make
	// the current visible generation crash-safe.
	Buffered
	// Memory keeps the complete database in process memory and never accesses
	// the path passed to Open. Close releases the catalog without persistence.
	Memory
)

// AdvancedOptions is the opt-in configuration surface for callers that need
// engine geometry or descriptor ownership. Most applications need only
// WithDurability.
//
// Engine contains the low-level collection options applied to newly created
// collections. Its Durability field is controlled by Durability here and must
// not request a different publication mode. Because durable.DurabilitySync is
// also Engine's zero value, Buffered treats that zero as unspecified and
// replaces it with DurabilityBufferedVisible; a different non-zero mode is a
// conflict. In the Memory profile, only Engine.Collection is meaningful;
// disk-specific fields are rejected.
type AdvancedOptions struct {
	Durability Durability
	Engine     durable.Options
	// FileMode and DirMode affect files and directories created by Open. Their
	// zero values select private 0o600 and 0o700 permissions respectively.
	FileMode os.FileMode
	DirMode  os.FileMode
	// TxnLimits bounds one multi-collection transaction across participants.
	// Zero dimensions normalize to the durable package defaults (16
	// collections; documents and bytes four times the single-collection
	// batch defaults). Violations surface as ErrTxTooLarge.
	TxnLimits durable.TxnLimits
}

// Option configures Open.
type Option interface {
	apply(*openOptions)
}

type optionFunc func(*openOptions)

func (fn optionFunc) apply(options *openOptions) { fn(options) }

type openOptions struct {
	advanced AdvancedOptions
}

// WithDurability selects one product durability profile.
func WithDurability(profile Durability) Option {
	return optionFunc(func(options *openOptions) {
		options.advanced.Durability = profile
	})
}

// WithAdvancedOptions replaces Open's complete advanced configuration. When
// combined with other options, options are applied from left to right.
func WithAdvancedOptions(advanced AdvancedOptions) Option {
	return optionFunc(func(options *openOptions) {
		options.advanced = advanced
	})
}

type openTarget uint8

const (
	openPath openTarget = iota
	openFile
)

type normalizedOptions struct {
	profile          Durability
	engine           durable.Options
	fileMode         os.FileMode
	dirMode          os.FileMode
	maxKeyBytes      int
	maxDocumentBytes int
	txnLimits        durable.TxnLimits
}

// normalizeOptions is the single compatibility boundary for Open and
// OpenFile. It runs before either function creates, opens, truncates, or locks
// filesystem state.
func normalizeOptions(options AdvancedOptions, target openTarget) (normalizedOptions, error) {
	if options.Durability > Memory {
		return normalizedOptions{}, fmt.Errorf("%w: unknown durability profile %d", ErrInvalidOptions, options.Durability)
	}
	if options.FileMode&^os.ModePerm != 0 || options.DirMode&^os.ModePerm != 0 {
		return normalizedOptions{}, fmt.Errorf("%w: file and directory modes may contain permission bits only", ErrInvalidOptions)
	}
	if target == openFile && (options.FileMode != 0 || options.DirMode != 0) {
		return normalizedOptions{}, fmt.Errorf("%w: OpenFile does not own file or directory permissions", ErrInvalidOptions)
	}

	normalized := normalizedOptions{
		profile: options.Durability, engine: cloneEngineOptions(options.Engine),
		fileMode: options.FileMode, dirMode: options.DirMode,
		maxKeyBytes: defaultMaxKeyBytes, maxDocumentBytes: defaultMaxDocumentBytes,
		txnLimits: normalizeFacadeTxnLimits(options.TxnLimits),
	}
	if schema := normalized.engine.Collection.Schema; schema != nil {
		if !schema.Valid() {
			return normalizedOptions{}, fmt.Errorf("%w: invalid compiled schema", ErrInvalidOptions)
		}
		frozen, err := store.CompileSchema(schema.Definition())
		if err != nil {
			return normalizedOptions{}, fmt.Errorf("%w: %w", ErrInvalidOptions, err)
		}
		normalized.engine.Collection.Schema = frozen
	}
	if options.Durability == Memory {
		if target == openFile {
			return normalizedOptions{}, fmt.Errorf("%w: the Memory profile has no file descriptor", ErrInvalidOptions)
		}
		memoryEngine := durable.Options{Collection: normalized.engine.Collection}
		if !reflect.DeepEqual(normalized.engine, memoryEngine) {
			return normalizedOptions{}, fmt.Errorf("%w: disk engine options cannot be used with the Memory profile", ErrInvalidOptions)
		}
		collection, err := normalized.engine.Collection.Normalized()
		if err != nil {
			return normalizedOptions{}, fmt.Errorf("%w: %w", ErrInvalidOptions, err)
		}
		normalized.engine.Collection = collection
		if options.FileMode != 0 || options.DirMode != 0 {
			return normalizedOptions{}, fmt.Errorf("%w: file modes cannot be used with the Memory profile", ErrInvalidOptions)
		}
		return normalized, nil
	}

	expected := durable.DurabilitySync
	if options.Durability == Buffered {
		expected = durable.DurabilityBufferedVisible
	}
	if options.Engine.Durability != durable.DurabilitySync &&
		options.Engine.Durability != expected {
		return normalizedOptions{}, fmt.Errorf("%w: product durability conflicts with Engine.Durability", ErrInvalidOptions)
	}
	if options.Engine.RecoveryJournal {
		return normalizedOptions{}, fmt.Errorf("%w: RecoveryJournal changes the selected product durability contract", ErrInvalidOptions)
	}
	normalized.engine.Durability = expected
	if err := durable.ValidateOptions(normalized.engine); err != nil {
		return normalizedOptions{}, fmt.Errorf("%w: %w", ErrInvalidOptions, err)
	}
	if normalized.engine.MaxKeyBytes != 0 {
		normalized.maxKeyBytes = normalized.engine.MaxKeyBytes
	}
	if normalized.engine.MaxDocumentBytes != 0 {
		normalized.maxDocumentBytes = normalized.engine.MaxDocumentBytes
	}
	return normalized, nil
}

func cloneEngineOptions(options durable.Options) durable.Options {
	options.Indexes = slices.Clone(options.Indexes)
	options.SkipIndexes = slices.Clone(options.SkipIndexes)
	for i := range options.Indexes {
		options.Indexes[i].Name = strings.Clone(options.Indexes[i].Name)
		options.Indexes[i].Paths = slices.Clone(options.Indexes[i].Paths)
		for j := range options.Indexes[i].Paths {
			options.Indexes[i].Paths[j] = strings.Clone(options.Indexes[i].Paths[j])
		}
	}
	return options
}

// Database is an owned catalog of named collections. Open owns every file and
// descriptor behind it; Close synchronizes buffered collections before
// releasing those resources. A Database must not be copied after first use.
type Database struct {
	profile          Durability
	engine           durable.Options
	maxKeyBytes      int
	maxDocumentBytes int
	txnLimits        durable.TxnLimits

	closed    atomic.Bool
	closeDone atomic.Bool
	closeMu   sync.Mutex
	closeErr  error
	disk      *durable.Database
	heap      store.Database

	handlesMu sync.Mutex
	handles   map[string]*Collection

	// commitMu serializes Commit validation and publication across every
	// transaction on this handle. Begin does not take it; think-time overlaps.
	commitMu sync.Mutex

	// The native serializable lane samples one database-global logical revision
	// before capturing its cut, then retains independently bounded histories per
	// collection. This preserves lazy-collection correctness without allowing
	// unrelated key churn to overflow another collection's exact history.
	clockMu            sync.Mutex
	txnRevision        uint64
	txnRevisionStopped bool
	txnActive          map[uint64]txnActiveRevision
	txnActiveOldest    uint64
	txnActiveNewest    uint64
	txnActiveLinked    bool
	txnActiveCount     atomic.Uint64
	txnHistoryFloor    uint64
	txnHistories       map[string]*txnclock.ExternalHistory

	// Deterministic in-package concurrency seams. Production leaves both nil.
	testAfterTxValidation     func()
	testDirectMutationBlocked func(*Collection)

	// updateDepth tracks Update/View closure nesting per goroutine. Concurrent
	// closures on distinct goroutines are allowed; re-entering Update on the
	// same goroutine is a typed refusal (not a savepoint).
	updateDepth sync.Map // uint64 goid → int depth
}

// Open opens path using the Durable profile unless an option selects another
// profile. Durable and Buffered databases use path as a directory containing
// one independently durable file per collection. Memory deliberately ignores
// path and performs no filesystem operation.
func Open(path string, options ...Option) (*Database, error) {
	configured := openOptions{advanced: AdvancedOptions{Durability: Durable}}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil Open option", ErrInvalidOptions)
		}
		option.apply(&configured)
	}
	normalized, err := normalizeOptions(configured.advanced, openPath)
	if err != nil {
		return nil, err
	}
	if normalized.profile != Memory && path == "" {
		return nil, fmt.Errorf("%w: durable and buffered databases require a path", ErrInvalidOptions)
	}
	db := &Database{
		profile:          normalized.profile,
		engine:           normalized.engine,
		maxKeyBytes:      normalized.maxKeyBytes,
		maxDocumentBytes: normalized.maxDocumentBytes,
		txnLimits:        normalized.txnLimits,
		handles:          make(map[string]*Collection),
	}
	if normalized.profile == Memory {
		return db, nil
	}
	db.disk, err = durable.OpenDatabase(path, durable.DatabaseOptions{
		Options:  normalized.engine,
		FileMode: normalized.fileMode,
		DirMode:  normalized.dirMode,
	})
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Collection returns a stable, lazy handle for name. Merely obtaining a handle
// performs no I/O and does not create an empty collection; the first mutation
// does. Invalid names are reported by operations on the returned handle.
func (d *Database) Collection(name string) *Collection {
	if d == nil {
		return &Collection{name: name, initialErr: ErrClosed}
	}
	if d.closed.Load() {
		return &Collection{name: name, initialErr: ErrClosed}
	}
	if !validCollectionName(name) {
		return &Collection{name: name, initialErr: ErrInvalidCollectionName}
	}
	d.handlesMu.Lock()
	defer d.handlesMu.Unlock()
	if d.closed.Load() {
		return &Collection{name: name, initialErr: ErrClosed}
	}
	if collection := d.handles[name]; collection != nil {
		return collection
	}
	collection := &Collection{name: strings.Clone(name), owner: d}
	d.handles[collection.name] = collection
	return collection
}

// Durability reports the database's selected product profile.
func (d *Database) Durability() Durability {
	if d == nil {
		return Durable
	}
	return d.profile
}

// Flush makes every mutation currently visible in a Buffered or Durable
// database crash-safe. It is a no-op for Memory.
func (d *Database) Flush() error {
	if d == nil {
		return ErrClosed
	}
	if d.closed.Load() {
		return ErrClosed
	}
	if d.disk == nil {
		return nil
	}
	var first error
	d.disk.All(func(_ string, collection *durable.Collection) bool {
		if err := collection.Flush(); err != nil && first == nil {
			first = facadeError(err)
		}
		return true
	})
	return first
}

// Close synchronizes and closes every owned collection and descriptor. It is
// idempotent. A descriptor passed to OpenFile is not owned by a Database and is
// instead released through Collection.Close.
func (d *Database) Close() error {
	if d == nil {
		return nil
	}
	d.closeMu.Lock()
	defer d.closeMu.Unlock()
	if d.closeDone.Load() {
		return d.closeErr
	}
	d.closed.Store(true)
	if d.disk != nil {
		closeErr := d.disk.Close()
		if !d.disk.CloseCompleted() {
			return facadeError(closeErr)
		}
		d.closeErr = facadeError(closeErr)
	}
	d.releaseHandles()
	d.closeDone.Store(true)
	return d.closeErr
}

// CloseCompleted reports whether Database.Close has released every owned
// collection, descriptor, and writer lock. Admission closes at the first Close
// attempt; this remains false if teardown returned a retryable reader, cache,
// or writer-unlock error.
func (d *Database) CloseCompleted() bool {
	return d == nil || d.closeDone.Load()
}

func (d *Database) releaseHandles() {
	d.handlesMu.Lock()
	defer d.handlesMu.Unlock()
	for _, collection := range d.handles {
		collection.resolveMu.Lock()
		collection.memoryPutMu.Lock()
		collection.memory.Store(nil)
		collection.disk.Store(nil)
		collection.memoryCanonical = nil
		collection.memoryPutMu.Unlock()
		collection.resolveMu.Unlock()
	}
	d.handles = nil
	d.heap = store.Database{}
}

// Collection is the profile-independent CRUD handle returned by
// Database.Collection and OpenFile. Get and Range return canonical JSON document
// bytes owned by the caller or borrowed only for the callback, respectively;
// engine page and workspace types are not part of this surface.
type Collection struct {
	name       string
	owner      *Database
	initialErr error
	profile    Durability // standalone OpenFile profile; owner is authoritative otherwise
	// validationOptions is the frozen collection contract for a standalone
	// OpenFile handle. Database-owned handles read the same contract from owner.
	validationOptions store.Options
	resolveMu         sync.Mutex
	memory            atomic.Pointer[store.Collection]
	disk              atomic.Pointer[durable.Collection]
	// Heap writes are already serialized by store.Collection. Holding the same
	// product-level interval across canonicalization lets their output buffer be
	// reused without adding a shared lock to durable writes or heap reads.
	memoryPutMu     sync.Mutex
	memoryCanonical []byte
	// indexMu serializes facade DDL only. It never participates in Put, Delete,
	// or reads; its purpose is to make the Memory profile's low-level
	// create-then-backfill sequence one retryable facade operation.
	indexMu sync.Mutex
	// txnFence closes the validation-to-publication window between native
	// transactions and direct point writes. Transactions take every dirty
	// collection's fence in name order; Put and Delete take only their own, so
	// unrelated collections retain independent write concurrency.
	txnFence sync.Mutex

	closed    atomic.Bool // standalone OpenFile collections only
	closeMu   sync.Mutex
	closeErr  error
	closeDone bool
}

// OpenFile opens or initializes one durable collection in file. file must be a
// regular, non-symlink file with a non-empty absolute Name that still resolves
// to this exact descriptor. Durable and Buffered collections manage a recovery-journal
// sibling at file.Name()+".rjournal" and synchronize its parent directory, so
// the caller must grant directory create/read/write/sync authority. Rename,
// backup, and restore operations must treat the primary and journal as one
// atomic pair.
//
// The caller retains ownership of the primary descriptor, but lends it
// exclusively to the Collection: keep it open and do not independently read,
// write, truncate, seek, or lock it until Collection.Close completes. Do not
// rename, replace, unlink, or retarget its lexical path during that borrow.
// Collection.Close synchronizes the collection and releases engine resources,
// including the engine-owned journal descriptor, but does not close file.
func OpenFile(file *os.File, options AdvancedOptions) (*Collection, error) {
	normalized, err := normalizeOptions(options, openFile)
	if err != nil {
		return nil, err
	}
	if file == nil {
		return nil, fmt.Errorf("%w: nil file", ErrInvalidOptions)
	}
	info, err := preflightOpenFile(file)
	if err != nil {
		return nil, err
	}
	var collection *durable.Collection
	if info.Size() == 0 {
		collection, err = durable.Create(file, normalized.engine)
	} else {
		collection, err = durable.Open(file, normalized.engine)
	}
	if err != nil {
		return nil, err
	}
	result := &Collection{
		profile:           normalized.profile,
		validationOptions: normalized.engine.Collection,
	}
	result.disk.Store(collection)
	return result, nil
}

// preflightOpenFile proves that the path-derived recovery-journal pair can be
// named from this descriptor before durable.Create or durable.Open is allowed
// to touch either file. In particular, an unlinked descriptor or a descriptor
// whose old name now addresses a different inode cannot accidentally create a
// journal for the wrong primary.
func preflightOpenFile(file *os.File) (os.FileInfo, error) {
	name := file.Name()
	if name == "" || !filepath.IsAbs(name) {
		return nil, fmt.Errorf(
			"%w: OpenFile requires a stable absolute file name",
			ErrInvalidOptions,
		)
	}
	base := filepath.Base(name)
	if len(base)+len(collectionname.JournalSuffix) > collectionname.MaxComponentBytes {
		return nil, fmt.Errorf(
			"%w: OpenFile primary name leaves no portable recovery-journal suffix space",
			ErrInvalidOptions,
		)
	}
	descriptorInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !descriptorInfo.Mode().IsRegular() {
		return nil, fmt.Errorf(
			"%w: OpenFile requires a regular file", ErrInvalidOptions,
		)
	}
	pathInfo, err := os.Lstat(name)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: OpenFile name does not resolve to its descriptor: %v",
			ErrInvalidOptions, err,
		)
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.Mode().IsRegular() {
		return nil, fmt.Errorf(
			"%w: OpenFile path must name a regular non-symlink file",
			ErrInvalidOptions,
		)
	}
	if !os.SameFile(descriptorInfo, pathInfo) {
		return nil, fmt.Errorf(
			"%w: OpenFile name resolves to a different file",
			ErrInvalidOptions,
		)
	}
	parentInfo, err := os.Stat(filepath.Dir(name))
	if err != nil || !parentInfo.IsDir() {
		return nil, fmt.Errorf(
			"%w: OpenFile requires an accessible parent directory",
			ErrInvalidOptions,
		)
	}
	return descriptorInfo, nil
}

// Name returns the catalog name, or an empty string for an OpenFile
// collection.
func (c *Collection) Name() string {
	if c == nil {
		return ""
	}
	return c.name
}

// backend resolves a lazy catalog handle once. Resolved pointers are atomic so
// the hot path adds no facade mutex above the engine's own concurrency control.
// create controls whether a missing collection is materialized.
//
// The closed check is an admission check, not a lifetime lock. An operation
// that wins admission concurrently with Close is safe because durable.Collection
// closes its mutation waiters, read epochs, and snapshots before releasing I/O
// resources. Such an operation either completes or reports ErrClosed. Memory
// owns no external resources and permits an already-admitted operation to
// finish before the logical close takes effect.
func (c *Collection) backend(create bool) (*store.Collection, *durable.Collection, error) {
	if c == nil {
		return nil, nil, ErrClosed
	}
	if c.initialErr != nil {
		return nil, nil, c.initialErr
	}
	if c.owner == nil {
		if c.closed.Load() {
			return nil, nil, ErrClosed
		}
		memory, disk := c.memory.Load(), c.disk.Load()
		// Close clears the standalone backend only after it has closed
		// admission. If that clear wins the two loads above, rechecking closed
		// prevents a concurrent operation from turning teardown into a false
		// miss or a successful no-op.
		if memory == nil && disk == nil && c.closed.Load() {
			return nil, nil, ErrClosed
		}
		return memory, disk, nil
	}

	db := c.owner
	if db.closed.Load() {
		return nil, nil, ErrClosed
	}
	if memory, disk := c.memory.Load(), c.disk.Load(); memory != nil || disk != nil {
		return memory, disk, nil
	}
	c.resolveMu.Lock()
	defer c.resolveMu.Unlock()
	if db.closed.Load() {
		return nil, nil, ErrClosed
	}
	memory, disk := c.memory.Load(), c.disk.Load()
	if memory == nil && disk == nil {
		if db.profile == Memory {
			memory, _ = db.heap.Collection(c.name)
			if memory == nil && create {
				var err error
				memory, err = db.heap.CreateCollection(c.name, db.engine.Collection)
				if err != nil {
					return nil, nil, facadeError(err)
				}
			}
			if memory != nil {
				c.memory.Store(memory)
			}
		} else {
			disk, _ = db.disk.Collection(c.name)
			if disk == nil && create {
				var err error
				disk, err = db.disk.CreateCollection(c.name, db.engine)
				if err != nil {
					return nil, nil, facadeError(err)
				}
			}
			if disk != nil {
				c.disk.Store(disk)
			}
		}
	}
	// Database.Close closes admission before the low-level catalog disappears.
	// An unresolved lazy handle can lose its Collection lookup to that close;
	// distinguish that race from a collection which was genuinely absent while
	// the database remained open.
	if memory == nil && disk == nil && db.closed.Load() {
		return nil, nil, ErrClosed
	}
	return memory, disk, nil
}

// Put validates document and atomically inserts or replaces key. created is
// true only when key was absent.
func (c *Collection) Put(key string, document []byte) (created bool, err error) {
	if err := c.admissionError(); err != nil {
		return false, err
	}
	// Resolve an existing collection before enforcing bounds: a zero-option
	// reopen adopts the immutable limits persisted in that collection's catalog,
	// which may deliberately differ from the facade's creation defaults.
	// Resolving without create preserves invalid-first-write non-creation.
	memory, disk, err := c.backend(false)
	if err != nil {
		return false, err
	}
	maxKeyBytes, maxDocumentBytes := c.bounds(disk)
	if len(key) == 0 || len(key) > maxKeyBytes {
		return false, ErrKeyTooLarge
	}
	if len(document) == 0 || len(document) > maxDocumentBytes {
		return false, ErrDocumentTooLarge
	}
	// The existing engine is the authoritative validator, so the steady-state
	// disk path parses exactly once. Only a genuinely absent lazy collection
	// needs a facade preflight before its file may be created.
	if memory == nil && disk == nil {
		if err := validateDocument(c.storeOptions(), document); err != nil {
			return false, err
		}
	}
	c.lockDirectMutation()
	defer c.unlockDirectMutation()
	// The fence may have waited behind a transaction that materialized this lazy
	// collection, or Close may have won admission while it waited. Resolve again
	// inside the publication interval before deciding whether creation is needed.
	memory, disk, err = c.backend(false)
	if err != nil {
		return false, err
	}
	if memory == nil && disk == nil {
		memory, disk, err = c.backend(true)
		if err != nil {
			return false, err
		}
	}
	if memory != nil {
		c.memoryPutMu.Lock()
		canonical, canonicalErr := vibejson.AppendCanonicalize(
			c.memoryCanonical[:0], document,
		)
		if canonicalErr == nil {
			created, canonicalErr = memory.Put(key, canonical)
		}
		if c.owner != nil && !c.owner.closed.Load() &&
			cap(canonical) <= memoryCanonicalRetainBytes {
			c.memoryCanonical = canonical[:0]
		} else {
			c.memoryCanonical = nil
		}
		c.memoryPutMu.Unlock()
		if canonicalErr == nil {
			c.recordCommittedKey(key)
		}
		return created, facadeError(canonicalErr)
	}
	before := disk.Generation()
	created, err = disk.Put(byteview.Bytes(key), document)
	if durableMutationPublished(disk, before, err) {
		c.recordCommittedKey(key)
	}
	return created, facadeError(err)
}

// Delete atomically removes key. deleted is false when key was absent.
func (c *Collection) Delete(key string) (deleted bool, err error) {
	memory, disk, err := c.backend(false)
	if err != nil {
		return false, err
	}
	maxKeyBytes, _ := c.bounds(disk)
	if len(key) == 0 || len(key) > maxKeyBytes {
		return false, ErrKeyTooLarge
	}
	c.lockDirectMutation()
	defer c.unlockDirectMutation()
	memory, disk, err = c.backend(false)
	if err != nil {
		return false, err
	}
	if memory != nil {
		deleted, err = memory.Delete(key)
		if err == nil && deleted {
			c.recordCommittedKey(key)
		}
		return deleted, facadeError(err)
	}
	if disk == nil {
		return false, nil
	}
	before := disk.Generation()
	deleted, err = disk.Delete(byteview.Bytes(key))
	if durableMutationPublished(disk, before, err) {
		c.recordCommittedKey(key)
	}
	return deleted, facadeError(err)
}

// lockDirectMutation enters the per-collection validation/publication fence for
// a database-owned handle. Standalone OpenFile collections cannot participate
// in a Database transaction and therefore need no facade-level fence.
func (c *Collection) lockDirectMutation() {
	if c == nil || c.owner == nil {
		return
	}
	if !c.txnFence.TryLock() {
		if hook := c.owner.testDirectMutationBlocked; hook != nil {
			hook(c)
		}
		c.txnFence.Lock()
	}
}

func (c *Collection) unlockDirectMutation() {
	if c != nil && c.owner != nil {
		c.txnFence.Unlock()
	}
}

// durableMutationPublished conservatively recognizes both an ordinary durable
// point-write publication and an error whose in-process outcome is uncertain.
// The latter must still advance conflict history or a transaction that began
// before it could overwrite the possibly published value.
func durableMutationPublished(c *durable.Collection, before uint64, err error) bool {
	if c == nil {
		return false
	}
	if c.Generation() != before {
		return true
	}
	return err != nil && c.PersistenceError() != nil
}

// Append appends key's canonical JSON document to dst. A miss leaves dst
// unchanged. The returned bytes belong to the caller and remain valid after a
// later mutation or Close.
func (c *Collection) Append(dst []byte, key string) ([]byte, bool, error) {
	memory, disk, err := c.backend(false)
	if err != nil {
		return dst, false, err
	}
	maxKeyBytes, _ := c.bounds(disk)
	if len(key) == 0 || len(key) > maxKeyBytes {
		return dst, false, ErrKeyTooLarge
	}
	if memory != nil {
		out, ok := memory.AppendRaw(dst, key)
		return out, ok, nil
	}
	if disk == nil {
		return dst, false, nil
	}
	out, ok, err := disk.AppendRaw(dst, byteview.Bytes(key))
	return out, ok, facadeError(err)
}

// Get returns an owned copy of key's canonical JSON document. A miss returns
// (nil, false, nil).
func (c *Collection) Get(key string) ([]byte, bool, error) {
	return c.Append(nil, key)
}

// Range visits one immutable current generation. key and document are borrowed,
// read-only views valid only for the callback; they must not be modified and
// must be copied if retained.
func (c *Collection) Range(fn func(key string, document []byte) error) error {
	memory, disk, err := c.backend(false)
	if err != nil {
		return err
	}
	if fn == nil {
		return nil
	}
	if memory != nil {
		snapshot, err := memory.Snapshot()
		if err != nil {
			return facadeError(err)
		}
		var visitErr error
		snapshot.Range(func(key string, value vibejson.RawValue) bool {
			visitErr = fn(key, value.Bytes())
			return visitErr == nil
		})
		return visitErr
	}
	if disk == nil {
		return nil
	}
	var callbackErr error
	err = disk.RangeRawCurrent(func(key, document []byte) error {
		callbackErr = fn(byteview.String(key), document)
		return callbackErr
	})
	if callbackErr != nil {
		return callbackErr
	}
	return facadeError(err)
}

// CreateIndex builds and publishes one exact scalar or compound index.
func (c *Collection) CreateIndex(name string, paths ...string) error {
	if err := c.admissionError(); err != nil {
		return err
	}
	definition := store.IndexDefinition{Name: name, Paths: paths}
	if _, err := store.CompileExactIndex(definition); err != nil {
		return err
	}
	c.indexMu.Lock()
	defer c.indexMu.Unlock()
	memory, disk, err := c.backend(true)
	if err != nil {
		return err
	}
	if memory != nil {
		if _, err := memory.CreateIndex(definition); err != nil {
			return facadeError(err)
		}
		if _, err := memory.BackfillIndex(name, 0); err != nil {
			// The facade exposes a one-shot CreateIndex, not the heap engine's
			// resumable Building state. Roll the unpublished facade operation
			// back so callers can repair their data and retry the same name.
			return facadeError(errors.Join(err, memory.DropIndex(name)))
		}
		return nil
	}
	_, err = disk.CreateIndex(definition)
	return facadeError(err)
}

// Flush makes this collection's current visible generation crash-safe. It is
// a no-op for Memory and for a lazy collection that has not been created.
func (c *Collection) Flush() error {
	_, disk, err := c.backend(false)
	if err != nil {
		return err
	}
	if disk == nil {
		return nil
	}
	return facadeError(disk.Flush())
}

// Metrics is the stable product-level operational snapshot. Detailed engine
// counters remain available from the low-level packages.
type Metrics struct {
	Durability          Durability
	Documents           uint64
	PublishedGeneration uint64
	// DurableGeneration is zero for Memory. In Buffered it can trail
	// PublishedGeneration until Flush or Close succeeds.
	DurableGeneration uint64
}

// Metrics returns a detached, constant-size snapshot without exposing mutable
// engine workspaces or implementation-specific statistics structures.
func (c *Collection) Metrics() (Metrics, error) {
	memory, disk, err := c.backend(false)
	if err != nil {
		return Metrics{}, err
	}
	profile := Durable
	if c.owner != nil {
		profile = c.owner.profile
	} else {
		profile = c.profile
	}
	metrics := Metrics{Durability: profile}
	if memory != nil {
		state := memory.StateMetrics()
		metrics.Documents = state.Documents
		metrics.PublishedGeneration = state.Generation
		return metrics, nil
	}
	if disk != nil {
		state := disk.StateMetrics()
		metrics.Documents = state.Documents
		metrics.PublishedGeneration = state.PublishedGeneration
		metrics.DurableGeneration = state.DurableGeneration
	}
	return metrics, nil
}

// Close synchronizes and releases a standalone OpenFile collection. Collections
// returned by Database.Collection are closed only by their owning database. A
// failed standalone Close may be retried; data operations remain closed after
// the first attempt.
func (c *Collection) Close() error {
	if c == nil {
		return nil
	}
	if c.owner != nil {
		return ErrManagedCollection
	}
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.closeDone {
		return c.closeErr
	}
	c.closed.Store(true)
	if disk := c.disk.Load(); disk != nil {
		c.closeErr = disk.Close()
		if !disk.CloseCompleted() {
			// Low-level Close deliberately leaves some non-persistence failures
			// retryable (notably a writer-unlock failure). Keep the engine handle
			// for a later Close call while admission remains shut.
			return c.closeErr
		}
		c.disk.Store(nil)
	}
	c.closeDone = true
	return c.closeErr
}

// CloseCompleted reports whether a standalone OpenFile collection has
// released its exclusive borrow of the caller's descriptor. Close may return a
// non-nil sticky persistence error after completing teardown, or a retryable
// cleanup error while teardown remains incomplete; this method distinguishes
// those cases. A database-managed handle reports its owner's completion state.
func (c *Collection) CloseCompleted() bool {
	if c == nil {
		return true
	}
	if c.owner != nil {
		return c.owner.CloseCompleted()
	}
	c.closeMu.Lock()
	done := c.closeDone
	c.closeMu.Unlock()
	return done
}

func validCollectionName(name string) bool {
	return collectionname.Valid(name)
}

func facadeError(err error) error {
	if err == nil || errors.Is(err, ErrClosed) {
		return err
	}
	if errors.Is(err, durable.ErrClosed) || errors.Is(err, durable.ErrDatabaseClosed) {
		return fmt.Errorf("%w: %w", ErrClosed, err)
	}
	if errors.Is(err, durable.ErrKeyTooLarge) {
		return fmt.Errorf("%w: %w", ErrKeyTooLarge, err)
	}
	if errors.Is(err, durable.ErrDocumentTooLarge) {
		return fmt.Errorf("%w: %w", ErrDocumentTooLarge, err)
	}
	if errors.Is(err, durable.ErrTxnTooLarge) ||
		errors.Is(err, durable.ErrBatchTooLarge) ||
		errors.Is(err, store.ErrTxnTooLarge) ||
		errors.Is(err, store.ErrBatchTooLarge) {
		return fmt.Errorf("%w: %w", ErrTxTooLarge, err)
	}
	return err
}

func (c *Collection) recordCommittedKey(key string) {
	if c == nil || c.owner == nil || key == "" {
		return
	}
	c.owner.recordClockKey(c.name, key)
}

// normalizeFacadeTxnLimits substitutes package defaults for any zero
// dimension. UpdateCollections itself stays fail-closed at zero; this layer
// owns the defaults.
func normalizeFacadeTxnLimits(limits durable.TxnLimits) durable.TxnLimits {
	defaults := defaultFacadeTxnLimits()
	if limits.MaxCollections == 0 {
		limits.MaxCollections = defaults.MaxCollections
	}
	if limits.MaxDocuments == 0 {
		limits.MaxDocuments = defaults.MaxDocuments
	}
	if limits.MaxBytes == 0 {
		limits.MaxBytes = defaults.MaxBytes
	}
	return limits
}

func defaultFacadeTxnLimits() durable.TxnLimits {
	// Pin the same numbers durable.Database.Update uses (defaultTxnLimits).
	const defaultBatchValueBytes = 16 << 20
	return durable.TxnLimits{
		MaxCollections: 16,
		MaxDocuments:   4 * store.MaxChunkDocuments,
		MaxBytes: 4 * (int64(store.MaxChunkDocuments)*int64(defaultMaxKeyBytes) +
			defaultBatchValueBytes),
	}
}

func (c *Collection) bounds(
	disk *durable.Collection,
) (maxKeyBytes, maxDocumentBytes int) {
	if disk != nil {
		// Open rehydrates these immutable limits from the file catalog when the
		// caller leaves them at zero. Reading them through the already-published
		// backend keeps lazy resolution race-free and binds this operation to the
		// same engine instance it is about to use.
		return disk.MaxKeyBytes(), disk.MaxDocumentBytes()
	}
	if c.owner != nil {
		return c.owner.maxKeyBytes, c.owner.maxDocumentBytes
	}
	return defaultMaxKeyBytes, defaultMaxDocumentBytes
}

func (c *Collection) admissionError() error {
	if c == nil {
		return ErrClosed
	}
	if c.initialErr != nil {
		return c.initialErr
	}
	if c.owner != nil {
		if c.owner.closed.Load() {
			return ErrClosed
		}
		return nil
	}
	if c.closed.Load() {
		return ErrClosed
	}
	return nil
}

func (c *Collection) storeOptions() store.Options {
	if c.owner != nil {
		return c.owner.engine.Collection
	}
	return c.validationOptions
}

// validateDocument proves every document property that is independent of a
// collection's current contents before a lazy handle is allowed to create its
// backing file. The ordinary schemaless path is the parser's concurrent,
// allocation-free validator. Explicit depth limits and schemas use the same
// structural-index builder and compiled-schema validator as the engines. A
// small fixed tape keeps common advanced documents on the stack; only a larger
// document needs temporary overflow storage, with no shared lock between
// writers.
func validateDocument(options store.Options, src []byte) error {
	if options.Schema == nil && options.IndexOptions.MaxDepth <= 0 {
		return vibejson.Validate(src)
	}
	var inline [64]vibejson.IndexEntry
	index, err := vibejson.BuildIndexOptions(src, inline[:], options.IndexOptions)
	if errors.Is(err, document.ErrIndexFull) {
		index, err = vibejson.BuildIndexOptions(
			src, make([]vibejson.IndexEntry, len(src)+2), options.IndexOptions,
		)
	}
	if err != nil {
		return err
	}
	return options.Schema.ValidateIndex(index)
}
