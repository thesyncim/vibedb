package durable

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/storeio"
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
// collection's snapshotGate read side, in a process-global handle order, and
// acquires one lease per collection while the whole set is held. The handle
// order remains stable even when separate catalogs give the same collections
// different names.
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
// still. Snapshot and multi-collection publication paths follow stable orders,
// preventing the wait cycle that unordered acquisition of several RWMutex
// gates could otherwise create.
//
// The cost lands entirely on the reader, exactly as on the heap side. Nothing
// is added to the write path: no shared gate to acquire per mutation, no atomic
// beyond the ones a commit already performs, and single-collection
// Collection.Snapshot is untouched.

var (
	// ErrCollectionName reports an empty, invalid UTF-8, or overlong collection
	// name. Valid logical names are encoded rather than treated as path elements.
	ErrCollectionName = errors.New("vibedb: invalid durable collection name")
	// ErrCollectionExists reports duplicate collection creation.
	ErrCollectionExists = errors.New("vibedb: durable collection already exists")
	// ErrDatabaseClosed reports use of a closed Database.
	ErrDatabaseClosed = errors.New("vibedb: durable database is closed")
	// ErrUnsupportedDatabaseLayout reports a recognizable collection filename
	// that is not the current reversible encoding. The project is unreleased:
	// recreate the database instead of relying on a compatibility migration.
	ErrUnsupportedDatabaseLayout = errors.New(
		"vibedb: unsupported durable database filename layout",
	)
)

const (
	collectionFilePrefix = collectionname.FilePrefix
	collectionFileSuffix = collectionname.PrimarySuffix
	// MaxCollectionNameBytes is the UTF-8 byte limit for a logical collection
	// name. The encoded primary plus its .rjournal suffix fits a portable
	// 255-byte filename component at this exact bound.
	MaxCollectionNameBytes = collectionname.MaxNameBytes
)

// A Database is a catalog of durable collections that share one directory,
// with a consistent multi-collection snapshot.
//
// Layout is one file per collection. Logical names are encoded as "c-" plus
// lowercase hexadecimal UTF-8 bytes plus ".vjc"; "orders", for example, is
// "c-6f7264657273.vjc". Its journal appends ".rjournal". This reversible,
// case-insensitive-safe mapping avoids separators, reserved devices, Unicode
// normalization, and trailing-dot/space aliases on every supported filesystem.
// The one-file choice is deliberate and its reasoning belongs here, because
// the alternative — several collections sharing one file — is the one a reader
// will wonder about.
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
// Cross-collection atomicity rides a sibling sidecar, not a shared primary
// file. Database.Update (and the caller-owned UpdateCollections primitive)
// prepares one conditional journal record per dirty target, then appends
// and syncs a single decision record in txn.vtm — that sync is the sole
// atomic commit point — before publishing under every snapshotGate held at
// once. Read cuts never observe a torn multi-collection commit. Standalone
// [Open] of a target that still holds an uncovered conditional fails
// closed; open the database directory instead. The one-file layout's cost
// remains: N open descriptors and N writer locks, and a K-target commit
// performs K+1 fsyncs.
//
// A Database owns the files it opened and closes them in Close. OpenDatabase
// stores a cleaned absolute directory with existing symlink components
// resolved. That directory must not be renamed or replaced before Close:
// recovery journals are intentionally paired by stable primary pathname, and
// txn.vtm is paired by the same directory.
type Database struct {
	dir      string
	fileMode os.FileMode

	mu          sync.RWMutex
	closed      bool
	closeDone   bool
	closeErr    error
	collections map[string]*databaseEntry

	// order is the collection set in name order. snapshotOrder contains the
	// same handles in their process-global gate order. Both are rebuilt on
	// catalog mutation so SnapshotInto remains allocation-free while results
	// and catalog iteration stay name ordered.
	order         []*databaseEntry
	snapshotOrder []*Collection
}

// A databaseEntry is one cataloged collection plus the file the Database opened
// for it. The file is kept because a Collection deliberately does not own its
// caller's *os.File lifetime, so something above it must.
type databaseEntry struct {
	name       string
	filename   string
	collection *Collection
	file       *os.File
	// deleting keeps a successfully closed entry in the map until both its
	// primary and journal removals have crossed their directory fences. It is
	// hidden from reads/snapshots but makes a failed Drop retryable by name.
	deleting       bool
	primaryRemoved bool
	closeDone      bool
	closeErr       error
}

// DatabaseOptions configure a Database itself, as opposed to the collections in
// it. Its zero value is usable.
type DatabaseOptions struct {
	// Options is the per-collection configuration applied to every collection
	// OpenDatabase discovers on disk. Collections created through
	// CreateCollection take their own. Durable geometry and schema presence are
	// frozen into each file and validated by Open. The index catalog is
	// rehydrated from the file; a non-nil Options.Indexes additionally asserts
	// its exact current value.
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
// costs K bounded recoveries and K open descriptors. When txn.vtm is present,
// OpenDatabase loads the decision log first, opens every collection with a
// target-binding resolver (so committed conditionals roll forward and
// undecided ones are presumed abort and consumed), then reconciles
// target presence and log-removal legality. Collection opens complete
// before the attached TxnLog accepts its first commit.
//
// Relative dir values are resolved once at entry, so a later process-wide
// working-directory change cannot retarget CreateCollection, DropCollection,
// or recovery-journal I/O. Every collection in the directory must accept
// options.Options. Durable
// collections validate frozen geometry and schema presence against those
// options. Index catalogs may differ when Options.Indexes is nil; a non-nil
// slice asserts the same catalog for every collection.
func OpenDatabase(dir string, options DatabaseOptions) (*Database, error) {
	if dir == "" {
		return nil, fmt.Errorf("vibedb: durable database requires a directory")
	}
	absoluteDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	dir = filepath.Clean(absoluteDir)
	if options.FileMode&^os.ModePerm != 0 || options.DirMode&^os.ModePerm != 0 {
		return nil, fmt.Errorf("vibedb: database file and directory modes may contain permission bits only")
	}
	fileMode := options.FileMode
	if fileMode == 0 {
		fileMode = 0o600
	}
	dirMode := options.DirMode
	if dirMode == 0 {
		dirMode = 0o700
	}
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, err
	}
	// Freeze relative paths and existing directory symlink components before
	// retaining any primary name. Collection.Open derives its journal from the
	// descriptor name, so every owned descriptor must carry the same stable,
	// absolute namespace for the lifetime of the database.
	dir, err = filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}
	dir = filepath.Clean(dir)
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	items, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return nil, err
	}
	if err := validateDatabaseLayout(root, items); err != nil {
		return nil, err
	}
	// A format-0 certificate makes the special fixed-membership opener the only
	// recovery authority. Fail before txn.vtm or any participant journal can be
	// replayed, reconciled, or folded by ordinary database recovery.
	if err := rejectCheckpointGroupCertificate(root); err != nil {
		return nil, err
	}
	if checkpointGroupGenericDatabaseAfterPrecheckHook != nil {
		checkpointGroupGenericDatabaseAfterPrecheckHook()
	}
	releaseNamespace, err := acquireCheckpointGroupGenericCatalogNamespace(root)
	if err != nil {
		return nil, err
	}
	defer releaseNamespace()
	txnRecovery, err := loadDatabaseTxnRecovery(dir, TxnLogOptions{})
	if err != nil {
		return nil, err
	}
	d := &Database{
		dir: dir, fileMode: fileMode,
		collections: make(map[string]*databaseEntry),
	}
	opened := make([]*Collection, 0, len(items))
	for _, item := range items {
		base := item.Name()
		if isTxnMarkerFilename(base) {
			continue
		}
		name, encoded := collectionname.Decode(base)
		if !encoded {
			if strings.HasSuffix(base, collectionFileSuffix) {
				return nil, abortDatabaseOpen(d, txnRecovery, fmt.Errorf(
					"%w: %q; recreate the unreleased database",
					ErrUnsupportedDatabaseLayout, base,
				))
			}
			continue
		}
		file, openErr := openCatalogPrimary(root, base)
		if openErr != nil {
			return nil, abortDatabaseOpen(d, txnRecovery, fmt.Errorf(
				"vibedb: durable collection %q: %w", name, openErr,
			))
		}
		cfg := collectionOpenConfig{checkpointGroupNamespaceHeld: true}
		cfg.deferJournalReplay = true
		if txnRecovery.absent {
			cfg.absentLog = true
		} else {
			cfg.decisions = txnRecovery.decisions
		}
		collection, openErr := openCollection(file, options.Options, cfg)
		if openErr != nil {
			_ = file.Close()
			return nil, abortDatabaseOpen(d, txnRecovery, fmt.Errorf(
				"vibedb: durable collection %q: %w", name, openErr,
			))
		}
		d.collections[name] = &databaseEntry{
			name: name, filename: base, collection: collection, file: file,
		}
		opened = append(opened, collection)
	}
	if err := validateOrphanRecoveryJournals(
		root, items, txnRecovery.decisions,
	); err != nil {
		return nil, abortDatabaseOpen(d, txnRecovery, err)
	}
	if err := preflightAndReplayDatabaseTxnRecovery(
		txnRecovery, opened,
	); err != nil {
		return nil, abortDatabaseOpen(d, txnRecovery, err)
	}
	if err := reconcileDatabaseTxnAfterOpens(txnRecovery, opened); err != nil {
		return nil, abortDatabaseOpen(d, txnRecovery, err)
	}
	if err := removeOrphanRecoveryJournals(dir, items); err != nil {
		return nil, abortDatabaseOpen(d, txnRecovery, err)
	}
	// Attach only when txn.vtm still exists after reconciliation. Lazy mint on
	// the first Database.Update stands otherwise. Collection opens (and their
	// stray consumption) completed above before any commit can run.
	if !txnRecovery.absent && txnRecovery.log != nil && txnRecovery.log.marker != nil {
		attachDatabaseTxnLog(d, txnRecovery.log)
	} else if txnRecovery.log != nil {
		_ = txnRecovery.log.Close()
	}
	d.reorder()
	return d, nil
}

// abortDatabaseOpen releases construction-owned resources without invoking
// Collection.Close. A deferred-open collection may still hold unreplayed
// journal evidence; the public close path is allowed to checkpoint and recycle
// that evidence and therefore must never run before catalog recovery succeeds.
func abortDatabaseOpen(
	d *Database, recovery *databaseTxnRecovery, cause error,
) error {
	cleanupErr := error(nil)
	if d != nil {
		for _, entry := range d.collections {
			if entry == nil {
				continue
			}
			if entry.collection != nil {
				cleanupErr = errors.Join(
					cleanupErr, entry.collection.closeResources(),
				)
			}
			if entry.file != nil {
				cleanupErr = errors.Join(cleanupErr, entry.file.Close())
			}
		}
		d.collections = nil
		d.order = nil
		d.snapshotOrder = nil
	}
	if recovery != nil && recovery.log != nil {
		cleanupErr = errors.Join(cleanupErr, recovery.log.Close())
		recovery.log = nil
	}
	return errors.Join(cause, cleanupErr)
}

// validateDatabaseLayout runs before orphan cleanup so an aliased primary can
// never make its live canonical journal look orphaned on a case-insensitive
// filesystem. Recognizable primary and journal names are engine namespace, not
// ignorable sidecars: every such entry must use exact lowercase spelling and
// be a regular non-symlink file.
func validateDatabaseLayout(root *os.Root, items []fs.DirEntry) error {
	for _, item := range items {
		base := item.Name()
		if collectionname.PrimaryCaseAlias(base) ||
			collectionname.JournalCaseAlias(base) {
			return fmt.Errorf(
				"%w: non-canonical case alias %q",
				ErrUnsupportedDatabaseLayout, base,
			)
		}
		if isTxnMarkerFilename(base) {
			// txn.vtm is a reserved sidecar, never a decodable collection
			// filename (no c- prefix). Require a regular non-symlink file.
			if err := validateTxnMarkerLayout(root, base); err != nil {
				return err
			}
			continue
		}
		_, primary := collectionname.Decode(base)
		_, journal := collectionname.DecodeJournal(base)
		if !primary && !journal {
			if strings.HasSuffix(strings.ToLower(base), collectionFileSuffix) {
				return fmt.Errorf(
					"%w: %q; recreate the unreleased database",
					ErrUnsupportedDatabaseLayout, base,
				)
			}
			continue
		}
		info, err := root.Lstat(base)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf(
				"%w: canonical database entry %q is not a regular non-symlink file",
				ErrUnsupportedDatabaseLayout, base,
			)
		}
	}
	return nil
}

// openCatalogPrimary opens one already-discovered collection relative to the
// directory descriptor held by root. Root confines every symlink traversal to
// the database directory; the two Lstat checks additionally make symbolic
// links and non-regular files invalid catalog entries rather than aliases for
// another primary. Comparing both pathname observations with the opened
// descriptor closes the replace-between-check-and-open race.
func openCatalogPrimary(root *os.Root, base string) (*os.File, error) {
	before, err := root.Lstat(base)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, fmt.Errorf(
			"%w: canonical collection %q is not a regular non-symlink file",
			ErrUnsupportedDatabaseLayout, base,
		)
	}
	file, err := root.OpenFile(base, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	descriptor, descriptorErr := file.Stat()
	after, pathErr := root.Lstat(base)
	if descriptorErr != nil || pathErr != nil || !descriptor.Mode().IsRegular() ||
		!after.Mode().IsRegular() || !os.SameFile(before, after) ||
		!os.SameFile(descriptor, after) {
		_ = file.Close()
		if descriptorErr != nil {
			return nil, descriptorErr
		}
		if pathErr != nil {
			return nil, pathErr
		}
		return nil, fmt.Errorf(
			"%w: canonical collection %q changed while it was opened",
			ErrUnsupportedDatabaseLayout, base,
		)
	}
	return file, nil
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
	if log := lookupDatabaseTxnLog(d); log != nil && log.checkpointGroupMutationFenced() {
		return nil, ErrCheckpointGroupOwned
	}
	if _, exists := d.collections[name]; exists {
		return nil, ErrCollectionExists
	}
	filename, _ := collectionname.Encode(name)
	path := filepath.Join(d.dir, filename)
	// O_EXCL is what makes the catalog check above authoritative rather than
	// advisory: a file left behind by a previous process is refused here rather
	// than silently adopted and then rejected by Create's empty-file rule under
	// a much less obvious message.
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, d.fileMode)
	if err != nil {
		if os.IsExist(err) {
			return nil, ErrCollectionExists
		}
		return nil, err
	}
	collection, err := createCollection(file, options)
	if err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if log := lookupDatabaseTxnLog(d); log != nil {
		if err := log.registerCollectionUnlessCheckpointGroup(collection); err != nil {
			_ = collection.closeResources()
			_ = file.Close()
			_ = os.Remove(path)
			return nil, err
		}
	}
	entry := &databaseEntry{
		name: strings.Clone(name), filename: filename,
		collection: collection, file: file,
	}
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
	if d.closed {
		d.mu.RUnlock()
		return nil, false
	}
	entry, ok := d.collections[name]
	if ok && entry.deleting {
		ok = false
	}
	d.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return entry.collection, true
}

// DropCollection closes name and removes its primary+journal pair. The primary
// is removed and its parent directory synchronized before the orphan journal
// is removed and synchronized; a crash can therefore leave an ignorable orphan
// journal, never a live primary missing recovery state. Dropping a name that is
// not cataloged is not an error.
//
// When the collection still holds conditional journal records or an
// undischarged decision names its StoreID, DropCollection first folds the
// journal past those records and appends a target-retired record to
// txn.vtm, then performs today's ordered primary→journal deletion. A crash
// around that barrier leaves a consistent reopen image either way.
//
// A cleanup failure leaves a hidden pending-delete entry that reserves name;
// call DropCollection(name) again after repairing the filesystem. Collection,
// Names, All, Len, and Snapshot exclude pending deletes, while
// CreateCollection returns ErrCollectionExists until cleanup completes.
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
	defer d.mu.Unlock()
	if d.closed {
		return ErrDatabaseClosed
	}
	if log := lookupDatabaseTxnLog(d); log != nil && log.checkpointGroupMutationFenced() {
		return ErrCheckpointGroupOwned
	}
	entry, ok := d.collections[name]
	if !ok {
		return nil
	}
	if !entry.deleting {
		if err := d.retireCollectionBeforeDrop(entry); err != nil {
			return err
		}
		entry.deleting = true
		d.reorder()
	}
	closeErr := entry.close()
	if !entry.closeDone {
		return closeErr
	}
	primary := filepath.Join(d.dir, entry.filename)
	if !entry.primaryRemoved {
		if err := os.Remove(primary); err != nil && !errors.Is(err, os.ErrNotExist) {
			return errors.Join(closeErr, err)
		}
		// Persist logical absence before touching the recovery journal. Reversing
		// this order creates a crash window with a live primary whose named journal
		// is missing, which Open must fail closed.
		if err := syncRecoveryJournalParent(primary); err != nil {
			return errors.Join(closeErr,
				fmt.Errorf("vibedb: persist dropped collection: %w", err))
		}
		entry.primaryRemoved = true
	}
	journal := RecoveryJournalPath(primary)
	if err := os.Remove(journal); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errors.Join(closeErr, err)
	}
	if err := syncRecoveryJournalParent(journal); err != nil {
		return errors.Join(closeErr,
			fmt.Errorf("vibedb: persist dropped collection journal: %w", err))
	}
	if log := lookupDatabaseTxnLog(d); log != nil {
		log.unregisterCollection(entry.collection)
	}
	delete(d.collections, name)
	d.reorder()
	return closeErr
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
// It joins completed entries' cached terminal errors. A transient child-close
// error is joined into that attempt's result while ownership is retained; call
// Close again after releasing the blocking reader or repairing the writer
// unlock. Once every entry completes, repeated calls return the exact cached
// terminal result.
func (d *Database) Close() error {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closeDone {
		return d.closeErr
	}
	if log := lookupDatabaseTxnLog(d); log != nil && log.checkpointGroupOwned() {
		return ErrCheckpointGroupOwned
	}
	if !d.closed {
		d.closed = true
		// Admission and catalog visibility end at the first attempt, but entry
		// ownership does not. Unfinished entries remain in collections so a
		// later Close can resume their low-level teardown state machine.
		d.order = nil
		d.snapshotOrder = nil
		// Detach and close the decision log on the first attempt so a closed
		// Database cannot leak the txn.vtm fd or leave a stale registry entry
		// for a reopened Database in the same process.
		if log := detachDatabaseTxnLog(d); log != nil {
			d.rememberCloseError(log.Close())
		}
	}
	var retryable error
	for _, entry := range d.collections {
		err := entry.close()
		if !entry.closeDone {
			retryable = errors.Join(retryable, err)
			continue
		}
		d.rememberCloseError(err)
	}
	if retryable != nil {
		return errors.Join(d.closeErr, retryable)
	}
	d.collections = nil
	d.closeDone = true
	return d.closeErr
}

// CloseCompleted reports whether Close has released every collection engine,
// owner descriptor, and writer lock. The first Close attempt rejects new
// catalog operations immediately; this remains false while any child teardown
// is retryable.
func (d *Database) CloseCompleted() bool {
	if d == nil {
		return true
	}
	d.mu.RLock()
	done := d.closeDone
	d.mu.RUnlock()
	return done
}

func (d *Database) rememberCloseError(err error) {
	if err == nil || errors.Is(d.closeErr, err) {
		return
	}
	if d.closeErr == nil {
		d.closeErr = err
		return
	}
	d.closeErr = errors.Join(d.closeErr, err)
}

func (e *databaseEntry) close() error {
	if e.closeDone {
		return e.closeErr
	}
	collectionErr := e.collection.Close()
	if !e.collection.CloseCompleted() {
		return collectionErr
	}
	// os.File.Close consumes the descriptor even when it reports an error. Once
	// the engine has completed teardown there is no useful or safe retry of this
	// descriptor close, so cache the joined terminal result and let catalog Drop
	// continue removing the paired namespace.
	e.closeErr = errors.Join(collectionErr, e.file.Close())
	e.closeDone = true
	return e.closeErr
}

// removeOrphanRecoveryJournals completes the only crash residue permitted by
// primary-first Drop: a canonical journal whose encoded primary is absent. It
// never scans collection contents and never touches arbitrary *.rjournal
// files. All removals share one parent-directory durability fence.
func validateOrphanRecoveryJournals(
	root *os.Root, items []os.DirEntry, decisions *storeio.TxnDecisions,
) error {
	present := make(map[string]struct{}, len(items))
	for _, item := range items {
		present[item.Name()] = struct{}{}
	}
	for _, item := range items {
		base := item.Name()
		if _, canonical := collectionname.DecodeJournal(base); !canonical {
			continue
		}
		primary := strings.TrimSuffix(base, collectionname.JournalSuffix)
		if _, exists := present[primary]; exists {
			continue
		}
		file, err := root.OpenFile(base, os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		inspection, err := storeio.InspectRecoveryJournal(file)
		if err != nil {
			return errors.Join(err, file.Close())
		}
		holds := false
		replayErr := inspection.Replay(
			inspection.BaseGeneration(),
			func(rec storeio.RecoveryRecord) error {
				if rec.Kind == storeio.RecoveryRecordKindConditionalBatch {
					holds = true
				}
				return nil
			},
		)
		header := inspection.Header()
		closeErr := inspection.Close()
		if replayErr != nil || closeErr != nil {
			return errors.Join(replayErr, closeErr)
		}
		if holds && (decisions == nil || !decisions.Retired(header.StoreID)) {
			return fmt.Errorf(
				"%w: orphan journal %q for store %x retains a conditional prepare without participant retirement",
				ErrTransactionCollectionMissing, base, header.StoreID,
			)
		}
	}
	return nil
}

func removeOrphanRecoveryJournals(dir string, items []os.DirEntry) error {
	present := make(map[string]struct{}, len(items))
	for _, item := range items {
		present[item.Name()] = struct{}{}
	}
	var removedPath string
	for _, item := range items {
		base := item.Name()
		if _, canonical := collectionname.DecodeJournal(base); !canonical {
			continue
		}
		primary := strings.TrimSuffix(base, collectionname.JournalSuffix)
		if _, exists := present[primary]; exists {
			continue
		}
		path := filepath.Join(dir, base)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("vibedb: remove orphan recovery journal %q: %w", base, err)
		}
		removedPath = path
	}
	if removedPath != "" {
		if err := syncRecoveryJournalParent(removedPath); err != nil {
			return fmt.Errorf("vibedb: persist orphan recovery-journal cleanup: %w", err)
		}
	}
	return nil
}

// reorder rebuilds the name-ordered catalog view and global snapshot-gate
// order. The caller holds the write lock.
func (d *Database) reorder() {
	d.order = d.order[:0]
	d.snapshotOrder = d.snapshotOrder[:0]
	for _, entry := range d.collections {
		if entry.deleting {
			continue
		}
		d.order = append(d.order, entry)
		d.snapshotOrder = append(d.snapshotOrder, entry.collection)
	}
	slices.SortFunc(d.order, func(a, b *databaseEntry) int {
		return strings.Compare(a.name, b.name)
	})
	sortCollectionSnapshotOrder(d.snapshotOrder)
}

// validCollectionName reports whether name has one portable reversible
// filename encoding. Logical names never become path elements directly.
func validCollectionName(name string) bool {
	return collectionname.Valid(name)
}
