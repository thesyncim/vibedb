package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"runtime"
	"sync"
	"sync/atomic"

	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

const (
	storePageDBMaxDirectoryDepth = 6
	storePageDBKeyLeafCapacity   = (int(storePageQuantum) - storeio.PageHeaderSize - storeio.PageTrailerSize -
		storeio.PageKeyDirectoryPayloadHeaderSize) / storeio.PageKeyLeafEntrySize
	storePageDBKeyBranchCapacity = (int(storePageQuantum) - storeio.PageHeaderSize - storeio.PageTrailerSize -
		storeio.PageKeyDirectoryPayloadHeaderSize) / storeio.PageKeyBranchEntrySize
	// A worst-case insert can split every key-tree level: two physical
	// versions per level plus a new root, alongside one document, six radix
	// nodes, and the state page. Buffers remain fixed for the database life.
	storePageDBMaxCommitPages = storePageDBMaxDirectoryDepth + 2*storePageKeyMaxDepth + 3
	storePageDBDefaultBuffers = storePageDBMaxCommitPages + 2
)

var (
	// ErrStorePageInvalidJSON reports a Put value that is not one complete JSON
	// document. Validation happens before any page or generation is published.
	ErrStorePageInvalidJSON = errors.New("vibejson: invalid Store page JSON")
	// ErrStorePageWriterLocked reports a second mutable opener for one file.
	// Immutable StorePageReader instances may coexist with the single writer.
	ErrStorePageWriterLocked = storeio.ErrWriterLocked
	// ErrStorePageWriterLockUnsupported reports a platform where the package
	// cannot enforce its single-writer safety invariant.
	ErrStorePageWriterLockUnsupported = storeio.ErrWriterLockUnsupported
)

// StorePageCommitBackend selects the durable writer used by StorePageDB.
// Auto prefers the pure-Go Linux io_uring path and falls back only when the
// host or filesystem explicitly cannot provide it.
type StorePageCommitBackend uint8

const (
	StorePageCommitAuto StorePageCommitBackend = iota
	StorePageCommitPortable
	StorePageCommitIOUring
)

func (b StorePageCommitBackend) String() string {
	switch b {
	case StorePageCommitAuto:
		return "auto"
	case StorePageCommitPortable:
		return "portable"
	case StorePageCommitIOUring:
		return "io_uring"
	default:
		return "unknown"
	}
}

func (b StorePageCommitBackend) internal() storeio.Backend {
	switch b {
	case StorePageCommitPortable:
		return storeio.BackendPortable
	case StorePageCommitIOUring:
		return storeio.BackendIOUring
	default:
		return storeio.BackendAuto
	}
}

func storePageCommitBackend(backend storeio.Backend) StorePageCommitBackend {
	switch backend {
	case storeio.BackendPortable:
		return StorePageCommitPortable
	case storeio.BackendIOUring:
		return StorePageCommitIOUring
	default:
		return StorePageCommitAuto
	}
}

// StorePageDBOptions fixes both bounded read residency and bounded writer
// staging. WriterBuffers zero selects 43 buffers: enough for one document,
// six packed-radix nodes, a split at every level of the sixteen-level key
// tree, a new key root, the state page, and two spares.
// Every writer buffer is max(MaxDocumentPageBytes, host page size), remains
// reusable for the life of the database, lives outside the Go heap on
// supported Unix systems, and is registered when the native backend is used.
type StorePageDBOptions struct {
	Open             StorePageOpenOptions
	CommitBackend    StorePageCommitBackend
	WriterBuffers    int
	WriterQueueDepth int
}

func (o StorePageDBOptions) normalized() (StorePageDBOptions, error) {
	open, err := o.Open.normalized()
	if err != nil {
		return StorePageDBOptions{}, err
	}
	o.Open = open
	if o.CommitBackend > StorePageCommitIOUring {
		return StorePageDBOptions{}, fmt.Errorf("vibejson: invalid Store page commit backend %d", o.CommitBackend)
	}
	if o.WriterBuffers == 0 {
		o.WriterBuffers = storePageDBDefaultBuffers
	}
	if o.WriterBuffers <= storePageDBMaxCommitPages {
		return StorePageDBOptions{}, fmt.Errorf("vibejson: Store page writer needs at least %d buffers", storePageDBMaxCommitPages+1)
	}
	if o.WriterQueueDepth == 0 {
		o.WriterQueueDepth = o.WriterBuffers
	}
	if o.WriterQueueDepth < o.WriterBuffers {
		return StorePageDBOptions{}, fmt.Errorf("vibejson: Store page writer queue depth is smaller than its buffer set")
	}
	return o, nil
}

type storePageDBDirectoryNode struct {
	header    storeio.ChunkDirectoryHeader
	refs      [64]storeio.PageRef
	count     int
	rank      int
	newBitmap uint64
	newRef    storeio.PageRef
	hadChild  bool
}

type storePageDBKeyLeaf struct {
	header     storeio.PageKeyDirectoryHeader
	entries    [storePageDBKeyLeafCapacity + 1]storeio.PageKeyLocation
	count      int
	rank       int
	newCount   int
	newRef     storeio.PageRef
	rightRef   storeio.PageRef
	rightCount int
}

type storePageDBKeyBranchNode struct {
	header     storeio.PageKeyDirectoryHeader
	entries    [storePageDBKeyBranchCapacity + 1]storeio.PageKeyBranch
	count      int
	rank       int
	newCount   int
	newRef     storeio.PageRef
	rightRef   storeio.PageRef
	rightCount int
}

type storePageDBPublishedView struct {
	root    storeio.StateRoot
	fileEnd uint64
}

type storePageDBPublishedSlot struct {
	view storePageDBPublishedView
}

// storePageDBPublished is a two-slot, pointer-free RCU root. Readers enter the
// epoch before loading current and leave immediately after the value copy. A
// writer reuses only the inactive slot and waits for that sub-microsecond
// copy epoch to quiesce; readers never wait for page construction or storage.
type storePageDBPublished struct {
	current atomic.Uint32
	readers atomic.Uint64
	slots   [2]storePageDBPublishedSlot
}

func (p *storePageDBPublished) store(root storeio.StateRoot, fileEnd uint64) {
	next := p.current.Load() ^ 1
	for p.readers.Load() != 0 {
		runtime.Gosched()
	}
	p.slots[next].view = storePageDBPublishedView{
		root: root, fileEnd: fileEnd,
	}
	p.current.Store(next)
}

func (p *storePageDBPublished) load() storePageDBPublishedView {
	p.readers.Add(1)
	view := p.slots[p.current.Load()].view
	p.readers.Add(^uint64(0))
	return view
}

func (p *storePageDBPublished) loadLookup() (uint64, storeio.PageRef, storeio.PageRef) {
	p.readers.Add(1)
	view := &p.slots[p.current.Load()].view
	documents := view.root.DocumentCount
	keyRoot, chunkRoot := view.root.KeyDirectory, view.root.ChunkDirectory
	p.readers.Add(^uint64(0))
	return documents, keyRoot, chunkRoot
}

// StorePageDB is a bounded-residency, crash-consistent mutable view of a page
// file created by Collection.WritePageFile. Put inserts or replaces a stable slot;
// Delete removes one. Every mutation appends immutable pages, passes a data
// barrier, and publishes the alternate superblock before returning.
// Applications never call a separate persistence method after success.
//
// One writer is serialized. Readers take a pointer-free atomic root snapshot,
// then traverse immutable pages without locking. A reader that copied the old
// root may finish concurrently with a commit while later readers see the new
// generation. StorePageDB must not be copied after first use.
type StorePageDB struct {
	writeMu   sync.Mutex
	root      storeio.StateRoot // writeMu only
	fileEnd   uint64            // writeMu only
	published storePageDBPublished
	storeID   [16]byte

	pages     atomic.Pointer[storeio.PageFile]
	file      *os.File
	committer *storeio.Committer
	options   StorePageDBOptions

	rows    [64]storeio.DocumentRecord
	path    [storePageDBMaxDirectoryDepth]storePageDBDirectoryNode
	keyLeaf storePageDBKeyLeaf
	keyPath [storePageKeyMaxDepth - 1]storePageDBKeyBranchNode

	parseScratch []vibejson.IndexEntry
}

// OpenStorePageDB recovers the newest valid generation, truncates any
// unreachable crash tail, and starts the bounded automatic committer. The
// file must currently contain no secondary-index root; unsupported metadata
// fails closed instead of becoming silently stale.
func OpenStorePageDB(path string, options StorePageDBOptions) (*StorePageDB, error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err := storeio.LockWriter(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	closeWriter := func(primary error) error {
		return errors.Join(primary, storeio.UnlockWriter(file), file.Close())
	}
	var scratch [storePageQuantum]byte
	super, root, rootSlot, fallbackGeneration, err := storeio.RecoverStateRootWithFallback(
		file, storePageQuantum, scratch[:],
	)
	if err != nil {
		return nil, closeWriter(storePageReadError(err))
	}
	catalog, err := openFilePageCatalog(
		file, root, super.FileEnd, scratch[:],
	)
	if err == nil {
		options.Open, err = resolveStorePageOpenOptions(
			options.Open, root, catalog,
		)
	}
	if err == nil {
		options, err = options.normalized()
	}
	if err != nil {
		return nil, closeWriter(storePageReadError(err))
	}
	// A failed pre-root commit can leave valid-looking bytes beyond FileEnd.
	// They are unreachable by definition and must be overwritten, not allowed
	// to turn the append cursor into a leak after recovery.
	if err := file.Truncate(int64(super.FileEnd)); err != nil {
		return nil, closeWriter(err)
	}

	db := &StorePageDB{
		root: root, fileEnd: super.FileEnd, storeID: root.StoreID, file: file, options: options,
	}
	db.published.store(root, super.FileEnd)
	pages, err := openStorePageFile(path, options.Open, root.StoreID, db.snapshot)
	if err != nil {
		return nil, closeWriter(err)
	}
	committer, err := storeio.NewCommitter(file, storeio.DeviceOptions{
		Backend: options.CommitBackend.internal(), BufferCount: options.WriterBuffers,
		BufferSize: max(int(options.Open.MaxDocumentPageBytes), os.Getpagesize()), QueueDepth: options.WriterQueueDepth,
		SingleIssuer: true,
	}, storeio.CommitterOptions{
		QueueSlots: 2, MaxPagesPerBatch: storePageDBMaxCommitPages, GroupLimit: 2,
	})
	if err != nil {
		_ = pages.Close()
		return nil, closeWriter(err)
	}
	db.committer = committer
	if err := db.committer.InitializeRecovery(
		root.Generation, rootSlot, fallbackGeneration,
	); err != nil {
		_ = db.committer.Close()
		_ = pages.Close()
		return nil, closeWriter(err)
	}
	db.pages.Store(pages)
	return db, nil
}

func (db *StorePageDB) snapshot() (storeio.StateRoot, uint64) {
	if db == nil {
		return storeio.StateRoot{}, 0
	}
	view := db.published.load()
	return view.root, view.fileEnd
}

func (db *StorePageDB) publish(root storeio.StateRoot, fileEnd uint64) {
	db.root, db.fileEnd = root, fileEnd
	db.published.store(root, fileEnd)
}

// StorePageDBKey caches only the deterministic persistent hash. Unlike an
// immutable StorePageKey it deliberately does not cache a physical document
// reference, which could name an older generation after Put or Delete.
type StorePageDBKey struct {
	key     string
	storeID [16]byte
	hash    uint64
}

// CompileKey prepares a repeated mutable point lookup without touching
// storage. The returned value holds the caller's key string; keep that string
// unchanged while the compiled key is in use.
func (db *StorePageDB) CompileKey(key string) StorePageDBKey {
	if db == nil {
		return StorePageDBKey{key: key}
	}
	return StorePageDBKey{key: key, storeID: db.storeID, hash: storeio.KeyHash(db.storeID, key)}
}

// AppendRaw appends the exact stored JSON to caller-owned dst. With adequate
// dst capacity, resident pages, and a warm cache it allocates no Go memory.
func (db *StorePageDB) AppendRaw(dst []byte, key string) ([]byte, bool, error) {
	return db.AppendRawKey(dst, db.CompileKey(key))
}

// AppendRawKey is AppendRaw through a reusable generation-safe compiled key.
func (db *StorePageDB) AppendRawKey(dst []byte, key StorePageDBKey) ([]byte, bool, error) {
	if db == nil {
		return dst, false, nil
	}
	pages := db.pages.Load()
	if pages == nil {
		return dst, false, ErrStorePageClosed
	}
	documents, keyRoot, chunkRoot := db.published.loadLookup()
	if documents == 0 {
		return dst, false, nil
	}
	if key.storeID != db.storeID {
		key.storeID = db.storeID
		key.hash = storeio.KeyHash(db.storeID, key.key)
	}
	compiled := StorePageKey{key: key.key, storeID: key.storeID, hash: key.hash}
	value, ok, err := lookupStorePageKey(pages, keyRoot, chunkRoot, compiled, nil)
	if err != nil || !ok {
		return dst, ok, err
	}
	dst = append(dst, value.Bytes()...)
	if err := value.Close(); err != nil {
		return dst, false, err
	}
	return dst, true, nil
}

// Put validates src and durably inserts or replaces key. New keys reuse the
// first stable-slot hole at or above the persistent free-chunk hint before
// extending ChunkHighWater.
func (db *StorePageDB) Put(key string, src []byte) (created bool, err error) {
	if db == nil {
		return false, ErrStorePageClosed
	}
	if db.options.Open.Schema == nil && !vibejson.Valid(src) {
		return false, ErrStorePageInvalidJSON
	}
	return db.mutate(key, src, false)
}

// Delete durably removes key and reports whether it existed. The rewritten
// document page contains only live rows, and empty radix nodes disappear from
// the new generation; no document tombstone or scan-time compaction remains.
func (db *StorePageDB) Delete(key string) (deleted bool, err error) {
	_, err = db.mutate(key, nil, true)
	if errors.Is(err, errStorePageKeyMissing) {
		return false, nil
	}
	return err == nil, err
}

var errStorePageKeyMissing = errors.New("vibejson: Store page key is missing")

func (db *StorePageDB) mutate(key string, src []byte, deleting bool) (bool, error) {
	if db == nil {
		return false, ErrStorePageClosed
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	pages := db.pages.Load()
	if pages == nil {
		return false, ErrStorePageClosed
	}
	if !deleting && db.options.Open.Schema != nil {
		if err := db.validateDocument(src); err != nil {
			return false, err
		}
	}
	root, fileEnd := db.root, db.fileEnd
	compiled := StorePageKey{key: key, storeID: root.StoreID, hash: storeio.KeyHash(root.StoreID, key)}
	value, found, err := lookupStorePageKey(pages, root.KeyDirectory, root.ChunkDirectory, compiled, &compiled)
	if err != nil {
		return false, err
	}
	if !found {
		if deleting {
			return false, errStorePageKeyMissing
		}
		err := db.insertLocked(pages, root, fileEnd, key, src, compiled.hash)
		return err == nil, err
	}
	if !deleting && bytes.Equal(value.Bytes(), src) {
		return false, value.Close()
	}

	doc := storeio.AdmittedDocumentPage(value.lease.Bytes())
	if doc.Header().ChunkID != compiled.chunk {
		_ = value.Close()
		return false, corruptStorePage("mutable document identity", storeio.ErrDocumentPageCorrupt)
	}
	keyDepth, rewriteKey := 0, false
	if deleting {
		keyDepth, rewriteKey, err = db.loadKeyDeletePath(pages, root, compiled.hash, storeio.PageKeyLocation{
			Hash: compiled.hash, Chunk: compiled.chunk, Slot: compiled.slot,
		})
		if err != nil {
			_ = value.Close()
			return false, err
		}
	}
	depth, err := db.loadDirectoryPath(pages, root, compiled.chunk, compiled.document)
	if err != nil {
		_ = value.Close()
		return false, err
	}
	rowCount, live, required, err := db.buildDocumentRows(doc, compiled.slot, src, deleting)
	if err != nil {
		_ = value.Close()
		return false, err
	}
	docSize := uint32(0)
	if rowCount != 0 {
		var ok bool
		docSize, ok = storePageExtent(required, db.options.Open.MaxDocumentPageBytes)
		if !ok {
			_ = value.Close()
			clear(db.rows[:])
			return false, fmt.Errorf("%w: chunk=%d bytes=%d max=%d", ErrStoreDocumentPageTooLarge,
				compiled.chunk, required, db.options.Open.MaxDocumentPageBytes)
		}
	}
	err = db.commitMutation(root, fileEnd, compiled.chunk, docSize, rowCount, depth,
		keyDepth, rewriteKey, live, deleting, &value)
	clear(db.rows[:])
	var closeErr error
	if value.raw != nil {
		closeErr = value.Close()
	}
	return false, errors.Join(err, closeErr)
}

func (db *StorePageDB) validateDocument(src []byte) error {
	if len(src) > int(db.options.Open.MaxDocumentPageBytes) {
		return ErrStoreDocumentPageTooLarge
	}
	estimate := max(len(src)/8+8, 8)
	if cap(db.parseScratch) < estimate {
		db.parseScratch = make([]vibejson.IndexEntry, estimate)
	}
	indexOptions := document.IndexOptions{
		MaxDepth: int(db.root.IndexMaxDepth),
		HashKeys: db.root.Options&storeio.StateOptionHashKeys != 0,
	}
	for {
		index, err := vibejson.BuildIndexOptions(
			src, db.parseScratch[:cap(db.parseScratch)], indexOptions,
		)
		if err != document.ErrIndexFull {
			if err != nil {
				return fmt.Errorf("%w: %v", ErrStorePageInvalidJSON, err)
			}
			return db.options.Open.Schema.ValidateIndex(index)
		}
		limit := int(db.options.Open.MaxDocumentPageBytes)
		if cap(db.parseScratch) >= limit {
			return ErrStoreDocumentPageTooLarge
		}
		db.parseScratch = make(
			[]vibejson.IndexEntry, min(cap(db.parseScratch)*2, limit),
		)
	}
}

func (db *StorePageDB) buildDocumentRows(doc storeio.DocumentPageView, slot uint8, src []byte,
	deleting bool) (count int, live uint64, required uint64, err error) {
	live = doc.Header().Live
	if deleting {
		live &^= uint64(1) << slot
	}
	required = uint64(storeio.PageHeaderSize + storeio.PageTrailerSize +
		storeio.DocumentPagePayloadHeaderSize)
	found := false
	for rank := 0; rank < doc.Len(); rank++ {
		row, ok := doc.RecordAt(rank)
		if !ok {
			return 0, 0, 0, corruptStorePage("mutable document record", storeio.ErrDocumentPageCorrupt)
		}
		if row.Slot == slot {
			found = true
			if deleting {
				continue
			}
			row.JSON = src
		}
		db.rows[count] = row
		count++
		required += uint64(storeio.DocumentPageRecordSize + len(row.Key) + len(row.JSON))
	}
	if !found {
		return 0, 0, 0, corruptStorePage("mutable stable slot", storeio.ErrDocumentPageCorrupt)
	}
	return count, live, required, nil
}

func (db *StorePageDB) loadDirectoryPath(pages *storeio.PageFile, root storeio.StateRoot,
	chunkID uint32, document storeio.PageRef) (int, error) {
	ref := root.ChunkDirectory
	expectedShift := uint8(0)
	haveExpectedShift := false
	for depth := 0; depth < len(db.path); depth++ {
		if ref == (storeio.PageRef{}) {
			return 0, corruptStorePage("mutable missing chunk path", storeio.ErrChunkDirectoryCorrupt)
		}
		lease, err := pages.Cache().Pin(ref)
		if err != nil {
			return 0, storePageReadError(err)
		}
		view := storeio.AdmittedChunkDirectoryPage(lease.Bytes())
		header := view.Header()
		if haveExpectedShift && header.Shift != expectedShift {
			_ = lease.Close()
			return 0, corruptStorePage("mutable chunk-directory level", storeio.ErrChunkDirectoryCorrupt)
		}
		lane := uint8(chunkID >> header.Shift & 63)
		bit := uint64(1) << lane
		if header.Bitmap&bit == 0 {
			_ = lease.Close()
			return 0, corruptStorePage("mutable chunk-directory lane", storeio.ErrChunkDirectoryCorrupt)
		}
		node := &db.path[depth]
		node.header = header
		node.count = view.Len()
		node.rank = bits.OnesCount64(header.Bitmap & (bit - 1))
		node.newBitmap = header.Bitmap
		node.newRef = storeio.PageRef{}
		node.hadChild = true
		for rank := 0; rank < node.count; rank++ {
			child, ok := view.RefAt(rank)
			if !ok {
				_ = lease.Close()
				return 0, corruptStorePage("mutable chunk-directory reference", storeio.ErrChunkDirectoryCorrupt)
			}
			node.refs[rank] = child
		}
		child := node.refs[node.rank]
		if closeErr := lease.Close(); closeErr != nil {
			return 0, closeErr
		}
		if child.Offset >= ref.Offset {
			return 0, corruptStorePage("mutable chunk-directory order", storeio.ErrChunkDirectoryCorrupt)
		}
		if header.Shift == 0 {
			if child != document || child.Kind != storeio.PageDocument {
				return 0, corruptStorePage("mutable document reference", storeio.ErrChunkDirectoryCorrupt)
			}
			return depth + 1, nil
		}
		if child.Kind != storeio.PageChunkDirectory || header.Shift < 6 {
			return 0, corruptStorePage("mutable directory child kind", storeio.ErrChunkDirectoryCorrupt)
		}
		expectedShift = header.Shift - 6
		haveExpectedShift = true
		ref = child
	}
	return 0, corruptStorePage("mutable chunk-directory depth", storeio.ErrChunkDirectoryCorrupt)
}

// loadKeyDeletePath copies the exact B+tree leaf path into fixed writer
// scratch. Equal-hash runs advance through parent-derived successors instead
// of physical leaf links, so COW always rewrites the generation-visible leaf
// and leaves no stale candidate behind.
func (db *StorePageDB) loadKeyDeletePath(pages *storeio.PageFile, root storeio.StateRoot,
	hash uint64, location storeio.PageKeyLocation) (depth int, rewrite bool, err error) {
	ref := root.KeyDirectory
	expectedLevel := uint8(0)
	haveExpectedLevel := false
	for ref != (storeio.PageRef{}) {
		lease, pinErr := pages.Cache().Pin(ref)
		if pinErr != nil {
			return 0, false, storePageReadError(pinErr)
		}
		view := storeio.AdmittedPageKeyDirectory(lease.Bytes())
		header := view.Header()
		if haveExpectedLevel && header.Level != expectedLevel {
			_ = lease.Close()
			return 0, false, corruptStorePage("mutable key-directory level", storeio.ErrKeyDirectoryCorrupt)
		}
		if header.Level == 0 {
			leaf := &db.keyLeaf
			leaf.header = header
			leaf.count = view.Len()
			leaf.rank = -1
			leaf.newCount = leaf.count
			leaf.newRef = storeio.PageRef{}
			for rank := 0; rank < leaf.count; rank++ {
				entry, ok := view.LocationAt(rank)
				if !ok {
					_ = lease.Close()
					return 0, false, corruptStorePage("mutable key leaf", storeio.ErrKeyDirectoryCorrupt)
				}
				leaf.entries[rank] = entry
				if entry == location {
					leaf.rank = rank
				}
			}
			if closeErr := lease.Close(); closeErr != nil {
				return 0, false, closeErr
			}
			if leaf.rank >= 0 {
				leaf.newCount--
				return depth, true, nil
			}
			if header.MaxHash != hash {
				return 0, false, corruptStorePage("mutable key location", storeio.ErrKeyDirectoryCorrupt)
			}
			next, ok, nextErr := db.nextKeyDeleteLeaf(pages, &depth)
			if nextErr != nil {
				return 0, false, nextErr
			}
			if !ok {
				return 0, false, corruptStorePage("mutable key collision continuation", storeio.ErrKeyDirectoryCorrupt)
			}
			ref = next
			expectedLevel = 0
			haveExpectedLevel = true
			continue
		}
		if depth == len(db.keyPath) {
			_ = lease.Close()
			return 0, false, corruptStorePage("mutable key-directory depth", storeio.ErrKeyDirectoryCorrupt)
		}
		node := &db.keyPath[depth]
		node.header = header
		node.count = view.Len()
		node.rank = -1
		node.newCount = node.count
		node.newRef = storeio.PageRef{}
		for rank := 0; rank < node.count; rank++ {
			entry, ok := view.BranchAt(rank)
			if !ok {
				_ = lease.Close()
				return 0, false, corruptStorePage("mutable key branch", storeio.ErrKeyDirectoryCorrupt)
			}
			node.entries[rank] = entry
			if node.rank < 0 && entry.MaxHash >= hash {
				node.rank = rank
			}
		}
		if node.rank < 0 {
			_ = lease.Close()
			return 0, false, corruptStorePage("mutable key branch range", storeio.ErrKeyDirectoryCorrupt)
		}
		child := node.entries[node.rank].Child
		if closeErr := lease.Close(); closeErr != nil {
			return 0, false, closeErr
		}
		if child.Kind != storeio.PageKeyDirectory || child.Offset >= ref.Offset || header.Level == 0 {
			return 0, false, corruptStorePage("mutable key branch child", storeio.ErrKeyDirectoryCorrupt)
		}
		expectedLevel = header.Level - 1
		haveExpectedLevel = true
		ref = child
		depth++
	}
	return 0, false, corruptStorePage("mutable missing key root", storeio.ErrKeyDirectoryCorrupt)
}

// nextKeyDeleteLeaf advances the writer's copied branch path to the next
// physical leaf in the selected immutable root. Nodes below the first branch
// with a successor are replaced in scratch by their leftmost descent, leaving
// commitMutation with the exact path it must copy.
func (db *StorePageDB) nextKeyDeleteLeaf(pages *storeio.PageFile, depth *int) (storeio.PageRef, bool, error) {
	for level := *depth - 1; level >= 0; level-- {
		node := &db.keyPath[level]
		if node.rank+1 >= node.count {
			continue
		}
		node.rank++
		ref := node.entries[node.rank].Child
		*depth = level + 1
		expected := node.header.Level - 1
		for expected != 0 {
			if *depth == len(db.keyPath) {
				return storeio.PageRef{}, false, corruptStorePage("mutable key-successor depth", storeio.ErrKeyDirectoryCorrupt)
			}
			lease, err := pages.Cache().Pin(ref)
			if err != nil {
				return storeio.PageRef{}, false, storePageReadError(err)
			}
			view := storeio.AdmittedPageKeyDirectory(lease.Bytes())
			header := view.Header()
			if header.Level != expected {
				_ = lease.Close()
				return storeio.PageRef{}, false, corruptStorePage("mutable key-successor level", storeio.ErrKeyDirectoryCorrupt)
			}
			childNode := &db.keyPath[*depth]
			childNode.header = header
			childNode.count = view.Len()
			childNode.rank = 0
			childNode.newCount = childNode.count
			childNode.newRef = storeio.PageRef{}
			for rank := 0; rank < childNode.count; rank++ {
				entry, ok := view.BranchAt(rank)
				if !ok {
					_ = lease.Close()
					return storeio.PageRef{}, false, corruptStorePage("mutable key-successor branch", storeio.ErrKeyDirectoryCorrupt)
				}
				childNode.entries[rank] = entry
			}
			child := childNode.entries[0].Child
			if closeErr := lease.Close(); closeErr != nil {
				return storeio.PageRef{}, false, closeErr
			}
			if child.Offset >= ref.Offset {
				return storeio.PageRef{}, false, corruptStorePage("mutable key-successor child order", storeio.ErrKeyDirectoryCorrupt)
			}
			ref = child
			*depth = *depth + 1
			expected--
		}
		return ref, true, nil
	}
	return storeio.PageRef{}, false, nil
}

func (db *StorePageDB) commitMutation(root storeio.StateRoot, oldFileEnd uint64, chunkID uint32,
	docSize uint32, rowCount, depth, keyDepth int, rewriteKey bool, live uint64, deleting bool,
	oldValue *StorePageValue) (err error) {
	if root.Generation == ^uint64(0) {
		return fmt.Errorf("%w: generation exhausted", store.ErrTooLarge)
	}
	generation := root.Generation + 1
	childExists := rowCount != 0
	directoryPages := 0
	for level := depth - 1; level >= 0; level-- {
		node := &db.path[level]
		if !childExists {
			lane := uint8(chunkID >> node.header.Shift & 63)
			node.newBitmap &^= uint64(1) << lane
		}
		childExists = node.newBitmap != 0
		if childExists {
			directoryPages++
		}
	}
	keyPages := 0
	keyChildExists := false
	if rewriteKey {
		keyChildExists = db.keyLeaf.newCount != 0
		if keyChildExists {
			keyPages++
		}
		for level := keyDepth - 1; level >= 0; level-- {
			node := &db.keyPath[level]
			node.newCount = node.count
			if !keyChildExists {
				node.newCount--
			}
			keyChildExists = node.newCount != 0
			if keyChildExists {
				keyPages++
			}
		}
	}
	pageCount := directoryPages + keyPages + 1 // state root
	if rowCount != 0 {
		pageCount++
	}
	batch, err := db.committer.Begin(pageCount)
	if err != nil {
		return err
	}
	owned := true
	defer func() {
		if owned {
			err = errors.Join(err, batch.Abort())
		}
	}()

	offset := oldFileEnd
	nextLogical := root.NextLogicalID
	pageIndex := 0
	newDocument := storeio.PageRef{}
	if rowCount != 0 {
		leaf := &db.path[depth-1]
		oldDocument := leaf.refs[leaf.rank]
		newDocument = storeio.PageRef{Offset: offset, LogicalID: oldDocument.LogicalID, Generation: generation,
			Length: docSize, Kind: storeio.PageDocument}
		offset += uint64(docSize)
	}
	for level := depth - 1; level >= 0; level-- {
		node := &db.path[level]
		if node.newBitmap == 0 {
			continue
		}
		node.newRef = storeio.PageRef{Offset: offset, LogicalID: node.header.LogicalID, Generation: generation,
			Length: storePageQuantum, Kind: storeio.PageChunkDirectory}
		offset += uint64(storePageQuantum)
	}
	if rewriteKey && db.keyLeaf.newCount != 0 {
		db.keyLeaf.newRef = storeio.PageRef{
			Offset: offset, LogicalID: db.keyLeaf.header.LogicalID, Generation: generation,
			Length: storePageQuantum, Kind: storeio.PageKeyDirectory,
		}
		offset += uint64(storePageQuantum)
	}
	if rewriteKey {
		for level := keyDepth - 1; level >= 0; level-- {
			node := &db.keyPath[level]
			if node.newCount == 0 {
				continue
			}
			node.newRef = storeio.PageRef{
				Offset: offset, LogicalID: node.header.LogicalID, Generation: generation,
				Length: storePageQuantum, Kind: storeio.PageKeyDirectory,
			}
			offset += uint64(storePageQuantum)
		}
	}
	stateOffset := offset
	fileEnd := stateOffset + uint64(storePageQuantum)

	if rowCount != 0 {
		buffer, bufferErr := batch.PageBuffer(pageIndex)
		if bufferErr != nil {
			return bufferErr
		}
		page, encodeErr := storeio.EncodeDocumentPage(buffer[:docSize], storeio.DocumentPageHeader{
			StoreID: root.StoreID, Generation: generation, LogicalID: newDocument.LogicalID,
			PageSize: docSize, ChunkID: chunkID, Live: live,
		}, db.rows[:rowCount], nextLogical)
		if encodeErr != nil {
			return encodeErr
		}
		if setErr := batch.SetPage(pageIndex, int64(newDocument.Offset), len(page)); setErr != nil {
			return setErr
		}
		pageIndex++
	}

	child := newDocument
	for level := depth - 1; level >= 0; level-- {
		node := &db.path[level]
		if child == (storeio.PageRef{}) {
			copy(node.refs[node.rank:], node.refs[node.rank+1:node.count])
			node.count--
		} else {
			node.refs[node.rank] = child
		}
		if node.newBitmap == 0 {
			child = storeio.PageRef{}
			continue
		}
		buffer, bufferErr := batch.PageBuffer(pageIndex)
		if bufferErr != nil {
			return bufferErr
		}
		header := node.header
		header.Generation = generation
		header.LogicalID = node.newRef.LogicalID
		header.Bitmap = node.newBitmap
		page, encodeErr := storeio.EncodeChunkDirectoryPage(buffer[:storePageQuantum], header,
			node.refs[:node.count], fileEnd, nextLogical)
		if encodeErr != nil {
			return encodeErr
		}
		if setErr := batch.SetPage(pageIndex, int64(node.newRef.Offset), len(page)); setErr != nil {
			return setErr
		}
		pageIndex++
		child = node.newRef
	}

	keyRoot := root.KeyDirectory
	if rewriteKey {
		leaf := &db.keyLeaf
		copy(leaf.entries[leaf.rank:], leaf.entries[leaf.rank+1:leaf.count])
		leaf.count--
		keyChild := storeio.PageRef{}
		keyChildMax := uint64(0)
		if leaf.count != 0 {
			buffer, bufferErr := batch.PageBuffer(pageIndex)
			if bufferErr != nil {
				return bufferErr
			}
			header := leaf.header
			header.Generation = generation
			header.LogicalID = leaf.newRef.LogicalID
			header.MinHash = leaf.entries[0].Hash
			header.MaxHash = leaf.entries[leaf.count-1].Hash
			page, encodeErr := storeio.EncodePageKeyLeaf(buffer[:storePageQuantum], header,
				leaf.entries[:leaf.count], fileEnd, nextLogical, root.ChunkHighWater, root.ChunkDocuments)
			if encodeErr != nil {
				return encodeErr
			}
			if setErr := batch.SetPage(pageIndex, int64(leaf.newRef.Offset), len(page)); setErr != nil {
				return setErr
			}
			pageIndex++
			keyChild = leaf.newRef
			keyChildMax = header.MaxHash
		}
		for level := keyDepth - 1; level >= 0; level-- {
			node := &db.keyPath[level]
			if keyChild == (storeio.PageRef{}) {
				copy(node.entries[node.rank:], node.entries[node.rank+1:node.count])
				node.count--
			} else {
				node.entries[node.rank].Child = keyChild
				node.entries[node.rank].MaxHash = keyChildMax
			}
			if node.count == 0 {
				keyChild = storeio.PageRef{}
				continue
			}
			buffer, bufferErr := batch.PageBuffer(pageIndex)
			if bufferErr != nil {
				return bufferErr
			}
			header := node.header
			header.Generation = generation
			header.LogicalID = node.newRef.LogicalID
			header.MaxHash = node.entries[node.count-1].MaxHash
			page, encodeErr := storeio.EncodePageKeyBranch(buffer[:storePageQuantum], header,
				node.entries[:node.count], fileEnd, nextLogical)
			if encodeErr != nil {
				return encodeErr
			}
			if setErr := batch.SetPage(pageIndex, int64(node.newRef.Offset), len(page)); setErr != nil {
				return setErr
			}
			pageIndex++
			keyChild = node.newRef
			keyChildMax = header.MaxHash
		}
		keyRoot = keyChild
	}

	next := root
	next.Generation = generation
	next.NextLogicalID = nextLogical
	next.ChunkDirectory = child
	next.KeyDirectory = keyRoot
	if deleting {
		next.DocumentCount--
		next.FreeChunkHint = min(next.FreeChunkHint, chunkID)
		if rowCount == 0 {
			next.LiveChunks--
		}
		if next.DocumentCount == 0 {
			next.KeyDirectory = storeio.PageRef{}
		}
	}
	stateBuffer, err := batch.PageBuffer(pageIndex)
	if err != nil {
		return err
	}
	statePage, err := storeio.EncodeStateRootPage(stateBuffer[:storePageQuantum], next, fileEnd)
	if err != nil {
		return err
	}
	if err := batch.SetPage(pageIndex, int64(stateOffset), len(statePage)); err != nil {
		return err
	}
	pageIndex++
	if pageIndex != pageCount {
		return fmt.Errorf("%w: planned %d pages, encoded %d", storeio.ErrInvalidWrite, pageCount, pageIndex)
	}
	if err := batch.SetSuperblock(storeio.Superblock{
		StoreID: root.StoreID, Generation: generation, StateOffset: stateOffset,
		StateLength: storePageQuantum, StateChecksum: storeio.PageChecksum(statePage),
		FileEnd: fileEnd, PageSize: storePageQuantum,
	}); err != nil {
		return err
	}
	if err := batch.Publish(generation); err != nil {
		return err
	}
	owned = false
	// Every staged page owns its bytes now. Drop aliases into the old document
	// frame before paying storage latency so a two-frame cache still leaves
	// both frames available to concurrent point readers.
	clear(db.rows[:])
	closeErr := oldValue.Close()
	if err := db.committer.Wait(generation); err != nil {
		return errors.Join(err, closeErr)
	}
	db.publish(next, fileEnd)
	return closeErr
}

// Flush waits for every generation accepted before the call. Mutations are
// durable before they return, so Flush is normally a cheap progress check.
func (db *StorePageDB) Flush() error {
	if db == nil || db.committer == nil {
		return nil
	}
	return db.committer.Flush()
}

// Len returns the document count of the currently published durable root.
func (db *StorePageDB) Len() uint64 {
	if db == nil {
		return 0
	}
	return db.published.load().root.DocumentCount
}

// Generation returns the generation visible to new readers.
func (db *StorePageDB) Generation() uint64 {
	if db == nil {
		return 0
	}
	return db.published.load().root.Generation
}

// DurableGeneration returns the latest generation that passed both barriers.
func (db *StorePageDB) DurableGeneration() uint64 {
	if db == nil {
		return 0
	}
	view := db.published.load()
	if db.committer == nil {
		return view.root.Generation
	}
	durable := db.committer.DurableGeneration()
	if durable == 0 {
		return view.root.Generation
	}
	return durable
}

// StorePageDBStats is a non-allocating control-plane snapshot.
type StorePageDBStats struct {
	Documents          uint64
	Generation         uint64
	DurableGeneration  uint64
	FileBytes          uint64
	ChunkHighWater     uint32
	LiveChunks         uint32
	FreeChunkHint      uint32
	DirectIO           bool
	CommitBackend      StorePageCommitBackend
	QueuedGenerations  uint64
	DeviceCommits      uint64
	CommittedBatches   uint64
	LargestCommitGroup uint32
	Cache              StorePageCacheStats
}

// Stats reports durable progress, append high-water mark, and bounded-cache
// counters without traversing database contents.
func (db *StorePageDB) Stats() StorePageDBStats {
	if db == nil {
		return StorePageDBStats{}
	}
	view := db.published.load()
	stats := StorePageDBStats{Documents: view.root.DocumentCount, Generation: view.root.Generation,
		DurableGeneration: db.DurableGeneration(), FileBytes: view.fileEnd,
		ChunkHighWater: view.root.ChunkHighWater, LiveChunks: view.root.LiveChunks,
		FreeChunkHint: view.root.FreeChunkHint}
	if committer := db.committer; committer != nil {
		commit := committer.Stats()
		stats.CommitBackend = storePageCommitBackend(commit.Backend)
		stats.QueuedGenerations = commit.QueuedGenerations
		stats.DeviceCommits = commit.DeviceCommits
		stats.CommittedBatches = commit.CommittedBatches
		stats.LargestCommitGroup = commit.LargestGroup
	}
	pages := db.pages.Load()
	if pages == nil {
		return stats
	}
	stats.DirectIO = pages.Direct()
	cache := pages.Cache().Stats()
	stats.Cache = StorePageCacheStats{
		CapacityBytes: cache.CapacityBytes, ResidentBytes: cache.ResidentBytes,
		ReservedBytes: cache.ReservedBytes,
		FrameSize:     cache.FrameSize, Frames: cache.Frames, ReadyFrames: cache.ReadyFrames,
		LoadingFrames: cache.LoadingFrames, FailedFrames: cache.FailedFrames,
		PinnedFrames: cache.PinnedFrames, Pins: cache.Pins, Hits: cache.Hits,
		Misses: cache.Misses, Coalesced: cache.Coalesced, PageReads: cache.PageReads,
		ReadBytes: cache.ReadBytes, Evictions: cache.Evictions, ReadErrors: cache.ReadErrors,
	}
	return stats
}

// Close stops new operations, drains the committer, waits for live cache
// users, and releases both file descriptors. It is safe to call repeatedly.
func (db *StorePageDB) Close() error {
	if db == nil {
		return nil
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	pages := db.pages.Swap(nil)
	if pages == nil {
		return nil
	}
	commitErr := db.committer.Close()
	unlockErr := storeio.UnlockWriter(db.file)
	fileErr := db.file.Close()
	pageErr := pages.Close()
	return errors.Join(commitErr, unlockErr, fileErr, pageErr)
}
