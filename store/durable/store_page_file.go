package durable

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"slices"
	"sync/atomic"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson/x/byteview"
)

const (
	storePageQuantum            = uint32(4096)
	storePageDefaultMaxDocument = uint32(64 << 10)
	storePageDefaultResident    = int64(64 << 20)
	storePageKeyMaxDepth        = 16
)

var (
	// ErrStorePageFileExists rejects destructive in-place checkpoint creation.
	// WritePageFile requires an empty file so a crash cannot destroy an older
	// valid generation; applications write a temporary file and rename it.
	ErrStorePageFileExists = errors.New("vibejson: Store page file is not empty")
	// ErrStorePageUnsupported reports state not yet represented by the paged
	// read tier, currently secondary-index roots.
	ErrStorePageUnsupported = errors.New("vibejson: collection state is not supported by paged file")
	// ErrStoreDocumentPageTooLarge reports a chunk that needs the future
	// overflow-page schema instead of one contiguous bounded extent.
	ErrStoreDocumentPageTooLarge = errors.New("vibejson: collection document page exceeds configured maximum")
	// ErrStorePageCacheFull reports bounded-cache backpressure. Release a
	// StorePageValue before retrying; the reader never expands its budget.
	ErrStorePageCacheFull = storeio.ErrPageCacheFull
	// ErrStorePageClosed reports a read started after StorePageReader.Close.
	ErrStorePageClosed = storeio.ErrPageCacheClosed
	// ErrStoreDirectIOUnsupported reports that StoreDirectRequire or
	// a required Store direct read/write mode cannot be honored by the
	// platform or filesystem.
	ErrStoreDirectIOUnsupported = storeio.ErrDirectIOUnsupported
	// ErrStorePageCorrupt reports a checksum, identity, schema, or durable
	// graph violation while opening or reading a page file.
	ErrStorePageCorrupt = errors.New("vibejson: corrupt Store page file")
	// ErrStorePageSchemaMismatch reports a missing or different compiled
	// schema when opening a schema-bound page file.
	ErrStorePageSchemaMismatch = errors.New(
		"vibejson: Store page schema mismatch",
	)
)

func corruptStorePage(detail string, cause error) error {
	return fmt.Errorf("%w: %s: %w", ErrStorePageCorrupt, detail, cause)
}

func storePageReadError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrStorePageCorrupt) {
		return err
	}
	if errors.Is(err, storeio.ErrPageCorrupt) || errors.Is(err, storeio.ErrPageReference) ||
		errors.Is(err, storeio.ErrDocumentPageCorrupt) || errors.Is(err, storeio.ErrChunkDirectoryCorrupt) ||
		errors.Is(err, storeio.ErrKeyDirectoryCorrupt) || errors.Is(err, storeio.ErrStateRootCorrupt) ||
		errors.Is(err, storeio.ErrPageCatalogCorrupt) ||
		errors.Is(err, storeio.ErrSuperblockCorrupt) || errors.Is(err, storeio.ErrSuperblockNotFound) ||
		errors.Is(err, storeio.ErrSuperblockConflict) {
		return corruptStorePage("durable data", err)
	}
	return err
}

// StorePageWriteOptions fixes the largest contiguous document micro-page.
// Zero selects 64 KiB. Larger values must be powers of two and at least 4 KiB.
type StorePageWriteOptions struct {
	MaxDocumentPageBytes uint32
}

func (o StorePageWriteOptions) normalized() (StorePageWriteOptions, error) {
	if o.MaxDocumentPageBytes == 0 {
		o.MaxDocumentPageBytes = storePageDefaultMaxDocument
	}
	if o.MaxDocumentPageBytes < storePageQuantum || o.MaxDocumentPageBytes&(o.MaxDocumentPageBytes-1) != 0 ||
		o.MaxDocumentPageBytes > storeio.MaxPhysicalPageSize ||
		uint64(o.MaxDocumentPageBytes) > uint64(math.MaxInt) {
		return StorePageWriteOptions{}, fmt.Errorf("%w: max document page %d", ErrStoreDocumentPageTooLarge, o.MaxDocumentPageBytes)
	}
	return o, nil
}

// StoreDirectIO controls explicit Linux direct reads for a StorePageReader.
type StoreDirectIO uint8

const (
	// StoreDirectOff uses ordinary buffered file reads.
	StoreDirectOff StoreDirectIO = iota
	// StoreDirectTry requests Linux direct I/O and falls back only when the
	// platform or filesystem explicitly does not support it.
	StoreDirectTry
	// StoreDirectRequire fails open unless Linux direct I/O is active.
	StoreDirectRequire
)

// StorePageOpenOptions fixes the complete external frame budget and may cap
// the largest readable document extent. Zero ResidentBytes selects 64 MiB;
// zero MaxDocumentPageBytes adopts the exact persisted maximum.
type StorePageOpenOptions struct {
	ResidentBytes        int64
	MaxDocumentPageBytes uint32
	DirectIO             StoreDirectIO
	// A non-nil Schema is an exact assertion against the persisted canonical
	// definition. Nil rehydrates either the persisted schema or schemaless
	// state, allowing a zero-option reopen. StorePageDB applies the resolved
	// schema to every later Put.
	Schema *store.Schema
}

func (o StorePageOpenOptions) normalized() (StorePageOpenOptions, error) {
	if o.ResidentBytes == 0 {
		o.ResidentBytes = storePageDefaultResident
	}
	if o.MaxDocumentPageBytes == 0 {
		o.MaxDocumentPageBytes = storePageDefaultMaxDocument
	}
	if o.DirectIO > StoreDirectRequire || o.MaxDocumentPageBytes < storePageQuantum ||
		o.MaxDocumentPageBytes&(o.MaxDocumentPageBytes-1) != 0 ||
		o.MaxDocumentPageBytes > storeio.MaxPhysicalPageSize ||
		o.ResidentBytes < 2*int64(o.MaxDocumentPageBytes) {
		return StorePageOpenOptions{}, fmt.Errorf("vibejson: invalid Store page-open options")
	}
	if o.Schema != nil && !o.Schema.Valid() {
		return StorePageOpenOptions{}, fmt.Errorf(
			"%w: uninitialized compiled schema",
			store.ErrSchemaDefinition,
		)
	}
	return o, nil
}

// WritePageFile writes one immutable, checksummed, bounded-residency collection
// generation to an empty file. It publishes the superblock only after every
// document and directory page plus the state root have been written and
// synced. The file remains open and owned by the caller.
//
// This first attached read tier persists keys and exact JSON. Secondary-index
// page roots are rejected rather than silently omitted. The existing
// WriteTo/OpenStore image remains the full-state checkpoint format while those
// directory schemas are attached.
func WritePageFile(collection *store.Collection, file *os.File, options StorePageWriteOptions) (int64, error) {
	if collection == nil || file == nil {
		return 0, fmt.Errorf("vibejson: WritePageFile requires non-nil collection and file")
	}
	options, err := options.normalized()
	if err != nil {
		return 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() != 0 {
		return 0, ErrStorePageFileExists
	}

	var state *store.State
	schema := collection.Options.Schema
	snapshotErr := collection.WithBulkSnapshot(func(snapshot *store.State) error {
		state = snapshot
		if len(state.Indexes) != 0 {
			return ErrStorePageUnsupported
		}
		return nil
	})
	if snapshotErr != nil {
		return 0, snapshotErr
	}

	var storeID [16]byte
	if _, err := rand.Read(storeID[:]); err != nil {
		return 0, fmt.Errorf("create Store page id: %w", err)
	}
	generation := state.Generation
	if generation == 0 {
		generation = 1
	}
	nextLogical := uint64(storeio.StateRootLogicalID + 1)
	layout, err := storeio.MutableStoreLayout(storePageQuantum)
	if err != nil {
		return 0, err
	}
	catalog, err := buildStorePageCatalog(schema)
	if err != nil {
		return 0, err
	}
	catalogPlan, err := planFilePageCatalog(
		catalog, storeID, generation, storePageQuantum,
		layout.DataStart, nextLogical,
	)
	if err != nil {
		return 0, err
	}
	offset := catalogPlan.fileEnd
	nextLogical = catalogPlan.nextID

	docPlans := make([]storeDocumentPagePlan, 0, state.ChunkCount)
	keyEntries := make([]storeio.PageKeyLocation, 0, state.Count)
	chunkItems := make([]storeChunkDirectoryItem, 0, state.ChunkCount)
	freeChunkHint := state.Chunks.Count
	for chunkID := uint32(0); chunkID < state.Chunks.Count; chunkID++ {
		chunk := state.Chunks.Get(chunkID)
		if chunk == nil {
			if freeChunkHint == state.Chunks.Count {
				freeChunkHint = chunkID
			}
			continue
		}
		if int(chunk.Count) < state.StateOptions.ChunkDocuments && freeChunkHint == state.Chunks.Count {
			freeChunkHint = chunkID
		}
		required := uint64(storeio.PageHeaderSize + storeio.PageTrailerSize +
			storeio.DocumentPagePayloadHeaderSize + int(chunk.Count)*storeio.DocumentPageRecordSize)
		for live := chunk.Live; live != 0; live &= live - 1 {
			slot := bits.TrailingZeros64(live)
			key := chunk.Key(slot)
			raw := chunk.Docs.RawAt(int(chunk.Ord[slot]))
			required += uint64(len(key) + len(raw))
			keyEntries = append(keyEntries, storeio.PageKeyLocation{
				Hash: storeio.KeyHash(storeID, key), Chunk: chunkID, Slot: uint8(slot),
			})
		}
		pageSize, ok := storePageExtent(required, options.MaxDocumentPageBytes)
		if !ok {
			return 0, fmt.Errorf("%w: chunk=%d bytes=%d max=%d", ErrStoreDocumentPageTooLarge, chunkID, required, options.MaxDocumentPageBytes)
		}
		ref := storeio.PageRef{
			Offset: offset, LogicalID: nextLogical, Generation: generation,
			Length: pageSize, Kind: storeio.PageDocument,
		}
		nextLogical++
		offset += uint64(pageSize)
		docPlans = append(docPlans, storeDocumentPagePlan{chunkID: chunkID, chunk: chunk, ref: ref})
		chunkItems = append(chunkItems, storeChunkDirectoryItem{id: chunkID, ref: ref})
	}

	chunkPlans, chunkRoot := planStoreChunkDirectories(chunkItems, generation, storePageQuantum, &nextLogical, &offset)
	slices.SortFunc(keyEntries, func(a, b storeio.PageKeyLocation) int {
		if a.Hash < b.Hash {
			return -1
		}
		if a.Hash > b.Hash {
			return 1
		}
		if a.Chunk < b.Chunk {
			return -1
		}
		if a.Chunk > b.Chunk {
			return 1
		}
		return int(a.Slot) - int(b.Slot)
	})
	keyPlans, keyRoot := planStoreKeyDirectories(keyEntries, generation, &nextLogical, &offset)
	stateOffset := offset
	offset += uint64(storePageQuantum)
	fileEnd := offset
	nextLogicalID := nextLogical

	if err := file.Truncate(int64(fileEnd)); err != nil {
		return 0, err
	}
	maxScratch := max(int(options.MaxDocumentPageBytes), int(storePageQuantum))
	scratch := make([]byte, maxScratch)
	if err := catalogPlan.write(
		file, fileEnd, nextLogicalID, scratch,
	); err != nil {
		return 0, err
	}
	rows := make([]storeio.DocumentRecord, 0, store.MaxChunkDocuments)
	for _, plan := range docPlans {
		rows = rows[:0]
		for live := plan.chunk.Live; live != 0; live &= live - 1 {
			slot := bits.TrailingZeros64(live)
			rows = append(rows, storeio.DocumentRecord{
				Key:  byteview.Bytes(plan.chunk.Key(slot)),
				JSON: plan.chunk.Docs.RawAt(int(plan.chunk.Ord[slot])),
				Slot: uint8(slot),
			})
		}
		page, err := storeio.EncodeDocumentPage(scratch[:plan.ref.Length], storeio.DocumentPageHeader{
			StoreID: storeID, Generation: generation, LogicalID: plan.ref.LogicalID,
			PageSize: plan.ref.Length, ChunkID: plan.chunkID, Live: plan.chunk.Live,
		}, rows, nextLogicalID)
		if err != nil {
			return 0, err
		}
		if err := writeStorePageAt(file, page, plan.ref.Offset); err != nil {
			return 0, err
		}
	}
	for _, plan := range chunkPlans {
		page, err := storeio.EncodeChunkDirectoryPage(scratch[:storePageQuantum], storeio.ChunkDirectoryHeader{
			StoreID: storeID, Generation: generation, LogicalID: plan.ref.LogicalID,
			PageSize: storePageQuantum, Prefix: plan.prefix, Bitmap: plan.bitmap, Shift: plan.shift,
		}, plan.children, fileEnd, nextLogicalID)
		if err != nil {
			return 0, err
		}
		if err := writeStorePageAt(file, page, plan.ref.Offset); err != nil {
			return 0, err
		}
	}
	for _, plan := range keyPlans {
		header := storeio.PageKeyDirectoryHeader{
			StoreID: storeID, Generation: generation, LogicalID: plan.ref.LogicalID,
			PageSize: storePageQuantum, MinHash: plan.minHash, MaxHash: plan.maxHash,
			Level: plan.level, Next: plan.next,
		}
		var page []byte
		if plan.level == 0 {
			page, err = storeio.EncodePageKeyLeaf(scratch[:storePageQuantum], header, plan.leaf,
				fileEnd, nextLogicalID, state.Chunks.Count, uint32(state.StateOptions.ChunkDocuments))
		} else {
			page, err = storeio.EncodePageKeyBranch(scratch[:storePageQuantum], header, plan.branches, fileEnd, nextLogicalID)
		}
		if err != nil {
			return 0, err
		}
		if err := writeStorePageAt(file, page, plan.ref.Offset); err != nil {
			return 0, err
		}
	}

	root := storeio.StateRoot{
		StoreID: storeID, Generation: generation, PageSize: storePageQuantum,
		MaxPageSize: uint32(options.MaxDocumentPageBytes),
		Options:     storePageOptionFlags(state.StateOptions), DocumentCount: uint64(state.Count),
		NextLogicalID: nextLogicalID, ChunkHighWater: state.Chunks.Count,
		LiveChunks: state.ChunkCount, ChunkDocuments: uint32(state.StateOptions.ChunkDocuments),
		IndexMaxDepth:  uint32(max(state.StateOptions.IndexOptions.MaxDepth, 0)),
		FreeChunkHint:  freeChunkHint,
		ChunkDirectory: chunkRoot, KeyDirectory: keyRoot,
	}
	if schema != nil {
		root.Options |= storeio.StateOptionSchema
		root.IndexCatalogHash = schema.Hash
	}
	if err := catalogPlan.apply(
		&root, options.MaxDocumentPageBytes,
	); err != nil {
		return 0, err
	}
	statePage, err := storeio.EncodeStateRootPage(scratch[:storePageQuantum], root, fileEnd)
	if err != nil {
		return 0, err
	}
	if err := writeStorePageAt(file, statePage, stateOffset); err != nil {
		return 0, err
	}
	if err := file.Sync(); err != nil {
		return 0, err
	}
	rootRecord := storeio.Superblock{
		StoreID: storeID, Generation: generation, StateOffset: stateOffset,
		StateLength: storePageQuantum, StateChecksum: storeio.PageChecksum(statePage),
		FileEnd: fileEnd, PageSize: storePageQuantum,
	}
	rootPage := scratch[:storePageQuantum]
	clear(rootPage)
	if _, err := storeio.EncodeSuperblock(rootPage[:storeio.SuperblockSize], rootRecord); err != nil {
		return 0, err
	}
	rootOffset, err := storeio.SuperblockOffset(generation, storePageQuantum)
	if err != nil {
		return 0, err
	}
	if err := writeStorePageAt(file, rootPage, uint64(rootOffset)); err != nil {
		return 0, err
	}
	if err := file.Sync(); err != nil {
		return 0, err
	}
	return int64(fileEnd), nil
}

// StorePageReader is a read-only, bounded-residency view of a Store page file.
// It keeps only the recovered root and a fixed external frame cache resident;
// key, directory, and document pages enter on demand. Use AppendRaw for a
// lifetime-independent copy or ViewRaw for a pinned zero-copy value. A reader
// must not be copied after first use; OpenStorePageReader returns its pointer.
type StorePageReader struct {
	pages   atomic.Pointer[storeio.PageFile]
	root    storeio.StateRoot
	fileEnd uint64
}

// OpenStorePageReader recovers the newest valid root and opens its bounded
// cache. The metadata quantum is fixed at 4 KiB in format version one.
func OpenStorePageReader(path string, options StorePageOpenOptions) (*StorePageReader, error) {
	recovery, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	scratch := make([]byte, storePageQuantum)
	super, root, _, err := storeio.RecoverStateRoot(recovery, storePageQuantum, scratch)
	if err != nil {
		_ = recovery.Close()
		return nil, storePageReadError(err)
	}
	catalog, err := openFilePageCatalog(
		recovery, root, super.FileEnd, scratch,
	)
	if err == nil {
		options, err = resolveStorePageOpenOptions(
			options, root, catalog,
		)
	}
	closeErr := recovery.Close()
	if err != nil {
		return nil, storePageReadError(err)
	}
	if closeErr != nil {
		return nil, closeErr
	}
	pages, err := openStorePageFile(path, options, root.StoreID, func() (storeio.StateRoot, uint64) {
		return root, super.FileEnd
	})
	if err != nil {
		return nil, err
	}
	reader := &StorePageReader{root: root, fileEnd: super.FileEnd}
	reader.pages.Store(pages)
	return reader, nil
}

// openStorePageFile centralizes page admission for immutable readers and the
// mutable page database. bounds must return a self-consistent root and file
// high-water mark. The mutable caller copies them through its value-only RCU
// root; no epoch is held while the admitted page is checked or consumed.
func openStorePageFile(path string, options StorePageOpenOptions, storeID [16]byte,
	bounds func() (storeio.StateRoot, uint64)) (*storeio.PageFile, error) {
	return storeio.OpenPageFile(path, storeio.PageFileOptions{
		Cache: storeio.PageCacheOptions{
			StoreID: storeID, ResidentBytes: options.ResidentBytes,
			FrameSize: options.MaxDocumentPageBytes,
			Validate: func(page []byte, ref storeio.PageRef) error {
				root, fileEnd := bounds()
				return storePageReadError(validateStorePageCachePage(page, ref, root, fileEnd))
			},
		},
		Direct: storeio.DirectMode(options.DirectIO),
	})
}

func validateStorePageCachePage(
	page []byte, ref storeio.PageRef, root storeio.StateRoot, fileEnd uint64,
) error {
	switch ref.Kind {
	case storeio.PageDocument:
		_, err := storeio.OpenDocumentPage(page, root.ChunkHighWater, root.NextLogicalID)
		return err
	case storeio.PageChunkDirectory:
		_, err := storeio.OpenChunkDirectoryPage(page, fileEnd, root.NextLogicalID)
		return err
	case storeio.PageKeyDirectory:
		_, err := storeio.OpenPageKeyDirectory(
			page, fileEnd, root.NextLogicalID, root.ChunkHighWater, root.ChunkDocuments,
		)
		return err
	case storeio.PageFingerprintDirectory:
		_, err := storeio.OpenPageFingerprintDirectory(
			page, fileEnd, root.NextLogicalID, root.ChunkHighWater, root.ChunkDocuments,
		)
		return err
	default:
		return fmt.Errorf("%w: unsupported page kind %d", ErrStorePageUnsupported, ref.Kind)
	}
}

// StorePageKey caches the deterministic persistent hash for one reader.
type StorePageKey struct {
	key      string
	storeID  [16]byte
	hash     uint64
	document storeio.PageRef
	chunk    uint32
	slot     uint8
	resolved bool
}

// CompileKey compiles a repeated page-file key without touching storage.
func (r *StorePageReader) CompileKey(key string) StorePageKey {
	if r == nil {
		return StorePageKey{key: key}
	}
	return StorePageKey{key: key, storeID: r.root.StoreID, hash: storeio.KeyHash(r.root.StoreID, key)}
}

// PrepareKey resolves an immutable key to its document page and stable slot.
// The returned value stores no frame pointer and pins no memory: eviction is
// unrestricted, while repeated reads skip both durable directories and admit
// only the named document page. The complete key is still rechecked on every
// read. Preparation may perform I/O and reports a missing key as ok=false.
func (r *StorePageReader) PrepareKey(key string) (prepared StorePageKey, ok bool, err error) {
	prepared = r.CompileKey(key)
	if r == nil {
		return prepared, false, nil
	}
	pages := r.pages.Load()
	if pages == nil {
		return prepared, false, ErrStorePageClosed
	}
	value, ok, err := r.lookupPageKey(pages, prepared, &prepared)
	if err != nil || !ok {
		return prepared, ok, err
	}
	if err := value.Close(); err != nil {
		return StorePageKey{}, false, err
	}
	return prepared, true, nil
}

// StorePageValue pins one document frame. It must not be copied and must be
// closed; Bytes is invalid immediately after Close.
type StorePageValue struct {
	lease storeio.PageLease
	raw   []byte
}

// Bytes returns exact JSON borrowed from the pinned document page.
func (v *StorePageValue) Bytes() []byte {
	if v == nil {
		return nil
	}
	return v.raw
}

// Close releases the document frame.
func (v *StorePageValue) Close() error {
	if v == nil {
		return nil
	}
	v.raw = nil
	return v.lease.Close()
}

// ViewRaw resolves key and returns a pinned zero-copy JSON view.
func (r *StorePageReader) ViewRaw(key string) (StorePageValue, bool, error) {
	return r.ViewRawKey(r.CompileKey(key))
}

// ViewRawKey is ViewRaw through a reusable compiled page-file key.
func (r *StorePageReader) ViewRawKey(key StorePageKey) (StorePageValue, bool, error) {
	if r == nil {
		return StorePageValue{}, false, nil
	}
	pages := r.pages.Load()
	if pages == nil {
		return StorePageValue{}, false, ErrStorePageClosed
	}
	if r.root.DocumentCount == 0 {
		return StorePageValue{}, false, nil
	}
	if key.storeID != r.root.StoreID {
		key.storeID = r.root.StoreID
		key.hash = storeio.KeyHash(r.root.StoreID, key.key)
		key.document = storeio.PageRef{}
		key.resolved = false
	}
	if key.resolved {
		return r.viewPreparedPageKey(pages, key)
	}
	return r.lookupPageKey(pages, key, nil)
}

func (r *StorePageReader) viewPreparedPageKey(pages *storeio.PageFile, key StorePageKey) (StorePageValue, bool, error) {
	docLease, err := pages.Cache().Pin(key.document)
	if err != nil {
		return StorePageValue{}, false, storePageReadError(err)
	}
	doc := storeio.AdmittedDocumentPage(docLease.Bytes())
	if doc.Header().ChunkID != key.chunk {
		_ = docLease.Close()
		return StorePageValue{}, false, corruptStorePage("prepared document chunk identity", storeio.ErrDocumentPageCorrupt)
	}
	raw, exact := doc.LookupString(key.slot, key.key)
	if !exact {
		_ = docLease.Close()
		return StorePageValue{}, false, corruptStorePage("prepared key location", storeio.ErrDocumentPageCorrupt)
	}
	return StorePageValue{lease: docLease, raw: raw}, true, nil
}

func (r *StorePageReader) lookupPageKey(pages *storeio.PageFile, key StorePageKey, prepared *StorePageKey) (StorePageValue, bool, error) {
	return lookupStorePageKey(pages, r.root.KeyDirectory, r.root.ChunkDirectory, key, prepared)
}

// storePageKeyPathEntry retains only the value identity and selected rank of
// one immutable key branch. It is stack-resident lookup state, not a durable
// or heap-side page table. A collision continuation can therefore derive the
// next leaf from the current root even after copy-on-write moved that leaf.
type storePageKeyPathEntry struct {
	ref   storeio.PageRef
	rank  uint16
	level uint8
}

// lookupStorePageKey is the shared immutable-graph point lookup. Supplying
// both root references by value lets a mutable caller finish against one
// generation while a writer publishes the next; no traversed page is mutable.
func lookupStorePageKey(pages *storeio.PageFile, keyRoot, chunkRoot storeio.PageRef, key StorePageKey,
	prepared *StorePageKey) (StorePageValue, bool, error) {
	ref := keyRoot
	expectedLevel := uint8(0)
	haveExpectedLevel := false
	for ref != (storeio.PageRef{}) {
		lease, err := pages.Cache().Pin(ref)
		if err != nil {
			return StorePageValue{}, false, storePageReadError(err)
		}
		view := storeio.AdmittedPageKeyDirectory(lease.Bytes())
		if haveExpectedLevel && view.Header().Level != expectedLevel {
			_ = lease.Close()
			return StorePageValue{}, false, corruptStorePage("key-directory level", storeio.ErrKeyDirectoryCorrupt)
		}
		if view.Header().Level != 0 {
			child, ok := view.Child(key.hash)
			if closeErr := lease.Close(); closeErr != nil {
				return StorePageValue{}, false, closeErr
			}
			if !ok {
				return StorePageValue{}, false, nil
			}
			if child.Offset >= ref.Offset {
				return StorePageValue{}, false, corruptStorePage("key-directory child order", storeio.ErrKeyDirectoryCorrupt)
			}
			expectedLevel = view.Header().Level - 1
			haveExpectedLevel = true
			ref = child
			continue
		}
		first, end, candidates := view.CandidateRange(key.hash)
		for i := first; candidates && i < end; i++ {
			location, _ := view.LocationAt(i)
			docRef, ok, resolveErr := resolveStoreDocumentPage(pages, chunkRoot, location.Chunk)
			if resolveErr != nil {
				_ = lease.Close()
				return StorePageValue{}, false, resolveErr
			}
			if !ok {
				continue
			}
			docLease, pinErr := pages.Cache().Pin(docRef)
			if pinErr != nil {
				_ = lease.Close()
				return StorePageValue{}, false, storePageReadError(pinErr)
			}
			doc := storeio.AdmittedDocumentPage(docLease.Bytes())
			if doc.Header().ChunkID != location.Chunk {
				_ = docLease.Close()
				_ = lease.Close()
				return StorePageValue{}, false, corruptStorePage("document chunk identity", storeio.ErrDocumentPageCorrupt)
			}
			if raw, exact := doc.LookupString(location.Slot, key.key); exact {
				if prepared != nil {
					prepared.document = docRef
					prepared.chunk = location.Chunk
					prepared.slot = location.Slot
					prepared.resolved = true
				}
				if closeErr := lease.Close(); closeErr != nil {
					_ = docLease.Close()
					return StorePageValue{}, false, closeErr
				}
				return StorePageValue{lease: docLease, raw: raw}, true, nil
			}
			if closeErr := docLease.Close(); closeErr != nil {
				_ = lease.Close()
				return StorePageValue{}, false, closeErr
			}
		}
		follow := view.Header().MaxHash == key.hash
		if closeErr := lease.Close(); closeErr != nil {
			return StorePageValue{}, false, closeErr
		}
		if !follow {
			return StorePageValue{}, false, nil
		}
		return lookupStorePageKeyCollisionSuccessors(pages, keyRoot, chunkRoot, key, prepared)
	}
	return StorePageValue{}, false, nil
}

// lookupStorePageKeyCollisionSuccessors reconstructs a parent cursor only
// after the common first leaf exhausted an equal-hash run. Ordinary lookups
// therefore pay neither the cursor stores nor its stack frame, while the
// adversarial path remains generation-correct under copy-on-write.
func lookupStorePageKeyCollisionSuccessors(pages *storeio.PageFile, keyRoot, chunkRoot storeio.PageRef,
	key StorePageKey, prepared *StorePageKey) (StorePageValue, bool, error) {
	var path [storePageKeyMaxDepth - 1]storePageKeyPathEntry
	depth := 0
	ref := keyRoot
	for {
		lease, err := pages.Cache().Pin(ref)
		if err != nil {
			return StorePageValue{}, false, storePageReadError(err)
		}
		view := storeio.AdmittedPageKeyDirectory(lease.Bytes())
		if view.Header().Level == 0 {
			if closeErr := lease.Close(); closeErr != nil {
				return StorePageValue{}, false, closeErr
			}
			break
		}
		child, rank, ok := view.ChildIndex(key.hash)
		if !ok || depth == len(path) {
			_ = lease.Close()
			return StorePageValue{}, false, corruptStorePage("key-collision cursor", storeio.ErrKeyDirectoryCorrupt)
		}
		path[depth] = storePageKeyPathEntry{ref: ref, rank: uint16(rank), level: view.Header().Level}
		depth++
		if closeErr := lease.Close(); closeErr != nil {
			return StorePageValue{}, false, closeErr
		}
		if child.Offset >= ref.Offset {
			return StorePageValue{}, false, corruptStorePage("key-collision child order", storeio.ErrKeyDirectoryCorrupt)
		}
		ref = child
	}

	for {
		ref, ok, err := nextStorePageKeyLeaf(pages, &path, &depth)
		if err != nil || !ok {
			return StorePageValue{}, false, err
		}
		lease, err := pages.Cache().Pin(ref)
		if err != nil {
			return StorePageValue{}, false, storePageReadError(err)
		}
		view := storeio.AdmittedPageKeyDirectory(lease.Bytes())
		if view.Header().Level != 0 {
			_ = lease.Close()
			return StorePageValue{}, false, corruptStorePage("key-collision leaf level", storeio.ErrKeyDirectoryCorrupt)
		}
		first, end, candidates := view.CandidateRange(key.hash)
		for i := first; candidates && i < end; i++ {
			location, _ := view.LocationAt(i)
			docRef, exists, resolveErr := resolveStoreDocumentPage(pages, chunkRoot, location.Chunk)
			if resolveErr != nil {
				_ = lease.Close()
				return StorePageValue{}, false, resolveErr
			}
			if !exists {
				continue
			}
			docLease, pinErr := pages.Cache().Pin(docRef)
			if pinErr != nil {
				_ = lease.Close()
				return StorePageValue{}, false, storePageReadError(pinErr)
			}
			doc := storeio.AdmittedDocumentPage(docLease.Bytes())
			if doc.Header().ChunkID != location.Chunk {
				_ = docLease.Close()
				_ = lease.Close()
				return StorePageValue{}, false, corruptStorePage("collision document identity", storeio.ErrDocumentPageCorrupt)
			}
			if raw, exact := doc.LookupString(location.Slot, key.key); exact {
				if prepared != nil {
					prepared.document = docRef
					prepared.chunk = location.Chunk
					prepared.slot = location.Slot
					prepared.resolved = true
				}
				if closeErr := lease.Close(); closeErr != nil {
					_ = docLease.Close()
					return StorePageValue{}, false, closeErr
				}
				return StorePageValue{lease: docLease, raw: raw}, true, nil
			}
			if closeErr := docLease.Close(); closeErr != nil {
				_ = lease.Close()
				return StorePageValue{}, false, closeErr
			}
		}
		follow := view.Header().MaxHash == key.hash
		if closeErr := lease.Close(); closeErr != nil {
			return StorePageValue{}, false, closeErr
		}
		if !follow {
			return StorePageValue{}, false, nil
		}
	}
}

// nextStorePageKeyLeaf advances one B+tree cursor without following the
// leaf's physical Next hint. Parent pages belong to the selected immutable
// root, so their child references always name the correct COW generation.
// The uncommon collision path may reread branch pages but allocates nothing.
func nextStorePageKeyLeaf(pages *storeio.PageFile,
	path *[storePageKeyMaxDepth - 1]storePageKeyPathEntry, depth *int) (storeio.PageRef, bool, error) {
	for level := *depth - 1; level >= 0; level-- {
		cursor := &path[level]
		lease, err := pages.Cache().Pin(cursor.ref)
		if err != nil {
			return storeio.PageRef{}, false, storePageReadError(err)
		}
		view := storeio.AdmittedPageKeyDirectory(lease.Bytes())
		if view.Header().Level != cursor.level || view.Header().Level == 0 {
			_ = lease.Close()
			return storeio.PageRef{}, false, corruptStorePage("key-successor branch level", storeio.ErrKeyDirectoryCorrupt)
		}
		rank := int(cursor.rank) + 1
		if rank >= view.Len() {
			if closeErr := lease.Close(); closeErr != nil {
				return storeio.PageRef{}, false, closeErr
			}
			continue
		}
		branch, ok := view.BranchAt(rank)
		if !ok {
			_ = lease.Close()
			return storeio.PageRef{}, false, corruptStorePage("key-successor branch rank", storeio.ErrKeyDirectoryCorrupt)
		}
		cursor.rank = uint16(rank)
		parent := cursor.ref
		if closeErr := lease.Close(); closeErr != nil {
			return storeio.PageRef{}, false, closeErr
		}
		if branch.Child.Offset >= parent.Offset {
			return storeio.PageRef{}, false, corruptStorePage("key-successor child order", storeio.ErrKeyDirectoryCorrupt)
		}
		*depth = level + 1
		ref := branch.Child
		expected := cursor.level - 1
		for expected != 0 {
			if *depth == len(path) {
				return storeio.PageRef{}, false, corruptStorePage("key-successor depth", storeio.ErrKeyDirectoryCorrupt)
			}
			lease, err := pages.Cache().Pin(ref)
			if err != nil {
				return storeio.PageRef{}, false, storePageReadError(err)
			}
			view := storeio.AdmittedPageKeyDirectory(lease.Bytes())
			if view.Header().Level != expected {
				_ = lease.Close()
				return storeio.PageRef{}, false, corruptStorePage("key-successor descent level", storeio.ErrKeyDirectoryCorrupt)
			}
			branch, ok := view.BranchAt(0)
			if !ok {
				_ = lease.Close()
				return storeio.PageRef{}, false, corruptStorePage("key-successor first child", storeio.ErrKeyDirectoryCorrupt)
			}
			path[*depth] = storePageKeyPathEntry{ref: ref, level: expected}
			*depth = *depth + 1
			parent = ref
			if closeErr := lease.Close(); closeErr != nil {
				return storeio.PageRef{}, false, closeErr
			}
			if branch.Child.Offset >= parent.Offset {
				return storeio.PageRef{}, false, corruptStorePage("key-successor descent order", storeio.ErrKeyDirectoryCorrupt)
			}
			ref = branch.Child
			expected--
		}
		return ref, true, nil
	}
	return storeio.PageRef{}, false, nil
}

func resolveStoreDocumentPage(pages *storeio.PageFile, chunkRoot storeio.PageRef,
	chunkID uint32) (storeio.PageRef, bool, error) {
	ref := chunkRoot
	expectedShift := uint8(0)
	haveExpectedShift := false
	depth := 0
	for ref != (storeio.PageRef{}) {
		if depth >= 6 {
			return storeio.PageRef{}, false, corruptStorePage("chunk-directory depth", storeio.ErrChunkDirectoryCorrupt)
		}
		depth++
		lease, err := pages.Cache().Pin(ref)
		if err != nil {
			return storeio.PageRef{}, false, storePageReadError(err)
		}
		view := storeio.AdmittedChunkDirectoryPage(lease.Bytes())
		if haveExpectedShift && view.Header().Shift != expectedShift {
			_ = lease.Close()
			return storeio.PageRef{}, false, corruptStorePage("chunk-directory level", storeio.ErrChunkDirectoryCorrupt)
		}
		child, ok := view.Lookup(chunkID)
		if closeErr := lease.Close(); closeErr != nil {
			return storeio.PageRef{}, false, closeErr
		}
		if !ok {
			return storeio.PageRef{}, false, nil
		}
		if child.Kind == storeio.PageDocument {
			if view.Header().Shift != 0 {
				return storeio.PageRef{}, false, corruptStorePage("document above leaf", storeio.ErrChunkDirectoryCorrupt)
			}
			return child, true, nil
		}
		if view.Header().Shift < 6 || child.Offset >= ref.Offset {
			return storeio.PageRef{}, false, corruptStorePage("chunk-directory child order", storeio.ErrChunkDirectoryCorrupt)
		}
		expectedShift = view.Header().Shift - 6
		haveExpectedShift = true
		ref = child
	}
	return storeio.PageRef{}, false, nil
}

// RangeRaw visits exact key and JSON bytes in logical chunk/slot order while
// keeping residency bounded. Both slices borrow one pinned document frame and
// are valid only for the callback. Returning false stops successfully. The
// callback must not retain or modify either slice.
func (r *StorePageReader) RangeRaw(visit func(key, json []byte) bool) error {
	if r == nil || visit == nil {
		return nil
	}
	pages := r.pages.Load()
	if pages == nil {
		return ErrStorePageClosed
	}
	if r.root.DocumentCount == 0 {
		return nil
	}
	type directoryCursor struct {
		ref      storeio.PageRef
		next     int
		count    int
		shift    uint8
		ready    bool
		expected bool
	}
	var stack [6]directoryCursor
	stack[0].ref = r.root.ChunkDirectory
	depth := 1
	var visited uint64
	for depth != 0 {
		cursor := &stack[depth-1]
		if cursor.ready && cursor.next == cursor.count {
			depth--
			continue
		}
		lease, err := pages.Cache().Pin(cursor.ref)
		if err != nil {
			return storePageReadError(err)
		}
		view := storeio.AdmittedChunkDirectoryPage(lease.Bytes())
		if !cursor.ready {
			if cursor.expected && cursor.shift != view.Header().Shift {
				_ = lease.Close()
				return corruptStorePage("enumerated directory level", storeio.ErrChunkDirectoryCorrupt)
			}
			cursor.count = view.Len()
			cursor.shift = view.Header().Shift
			cursor.ready = true
		} else if cursor.count != view.Len() || cursor.shift != view.Header().Shift {
			_ = lease.Close()
			return corruptStorePage("unstable admitted directory", storeio.ErrChunkDirectoryCorrupt)
		}
		rank := cursor.next
		child, ok := view.RefAt(rank)
		cursor.next++
		var chunkID uint32
		if cursor.shift == 0 {
			chunkID, ok = view.ChunkIDAt(rank)
		}
		if !ok || child.Offset >= cursor.ref.Offset {
			_ = lease.Close()
			return corruptStorePage("enumerated child", storeio.ErrChunkDirectoryCorrupt)
		}
		if child.Kind == storeio.PageChunkDirectory {
			if closeErr := lease.Close(); closeErr != nil {
				return closeErr
			}
			if cursor.shift < 6 || depth == len(stack) {
				return corruptStorePage("enumerated depth", storeio.ErrChunkDirectoryCorrupt)
			}
			stack[depth] = directoryCursor{ref: child, shift: cursor.shift - 6, expected: true}
			depth++
			continue
		}
		if child.Kind != storeio.PageDocument || cursor.shift != 0 {
			_ = lease.Close()
			return corruptStorePage("enumerated leaf kind", storeio.ErrChunkDirectoryCorrupt)
		}
		// Keep the leaf pinned across the document visit. With the minimum two
		// frames, CLOCK can then replace only the preceding document instead
		// of rereading the hot 64-way leaf between adjacent chunks.
		docLease, err := pages.Cache().Pin(child)
		if err != nil {
			_ = lease.Close()
			return storePageReadError(err)
		}
		doc := storeio.AdmittedDocumentPage(docLease.Bytes())
		if doc.Header().ChunkID != chunkID {
			_ = docLease.Close()
			_ = lease.Close()
			return corruptStorePage("enumerated document identity", storeio.ErrDocumentPageCorrupt)
		}
		for row := 0; row < doc.Len(); row++ {
			record, ok := doc.RecordAt(row)
			if !ok {
				_ = docLease.Close()
				_ = lease.Close()
				return corruptStorePage("enumerated record", storeio.ErrDocumentPageCorrupt)
			}
			visited++
			if !visit(record.Key, record.JSON) {
				return errors.Join(docLease.Close(), lease.Close())
			}
		}
		if err := docLease.Close(); err != nil {
			_ = lease.Close()
			return err
		}
		if err := lease.Close(); err != nil {
			return err
		}
	}
	if visited != r.root.DocumentCount {
		return corruptStorePage(fmt.Sprintf("enumerated %d documents, root names %d", visited, r.root.DocumentCount),
			storeio.ErrDocumentPageCorrupt)
	}
	return nil
}

// AppendRaw appends exact JSON to dst and releases every frame before return.
func (r *StorePageReader) AppendRaw(dst []byte, key string) ([]byte, bool, error) {
	return r.AppendRawKey(dst, r.CompileKey(key))
}

// AppendRawKey is AppendRaw through a reusable compiled key.
func (r *StorePageReader) AppendRawKey(dst []byte, key StorePageKey) ([]byte, bool, error) {
	value, ok, err := r.ViewRawKey(key)
	if err != nil || !ok {
		return dst, ok, err
	}
	dst = append(dst, value.Bytes()...)
	if err := value.Close(); err != nil {
		return dst, false, err
	}
	return dst, true, nil
}

// Len returns the recovered document count.
func (r *StorePageReader) Len() uint64 {
	if r == nil {
		return 0
	}
	return r.root.DocumentCount
}

// Generation returns the recovered durable generation.
func (r *StorePageReader) Generation() uint64 {
	if r == nil {
		return 0
	}
	return r.root.Generation
}

// DirectIO reports whether explicit direct reads are active.
func (r *StorePageReader) DirectIO() bool {
	if r == nil {
		return false
	}
	pages := r.pages.Load()
	return pages != nil && pages.Direct()
}

// StorePageCacheStats reports bounded residency and page-I/O behavior without
// exposing the internal page subsystem as part of the public API.
type StorePageCacheStats struct {
	CapacityBytes uint64
	ResidentBytes uint64
	ReservedBytes uint64
	FrameSize     uint32
	Frames        uint32
	ReadyFrames   uint32
	LoadingFrames uint32
	FailedFrames  uint32
	PinnedFrames  uint32
	Pins          uint64
	Hits          uint64
	Misses        uint64
	Coalesced     uint64
	PageReads     uint64
	ReadBytes     uint64
	Evictions     uint64
	ReadErrors    uint64
}

// StorePageStats reports the fixed cache budget and page I/O counters.
type StorePageStats struct {
	Documents  uint64
	Generation uint64
	FileBytes  uint64
	DirectIO   bool
	Cache      StorePageCacheStats
}

// Stats returns a non-allocating control-plane snapshot.
func (r *StorePageReader) Stats() StorePageStats {
	if r == nil {
		return StorePageStats{}
	}
	stats := StorePageStats{
		Documents: r.root.DocumentCount, Generation: r.root.Generation,
		FileBytes: r.fileEnd, DirectIO: r.DirectIO(),
	}
	pages := r.pages.Load()
	if pages == nil {
		return stats
	}
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

// Close rejects new reads, waits for live values, then releases frames and the
// file descriptor. It is safe to race with reads and to call more than once.
func (r *StorePageReader) Close() error {
	if r == nil {
		return nil
	}
	pages := r.pages.Swap(nil)
	if pages == nil {
		return nil
	}
	return pages.Close()
}
