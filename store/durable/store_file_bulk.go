package durable

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"os"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// CreateFrom writes a completed in-memory store.Collection as one durable
// generation in an empty file. Keys, documents, schema, and compatible
// exact indexes are preserved without replaying individual mutations: unlike
// repeated Put calls, bulk creation writes each live document, directory
// node and posting stream exactly once, then publishes one
// double-root durability fence. The resulting file opens with [Open] and
// supports ordinary update, delete, and index operations
// immediately.
//
// CreateFrom borrows the collection's selected immutable state while writing.
// It does not retain source slices, and file remains owned by the caller.
// Indexes are rebuilt from options.Indexes even when the source collection has
// no corresponding heap index. A failed call may leave an unpublished partial
// file; as with [WritePageFile], discard it instead of retrying in place.
// CreateFrom is a free function, not a method on [store.Collection], because
// it lives outside store.Collection's own package: it reaches the collection's
// internals only through [store.Collection.WithBulkSnapshot].
func CreateFrom(collection *store.Collection, file *os.File, options Options) (int64, error) {
	if collection == nil || file == nil {
		return 0, fmt.Errorf("vibejson: CreateFrom requires non-nil collection and file")
	}
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() != 0 {
		return 0, ErrNotEmpty
	}
	normalized, err := options.normalized()
	if err != nil {
		return 0, err
	}

	var state *store.State
	var rows []fileStoreBulkRow
	snapshotErr := collection.WithBulkSnapshot(func(snapshot *store.State) error {
		state = snapshot
		r, collectErr := collectFileStoreBulkRows(snapshot, normalized)
		rows = r
		return collectErr
	})
	if snapshotErr != nil {
		return 0, snapshotErr
	}

	var storeID [16]byte
	if _, err := rand.Read(storeID[:]); err != nil {
		return 0, fmt.Errorf("vibejson: create collection identity: %w", err)
	}
	layout, err := storeio.MutableStoreLayout(uint32(normalized.PageSize))
	if err != nil {
		return 0, err
	}
	catalog, err := planFilePageCatalog(
		normalized.pageCatalog, storeID, 1,
		uint32(normalized.PageSize), layout.DataStart,
		storeio.StateRootLogicalID+1,
	)
	if err != nil {
		return 0, err
	}
	build := fileStoreBulkBuild{
		source: state, rows: rows, options: normalized, storeID: storeID,
		catalog: catalog,
		allocator: fileStoreBulkAllocator{
			offset:      catalog.fileEnd,
			nextLogical: catalog.nextID,
			generation:  1,
			pageSize:    uint32(normalized.PageSize),
		},
	}
	if err := build.plan(); err != nil {
		return 0, err
	}
	if err := build.write(file); err != nil {
		return 0, err
	}
	return int64(build.fileEnd), nil
}

type fileStoreBulkRow struct {
	sourceChunk  uint32
	sourceSlot   uint8
	overflowBase int
	overflowN    int
}

func collectFileStoreBulkRows(state *store.State, options normalizedFileStoreOptions) ([]fileStoreBulkRow, error) {
	if state.Count < 0 || uint64(state.Count) > uint64(^uint32(0))*uint64(options.Collection.ChunkDocuments) {
		return nil, store.ErrTooLarge
	}
	rows := make([]fileStoreBulkRow, 0, state.Count)
	var collectErr error
	state.Chunks.Each(func(chunkID uint32, chunk *store.Chunk) bool {
		for live := chunk.Live; live != 0; live &= live - 1 {
			slot := uint8(bits.TrailingZeros64(live))
			key := chunk.Key(int(slot))
			raw := chunk.Docs.RawAt(int(chunk.Ord[slot]))
			if len(key) > options.MaxKeyBytes {
				collectErr = ErrKeyTooLarge
				return false
			}
			if len(raw) > options.MaxDocumentBytes {
				collectErr = ErrDocumentTooLarge
				return false
			}
			row := fileStoreBulkRow{sourceChunk: chunkID, sourceSlot: slot, overflowBase: -1}
			rows = append(rows, row)
		}
		return true
	})
	if collectErr != nil {
		return nil, collectErr
	}
	if len(rows) != state.Count {
		return nil, fmt.Errorf("vibejson: collection bulk source count invariant")
	}
	return rows, nil
}

type fileStoreBulkAllocator struct {
	offset      uint64
	nextLogical uint64
	generation  uint64
	pageSize    uint32
}

func (a *fileStoreBulkAllocator) allocate(kind storeio.PageKind, length uint32) (storeio.PageRef, error) {
	if length < a.pageSize || length%a.pageSize != 0 || a.nextLogical == 0 ||
		a.nextLogical == math.MaxUint64 || a.offset > math.MaxInt64-uint64(length) {
		return storeio.PageRef{}, store.ErrCheckpointTooLarge
	}
	ref := storeio.PageRef{
		Offset: a.offset, LogicalID: a.nextLogical, Generation: a.generation,
		Length: length, Kind: kind,
	}
	a.offset += uint64(length)
	a.nextLogical++
	return ref, nil
}

type fileStoreBulkOverflowPlan struct {
	row        int
	start, end int
	ref, next  storeio.PageRef
}

type fileStoreBulkDocumentPlan struct {
	first, last int
	chunk       uint32
	live        uint64
	ref         storeio.PageRef
	required    int
	overflow    bool
	group       int // group plan index + 1; zero is an ordinary document page
}

type fileStoreBulkDocumentGroupPlan struct {
	first, last             int // logical document-plan range
	columnFirst, columnLast int // non-empty only on the shared sidecar owner
	ref                     storeio.PageRef
	columns                 storeio.PageRef
}

type fileStoreBulkFloat64DirectoryPlan struct {
	level       uint8
	first, last int
	children    []storeio.Float64DirectoryEntry
	ref         storeio.PageRef
}

type fileStoreBulkFloat64StripePlan struct {
	first, last int
	rows        uint32
	ref         storeio.PageRef
}

type fileStoreBulkIndexGroupPlan struct {
	first, last int
	ref, next   storeio.PageRef
}

type fileStoreBulkKeyPlan struct {
	level       uint8
	first, last int
	children    []storeio.PageKeyBranch
	minHash     uint64
	maxHash     uint64
	next        storeio.PageRef
	ref         storeio.PageRef
}

type fileStoreBulkPostingMask struct {
	key        storeio.IndexDirectoryKey
	bits       uint64
	certStart  uint32
	certLength uint16
	collision  bool
}

type fileStoreBulkIndexPlan struct {
	level       uint8
	first, last int
	children    []storeio.IndexDirectoryChild
	ref         storeio.PageRef
}

type fileStoreBulkBuild struct {
	source  *store.State
	rows    []fileStoreBulkRow
	options normalizedFileStoreOptions
	storeID [16]byte
	catalog filePageCatalogPlan

	allocator fileStoreBulkAllocator
	fileEnd   uint64

	overflows            []fileStoreBulkOverflowPlan
	documents            []fileStoreBulkDocumentPlan
	documentGroups       []fileStoreBulkDocumentGroupPlan
	float64Directories   []fileStoreBulkFloat64DirectoryPlan
	float64DirectoryRows []storeio.Float64DirectoryEntry
	float64Stripes       []fileStoreBulkFloat64StripePlan
	float64ScanDocuments int
	indexGroupEntries    []storeio.IndexGroupCatalogEntry
	indexGroupCatalogs   []fileStoreBulkIndexGroupPlan
	indexGroupRef        storeio.PageRef
	indexGroupCovered    uint64
	indexGroupBlocked    uint64
	indexGroupMissing    [64]uint64
	indexGroupFirst      [64]uint64
	chunks               []storeChunkDirectoryPlan
	keys                 []fileStoreBulkKeyPlan
	keyRows              []storeio.PageKeyLocation
	masks                []fileStoreBulkPostingMask
	indexes              []fileStoreBulkIndexPlan
	indexRows            []storeio.IndexDirectoryEntry
	indexCertificates    []byte

	chunkRoot   storeio.PageRef
	keyRoot     storeio.PageRef
	indexRoot   storeio.PageRef
	float64Head storeio.PageRef
	root        storeio.StateRoot

	groupChunks  []storeio.DocumentGroupChunk
	groupRecords []storeio.DocumentGroupRecord
	groupSpans   []storeio.DocumentGroupSpan
	groupMasks   []uint64
	groupValues  []float64
	groupCodec   storeio.DocumentGroupWorkspace

	float64Counts        []uint8
	float64Encodings     []uint8
	float64StripeValues  []byte
	float64StripeColumns []storeio.Float64StripeColumn
}

func (b *fileStoreBulkBuild) sourceRow(row int) (*store.Chunk, string, []byte) {
	entry := b.rows[row]
	chunk := b.source.Chunks.Get(entry.sourceChunk)
	return chunk, chunk.Key(int(entry.sourceSlot)), chunk.Docs.RawAt(int(chunk.Ord[entry.sourceSlot]))
}

func (b *fileStoreBulkBuild) sourceFloat64(row, column int) (float64, bool, error) {
	entry := b.rows[row]
	chunk := b.source.Chunks.Get(entry.sourceChunk)
	sourceRow := [1]int{int(chunk.Ord[entry.sourceSlot])}
	var storage [1]vibejson.RawValue
	values, err := chunk.Docs.AppendPointerRows(
		storage[:0], sourceRow[:], b.options.float64Columns[column].pointer,
	)
	if err != nil || len(values) != 1 {
		return 0, false, err
	}
	value, ok := values[0].Float64()
	if !ok || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false, nil
	}
	return value, true, nil
}

func (b *fileStoreBulkBuild) prepareDocumentGroup(first, last int) ([]storeio.DocumentGroupChunk, error) {
	return b.prepareGroupedChunks(first, last, true)
}

func (b *fileStoreBulkBuild) prepareFloat64Group(first, last int) ([]storeio.DocumentGroupChunk, error) {
	return b.prepareGroupedChunks(first, last, false)
}

// prepareGroupedChunks materializes one bounded encoder view into reusable
// builder storage. Detached typed extents skip keys, JSON, and structural
// spans entirely; document groups retain them. Both paths share column
// extraction and the stable-slot invariants.
func (b *fileStoreBulkBuild) prepareGroupedChunks(first, last int, documents bool) ([]storeio.DocumentGroupChunk, error) {
	chunkCount := last - first
	rowCount := 0
	if documents {
		for i := first; i < last; i++ {
			rowCount += b.documents[i].last - b.documents[i].first
		}
	}
	columnCount := len(b.options.float64Columns)
	if cap(b.groupChunks) < chunkCount {
		b.groupChunks = make([]storeio.DocumentGroupChunk, chunkCount)
	} else {
		b.groupChunks = b.groupChunks[:chunkCount]
		clear(b.groupChunks)
	}
	if cap(b.groupRecords) < rowCount {
		b.groupRecords = make([]storeio.DocumentGroupRecord, 0, rowCount)
	} else {
		b.groupRecords = b.groupRecords[:0]
	}
	b.groupSpans = b.groupSpans[:0]
	maskCount := chunkCount * columnCount
	if cap(b.groupMasks) < maskCount {
		b.groupMasks = make([]uint64, maskCount)
	} else {
		b.groupMasks = b.groupMasks[:maskCount]
		clear(b.groupMasks)
	}
	valueCount := maskCount * 64
	if cap(b.groupValues) < valueCount {
		b.groupValues = make([]float64, valueCount)
	} else {
		b.groupValues = b.groupValues[:valueCount]
		clear(b.groupValues)
	}

	for chunkOrdinal := range chunkCount {
		plan := &b.documents[first+chunkOrdinal]
		if plan.overflow {
			return nil, fmt.Errorf("%w: overflow row in document group", storeio.ErrInvalidWrite)
		}
		recordStart := len(b.groupRecords)
		if documents {
			for row := plan.first; row < plan.last; row++ {
				_, key, raw := b.sourceRow(row)
				spanStart := len(b.groupSpans)
				var err error
				b.groupSpans, err = b.appendDocumentGroupSpans(b.groupSpans, row)
				if err != nil {
					return nil, err
				}
				b.groupRecords = append(b.groupRecords, storeio.DocumentGroupRecord{
					Key: byteview.Bytes(key), JSON: raw,
					Spans: b.groupSpans[spanStart:len(b.groupSpans)], Slot: uint8(row - plan.first),
				})
			}
		}
		maskBase := chunkOrdinal * columnCount
		valueBase := maskBase * 64
		for column := range b.options.float64Columns {
			for row := plan.first; row < plan.last; row++ {
				value, ok, err := b.sourceFloat64(row, column)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
				slot := row - plan.first
				b.groupMasks[maskBase+column] |= uint64(1) << uint(slot)
				b.groupValues[valueBase+column*64+slot] = value
			}
		}
		b.groupChunks[chunkOrdinal] = storeio.DocumentGroupChunk{
			ChunkID: plan.chunk, Live: plan.live,
			Rows: b.groupRecords[recordStart:len(b.groupRecords)],
			Columns: storeio.DocumentFloat64Columns{
				Masks:  b.groupMasks[maskBase : maskBase+columnCount],
				Values: b.groupValues[valueBase : valueBase+columnCount*64],
			},
		}
	}
	return b.groupChunks, nil
}

// appendDocumentGroupSpans reads compact source metadata directly. It never
// widens or caches a classic tape per document.
func (b *fileStoreBulkBuild) appendDocumentGroupSpans(dst []storeio.DocumentGroupSpan, row int) ([]storeio.DocumentGroupSpan, error) {
	entry := b.rows[row]
	chunk := b.source.Chunks.Get(entry.sourceChunk)
	ordinal := int(chunk.Ord[entry.sourceSlot])
	raw := chunk.Docs.RawAt(ordinal)
	start := len(dst)
	if template, ok := chunk.Docs.TemplateAt(ordinal); ok {
		for i := range template.Index.Entries {
			tape := &template.Index.Entries[i]
			if tape.Flags()&vibejson.TapeFlagKey != 0 || tape.Next != 1 {
				continue
			}
			span := chunk.Docs.TemplateSpan(ordinal, template, i)
			dst = append(dst, storeio.DocumentGroupSpan{Start: span & 0xffff, End: span >> 16})
		}
	} else if ref := chunk.Docs.ShapeTapeRefAt(ordinal); ref.Rec != nil {
		if ref.Narrow {
			for field := range ref.Rec.Fields {
				value := chunk.Docs.NarrowAt(ordinal, ref, field)
				dst = append(dst, storeio.DocumentGroupSpan{Start: value.Start(), End: value.End()})
			}
		} else {
			index := chunk.Docs.DocAt(ordinal)
			for i := range index.Entries {
				value := &index.Entries[i]
				dst = append(dst, storeio.DocumentGroupSpan{Start: value.Start, End: value.End})
			}
		}
	} else {
		index := chunk.Docs.DocAt(ordinal)
		if len(index.Entries) == 0 {
			return nil, fmt.Errorf("%w: missing document tape for group encoding", storeio.ErrInvalidWrite)
		}
		for i := range index.Entries {
			tape := &index.Entries[i]
			if tape.Flags()&vibejson.TapeFlagKey == 0 && tape.Next == 1 {
				dst = append(dst, storeio.DocumentGroupSpan{Start: tape.Start, End: tape.End})
			}
		}
	}
	for i := start + 1; i < len(dst); i++ {
		if dst[i-1].End > dst[i].Start || uint64(dst[i].End) > uint64(len(raw)) {
			return nil, fmt.Errorf("%w: unordered document-group spans", storeio.ErrInvalidWrite)
		}
	}
	if start < len(dst) && (dst[start].Start >= dst[start].End ||
		uint64(dst[len(dst)-1].End) > uint64(len(raw))) {
		return nil, fmt.Errorf("%w: invalid document-group span bounds", storeio.ErrInvalidWrite)
	}
	return dst, nil
}

func (b *fileStoreBulkBuild) documentFloat64Bytes(first, last int) (int, error) {
	bytes := len(b.options.float64Columns) * 8
	var counts [fileStoreMaxFloat64Columns]uint8
	var encodings [fileStoreMaxFloat64Columns]uint8
	for column := range b.options.float64Columns {
		for row := first; row < last; row++ {
			value, ok, err := b.sourceFloat64(row, column)
			if err != nil {
				return 0, err
			}
			if ok {
				bytes += 8
				counts[column]++
				encodings[column] = max(encodings[column], fileStoreFloat64Encoding(value))
			}
		}
	}
	columns := len(b.options.float64Columns)
	b.float64Counts = append(b.float64Counts, counts[:columns]...)
	b.float64Encodings = append(b.float64Encodings, encodings[:columns]...)
	return bytes, nil
}

// fileStoreFloat64Encoding orders exact representations from narrowest to
// widest. Three is the general IEEE float64 fallback.
func fileStoreFloat64Encoding(value float64) uint8 {
	switch {
	case math.Signbit(value) || value > math.MaxUint32 || value != math.Trunc(value):
		return 3
	case value <= math.MaxUint8:
		return 0
	case value <= math.MaxUint16:
		return 1
	default:
		return 2
	}
}

func (b *fileStoreBulkBuild) targetLocation(row int) storeio.KeyLocation {
	chunkDocuments := b.options.Collection.ChunkDocuments
	return storeio.KeyLocation{
		Chunk: uint32(row / chunkDocuments), Slot: uint8(row % chunkDocuments),
	}
}

func (b *fileStoreBulkBuild) plan() error {
	if err := b.validateSchema(); err != nil {
		return err
	}
	if err := b.planDocuments(); err != nil {
		return err
	}
	if err := b.planFloat64Directory(); err != nil {
		return err
	}
	items := make([]storeChunkDirectoryItem, len(b.documents))
	for i := range b.documents {
		items[i] = storeChunkDirectoryItem{
			id: b.documents[i].chunk, ref: b.documents[i].ref, zone: b.bulkChunkZone(i),
		}
	}
	var err error
	b.chunks, b.chunkRoot, err = planFileStoreBulkChunkDirectories(items, &b.allocator)
	if err != nil {
		return err
	}
	if err := b.planKeys(); err != nil {
		return err
	}
	if err := b.planPostings(); err != nil {
		return err
	}
	if err := b.planIndexGroups(); err != nil {
		return err
	}
	if err := b.planIndexTree(); err != nil {
		return err
	}
	b.fileEnd = b.allocator.offset

	chunkHighWater := uint32(len(b.documents))
	freeChunkHint := chunkHighWater
	if len(b.rows) != 0 && len(b.rows)%b.options.Collection.ChunkDocuments != 0 {
		freeChunkHint--
	}
	b.root = storeio.StateRoot{
		StoreID: b.storeID, Generation: b.allocator.generation, PageSize: b.allocator.pageSize,
		DocumentCount: uint64(len(b.rows)),
		NextLogicalID: b.allocator.nextLogical, ChunkHighWater: chunkHighWater,
		LiveChunks: chunkHighWater, ChunkDocuments: uint32(b.options.Collection.ChunkDocuments),
		IndexCount: uint32(len(b.options.indexes)), IndexCatalogHash: b.options.indexCatalogHash,
		IndexMaxDepth:    uint32(max(b.options.Collection.IndexOptions.MaxDepth, 0)),
		MaxKeyBytes:      uint32(b.options.MaxKeyBytes),
		InlineValueBytes: uint32(b.options.InlineValueBytes),
		MaxDocumentBytes: uint32(b.options.MaxDocumentBytes),
		FreeChunkHint:    freeChunkHint, ChunkDirectory: b.chunkRoot, KeyDirectory: b.keyRoot,
		IndexDirectory: b.indexRoot, Float64ScanHead: b.float64Head,
		IndexGroupHead: b.indexGroupRef,
	}
	b.root.Options = fileStoreCollectionOptionFlags(b.options.Collection)
	if len(b.options.float64Columns) != 0 {
		b.root.Options |= storeio.StateOptionFloat64Columns
	}
	if b.options.Collection.Schema != nil {
		b.root.Options |= storeio.StateOptionSchema
	}
	if b.options.MaterializationDamageGranule != 0 {
		b.root.Options |= storeio.StateOptionCanonicalMaterialization
		b.root.MaterializationDamageGranule =
			uint32(b.options.MaterializationDamageGranule)
	}
	if err := b.catalog.apply(
		&b.root, uint32(b.options.MaxPageSize),
	); err != nil {
		return err
	}
	return nil
}

func (b *fileStoreBulkBuild) validateSchema() error {
	schema := b.options.Collection.Schema
	if schema == nil {
		return nil
	}
	var (
		rows   [store.MaxChunkDocuments]int
		values [store.MaxChunkDocuments]vibejson.RawValue
	)
	for first := 0; first < len(b.rows); {
		chunkID := b.rows[first].sourceChunk
		chunk := b.source.Chunks.Get(chunkID)
		if chunk == nil {
			return storeio.ErrDocumentPageCorrupt
		}
		last := first
		for last < len(b.rows) &&
			b.rows[last].sourceChunk == chunkID {
			entry := b.rows[last]
			if last-first == len(rows) ||
				chunk.Live&(uint64(1)<<entry.sourceSlot) == 0 {
				return storeio.ErrDocumentPageCorrupt
			}
			rows[last-first] = int(chunk.Ord[entry.sourceSlot])
			last++
		}
		failed, err := schema.ValidateSegmentRows(
			&chunk.Docs, rows[:last-first], values[:0],
		)
		if err != nil {
			if failed < 0 {
				return err
			}
			return fmt.Errorf(
				"vibejson: row %d: %w", first+failed, err,
			)
		}
		first = last
	}
	return nil
}

// A collection chunk lookup has a fixed six-node radix path (shifts
// 30,24,18,12,6,0). The read-only page checkpoint may stop at a shallower
// root, so its otherwise shared planner cannot be used here.
func planFileStoreBulkChunkDirectories(items []storeChunkDirectoryItem, allocator *fileStoreBulkAllocator) ([]storeChunkDirectoryPlan, storeio.PageRef, error) {
	if len(items) == 0 {
		return nil, storeio.PageRef{}, nil
	}
	all := make([]storeChunkDirectoryPlan, 0, (len(items)+62)/63+5)
	for shift := uint8(0); ; shift += 6 {
		next := make([]storeChunkDirectoryItem, 0, (len(items)+63)/64)
		for start := 0; start < len(items); {
			covered := uint(shift) + 6
			group := items[start].id
			if covered < 32 {
				group >>= covered
			} else {
				group = 0
			}
			end := start + 1
			for end < len(items) {
				other := items[end].id
				if covered < 32 {
					other >>= covered
				} else {
					other = 0
				}
				if other != group {
					break
				}
				end++
			}
			children := make([]storeio.PageRef, end-start)
			var zones []storeio.ChunkZone
			if shift == 0 {
				zones = make([]storeio.ChunkZone, end-start)
			}
			var bitmap uint64
			for i := start; i < end; i++ {
				bitmap |= uint64(1) << uint(items[i].id>>shift&63)
				children[i-start] = items[i].ref
				if zones != nil {
					zones[i-start] = items[i].zone
				}
			}
			prefix := items[start].id
			if covered < 32 {
				prefix &^= uint32(1)<<covered - 1
			} else {
				prefix = 0
			}
			ref, err := allocator.allocate(storeio.PageChunkDirectory, allocator.pageSize)
			if err != nil {
				return nil, storeio.PageRef{}, err
			}
			all = append(all, storeChunkDirectoryPlan{
				prefix: prefix, shift: shift, bitmap: bitmap,
				children: children, zones: zones, ref: ref,
			})
			next = append(next, storeChunkDirectoryItem{id: prefix, ref: ref})
			start = end
		}
		// Stop at the first level that collapses to one node. Building on to
		// the uint32 ceiling would leave a spine of single-child nodes that
		// every later copy-on-write Put would have to rewrite, so a bulk file
		// must have exactly the height incremental growth would have reached.
		if len(next) == 1 {
			return all, next[0].ref, nil
		}
		if shift == 30 {
			return nil, storeio.PageRef{}, fmt.Errorf(
				"%w: collection chunk radix root", storeio.ErrInvalidWrite,
			)
		}
		items = next
	}
}

func (b *fileStoreBulkBuild) planDocuments() error {
	if len(b.rows) == 0 {
		return nil
	}
	chunkDocuments := b.options.Collection.ChunkDocuments
	chunkCount := (len(b.rows) + chunkDocuments - 1) / chunkDocuments
	if uint64(chunkCount) > uint64(^uint32(0)) {
		return store.ErrTooLarge
	}
	overflowPayload := b.options.MaxPageSize - storeio.PageHeaderSize -
		storeio.PageTrailerSize - storeio.OverflowPagePayloadHeaderSize
	b.documents = make([]fileStoreBulkDocumentPlan, 0, chunkCount)
	for first := 0; first < len(b.rows); first += chunkDocuments {
		last := min(first+chunkDocuments, len(b.rows))
		chunkID := uint32(first / chunkDocuments)
		required := storeio.PageHeaderSize + storeio.PageTrailerSize +
			storeio.DocumentPagePayloadHeaderSize + (last-first)*storeio.DocumentPageRecordSize
		for row := first; row < last; row++ {
			_, key, raw := b.sourceRow(row)
			required += len(key) + len(raw)
		}
		float64Bytes, err := b.documentFloat64Bytes(first, last)
		if err != nil {
			return err
		}
		required += float64Bytes
		// InlineValueBytes is the ordinary online-write threshold, not a
		// format limit. A compact generation instead keeps complete values in
		// the document extent while the chunk fits, avoiding a 64 KiB overflow
		// extent for a value that only pushed one 4 KiB chunk to 8 KiB. If the
		// chunk is genuinely too large, spill the largest remaining value
		// first; this reaches the bound with the fewest overflow chains.
		var overflowMask uint64
		for required > b.options.MaxPageSize {
			largestRow, largestBytes := -1, -1
			for row := first; row < last; row++ {
				bit := uint64(1) << uint(row-first)
				if overflowMask&bit != 0 {
					continue
				}
				_, _, raw := b.sourceRow(row)
				if len(raw) > largestBytes {
					largestRow, largestBytes = row, len(raw)
				}
			}
			if largestRow < 0 {
				return ErrDocumentTooLarge
			}
			overflowMask |= uint64(1) << uint(largestRow-first)
			required -= largestBytes
			required += storeio.DocumentOverflowDescriptorSize
		}
		for row := first; row < last; row++ {
			if overflowMask&(uint64(1)<<uint(row-first)) == 0 {
				continue
			}
			_, _, raw := b.sourceRow(row)
			base := len(b.overflows)
			for start := 0; start < len(raw); start += overflowPayload {
				end := min(start+overflowPayload, len(raw))
				size, fits := fileStoreBulkPrimaryExtent(
					storeio.PageHeaderSize+storeio.PageTrailerSize+
						storeio.OverflowPagePayloadHeaderSize+end-start,
					b.options.PageSize, b.options.MaxPageSize,
				)
				if !fits {
					return ErrDocumentTooLarge
				}
				ref, err := b.allocator.allocate(storeio.PageOverflow, size)
				if err != nil {
					return err
				}
				b.overflows = append(b.overflows, fileStoreBulkOverflowPlan{
					row: row, start: start, end: end, ref: ref,
				})
			}
			b.rows[row].overflowBase = base
			b.rows[row].overflowN = len(b.overflows) - base
			for i := base; i+1 < len(b.overflows); i++ {
				b.overflows[i].next = b.overflows[i+1].ref
			}
		}
		count := last - first
		live := ^uint64(0)
		if count < 64 {
			live = uint64(1)<<uint(count) - 1
		}
		b.documents = append(b.documents, fileStoreBulkDocumentPlan{
			first: first, last: last, chunk: chunkID, live: live,
			required: required, overflow: overflowMask != 0,
		})
	}
	return b.planDocumentGroups()
}

const (
	fileStoreBulkDocumentGroupRows   = 128
	fileStoreBulkFloat64RegionGroups = 120
)

type fileStoreBulkFloat64Segment struct {
	firstGroup, lastGroup       int
	firstDocument, lastDocument int
	columnSize                  uint32
}

// planDocumentGroups keeps the online stable-slot unit unchanged while
// packing consecutive compact-generation chunks into larger immutable
// extents. A later update redirects only its touched logical chunk to an
// ordinary page; untouched lanes continue sharing the original group.
func (b *fileStoreBulkBuild) planDocumentGroups() error {
	var segments [fileStoreBulkFloat64RegionGroups]fileStoreBulkFloat64Segment
	segmentCount := 0
	regionGroups := 0
	flushColumns := func() error {
		if segmentCount == 0 {
			return nil
		}
		for segmentIndex := 0; segmentIndex < segmentCount; segmentIndex++ {
			segment := segments[segmentIndex]
			columns, err := b.allocator.allocate(storeio.PageFloat64Group, segment.columnSize)
			if err != nil {
				return err
			}
			for groupIndex := segment.firstGroup; groupIndex < segment.lastGroup; groupIndex++ {
				group := &b.documentGroups[groupIndex]
				group.ref, err = storeio.AttachDocumentGroupFloat64Sidecar(
					group.ref, columns, uint32(b.options.PageSize),
				)
				if err != nil {
					return err
				}
				group.columns = columns
				for document := group.first; document < group.last; document++ {
					b.documents[document].ref = group.ref
				}
			}
			owner := &b.documentGroups[segment.firstGroup]
			owner.columnFirst = segment.firstDocument
			owner.columnLast = segment.lastDocument
		}
		clear(segments[:segmentCount])
		segmentCount = 0
		regionGroups = 0
		return nil
	}
	regionFits := func(groupSize, columnSize uint32, newSegment bool) bool {
		endGroups := b.allocator.offset + uint64(groupSize)
		sidecarOffset := endGroups
		maxForward := storeio.DocumentGroupFloat64MaxForwardBytes(uint32(b.options.PageSize))
		for segmentIndex := 0; segmentIndex < segmentCount; segmentIndex++ {
			segment := segments[segmentIndex]
			size := segment.columnSize
			if !newSegment && segmentIndex == segmentCount-1 {
				size = columnSize
			}
			firstOffset := b.documentGroups[segment.firstGroup].ref.Offset
			if sidecarOffset < firstOffset || sidecarOffset-firstOffset > maxForward {
				return false
			}
			sidecarOffset += uint64(size)
		}
		if newSegment &&
			(sidecarOffset < b.allocator.offset ||
				sidecarOffset-b.allocator.offset > maxForward) {
			return false
		}
		return true
	}

	for first := 0; first < len(b.documents); {
		last, rows := first, 0
		for last < len(b.documents) && !b.documents[last].overflow {
			next := b.documents[last].last - b.documents[last].first
			if rows+next > fileStoreBulkDocumentGroupRows {
				break
			}
			rows += next
			last++
		}
		grouped := false
		// Grouping is what makes a bulk file compact, and it is opt-in: with the
		// default verbatim format every chunk falls through to the ordinary
		// single-extent path below, which is the same representation Put
		// produces and the one a scan can read without decoding anything.
		if b.options.DocumentFormat == DocumentFormatCompact && last-first >= 2 {
			chunks, err := b.prepareDocumentGroup(first, last)
			if err != nil {
				return err
			}
			columnSize := uint32(0)
			if len(b.options.float64Columns) != 0 {
				if size, ok := b.float64GroupExtent(first, last); ok {
					columnSize = size
					clearDocumentGroupColumns(chunks)
				}
			}
			groupSize, ok := storeio.DocumentGroupSize(
				chunks, uint32(b.options.PageSize), &b.groupCodec,
			)
			individualBytes := 0
			for i := first; i < last; i++ {
				size, fits := fileStoreBulkPrimaryExtent(
					b.documents[i].required, b.options.PageSize, b.options.MaxPageSize,
				)
				if !fits {
					return ErrDocumentTooLarge
				}
				individualBytes += int(size)
			}
			canGroup := ok && groupSize <= uint32(b.options.MaxPageSize) &&
				int(groupSize)+int(columnSize) < individualBytes
			newSegment := false
			segmentColumnSize := columnSize
			if canGroup && columnSize != 0 {
				newSegment = segmentCount == 0
				if !newSegment {
					current := segments[segmentCount-1]
					combinedSize, combined := b.float64GroupExtent(
						current.firstDocument, last,
					)
					if combined {
						segmentColumnSize = combinedSize
					} else {
						newSegment = true
					}
				}
				if regionGroups == fileStoreBulkFloat64RegionGroups ||
					!regionFits(groupSize, segmentColumnSize, newSegment) {
					if err := flushColumns(); err != nil {
						return err
					}
					newSegment = true
					segmentColumnSize = columnSize
				}
				canGroup = regionFits(groupSize, segmentColumnSize, newSegment)
			}
			if canGroup {
				if columnSize == 0 {
					if err := flushColumns(); err != nil {
						return err
					}
				}
				ref, allocateErr := b.allocator.allocate(storeio.PageDocumentGroup, groupSize)
				if allocateErr != nil {
					return allocateErr
				}
				group := len(b.documentGroups)
				if columnSize != 0 {
					if newSegment {
						if segmentCount == len(segments) {
							return fmt.Errorf("%w: float64 region segments", storeio.ErrInvalidWrite)
						}
						segments[segmentCount] = fileStoreBulkFloat64Segment{
							firstGroup: group, firstDocument: first, columnSize: segmentColumnSize,
						}
						segmentCount++
					}
					current := &segments[segmentCount-1]
					current.lastGroup = group + 1
					current.lastDocument = last
					current.columnSize = segmentColumnSize
					regionGroups++
				}
				b.documentGroups = append(b.documentGroups, fileStoreBulkDocumentGroupPlan{
					first: first, last: last, ref: ref,
				})
				for i := first; i < last; i++ {
					b.documents[i].ref = ref
					b.documents[i].group = group + 1
				}
				first = last
				grouped = true
			}
		}
		if grouped {
			continue
		}
		if err := flushColumns(); err != nil {
			return err
		}
		size, ok := fileStoreBulkPrimaryExtent(
			b.documents[first].required, b.options.PageSize, b.options.MaxPageSize,
		)
		if !ok {
			return ErrDocumentTooLarge
		}
		ref, err := b.allocator.allocate(storeio.PageDocument, size)
		if err != nil {
			return err
		}
		b.documents[first].ref = ref
		first++
	}
	if err := flushColumns(); err != nil {
		return err
	}
	b.prepareFloat64ScanPlan()
	return nil
}

// prepareFloat64ScanPlan records the immutable source range used by the
// aggregate-only stripe. The stripe is independent of document grouping and
// covers ordinary pages, shared sidecars, and overflow-backed documents.
func (b *fileStoreBulkBuild) prepareFloat64ScanPlan() {
	b.float64Head = storeio.PageRef{}
	b.float64DirectoryRows = b.float64DirectoryRows[:0]
	b.float64ScanDocuments = 0
	if len(b.options.float64Columns) != 0 {
		b.float64ScanDocuments = len(b.documents)
	}
}

// planFloat64Directory builds a 64-way ordered tree over immutable packed
// stripes. Nodes stay at PageSize so online path copying never scales with a
// large MaxPageSize; full scans still touch fewer than one directory page per
// 64 stripes.
func (b *fileStoreBulkBuild) planFloat64Directory() error {
	if b.float64ScanDocuments == 0 {
		return nil
	}
	if err := b.planFloat64Stripes(); err != nil {
		return err
	}
	b.float64DirectoryRows = b.float64DirectoryRows[:0]
	for _, stripe := range b.float64Stripes {
		if stripe.first < 0 || stripe.first >= len(b.documents) {
			return storeio.ErrInvalidWrite
		}
		b.float64DirectoryRows = append(
			b.float64DirectoryRows,
			storeio.Float64DirectoryEntry{
				FirstChunk: b.documents[stripe.first].chunk,
				Ref:        stripe.ref,
			},
		)
	}
	if len(b.float64DirectoryRows) == 0 ||
		b.float64DirectoryRows[0].FirstChunk != 0 {
		return storeio.ErrInvalidWrite
	}
	levelStart := 0
	for first := 0; first < len(b.float64DirectoryRows); first += storeio.Float64DirectoryFanout {
		last := min(
			first+storeio.Float64DirectoryFanout,
			len(b.float64DirectoryRows),
		)
		ref, err := b.allocator.allocate(
			storeio.PageFloat64Catalog, b.allocator.pageSize,
		)
		if err != nil {
			return err
		}
		b.float64Directories = append(
			b.float64Directories,
			fileStoreBulkFloat64DirectoryPlan{
				first: first, last: last, ref: ref,
			},
		)
	}
	levelEnd := len(b.float64Directories)
	for level := uint8(1); levelEnd-levelStart > 1; level++ {
		if level > storeio.Float64DirectoryMaxLevel {
			return storeio.ErrFloat64DirectoryDepth
		}
		nextStart := len(b.float64Directories)
		for first := levelStart; first < levelEnd; first += storeio.Float64DirectoryFanout {
			last := min(
				first+storeio.Float64DirectoryFanout, levelEnd,
			)
			children := make(
				[]storeio.Float64DirectoryEntry, last-first,
			)
			for i := first; i < last; i++ {
				children[i-first] = storeio.Float64DirectoryEntry{
					FirstChunk: b.float64DirectoryLower(
						b.float64Directories[i],
					),
					Ref: b.float64Directories[i].ref,
				}
			}
			ref, err := b.allocator.allocate(
				storeio.PageFloat64Catalog, b.allocator.pageSize,
			)
			if err != nil {
				return err
			}
			b.float64Directories = append(
				b.float64Directories,
				fileStoreBulkFloat64DirectoryPlan{
					level: level, children: children, ref: ref,
				},
			)
		}
		levelStart, levelEnd = nextStart, len(b.float64Directories)
	}
	b.float64Head = b.float64Directories[levelStart].ref
	return nil
}

func (b *fileStoreBulkBuild) float64DirectoryLower(
	plan fileStoreBulkFloat64DirectoryPlan,
) uint32 {
	if plan.level == 0 {
		return b.float64DirectoryRows[plan.first].FirstChunk
	}
	return plan.children[0].FirstChunk
}

func (b *fileStoreBulkBuild) planFloat64Stripes() error {
	columns := len(b.options.float64Columns)
	if columns == 0 || b.float64ScanDocuments == 0 {
		return nil
	}
	var counts [fileStoreMaxFloat64Columns]uint64
	var encodings [fileStoreMaxFloat64Columns]uint8
	first := 0
	rows := uint64(0)
	for document := 0; document < b.float64ScanDocuments; {
		required := uint64(
			storeio.PageHeaderSize + storeio.PageTrailerSize +
				storeio.Float64StripePayloadHeaderSize + columns*storeio.Float64StripeColumnSize,
		)
		for column := range columns {
			offset := document*columns + column
			nextCount := counts[column] + uint64(b.float64Counts[offset])
			nextEncoding := max(encodings[column], b.float64Encodings[offset])
			width := [...]uint64{1, 2, 4, 8}[nextEncoding]
			required += nextCount * width
		}
		nextRows := rows + uint64(bits.OnesCount64(b.documents[document].live))
		tooLarge := required > uint64(b.options.MaxPageSize) ||
			uint64(document-first)+1 > math.MaxUint32 ||
			nextRows > math.MaxUint32
		if tooLarge && document > first {
			if err := b.allocateFloat64Stripe(
				first, document, uint32(rows), counts[:columns], encodings[:columns],
			); err != nil {
				return err
			}
			clear(counts[:columns])
			clear(encodings[:columns])
			first = document
			rows = 0
			continue
		}
		if tooLarge {
			return storeio.ErrInvalidWrite
		}
		for column := range columns {
			offset := document*columns + column
			counts[column] += uint64(b.float64Counts[offset])
			encodings[column] = max(encodings[column], b.float64Encodings[offset])
		}
		rows = nextRows
		document++
	}
	return b.allocateFloat64Stripe(
		first, b.float64ScanDocuments, uint32(rows), counts[:columns], encodings[:columns],
	)
}

func (b *fileStoreBulkBuild) allocateFloat64Stripe(
	first, last int,
	rows uint32,
	counts []uint64,
	encodings []uint8,
) error {
	if first < 0 || last <= first || last > b.float64ScanDocuments || rows == 0 ||
		uint64(last-first) > math.MaxUint32 {
		return storeio.ErrInvalidWrite
	}
	required := uint64(
		storeio.PageHeaderSize + storeio.PageTrailerSize +
			storeio.Float64StripePayloadHeaderSize +
			len(counts)*storeio.Float64StripeColumnSize,
	)
	for column, count := range counts {
		required += count * [...]uint64{1, 2, 4, 8}[encodings[column]]
	}
	if required > uint64(b.options.MaxPageSize) {
		return storeio.ErrInvalidWrite
	}
	size, ok := fileStoreBulkExtent(int(required), b.options.PageSize, b.options.MaxPageSize)
	if !ok {
		return storeio.ErrInvalidWrite
	}
	ref, err := b.allocator.allocate(storeio.PageFloat64Stripe, size)
	if err != nil {
		return err
	}
	b.float64Stripes = append(b.float64Stripes, fileStoreBulkFloat64StripePlan{
		first: first, last: last, rows: rows, ref: ref,
	})
	return nil
}

// float64GroupExtent computes the exact adaptive shared-sidecar extent from
// compact count/encoding metadata gathered for ordinary document pages. It
// avoids a second JSON extraction pass and never under-allocates after a
// segment widens from an integer lane to general float64.
func (b *fileStoreBulkBuild) float64GroupExtent(first, last int) (uint32, bool) {
	if first < 0 || last > len(b.documents) || last-first < 2 ||
		len(b.options.float64Columns) == 0 {
		return 0, false
	}
	required := storeio.PageHeaderSize + storeio.PageTrailerSize +
		storeio.Float64GroupPayloadHeaderSize +
		(last-first)*storeio.Float64GroupChunkSize +
		len(b.options.float64Columns)*4 +
		(last-first)*len(b.options.float64Columns)*8
	columns := len(b.options.float64Columns)
	for column := range columns {
		encoding := uint8(0)
		count := 0
		for document := first; document < last; document++ {
			offset := document*columns + column
			encoding = max(encoding, b.float64Encodings[offset])
			count += int(b.float64Counts[offset])
		}
		width := [...]int{1, 2, 4, 8}[encoding]
		required += count * width
	}
	return fileStoreBulkExtent(required, b.options.PageSize, b.options.MaxPageSize)
}

func clearDocumentGroupColumns(chunks []storeio.DocumentGroupChunk) {
	for i := range chunks {
		chunks[i].Columns = storeio.DocumentFloat64Columns{}
	}
}

func fileStoreBulkExtent(required, minimum, maximum int) (uint32, bool) {
	if required < 0 || required > maximum {
		return 0, false
	}
	size := minimum
	for size < required {
		if size > maximum/2 {
			return 0, false
		}
		size <<= 1
	}
	return uint32(size), true
}

// fileStoreBulkPrimaryExtent rounds ordinary document and overflow pages only
// to the Store allocation quantum. Shared groups, typed stripes, catalogs, and
// metadata continue to use fileStoreBulkExtent's power-of-two geometry.
func fileStoreBulkPrimaryExtent(required, minimum, maximum int) (uint32, bool) {
	if required < 0 || minimum <= 0 || required > maximum {
		return 0, false
	}
	size := required / minimum * minimum
	if required%minimum != 0 {
		size += minimum
	}
	if size < minimum {
		size = minimum
	}
	if size > maximum {
		return 0, false
	}
	return uint32(size), true
}

func (b *fileStoreBulkBuild) planKeys() error {
	if len(b.rows) == 0 {
		return nil
	}
	// Duplicate rejection remains an exact full-key operation. Hash routing
	// below is deliberately separate: a StoreID-keyed fingerprint is only a
	// pruning hint, and document pages remain authoritative for equality.
	exactOrder := make([]int, len(b.rows))
	for i := range exactOrder {
		exactOrder[i] = i
	}
	slices.SortFunc(exactOrder, func(a, c int) int {
		_, ak, _ := b.sourceRow(a)
		_, ck, _ := b.sourceRow(c)
		return strings.Compare(ak, ck)
	})
	for i := 1; i < len(exactOrder); i++ {
		_, previous, _ := b.sourceRow(exactOrder[i-1])
		_, current, _ := b.sourceRow(exactOrder[i])
		if previous == current {
			return fmt.Errorf("%w %q", store.ErrDuplicateKey, current)
		}
	}
	exactOrder = nil

	b.keyRows = make([]storeio.PageKeyLocation, len(b.rows))
	for row := range b.rows {
		_, key, _ := b.sourceRow(row)
		location := b.targetLocation(row)
		b.keyRows[row] = storeio.PageKeyLocation{
			Hash: storeio.KeyHash(b.storeID, key), Chunk: location.Chunk,
			Slot: location.Slot,
		}
	}
	slices.SortFunc(b.keyRows, compareFileStoreBulkKeyLocation)
	return b.planFingerprintKeys()
}

func compareFileStoreBulkKeyLocation(a, c storeio.PageKeyLocation) int {
	if a.Hash < c.Hash {
		return -1
	}
	if a.Hash > c.Hash {
		return 1
	}
	if a.Chunk < c.Chunk {
		return -1
	}
	if a.Chunk > c.Chunk {
		return 1
	}
	return int(a.Slot) - int(c.Slot)
}

// planFingerprintKeys builds exactly the same typed tree consumed by online
// point and batch mutations. It is split from planKeys so tests can exercise
// adversarial collision runs without needing to find a SipHash collision.
func (b *fileStoreBulkBuild) planFingerprintKeys() error {
	if len(b.keyRows) == 0 {
		return nil
	}
	for i := 1; i < len(b.keyRows); i++ {
		if compareFileStoreBulkKeyLocation(b.keyRows[i-1], b.keyRows[i]) >= 0 {
			return fmt.Errorf("%w: duplicate fingerprint location", storeio.ErrInvalidWrite)
		}
	}

	leafSpans, ok := fileStoreBulkFingerprintLeafSpans(
		uint32(b.options.PageSize), b.keyRows,
	)
	if !ok {
		return fmt.Errorf("%w: fingerprint leaf occupancy", storeio.ErrInvalidWrite)
	}
	levelStart := 0
	for _, span := range leafSpans {
		part := b.keyRows[span[0]:span[1]]
		ref, err := b.allocator.allocate(
			storeio.PageFingerprintDirectory, b.allocator.pageSize,
		)
		if err != nil {
			return err
		}
		b.keys = append(b.keys, fileStoreBulkKeyPlan{
			first: span[0], last: span[1], minHash: part[0].Hash,
			maxHash: part[len(part)-1].Hash, ref: ref,
		})
	}
	for i := 0; i+1 < len(b.keys); i++ {
		b.keys[i].next = b.keys[i+1].ref
	}
	levelEnd := len(b.keys)

	for level := uint8(1); levelEnd-levelStart > 1; level++ {
		if level > 15 {
			return storeio.ErrKeyTreeDepth
		}
		spans, ok := fileStoreBulkFingerprintBranchSpans(
			uint32(b.options.PageSize), levelEnd-levelStart,
		)
		if !ok {
			return fmt.Errorf("%w: fingerprint branch occupancy", storeio.ErrInvalidWrite)
		}
		nextStart := len(b.keys)
		for _, span := range spans {
			first := levelStart + span[0]
			last := levelStart + span[1]
			children := make([]storeio.PageKeyBranch, last-first)
			for child := first; child < last; child++ {
				children[child-first] = storeio.PageKeyBranch{
					MaxHash: b.keys[child].maxHash, Child: b.keys[child].ref,
				}
			}
			ref, err := b.allocator.allocate(
				storeio.PageFingerprintDirectory, b.allocator.pageSize,
			)
			if err != nil {
				return err
			}
			b.keys = append(b.keys, fileStoreBulkKeyPlan{
				level: level, children: children,
				minHash: b.keys[first].minHash, maxHash: b.keys[last-1].maxHash,
				ref: ref,
			})
		}
		levelStart, levelEnd = nextStart, len(b.keys)
	}
	b.keyRoot = b.keys[levelStart].ref
	return nil
}

func fileStoreBulkFingerprintLeafSpans(
	pageSize uint32, entries []storeio.PageKeyLocation,
) ([][2]int, bool) {
	if len(entries) == 0 {
		return nil, true
	}
	if storeio.PageKeyLeafEncodedSize(entries) <= int(pageSize) {
		return [][2]int{{0, len(entries)}}, true
	}

	// Every output is now a non-root leaf. The bound mirrors the online
	// The fixed-width leaf layout gives every entry the same space cost.
	usable := int(pageSize) - storeio.PageHeaderSize -
		storeio.PageTrailerSize - storeio.PageKeyDirectoryPayloadHeaderSize
	if usable <= 0 {
		return nil, false
	}
	minimum := usable / 2
	if minimum < 0 {
		return nil, false
	}

	bodyBytes := func(first, last int) int {
		return (last - first) * storeio.PageKeyLeafEntrySize
	}

	// Suffix dynamic programming prevents the classic greedy underfull tail.
	// For equal page counts the widest left page wins, making output stable.
	n := len(entries)
	impossible := n + 1
	best := make([]int, n+1)
	next := make([]int, n+1)
	for i := range best {
		best[i] = impossible
	}
	best[n] = 0
	for first := n - 1; first >= 0; first-- {
		for last := first + 1; last <= n; last++ {
			body := bodyBytes(first, last)
			if storeio.PageHeaderSize+storeio.PageTrailerSize+
				storeio.PageKeyDirectoryPayloadHeaderSize+body > int(pageSize) {
				break
			}
			if body < minimum || best[last] == impossible {
				continue
			}
			if best[last]+1 <= best[first] {
				best[first] = best[last] + 1
				next[first] = last
			}
		}
	}
	if best[0] == impossible {
		return nil, false
	}
	spans := make([][2]int, 0, best[0])
	for first := 0; first < n; first = next[first] {
		if next[first] <= first {
			return nil, false
		}
		spans = append(spans, [2]int{first, next[first]})
	}
	return spans, true
}

func fileStoreBulkFingerprintBranchSpans(
	pageSize uint32, count int,
) ([][2]int, bool) {
	if count <= 0 {
		return nil, count == 0
	}
	capacity := (int(pageSize) - storeio.PageHeaderSize -
		storeio.PageTrailerSize - storeio.PageKeyDirectoryPayloadHeaderSize) /
		storeio.PageKeyBranchEntrySize
	capacity = min(capacity, int(^uint16(0)))
	if capacity < 2 {
		return nil, false
	}
	// A single branch is the root and is exempt from half fill. Multiple
	// outputs are balanced instead of greedily leaving a short tail.
	pages := (count + capacity - 1) / capacity
	minimum := (capacity + 1) / 2
	if pages > 1 && count < pages*minimum {
		return nil, false
	}
	base, extra := count/pages, count%pages
	spans := make([][2]int, pages)
	first := 0
	for i := range spans {
		length := base
		if i < extra {
			length++
		}
		if length > capacity || pages > 1 && length < minimum {
			return nil, false
		}
		spans[i] = [2]int{first, first + length}
		first += length
	}
	return spans, true
}

func (b *fileStoreBulkBuild) planPostings() error {
	if len(b.options.indexes) == 0 || len(b.rows) == 0 {
		return nil
	}
	if len(b.rows) > int(^uint(0)>>1)/len(b.options.indexes) {
		return store.ErrTooLarge
	}
	b.masks = make([]fileStoreBulkPostingMask, 0, len(b.rows)*len(b.options.indexes))
	var textScratch []byte
	for row := range b.rows {
		chunk, _, _ := b.sourceRow(row)
		location := b.targetLocation(row)
		for indexID, exact := range b.options.indexes {
			hash, ok, values, scratch, err := fileStoreBulkTupleHash(
				exact, chunk, int(b.rows[row].sourceSlot), textScratch[:0],
			)
			textScratch = scratch
			if err != nil {
				return err
			}
			if !ok {
				if exact.N == 1 {
					if len(values[0].Bytes()) == 0 {
						if b.indexGroupMissing[indexID] == 0 {
							b.indexGroupFirst[indexID] =
								uint64(location.Chunk)<<6 | uint64(location.Slot)
						}
						b.indexGroupMissing[indexID]++
					} else {
						b.indexGroupBlocked |= uint64(1) << uint(indexID)
					}
				}
				continue
			}
			mask := fileStoreBulkPostingMask{
				key: storeio.IndexDirectoryKey{
					IndexID: uint32(indexID), TupleHash: hash, Chunk: location.Chunk,
				},
				bits: uint64(1) << location.Slot,
			}
			maxCertificate := storeio.IndexDirectoryMaxCertificate(
				uint32(b.options.PageSize),
			)
			certificateStart := len(b.indexCertificates)
			certificates, certified := appendFileIndexCertificate(
				b.indexCertificates, values[:exact.N], maxCertificate,
			)
			certificateLength := len(certificates) - certificateStart
			if certified && uint64(certificateStart) <= uint64(^uint32(0)) &&
				certificateLength <= int(^uint16(0)) {
				b.indexCertificates = certificates
				mask.certStart = uint32(certificateStart)
				mask.certLength = uint16(certificateLength)
			}
			b.masks = append(b.masks, mask)
		}
	}
	slices.SortFunc(b.masks, func(a, c fileStoreBulkPostingMask) int {
		return compareFileStoreBulkIndexKey(a.key, c.key)
	})
	if len(b.masks) != 0 {
		out := b.masks[:1]
		for _, entry := range b.masks[1:] {
			last := &out[len(out)-1]
			if compareFileStoreBulkIndexKey(last.key, entry.key) == 0 {
				last.bits |= entry.bits
				if last.certLength != 0 && entry.certLength != 0 {
					left := b.indexCertificates[last.certStart : last.certStart+uint32(last.certLength) : last.certStart+uint32(last.certLength)]
					right := b.indexCertificates[entry.certStart : entry.certStart+uint32(entry.certLength) : entry.certStart+uint32(entry.certLength)]
					columns := int(b.options.indexes[last.key.IndexID].N)
					if !fileIndexCertificatesEqual(left, right, columns) {
						last.collision = true
					}
				} else {
					last.certStart, last.certLength, last.collision = 0, 0, false
				}
				continue
			}
			out = append(out, entry)
		}
		clear(b.masks[len(out):])
		b.masks = out
	}

	// A merged mask is already the whole answer for its (index, hash, chunk)
	// triple, so it becomes one routing record directly. The compact and online
	// generations therefore produce byte-identical leaves for the same content,
	// which is what removes the base/delta distinction that used to keep whole
	// pages alive for a single 64-bit word.
	b.indexRows = slices.Grow(b.indexRows, len(b.masks))
	for _, mask := range b.masks {
		flags := uint16(0)
		if mask.collision {
			flags |= storeio.IndexEntryCollision
		}
		b.indexRows = append(b.indexRows, storeio.IndexDirectoryEntry{
			Key: mask.key, Bits: mask.bits, Flags: flags,
			Kind: storeio.IndexEntryInlineMask,
			Cert: storeio.CertSpan{Offset: mask.certStart, Length: mask.certLength},
		})
	}
	return nil
}

// planIndexGroups condenses scalar single-column exact indexes into linked,
// bounded aggregate-only pages. It reuses posting certificates and never adds
// per-row storage. High cardinality grows the number of pages, not one giant
// extent; only a single representative that cannot fit MaxPageSize declines.
// Containers and uncertified collisions retain the streaming exact-index
// execution path.
func (b *fileStoreBulkBuild) planIndexGroups() error {
	if len(b.rows) == 0 || len(b.options.indexes) == 0 {
		return nil
	}
	b.indexCertificates = slices.Grow(b.indexCertificates, 4*len(b.options.indexes))
	maskAt := 0
	for indexID, exact := range b.options.indexes {
		for maskAt < len(b.masks) && b.masks[maskAt].key.IndexID < uint32(indexID) {
			maskAt++
		}
		firstMask := maskAt
		for maskAt < len(b.masks) && b.masks[maskAt].key.IndexID == uint32(indexID) {
			maskAt++
		}
		if exact == nil || exact.N != 1 ||
			b.indexGroupBlocked&(uint64(1)<<uint(indexID)) != 0 {
			continue
		}

		entryStart := len(b.indexGroupEntries)
		eligible := true
		indexedRows := uint64(0)
		var (
			haveHash  bool
			hash      uint64
			hashStart int
		)
		for position := firstMask; position < maskAt; position++ {
			mask := b.masks[position]
			if mask.collision || mask.certLength == 0 {
				eligible = false
				break
			}
			certificate := b.indexCertificates[mask.certStart : mask.certStart+uint32(mask.certLength) : mask.certStart+uint32(mask.certLength)]
			if !fileIndexCertificateValid(certificate, 1) {
				return storeio.ErrInvalidWrite
			}
			if !haveHash || hash != mask.key.TupleHash {
				haveHash = true
				hash = mask.key.TupleHash
				hashStart = len(b.indexGroupEntries)
			}
			group := -1
			for candidate := hashStart; candidate < len(b.indexGroupEntries); candidate++ {
				if fileIndexCertificatesEqual(
					b.indexGroupEntries[candidate].Value, certificate, 1,
				) {
					group = candidate
					break
				}
			}
			rows := uint64(bits.OnesCount64(mask.bits))
			if rows == 0 || indexedRows > ^uint64(0)-rows {
				return storeio.ErrInvalidWrite
			}
			first := uint64(mask.key.Chunk)<<6 |
				uint64(bits.TrailingZeros64(mask.bits))
			if group < 0 {
				b.indexGroupEntries = append(
					b.indexGroupEntries,
					storeio.IndexGroupCatalogEntry{
						IndexID: uint32(indexID), Value: certificate,
						Count: rows, First: first,
					},
				)
			} else {
				entry := &b.indexGroupEntries[group]
				if entry.Count > ^uint64(0)-rows {
					return storeio.ErrInvalidWrite
				}
				entry.Count += rows
				entry.First = min(entry.First, first)
			}
			indexedRows += rows
		}
		missing := b.indexGroupMissing[indexID]
		if !eligible || indexedRows > uint64(len(b.rows)) ||
			missing != uint64(len(b.rows))-indexedRows {
			b.indexGroupEntries = b.indexGroupEntries[:entryStart]
			continue
		}
		if missing != 0 {
			group := -1
			for candidate := entryStart; candidate < len(b.indexGroupEntries); candidate++ {
				if fileIndexCertificatesEqual(
					b.indexGroupEntries[candidate].Value, []byte("null"), 1,
				) {
					group = candidate
					break
				}
			}
			if group < 0 {
				start := len(b.indexCertificates)
				b.indexCertificates = append(b.indexCertificates, "null"...)
				b.indexGroupEntries = append(
					b.indexGroupEntries,
					storeio.IndexGroupCatalogEntry{
						IndexID: uint32(indexID),
						Value:   b.indexCertificates[start : start+4 : start+4],
						Count:   missing, First: b.indexGroupFirst[indexID],
					},
				)
			} else {
				entry := &b.indexGroupEntries[group]
				entry.Count += missing
				entry.First = min(entry.First, b.indexGroupFirst[indexID])
			}
		}

		for _, entry := range b.indexGroupEntries[entryStart:] {
			size, err := storeio.IndexGroupCatalogEntryEncodedSize(entry)
			if err != nil ||
				storeio.PageHeaderSize+storeio.PageTrailerSize+
					storeio.SegmentedIndexGroupCatalogPayloadHeaderSize+
					size > b.options.MaxPageSize {
				eligible = false
				break
			}
		}
		if !eligible {
			b.indexGroupEntries = b.indexGroupEntries[:entryStart]
			continue
		}
		b.indexGroupCovered |= uint64(1) << uint(indexID)
	}
	if b.indexGroupCovered == 0 {
		return nil
	}
	payloadLimit := b.options.MaxPageSize -
		storeio.PageHeaderSize - storeio.PageTrailerSize
	for first := 0; first < len(b.indexGroupEntries); {
		used := storeio.SegmentedIndexGroupCatalogPayloadHeaderSize
		last := first
		for last < len(b.indexGroupEntries) {
			size, err := storeio.IndexGroupCatalogEntryEncodedSize(
				b.indexGroupEntries[last],
			)
			if err != nil {
				return err
			}
			if used > payloadLimit-size {
				break
			}
			used += size
			last++
		}
		if last == first {
			return storeio.ErrInvalidWrite
		}
		size, ok := fileStoreBulkExtent(
			storeio.PageHeaderSize+storeio.PageTrailerSize+used,
			b.options.PageSize, b.options.MaxPageSize,
		)
		if !ok {
			return storeio.ErrInvalidWrite
		}
		ref, err := b.allocator.allocate(
			storeio.PageIndexGroupCatalog, size,
		)
		if err != nil {
			return err
		}
		if len(b.indexGroupCatalogs) != 0 {
			b.indexGroupCatalogs[len(b.indexGroupCatalogs)-1].next = ref
		}
		b.indexGroupCatalogs = append(
			b.indexGroupCatalogs,
			fileStoreBulkIndexGroupPlan{
				first: first, last: last, ref: ref,
			},
		)
		first = last
	}
	b.indexGroupRef = b.indexGroupCatalogs[0].ref
	return nil
}

// fileStoreBulkTupleHash extracts directly from compact collection chunks. It
// avoids widening shape tapes into one cached classic Index per row while
// producing the same process-independent hash used by collection probes.
func fileStoreBulkTupleHash(exact *store.ExactIndex, chunk *store.Chunk, slot int, textScratch []byte) (uint64, bool, [store.MaxIndexColumns]vibejson.RawValue, []byte, error) {
	var values [store.MaxIndexColumns]vibejson.RawValue
	if !store.ExtractIndexColumns(chunk, slot, exact, &values) {
		return 0, false, values, textScratch, nil
	}
	hash := uint64(14695981039346656037)
	for _, raw := range values[:exact.N] {
		hash = fileIndexHashBytes(hash, []byte{byte(raw.Kind()), 0xff})
		switch raw.Kind() {
		case document.Null:
		case document.Bool:
			value, _ := raw.Bool()
			if value {
				hash = fileIndexHashBytes(hash, []byte{1})
			} else {
				hash = fileIndexHashBytes(hash, []byte{0})
			}
		case document.Number:
			if value, ok := raw.Float64(); ok {
				if value == 0 {
					value = 0
				}
				var encoded [8]byte
				binary.LittleEndian.PutUint64(encoded[:], math.Float64bits(value))
				hash = fileIndexHashBytes(hash, encoded[:])
			} else {
				hash = fileIndexHashBytes(hash, []byte{0x7f})
			}
		case document.String:
			if text, clean := raw.StringBytes(); clean {
				hash = fileIndexHashBytes(hash, text)
			} else {
				text, ok, err := raw.AppendText(textScratch[:0])
				if err != nil || !ok {
					return 0, false, values, textScratch, err
				}
				textScratch = text
				hash = fileIndexHashBytes(hash, text)
			}
		default:
			return 0, false, values, textScratch, nil
		}
		hash = fileIndexHashBytes(hash, []byte{0xfe})
	}
	return hash, true, values, textScratch, nil
}

func compareFileStoreBulkIndexKey(a, b storeio.IndexDirectoryKey) int {
	if a.IndexID < b.IndexID {
		return -1
	}
	if a.IndexID > b.IndexID {
		return 1
	}
	if a.TupleHash < b.TupleHash {
		return -1
	}
	if a.TupleHash > b.TupleHash {
		return 1
	}
	if a.Chunk < b.Chunk {
		return -1
	}
	if a.Chunk > b.Chunk {
		return 1
	}
	return 0
}

func (b *fileStoreBulkBuild) planIndexTree() error {
	if len(b.indexRows) == 0 {
		return nil
	}
	branchCapacity := min(64, (b.options.PageSize-storeio.PageHeaderSize-storeio.PageTrailerSize-
		storeio.IndexDirectoryPayloadHeaderSize)/storeio.IndexDirectoryBranchRecordSize)
	if branchCapacity < 2 {
		return storeio.ErrInvalidWrite
	}
	levelStart := 0
	// Leaf occupancy is a byte question: a leaf carries its entries'
	// certificates, and one run of equal representatives costs one copy while
	// alternating ones cost a copy each. Dividing by a fixed record capacity
	// would overflow the page for anything but empty certificates.
	for first := 0; first < len(b.indexRows); {
		count, err := storeio.IndexDirectoryLeafPrefix(
			b.indexRows[first:], b.indexCertificates, uint32(b.options.PageSize),
		)
		if err != nil {
			return err
		}
		last := first + count
		ref, err := b.allocator.allocate(storeio.PageIndexDirectory, b.allocator.pageSize)
		if err != nil {
			return err
		}
		b.indexes = append(b.indexes, fileStoreBulkIndexPlan{first: first, last: last, ref: ref})
		first = last
	}
	levelEnd := len(b.indexes)
	for level := uint8(1); levelEnd-levelStart > 1; level++ {
		if level > 10 {
			return storeio.ErrIndexTreeDepth
		}
		nextStart := len(b.indexes)
		for first := levelStart; first < levelEnd; first += branchCapacity {
			last := min(first+branchCapacity, levelEnd)
			children := make([]storeio.IndexDirectoryChild, last-first)
			for i := first; i < last; i++ {
				children[i-first] = storeio.IndexDirectoryChild{
					Lower: b.indexPlanLower(b.indexes[i]), Ref: b.indexes[i].ref,
				}
			}
			ref, err := b.allocator.allocate(storeio.PageIndexDirectory, b.allocator.pageSize)
			if err != nil {
				return err
			}
			b.indexes = append(b.indexes, fileStoreBulkIndexPlan{
				level: level, children: children, ref: ref,
			})
		}
		levelStart, levelEnd = nextStart, len(b.indexes)
	}
	b.indexRoot = b.indexes[levelStart].ref
	return nil
}

func (b *fileStoreBulkBuild) indexPlanLower(plan fileStoreBulkIndexPlan) storeio.IndexDirectoryKey {
	if plan.level == 0 {
		return b.indexRows[plan.first].Key
	}
	return plan.children[0].Lower
}

func (b *fileStoreBulkBuild) write(file *os.File) error {
	if err := file.Truncate(int64(b.fileEnd)); err != nil {
		return err
	}
	scratch := make([]byte, b.options.MaxPageSize)
	if err := b.catalog.write(
		file, b.fileEnd, b.allocator.nextLogical, scratch,
	); err != nil {
		return err
	}
	if err := b.writeOverflowPages(file, scratch); err != nil {
		return err
	}
	if err := b.writeDocumentPages(file, scratch); err != nil {
		return err
	}
	if err := b.writeFloat64StripePages(file, scratch); err != nil {
		return err
	}
	if err := b.writeFloat64DirectoryPages(file, scratch); err != nil {
		return err
	}
	if err := b.writeIndexGroupCatalogPages(file, scratch); err != nil {
		return err
	}
	for _, plan := range b.chunks {
		page, err := storeio.EncodeChunkDirectoryZonePage(scratch[:b.options.PageSize], storeio.ChunkDirectoryHeader{
			StoreID: b.storeID, Generation: b.allocator.generation, LogicalID: plan.ref.LogicalID,
			PageSize: b.allocator.pageSize, Prefix: plan.prefix, Bitmap: plan.bitmap, Shift: plan.shift,
		}, plan.children, plan.zones, b.fileEnd, b.allocator.nextLogical)
		if err != nil {
			return err
		}
		if err := writeStorePageAt(file, page, plan.ref.Offset); err != nil {
			return err
		}
	}
	if err := b.writeKeyPages(file, scratch); err != nil {
		return err
	}
	if err := b.writeIndexPages(file, scratch); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	inline := storeio.InlineSuperblock{
		StoreID: b.storeID, Generation: b.allocator.generation,
		FileEnd: b.fileEnd, PageSize: b.allocator.pageSize, State: b.root,
		FreeDelta: storeio.NewInlineFreeDelta(storeio.PageRef{}, storeio.PageRef{}),
	}
	rootPage := scratch[:b.options.PageSize]
	clear(rootPage)
	if _, err := storeio.EncodeInlineSuperblock(rootPage, inline); err != nil {
		return err
	}
	rootOffset, err := storeio.SuperblockOffset(b.allocator.generation, b.allocator.pageSize)
	if err != nil {
		return err
	}
	if err := writeStorePageAt(file, rootPage, uint64(rootOffset)); err != nil {
		return err
	}
	return file.Sync()
}

func (b *fileStoreBulkBuild) writeIndexGroupCatalogPages(
	file *os.File,
	scratch []byte,
) error {
	for _, plan := range b.indexGroupCatalogs {
		header := storeio.IndexGroupCatalogHeader{
			StoreID: b.storeID, Generation: b.allocator.generation,
			LogicalID: plan.ref.LogicalID, PageSize: plan.ref.Length,
			CoveredIndexes: b.indexGroupCovered,
			DocumentCount:  uint64(len(b.rows)), Next: plan.next,
		}
		var page []byte
		var err error
		if len(b.indexGroupCatalogs) == 1 {
			page, err = storeio.EncodeIndexGroupCatalogPage(
				scratch[:plan.ref.Length], header,
				b.indexGroupEntries[plan.first:plan.last],
				uint32(len(b.options.indexes)),
				uint32(len(b.documents)),
				uint32(b.options.Collection.ChunkDocuments),
			)
		} else {
			page, err = storeio.EncodeSegmentedIndexGroupCatalogPage(
				scratch[:plan.ref.Length], header,
				b.indexGroupEntries[plan.first:plan.last],
				uint32(len(b.options.indexes)),
				uint32(len(b.documents)),
				uint32(b.options.Collection.ChunkDocuments),
				b.fileEnd, b.allocator.nextLogical,
				b.allocator.pageSize,
			)
		}
		if err != nil {
			return err
		}
		if err := writeStorePageAt(file, page, plan.ref.Offset); err != nil {
			return err
		}
	}
	return nil
}

func (b *fileStoreBulkBuild) writeOverflowPages(file *os.File, scratch []byte) error {
	for _, plan := range b.overflows {
		_, _, raw := b.sourceRow(plan.row)
		location := b.targetLocation(plan.row)
		page, err := storeio.EncodeOverflowPage(scratch[:b.options.MaxPageSize], storeio.OverflowPageHeader{
			StoreID: b.storeID, Generation: b.allocator.generation, LogicalID: plan.ref.LogicalID,
			PageSize: plan.ref.Length, Chunk: location.Chunk, Slot: location.Slot,
			Total: uint64(len(raw)), Offset: uint64(plan.start), Next: plan.next,
		}, raw[plan.start:plan.end], b.fileEnd, b.allocator.nextLogical,
			b.allocator.pageSize, uint32(len(b.documents)), uint8(b.options.Collection.ChunkDocuments))
		if err != nil {
			return err
		}
		if err := writeStorePageAt(file, page, plan.ref.Offset); err != nil {
			return err
		}
	}
	return nil
}

func (b *fileStoreBulkBuild) writeDocumentPages(file *os.File, scratch []byte) error {
	var storage [store.MaxChunkDocuments]storeio.DocumentRecord
	masks := make([]uint64, len(b.options.float64Columns))
	values := make([]float64, len(b.options.float64Columns)*64)
	for document := range b.documents {
		plan := b.documents[document]
		if plan.group != 0 {
			group := b.documentGroups[plan.group-1]
			if document != group.first {
				continue
			}
			if group.columnLast > group.columnFirst {
				columnChunks, prepareErr := b.prepareFloat64Group(
					group.columnFirst, group.columnLast,
				)
				if prepareErr != nil {
					return prepareErr
				}
				page, encodeErr := storeio.EncodeFloat64Group(
					scratch[:group.columns.Length],
					storeio.Float64GroupHeader{
						StoreID: b.storeID, Generation: b.allocator.generation,
						LogicalID: group.columns.LogicalID, PageSize: group.columns.Length,
						FirstChunk: columnChunks[0].ChunkID, ChunkCount: uint16(len(columnChunks)),
						RowCount:    uint16(groupRows(columnChunks)),
						ColumnCount: uint16(len(b.options.float64Columns)),
					},
					columnChunks, b.allocator.nextLogical,
				)
				if encodeErr != nil {
					return encodeErr
				}
				if writeErr := writeStorePageAt(file, page, group.columns.Offset); writeErr != nil {
					return writeErr
				}
			}
			chunks, err := b.prepareDocumentGroup(group.first, group.last)
			if err != nil {
				return err
			}
			columnCount := len(b.options.float64Columns)
			if group.columns != (storeio.PageRef{}) {
				clearDocumentGroupColumns(chunks)
				columnCount = 0
			}
			page, err := storeio.EncodeDocumentGroup(
				scratch[:group.ref.Length],
				storeio.DocumentGroupHeader{
					StoreID: b.storeID, Generation: b.allocator.generation,
					LogicalID: group.ref.LogicalID, PageSize: group.ref.Length,
					FirstChunk: chunks[0].ChunkID, ChunkCount: uint16(len(chunks)),
					RowCount:    uint16(groupRows(chunks)),
					ColumnCount: uint16(columnCount), Flags: uint16(group.ref.Flags),
				},
				chunks, b.allocator.nextLogical, &b.groupCodec,
			)
			if err != nil {
				return err
			}
			if err := writeStorePageAt(file, page, group.ref.Offset); err != nil {
				return err
			}
			continue
		}
		rows := storage[:plan.last-plan.first]
		for i := range rows {
			rowIndex := plan.first + i
			_, key, raw := b.sourceRow(rowIndex)
			record := storeio.DocumentRecord{Key: byteview.Bytes(key), Slot: uint8(i)}
			if b.rows[rowIndex].overflowN == 0 {
				record.JSON = raw
			} else {
				record.Overflow = b.overflows[b.rows[rowIndex].overflowBase].ref
				record.JSONLength = uint64(len(raw))
			}
			rows[i] = record
		}
		clear(masks)
		for column := range b.options.float64Columns {
			for i := range rows {
				value, ok, err := b.sourceFloat64(plan.first+i, column)
				if err != nil {
					return err
				}
				if !ok {
					continue
				}
				masks[column] |= uint64(1) << uint(i)
				values[column*64+i] = value
			}
		}
		page, err := storeio.EncodeDocumentPageWithColumns(scratch[:plan.ref.Length], storeio.DocumentPageHeader{
			StoreID: b.storeID, Generation: b.allocator.generation, LogicalID: plan.ref.LogicalID,
			PageSize: plan.ref.Length, ChunkID: plan.chunk, Live: plan.live,
		}, rows, storeio.DocumentFloat64Columns{Masks: masks, Values: values},
			b.allocator.nextLogical, b.fileEnd, b.allocator.pageSize)
		if err != nil {
			return err
		}
		if err := writeStorePageAt(file, page, plan.ref.Offset); err != nil {
			return err
		}
		clear(rows)
	}
	return nil
}

func (b *fileStoreBulkBuild) writeFloat64DirectoryPages(
	file *os.File,
	scratch []byte,
) error {
	for _, plan := range b.float64Directories {
		header := storeio.Float64DirectoryHeader{
			StoreID: b.storeID, Generation: b.allocator.generation,
			LogicalID: plan.ref.LogicalID, PageSize: plan.ref.Length,
			Level: plan.level,
		}
		var page []byte
		var err error
		if plan.level == 0 {
			page, err = storeio.EncodeFloat64DirectoryLeaf(
				scratch[:plan.ref.Length], header,
				b.float64DirectoryRows[plan.first:plan.last],
				b.fileEnd, b.allocator.nextLogical,
				b.allocator.pageSize,
			)
		} else {
			page, err = storeio.EncodeFloat64DirectoryBranch(
				scratch[:plan.ref.Length], header, plan.children,
				b.fileEnd, b.allocator.nextLogical,
				b.allocator.pageSize,
			)
		}
		if err != nil {
			return err
		}
		if err := writeStorePageAt(file, page, plan.ref.Offset); err != nil {
			return err
		}
	}
	return nil
}

func (b *fileStoreBulkBuild) writeFloat64StripePages(file *os.File, scratch []byte) error {
	columns := len(b.options.float64Columns)
	var starts [fileStoreMaxFloat64Columns]int
	var ends [fileStoreMaxFloat64Columns]int
	if cap(b.float64StripeColumns) < columns {
		b.float64StripeColumns = make([]storeio.Float64StripeColumn, columns)
	} else {
		b.float64StripeColumns = b.float64StripeColumns[:columns]
	}
	for _, plan := range b.float64Stripes {
		b.float64StripeValues = b.float64StripeValues[:0]
		if cap(b.float64StripeValues) < int(plan.ref.Length) {
			b.float64StripeValues = make([]byte, 0, plan.ref.Length)
		}
		for column := range columns {
			encodingRank := uint8(0)
			for document := plan.first; document < plan.last; document++ {
				encodingRank = max(
					encodingRank, b.float64Encodings[document*columns+column],
				)
			}
			encoding := [...]storeio.Float64GroupEncoding{
				storeio.Float64GroupUint8,
				storeio.Float64GroupUint16,
				storeio.Float64GroupUint32,
				storeio.Float64GroupFloat64LE,
			}[encodingRank]
			starts[column] = len(b.float64StripeValues)
			for document := plan.first; document < plan.last; document++ {
				chunk := b.documents[document]
				for row := chunk.first; row < chunk.last; row++ {
					value, ok, err := b.sourceFloat64(row, column)
					if err != nil {
						return err
					}
					if !ok {
						continue
					}
					switch encoding {
					case storeio.Float64GroupUint8:
						b.float64StripeValues = append(b.float64StripeValues, byte(value))
					case storeio.Float64GroupUint16:
						offset := len(b.float64StripeValues)
						b.float64StripeValues = append(b.float64StripeValues, 0, 0)
						binary.LittleEndian.PutUint16(b.float64StripeValues[offset:], uint16(value))
					case storeio.Float64GroupUint32:
						offset := len(b.float64StripeValues)
						b.float64StripeValues = append(b.float64StripeValues, 0, 0, 0, 0)
						binary.LittleEndian.PutUint32(b.float64StripeValues[offset:], uint32(value))
					default:
						offset := len(b.float64StripeValues)
						b.float64StripeValues = append(b.float64StripeValues, 0, 0, 0, 0, 0, 0, 0, 0)
						binary.LittleEndian.PutUint64(b.float64StripeValues[offset:], math.Float64bits(value))
					}
				}
			}
			ends[column] = len(b.float64StripeValues)
			b.float64StripeColumns[column] = storeio.Float64StripeColumn{
				Encoding: encoding,
			}
		}
		for column := range columns {
			b.float64StripeColumns[column].Values =
				b.float64StripeValues[starts[column]:ends[column]:ends[column]]
		}
		page, err := storeio.EncodeFloat64Stripe(
			scratch[:plan.ref.Length],
			storeio.Float64StripeHeader{
				StoreID: b.storeID, Generation: b.allocator.generation,
				LogicalID: plan.ref.LogicalID, PageSize: plan.ref.Length,
				FirstChunk: b.documents[plan.first].chunk,
				ChunkCount: uint32(plan.last - plan.first), RowCount: plan.rows,
				ColumnCount: uint16(columns),
			},
			b.float64StripeColumns, b.allocator.nextLogical,
		)
		if err != nil {
			return err
		}
		if err := writeStorePageAt(file, page, plan.ref.Offset); err != nil {
			return err
		}
	}
	return nil
}

func groupRows(chunks []storeio.DocumentGroupChunk) int {
	rows := 0
	for _, chunk := range chunks {
		rows += bits.OnesCount64(chunk.Live)
	}
	return rows
}

func (b *fileStoreBulkBuild) writeKeyPages(file *os.File, scratch []byte) error {
	for _, plan := range b.keys {
		header := storeio.PageKeyDirectoryHeader{
			StoreID: b.storeID, Generation: b.allocator.generation,
			LogicalID: plan.ref.LogicalID, PageSize: b.allocator.pageSize,
			MinHash: plan.minHash, MaxHash: plan.maxHash, Level: plan.level,
			Next: plan.next,
		}
		var page []byte
		var err error
		if plan.level == 0 {
			page, err = storeio.EncodePageFingerprintLeaf(
				scratch[:b.options.PageSize], header,
				b.keyRows[plan.first:plan.last], b.fileEnd, b.allocator.nextLogical,
				uint32(len(b.documents)), uint32(b.options.Collection.ChunkDocuments),
			)
		} else {
			page, err = storeio.EncodePageFingerprintBranch(
				scratch[:b.options.PageSize], header, plan.children,
				b.fileEnd, b.allocator.nextLogical,
			)
		}
		if err != nil {
			return err
		}
		if err := writeStorePageAt(file, page, plan.ref.Offset); err != nil {
			return err
		}
	}
	return nil
}

func (b *fileStoreBulkBuild) writeIndexPages(file *os.File, scratch []byte) error {
	for _, plan := range b.indexes {
		header := storeio.IndexDirectoryHeader{
			StoreID: b.storeID, Generation: b.allocator.generation,
			LogicalID: plan.ref.LogicalID, PageSize: b.allocator.pageSize, Level: plan.level,
		}
		var page []byte
		var err error
		if plan.level == 0 {
			page, err = storeio.EncodeIndexDirectoryLeaf(
				scratch[:b.options.PageSize], header, b.indexRows[plan.first:plan.last],
				b.indexCertificates, b.allocator.nextLogical, uint32(len(b.options.indexes)),
			)
		} else {
			page, err = storeio.EncodeIndexDirectoryBranch(
				scratch[:b.options.PageSize], header, plan.children, b.fileEnd, b.allocator.nextLogical,
			)
		}
		if err != nil {
			return err
		}
		if err := writeStorePageAt(file, page, plan.ref.Offset); err != nil {
			return err
		}
	}
	return nil
}
