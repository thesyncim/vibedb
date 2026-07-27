package durable

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"runtime"
	"slices"

	"github.com/thesyncim/vibejson/x/byteview"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
)

// CreateFromPrimary writes one immutable ordered primary graph and publishes it
// through
// StateRoot.PrimaryRoot. Legacy chunk, fingerprint, index, float64, and zone
// roots remain empty. The resulting collection supports point reads,
// snapshots, and serialized Put/Delete through the ordered-primary COW path.
//
// Indexes, float64 columns, schemas, compact document groups, overflow values,
// and scan/zone construction are not implemented by this entry. Use
// CreateFrom when those features are required.
func CreateFromPrimary(
	collection *store.Collection,
	file *os.File,
	options Options,
) (int64, error) {
	if collection == nil || file == nil {
		return 0, fmt.Errorf(
			"vibejson: CreateFromPrimary requires non-nil collection and file",
		)
	}
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() != 0 {
		return 0, ErrNotEmpty
	}
	if len(options.Indexes) != 0 {
		return 0, fmt.Errorf(
			"%w: indexes are not available in CreateFromPrimary",
			ErrPrimaryCutoverUnsupported,
		)
	}
	if len(options.Float64Columns) != 0 {
		return 0, fmt.Errorf(
			"%w: float64 columns are not available in CreateFromPrimary",
			ErrPrimaryCutoverUnsupported,
		)
	}
	if options.Collection.Schema != nil {
		return 0, fmt.Errorf(
			"%w: schemas are not available in CreateFromPrimary",
			ErrPrimaryCutoverUnsupported,
		)
	}
	if options.DocumentFormat != DocumentFormatVerbatim {
		return 0, fmt.Errorf(
			"%w: compact documents are not available in CreateFromPrimary",
			ErrPrimaryCutoverUnsupported,
		)
	}
	requestedBuffers := options.BufferCount
	normalized, err := options.normalized()
	if err != nil {
		return 0, err
	}
	if normalized.PageSize != 4096 ||
		normalized.MaxPageSize < storeio.GlobalTabletCatalogRootBytes {
		return 0, fmt.Errorf(
			"%w: CreateFromPrimary requires 4 KiB pages and a 64 KiB maximum page",
			ErrPrimaryCutoverUnsupported,
		)
	}

	var (
		source  *store.State
		records []storeio.PrimaryGraphRecord
	)
	err = collection.WithBulkSnapshot(func(snapshot *store.State) error {
		source = snapshot
		rows, collectErr := collectFileStoreBulkRows(snapshot, normalized)
		if collectErr != nil {
			return collectErr
		}
		records = make([]storeio.PrimaryGraphRecord, len(rows))
		for at, row := range rows {
			chunk := snapshot.Chunks.Get(row.sourceChunk)
			if chunk == nil ||
				chunk.Live&(uint64(1)<<row.sourceSlot) == 0 {
				return storeio.ErrDocumentPageCorrupt
			}
			key := chunk.Key(int(row.sourceSlot))
			value := chunk.Docs.RawAt(int(chunk.Ord[row.sourceSlot]))
			if len(key) > storeio.CommonPrimaryLeafMaxKeyBytes {
				return fmt.Errorf(
					"%w: CreateFromPrimary key exceeds the ordered-leaf bound",
					ErrKeyTooLarge,
				)
			}
			if len(value) == 0 || len(value) > normalized.InlineValueBytes {
				return fmt.Errorf(
					"%w: CreateFromPrimary requires non-empty inline documents",
					ErrPrimaryCutoverUnsupported,
				)
			}
			// A State is immutable. Borrowing its key and document storage keeps
			// the bulk path to one descriptor per row; source is retained
			// explicitly through graph construction below.
			records[at] = storeio.PrimaryGraphRecord{
				Key: byteview.Bytes(key), Value: value,
			}
		}
		return sortPrimaryBulkRecords(records)
	})
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		return 0, fmt.Errorf(
			"%w: CreateFromPrimary requires at least one document",
			ErrPrimaryCutoverUnsupported,
		)
	}

	storeID := primaryBulkStoreID(records, normalized)
	pageCount, err := storeio.PrimaryGraphPageCount(storeID, records)
	if err != nil {
		return 0, err
	}
	bufferCount := pageCount + 1
	if requestedBuffers != 0 {
		if requestedBuffers <= pageCount {
			return 0, fmt.Errorf(
				"%w: CreateFromPrimary needs at least %d commit buffers",
				ErrPrimaryCutoverUnsupported, pageCount+1,
			)
		}
		bufferCount = requestedBuffers
	}
	if bufferCount > 1<<15 {
		return 0, fmt.Errorf(
			"%w: ordered primary graph needs %d transaction pages",
			ErrPrimaryCutoverUnsupported, pageCount,
		)
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
	if err := file.Truncate(int64(catalog.fileEnd)); err != nil {
		return 0, err
	}
	if catalog.segments != 0 {
		scratch := make([]byte, normalized.PageSize)
		if err := catalog.write(
			file, catalog.fileEnd, catalog.nextID,
			scratch,
		); err != nil {
			return 0, err
		}
		if err := file.Sync(); err != nil {
			return 0, err
		}
	}

	committer, err := storeio.NewCommitter(
		file,
		storeio.DeviceOptions{
			Backend:     storeio.Backend(normalized.Backend),
			BufferCount: bufferCount,
			BufferSize:  max(normalized.MaxPageSize, os.Getpagesize()),
			QueueDepth:  bufferCount,
			CheckpointSync: storeio.CheckpointSync(
				normalized.CheckpointStrength,
			),
		},
		storeio.CommitterOptions{
			QueueSlots: 1, MaxPagesPerBatch: pageCount, GroupLimit: 1,
		},
	)
	if err != nil {
		return 0, err
	}
	tx, err := storeio.BeginWriteTransaction(
		committer, nil, pageCount,
		storeio.WriteTransactionOptions{
			StoreID: storeID, Generation: 1,
			PageSize:      uint32(normalized.PageSize),
			FileEnd:       catalog.fileEnd,
			NextLogicalID: storeio.PrimaryFirstDynamicLogicalID,
		},
	)
	if err != nil {
		_ = committer.Close()
		return 0, err
	}
	primaryRoot, err := storeio.BuildPrimaryGraph(tx, records)
	runtime.KeepAlive(source)
	if err != nil {
		_ = tx.Abort()
		_ = committer.Close()
		return 0, err
	}
	root := storeio.StateRoot{
		StoreID: storeID, Generation: 1,
		PageSize:       uint32(normalized.PageSize),
		DocumentCount:  uint64(len(records)),
		NextLogicalID:  tx.NextLogicalID(),
		ChunkDocuments: uint32(normalized.Collection.ChunkDocuments),
		IndexMaxDepth: uint32(max(
			normalized.Collection.IndexOptions.MaxDepth, 0,
		)),
		MaxKeyBytes:      uint32(normalized.MaxKeyBytes),
		InlineValueBytes: uint32(normalized.InlineValueBytes),
		MaxDocumentBytes: uint32(normalized.MaxDocumentBytes),
		PrimaryRoot:      primaryRoot,
	}
	root.Options = fileStoreCollectionOptionFlags(normalized.Collection)
	if err := catalog.apply(
		&root, uint32(normalized.MaxPageSize),
	); err != nil {
		_ = tx.Abort()
		_ = committer.Close()
		return 0, err
	}
	if err := tx.PublishInline(
		root,
		storeio.NewInlineFreeDelta(storeio.PageRef{}, storeio.PageRef{}),
	); err != nil {
		_ = tx.Abort()
		_ = committer.Close()
		return 0, err
	}
	fileEnd := int64(tx.FileEnd())
	if err := committer.Wait(root.Generation); err != nil {
		_ = committer.Close()
		return 0, err
	}
	if err := committer.Close(); err != nil {
		return 0, err
	}
	return fileEnd, nil
}

// sortPrimaryBulkRecords establishes the exact ordering contract shared by
// Builder and the durable primary graph. Builder rejects the second occurrence
// at Append time; a bulk source that violates that invariant reports the same
// typed error after lexical sorting, before any page is allocated or written.
func sortPrimaryBulkRecords(records []storeio.PrimaryGraphRecord) error {
	slices.SortFunc(records, func(a, b storeio.PrimaryGraphRecord) int {
		return bytes.Compare(a.Key, b.Key)
	})
	for at := 1; at < len(records); at++ {
		if bytes.Equal(records[at-1].Key, records[at].Key) {
			return fmt.Errorf("%w %q", store.ErrDuplicateKey, records[at].Key)
		}
	}
	return nil
}

// primaryBulkStoreID makes an immutable primary build reproducible. The
// identity covers every byte that affects the ordered graph plus the durable
// collection policy recorded in StateRoot. Operational read/cache/queue
// options deliberately do not perturb the file image.
func primaryBulkStoreID(
	records []storeio.PrimaryGraphRecord,
	options normalizedFileStoreOptions,
) [16]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibejson primary bulk v1\x00"))
	var fixed [8]byte
	writeUint64 := func(value uint64) {
		binary.LittleEndian.PutUint64(fixed[:], value)
		_, _ = hash.Write(fixed[:])
	}
	writeUint64(uint64(options.PageSize))
	writeUint64(uint64(options.MaxPageSize))
	writeUint64(uint64(options.Collection.ChunkDocuments))
	writeUint64(uint64(max(options.Collection.IndexOptions.MaxDepth, 0)))
	writeUint64(uint64(options.MaxKeyBytes))
	writeUint64(uint64(options.InlineValueBytes))
	writeUint64(uint64(options.MaxDocumentBytes))
	writeUint64(uint64(fileStoreCollectionOptionFlags(options.Collection)))
	writeUint64(uint64(len(records)))
	for _, record := range records {
		writeUint64(uint64(len(record.Key)))
		_, _ = hash.Write(record.Key)
		writeUint64(uint64(len(record.Value)))
		_, _ = hash.Write(record.Value)
	}
	sum := hash.Sum(nil)
	var storeID [16]byte
	copy(storeID[:], sum)
	if storeID == ([16]byte{}) {
		storeID[0] = 1
	}
	return storeID
}
