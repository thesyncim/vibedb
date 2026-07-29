package durable

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// CreateFromPrimary writes one immutable ordered primary graph and publishes it
// through StateRoot.PrimaryRoot; the retired root slots stay empty. The
// resulting collection supports point reads, snapshots, and serialized
// Put/Delete through the ordered-primary COW path.
//
// Exact indexes are built as posting tiles beside the ordered primary, and
// Options.DocumentFormat selects verbatim or compact leaves. Schemas and
// overflow values are not implemented by this entry; create an empty collection
// with Create and load it through Put when they are required.
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
	if options.Collection.Schema != nil {
		return 0, fmt.Errorf(
			"%w: schemas are not available in CreateFromPrimary",
			ErrPrimaryCutoverUnsupported,
		)
	}
	if options.DocumentFormat > DocumentFormatCompact {
		return 0, fmt.Errorf(
			"%w: unknown document format",
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
				return fmt.Errorf("vibejson: bulk source row references a missing document")
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

	// DocumentFormatCompact stages every leaf as the compact document-group
	// class; verbatim uses the adaptive succinct/template classes; UnifiedLeaves
	// routes to the class-5 unified planner and codec. The policy is threaded
	// identically into the page-count reservation and the build so the single
	// staging transaction reserves exactly the leaves the build produces.
	leafPolicy := storeio.PrimaryLeafAdaptive
	formatTag := options.DocumentFormat
	if options.DocumentFormat == DocumentFormatCompact {
		leafPolicy = storeio.PrimaryLeafCompact
	}
	if options.UnifiedLeaves {
		leafPolicy = storeio.PrimaryLeafUnified
		// The unified representation stores canonical bytes, so the build's
		// deterministic identity must not collide with a verbatim or compact
		// build over the same records: hash a distinct format tag beyond the
		// two public DocumentFormat values.
		formatTag = DocumentFormat(2)
		// Canonicalize every document once, up front, so the leaf encoder, the
		// exact-index term derivation, and every later read all observe exactly
		// one spelling (unified-leaf design §3.2/§7.5: canonicalization happens
		// at admission, before anything else consumes the bytes). The snapshot
		// backing the borrowed values stays pinned through the KeepAlive below.
		if err := canonicalizePrimaryBulkRecords(records); err != nil {
			return 0, err
		}
	}
	storeID := primaryBulkStoreID(records, normalized, formatTag)
	primaryPageCount, err := storeio.PrimaryGraphPageCount(storeID, records, leafPolicy)
	if err != nil {
		return 0, err
	}
	pageCount := primaryPageCount
	if len(normalized.indexes) != 0 {
		// The spanned exact indexes stage one page per cutter-emitted term
		// leaf plus each index's catalog and the shared root; the bound is
		// computed by planning the leaves and running the real cutter over
		// simulated posting tiles, so the single transaction that stages the
		// graph and every exact-index page together reserves a count that can
		// never under-provision (see primaryExactIndexPageBound).
		exactPageBound, boundErr := primaryExactIndexPageBound(
			storeID, records, normalized.indexes,
			uint32(normalized.MaxPageSize), leafPolicy,
		)
		if boundErr != nil {
			return 0, boundErr
		}
		pageCount += exactPageBound
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
	placements := make([]storeio.PrimaryGraphPlacement, len(records))
	primaryRoot, err := storeio.BuildPrimaryGraphPlaced(tx, records, placements, leafPolicy)
	if err != nil {
		_ = tx.Abort()
		_ = committer.Close()
		return 0, err
	}
	exactIndexRoot, err := buildPrimaryExactIndexes(
		tx, records, placements, normalized.indexes,
		uint32(normalized.PageSize), uint32(normalized.MaxPageSize),
	)
	runtime.KeepAlive(source)
	if err != nil {
		_ = tx.Abort()
		_ = committer.Close()
		return 0, err
	}
	root := storeio.StateRoot{
		StoreID: storeID, Generation: 1,
		PageSize:         uint32(normalized.PageSize),
		DocumentCount:    uint64(len(records)),
		NextLogicalID:    tx.NextLogicalID(),
		ChunkDocuments:   uint32(normalized.Collection.ChunkDocuments),
		IndexCount:       uint32(len(normalized.indexes)),
		IndexCatalogHash: normalized.indexCatalogHash,
		IndexMaxDepth: uint32(max(
			normalized.Collection.IndexOptions.MaxDepth, 0,
		)),
		MaxKeyBytes:      uint32(normalized.MaxKeyBytes),
		InlineValueBytes: uint32(normalized.InlineValueBytes),
		MaxDocumentBytes: uint32(normalized.MaxDocumentBytes),
		PrimaryRoot:      primaryRoot,
		ExactIndexRoot:   exactIndexRoot,
	}
	root.Options = fileStoreCollectionOptionFlags(normalized.Collection)
	if err := catalog.apply(
		&root, uint32(normalized.MaxPageSize),
	); err != nil {
		_ = tx.Abort()
		_ = committer.Close()
		return 0, err
	}
	// Mint the paired journal before the root that names it is published, so a
	// crash after the root is durable finds the journal file present.
	// DurabilitySync is journal-backed on the primary graph unconditionally — it
	// is how sync acknowledges — while buffered-visible carries a journal only on
	// the RecoveryJournal opt-in. Async-visible never carries one.
	if normalized.Durability == DurabilitySync ||
		normalized.RecoveryJournal &&
			normalized.Durability == DurabilityBufferedVisible {
		var journalID [16]byte
		if _, err := rand.Read(journalID[:]); err != nil {
			_ = tx.Abort()
			_ = committer.Close()
			return 0, fmt.Errorf(
				"vibejson: mint recovery journal identity: %w", err)
		}
		if err := createSiblingRecoveryJournal(
			file.Name(),
			recoveryJournalHeaderFor(
				storeID, journalID, uint32(normalized.PageSize),
				normalized.MaxKeyBytes, normalized.InlineValueBytes,
				normalized.MaxDocumentBytes, 1,
			),
		); err != nil {
			_ = tx.Abort()
			_ = committer.Close()
			return 0, err
		}
		root.JournalID = journalID
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

// canonicalizePrimaryBulkRecords rewrites every record value to its vibejson
// canonical form (unified-leaf design §3.2). Already-canonical values — the
// steady state for engine-generated input (§8) — are left borrowed from the
// bulk snapshot; rewritten spellings live in one arena sized up front. The
// arena never reallocates because canonicalization can only shrink or keep a
// document's length under the pinned encoder: whitespace is dropped and every
// escape normalization collapses (raw control bytes are illegal JSON, so a
// control character's source spelling is never shorter than its canonical
// one), which the capacity check below still enforces defensively.
func canonicalizePrimaryBulkRecords(records []storeio.PrimaryGraphRecord) error {
	total := 0
	for at := range records {
		total += len(records[at].Value)
	}
	arena := make([]byte, 0, total)
	var ws storeio.CanonicalWorkspace
	entryStore := make([]vibejson.IndexEntry, 0, 128)
	for at := range records {
		var index vibejson.Index
		for {
			var err error
			index, err = vibejson.BuildIndex(
				records[at].Value, entryStore[:cap(entryStore)],
			)
			if err == nil {
				entryStore = index.Entries
				break
			}
			if !errors.Is(err, document.ErrIndexFull) {
				return err
			}
			grown := max(cap(entryStore)*2, 128)
			entryStore = make([]vibejson.IndexEntry, 0, grown)
		}
		if storeio.IndexIsCanonical(index, &ws) {
			continue
		}
		off := len(arena)
		out, err := storeio.AppendCanonicalIndexed(arena, index, &ws)
		if err != nil {
			return err
		}
		if cap(out) != cap(arena) && off != 0 {
			return fmt.Errorf(
				"vibejson: canonical bulk arena grew past its sized capacity",
			)
		}
		arena = out
		records[at].Value = arena[off:len(arena):len(arena)]
	}
	return nil
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
	format DocumentFormat,
) [16]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibejson primary bulk v1\x00"))
	// The document format changes the physical leaf image byte for byte, so a
	// compact build must not share an identity with a verbatim one over the same
	// records and policy.
	_, _ = hash.Write([]byte{byte(format)})
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
	// The canonical index catalog is part of the immutable file image: a build
	// with different indexes must produce a different identity. indexCatalogHash
	// is the deterministic FNV of the canonical alias names and paths, so it
	// makes the identity index-aware without re-walking the definitions here.
	writeUint64(uint64(len(options.indexes)))
	writeUint64(options.indexCatalogHash)
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
