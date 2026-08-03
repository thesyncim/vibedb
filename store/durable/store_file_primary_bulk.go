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
)

// CreateFromPrimary writes one immutable ordered primary graph and publishes it
// through StateRoot.PrimaryRoot. The
// resulting collection supports point reads, snapshots, and serialized
// Put/Delete through the ordered-primary COW path.
//
// Exact indexes are built as posting tiles beside the ordered primary. Every
// document is canonicalized once and every leaf uses the unified class-5
// grammar. Schemas and overflow values are not implemented by this entry;
// create an empty collection with Create and load it through Put when required.
func CreateFromPrimary(
	collection *store.Collection,
	file *os.File,
	options Options,
) (int64, error) {
	if collection == nil || file == nil {
		return 0, fmt.Errorf(
			"vibedb: CreateFromPrimary requires non-nil collection and file",
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
		var collectErr error
		records, collectErr = collectFileStoreBulkRecords(snapshot, normalized)
		if collectErr != nil {
			return collectErr
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

	// Canonicalize every document once, up front, so the leaf encoder, exact
	// index term derivation, and every later read observe exactly one spelling.
	if err := canonicalizePrimaryBulkRecords(
		records, normalized.Collection.IndexOptions,
	); err != nil {
		return 0, err
	}
	storeID := primaryBulkStoreID(records, normalized)
	placed := len(normalized.indexes) != 0
	primaryPlan, err := storeio.PlanPrimaryGraph(storeID, records, placed)
	if err != nil {
		return 0, err
	}
	primaryPageCount := primaryPlan.PageCount()
	pageCount := primaryPageCount
	if placed {
		// The spanned exact indexes stage one page per cutter-emitted term
		// leaf plus each index's catalog and the shared root; the bound is
		// computed by planning the leaves and running the real cutter over
		// simulated posting tiles, so the single transaction that stages the
		// graph and every exact-index page together reserves a count that can
		// never under-provision (see primaryExactIndexPageBound).
		spans, spanErr := primaryPlan.LeafSpans()
		if spanErr != nil {
			return 0, spanErr
		}
		exactPageBound, boundErr := primaryExactIndexPageBound(
			storeID, records, spans, normalized.indexes,
			uint32(normalized.MaxPageSize),
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
		// BufferCount sizes the long-lived mutation committer. This committer
		// publishes exactly one fully planned transaction and closes, so retaining
		// caller surplus cannot increase concurrency or absorb a later batch.
		// Keep the explicit under-provision check, but stage only the exact graph.
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
		1,
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
	var placements []storeio.PrimaryGraphPlacement
	var primaryRoot storeio.PageRef
	if !placed {
		primaryRoot, err = storeio.BuildPlannedPrimaryGraph(
			tx, &primaryPlan, nil,
		)
	} else {
		placements = make([]storeio.PrimaryGraphPlacement, len(records))
		primaryRoot, err = storeio.BuildPlannedPrimaryGraph(
			tx, &primaryPlan, placements,
		)
	}
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
	if normalized.MaterializationDamageGranule != 0 {
		root.Options |= storeio.StateOptionCanonicalMaterialization
		root.MaterializationDamageGranule =
			uint32(normalized.MaterializationDamageGranule)
	}
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
	// is how sync acknowledges — and every buffered-visible store carries a
	// journal for checkpoint deltas or per-mutation acknowledgement.
	// Async-visible never carries one.
	if normalized.Durability == DurabilitySync ||
		normalized.Durability == DurabilityBufferedVisible {
		var journalID [16]byte
		if _, err := rand.Read(journalID[:]); err != nil {
			_ = tx.Abort()
			_ = committer.Close()
			return 0, fmt.Errorf(
				"vibedb: mint recovery journal identity: %w", err)
		}
		if err := createSiblingRecoveryJournal(
			file.Name(),
			recoveryJournalHeaderFor(
				storeID, journalID, uint32(normalized.PageSize),
				normalized.MaxKeyBytes, normalized.InlineValueBytes,
				normalized.MaxDocumentBytes, 1,
				func() int {
					if normalized.Durability == DurabilityBufferedVisible &&
						!normalized.RecoveryJournal {
						return normalized.primaryUnifiedOverlayBytes
					}
					return 0
				}(),
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
// canonical form. Already-canonical values — the steady state for
// engine-generated input — are left borrowed from the
// bulk snapshot; rewritten spellings live in one arena allocated lazily at the
// first rewrite and sized for the complete source corpus. The arena never
// reallocates because canonicalization can only shrink or keep a document's
// length under the pinned encoder: whitespace is dropped and every escape
// normalization collapses (raw control bytes are illegal JSON, so a control
// character's source spelling is never shorter than its canonical one), which
// the capacity check below still enforces defensively.
func canonicalizePrimaryBulkRecords(
	records []storeio.PrimaryGraphRecord,
	indexOptions document.IndexOptions,
) error {
	total := 0
	for at := range records {
		total += len(records[at].Value)
	}
	var arena []byte
	var ws storeio.CanonicalWorkspace
	entryStore := make([]vibejson.IndexEntry, 0, 128)
	for at := range records {
		var index vibejson.Index
		for {
			var err error
			index, err = vibejson.BuildIndexOptions(
				records[at].Value, entryStore[:cap(entryStore)], indexOptions,
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
		if arena == nil {
			arena = make([]byte, 0, total)
		}
		off := len(arena)
		out, err := storeio.AppendCanonicalIndexed(arena, index, &ws)
		if err != nil {
			return err
		}
		if cap(out) != cap(arena) && off != 0 {
			return fmt.Errorf(
				"vibedb: canonical bulk arena grew past its sized capacity",
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
) [16]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibejson unified primary bulk v0\x00"))
	var fixed [8]byte
	writeUint64 := func(value uint64) {
		binary.LittleEndian.PutUint64(fixed[:], value)
		_, _ = hash.Write(fixed[:])
	}
	writeUint64(uint64(options.PageSize))
	writeUint64(uint64(options.MaxPageSize))
	writeUint64(uint64(options.MaterializationDamageGranule))
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
