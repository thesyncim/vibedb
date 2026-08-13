package durable

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

// PrimaryBulkRecord is one key/document pair borrowed by CreateFromRecords for
// the duration of the call. The native durable bulk path validates,
// canonicalizes, sorts, and writes these rows directly; it does not construct
// an intermediate in-memory store.Collection.
type PrimaryBulkRecord struct {
	Key   string
	Value []byte
}

// CreateFromRecords writes borrowed rows directly into the ordered durable
// primary graph. It is the native bulk-load entry point for callers that
// already own a complete batch. Inputs may be reused after the call returns.
func CreateFromRecords(
	input []PrimaryBulkRecord,
	file *os.File,
	options Options,
) (fileEnd int64, err error) {
	if file == nil {
		return 0, fmt.Errorf("vibedb: CreateFromRecords requires a non-nil file")
	}
	if err := storeio.LockWriter(file); err != nil {
		return 0, err
	}
	defer func() {
		if unlockErr := unlockCollectionWriter(file); unlockErr != nil {
			err = errors.Join(err, unlockErr)
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if info.Size() != 0 {
		return 0, ErrNotEmpty
	}
	if options.Collection.Schema != nil {
		return 0, fmt.Errorf(
			"%w: schemas are not available in CreateFromRecords",
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
			"%w: CreateFromRecords requires 4 KiB pages and a 64 KiB maximum page",
			ErrPrimaryCutoverUnsupported,
		)
	}
	if len(input) == 0 {
		return 0, fmt.Errorf(
			"%w: CreateFromRecords requires at least one document",
			ErrPrimaryCutoverUnsupported,
		)
	}
	records := make([]storeio.PrimaryGraphRecord, len(input))
	for i := range input {
		if len(input[i].Key) == 0 ||
			len(input[i].Key) > normalized.MaxKeyBytes ||
			len(input[i].Key) > storeio.CommonPrimaryLeafMaxKeyBytes {
			return 0, ErrKeyTooLarge
		}
		if len(input[i].Value) == 0 ||
			len(input[i].Value) > normalized.MaxDocumentBytes {
			return 0, ErrDocumentTooLarge
		}
		if len(input[i].Value) > normalized.InlineValueBytes {
			return 0, ErrPrimaryCutoverUnsupported
		}
		records[i] = storeio.PrimaryGraphRecord{
			Key: input[i].Key, Value: byteview.String(input[i].Value),
		}
	}
	if err := sortPrimaryBulkRecords(records); err != nil {
		return 0, err
	}
	return createFromPrimaryGraphRecords(
		records, input, file, normalized, requestedBuffers,
	)
}

// CreateFromPrimary writes one immutable ordered primary graph and publishes it
// through StateRoot.PrimaryRoot. The
// resulting collection supports point reads, snapshots, and serialized
// Put/Delete through the ordered-primary COW path.
//
// Exact indexes are built as posting tiles beside the ordered primary. Every
// document is canonicalized once and every leaf uses the compact stripe
// grammar. Schemas and overflow values are not implemented by this entry;
// create an empty collection with Create and load it through Put when required.
func CreateFromPrimary(
	collection *store.Collection,
	file *os.File,
	options Options,
) (fileEnd int64, err error) {
	if collection == nil || file == nil {
		return 0, fmt.Errorf(
			"vibedb: CreateFromPrimary requires non-nil collection and file",
		)
	}
	if err := storeio.LockWriter(file); err != nil {
		return 0, err
	}
	defer func() {
		if unlockErr := unlockCollectionWriter(file); unlockErr != nil {
			err = errors.Join(err, unlockErr)
		}
	}()
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
	return createFromPrimaryGraphRecords(
		records, source, file, normalized, requestedBuffers,
	)
}

func createFromPrimaryGraphRecords(
	records []storeio.PrimaryGraphRecord,
	source any,
	file *os.File,
	normalized normalizedFileStoreOptions,
	requestedBuffers int,
) (int64, error) {
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
			PageSize:               uint32(normalized.PageSize),
			FileEnd:                catalog.fileEnd,
			PhysicalHighWaterBytes: normalized.PhysicalCapacityBytes,
			NextLogicalID:          storeio.PrimaryFirstDynamicLogicalID,
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
		MaxKeyBytes:           uint32(normalized.MaxKeyBytes),
		InlineValueBytes:      uint32(normalized.InlineValueBytes),
		MaxDocumentBytes:      uint32(normalized.MaxDocumentBytes),
		PhysicalCapacityBytes: normalized.PhysicalCapacityBytes,
		PrimaryRoot:           primaryRoot,
		ExactIndexRoot:        exactIndexRoot,
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
	// Every graph and index page is staged only in committer memory up to this
	// point. Provision the exact final main-file prefix before writing the
	// catalog or allowing PublishInline to issue its first data write.
	physicalEnd := tx.FileEnd()
	if normalized.PhysicalCapacityBytes != 0 {
		if physicalEnd > normalized.PhysicalCapacityBytes {
			_ = tx.Abort()
			_ = committer.Close()
			return 0, fmt.Errorf(
				"%w: bulk build needs=%d ceiling=%d",
				ErrPhysicalCapacity, physicalEnd,
				normalized.PhysicalCapacityBytes,
			)
		}
		if err := fileStoreCapacityOps.allocate(
			file, 0, int64(physicalEnd),
		); err != nil {
			_ = tx.Abort()
			_ = committer.Close()
			return 0, fmt.Errorf(
				"%w: strictly allocate bulk main file: %w",
				ErrPhysicalCapacity, err,
			)
		}
		if err := fileStoreCapacityOps.sync(file); err != nil {
			_ = tx.Abort()
			_ = committer.Close()
			return 0, fmt.Errorf(
				"%w: sync strictly allocated bulk main file: %w",
				ErrPhysicalCapacity, err,
			)
		}
	} else if err := file.Truncate(int64(catalog.fileEnd)); err != nil {
		_ = tx.Abort()
		_ = committer.Close()
		return 0, err
	}
	if catalog.segments != 0 {
		scratch := make([]byte, normalized.PageSize)
		if err := catalog.write(
			file, catalog.fileEnd, catalog.nextID, scratch,
		); err != nil {
			_ = tx.Abort()
			_ = committer.Close()
			return 0, err
		}
		if err := file.Sync(); err != nil {
			_ = tx.Abort()
			_ = committer.Close()
			return 0, err
		}
	}
	// Mint the paired journal before the root that names it is published, so a
	// crash after the root is durable finds the journal file present.
	// DurabilitySync is journal-backed on the primary graph unconditionally — it
	// is how sync acknowledges — and explicit RecoveryJournal mode needs one for
	// per-mutation acknowledgement. Ordinary buffered-visible bulk stores defer
	// the sibling until their first valid mutation, preserving the compact
	// immutable/log-serving footprint. Async-visible never carries one.
	if normalized.Durability == DurabilitySync ||
		normalized.Durability == DurabilityBufferedVisible &&
			normalized.RecoveryJournal {
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
				recoveryJournalInitialDocumentBytes(
					normalized.Durability,
					normalized.InlineValueBytes,
					normalized.MaxDocumentBytes,
				), 1,
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
				byteview.Bytes(records[at].Value), entryStore[:cap(entryStore)], indexOptions,
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
		records[at].Value = byteview.String(arena[off:len(arena):len(arena)])
	}
	return nil
}

// sortPrimaryBulkRecords establishes the exact ordering contract shared by
// Builder and the durable primary graph. Builder rejects the second occurrence
// at Append time; a bulk source that violates that invariant reports the same
// typed error after lexical sorting, before any page is allocated or written.
func sortPrimaryBulkRecords(records []storeio.PrimaryGraphRecord) error {
	slices.SortFunc(records, func(a, b storeio.PrimaryGraphRecord) int {
		return strings.Compare(a.Key, b.Key)
	})
	for at := 1; at < len(records); at++ {
		if records[at-1].Key == records[at].Key {
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
	writeUint64(options.PhysicalCapacityBytes)
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
		_, _ = hash.Write(byteview.Bytes(record.Key))
		writeUint64(uint64(len(record.Value)))
		_, _ = hash.Write(byteview.Bytes(record.Value))
	}
	sum := hash.Sum(nil)
	var storeID [16]byte
	copy(storeID[:], sum)
	if storeID == ([16]byte{}) {
		storeID[0] = 1
	}
	return storeID
}
