package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// StateRootPayloadSize leaves a zero-filled suffix for development fields
	// within each inline root.
	StateRootPayloadSize = 512
	// PageRefSize is the fixed encoded width of one pointer-free physical page
	// reference.
	PageRefSize = 32

	stateRootVersion               = DevelopmentFormatVersion
	stateRootMaterializationOffset = 40
	stateRootMaterializationEnd    = stateRootMaterializationOffset + 4
	stateRootMaxPageSizeOffset     = stateRootMaterializationEnd
	stateRootMaxPageSizeEnd        = stateRootMaxPageSizeOffset + 4
	stateRootPageCatalogOffset     = stateRootMaxPageSizeEnd
	stateRootPageCatalogEnd        = stateRootPageCatalogOffset + PageRefSize
	stateRootPageCatalogDigestEnd  = stateRootPageCatalogEnd + PageCatalogDigestSize
	stateRootPageCatalogBytesEnd   = stateRootPageCatalogDigestEnd + 4
	stateRootMaxKeyBytesEnd        = stateRootPageCatalogBytesEnd + 4
	stateRootInlineValueBytesEnd   = stateRootMaxKeyBytesEnd + 4
	stateRootMaxDocumentBytesEnd   = stateRootInlineValueBytesEnd + 4
	stateRootPrimaryOffset         = stateRootMaxDocumentBytesEnd
	stateRootPrimaryEnd            = stateRootPrimaryOffset + PageRefSize
	// stateRootJournalIDOffset names the paired recovery journal. The journal
	// file's own header carries {StoreID, JournalID}; recovery cross-checks both
	// before replaying.
	stateRootJournalIDOffset = stateRootPrimaryEnd
	stateRootJournalIDEnd    = stateRootJournalIDOffset + 16
	// stateRootExactIndexOffset names the PagePrimaryExactRoot published beside
	// PrimaryRoot for exact indexes carried as posting tiles on the ordered
	// graph.
	stateRootExactIndexOffset = stateRootJournalIDEnd
	stateRootExactIndexEnd    = stateRootExactIndexOffset + PageRefSize
	// stateRootPhysicalCapacityOffset carries the immutable maximum main-file
	// high-water mark. Zero selects elastic allocation; a non-zero
	// value is page aligned and bounds every physical reference the Store may
	// ever publish.
	stateRootPhysicalCapacityOffset = stateRootExactIndexEnd
	stateRootPhysicalCapacityEnd    = stateRootPhysicalCapacityOffset + 8
	// stateRootReservedOffset begins the zero-filled suffix described on
	// StateRootPayloadSize.
	stateRootReservedOffset = stateRootPhysicalCapacityEnd
)

// State-root option bits are durable equivalents of Store construction
// options. Unknown bits fail closed.
const (
	// StateOptionSchema means the durable catalog hash also binds an
	// application-supplied document schema. The schema definition remains
	// caller configuration; reopening with a different definition fails.
	StateOptionSchema uint32 = 1 << iota
	// StateOptionSkipIndexes means the durable catalog also binds compact
	// primary-stripe min/max paths.
	StateOptionSkipIndexes
	// StateOptionCanonicalMaterialization means the file may contain
	// recovery-journaled canonical page replacements. The exact qualified
	// power-loss damage granule is carried by StateRoot so Open can recover
	// safely before consulting caller options.
	StateOptionCanonicalMaterialization
)

const stateRootKnownOptions = StateOptionSchema |
	StateOptionSkipIndexes |
	StateOptionCanonicalMaterialization

// ErrStateRootCorrupt reports a common page that passed basic framing but does
// not encode a valid Store state root.
var ErrStateRootCorrupt = errors.New("vibedb: corrupt Store state root")

// PageRef is a durable pointer to one immutable logical-page version. Offset
// is physical and changes on replacement; LogicalID is stable. Generation may
// be older than the state root because unchanged pages are shared across
// copy-on-write generations.
type PageRef struct {
	Offset     uint64
	LogicalID  uint64
	Generation uint64
	Length     uint32
	Kind       PageKind
}

// StateRoot is the compact, pointer-free graph root embedded in an InlineSuperblock.
// PrimaryRoot selects the ordered document graph, ExactIndexRoot selects its
// secondary indexes, and free-page state is carried by the inline root.
type StateRoot struct {
	StoreID       [16]byte
	Generation    uint64
	PageSize      uint32
	Options       uint32
	DocumentCount uint64
	NextLogicalID uint64
	IndexCount    uint32
	IndexMaxDepth uint32
	// IndexCatalogHash is a compact non-authoritative rejection key for the
	// exact PageCatalog identity. Open must reconstruct and compare the
	// canonical catalog bytes; this value is never a fallback authority.
	IndexCatalogHash uint64
	// MaterializationDamageGranule is the largest complete sector a power
	// failure may damage during one qualified canonical overwrite. Zero means
	// the file never uses in-place materialization.
	MaterializationDamageGranule uint32
	// MaxPageSize is the largest physical extent this Store may admit. It is
	// persisted so Open can size recovery scratch before constructing runtime
	// resources.
	MaxPageSize uint32
	// PageCatalogHead names an immutable, contiguous PageSize run. The number
	// of pages is derived exactly from PageCatalogBytes and the catalog segment
	// geometry; logical IDs are contiguous by the same count.
	PageCatalogHead PageRef
	// PageCatalogDigest authenticates the exact canonical bytes. The all-zero
	// digest is a valid hash value, so catalog presence is determined only by
	// PageCatalogBytes and PageCatalogHead.
	PageCatalogDigest [PageCatalogDigestSize]byte
	PageCatalogBytes  uint32
	// These immutable FileStore admission bounds make zero-option reopen exact:
	// it cannot silently shrink accepted keys/values or change inline versus
	// overflow placement. A zero triplet denotes a StorePage root, whose
	// single-page document bound is MaxPageSize.
	MaxKeyBytes      uint32
	InlineValueBytes uint32
	MaxDocumentBytes uint32
	// PrimaryRoot selects the ordered tablet graph and is the sole document root.
	PrimaryRoot PageRef
	// ExactIndexRoot names the PagePrimaryExactRoot carrying the physical
	// exact-term posting leaves associated with PrimaryRoot. It is absent for an
	// ordered primary without indexes.
	ExactIndexRoot PageRef
	// JournalID is the UUID of the recovery journal file paired with this store.
	// Zero means no journal is referenced: the store never acknowledged a
	// mutation through the redo journal, and recovery must find no journal to
	// replay. A non-zero value binds a specific sibling journal file; recovery
	// fails closed if that journal is missing or its header identity does not
	// match both StoreID and this JournalID.
	JournalID [16]byte
	// PhysicalCapacityBytes is the immutable sealed main-file ceiling. It does
	// not claim that the complete ceiling is allocated: the apparent file size,
	// after strict platform proof, is the current allocation certificate. Zero
	// selects elastic allocation.
	PhysicalCapacityBytes uint64
}

// encodeStateRootPayload writes the identity-free StateRoot body shared by the
// inline root envelope. The caller validates root against its enclosing FileEnd
// before calling it.
func encodeStateRootPayload(payload []byte, root StateRoot) {
	clear(payload)
	binary.LittleEndian.PutUint32(payload[0:4], stateRootVersion)
	binary.LittleEndian.PutUint32(payload[4:8], root.Options)
	binary.LittleEndian.PutUint64(payload[8:16], root.DocumentCount)
	binary.LittleEndian.PutUint64(payload[16:24], root.NextLogicalID)
	binary.LittleEndian.PutUint32(payload[24:28], root.IndexCount)
	binary.LittleEndian.PutUint32(payload[28:32], root.IndexMaxDepth)
	binary.LittleEndian.PutUint64(payload[32:40], root.IndexCatalogHash)
	binary.LittleEndian.PutUint32(
		payload[stateRootMaterializationOffset:stateRootMaterializationEnd],
		root.MaterializationDamageGranule,
	)
	binary.LittleEndian.PutUint32(
		payload[stateRootMaxPageSizeOffset:stateRootMaxPageSizeEnd],
		root.MaxPageSize,
	)
	encodePageRef(
		payload[stateRootPageCatalogOffset:stateRootPageCatalogEnd],
		root.PageCatalogHead,
	)
	copy(
		payload[stateRootPageCatalogEnd:stateRootPageCatalogDigestEnd],
		root.PageCatalogDigest[:],
	)
	binary.LittleEndian.PutUint32(
		payload[stateRootPageCatalogDigestEnd:stateRootPageCatalogBytesEnd],
		root.PageCatalogBytes,
	)
	binary.LittleEndian.PutUint32(
		payload[stateRootPageCatalogBytesEnd:stateRootMaxKeyBytesEnd],
		root.MaxKeyBytes,
	)
	binary.LittleEndian.PutUint32(
		payload[stateRootMaxKeyBytesEnd:stateRootInlineValueBytesEnd],
		root.InlineValueBytes,
	)
	binary.LittleEndian.PutUint32(
		payload[stateRootInlineValueBytesEnd:stateRootMaxDocumentBytesEnd],
		root.MaxDocumentBytes,
	)
	encodePageRef(
		payload[stateRootPrimaryOffset:stateRootPrimaryEnd],
		root.PrimaryRoot,
	)
	copy(payload[stateRootJournalIDOffset:stateRootJournalIDEnd], root.JournalID[:])
	encodePageRef(
		payload[stateRootExactIndexOffset:stateRootExactIndexEnd],
		root.ExactIndexRoot,
	)
	binary.LittleEndian.PutUint64(
		payload[stateRootPhysicalCapacityOffset:stateRootPhysicalCapacityEnd],
		root.PhysicalCapacityBytes,
	)
}

// decodeStateRootPayload decodes the identity-free StateRoot body shared by
// inline roots. Identity comes from the checksummed enclosing
// envelope and is validated with every state field before return.
func decodeStateRootPayload(
	payload []byte, storeID [16]byte, generation uint64, pageSize uint32, fileEnd uint64,
) (StateRoot, error) {
	if len(payload) != StateRootPayloadSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != stateRootVersion ||
		!pageRefReservedZero(payload[stateRootPageCatalogOffset:stateRootPageCatalogEnd]) ||
		!pageRefReservedZero(payload[stateRootPrimaryOffset:stateRootPrimaryEnd]) ||
		!pageRefReservedZero(payload[stateRootExactIndexOffset:stateRootExactIndexEnd]) ||
		!allZero(payload[stateRootReservedOffset:]) {
		return StateRoot{}, fmt.Errorf("%w: header, version, or reserved bytes", ErrStateRootCorrupt)
	}
	root := StateRoot{
		StoreID:          storeID,
		Generation:       generation,
		PageSize:         pageSize,
		Options:          binary.LittleEndian.Uint32(payload[4:8]),
		DocumentCount:    binary.LittleEndian.Uint64(payload[8:16]),
		NextLogicalID:    binary.LittleEndian.Uint64(payload[16:24]),
		IndexCount:       binary.LittleEndian.Uint32(payload[24:28]),
		IndexMaxDepth:    binary.LittleEndian.Uint32(payload[28:32]),
		IndexCatalogHash: binary.LittleEndian.Uint64(payload[32:40]),
		MaterializationDamageGranule: binary.LittleEndian.Uint32(
			payload[stateRootMaterializationOffset:stateRootMaterializationEnd],
		),
		MaxPageSize: binary.LittleEndian.Uint32(
			payload[stateRootMaxPageSizeOffset:stateRootMaxPageSizeEnd],
		),
		PageCatalogHead: decodePageRef(
			payload[stateRootPageCatalogOffset:stateRootPageCatalogEnd],
		),
		PageCatalogBytes: binary.LittleEndian.Uint32(
			payload[stateRootPageCatalogDigestEnd:stateRootPageCatalogBytesEnd],
		),
		MaxKeyBytes: binary.LittleEndian.Uint32(
			payload[stateRootPageCatalogBytesEnd:stateRootMaxKeyBytesEnd],
		),
		InlineValueBytes: binary.LittleEndian.Uint32(
			payload[stateRootMaxKeyBytesEnd:stateRootInlineValueBytesEnd],
		),
		MaxDocumentBytes: binary.LittleEndian.Uint32(
			payload[stateRootInlineValueBytesEnd:stateRootMaxDocumentBytesEnd],
		),
		PrimaryRoot: decodePageRef(
			payload[stateRootPrimaryOffset:stateRootPrimaryEnd],
		),
		ExactIndexRoot: decodePageRef(
			payload[stateRootExactIndexOffset:stateRootExactIndexEnd],
		),
		PhysicalCapacityBytes: binary.LittleEndian.Uint64(
			payload[stateRootPhysicalCapacityOffset:stateRootPhysicalCapacityEnd],
		),
	}
	copy(root.JournalID[:], payload[stateRootJournalIDOffset:stateRootJournalIDEnd])
	copy(
		root.PageCatalogDigest[:],
		payload[stateRootPageCatalogEnd:stateRootPageCatalogDigestEnd],
	)
	if err := validateStateRoot(root, fileEnd); err != nil {
		return StateRoot{}, fmt.Errorf("%w: %v", ErrStateRootCorrupt, err)
	}
	return root, nil
}

func encodePageRef(dst []byte, ref PageRef) {
	binary.LittleEndian.PutUint64(dst[0:8], ref.Offset)
	binary.LittleEndian.PutUint64(dst[8:16], ref.LogicalID)
	binary.LittleEndian.PutUint64(dst[16:24], ref.Generation)
	binary.LittleEndian.PutUint32(dst[24:28], ref.Length)
	dst[28] = byte(ref.Kind)
	clear(dst[29:32])
}

func decodePageRef(src []byte) PageRef {
	return PageRef{
		Offset:     binary.LittleEndian.Uint64(src[0:8]),
		LogicalID:  binary.LittleEndian.Uint64(src[8:16]),
		Generation: binary.LittleEndian.Uint64(src[16:24]),
		Length:     binary.LittleEndian.Uint32(src[24:28]),
		Kind:       PageKind(src[28]),
	}
}

func pageRefReservedZero(src []byte) bool {
	return allZero(src[29:PageRefSize])
}

func validateStateRoot(root StateRoot, fileEnd uint64) error {
	if root.StoreID == ([16]byte{}) || root.Generation == 0 ||
		!validPhysicalPageSize(root.PageSize) || root.Options&^stateRootKnownOptions != 0 {
		return fmt.Errorf("%w: state identity, page size, or options", ErrInvalidWrite)
	}
	materializationEnabled :=
		root.Options&StateOptionCanonicalMaterialization != 0
	damageGranule := root.MaterializationDamageGranule
	if materializationEnabled != (damageGranule != 0) ||
		damageGranule != 0 &&
			(damageGranule < MaterializationJournalMinSectorSize ||
				damageGranule&(damageGranule-1) != 0 ||
				damageGranule > MaterializationJournalMaxData ||
				root.PageSize%damageGranule != 0) {
		return fmt.Errorf("%w: canonical materialization geometry", ErrInvalidWrite)
	}
	pageSize := uint64(root.PageSize)
	layout, err := MutableStoreLayout(root.PageSize)
	if err != nil {
		return err
	}
	if fileEnd < layout.DataStart || fileEnd > maxSuperblockFileOffset || fileEnd%pageSize != 0 {
		return fmt.Errorf("%w: state file high-water mark", ErrInvalidWrite)
	}
	if root.PhysicalCapacityBytes != 0 &&
		(root.PhysicalCapacityBytes < fileEnd ||
			root.PhysicalCapacityBytes > maxSuperblockFileOffset ||
			root.PhysicalCapacityBytes%pageSize != 0) {
		return fmt.Errorf("%w: sealed physical capacity", ErrInvalidWrite)
	}
	if root.PhysicalCapacityBytes != 0 &&
		(root.JournalID != ([16]byte{}) || materializationEnabled) {
		return fmt.Errorf(
			"%w: sealed physical capacity requires a rooted copy-on-write state",
			ErrInvalidWrite,
		)
	}
	hasCatalog := root.IndexCount != 0 ||
		root.Options&(StateOptionSchema|StateOptionSkipIndexes) != 0
	hasExactCatalog := root.PageCatalogBytes != 0
	if !validPhysicalPageSize(root.MaxPageSize) ||
		root.MaxPageSize < root.PageSize ||
		root.MaxPageSize%root.PageSize != 0 {
		return fmt.Errorf("%w: maximum page geometry", ErrInvalidWrite)
	}
	hasAdmissionBounds := root.MaxKeyBytes != 0 ||
		root.InlineValueBytes != 0 ||
		root.MaxDocumentBytes != 0
	if hasAdmissionBounds &&
		(root.MaxKeyBytes == 0 ||
			root.InlineValueBytes == 0 ||
			root.MaxDocumentBytes == 0 ||
			root.InlineValueBytes > root.MaxDocumentBytes) {
		return fmt.Errorf("%w: immutable admission geometry", ErrInvalidWrite)
	}
	if hasCatalog != hasExactCatalog ||
		hasCatalog != (root.IndexCatalogHash != 0) {
		return fmt.Errorf("%w: canonical catalog presence", ErrInvalidWrite)
	}
	if !hasCatalog {
		if root.PageCatalogHead != (PageRef{}) ||
			root.PageCatalogDigest != ([PageCatalogDigestSize]byte{}) {
			return fmt.Errorf("%w: empty canonical catalog identity", ErrInvalidWrite)
		}
	} else if root.PageCatalogHead == (PageRef{}) ||
		root.PageCatalogBytes < PageCatalogCanonicalHeaderSize ||
		root.PageCatalogBytes > PageCatalogMaxCanonicalBytes {
		return fmt.Errorf("%w: canonical catalog identity", ErrInvalidWrite)
	}
	if root.NextLogicalID == 0 {
		return fmt.Errorf("%w: state counts", ErrInvalidWrite)
	}
	if root.DocumentCount != 0 && root.PrimaryRoot == (PageRef{}) {
		return fmt.Errorf("%w: documents without primary root", ErrInvalidWrite)
	}

	if err := validateStatePrimaryRoot(root, fileEnd); err != nil {
		return err
	}
	if err := validateStateExactIndexRoot(root, fileEnd); err != nil {
		return err
	}
	if root.ExactIndexRoot != (PageRef{}) &&
		(root.PrimaryRoot.LogicalID == root.ExactIndexRoot.LogicalID ||
			root.PrimaryRoot.Offset == root.ExactIndexRoot.Offset) {
		return fmt.Errorf("%w: duplicate primary/exact-index root", ErrInvalidWrite)
	}
	if hasCatalog {
		if err := validateStatePageCatalog(root, fileEnd); err != nil {
			return err
		}
	}
	return nil
}

func validateStatePageCatalog(root StateRoot, fileEnd uint64) error {
	if err := validateStatePageRef(
		root.PageCatalogHead, PageCatalogSegment, true, root, fileEnd,
	); err != nil {
		return err
	}
	catalogExtent, logicalEnd, ok := stateRootPageCatalogRun(root)
	runEnd := catalogExtent.Offset + catalogExtent.Length
	if !ok || runEnd > fileEnd ||
		logicalEnd > root.NextLogicalID {
		return fmt.Errorf("%w: canonical catalog run", ErrInvalidWrite)
	}
	refs := [...]PageRef{
		root.PrimaryRoot,
		root.ExactIndexRoot,
	}
	for _, ref := range refs {
		if ref == (PageRef{}) {
			continue
		}
		refExtent := FreeExtent{
			Offset: ref.Offset,
			Length: uint64(ref.Length),
		}
		if extentsOverlap(catalogExtent, refExtent) ||
			ref.LogicalID >= root.PageCatalogHead.LogicalID &&
				ref.LogicalID < logicalEnd {
			return fmt.Errorf(
				"%w: canonical catalog overlaps live root",
				ErrInvalidWrite,
			)
		}
	}
	return nil
}

func validateStatePrimaryRoot(root StateRoot, fileEnd uint64) error {
	ref := root.PrimaryRoot
	if ref == (PageRef{}) {
		return nil
	}
	layout, err := MutableStoreLayout(root.PageSize)
	if err != nil {
		return err
	}
	length := uint64(ref.Length)
	if ref.Kind != PagePrimaryCatalog ||
		ref.Length != GlobalTabletCatalogRootBytes ||
		root.MaxPageSize < ref.Length ||
		ref.Generation == 0 || ref.Generation > root.Generation ||
		ref.LogicalID != PrimaryCatalogRootLogicalID ||
		root.NextLogicalID < PrimaryFirstDynamicLogicalID ||
		ref.Offset < layout.DataStart ||
		ref.Offset%uint64(root.PageSize) != 0 ||
		length > fileEnd || ref.Offset > maxSuperblockFileOffset ||
		ref.Offset > fileEnd-length {
		return fmt.Errorf("%w: invalid ordered primary root", ErrInvalidWrite)
	}
	return nil
}

// validateStateExactIndexRoot validates the PagePrimaryExactRoot published for
// exact indexes carried as posting tiles on the ordered graph. The root is
// required exactly when an ordered primary declares indexes, and is forbidden
// otherwise.
func validateStateExactIndexRoot(root StateRoot, fileEnd uint64) error {
	ref := root.ExactIndexRoot
	required := root.PrimaryRoot != (PageRef{}) && root.IndexCount != 0
	if ref == (PageRef{}) {
		if required {
			return fmt.Errorf("%w: missing ordered-primary exact-index root", ErrInvalidWrite)
		}
		return nil
	}
	if !required {
		return fmt.Errorf("%w: unneeded ordered-primary exact-index root", ErrInvalidWrite)
	}
	bounds := PrimaryExactIndexBounds{
		StoreID: root.StoreID, Generation: root.Generation,
		FileEnd: fileEnd, NextLogicalID: root.NextLogicalID,
		AllocationQuantum: root.PageSize, MaxPageSize: root.MaxPageSize,
		IndexCount: root.IndexCount,
	}
	if ref.Kind != PagePrimaryExactRoot ||
		!validPrimaryExactRef(ref, PagePrimaryExactRoot, bounds) {
		return fmt.Errorf("%w: invalid ordered-primary exact-index root", ErrInvalidWrite)
	}
	return nil
}

func stateRootPageCatalogRun(
	root StateRoot,
) (FreeExtent, uint64, bool) {
	if root.PageCatalogBytes == 0 ||
		root.PageCatalogHead == (PageRef{}) {
		return FreeExtent{}, 0, false
	}
	segmentCount := pageCatalogSegmentCountFor(
		root.PageCatalogBytes, root.PageSize,
	)
	if segmentCount == 0 {
		return FreeExtent{}, 0, false
	}
	runLength := uint64(segmentCount) * uint64(root.PageSize)
	runEnd := root.PageCatalogHead.Offset + runLength
	logicalEnd := root.PageCatalogHead.LogicalID + uint64(segmentCount)
	if runEnd < root.PageCatalogHead.Offset ||
		logicalEnd < root.PageCatalogHead.LogicalID {
		return FreeExtent{}, 0, false
	}
	return FreeExtent{
		Offset: root.PageCatalogHead.Offset,
		Length: runLength,
	}, logicalEnd, true
}

// StateRootPageCatalogExtent returns the immutable physical catalog run
// published by root. Callers that replay or restore external free-space state
// use this value as FreeLogBounds.ProtectedExtent before admitting reusable
// extents.
func StateRootPageCatalogExtent(root StateRoot) (FreeExtent, bool) {
	extent, _, ok := stateRootPageCatalogRun(root)
	return extent, ok
}

func validateStatePageRef(ref PageRef, kind PageKind, required bool, root StateRoot, fileEnd uint64) error {
	if ref == (PageRef{}) {
		if required {
			return fmt.Errorf("%w: missing %d root", ErrInvalidWrite, kind)
		}
		return nil
	}
	pageSize := uint64(root.PageSize)
	layout, err := MutableStoreLayout(root.PageSize)
	if err != nil {
		return err
	}
	if ref.Kind != kind || ref.Length != root.PageSize ||
		ref.Generation == 0 || ref.Generation > root.Generation ||
		ref.LogicalID == 0 || ref.LogicalID >= root.NextLogicalID ||
		ref.Offset < layout.DataStart || ref.Offset%pageSize != 0 ||
		ref.Offset > maxSuperblockFileOffset || ref.Offset > fileEnd-pageSize {
		return fmt.Errorf("%w: invalid %d root reference", ErrInvalidWrite, kind)
	}
	return nil
}
