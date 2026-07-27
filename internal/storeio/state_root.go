package storeio

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	// StateRootLogicalID is fixed so a recovered superblock cannot silently
	// reinterpret an arbitrary page as the root of a Store generation.
	StateRootLogicalID = uint64(1)
	// StateRootPayloadSize is deliberately larger than the fields in use. Every
	// current value ends at stateRootReservedOffset, and the remainder is
	// reserved, written zero, and validated zero on decode. A later development
	// field advances that boundary without moving an existing offset or changing
	// the payload length.
	//
	// It has been exactly full once already — six page references ending on the
	// byte the payload ended on — which turned the tail check into a comparison
	// against an empty slice and would have forced a layout change on the next
	// field added. The reserve exists so that does not recur; do not shrink the
	// payload back to the last field.
	//
	// The room is free: a state root occupies a whole page, which is at least
	// 4096 bytes, so the payload length is a schema bound rather than a
	// physical one.
	StateRootPayloadSize = 512
	// PageRefSize is the fixed encoded width of one pointer-free physical page
	// reference.
	PageRefSize = 32

	// stateRootVersion is 0 for as long as the format is under development.
	// Nothing is released, so there is no older file to read and no ladder to
	// climb: a layout change edits this schema in place rather than adding a
	// version, and every file written before it simply stops opening. The first
	// release takes version 1 and starts the ladder for real.
	stateRootVersion               = uint32(0)
	stateRootFreeHintOffset        = 52
	stateRootChunkRefOffset        = 56
	stateRootKeyRefOffset          = stateRootChunkRefOffset + PageRefSize
	stateRootIndexRefOffset        = stateRootKeyRefOffset + PageRefSize
	stateRootRefsEnd               = stateRootIndexRefOffset + PageRefSize
	stateRootFloat64Offset         = stateRootRefsEnd
	stateRootFloat64End            = stateRootFloat64Offset + PageRefSize
	stateRootIndexGroupOffset      = stateRootFloat64End
	stateRootIndexGroupEnd         = stateRootIndexGroupOffset + PageRefSize
	stateRootMaterializationOffset = stateRootIndexGroupEnd
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
	// stateRootReservedOffset begins the zero-filled suffix described on
	// StateRootPayloadSize.
	stateRootReservedOffset = stateRootPrimaryEnd
)

// State-root option bits are durable equivalents of Store construction
// options. Unknown bits fail closed.
const (
	StateOptionShapeTapes uint32 = 1 << iota
	StateOptionPostings
	StateOptionValueDict
	StateOptionHashKeys
	// StateOptionFloat64Columns means every live document page carries the
	// complete configured float64 covering-column catalog.
	StateOptionFloat64Columns
	// StateOptionSchema means the durable catalog hash also binds an
	// application-supplied document schema. The schema definition remains
	// caller configuration; reopening with a different definition fails.
	StateOptionSchema
	// StateOptionCanonicalMaterialization means the file may contain
	// recovery-journaled canonical page replacements. The exact qualified
	// power-loss damage granule is carried by StateRoot so Open can recover
	// safely before consulting caller options.
	StateOptionCanonicalMaterialization
)

const stateRootKnownOptions = StateOptionShapeTapes |
	StateOptionPostings |
	StateOptionValueDict |
	StateOptionHashKeys |
	StateOptionFloat64Columns |
	StateOptionSchema |
	StateOptionCanonicalMaterialization

// ErrStateRootCorrupt reports a common page that passed basic framing but does
// not encode a valid Store state root.
var ErrStateRootCorrupt = errors.New("vibejson: corrupt Store state root")

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
	Flags      uint8
	// Aux is routing metadata for schemas that explicitly define it. It is
	// zero for every ordinary reference. Document-group leaves use it only
	// with DocumentGroupFlagFloat64Sidecar to encode bounded physical and
	// logical deltas to a shared typed extent.
	Aux uint16
}

// StateRoot is the compact, pointer-free graph root named by a Superblock.
// Its current directory references separate document placement, key lookup,
// and secondary indexes so a mutation copies only affected paths. PrimaryRoot
// independently selects the ordered replacement graph during its staged
// cutover. The persistent free-page tree remains a separate Superblock root.
type StateRoot struct {
	StoreID        [16]byte
	Generation     uint64
	PageSize       uint32
	Options        uint32
	DocumentCount  uint64
	NextLogicalID  uint64
	ChunkHighWater uint32
	LiveChunks     uint32
	ChunkDocuments uint32
	IndexCount     uint32
	IndexMaxDepth  uint32
	// IndexCatalogHash is a compact non-authoritative rejection key for the
	// exact PageCatalog identity. Open must reconstruct and compare the
	// canonical catalog bytes; this value is never a fallback authority.
	IndexCatalogHash uint64
	// FreeChunkHint is a conservative lower bound for the first chunk that
	// may contain a free stable slot. ChunkHighWater means no known hole.
	// Insertion advances it while delete can lower it in O(1), avoiding a
	// heap-side free-slot object or pointer for every key.
	FreeChunkHint uint32
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
	ChunkDirectory   PageRef
	KeyDirectory     PageRef
	IndexDirectory   PageRef
	// Float64ScanHead names the ordered value-stripe directory of a compact or
	// incrementally maintained generation. It is only a scan accelerator;
	// documented mutation fallbacks clear it and use the authoritative tree.
	Float64ScanHead PageRef
	// IndexGroupHead names bounded aggregate-only categorical cover pages.
	// Exact postings remain authoritative.
	IndexGroupHead PageRef
	// PrimaryRoot selects the ordered tablet graph. Zero keeps the current
	// fingerprint/chunk primary authoritative during the staged cutover.
	PrimaryRoot PageRef
}

// EncodeStateRootPage writes and seals one complete common-format page into
// caller storage. fileEnd is copied from the prospective Superblock and bounds
// every physical reference before publication. No allocation is performed.
func EncodeStateRootPage(dst []byte, root StateRoot, fileEnd uint64) ([]byte, error) {
	if err := validateStateRoot(root, fileEnd); err != nil {
		return nil, err
	}
	header := PageHeader{
		StoreID:       root.StoreID,
		Generation:    root.Generation,
		LogicalID:     StateRootLogicalID,
		PageSize:      root.PageSize,
		PayloadLength: StateRootPayloadSize,
		Kind:          PageStateRoot,
	}
	payload, err := InitPage(dst, header)
	if err != nil {
		return nil, err
	}
	encodeStateRootPayload(payload, root)
	page := dst[:int(root.PageSize)]
	if _, err := sealInitializedPage(page); err != nil {
		return nil, err
	}
	return page, nil
}

// encodeStateRootPayload writes the identity-free StateRoot body shared by the
// standalone page envelope and the inline superblock envelope. The caller
// validates root against its enclosing FileEnd before calling it.
func encodeStateRootPayload(payload []byte, root StateRoot) {
	clear(payload)
	binary.LittleEndian.PutUint32(payload[0:4], stateRootVersion)
	binary.LittleEndian.PutUint32(payload[4:8], root.Options)
	binary.LittleEndian.PutUint64(payload[8:16], root.DocumentCount)
	binary.LittleEndian.PutUint64(payload[16:24], root.NextLogicalID)
	binary.LittleEndian.PutUint32(payload[24:28], root.ChunkHighWater)
	binary.LittleEndian.PutUint32(payload[28:32], root.LiveChunks)
	binary.LittleEndian.PutUint32(payload[32:36], root.ChunkDocuments)
	binary.LittleEndian.PutUint32(payload[36:40], root.IndexCount)
	binary.LittleEndian.PutUint32(payload[40:44], root.IndexMaxDepth)
	binary.LittleEndian.PutUint64(payload[44:52], root.IndexCatalogHash)
	binary.LittleEndian.PutUint32(payload[stateRootFreeHintOffset:stateRootChunkRefOffset], root.FreeChunkHint)
	encodePageRef(payload[stateRootChunkRefOffset:stateRootKeyRefOffset], root.ChunkDirectory)
	encodePageRef(payload[stateRootKeyRefOffset:stateRootIndexRefOffset], root.KeyDirectory)
	encodePageRef(payload[stateRootIndexRefOffset:stateRootRefsEnd], root.IndexDirectory)
	encodePageRef(payload[stateRootFloat64Offset:stateRootFloat64End], root.Float64ScanHead)
	encodePageRef(payload[stateRootIndexGroupOffset:stateRootIndexGroupEnd], root.IndexGroupHead)
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
}

// DecodeStateRootPage verifies a complete common page and its state-root
// schema. fileEnd is the high-water mark from the selecting Superblock. The
// result contains values only and retains no reference to src.
func DecodeStateRootPage(src []byte, fileEnd uint64) (StateRoot, error) {
	header, payload, err := OpenPage(src)
	if err != nil {
		return StateRoot{}, fmt.Errorf("%w: %w", ErrStateRootCorrupt, err)
	}
	if header.Kind != PageStateRoot || header.LogicalID != StateRootLogicalID {
		return StateRoot{}, fmt.Errorf("%w: header, version, or reserved bytes", ErrStateRootCorrupt)
	}
	return decodeStateRootPayload(
		payload, header.StoreID, header.Generation, header.PageSize, fileEnd,
	)
}

// decodeStateRootPayload decodes the identity-free StateRoot body shared by
// standalone and inline roots. Identity comes from the checksummed enclosing
// envelope and is validated with every state field before return.
func decodeStateRootPayload(
	payload []byte, storeID [16]byte, generation uint64, pageSize uint32, fileEnd uint64,
) (StateRoot, error) {
	if len(payload) != StateRootPayloadSize ||
		binary.LittleEndian.Uint32(payload[0:4]) != stateRootVersion ||
		!pageRefReservedZero(payload[stateRootChunkRefOffset:stateRootKeyRefOffset]) ||
		!pageRefReservedZero(payload[stateRootKeyRefOffset:stateRootIndexRefOffset]) ||
		!pageRefReservedZero(payload[stateRootIndexRefOffset:stateRootRefsEnd]) ||
		!pageRefReservedZero(payload[stateRootFloat64Offset:stateRootFloat64End]) ||
		!pageRefReservedZero(payload[stateRootIndexGroupOffset:stateRootIndexGroupEnd]) ||
		!pageRefReservedZero(payload[stateRootPageCatalogOffset:stateRootPageCatalogEnd]) ||
		!pageRefReservedZero(payload[stateRootPrimaryOffset:stateRootPrimaryEnd]) ||
		!allZero(payload[stateRootReservedOffset:]) {
		return StateRoot{}, fmt.Errorf("%w: header, version, or reserved bytes", ErrStateRootCorrupt)
	}
	freeChunkHint := binary.LittleEndian.Uint32(payload[stateRootFreeHintOffset:stateRootChunkRefOffset])
	float64ScanHead := decodePageRef(payload[stateRootFloat64Offset:stateRootFloat64End])
	indexGroupHead := decodePageRef(payload[stateRootIndexGroupOffset:stateRootIndexGroupEnd])
	root := StateRoot{
		StoreID:          storeID,
		Generation:       generation,
		PageSize:         pageSize,
		Options:          binary.LittleEndian.Uint32(payload[4:8]),
		DocumentCount:    binary.LittleEndian.Uint64(payload[8:16]),
		NextLogicalID:    binary.LittleEndian.Uint64(payload[16:24]),
		ChunkHighWater:   binary.LittleEndian.Uint32(payload[24:28]),
		LiveChunks:       binary.LittleEndian.Uint32(payload[28:32]),
		ChunkDocuments:   binary.LittleEndian.Uint32(payload[32:36]),
		IndexCount:       binary.LittleEndian.Uint32(payload[36:40]),
		IndexMaxDepth:    binary.LittleEndian.Uint32(payload[40:44]),
		IndexCatalogHash: binary.LittleEndian.Uint64(payload[44:52]),
		FreeChunkHint:    freeChunkHint,
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
		ChunkDirectory:  decodePageRef(payload[stateRootChunkRefOffset:stateRootKeyRefOffset]),
		KeyDirectory:    decodePageRef(payload[stateRootKeyRefOffset:stateRootIndexRefOffset]),
		IndexDirectory:  decodePageRef(payload[stateRootIndexRefOffset:stateRootRefsEnd]),
		Float64ScanHead: float64ScanHead,
		IndexGroupHead:  indexGroupHead,
		PrimaryRoot: decodePageRef(
			payload[stateRootPrimaryOffset:stateRootPrimaryEnd],
		),
	}
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
	dst[29] = ref.Flags
	binary.LittleEndian.PutUint16(dst[30:32], ref.Aux)
}

func decodePageRef(src []byte) PageRef {
	return PageRef{
		Offset:     binary.LittleEndian.Uint64(src[0:8]),
		LogicalID:  binary.LittleEndian.Uint64(src[8:16]),
		Generation: binary.LittleEndian.Uint64(src[16:24]),
		Length:     binary.LittleEndian.Uint32(src[24:28]),
		Kind:       PageKind(src[28]),
		Flags:      src[29],
		Aux:        binary.LittleEndian.Uint16(src[30:32]),
	}
}

func pageRefReservedZero(src []byte) bool {
	return binary.LittleEndian.Uint16(src[30:PageRefSize]) == 0
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
	hasCatalog := root.IndexCount != 0 ||
		root.Options&(StateOptionFloat64Columns|StateOptionSchema) != 0
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
	if root.ChunkDocuments == 0 || root.ChunkDocuments > 64 ||
		root.LiveChunks > root.ChunkHighWater ||
		root.FreeChunkHint > root.ChunkHighWater ||
		root.NextLogicalID <= StateRootLogicalID {
		return fmt.Errorf("%w: state counts", ErrInvalidWrite)
	}
	primarySelected := root.PrimaryRoot != (PageRef{})
	if root.LiveChunks == 0 {
		if root.DocumentCount != 0 && !primarySelected {
			return fmt.Errorf("%w: documents without chunks", ErrInvalidWrite)
		}
	} else if root.DocumentCount < uint64(root.LiveChunks) ||
		root.DocumentCount > uint64(root.LiveChunks)*uint64(root.ChunkDocuments) {
		return fmt.Errorf("%w: document/chunk counts", ErrInvalidWrite)
	}

	keyKind := PageKeyDirectory
	if root.KeyDirectory.Kind == PageFingerprintDirectory {
		keyKind = PageFingerprintDirectory
	}
	refs := [3]struct {
		ref      PageRef
		kind     PageKind
		required bool
		allowed  bool
	}{
		{root.ChunkDirectory, PageChunkDirectory, root.LiveChunks != 0, root.LiveChunks != 0},
		{
			root.KeyDirectory, keyKind,
			root.DocumentCount != 0 && !primarySelected,
			root.DocumentCount != 0 && (!primarySelected || root.LiveChunks != 0),
		},
		{root.IndexDirectory, PageIndexDirectory, false, root.IndexCount != 0},
	}
	for i := range refs {
		if err := validateStatePageRef(refs[i].ref, refs[i].kind, refs[i].required, root, fileEnd); err != nil {
			return err
		}
		if !refs[i].allowed && refs[i].ref != (PageRef{}) {
			return fmt.Errorf("%w: unneeded %d root", ErrInvalidWrite, refs[i].kind)
		}
		if refs[i].ref == (PageRef{}) {
			continue
		}
		for j := 0; j < i; j++ {
			if refs[j].ref != (PageRef{}) &&
				(refs[j].ref.LogicalID == refs[i].ref.LogicalID || refs[j].ref.Offset == refs[i].ref.Offset) {
				return fmt.Errorf("%w: duplicate root reference", ErrInvalidWrite)
			}
		}
	}
	if err := validateStatePrimaryRoot(root, fileEnd); err != nil {
		return err
	}
	if root.PrimaryRoot != (PageRef{}) {
		for i := range refs {
			if refs[i].ref != (PageRef{}) &&
				(refs[i].ref.LogicalID == root.PrimaryRoot.LogicalID ||
					refs[i].ref.Offset == root.PrimaryRoot.Offset) {
				return fmt.Errorf("%w: duplicate primary root", ErrInvalidWrite)
			}
		}
	}
	if root.Float64ScanHead != (PageRef{}) {
		ref := root.Float64ScanHead
		length := uint64(ref.Length)
		if root.Options&StateOptionFloat64Columns == 0 ||
			ref.Kind != PageFloat64Catalog || ref.Flags != 0 || ref.Aux != 0 ||
			ref.Length != root.PageSize ||
			ref.Generation == 0 || ref.Generation > root.Generation ||
			ref.LogicalID <= StateRootLogicalID || ref.LogicalID >= root.NextLogicalID ||
			ref.Offset < layout.DataStart || ref.Offset%pageSize != 0 ||
			length > fileEnd || ref.Offset > maxSuperblockFileOffset ||
			ref.Offset > fileEnd-length {
			return fmt.Errorf("%w: invalid float64 scan head", ErrInvalidWrite)
		}
		for i := range refs {
			if refs[i].ref != (PageRef{}) &&
				(refs[i].ref.LogicalID == ref.LogicalID || refs[i].ref.Offset == ref.Offset) {
				return fmt.Errorf("%w: duplicate float64 scan head", ErrInvalidWrite)
			}
		}
		if root.PrimaryRoot != (PageRef{}) &&
			(root.PrimaryRoot.LogicalID == ref.LogicalID ||
				root.PrimaryRoot.Offset == ref.Offset) {
			return fmt.Errorf("%w: duplicate primary/float64 root", ErrInvalidWrite)
		}
	}
	if root.IndexGroupHead != (PageRef{}) {
		ref := root.IndexGroupHead
		length := uint64(ref.Length)
		if root.IndexCount == 0 || root.DocumentCount == 0 ||
			ref.Kind != PageIndexGroupCatalog || ref.Flags != 0 || ref.Aux != 0 ||
			!validPhysicalPageSize(ref.Length) || ref.Length < root.PageSize ||
			ref.Length%root.PageSize != 0 ||
			ref.Generation == 0 || ref.Generation > root.Generation ||
			ref.LogicalID <= StateRootLogicalID || ref.LogicalID >= root.NextLogicalID ||
			ref.Offset < layout.DataStart || ref.Offset%pageSize != 0 ||
			length > fileEnd || ref.Offset > maxSuperblockFileOffset ||
			ref.Offset > fileEnd-length {
			return fmt.Errorf("%w: invalid index group head", ErrInvalidWrite)
		}
		for i := range refs {
			if refs[i].ref != (PageRef{}) &&
				(refs[i].ref.LogicalID == ref.LogicalID || refs[i].ref.Offset == ref.Offset) {
				return fmt.Errorf("%w: duplicate index group head", ErrInvalidWrite)
			}
		}
		if root.Float64ScanHead != (PageRef{}) &&
			(root.Float64ScanHead.LogicalID == ref.LogicalID ||
				root.Float64ScanHead.Offset == ref.Offset) {
			return fmt.Errorf("%w: duplicate aggregate head", ErrInvalidWrite)
		}
		if root.PrimaryRoot != (PageRef{}) &&
			(root.PrimaryRoot.LogicalID == ref.LogicalID ||
				root.PrimaryRoot.Offset == ref.Offset) {
			return fmt.Errorf("%w: duplicate primary/aggregate root", ErrInvalidWrite)
		}
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
		root.ChunkDirectory,
		root.KeyDirectory,
		root.IndexDirectory,
		root.Float64ScanHead,
		root.IndexGroupHead,
		root.PrimaryRoot,
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
	if ref.Kind != PagePrimaryCatalog || ref.Flags != 0 || ref.Aux != 0 ||
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
	if ref.Kind != kind || ref.Flags != 0 || ref.Aux != 0 || ref.Length != root.PageSize ||
		ref.Generation == 0 || ref.Generation > root.Generation ||
		ref.LogicalID <= StateRootLogicalID || ref.LogicalID >= root.NextLogicalID ||
		ref.Offset < layout.DataStart || ref.Offset%pageSize != 0 ||
		ref.Offset > maxSuperblockFileOffset || ref.Offset > fileEnd-pageSize {
		return fmt.Errorf("%w: invalid %d root reference", ErrInvalidWrite, kind)
	}
	return nil
}
