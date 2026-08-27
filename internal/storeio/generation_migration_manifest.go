package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	GenerationMigrationManifestBytes = 4096
	generationMigrationHeaderBytes   = 368
	generationMigrationTrailerAt     = GenerationMigrationManifestBytes - 8
	generationMigrationMagic         = "SGMIGR00"
)

var ErrGenerationMigrationManifestCorrupt = errors.New("vibedb: corrupt generation migration manifest")

type GenerationMigrationPhase uint8

const (
	GenerationMigrationCopying GenerationMigrationPhase = iota + 1
	GenerationMigrationCatchingUp
	GenerationMigrationReady
	GenerationMigrationPublished
)

type GenerationMigrationRetirementPhase uint8

const (
	GenerationMigrationRetireNone GenerationMigrationRetirementPhase = iota
	GenerationMigrationRetirePrimary
	GenerationMigrationRetireExact
	GenerationMigrationRetireCatalog
	GenerationMigrationRetireScratch
	GenerationMigrationRetireDone
)

// GenerationMigrationManifest is one bounded, restartable migration cut.
// Cursor is the last fully copied bytewise key; capture sequences delimit the
// ordered mutation suffix that must be applied before conditional publication.
type GenerationMigrationManifest struct {
	StoreID                                                    [16]byte
	MigrationID                                                [16]byte
	Phase                                                      GenerationMigrationPhase
	SourceGeneration                                           uint64
	TargetGeneration                                           uint64
	CapturedSequence                                           uint64
	AppliedSequence                                            uint64
	SourceFileEnd                                              uint64
	TargetFileEnd                                              uint64
	ReservedOffset                                             uint64
	ReservedBytes                                              uint64
	FirstLogicalID                                             uint64
	LogicalIDCount                                             uint64
	SourcePrimaryRoot, SourceExactIndexRoot, SourceCatalogHead PageRef
	TargetPrimaryRoot, TargetExactIndexRoot, TargetCatalogHead PageRef
	SourceCatalogBytes, SourceIndexCount                       uint32
	ManifestSequence, RetirementOrdinal                        uint64
	TargetScratchOffset, TargetScratchBytes                    uint64
	RetirementPhase                                            GenerationMigrationRetirementPhase
	Cursor                                                     []byte
}

func validGenerationMigrationRoot(ref PageRef, kind PageKind, required bool) bool {
	if ref == (PageRef{}) {
		return !required
	}
	return ref.Offset != 0 && ref.LogicalID != 0 && ref.Generation != 0 &&
		ref.Length != 0 && validPhysicalPageSize(ref.Length) && ref.Kind == kind
}

func EncodeGenerationMigrationManifest(dst []byte, m GenerationMigrationManifest) ([]byte, error) {
	if len(dst) < GenerationMigrationManifestBytes || m.StoreID == ([16]byte{}) ||
		m.MigrationID == ([16]byte{}) || m.Phase < GenerationMigrationCopying ||
		m.Phase > GenerationMigrationPublished || m.SourceGeneration == 0 ||
		m.TargetGeneration <= m.SourceGeneration ||
		m.AppliedSequence > m.CapturedSequence || len(m.Cursor) >
		generationMigrationTrailerAt-generationMigrationHeaderBytes ||
		m.ReservedBytes == 0 || m.LogicalIDCount == 0 ||
		m.ReservedOffset > ^uint64(0)-m.ReservedBytes ||
		m.FirstLogicalID > ^uint64(0)-m.LogicalIDCount ||
		!validGenerationMigrationRoot(m.SourcePrimaryRoot, PagePrimaryCatalog, true) ||
		!validGenerationMigrationRoot(m.SourceExactIndexRoot, PagePrimaryExactRoot, false) ||
		!validGenerationMigrationRoot(m.SourceCatalogHead, PageCatalogSegment, false) ||
		!validGenerationMigrationRoot(m.TargetPrimaryRoot, PagePrimaryCatalog, m.Phase >= GenerationMigrationReady) ||
		!validGenerationMigrationRoot(m.TargetExactIndexRoot, PagePrimaryExactRoot, false) ||
		!validGenerationMigrationRoot(m.TargetCatalogHead, PageCatalogSegment, false) ||
		(m.SourceCatalogBytes == 0) != (m.SourceCatalogHead == (PageRef{})) ||
		(m.SourceIndexCount == 0) != (m.SourceExactIndexRoot == (PageRef{})) ||
		m.RetirementPhase > GenerationMigrationRetireDone ||
		(m.RetirementPhase != GenerationMigrationRetireNone && m.Phase != GenerationMigrationPublished) ||
		(m.TargetScratchOffset == 0) != (m.TargetScratchBytes == 0) ||
		m.TargetScratchOffset&4095 != 0 || m.TargetScratchBytes&4095 != 0 ||
		m.TargetScratchOffset > ^uint64(0)-m.TargetScratchBytes ||
		(m.TargetScratchBytes != 0 && (m.TargetScratchOffset < m.ReservedOffset || m.TargetScratchOffset+m.TargetScratchBytes > m.ReservedOffset+m.ReservedBytes)) ||
		m.SourcePrimaryRoot.Generation > m.SourceGeneration ||
		m.TargetPrimaryRoot.Generation > m.TargetGeneration {
		return nil, fmt.Errorf("%w: fields", ErrInvalidWrite)
	}
	image := dst[:GenerationMigrationManifestBytes]
	clear(image)
	copy(image[:8], generationMigrationMagic)
	binary.LittleEndian.PutUint32(image[8:12], DevelopmentFormatVersion)
	image[12] = byte(m.Phase)
	copy(image[16:32], m.StoreID[:])
	copy(image[32:48], m.MigrationID[:])
	binary.LittleEndian.PutUint64(image[48:56], m.SourceGeneration)
	binary.LittleEndian.PutUint64(image[56:64], m.TargetGeneration)
	binary.LittleEndian.PutUint64(image[64:72], m.CapturedSequence)
	binary.LittleEndian.PutUint64(image[72:80], m.AppliedSequence)
	binary.LittleEndian.PutUint64(image[80:88], m.SourceFileEnd)
	binary.LittleEndian.PutUint64(image[88:96], m.TargetFileEnd)
	binary.LittleEndian.PutUint64(image[96:104], m.ReservedOffset)
	binary.LittleEndian.PutUint64(image[104:112], m.ReservedBytes)
	binary.LittleEndian.PutUint64(image[112:120], m.FirstLogicalID)
	binary.LittleEndian.PutUint64(image[120:128], m.LogicalIDCount)
	encodePageRef(image[128:160], m.SourcePrimaryRoot)
	encodePageRef(image[160:192], m.SourceExactIndexRoot)
	encodePageRef(image[192:224], m.SourceCatalogHead)
	encodePageRef(image[224:256], m.TargetPrimaryRoot)
	encodePageRef(image[256:288], m.TargetExactIndexRoot)
	encodePageRef(image[288:320], m.TargetCatalogHead)
	binary.LittleEndian.PutUint32(image[320:324], uint32(len(m.Cursor)))
	binary.LittleEndian.PutUint32(image[324:328], m.SourceCatalogBytes)
	binary.LittleEndian.PutUint32(image[328:332], m.SourceIndexCount)
	image[332] = byte(m.RetirementPhase)
	binary.LittleEndian.PutUint64(image[336:344], m.ManifestSequence)
	binary.LittleEndian.PutUint64(image[344:352], m.RetirementOrdinal)
	binary.LittleEndian.PutUint64(image[352:360], m.TargetScratchOffset)
	binary.LittleEndian.PutUint64(image[360:368], m.TargetScratchBytes)
	copy(image[generationMigrationHeaderBytes:], m.Cursor)
	checksum := PageChecksum(image[:generationMigrationTrailerAt])
	binary.LittleEndian.PutUint32(image[generationMigrationTrailerAt:], checksum)
	binary.LittleEndian.PutUint32(image[generationMigrationTrailerAt+4:], ^checksum)
	return image, nil
}

func OpenGenerationMigrationManifest(src []byte) (GenerationMigrationManifest, error) {
	var m GenerationMigrationManifest
	if len(src) != GenerationMigrationManifestBytes ||
		!bytes.Equal(src[:8], []byte(generationMigrationMagic)) ||
		binary.LittleEndian.Uint32(src[8:12]) != DevelopmentFormatVersion {
		return m, ErrGenerationMigrationManifestCorrupt
	}
	checksum := binary.LittleEndian.Uint32(src[generationMigrationTrailerAt:])
	if binary.LittleEndian.Uint32(src[generationMigrationTrailerAt+4:]) != ^checksum ||
		PageChecksum(src[:generationMigrationTrailerAt]) != checksum {
		return m, ErrGenerationMigrationManifestCorrupt
	}
	m.Phase = GenerationMigrationPhase(src[12])
	copy(m.StoreID[:], src[16:32])
	copy(m.MigrationID[:], src[32:48])
	m.SourceGeneration = binary.LittleEndian.Uint64(src[48:56])
	m.TargetGeneration = binary.LittleEndian.Uint64(src[56:64])
	m.CapturedSequence = binary.LittleEndian.Uint64(src[64:72])
	m.AppliedSequence = binary.LittleEndian.Uint64(src[72:80])
	m.SourceFileEnd = binary.LittleEndian.Uint64(src[80:88])
	m.TargetFileEnd = binary.LittleEndian.Uint64(src[88:96])
	m.ReservedOffset = binary.LittleEndian.Uint64(src[96:104])
	m.ReservedBytes = binary.LittleEndian.Uint64(src[104:112])
	m.FirstLogicalID = binary.LittleEndian.Uint64(src[112:120])
	m.LogicalIDCount = binary.LittleEndian.Uint64(src[120:128])
	m.SourcePrimaryRoot = decodePageRef(src[128:160])
	m.SourceExactIndexRoot = decodePageRef(src[160:192])
	m.SourceCatalogHead = decodePageRef(src[192:224])
	m.TargetPrimaryRoot = decodePageRef(src[224:256])
	m.TargetExactIndexRoot = decodePageRef(src[256:288])
	m.TargetCatalogHead = decodePageRef(src[288:320])
	m.SourceCatalogBytes = binary.LittleEndian.Uint32(src[324:328])
	m.SourceIndexCount = binary.LittleEndian.Uint32(src[328:332])
	m.RetirementPhase = GenerationMigrationRetirementPhase(src[332])
	m.ManifestSequence = binary.LittleEndian.Uint64(src[336:344])
	m.RetirementOrdinal = binary.LittleEndian.Uint64(src[344:352])
	m.TargetScratchOffset = binary.LittleEndian.Uint64(src[352:360])
	m.TargetScratchBytes = binary.LittleEndian.Uint64(src[360:368])
	cursorBytes := int(binary.LittleEndian.Uint32(src[320:324]))
	if m.StoreID == ([16]byte{}) || m.MigrationID == ([16]byte{}) ||
		m.Phase < GenerationMigrationCopying || m.Phase > GenerationMigrationPublished ||
		m.SourceGeneration == 0 || m.TargetGeneration <= m.SourceGeneration ||
		m.AppliedSequence > m.CapturedSequence || cursorBytes < 0 ||
		m.ReservedBytes == 0 || m.LogicalIDCount == 0 ||
		m.ReservedOffset > ^uint64(0)-m.ReservedBytes ||
		m.FirstLogicalID > ^uint64(0)-m.LogicalIDCount ||
		!validGenerationMigrationRoot(m.SourcePrimaryRoot, PagePrimaryCatalog, true) ||
		!validGenerationMigrationRoot(m.SourceExactIndexRoot, PagePrimaryExactRoot, false) ||
		!validGenerationMigrationRoot(m.SourceCatalogHead, PageCatalogSegment, false) ||
		!validGenerationMigrationRoot(m.TargetPrimaryRoot, PagePrimaryCatalog, m.Phase >= GenerationMigrationReady) ||
		!validGenerationMigrationRoot(m.TargetExactIndexRoot, PagePrimaryExactRoot, false) ||
		!validGenerationMigrationRoot(m.TargetCatalogHead, PageCatalogSegment, false) ||
		(m.SourceCatalogBytes == 0) != (m.SourceCatalogHead == (PageRef{})) ||
		(m.SourceIndexCount == 0) != (m.SourceExactIndexRoot == (PageRef{})) ||
		m.RetirementPhase > GenerationMigrationRetireDone ||
		(m.RetirementPhase != GenerationMigrationRetireNone && m.Phase != GenerationMigrationPublished) ||
		(m.TargetScratchOffset == 0) != (m.TargetScratchBytes == 0) ||
		m.TargetScratchOffset&4095 != 0 || m.TargetScratchBytes&4095 != 0 ||
		m.TargetScratchOffset > ^uint64(0)-m.TargetScratchBytes ||
		(m.TargetScratchBytes != 0 && (m.TargetScratchOffset < m.ReservedOffset || m.TargetScratchOffset+m.TargetScratchBytes > m.ReservedOffset+m.ReservedBytes)) ||
		m.SourcePrimaryRoot.Generation > m.SourceGeneration ||
		m.TargetPrimaryRoot.Generation > m.TargetGeneration ||
		!allZero(src[333:336]) ||
		cursorBytes > generationMigrationTrailerAt-generationMigrationHeaderBytes ||
		!allZero(src[generationMigrationHeaderBytes+cursorBytes:generationMigrationTrailerAt]) {
		return GenerationMigrationManifest{}, ErrGenerationMigrationManifestCorrupt
	}
	m.Cursor = src[generationMigrationHeaderBytes : generationMigrationHeaderBytes+cursorBytes]
	return m, nil
}

// ValidateGenerationMigrationAdvance rejects rollback or identity substitution
// before a newer manifest record is made durable.
func ValidateGenerationMigrationAdvance(previous, next GenerationMigrationManifest) error {
	if previous.StoreID != next.StoreID || previous.MigrationID != next.MigrationID ||
		previous.SourceGeneration != next.SourceGeneration ||
		previous.TargetGeneration != next.TargetGeneration || next.Phase < previous.Phase ||
		next.CapturedSequence < previous.CapturedSequence ||
		next.AppliedSequence < previous.AppliedSequence ||
		next.AppliedSequence > next.CapturedSequence ||
		next.SourceFileEnd != previous.SourceFileEnd ||
		next.ReservedOffset != previous.ReservedOffset ||
		next.ReservedBytes != previous.ReservedBytes ||
		next.FirstLogicalID != previous.FirstLogicalID ||
		next.LogicalIDCount != previous.LogicalIDCount ||
		next.SourcePrimaryRoot != previous.SourcePrimaryRoot ||
		next.SourceExactIndexRoot != previous.SourceExactIndexRoot ||
		next.SourceCatalogHead != previous.SourceCatalogHead ||
		next.SourceCatalogBytes != previous.SourceCatalogBytes ||
		next.SourceIndexCount != previous.SourceIndexCount ||
		(previous.ManifestSequence != 0 && next.ManifestSequence <= previous.ManifestSequence) ||
		next.RetirementPhase < previous.RetirementPhase ||
		(next.RetirementPhase == previous.RetirementPhase && next.RetirementOrdinal < previous.RetirementOrdinal) ||
		(previous.TargetScratchBytes != 0 && (next.TargetScratchOffset != previous.TargetScratchOffset || next.TargetScratchBytes != previous.TargetScratchBytes)) ||
		previous.TargetPrimaryRoot != (PageRef{}) && next.TargetPrimaryRoot != previous.TargetPrimaryRoot ||
		previous.TargetExactIndexRoot != (PageRef{}) && next.TargetExactIndexRoot != previous.TargetExactIndexRoot ||
		previous.TargetCatalogHead != (PageRef{}) && next.TargetCatalogHead != previous.TargetCatalogHead ||
		!validGenerationMigrationRoot(next.SourcePrimaryRoot, PagePrimaryCatalog, true) ||
		!validGenerationMigrationRoot(next.TargetPrimaryRoot, PagePrimaryCatalog, next.Phase >= GenerationMigrationReady) ||
		next.SourcePrimaryRoot.Generation > next.SourceGeneration ||
		next.TargetPrimaryRoot.Generation > next.TargetGeneration ||
		next.TargetFileEnd < previous.TargetFileEnd ||
		(next.Phase == GenerationMigrationCopying &&
			bytes.Compare(next.Cursor, previous.Cursor) < 0) ||
		(next.Phase >= GenerationMigrationReady &&
			next.AppliedSequence != next.CapturedSequence) {
		return fmt.Errorf("%w: non-monotonic advance", ErrInvalidWrite)
	}
	return nil
}
