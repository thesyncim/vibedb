package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	GenerationMigrationManifestBytes = 4096
	generationMigrationHeaderBytes   = 112
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

// GenerationMigrationManifest is one bounded, restartable migration cut.
// Cursor is the last fully copied bytewise key; capture sequences delimit the
// ordered mutation suffix that must be applied before conditional publication.
type GenerationMigrationManifest struct {
	StoreID          [16]byte
	MigrationID      [16]byte
	Phase            GenerationMigrationPhase
	SourceGeneration uint64
	TargetGeneration uint64
	CapturedSequence uint64
	AppliedSequence  uint64
	SourceFileEnd    uint64
	TargetFileEnd    uint64
	Cursor           []byte
}

func EncodeGenerationMigrationManifest(dst []byte, m GenerationMigrationManifest) ([]byte, error) {
	if len(dst) < GenerationMigrationManifestBytes || m.StoreID == ([16]byte{}) ||
		m.MigrationID == ([16]byte{}) || m.Phase < GenerationMigrationCopying ||
		m.Phase > GenerationMigrationPublished || m.SourceGeneration == 0 ||
		m.TargetGeneration <= m.SourceGeneration ||
		m.AppliedSequence > m.CapturedSequence || len(m.Cursor) >
		generationMigrationTrailerAt-generationMigrationHeaderBytes {
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
	binary.LittleEndian.PutUint32(image[96:100], uint32(len(m.Cursor)))
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
	cursorBytes := int(binary.LittleEndian.Uint32(src[96:100]))
	if m.StoreID == ([16]byte{}) || m.MigrationID == ([16]byte{}) ||
		m.Phase < GenerationMigrationCopying || m.Phase > GenerationMigrationPublished ||
		m.SourceGeneration == 0 || m.TargetGeneration <= m.SourceGeneration ||
		m.AppliedSequence > m.CapturedSequence || cursorBytes < 0 ||
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
		next.TargetFileEnd < previous.TargetFileEnd ||
		(next.Phase == GenerationMigrationCopying &&
			bytes.Compare(next.Cursor, previous.Cursor) < 0) ||
		(next.Phase >= GenerationMigrationReady &&
			next.AppliedSequence != next.CapturedSequence) {
		return fmt.Errorf("%w: non-monotonic advance", ErrInvalidWrite)
	}
	return nil
}
