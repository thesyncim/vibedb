package storeio

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	generationMigrationTabletVectorPageBytes = 4096
	generationMigrationTabletVectorHeader    = 64
	generationMigrationTabletVectorTrailer   = 8
	generationMigrationTabletVectorEntry     = 2 * PageRefSize
	GenerationMigrationTabletVectorFanout    = (generationMigrationTabletVectorPageBytes - generationMigrationTabletVectorHeader - generationMigrationTabletVectorTrailer) / generationMigrationTabletVectorEntry
	generationMigrationTabletVectorMagic     = "SGTVEC00"
)

// GenerationMigrationTabletRef is the immutable source witness and current
// staged replacement of one macro-tablet. The tablet ID is its vector ordinal.
type GenerationMigrationTabletRef struct {
	Source PageRef
	Target PageRef
}

// GenerationMigrationTabletVector is a bounded-memory, fixed-position on-disk
// vector. Every logical block has two independently checksummed slots, so a
// torn replacement falls back to the prior source/target witness.
type GenerationMigrationTabletVector struct {
	file        *os.File
	offset      int64
	tablets     uint32
	storeID     [16]byte
	migrationID [16]byte
	mu          sync.Mutex
	pages       [3][generationMigrationTabletVectorPageBytes]byte
}

func OpenGenerationMigrationTabletVector(file *os.File, offset int64, tablets uint32, storeID, migrationID [16]byte) (*GenerationMigrationTabletVector, error) {
	if file == nil || offset < 0 || offset%generationMigrationTabletVectorPageBytes != 0 || tablets == 0 || storeID == ([16]byte{}) || migrationID == ([16]byte{}) {
		return nil, fmt.Errorf("%w: migration tablet vector", ErrInvalidWrite)
	}
	return &GenerationMigrationTabletVector{file: file, offset: offset, tablets: tablets, storeID: storeID, migrationID: migrationID}, nil
}

func (v *GenerationMigrationTabletVector) PhysicalBytes() uint64 {
	blocks := (uint64(v.tablets) + GenerationMigrationTabletVectorFanout - 1) / GenerationMigrationTabletVectorFanout
	return blocks * 2 * generationMigrationTabletVectorPageBytes
}

func (v *GenerationMigrationTabletVector) Get(tablet uint32) (GenerationMigrationTabletRef, bool, error) {
	if v == nil || tablet >= v.tablets {
		return GenerationMigrationTabletRef{}, false, fmt.Errorf("%w: tablet vector ordinal", ErrInvalidWrite)
	}
	block := tablet / GenerationMigrationTabletVectorFanout
	v.mu.Lock()
	entries, _, found, err := v.readBlockLocked(block)
	v.mu.Unlock()
	if err != nil || !found {
		return GenerationMigrationTabletRef{}, false, err
	}
	entry := entries[tablet%GenerationMigrationTabletVectorFanout]
	return entry, entry.Source != (PageRef{}), nil
}

func (v *GenerationMigrationTabletVector) Put(tablet uint32, entry GenerationMigrationTabletRef) error {
	if v == nil || tablet >= v.tablets || !validMigrationTabletRef(entry.Source) ||
		(entry.Target != (PageRef{}) && !validMigrationTabletRef(entry.Target)) {
		return fmt.Errorf("%w: tablet vector entry", ErrInvalidWrite)
	}
	block := tablet / GenerationMigrationTabletVectorFanout
	v.mu.Lock()
	defer v.mu.Unlock()
	entries, sequence, found, err := v.readBlockLocked(block)
	if err != nil {
		return err
	}
	if !found {
		entries = [GenerationMigrationTabletVectorFanout]GenerationMigrationTabletRef{}
	}
	ordinal := tablet % GenerationMigrationTabletVectorFanout
	previous := entries[ordinal]
	if previous.Source != (PageRef{}) && previous.Source != entry.Source {
		return fmt.Errorf("%w: tablet source witness changed", ErrInvalidWrite)
	}
	if sequence == ^uint64(0) {
		return fmt.Errorf("%w: tablet vector sequence exhausted", ErrInvalidWrite)
	}
	entries[ordinal] = entry
	sequence++
	image := &v.pages[2]
	encodeGenerationMigrationTabletVectorPage(image[:], v.storeID, v.migrationID, block, sequence, entries[:])
	slot := sequence & 1
	_, err = v.file.WriteAt(image[:], v.blockSlotOffset(block, slot))
	return err
}

func (v *GenerationMigrationTabletVector) Sync() error { return v.file.Sync() }

func (v *GenerationMigrationTabletVector) Visit(fn func(uint32, GenerationMigrationTabletRef) error) error {
	if v == nil || fn == nil {
		return fmt.Errorf("%w: tablet vector visitor", ErrInvalidWrite)
	}
	blocks := (v.tablets + GenerationMigrationTabletVectorFanout - 1) /
		GenerationMigrationTabletVectorFanout
	for block := uint32(0); block < blocks; block++ {
		v.mu.Lock()
		entries, _, found, err := v.readBlockLocked(block)
		v.mu.Unlock()
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		for local := uint32(0); local < GenerationMigrationTabletVectorFanout; local++ {
			tablet := block*GenerationMigrationTabletVectorFanout + local
			if tablet >= v.tablets {
				break
			}
			entry := entries[local]
			if entry.Source != (PageRef{}) {
				if err := fn(tablet, entry); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func validMigrationTabletRef(ref PageRef) bool {
	return ref.Offset != 0 && ref.LogicalID != 0 && ref.Generation != 0 && ref.Length != 0 && validPhysicalPageSize(ref.Length) && ref.Kind == PageTabletRoute
}

func (v *GenerationMigrationTabletVector) readBlockLocked(block uint32) ([GenerationMigrationTabletVectorFanout]GenerationMigrationTabletRef, uint64, bool, error) {
	var best [GenerationMigrationTabletVectorFanout]GenerationMigrationTabletRef
	var bestSequence uint64
	found := false
	for slot := uint64(0); slot < 2; slot++ {
		image := &v.pages[slot]
		_, readErr := v.file.ReadAt(image[:], v.blockSlotOffset(block, slot))
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return best, 0, false, readErr
		}
		entries, sequence, err := openGenerationMigrationTabletVectorPage(image[:], v.storeID, v.migrationID, block)
		if err == nil && (!found || sequence > bestSequence) {
			best, bestSequence, found = entries, sequence, true
		}
	}
	return best, bestSequence, found, nil
}

func (v *GenerationMigrationTabletVector) blockSlotOffset(block uint32, slot uint64) int64 {
	return v.offset + int64((uint64(block)*2+slot)*generationMigrationTabletVectorPageBytes)
}

func encodeGenerationMigrationTabletVectorPage(dst []byte, storeID, migrationID [16]byte, block uint32, sequence uint64, entries []GenerationMigrationTabletRef) {
	clear(dst)
	copy(dst[:8], generationMigrationTabletVectorMagic)
	binary.LittleEndian.PutUint32(dst[8:12], DevelopmentFormatVersion)
	binary.LittleEndian.PutUint32(dst[12:16], block)
	binary.LittleEndian.PutUint64(dst[16:24], sequence)
	copy(dst[24:40], storeID[:])
	copy(dst[40:56], migrationID[:])
	binary.LittleEndian.PutUint16(dst[56:58], GenerationMigrationTabletVectorFanout)
	for index := 0; index < GenerationMigrationTabletVectorFanout; index++ {
		at := generationMigrationTabletVectorHeader + index*generationMigrationTabletVectorEntry
		encodePageRef(dst[at:at+PageRefSize], entries[index].Source)
		encodePageRef(dst[at+PageRefSize:at+2*PageRefSize], entries[index].Target)
	}
	trailer := len(dst) - generationMigrationTabletVectorTrailer
	checksum := PageChecksum(dst[:trailer])
	binary.LittleEndian.PutUint32(dst[trailer:trailer+4], checksum)
	binary.LittleEndian.PutUint32(dst[trailer+4:], ^checksum)
}

func openGenerationMigrationTabletVectorPage(src []byte, storeID, migrationID [16]byte, block uint32) ([GenerationMigrationTabletVectorFanout]GenerationMigrationTabletRef, uint64, error) {
	var entries [GenerationMigrationTabletVectorFanout]GenerationMigrationTabletRef
	if len(src) != generationMigrationTabletVectorPageBytes || !bytes.Equal(src[:8], []byte(generationMigrationTabletVectorMagic)) || binary.LittleEndian.Uint32(src[8:12]) != DevelopmentFormatVersion || binary.LittleEndian.Uint32(src[12:16]) != block || !bytes.Equal(src[24:40], storeID[:]) || !bytes.Equal(src[40:56], migrationID[:]) || binary.LittleEndian.Uint16(src[56:58]) != GenerationMigrationTabletVectorFanout || !allZero(src[58:64]) {
		return entries, 0, ErrGenerationMigrationManifestCorrupt
	}
	trailer := len(src) - generationMigrationTabletVectorTrailer
	checksum := binary.LittleEndian.Uint32(src[trailer : trailer+4])
	if binary.LittleEndian.Uint32(src[trailer+4:]) != ^checksum || PageChecksum(src[:trailer]) != checksum {
		return entries, 0, ErrGenerationMigrationManifestCorrupt
	}
	sequence := binary.LittleEndian.Uint64(src[16:24])
	if sequence == 0 {
		return entries, 0, ErrGenerationMigrationManifestCorrupt
	}
	for index := range entries {
		at := generationMigrationTabletVectorHeader + index*generationMigrationTabletVectorEntry
		entries[index] = GenerationMigrationTabletRef{Source: decodePageRef(src[at : at+PageRefSize]), Target: decodePageRef(src[at+PageRefSize : at+2*PageRefSize])}
		if entries[index].Source == (PageRef{}) {
			if entries[index].Target != (PageRef{}) {
				return entries, 0, ErrGenerationMigrationManifestCorrupt
			}
			continue
		}
		if !validMigrationTabletRef(entries[index].Source) || entries[index].Target != (PageRef{}) && !validMigrationTabletRef(entries[index].Target) {
			return entries, 0, ErrGenerationMigrationManifestCorrupt
		}
	}
	return entries, sequence, nil
}
