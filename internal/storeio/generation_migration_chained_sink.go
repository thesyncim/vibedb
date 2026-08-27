package storeio

import (
	"fmt"
	"os"
)

type GenerationMigrationStagingGrowFunc func(
	minimumDataBytes, minimumLogicalIDs uint64,
) (UnrootedGenerationReservation, GenerationMigrationManifest, error)

// GenerationMigrationChainedSink streams graph/index/catalog pages across
// independently admitted staging extents. It holds one max-page scratch window
// and one bounded current extent; it never sizes an arena from source FileEnd.
type GenerationMigrationChainedSink struct {
	file                        *os.File
	storeID                     [16]byte
	generation                  uint64
	pageSize, chunkBytes        uint32
	logicalIDsPerChunk          uint64
	grow                        GenerationMigrationStagingGrowFunc
	scratch                     []byte
	writer                      *UnrootedGenerationWriter
	nextLogicalID, finalFileEnd uint64
	pending                     bool
}

func NewGenerationMigrationChainedSink(
	file *os.File, storeID [16]byte, generation uint64,
	pageSize, chunkBytes uint32, logicalIDsPerChunk uint64,
	scratch []byte, grow GenerationMigrationStagingGrowFunc,
) (*GenerationMigrationChainedSink, error) {
	if file == nil || storeID == ([16]byte{}) || generation == 0 ||
		!validPhysicalPageSize(pageSize) || chunkBytes < pageSize ||
		uint64(chunkBytes)%uint64(pageSize) != 0 || logicalIDsPerChunk == 0 ||
		len(scratch) < int(pageSize) || grow == nil {
		return nil, fmt.Errorf("%w: migration chained sink", ErrInvalidWrite)
	}
	return &GenerationMigrationChainedSink{
		file: file, storeID: storeID, generation: generation,
		pageSize: pageSize, chunkBytes: chunkBytes,
		logicalIDsPerChunk: logicalIDsPerChunk,
		scratch:            scratch, grow: grow,
	}, nil
}

type generationMigrationChainedPage struct {
	owner *GenerationMigrationChainedSink
	ref   PageRef
	image []byte
}

func (p *generationMigrationChainedPage) Bytes() []byte { return p.image }
func (p *generationMigrationChainedPage) Ref() PageRef  { return p.ref }
func (p *generationMigrationChainedPage) Stage() error {
	if p == nil || p.owner == nil || !p.owner.pending {
		return ErrBatchState
	}
	if err := p.owner.writer.Append(p.ref, p.image); err != nil {
		return err
	}
	p.owner.pending = false
	return nil
}

func (s *GenerationMigrationChainedSink) AllocatePage(
	kind PageKind, length uint32, logicalID uint64,
) (PrimaryGraphBuildPage, error) {
	if s == nil || s.pending || length == 0 || length > uint32(len(s.scratch)) ||
		length%physicalPageQuantum != 0 {
		return nil, ErrBatchState
	}
	if s.writer == nil || uint64(length) > s.writer.reservation.Length-s.writer.written {
		minimum := uint64(max(length, s.chunkBytes))
		logicalIDs := max(s.logicalIDsPerChunk, minimum/uint64(s.pageSize))
		reservation, manifest, err := s.grow(minimum, logicalIDs)
		if err != nil {
			return nil, err
		}
		if reservation.Length < minimum || reservation.Offset == 0 ||
			reservation.FirstLogicalID == 0 || reservation.LogicalIDCount < logicalIDs {
			return nil, fmt.Errorf("%w: migration chained growth witness", ErrInvalidWrite)
		}
		writer, err := NewUnrootedGenerationWriter(
			s.file, reservation, s.storeID, s.generation, 0,
		)
		if err != nil {
			return nil, err
		}
		s.writer = writer
		s.nextLogicalID = reservation.FirstLogicalID
		s.finalFileEnd = manifest.TargetFileEnd
	}
	if logicalID == 0 {
		logicalID = s.nextLogicalID
		s.nextLogicalID++
	} else if logicalID >= s.nextLogicalID {
		return nil, fmt.Errorf("%w: migration replacement logical id", ErrInvalidWrite)
	}
	ref := PageRef{
		Offset:    s.writer.reservation.Offset + s.writer.written,
		LogicalID: logicalID, Generation: s.generation,
		Length: length, Kind: kind,
	}
	image := s.scratch[:int(length):int(length)]
	clear(image)
	s.pending = true
	return &generationMigrationChainedPage{owner: s, ref: ref, image: image}, nil
}

func (s *GenerationMigrationChainedSink) StoreIdentity() [16]byte    { return s.storeID }
func (s *GenerationMigrationChainedSink) BuildGeneration() uint64    { return s.generation }
func (s *GenerationMigrationChainedSink) BuildFileEnd() uint64       { return s.finalFileEnd }
func (s *GenerationMigrationChainedSink) BuildNextLogicalID() uint64 { return s.nextLogicalID }
func (s *GenerationMigrationChainedSink) MaxBuildPageBytes() int     { return len(s.scratch) }
func (s *GenerationMigrationChainedSink) Sync() error {
	if s == nil || s.pending {
		return ErrBatchState
	}
	if s.writer == nil {
		return nil
	}
	return s.writer.Sync()
}
