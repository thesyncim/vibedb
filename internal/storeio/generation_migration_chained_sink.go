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
	allocatedBytes              uint64
	scratchUsed                 int
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
	if p == nil || p.owner == nil || p.owner.writer == nil ||
		p.ref.Offset != p.owner.writer.reservation.Offset+p.owner.writer.written {
		return ErrBatchState
	}
	if err := p.owner.writer.Append(p.ref, p.image); err != nil {
		return err
	}
	if p.owner.writer.written == p.owner.allocatedBytes {
		p.owner.scratchUsed = 0
	}
	return nil
}

func (s *GenerationMigrationChainedSink) AllocatePage(
	kind PageKind, length uint32, logicalID uint64,
) (PrimaryGraphBuildPage, error) {
	if s == nil || length == 0 || int(length) > len(s.scratch)-s.scratchUsed ||
		length%physicalPageQuantum != 0 {
		return nil, ErrBatchState
	}
	if err := s.EnsureContiguousBuildBytes(uint64(length)); err != nil {
		return nil, err
	}
	if logicalID == 0 {
		logicalID = s.nextLogicalID
		s.nextLogicalID++
	} else if logicalID >= s.nextLogicalID {
		return nil, fmt.Errorf("%w: migration replacement logical id", ErrInvalidWrite)
	}
	ref := PageRef{
		Offset:    s.writer.reservation.Offset + s.allocatedBytes,
		LogicalID: logicalID, Generation: s.generation,
		Length: length, Kind: kind,
	}
	image := s.scratch[s.scratchUsed : s.scratchUsed+int(length) : s.scratchUsed+int(length)]
	clear(image)
	s.scratchUsed += int(length)
	s.allocatedBytes += uint64(length)
	return &generationMigrationChainedPage{owner: s, ref: ref, image: image}, nil
}

func (s *GenerationMigrationChainedSink) EnsureContiguousBuildBytes(minimum uint64) error {
	if s == nil || minimum == 0 || minimum%uint64(s.pageSize) != 0 {
		return ErrBatchState
	}
	if s.writer != nil && minimum <= s.writer.reservation.Length-s.allocatedBytes {
		return nil
	}
	if s.writer != nil && s.allocatedBytes != s.writer.written {
		return ErrBatchState
	}
	request := max(minimum, uint64(s.chunkBytes))
	logicalIDs := max(s.logicalIDsPerChunk, request/uint64(s.pageSize))
	reservation, manifest, err := s.grow(request, logicalIDs)
	if err != nil {
		return err
	}
	if reservation.Length < request || reservation.Offset == 0 ||
		reservation.FirstLogicalID == 0 || reservation.LogicalIDCount < logicalIDs {
		return fmt.Errorf("%w: migration chained growth witness", ErrInvalidWrite)
	}
	writer, err := NewUnrootedGenerationWriter(
		s.file, reservation, s.storeID, s.generation, 0,
	)
	if err != nil {
		return err
	}
	s.writer = writer
	s.allocatedBytes = 0
	s.scratchUsed = 0
	s.nextLogicalID = reservation.FirstLogicalID
	s.finalFileEnd = manifest.TargetFileEnd
	return nil
}

func (s *GenerationMigrationChainedSink) StoreIdentity() [16]byte    { return s.storeID }
func (s *GenerationMigrationChainedSink) BuildGeneration() uint64    { return s.generation }
func (s *GenerationMigrationChainedSink) BuildFileEnd() uint64       { return s.finalFileEnd }
func (s *GenerationMigrationChainedSink) BuildNextLogicalID() uint64 { return s.nextLogicalID }
func (s *GenerationMigrationChainedSink) MaxBuildPageBytes() int     { return len(s.scratch) }
func (s *GenerationMigrationChainedSink) Sync() error {
	if s == nil || s.writer != nil && s.allocatedBytes != s.writer.written {
		return ErrBatchState
	}
	if s.writer == nil {
		return nil
	}
	return s.writer.Sync()
}
