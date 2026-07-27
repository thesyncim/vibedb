package storeio

import (
	"fmt"
	"io"
	"os"
)

type portableDevice struct {
	file                     *os.File
	arena                    []byte
	frameArena               []byte
	frameSize                int
	bufferSize               int
	buffers                  int
	seen                     []uint64
	checkpointSync           CheckpointSync
	checkpointBarrier        func(*os.File) error
	checkpointFinalSync      func(*os.File) error
	materializationBarrier   func(*os.File) error
	materializationFinalSync func(*os.File) error
	closed                   bool
}

func openPortableDevice(file *os.File, options DeviceOptions) (*portableDevice, error) {
	if options.BufferCount > int(^uint(0)>>1)/options.BufferSize {
		return nil, fmt.Errorf("%w: staging arena overflow", ErrInvalidWrite)
	}
	arena, err := allocateArena(options.BufferCount * options.BufferSize)
	if err != nil {
		return nil, fmt.Errorf("allocate Store page arena: %w", err)
	}
	checkpointBarrier := dataBarrier
	checkpointFinalSync := dataSync
	if options.CheckpointSync == CheckpointSyncFilesystem {
		// The alternate root must never become filesystem-stable before every
		// page it names. Ordinary mode weakens only the primitive, not the
		// two-phase ordering: sync data, write the alternate root, then sync
		// the root.
		checkpointBarrier = filesystemSync
		checkpointFinalSync = filesystemSync
	}
	return &portableDevice{
		file:                     file,
		arena:                    arena,
		bufferSize:               options.BufferSize,
		buffers:                  options.BufferCount,
		seen:                     make([]uint64, (options.BufferCount+63)/64),
		checkpointSync:           options.CheckpointSync,
		checkpointBarrier:        checkpointBarrier,
		checkpointFinalSync:      checkpointFinalSync,
		materializationBarrier:   materializationPhaseBarrier,
		materializationFinalSync: materializationSync,
	}, nil
}

func (*portableDevice) Backend() Backend { return BackendPortable }

func (d *portableDevice) bindFrameArena(arena []byte, frameSize int) error {
	if d.closed || len(arena) == 0 || frameSize <= 0 ||
		len(arena)%frameSize != 0 {
		return ErrInvalidWrite
	}
	if len(d.frameArena) != 0 && &d.frameArena[0] != &arena[0] {
		return ErrInvalidWrite
	}
	d.frameArena = arena
	d.frameSize = frameSize
	return nil
}

func (d *portableDevice) Buffer(index int) ([]byte, error) {
	if d.closed {
		return nil, ErrClosed
	}
	if index < 0 || index >= d.buffers {
		return nil, ErrInvalidWrite
	}
	start := index * d.bufferSize
	return d.arena[start : start+d.bufferSize], nil
}

func (d *portableDevice) Commit(pages []Write, root Write) error {
	if d.closed {
		return ErrClosed
	}
	if err := validateCommit(d.buffers, d.bufferSize, d.seen, pages, root); err != nil {
		return err
	}
	if err := writeDataPages(
		d.file, d.arena, d.bufferSize,
		d.frameArena, d.frameSize, pages,
	); err != nil {
		return err
	}
	if len(pages) != 0 {
		barrier := d.checkpointBarrier
		if barrier == nil {
			barrier = dataBarrier
		}
		if err := barrier(d.file); err != nil {
			return err
		}
	}
	if err := d.write(root); err != nil {
		return commitOutcomeUnknown(err)
	}
	finalSync := d.checkpointFinalSync
	if finalSync == nil {
		finalSync = dataSync
	}
	return commitOutcomeUnknown(finalSync(d.file))
}

func (d *portableDevice) Prewrite(pages []Write) error {
	if d.closed {
		return ErrClosed
	}
	return writeDataPages(
		d.file, d.arena, d.bufferSize,
		d.frameArena, d.frameSize, pages,
	)
}

func (d *portableDevice) CommitMaterialized(
	journal Write,
	targets []Write,
	root Write,
	mode materializationCommitMode,
) (materializationCommitResult, error) {
	if d.closed {
		return materializationCommitResult{}, ErrClosed
	}
	if mode != materializationPatchOnly && mode != materializationHybrid {
		return materializationCommitResult{}, ErrInvalidWrite
	}
	if _, err := validateWrite(
		d.buffers, d.bufferSize, journal,
	); err != nil {
		return materializationCommitResult{}, err
	}
	if err := validateCommit(
		d.buffers, d.bufferSize, d.seen, targets, root,
	); err != nil {
		return materializationCommitResult{}, err
	}
	if err := d.write(journal); err != nil {
		return materializationCommitResult{}, err
	}
	barrier := d.materializationBarrier
	if barrier == nil {
		barrier = materializationPhaseBarrier
	}
	if err := barrier(d.file); err != nil {
		return materializationCommitResult{}, err
	}
	if err := writeDataPages(
		d.file, d.arena, d.bufferSize,
		d.frameArena, d.frameSize, targets,
	); err != nil {
		return materializationCommitResult{
			CompletedPhases: 1, CompletedBarriers: 1,
		}, err
	}
	result := materializationCommitResult{
		CompletedPhases: 1, CompletedBarriers: 1,
	}
	if mode == materializationHybrid {
		if err := barrier(d.file); err != nil {
			return result, err
		}
		result.CompletedPhases = 2
		result.CompletedBarriers = 2
	} else {
		result.CompletedPhases = 2
	}
	if err := d.write(root); err != nil {
		result.RootAttempted = true
		return result, err
	}
	result.RootAttempted = true
	finalSync := d.materializationFinalSync
	if finalSync == nil {
		finalSync = materializationSync
	}
	if err := finalSync(d.file); err != nil {
		return result, err
	}
	result.CompletedPhases = 3
	result.CompletedBarriers++
	return result, nil
}

func (d *portableDevice) write(write Write) error {
	return writeArenaAt(
		d.file, d.arena, d.bufferSize,
		d.frameArena, d.frameSize, write,
	)
}

func writeArenaAt(
	file *os.File,
	arena []byte,
	bufferSize int,
	frameArena []byte,
	frameSize int,
	write Write,
) error {
	data, err := writeBytes(
		arena, bufferSize, frameArena, frameSize, write,
	)
	if err != nil {
		return err
	}
	n, err := file.WriteAt(data, write.Offset)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return err
	}
	return nil
}

func writeBytes(
	arena []byte,
	bufferSize int,
	frameArena []byte,
	frameSize int,
	write Write,
) ([]byte, error) {
	if write.frameNative() {
		if frameSize <= 0 {
			return nil, ErrInvalidWrite
		}
		start64 := uint64(write.frameIndex) * uint64(frameSize)
		end64 := start64 + uint64(write.Length)
		if start64 > uint64(len(frameArena)) ||
			end64 < start64 || end64 > uint64(len(frameArena)) {
			return nil, ErrInvalidWrite
		}
		start, end := int(start64), int(end64)
		return frameArena[start:end:end], nil
	}
	start := int(write.Buffer) * bufferSize
	return arena[start : start+int(write.Length)], nil
}

func (d *portableDevice) Close() error {
	if d == nil || d.closed {
		return nil
	}
	d.closed = true
	err := releaseArena(d.arena)
	d.arena = nil
	d.frameArena = nil
	d.seen = nil
	if err != nil {
		return fmt.Errorf("release Store page arena: %w", err)
	}
	return nil
}
