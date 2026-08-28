package storeio

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"syscall"
)

const (
	ringDataVectorTag = ^uint64(0) - iota
	ringDataSyncTag
	ringRootTag
	ringRootSyncTag
)

type ringDevice struct {
	ring       *Ring
	bufferSize int
	buffers    int
	seen       []uint64
	frameArena []byte
	frameSize  int
	dsyncOff   bool
	closed     bool
}

func openRingDevice(file *os.File, options DeviceOptions) (*ringDevice, error) {
	ring, err := Open(Config{Entries: uint32(options.QueueDepth), SingleIssuer: options.SingleIssuer})
	if err != nil {
		return nil, err
	}
	if err := ring.RegisterFiles([]int{int(file.Fd())}); err != nil {
		_ = ring.Close()
		return nil, classifyRingSetupError("register file", err)
	}
	if err := ring.RegisterBuffers(options.BufferCount, options.BufferSize); err != nil {
		_ = ring.Close()
		return nil, classifyRingSetupError("register buffers", err)
	}
	return &ringDevice{
		ring:       ring,
		bufferSize: options.BufferSize,
		buffers:    options.BufferCount,
		seen:       make([]uint64, (options.BufferCount+63)/64),
	}, nil
}

func classifyRingSetupError(operation string, err error) error {
	if ringSetupUnavailable(err) {
		return fmt.Errorf("%w: io_uring %s: %v", ErrUnavailable, operation, err)
	}
	return err
}

func (*ringDevice) Backend() Backend { return BackendIOUring }

func (d *ringDevice) bindFrameArena(arena []byte, frameSize int) error {
	if d.closed || len(arena) == 0 || frameSize <= 0 ||
		len(arena)%frameSize != 0 {
		return ErrInvalidWrite
	}
	if len(d.frameArena) != 0 && &d.frameArena[0] != &arena[0] {
		return ErrInvalidWrite
	}
	d.frameArena = arena
	d.frameSize = frameSize
	// Keep the fixed staging/root buffers, but do not register the whole cold
	// cache. Stable-address frame writes pin only their submitted spans. This
	// trades per-I/O pinning work for avoiding eager cache-capacity residency;
	// ownership remains held through the ordinary completion/durability fence.
	return nil
}

func (d *ringDevice) Buffer(index int) ([]byte, error) { return d.ring.Buffer(index) }

func (d *ringDevice) Commit(pages []Write, root Write) error {
	if d.closed {
		return ErrClosed
	}
	if err := validateCommit(d.buffers, d.bufferSize, d.seen, pages, root); err != nil {
		return err
	}
	if d.vectorCommitEligible(pages, root) {
		return d.commitVector(pages, root)
	}
	for i, write := range pages {
		if err := d.prepareWrite(write, uint64(i), false); err != nil {
			return err
		}
	}
	if len(pages) != 0 {
		if err := d.ring.SubmitAndWait(uint32(len(pages))); err != nil {
			return err
		}
		completions, err := d.ring.PopBatch(uint32(len(pages)))
		if err != nil {
			return err
		}
		if len(completions) != len(pages) {
			return ErrOverflow
		}
		var first error
		for _, completion := range completions {
			if completion.UserData >= uint64(len(pages)) {
				return ErrOverflow
			}
			write := pages[completion.UserData]
			if err := completionResult(completion, write.Length); first == nil && err != nil {
				first = err
			}
		}
		if first != nil {
			return first
		}
	}
	if d.dsyncOff {
		return d.commitRootWithSync(root, len(pages) != 0)
	}

	if len(pages) != 0 {
		if err := d.ring.PrepareDataSync(0, ringDataSyncTag, true); err != nil {
			return err
		}
	}
	if err := d.ring.prepareWriteFixedDataSync(
		0, int(root.Buffer), int(root.Length), root.Offset, ringRootTag, false,
	); err != nil {
		return err
	}
	want := uint32(1)
	if len(pages) != 0 {
		want++
	}
	if err := d.ring.SubmitAndWait(want); err != nil {
		return commitOutcomeUnknown(err)
	}
	completions, err := d.ring.PopBatch(want)
	if err != nil {
		return commitOutcomeUnknown(err)
	}
	if len(completions) != int(want) {
		return commitOutcomeUnknown(ErrOverflow)
	}
	var dataSyncErr, rootErr error
	for _, completion := range completions {
		switch completion.UserData {
		case ringDataSyncTag:
			dataSyncErr = completionResult(completion, 0)
		case ringRootTag:
			rootErr = completionResult(completion, root.Length)
		default:
			return commitOutcomeUnknown(ErrOverflow)
		}
	}
	if dataSyncErr != nil {
		return commitOutcomeUnknown(errors.Join(dataSyncErr, rootErr))
	}
	if errors.Is(rootErr, syscall.EOPNOTSUPP) {
		d.dsyncOff = true
		return d.commitRootWithSync(root, false)
	}
	return commitOutcomeUnknown(rootErr)
}

// commitRootWithSync is the compatibility path for filesystems that reject
// per-write RWF_DSYNC. It preserves the original optional data barrier,
// linked root write, and final whole-file data sync sequence.
func (d *ringDevice) commitRootWithSync(root Write, dataBarrier bool) error {
	if dataBarrier {
		if err := d.ring.PrepareDataSync(0, ringDataSyncTag, true); err != nil {
			return err
		}
	}
	if err := d.ring.PrepareWriteFixed(
		0, int(root.Buffer), int(root.Length), root.Offset, ringRootTag, true,
	); err != nil {
		return err
	}
	if err := d.ring.PrepareDataSync(0, ringRootSyncTag, false); err != nil {
		return err
	}
	want := uint32(2)
	if dataBarrier {
		want++
	}
	if err := d.ring.SubmitAndWait(want); err != nil {
		return commitOutcomeUnknown(err)
	}
	completions, err := d.ring.PopBatch(want)
	if err != nil {
		return commitOutcomeUnknown(err)
	}
	if len(completions) != int(want) {
		return commitOutcomeUnknown(ErrOverflow)
	}
	var first error
	for _, completion := range completions {
		var expected uint32
		switch completion.UserData {
		case ringDataSyncTag, ringRootSyncTag:
		case ringRootTag:
			expected = root.Length
		default:
			return commitOutcomeUnknown(ErrOverflow)
		}
		if err := completionResult(completion, expected); first == nil && err != nil {
			first = err
		}
	}
	return commitOutcomeUnknown(first)
}

func (d *ringDevice) vectorCommitEligible(pages []Write, root Write) bool {
	if d.dsyncOff || len(pages) == 0 ||
		len(pages) > d.ring.vectorWriteCapacity() {
		return false
	}
	nextOffset := pages[0].Offset
	var total uint64
	for _, write := range pages {
		if write.Offset != nextOffset ||
			total+uint64(write.Length) > math.MaxInt32 {
			return false
		}
		if !write.frameNative() && !root.frameNative() &&
			write.Buffer == root.Buffer {
			return false
		}
		total += uint64(write.Length)
		nextOffset += int64(write.Length)
	}
	return true
}

// commitVector collapses a contiguous data extent into one durable WRITEV and
// links the durable root write behind it. A failed or short vector write
// severs the soft link in the kernel, so the root is never executed unless all
// data bytes reached data-integrity completion.
func (d *ringDevice) commitVector(pages []Write, root Write) error {
	var dataLength uint32
	var err error
	if len(pages) == 1 && !pages[0].frameNative() {
		dataLength = pages[0].Length
		err = d.ring.prepareWriteFixedDataSync(
			0, int(pages[0].Buffer), int(pages[0].Length),
			pages[0].Offset, ringDataVectorTag, true,
		)
	} else {
		dataLength, err = d.ring.prepareWriteVectorDataSync(
			0, pages, d.bufferSize, d.frameArena, d.frameSize,
			ringDataVectorTag, true,
		)
	}
	if err != nil {
		return err
	}
	if err := d.ring.prepareWriteFixedDataSync(
		0, int(root.Buffer), int(root.Length), root.Offset, ringRootTag, false,
	); err != nil {
		return err
	}
	if err := d.ring.SubmitAndWait(2); err != nil {
		return commitOutcomeUnknown(err)
	}
	completions, err := d.ring.PopBatch(2)
	if err != nil {
		return commitOutcomeUnknown(err)
	}
	if len(completions) != 2 {
		return commitOutcomeUnknown(ErrOverflow)
	}
	var dataErr, rootErr error
	var dataSeen, rootSeen bool
	for _, completion := range completions {
		switch completion.UserData {
		case ringDataVectorTag:
			if dataSeen {
				return commitOutcomeUnknown(ErrOverflow)
			}
			dataSeen = true
			dataErr = completionResult(completion, dataLength)
		case ringRootTag:
			if rootSeen {
				return commitOutcomeUnknown(ErrOverflow)
			}
			rootSeen = true
			rootErr = completionResult(completion, root.Length)
		default:
			return commitOutcomeUnknown(ErrOverflow)
		}
	}
	if !dataSeen || !rootSeen {
		return commitOutcomeUnknown(ErrOverflow)
	}
	if dataErr != nil {
		if errors.Is(dataErr, syscall.EOPNOTSUPP) &&
			errors.Is(rootErr, syscall.ECANCELED) {
			d.dsyncOff = true
			return d.Commit(pages, root)
		}
		if errors.Is(rootErr, syscall.ECANCELED) {
			return dataErr
		}
		return commitOutcomeUnknown(errors.Join(dataErr, rootErr))
	}
	if errors.Is(rootErr, syscall.EOPNOTSUPP) {
		d.dsyncOff = true
		return d.commitRootWithSync(root, false)
	}
	return commitOutcomeUnknown(rootErr)
}

func (d *ringDevice) Prewrite(pages []Write) error {
	if d.closed {
		return ErrClosed
	}
	for i, write := range pages {
		if err := d.prepareWrite(write, uint64(i), false); err != nil {
			return err
		}
	}
	if err := d.ring.SubmitAndWait(uint32(len(pages))); err != nil {
		return err
	}
	completions, err := d.ring.PopBatch(uint32(len(pages)))
	if err != nil {
		return err
	}
	if len(completions) != len(pages) {
		return ErrOverflow
	}
	var first error
	for _, completion := range completions {
		if completion.UserData >= uint64(len(pages)) {
			return ErrOverflow
		}
		write := pages[completion.UserData]
		if err := completionResult(completion, write.Length); first == nil && err != nil {
			first = err
		}
	}
	return first
}

func (d *ringDevice) prepareWrite(
	write Write, userData uint64, linked bool,
) error {
	if write.frameNative() {
		data, err := writeBytes(
			nil, d.bufferSize, d.frameArena, d.frameSize, write,
		)
		if err != nil {
			return err
		}
		return d.ring.prepareWriteBytes(
			0, data, write.Offset, userData, linked,
		)
	}
	return d.ring.PrepareWriteFixed(
		0, int(write.Buffer), int(write.Length), write.Offset,
		userData, linked,
	)
}

func (d *ringDevice) CommitMaterialized(
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
	journalPhase := [1]Write{journal}
	if err := d.materializationPhase(journalPhase[:]); err != nil {
		return materializationCommitResult{}, err
	}
	result := materializationCommitResult{
		CompletedPhases: 1, CompletedBarriers: 1,
	}
	if mode == materializationPatchOnly {
		rootAttempted, barrierCompleted, err :=
			d.materializationCombinedRootPhase(targets, root)
		result.RootAttempted = rootAttempted
		if rootAttempted {
			result.CompletedPhases = 2
		}
		if barrierCompleted {
			result.CompletedPhases = 3
			result.CompletedBarriers = 2
		}
		return result, err
	}
	if err := d.materializationPhase(targets); err != nil {
		return result, err
	}
	result.CompletedPhases = 2
	result.CompletedBarriers = 2
	rootPhase := [1]Write{root}
	result.RootAttempted = true
	if err := d.materializationPhase(rootPhase[:]); err != nil {
		return result, err
	}
	result.CompletedPhases = 3
	result.CompletedBarriers = 3
	return result, nil
}

func (d *ringDevice) materializationCombinedRootPhase(
	writes []Write,
	root Write,
) (rootAttempted, barrierCompleted bool, err error) {
	if len(writes) == 0 {
		return false, false, ErrInvalidWrite
	}
	for rank, write := range writes {
		if err := d.prepareWrite(write, uint64(rank), false); err != nil {
			return false, false, err
		}
	}
	if err := d.ring.PrepareWriteFixed(
		0, int(root.Buffer), int(root.Length), root.Offset,
		ringRootTag, false,
	); err != nil {
		return false, false, err
	}
	rootAttempted = true
	want := uint32(len(writes) + 1)
	if err := d.ring.SubmitAndWait(want); err != nil {
		return true, false, err
	}
	completions, err := d.ring.PopBatch(want)
	if err != nil {
		return true, false, err
	}
	if len(completions) != int(want) {
		return true, false, ErrOverflow
	}
	var first error
	for _, completion := range completions {
		var expected uint32
		switch {
		case completion.UserData == ringRootTag:
			expected = root.Length
		case completion.UserData < uint64(len(writes)):
			expected = writes[completion.UserData].Length
		default:
			return true, false, ErrOverflow
		}
		if err := completionResult(
			completion, expected,
		); first == nil && err != nil {
			first = err
		}
	}
	if first != nil {
		return true, false, first
	}
	if err := d.ring.PrepareDataSync(
		0, ringDataSyncTag, false,
	); err != nil {
		return true, false, err
	}
	if err := d.ring.SubmitAndWait(1); err != nil {
		return true, false, err
	}
	completions, err = d.ring.PopBatch(1)
	if err != nil {
		return true, false, err
	}
	if len(completions) != 1 || completions[0].UserData != ringDataSyncTag {
		return true, false, ErrOverflow
	}
	if err := completionResult(completions[0], 0); err != nil {
		return true, false, err
	}
	return true, true, nil
}

func (d *ringDevice) materializationPhase(writes []Write) error {
	if len(writes) == 0 {
		return ErrInvalidWrite
	}
	for rank, write := range writes {
		if err := d.prepareWrite(write, uint64(rank), false); err != nil {
			return err
		}
	}
	if err := d.ring.SubmitAndWait(uint32(len(writes))); err != nil {
		return err
	}
	completions, err := d.ring.PopBatch(uint32(len(writes)))
	if err != nil {
		return err
	}
	if len(completions) != len(writes) {
		return ErrOverflow
	}
	var first error
	for _, completion := range completions {
		if completion.UserData >= uint64(len(writes)) {
			return ErrOverflow
		}
		write := writes[completion.UserData]
		if err := completionResult(
			completion, write.Length,
		); first == nil && err != nil {
			first = err
		}
	}
	if first != nil {
		return first
	}
	if err := d.ring.PrepareDataSync(
		0, ringDataSyncTag, false,
	); err != nil {
		return err
	}
	if err := d.ring.SubmitAndWait(1); err != nil {
		return err
	}
	completions, err = d.ring.PopBatch(1)
	if err != nil {
		return err
	}
	if len(completions) != 1 || completions[0].UserData != ringDataSyncTag {
		return ErrOverflow
	}
	return completionResult(completions[0], 0)
}

func completionResult(completion Completion, expected uint32) error {
	if err := completion.Err(); err != nil {
		return err
	}
	if uint32(completion.Result) != expected {
		return io.ErrShortWrite
	}
	return nil
}

func (d *ringDevice) Close() error {
	if d == nil || d.closed {
		return nil
	}
	d.closed = true
	d.seen = nil
	d.frameArena = nil
	return d.ring.Close()
}
