package storeio

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

const (
	defaultBufferCount  = 64
	defaultBufferSize   = 64 << 10
	maxDeviceQueueDepth = 32768
	maxDeviceBuffers    = maxDeviceQueueDepth
)

var (
	// ErrInvalidWrite reports a buffer index, byte length, file offset, order,
	// or batch size outside the Device contract.
	ErrInvalidWrite = errors.New("vibejson: invalid Store page write")
	// ErrOverlappingWrite reports physical ranges that could overwrite one
	// another or the root descriptor in a single commit.
	ErrOverlappingWrite = errors.New("vibejson: overlapping Store page writes")
	// ErrDuplicateBuffer reports one staging buffer submitted by two concurrent
	// data-page writes. The root may reuse a data buffer because the data phase
	// completes before the root phase begins.
	ErrDuplicateBuffer = errors.New("vibejson: duplicate Store page buffer")
	// ErrCommitOutcomeUnknown reports a persistence failure after the alternate
	// root write may have reached the storage stack. The checksummed double root
	// makes the file recoverable, but only recovery can determine whether the
	// old or new generation won. A live writer must therefore stop and reopen
	// instead of retrying a mutation whose outcome it cannot know.
	ErrCommitOutcomeUnknown = errors.New("vibejson: Store commit outcome is unknown; reopen required")
)

func commitOutcomeUnknown(err error) error {
	if err == nil || errors.Is(err, ErrCommitOutcomeUnknown) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrCommitOutcomeUnknown, err)
}

// Backend selects the internal Store page-I/O implementation.
type Backend uint8

const (
	// BackendAuto tries the native asynchronous backend and falls back to the
	// portable correctness implementation when the OS or sandbox rejects it.
	BackendAuto Backend = iota
	// BackendPortable uses positional file writes and the platform's data-
	// integrity barrier (fdatasync on Linux, File.Sync elsewhere).
	BackendPortable
	// BackendIOUring requires the pure-Go Linux io_uring backend.
	BackendIOUring
)

func (b Backend) String() string {
	switch b {
	case BackendAuto:
		return "auto"
	case BackendPortable:
		return "portable"
	case BackendIOUring:
		return "io_uring"
	default:
		return "unknown"
	}
}

// DeviceOptions fixes the bounded staging and submission storage of a Device.
// Zero selects 64 buffers of 64 KiB each and an implementation-sized queue.
type DeviceOptions struct {
	Backend Backend
	// CheckpointSync selects the portable Device's two copy-on-write commit
	// barriers. The zero value uses the platform's power-safe primitive.
	// CheckpointSyncFilesystem uses ordinary filesystem sync and is intended
	// only for an explicitly weaker buffered-visible checkpoint contract.
	CheckpointSync CheckpointSync
	// BufferCount is the number of reusable page staging buffers for a Device
	// opened directly. NewCommitter accepts and normalizes the same value for
	// descriptor bounds, but caps its private Device arena:
	// WriteTransaction data pages are cache-frame-native.
	BufferCount int
	// BufferSize is the equal byte size of every staging buffer. It must be a
	// multiple of the host page size so native fixed-buffer I/O stays aligned.
	BufferSize int
	// QueueDepth is the requested native submission depth. Zero selects at
	// least BufferCount. The native backend may round it to a power of two.
	QueueDepth int
	// SingleIssuer enables native same-thread optimizations. The owner must
	// lock its goroutine to one OS thread before OpenDevice and keep it locked
	// through every operation and Close. Store's background writer does this.
	SingleIssuer bool
}

func (o DeviceOptions) normalized() (DeviceOptions, error) {
	if o.Backend > BackendIOUring {
		return DeviceOptions{}, fmt.Errorf("%w: unknown backend %d", ErrInvalidWrite, o.Backend)
	}
	if o.CheckpointSync > CheckpointSyncFilesystem ||
		o.CheckpointSync == CheckpointSyncFilesystem &&
			o.Backend != BackendPortable {
		return DeviceOptions{}, fmt.Errorf(
			"%w: checkpoint sync %d requires portable backend",
			ErrInvalidWrite, o.CheckpointSync,
		)
	}
	if o.BufferCount == 0 {
		o.BufferCount = defaultBufferCount
	}
	if o.BufferSize == 0 {
		o.BufferSize = defaultBufferSize
	}
	pageSize := os.Getpagesize()
	if o.BufferCount < 1 || o.BufferCount > maxDeviceBuffers || o.BufferSize < pageSize ||
		o.BufferSize%pageSize != 0 || uint64(o.BufferSize) > uint64(^uint32(0)) {
		return DeviceOptions{}, fmt.Errorf("%w: buffers=%d size=%d", ErrInvalidWrite, o.BufferCount, o.BufferSize)
	}
	if o.QueueDepth == 0 {
		o.QueueDepth = o.BufferCount
	}
	if o.QueueDepth < o.BufferCount || o.QueueDepth > maxDeviceQueueDepth {
		return DeviceOptions{}, fmt.Errorf("%w: queue depth %d for %d buffers", ErrInvalidWrite, o.QueueDepth, o.BufferCount)
	}
	return o, nil
}

// CheckpointSync selects the portable copy-on-write commit barriers.
type CheckpointSync uint8

const (
	// CheckpointSyncPowerSafe preserves the existing strongest platform
	// durability boundary, including F_FULLFSYNC on Darwin.
	CheckpointSyncPowerSafe CheckpointSync = iota
	// CheckpointSyncFilesystem uses the platform's ordinary filesystem-strength
	// ordering and final persistence (fdatasync on Linux, fsync on Darwin).
	// On Darwin that survives process failure but is not a promise that volatile
	// drive caches survive sudden power loss.
	CheckpointSyncFilesystem
)

// Write names one initialized prefix of a Device staging buffer and its
// physical file offset. Data-page writes in a commit must be ordered by Offset,
// non-overlapping, and use distinct buffers.
type Write struct {
	Offset     int64
	Length     uint32
	frameIndex uint32
	Buffer     uint16
	kind       PageKind
	// pendingFlags belongs to the manual-checkpoint committer. Device never
	// receives superseded writes. The flags also distinguish a compact
	// cache-frame source and record opportunistic pre-write completion.
	pendingFlags      uint8
	retiredGeneration uint64
}

func (w Write) frameNative() bool {
	return w.pendingFlags&pendingWriteFrameNative != 0
}

// Device is the internal, single-owner durable page-I/O boundary. Buffer
// storage is fixed at construction and is reused for every commit. Commit is
// synchronous at this layer: it returns only after data pages, a data barrier,
// the alternate root, and its final barrier have completed in that order.
// Store's background writer supplies asynchronous application semantics above
// this boundary.
//
// Device does not own the file passed to OpenDevice. The file and Device must
// remain open until all operations finish.
type Device interface {
	Backend() Backend
	Buffer(index int) ([]byte, error)
	Commit(pages []Write, root Write) error
	Close() error
}

type frameArenaDevice interface {
	bindFrameArena([]byte, int) error
}

// prewriteDevice writes data pages without a durability barrier or root
// publication. The committer uses it only as an optional manual-checkpoint
// optimization: recovery continues to select state exclusively through the
// last checksummed alternate root.
type prewriteDevice interface {
	Prewrite([]Write) error
}

// OpenDevice constructs a bounded page-I/O backend over file.
func OpenDevice(file *os.File, options DeviceOptions) (Device, error) {
	if file == nil {
		return nil, fmt.Errorf("%w: nil file", ErrInvalidWrite)
	}
	normalized, err := options.normalized()
	if err != nil {
		return nil, err
	}
	if normalized.Backend != BackendPortable {
		device, ringErr := openRingDevice(file, normalized)
		if ringErr == nil {
			return device, nil
		}
		if normalized.Backend == BackendIOUring || !ringFallbackError(ringErr) {
			return nil, ringErr
		}
	}
	return openPortableDevice(file, normalized)
}

func ringFallbackError(err error) bool {
	// BackendAuto is a preference, not a resource requirement. Linux may
	// support io_uring while refusing another ring because the process or
	// runner exhausted ring memory, descriptors, or a transient kernel
	// admission limit. Portable positional I/O needs no ring and can still
	// serve the already-open file, so these setup failures are valid fallback
	// signals. BackendIOUring remains fail-closed because OpenDevice checks the
	// explicit backend before consulting this predicate.
	return errors.Is(err, ErrUnavailable) ||
		errors.Is(err, ErrUnsupported) ||
		errors.Is(err, syscall.ENOMEM) ||
		errors.Is(err, syscall.EAGAIN) ||
		errors.Is(err, syscall.EMFILE) ||
		errors.Is(err, syscall.ENFILE)
}

func validateCommit(bufferCount, bufferSize int, seen []uint64, pages []Write, root Write) error {
	clear(seen)
	var previousEnd int64
	for i, write := range pages {
		end, err := validateWrite(bufferCount, bufferSize, write)
		if err != nil {
			return err
		}
		if !write.frameNative() {
			word, bit := int(write.Buffer)>>6, uint(write.Buffer)&63
			mask := uint64(1) << bit
			if seen[word]&mask != 0 {
				return ErrDuplicateBuffer
			}
			seen[word] |= mask
		}
		if i != 0 && write.Offset < previousEnd {
			return ErrOverlappingWrite
		}
		previousEnd = end
	}
	rootEnd, err := validateWrite(bufferCount, bufferSize, root)
	if err != nil {
		return err
	}
	for _, write := range pages {
		writeEnd := write.Offset + int64(write.Length)
		if write.Offset < rootEnd && root.Offset < writeEnd {
			return ErrOverlappingWrite
		}
	}
	return nil
}

func validateWrite(bufferCount, bufferSize int, write Write) (int64, error) {
	if write.Length == 0 ||
		write.Offset < 0 || int64(write.Length) > int64(^uint64(0)>>1)-write.Offset {
		return 0, ErrInvalidWrite
	}
	if write.frameNative() {
		if uint64(write.Length) > uint64(bufferSize) {
			return 0, ErrInvalidWrite
		}
	} else if int(write.Buffer) >= bufferCount ||
		uint64(write.Length) > uint64(bufferSize) {
		return 0, ErrInvalidWrite
	}
	return write.Offset + int64(write.Length), nil
}
