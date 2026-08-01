//go:build linux && (amd64 || arm64 || riscv64 || loong64)

package storeio

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// These layouts and constants are copied from Linux's stable UAPI
// include/uapi/linux/io_uring.h. Compile-time assertions below make an ABI
// drift a build failure rather than memory corruption.
const (
	sysIOUringSetup    = 425
	sysIOUringEnter    = 426
	sysIOUringRegister = 427

	ioUringOffSQRing = 0
	ioUringOffCQRing = 0x08000000
	ioUringOffSQEs   = 0x10000000

	ioUringSetupCQSize        = 1 << 3
	ioUringSetupSubmitAll     = 1 << 7
	ioUringSetupCoopTaskRun   = 1 << 8
	ioUringSetupSingleIssuer  = 1 << 12
	ioUringFeatSingleMmap     = 1 << 0
	ioUringFeatNoDrop         = 1 << 1
	ioUringEnterGetEvents     = 1 << 0
	ioUringRegisterBuffers    = 0
	ioUringUnregisterBuffers  = 1
	ioUringRegisterFiles      = 2
	ioUringUnregisterFiles    = 3
	ioUringRegisterProbe      = 8
	ioUringOpSupported        = 1 << 0
	ioUringOpWriteV           = 2
	ioUringOpFsync            = 3
	ioUringOpReadFixed        = 4
	ioUringOpWriteFixed       = 5
	ioUringOpRead             = 22
	ioUringOpWrite            = 23
	ioUringFsyncDataSync      = 1 << 0
	rwfDataSync               = 1 << 1
	ioSQEFixedFile            = 1 << 0
	ioSQEIOLink               = 1 << 2
	ioUringDefaultEntries     = 256
	ioUringMaxEntries         = 32768
	ioUringProbeOperations    = 256
	ioUringProbeHeaderSize    = 16
	ioUringProbeOperationSize = 8
	ioVectorMax               = 1024
)

type ioUringSQRingOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Flags       uint32
	Dropped     uint32
	Array       uint32
	Reserved    uint32
	UserAddress uint64
}

type ioUringCQRingOffsets struct {
	Head        uint32
	Tail        uint32
	RingMask    uint32
	RingEntries uint32
	Overflow    uint32
	CQEs        uint32
	Flags       uint32
	Reserved    uint32
	UserAddress uint64
}

type ioUringParams struct {
	SQEntries    uint32
	CQEntries    uint32
	Flags        uint32
	SQThreadCPU  uint32
	SQThreadIdle uint32
	Features     uint32
	WorkQueueFD  uint32
	Reserved     [3]uint32
	SQOffsets    ioUringSQRingOffsets
	CQOffsets    ioUringCQRingOffsets
}

type ioUringSQE struct {
	Opcode      uint8
	Flags       uint8
	IOPriority  uint16
	FD          int32
	Offset      uint64
	Address     uint64
	Length      uint32
	Operation   uint32
	UserData    uint64
	BufferIndex uint16
	Personality uint16
	FileIndex   int32
	Address3    uint64
	Pad         uint64
}

type ioUringCQE struct {
	UserData uint64
	Result   int32
	Flags    uint32
}

type ioVector struct {
	Base uintptr
	Len  uintptr
}

const (
	ioUringSQRingOffsetsSize  = int(unsafe.Sizeof(ioUringSQRingOffsets{}))
	ioUringCQRingOffsetsSize  = int(unsafe.Sizeof(ioUringCQRingOffsets{}))
	ioUringParamsSize         = int(unsafe.Sizeof(ioUringParams{}))
	ioUringSQESize            = int(unsafe.Sizeof(ioUringSQE{}))
	ioUringCQESize            = int(unsafe.Sizeof(ioUringCQE{}))
	ioUringParamsSQOffset     = int(unsafe.Offsetof(ioUringParams{}.SQOffsets))
	ioUringParamsCQOffset     = int(unsafe.Offsetof(ioUringParams{}.CQOffsets))
	ioUringSQEOffsetOffset    = int(unsafe.Offsetof(ioUringSQE{}.Offset))
	ioUringSQEAddressOffset   = int(unsafe.Offsetof(ioUringSQE{}.Address))
	ioUringSQELengthOffset    = int(unsafe.Offsetof(ioUringSQE{}.Length))
	ioUringSQEOperationOffset = int(unsafe.Offsetof(ioUringSQE{}.Operation))
	ioUringSQEUserDataOffset  = int(unsafe.Offsetof(ioUringSQE{}.UserData))
	ioUringSQEBufferOffset    = int(unsafe.Offsetof(ioUringSQE{}.BufferIndex))
	ioUringSQEFileOffset      = int(unsafe.Offsetof(ioUringSQE{}.FileIndex))
	ioUringSQEAddress3Offset  = int(unsafe.Offsetof(ioUringSQE{}.Address3))
	ioUringCQEResultOffset    = int(unsafe.Offsetof(ioUringCQE{}.Result))
)

var (
	_ [40 - ioUringSQRingOffsetsSize]byte
	_ [ioUringSQRingOffsetsSize - 40]byte
	_ [40 - ioUringCQRingOffsetsSize]byte
	_ [ioUringCQRingOffsetsSize - 40]byte
	_ [120 - ioUringParamsSize]byte
	_ [ioUringParamsSize - 120]byte
	_ [64 - ioUringSQESize]byte
	_ [ioUringSQESize - 64]byte
	_ [16 - ioUringCQESize]byte
	_ [ioUringCQESize - 16]byte
	_ [40 - ioUringParamsSQOffset]byte
	_ [ioUringParamsSQOffset - 40]byte
	_ [80 - ioUringParamsCQOffset]byte
	_ [ioUringParamsCQOffset - 80]byte
	_ [8 - ioUringSQEOffsetOffset]byte
	_ [ioUringSQEOffsetOffset - 8]byte
	_ [16 - ioUringSQEAddressOffset]byte
	_ [ioUringSQEAddressOffset - 16]byte
	_ [24 - ioUringSQELengthOffset]byte
	_ [ioUringSQELengthOffset - 24]byte
	_ [28 - ioUringSQEOperationOffset]byte
	_ [ioUringSQEOperationOffset - 28]byte
	_ [32 - ioUringSQEUserDataOffset]byte
	_ [ioUringSQEUserDataOffset - 32]byte
	_ [40 - ioUringSQEBufferOffset]byte
	_ [ioUringSQEBufferOffset - 40]byte
	_ [44 - ioUringSQEFileOffset]byte
	_ [ioUringSQEFileOffset - 44]byte
	_ [48 - ioUringSQEAddress3Offset]byte
	_ [ioUringSQEAddress3Offset - 48]byte
	_ [8 - ioUringCQEResultOffset]byte
	_ [ioUringCQEResultOffset - 8]byte
)

type requestSlot struct {
	token       uint64
	userData    uint64
	buffer      int32
	vectorCount uint32
	outstanding bool
}

// Ring is a bounded single-owner submission/completion ring. It contains no
// callbacks and starts no goroutines. A Store I/O worker owns one Ring and all
// its staging buffers, which keeps Go pointers out of kernel-retained state.
type Ring struct {
	fd       int
	features Features
	closed   bool

	sqRing  []byte
	cqRing  []byte
	sqesMap []byte
	sqes    []ioUringSQE
	sqArray []uint32
	cqes    []ioUringCQE

	sqHead       *uint32
	sqTail       *uint32
	sqMask       *uint32
	sqEntries    *uint32
	sqDropped    *uint32
	cqHead       *uint32
	cqTail       *uint32
	cqMask       *uint32
	cqEntries    *uint32
	cqOverflow   *uint32
	droppedBase  uint32
	overflowBase uint32

	requests     []requestSlot
	freeRequests []uint32
	freeCount    uint32
	nextToken    uint64
	requestCount uint64
	requestMask  uint64
	requestPow2  bool
	maxToken     uint64
	pending      uint32
	outstanding  uint32
	sqTailValue  uint32
	popScratch   []Completion

	files                    int
	bufferMap                []byte
	bufferSize               int
	buffers                  int
	bufferBusy               []bool
	writeVectorMap           []byte
	writeVectors             []ioVector
	writeVectorBuffers       []int32
	writeVectorBusy          bool
	writeVectorSupported     bool
	readArena                []byte
	readArenaFixed           bool
	readArenaBuffer          uint16
	readBuffersRegistered    bool
	frameArena               []byte
	frameBuffer              int
	bufferRegistrationBroken bool
}

// Open constructs and maps a ring, then probes the exact operations needed by
// the Store page engine. ErrUnavailable and ErrUnsupported are signals to use
// portable positional I/O, not fatal process errors.
func Open(config Config) (*Ring, error) {
	entries, err := normalizeEntries(config.Entries)
	if err != nil {
		return nil, err
	}
	params, fd, single, err := setupRing(entries, config.SingleIssuer)
	if err != nil {
		if errors.Is(err, syscall.ENOSYS) || errors.Is(err, syscall.EPERM) ||
			errors.Is(err, syscall.EACCES) {
			return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		return nil, err
	}
	r := &Ring{fd: fd, features: Features{
		SingleIssuer: single,
		SingleMmap:   params.Features&ioUringFeatSingleMmap != 0,
		NoDrop:       params.Features&ioUringFeatNoDrop != 0,
	}}
	if err := r.mapQueues(&params); err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}
	r.requests = make([]requestSlot, params.SQEntries)
	r.freeRequests = make([]uint32, params.SQEntries)
	r.requestCount = uint64(len(r.requests))
	r.requestPow2 = r.requestCount&(r.requestCount-1) == 0
	if r.requestPow2 {
		r.requestMask = r.requestCount - 1
	}
	r.maxToken = (uint64(math.MaxUint64) - (r.requestCount - 1)) / r.requestCount
	r.sqTailValue = atomic.LoadUint32(r.sqTail)
	r.popScratch = make([]Completion, params.SQEntries)
	for i := range r.freeRequests {
		r.freeRequests[i] = uint32(len(r.freeRequests) - 1 - i)
	}
	r.freeCount = params.SQEntries
	if err := r.requireOperations(); err != nil {
		_ = r.Close()
		return nil, err
	}
	return r, nil
}

func normalizeEntries(entries uint32) (uint32, error) {
	if entries == 0 {
		entries = ioUringDefaultEntries
	}
	if entries > ioUringMaxEntries {
		return 0, fmt.Errorf("vibedb: io_uring entries %d exceed %d", entries, ioUringMaxEntries)
	}
	entries--
	entries |= entries >> 1
	entries |= entries >> 2
	entries |= entries >> 4
	entries |= entries >> 8
	entries |= entries >> 16
	return entries + 1, nil
}

func setupRing(entries uint32, singleIssuer bool) (ioUringParams, int, bool, error) {
	base := uint32(ioUringSetupCQSize | ioUringSetupSubmitAll | ioUringSetupCoopTaskRun)
	flags := []uint32{base}
	if singleIssuer {
		// Keep cooperative task execution, but do not defer it. This owner waits
		// synchronously for each durability chain; DEFER_TASKRUN adds a second
		// enter/wakeup handoff to the critical path instead of creating useful
		// overlap. SINGLE_ISSUER retains the same-thread ring optimization.
		flags = []uint32{
			base | ioUringSetupSingleIssuer,
			base,
		}
	}
	flags = append(flags, ioUringSetupCQSize, 0)
	var last error
	for _, setupFlags := range flags {
		params := ioUringParams{Flags: setupFlags}
		if setupFlags&ioUringSetupCQSize != 0 {
			params.CQEntries = entries * 2
		}
		fd, err := ioUringSetup(entries, &params)
		if err == nil {
			return params, fd, setupFlags&ioUringSetupSingleIssuer != 0, nil
		}
		last = err
		if !errors.Is(err, syscall.EINVAL) {
			break
		}
	}
	return ioUringParams{}, -1, false, fmt.Errorf("io_uring_setup: %w", last)
}

func (r *Ring) mapQueues(params *ioUringParams) error {
	sqBytes, ok := mappedSize(params.SQOffsets.Array, params.SQEntries, 4)
	if !ok {
		return fmt.Errorf("%w: invalid SQ mapping", ErrOverflow)
	}
	cqBytes, ok := mappedSize(params.CQOffsets.CQEs, params.CQEntries, uint64(unsafe.Sizeof(ioUringCQE{})))
	if !ok {
		return fmt.Errorf("%w: invalid CQ mapping", ErrOverflow)
	}
	var err error
	if r.features.SingleMmap {
		ringBytes := max(sqBytes, cqBytes)
		r.sqRing, err = syscall.Mmap(r.fd, ioUringOffSQRing, ringBytes,
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		r.cqRing = r.sqRing
	} else {
		r.sqRing, err = syscall.Mmap(r.fd, ioUringOffSQRing, sqBytes,
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		if err == nil {
			r.cqRing, err = syscall.Mmap(r.fd, ioUringOffCQRing, cqBytes,
				syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
		}
	}
	if err != nil {
		r.unmapQueues()
		return fmt.Errorf("io_uring mmap queues: %w", err)
	}
	sqeBytes64 := uint64(params.SQEntries) * uint64(unsafe.Sizeof(ioUringSQE{}))
	if sqeBytes64 > uint64(maxInt()) {
		r.unmapQueues()
		return fmt.Errorf("%w: invalid SQE mapping", ErrOverflow)
	}
	r.sqesMap, err = syscall.Mmap(r.fd, ioUringOffSQEs, int(sqeBytes64),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		r.unmapQueues()
		return fmt.Errorf("io_uring mmap SQEs: %w", err)
	}

	if !validU32Offset(r.sqRing, params.SQOffsets.Head) ||
		!validU32Offset(r.sqRing, params.SQOffsets.Tail) ||
		!validU32Offset(r.sqRing, params.SQOffsets.RingMask) ||
		!validU32Offset(r.sqRing, params.SQOffsets.RingEntries) ||
		!validU32Offset(r.sqRing, params.SQOffsets.Dropped) ||
		!validSlice(r.sqRing, params.SQOffsets.Array, params.SQEntries, 4) ||
		!validU32Offset(r.cqRing, params.CQOffsets.Head) ||
		!validU32Offset(r.cqRing, params.CQOffsets.Tail) ||
		!validU32Offset(r.cqRing, params.CQOffsets.RingMask) ||
		!validU32Offset(r.cqRing, params.CQOffsets.RingEntries) ||
		!validU32Offset(r.cqRing, params.CQOffsets.Overflow) ||
		params.CQOffsets.CQEs&7 != 0 ||
		!validSlice(r.cqRing, params.CQOffsets.CQEs, params.CQEntries, 16) {
		r.unmapQueues()
		return fmt.Errorf("%w: kernel returned invalid ring offsets", ErrOverflow)
	}

	r.sqHead = u32At(r.sqRing, params.SQOffsets.Head)
	r.sqTail = u32At(r.sqRing, params.SQOffsets.Tail)
	r.sqMask = u32At(r.sqRing, params.SQOffsets.RingMask)
	r.sqEntries = u32At(r.sqRing, params.SQOffsets.RingEntries)
	r.sqDropped = u32At(r.sqRing, params.SQOffsets.Dropped)
	r.cqHead = u32At(r.cqRing, params.CQOffsets.Head)
	r.cqTail = u32At(r.cqRing, params.CQOffsets.Tail)
	r.cqMask = u32At(r.cqRing, params.CQOffsets.RingMask)
	r.cqEntries = u32At(r.cqRing, params.CQOffsets.RingEntries)
	r.cqOverflow = u32At(r.cqRing, params.CQOffsets.Overflow)
	if *r.sqEntries != params.SQEntries || *r.cqEntries != params.CQEntries ||
		*r.sqMask+1 != *r.sqEntries || *r.cqMask+1 != *r.cqEntries {
		r.unmapQueues()
		return fmt.Errorf("%w: inconsistent ring dimensions", ErrOverflow)
	}
	r.sqArray = unsafe.Slice((*uint32)(unsafe.Pointer(&r.sqRing[params.SQOffsets.Array])), params.SQEntries)
	r.sqes = unsafe.Slice((*ioUringSQE)(unsafe.Pointer(&r.sqesMap[0])), params.SQEntries)
	r.cqes = unsafe.Slice((*ioUringCQE)(unsafe.Pointer(&r.cqRing[params.CQOffsets.CQEs])), params.CQEntries)
	for index := range r.sqArray {
		r.sqArray[index] = uint32(index)
	}
	r.droppedBase = atomic.LoadUint32(r.sqDropped)
	r.overflowBase = atomic.LoadUint32(r.cqOverflow)
	return nil
}

func mappedSize(offset, count uint32, width uint64) (int, bool) {
	size := uint64(offset) + uint64(count)*width
	if size == 0 || size > uint64(maxInt()) {
		return 0, false
	}
	return int(size), true
}

func maxInt() int { return int(^uint(0) >> 1) }

func validU32Offset(mapping []byte, offset uint32) bool {
	return offset&3 == 0 && uint64(offset)+4 <= uint64(len(mapping))
}

func validSlice(mapping []byte, offset, count uint32, width uint64) bool {
	return uint64(offset)+uint64(count)*width <= uint64(len(mapping))
}

func u32At(mapping []byte, offset uint32) *uint32 {
	return (*uint32)(unsafe.Pointer(&mapping[offset]))
}

func (r *Ring) requireOperations() error {
	probe := make([]byte, ioUringProbeHeaderSize+ioUringProbeOperations*ioUringProbeOperationSize)
	if err := ioUringRegister(r.fd, ioUringRegisterProbe, unsafe.Pointer(&probe[0]), ioUringProbeOperations); err != nil {
		return fmt.Errorf("%w: opcode probe: %v", ErrUnsupported, err)
	}
	operations := int(probe[1])
	if operations > ioUringProbeOperations {
		return fmt.Errorf("%w: invalid opcode probe", ErrOverflow)
	}
	var supported [ioUringProbeOperations]bool
	for i := 0; i < operations; i++ {
		off := ioUringProbeHeaderSize + i*ioUringProbeOperationSize
		op := probe[off]
		flags := uint16(probe[off+2]) | uint16(probe[off+3])<<8
		if flags&ioUringOpSupported != 0 {
			supported[op] = true
		}
	}
	runtime.KeepAlive(probe)
	if !supported[ioUringOpReadFixed] ||
		!supported[ioUringOpWriteFixed] ||
		!supported[ioUringOpWrite] ||
		!supported[ioUringOpFsync] {
		return ErrUnsupported
	}
	r.features.AsyncRead = supported[ioUringOpRead]
	r.writeVectorSupported = supported[ioUringOpWriteV]
	return nil
}

// Features returns the setup features accepted by the running kernel.
func (r *Ring) Features() Features { return r.features }

func (r *Ring) vectorWriteCapacity() int {
	if !r.writeVectorSupported {
		return 0
	}
	return len(r.writeVectors)
}

// RegisterFiles pins file descriptions into the ring. Closing an original fd
// after successful registration is legal; the ring owns its kernel reference.
func (r *Ring) RegisterFiles(fds []int) error {
	if r.closed {
		return ErrClosed
	}
	if r.files != 0 || len(fds) == 0 || len(fds) > math.MaxInt32 {
		return syscall.EINVAL
	}
	registered := make([]int32, len(fds))
	for i, fd := range fds {
		if fd < 0 || uint64(fd) > math.MaxInt32 {
			return syscall.EBADF
		}
		registered[i] = int32(fd)
	}
	if err := ioUringRegister(r.fd, ioUringRegisterFiles, unsafe.Pointer(&registered[0]), uint32(len(registered))); err != nil {
		return fmt.Errorf("io_uring register files: %w", err)
	}
	runtime.KeepAlive(registered)
	r.files = len(registered)
	return nil
}

// RegisterBuffers allocates page-aligned anonymous memory outside the Go heap
// and registers each equal-sized region as a fixed buffer. size must be a
// multiple of the host page size so every buffer is independently aligned.
func (r *Ring) RegisterBuffers(count, size int) error {
	if r.closed {
		return ErrClosed
	}
	if r.buffers != 0 || r.readBuffersRegistered || len(r.readArena) != 0 || count <= 0 || count > math.MaxUint16+1 || size <= 0 ||
		uint64(size) > math.MaxUint32 || size%syscall.Getpagesize() != 0 {
		return syscall.EINVAL
	}
	if count > maxInt()/size {
		return syscall.EOVERFLOW
	}
	mapping, err := allocateArena(count * size)
	if err != nil {
		return fmt.Errorf("io_uring allocate fixed buffers: %w", err)
	}
	vectors := make([]ioVector, count)
	for i := range vectors {
		vectors[i] = ioVector{Base: uintptr(unsafe.Pointer(&mapping[i*size])), Len: uintptr(size)}
	}
	if err := ioUringRegister(r.fd, ioUringRegisterBuffers, unsafe.Pointer(&vectors[0]), uint32(len(vectors))); err != nil {
		_ = releaseArena(mapping)
		return fmt.Errorf("io_uring register buffers: %w", err)
	}
	runtime.KeepAlive(vectors)
	r.bufferMap = mapping
	r.bufferSize = size
	r.buffers = count
	r.bufferBusy = make([]bool, count)
	if r.writeVectorSupported {
		vectorCount := min(count, ioVectorMax)
		vectorBytes := vectorCount * int(unsafe.Sizeof(ioVector{}))
		vectorMap, vectorErr := allocateArena(vectorBytes)
		if vectorErr == nil {
			r.writeVectorMap = vectorMap
			r.writeVectors = unsafe.Slice(
				(*ioVector)(unsafe.Pointer(&vectorMap[0])), vectorCount,
			)
			r.writeVectorBuffers = make([]int32, vectorCount)
		}
	}
	return nil
}

// registerFrameArena extends the fixed-buffer table with one PageCache arena.
// If the kernel refuses the larger pinned region, the original committer
// buffer table is restored and callers may use non-fixed writes.
func (r *Ring) registerFrameArena(arena []byte) error {
	if r.closed {
		return ErrClosed
	}
	if len(r.frameArena) != 0 || r.buffers == 0 ||
		r.pending != 0 || r.outstanding != 0 || len(arena) == 0 ||
		uintptr(unsafe.Pointer(&arena[0]))%uintptr(syscall.Getpagesize()) != 0 ||
		len(arena)%syscall.Getpagesize() != 0 ||
		r.buffers+1 > math.MaxUint16+1 {
		return syscall.EINVAL
	}
	vectors := make([]ioVector, r.buffers+1)
	for index := range r.buffers {
		vectors[index] = ioVector{
			Base: uintptr(unsafe.Pointer(
				&r.bufferMap[index*r.bufferSize],
			)),
			Len: uintptr(r.bufferSize),
		}
	}
	vectors[r.buffers] = ioVector{
		Base: uintptr(unsafe.Pointer(&arena[0])),
		Len:  uintptr(len(arena)),
	}
	if err := ioUringRegister(
		r.fd, ioUringUnregisterBuffers, nil, 0,
	); err != nil {
		return err
	}
	if err := ioUringRegister(
		r.fd, ioUringRegisterBuffers,
		unsafe.Pointer(&vectors[0]), uint32(len(vectors)),
	); err != nil {
		restore := vectors[:r.buffers]
		if restoreErr := ioUringRegister(
			r.fd, ioUringRegisterBuffers,
			unsafe.Pointer(&restore[0]), uint32(len(restore)),
		); restoreErr != nil {
			r.bufferRegistrationBroken = true
			return errors.Join(err, restoreErr)
		}
		return err
	}
	runtime.KeepAlive(vectors)
	r.frameArena = arena
	r.frameBuffer = r.buffers
	return nil
}

// Buffer returns one registered staging region. The owner may use it until a
// successful PrepareWriteFixed; it must not retain or touch the slice again
// until the corresponding completion is consumed by Pop.
func (r *Ring) Buffer(index int) ([]byte, error) {
	if r.closed {
		return nil, ErrClosed
	}
	if index < 0 || index >= r.buffers {
		return nil, syscall.EINVAL
	}
	if r.bufferBusy[index] {
		return nil, ErrBufferBusy
	}
	start := index * r.bufferSize
	return r.bufferMap[start : start+r.bufferSize], nil
}

// useReadArena binds the PageCache's stable mmap memory for asynchronous reads.
// It opportunistically registers that arena for READ_FIXED and falls back to a
// stable-address READ when registration is refused. The ring borrows the arena
// until Close; it never unmaps it. Keeping this package-private prevents
// arbitrary Go-heap slices from reaching an asynchronous kernel request.
func (r *Ring) useReadArena(arena []byte) error {
	if r.closed {
		return ErrClosed
	}
	if len(r.readArena) != 0 || len(arena) == 0 ||
		uintptr(unsafe.Pointer(&arena[0]))%uintptr(syscall.Getpagesize()) != 0 {
		return syscall.EINVAL
	}
	r.readArena = arena
	if r.buffers == 0 {
		var vector ioVector
		vector.Base = uintptr(unsafe.Pointer(&arena[0]))
		vector.Len = uintptr(len(arena))
		if err := ioUringRegister(
			r.fd, ioUringRegisterBuffers, unsafe.Pointer(&vector), 1,
		); err == nil {
			r.readArenaFixed = true
			r.readBuffersRegistered = true
			r.features.FixedRead = true
		}
		runtime.KeepAlive(vector)
	}
	runtime.KeepAlive(arena)
	return nil
}

// PrepareWriteFixed appends one positional write using a registered file and
// buffer. linked makes failure cancel the immediately following request.
func (r *Ring) PrepareWriteFixed(file, buffer, length int, offset int64, userData uint64, linked bool) error {
	return r.prepareFixed(ioUringOpWriteFixed, file, buffer, length, offset, userData, linked, 0)
}

// prepareWriteFixedDataSync appends a fixed positional write with per-write
// data-integrity completion. RWF_DSYNC has the same completion contract as an
// O_DSYNC write, so a successful CQE makes a following whole-file fdatasync
// redundant for this write.
func (r *Ring) prepareWriteFixedDataSync(
	file, buffer, length int,
	offset int64,
	userData uint64,
	linked bool,
) error {
	return r.prepareFixed(
		ioUringOpWriteFixed, file, buffer, length, offset, userData, linked,
		rwfDataSync,
	)
}

// prepareWriteVectorDataSync appends one durable vectored write for a
// contiguous file range. The iovec array and every payload live in mmap-backed
// memory that remains stable until Pop consumes the completion.
func (r *Ring) prepareWriteVectorDataSync(
	file int,
	writes []Write,
	bufferSize int,
	frameArena []byte,
	frameSize int,
	userData uint64,
	linked bool,
) (uint32, error) {
	if r.closed {
		return 0, ErrClosed
	}
	if !r.writeVectorSupported || len(r.writeVectors) == 0 {
		return 0, ErrUnsupported
	}
	if file < 0 || file >= r.files || bufferSize != r.bufferSize ||
		len(writes) == 0 ||
		len(writes) > len(r.writeVectors) || r.writeVectorBusy {
		return 0, syscall.EINVAL
	}
	offset := writes[0].Offset
	nextOffset := offset
	var total uint64
	for index, write := range writes {
		if write.Offset != nextOffset {
			return 0, syscall.EINVAL
		}
		if _, err := validateWrite(r.buffers, bufferSize, write); err != nil {
			return 0, err
		}
		data, err := writeBytes(
			r.bufferMap, bufferSize, frameArena, frameSize, write,
		)
		if err != nil {
			return 0, err
		}
		if len(data) == 0 || total+uint64(len(data)) > math.MaxInt32 {
			return 0, syscall.EINVAL
		}
		buffer := int32(-1)
		if !write.frameNative() {
			buffer = int32(write.Buffer)
			if r.bufferBusy[buffer] {
				return 0, ErrBufferBusy
			}
		}
		r.writeVectors[index] = ioVector{
			Base: uintptr(unsafe.Pointer(&data[0])), Len: uintptr(len(data)),
		}
		r.writeVectorBuffers[index] = buffer
		total += uint64(len(data))
		nextOffset += int64(len(data))
		runtime.KeepAlive(data)
	}
	flags := uint8(ioSQEFixedFile)
	if linked {
		flags |= ioSQEIOLink
	}
	err := r.prepare(ioUringSQE{
		Opcode:    ioUringOpWriteV,
		Flags:     flags,
		FD:        int32(file),
		Offset:    uint64(offset),
		Address:   uint64(uintptr(unsafe.Pointer(&r.writeVectors[0]))),
		Length:    uint32(len(writes)),
		Operation: rwfDataSync,
	}, userData, -1, uint32(len(writes)))
	if err != nil {
		return 0, err
	}
	for index := range writes {
		if buffer := r.writeVectorBuffers[index]; buffer >= 0 {
			r.bufferBusy[buffer] = true
		}
	}
	r.writeVectorBusy = true
	runtime.KeepAlive(r.writeVectorMap)
	runtime.KeepAlive(frameArena)
	return uint32(total), nil
}

// prepareWriteBytes appends a positional non-fixed write from stable off-heap
// page-cache memory. The committer keeps the owning frame locked until Pop
// consumes the completion.
func (r *Ring) prepareWriteBytes(
	file int,
	data []byte,
	offset int64,
	userData uint64,
	linked bool,
) error {
	if r.closed {
		return ErrClosed
	}
	if file < 0 || file >= r.files || len(data) == 0 ||
		len(data) > math.MaxInt32 || offset < 0 {
		return syscall.EINVAL
	}
	flags := uint8(ioSQEFixedFile)
	if linked {
		flags |= ioSQEIOLink
	}
	err := r.prepare(ioUringSQE{
		Opcode:  ioUringOpWrite,
		Flags:   flags,
		FD:      int32(file),
		Offset:  uint64(offset),
		Address: uint64(uintptr(unsafe.Pointer(&data[0]))),
		Length:  uint32(len(data)),
	}, userData, -1, 0)
	runtime.KeepAlive(data)
	return err
}

func (r *Ring) prepareWriteFrame(
	file int,
	data []byte,
	offset int64,
	userData uint64,
	linked bool,
) error {
	if r.closed {
		return ErrClosed
	}
	if file < 0 || file >= r.files || len(data) == 0 ||
		len(data) > math.MaxInt32 || offset < 0 ||
		len(r.frameArena) == 0 {
		return syscall.EINVAL
	}
	start := uintptr(unsafe.Pointer(&data[0]))
	arenaStart := uintptr(unsafe.Pointer(&r.frameArena[0]))
	if start < arenaStart || start-arenaStart > uintptr(len(r.frameArena)-len(data)) {
		return syscall.EINVAL
	}
	flags := uint8(ioSQEFixedFile)
	if linked {
		flags |= ioSQEIOLink
	}
	err := r.prepare(ioUringSQE{
		Opcode:      ioUringOpWriteFixed,
		Flags:       flags,
		FD:          int32(file),
		Offset:      uint64(offset),
		Address:     uint64(start),
		Length:      uint32(len(data)),
		BufferIndex: uint16(r.frameBuffer),
	}, userData, -1, 0)
	runtime.KeepAlive(data)
	return err
}

// prepareReadArena appends one positional read directly into the stable arena
// supplied to useReadArena. The caller owns non-overlap and must not release or
// reuse the named bytes until Pop returns this request's completion.
func (r *Ring) prepareReadArena(file, arenaOffset, length int, offset int64, userData uint64) error {
	if r.closed {
		return ErrClosed
	}
	if !r.features.AsyncRead {
		return ErrUnsupported
	}
	if file < 0 || file >= r.files || arenaOffset < 0 || length <= 0 ||
		length > math.MaxInt32 || arenaOffset > len(r.readArena)-length || offset < 0 {
		return syscall.EINVAL
	}
	opcode := uint8(ioUringOpRead)
	bufferIndex := uint16(0)
	if r.readArenaFixed {
		opcode = ioUringOpReadFixed
		bufferIndex = r.readArenaBuffer
	}
	sqe := ioUringSQE{
		Opcode:      opcode,
		Flags:       ioSQEFixedFile,
		FD:          int32(file),
		Offset:      uint64(offset),
		Address:     uint64(uintptr(unsafe.Pointer(&r.readArena[arenaOffset]))),
		Length:      uint32(length),
		BufferIndex: bufferIndex,
	}
	err := r.prepare(sqe, userData, -1, 0)
	runtime.KeepAlive(r.readArena)
	return err
}

func (r *Ring) prepareFixed(
	op uint8,
	file, buffer, length int,
	offset int64,
	userData uint64,
	linked bool,
	rwFlags uint32,
) error {
	if r.closed {
		return ErrClosed
	}
	if file < 0 || file >= r.files || buffer < 0 || buffer >= r.buffers ||
		length < 0 || length > r.bufferSize || offset < 0 {
		return syscall.EINVAL
	}
	if r.bufferBusy[buffer] {
		return ErrBufferBusy
	}
	flags := uint8(ioSQEFixedFile)
	if linked {
		flags |= ioSQEIOLink
	}
	sqe := ioUringSQE{
		Opcode:      op,
		Flags:       flags,
		FD:          int32(file),
		Offset:      uint64(offset),
		Address:     uint64(uintptr(unsafe.Pointer(&r.bufferMap[buffer*r.bufferSize]))),
		Length:      uint32(length),
		Operation:   rwFlags,
		BufferIndex: uint16(buffer),
	}
	if err := r.prepare(sqe, userData, int32(buffer), 0); err != nil {
		return err
	}
	r.bufferBusy[buffer] = true
	return nil
}

// PrepareDataSync appends a data-integrity barrier for a registered file.
// linked makes failure cancel the immediately following request.
func (r *Ring) PrepareDataSync(file int, userData uint64, linked bool) error {
	if r.closed {
		return ErrClosed
	}
	if file < 0 || file >= r.files {
		return syscall.EINVAL
	}
	flags := uint8(ioSQEFixedFile)
	if linked {
		flags |= ioSQEIOLink
	}
	return r.prepare(ioUringSQE{
		Opcode:    ioUringOpFsync,
		Flags:     flags,
		FD:        int32(file),
		Operation: ioUringFsyncDataSync,
	}, userData, -1, 0)
}

func (r *Ring) prepare(
	sqe ioUringSQE,
	userData uint64,
	buffer int32,
	vectorCount uint32,
) error {
	head := atomic.LoadUint32(r.sqHead)
	tail := r.sqTailValue
	if tail-head >= *r.sqEntries || r.freeCount == 0 {
		return ErrQueueFull
	}
	r.freeCount--
	requestIndex := r.freeRequests[r.freeCount]
	r.nextToken++
	if r.nextToken == 0 || r.nextToken > r.maxToken {
		// Token wrap is unreachable in practice, but resetting is safe because a
		// free request slot cannot still have a completion in the queue.
		r.nextToken = 1
	}
	token := r.nextToken*uint64(len(r.requests)) + uint64(requestIndex)
	if token == 0 {
		r.nextToken++
		token = r.nextToken*uint64(len(r.requests)) + uint64(requestIndex)
	}
	request := &r.requests[requestIndex]
	if request.outstanding {
		r.freeRequests[r.freeCount] = requestIndex
		r.freeCount++
		return fmt.Errorf("%w: request-token collision", ErrOverflow)
	}
	request.token = token
	request.userData = userData
	request.buffer = buffer
	request.vectorCount = vectorCount
	request.outstanding = true
	sqe.UserData = token
	index := tail & *r.sqMask
	r.sqes[index] = sqe
	r.sqTailValue = tail + 1
	r.pending++
	r.outstanding++
	return nil
}

// SubmitAndWait submits every prepared request and waits until at least min
// completions are available. min may be zero and cannot exceed the number of
// outstanding requests.
func (r *Ring) SubmitAndWait(min uint32) error {
	if r.closed {
		return ErrClosed
	}
	if min > r.outstanding {
		return syscall.EINVAL
	}
	if r.pending != 0 {
		// The ring is not SQPOLL-enabled: the kernel cannot consume these SQEs
		// until io_uring_enter. Publishing the tail once for the whole batch
		// avoids one release store per prepared request while retaining the
		// required SQE/array-before-tail ordering.
		atomic.StoreUint32(r.sqTail, r.sqTailValue)
	}
	for r.pending != 0 {
		wait := uint32(0)
		flags := uint32(0)
		if available := r.available(); available < min {
			wait = min - available
			flags = ioUringEnterGetEvents
		}
		submitted, err := ioUringEnter(r.fd, r.pending, wait, flags)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("io_uring submit: %w", err)
		}
		if submitted == 0 {
			return fmt.Errorf("io_uring submit: %w", syscall.EAGAIN)
		}
		if submitted > r.pending {
			return fmt.Errorf("%w: kernel submitted more SQEs than requested", ErrOverflow)
		}
		r.pending -= submitted
	}
	for r.available() < min {
		need := min - r.available()
		_, err := ioUringEnter(r.fd, 0, need, ioUringEnterGetEvents)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("io_uring wait: %w", err)
		}
	}
	return r.checkAccounting()
}

// Available returns the number of completion entries ready for Pop.
func (r *Ring) Available() uint32 {
	if r.closed {
		return 0
	}
	return r.available()
}

func (r *Ring) available() uint32 {
	return atomic.LoadUint32(r.cqTail) - atomic.LoadUint32(r.cqHead)
}

// Pop consumes one completion. ok is false when the completion queue is empty.
func (r *Ring) Pop() (completion Completion, ok bool, err error) {
	completions, err := r.PopBatch(1)
	if err != nil {
		return Completion{}, false, err
	}
	if len(completions) == 0 {
		return Completion{}, false, nil
	}
	return completions[0], true, nil
}

// PopBatch consumes up to max ready completions and returns them from a ring
// owned scratch slice. A malformed CQE returns ErrOverflow; callers must treat
// the ring as failed, just as they must for a single Pop accounting error. The
// returned slice is overwritten by the next Pop or PopBatch call.
func (r *Ring) PopBatch(max uint32) ([]Completion, error) {
	if r.closed {
		return nil, ErrClosed
	}
	if max == 0 {
		return nil, nil
	}
	if max > uint32(len(r.popScratch)) {
		return nil, syscall.EINVAL
	}
	if err := r.checkAccounting(); err != nil {
		return nil, err
	}
	head := atomic.LoadUint32(r.cqHead)
	available := atomic.LoadUint32(r.cqTail) - head
	if available == 0 {
		return nil, nil
	}
	if available > max {
		available = max
	}
	if r.freeCount > uint32(len(r.freeRequests)) || available > r.outstanding ||
		available > uint32(len(r.freeRequests))-r.freeCount {
		return nil, fmt.Errorf("%w: completion count exceeds request accounting", ErrOverflow)
	}

	for index := uint32(0); index < available; index++ {
		cqe := r.cqes[(head+index)&*r.cqMask]
		requestIndex := r.requestIndex(cqe.UserData)
		request := &r.requests[requestIndex]
		if !request.outstanding || request.token != cqe.UserData {
			return nil, fmt.Errorf("%w: unknown completion token", ErrOverflow)
		}
		r.popScratch[index] = Completion{
			UserData: request.userData, Result: cqe.Result, Flags: cqe.Flags,
		}
		if request.buffer >= 0 {
			r.bufferBusy[request.buffer] = false
		}
		if request.vectorCount != 0 {
			if request.vectorCount > uint32(len(r.writeVectorBuffers)) {
				return nil, fmt.Errorf("%w: invalid vector request", ErrOverflow)
			}
			for vectorIndex := range request.vectorCount {
				if buffer := r.writeVectorBuffers[vectorIndex]; buffer >= 0 {
					r.bufferBusy[buffer] = false
				}
			}
			r.writeVectorBusy = false
		}
		*request = requestSlot{}
		r.freeRequests[r.freeCount] = requestIndex
		r.freeCount++
		r.outstanding--
	}
	atomic.StoreUint32(r.cqHead, head+available)
	return r.popScratch[:available], nil
}

func (r *Ring) requestIndex(token uint64) uint32 {
	if r.requestPow2 {
		return uint32(token & r.requestMask)
	}
	return uint32(token % r.requestCount)
}

func (r *Ring) checkAccounting() error {
	if atomic.LoadUint32(r.sqDropped) != r.droppedBase ||
		atomic.LoadUint32(r.cqOverflow) != r.overflowBase ||
		r.available() > *r.cqEntries {
		return ErrOverflow
	}
	return nil
}

// Close drains prepared and in-flight requests, unregisters kernel resources,
// closes the ring, and unmaps all shared and staging memory. It is idempotent.
func (r *Ring) Close() error {
	if r == nil || r.closed {
		return nil
	}
	var result error
	if r.outstanding != 0 {
		if err := r.SubmitAndWait(r.outstanding); err != nil {
			result = errors.Join(result, err)
		} else {
			for r.outstanding != 0 {
				_, ok, err := r.Pop()
				if err != nil {
					result = errors.Join(result, err)
					break
				}
				if !ok {
					result = errors.Join(result, ErrOverflow)
					break
				}
			}
		}
	}
	if r.buffers != 0 || r.readBuffersRegistered {
		if err := ioUringRegister(r.fd, ioUringUnregisterBuffers, nil, 0); err != nil {
			result = errors.Join(result, fmt.Errorf("io_uring unregister buffers: %w", err))
		}
	}
	if r.files != 0 {
		if err := ioUringRegister(r.fd, ioUringUnregisterFiles, nil, 0); err != nil {
			result = errors.Join(result, fmt.Errorf("io_uring unregister files: %w", err))
		}
	}
	if err := syscall.Close(r.fd); err != nil {
		result = errors.Join(result, err)
	}
	if len(r.bufferMap) != 0 {
		if err := releaseArena(r.bufferMap); err != nil {
			result = errors.Join(result, err)
		}
	}
	if len(r.writeVectorMap) != 0 {
		if err := releaseArena(r.writeVectorMap); err != nil {
			result = errors.Join(result, err)
		}
	}
	r.unmapQueues()
	r.closed = true
	r.bufferMap = nil
	r.writeVectorMap = nil
	r.writeVectors = nil
	r.writeVectorBuffers = nil
	r.readArena = nil
	r.readArenaFixed = false
	r.readBuffersRegistered = false
	r.frameArena = nil
	r.requests = nil
	r.freeRequests = nil
	r.popScratch = nil
	return result
}

func (r *Ring) unmapQueues() {
	if len(r.sqesMap) != 0 {
		_ = syscall.Munmap(r.sqesMap)
		r.sqesMap = nil
	}
	if len(r.sqRing) != 0 && len(r.cqRing) != 0 && &r.sqRing[0] == &r.cqRing[0] {
		_ = syscall.Munmap(r.sqRing)
		r.sqRing = nil
		r.cqRing = nil
		return
	}
	if len(r.sqRing) != 0 {
		_ = syscall.Munmap(r.sqRing)
		r.sqRing = nil
	}
	if len(r.cqRing) != 0 {
		_ = syscall.Munmap(r.cqRing)
		r.cqRing = nil
	}
}

func ioUringSetup(entries uint32, params *ioUringParams) (int, error) {
	fd, _, errno := syscall.Syscall(sysIOUringSetup, uintptr(entries), uintptr(unsafe.Pointer(params)), 0)
	runtime.KeepAlive(params)
	if errno != 0 {
		return -1, errno
	}
	return int(fd), nil
}

func ioUringEnter(fd int, submit, complete, flags uint32) (uint32, error) {
	n, _, errno := syscall.Syscall6(sysIOUringEnter, uintptr(fd), uintptr(submit),
		uintptr(complete), uintptr(flags), 0, 0)
	if errno != 0 {
		return 0, errno
	}
	return uint32(n), nil
}

func ioUringRegister(fd int, opcode uint32, argument unsafe.Pointer, count uint32) error {
	_, _, errno := syscall.Syscall6(sysIOUringRegister, uintptr(fd), uintptr(opcode),
		uintptr(argument), uintptr(count), 0, 0)
	runtime.KeepAlive(argument)
	if errno != 0 {
		return errno
	}
	return nil
}
