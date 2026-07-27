//go:build linux && (amd64 || arm64 || riscv64 || loong64)

package storeio

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"unsafe"
)

func TestUAPILayout(t *testing.T) {
	if got := unsafe.Sizeof(ioUringParams{}); got != 120 {
		t.Fatalf("io_uring_params size = %d, want 120", got)
	}
	if got := unsafe.Offsetof(ioUringParams{}.SQOffsets); got != 40 {
		t.Fatalf("io_uring_params.sq_off = %d, want 40", got)
	}
	if got := unsafe.Offsetof(ioUringParams{}.CQOffsets); got != 80 {
		t.Fatalf("io_uring_params.cq_off = %d, want 80", got)
	}
	if got := unsafe.Sizeof(ioUringSQE{}); got != 64 {
		t.Fatalf("io_uring_sqe size = %d, want 64", got)
	}
	if got := unsafe.Offsetof(ioUringSQE{}.UserData); got != 32 {
		t.Fatalf("io_uring_sqe.user_data = %d, want 32", got)
	}
	if got := unsafe.Sizeof(ioUringCQE{}); got != 16 {
		t.Fatalf("io_uring_cqe size = %d, want 16", got)
	}
}

func TestFixedWriteAndDataSync(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	ring, file := newFixedTestRing(t)
	defer func() {
		if err := ring.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	page, err := ring.Buffer(0)
	if err != nil {
		t.Fatal(err)
	}
	root, err := ring.Buffer(1)
	if err != nil {
		t.Fatal(err)
	}
	wantPage := []byte("immutable-data-page")
	wantRoot := []byte("alternate-root")
	copy(page, wantPage)
	copy(root, wantRoot)
	pageOffset := int64(syscall.Getpagesize())
	if err := ring.PrepareWriteFixed(0, 0, len(wantPage), pageOffset, 41, true); err != nil {
		t.Fatal(err)
	}
	if _, err := ring.Buffer(0); !errors.Is(err, ErrBufferBusy) {
		t.Fatalf("in-flight Buffer error = %v, want %v", err, ErrBufferBusy)
	}
	if err := ring.PrepareDataSync(0, 42, true); err != nil {
		t.Fatal(err)
	}
	if err := ring.PrepareWriteFixed(0, 1, len(wantRoot), 0, 43, true); err != nil {
		t.Fatal(err)
	}
	if err := ring.PrepareDataSync(0, 44, false); err != nil {
		t.Fatal(err)
	}
	if err := ring.SubmitAndWait(4); err != nil {
		t.Fatal(err)
	}
	for i, userData := range []uint64{41, 42, 43, 44} {
		completion, ok, err := ring.Pop()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("completion %d missing", i)
		}
		if completion.UserData != userData {
			t.Fatalf("completion %d user data = %d, want %d", i, completion.UserData, userData)
		}
		if err := completion.Err(); err != nil {
			t.Fatalf("completion %d: %v", i, err)
		}
	}
	if ring.Available() != 0 {
		t.Fatalf("available = %d, want 0", ring.Available())
	}
	gotPage := make([]byte, len(wantPage))
	if _, err := file.ReadAt(gotPage, pageOffset); err != nil {
		t.Fatal(err)
	}
	if string(gotPage) != string(wantPage) {
		t.Fatalf("page = %q, want %q", gotPage, wantPage)
	}
	gotRoot := make([]byte, len(wantRoot))
	if _, err := file.ReadAt(gotRoot, 0); err != nil {
		t.Fatal(err)
	}
	if string(gotRoot) != string(wantRoot) {
		t.Fatalf("root = %q, want %q", gotRoot, wantRoot)
	}
}

func TestArenaReadBatch(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	ring, err := Open(Config{Entries: 8, SingleIssuer: true})
	if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ring.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	if !ring.Features().AsyncRead {
		t.Skip("kernel does not support IORING_OP_READ")
	}
	file, err := os.CreateTemp(t.TempDir(), "ring-arena-read-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	pageSize := os.Getpagesize()
	first := []byte("first-arena-page")
	second := []byte("second-arena-page")
	if _, err := file.WriteAt(first, int64(pageSize)); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(second, int64(2*pageSize)); err != nil {
		t.Fatal(err)
	}
	if err := ring.RegisterFiles([]int{int(file.Fd())}); err != nil {
		if errors.Is(err, syscall.ENOMEM) || errors.Is(err, syscall.EPERM) {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	arena, err := allocateArena(2 * pageSize)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ring.Close(); err != nil {
			t.Errorf("Close before arena release: %v", err)
		}
		if err := releaseArena(arena); err != nil {
			t.Errorf("release arena: %v", err)
		}
	}()
	if err := ring.useReadArena(arena); err != nil {
		t.Fatal(err)
	}
	if err := ring.prepareReadArena(0, 0, len(first), int64(pageSize), 41); err != nil {
		t.Fatal(err)
	}
	if err := ring.prepareReadArena(0, pageSize, len(second), int64(2*pageSize), 42); err != nil {
		t.Fatal(err)
	}
	if err := ring.SubmitAndWait(2); err != nil {
		t.Fatal(err)
	}
	var seen [2]bool
	for range 2 {
		completion, ok, err := ring.Pop()
		if err != nil || !ok {
			t.Fatalf("Pop = (%+v,%v,%v)", completion, ok, err)
		}
		if err := completion.Err(); err != nil {
			t.Fatal(err)
		}
		switch completion.UserData {
		case 41:
			seen[0] = true
			if completion.Result != int32(len(first)) {
				t.Fatalf("first result = %d", completion.Result)
			}
		case 42:
			seen[1] = true
			if completion.Result != int32(len(second)) {
				t.Fatalf("second result = %d", completion.Result)
			}
		default:
			t.Fatalf("unexpected user data %d", completion.UserData)
		}
	}
	if !seen[0] || !seen[1] || string(arena[:len(first)]) != string(first) ||
		string(arena[pageSize:pageSize+len(second)]) != string(second) {
		t.Fatalf("arena batch = seen %v, first %q, second %q",
			seen, arena[:len(first)], arena[pageSize:pageSize+len(second)])
	}
}

func TestRingDeviceCommit(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	device, file := newRingTestDevice(t)
	defer func() {
		if err := device.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	if device.Backend() != BackendIOUring {
		t.Fatalf("backend = %v, want io_uring", device.Backend())
	}
	pageSize := os.Getpagesize()
	page0 := []byte("ring-page-zero")
	page1 := []byte("ring-page-one")
	root := []byte("ring-root")
	copy(deviceBuffer(t, device, 0), page0)
	copy(deviceBuffer(t, device, 1), page1)
	copy(deviceBuffer(t, device, 2), root)
	writes := [...]Write{
		{Offset: int64(pageSize), Length: uint32(len(page0)), Buffer: 0},
		{Offset: int64(2 * pageSize), Length: uint32(len(page1)), Buffer: 1},
	}
	if err := device.Commit(writes[:], Write{Length: uint32(len(root)), Buffer: 2}); err != nil {
		t.Fatal(err)
	}
	assertFileBytes(t, file, int64(pageSize), page0)
	assertFileBytes(t, file, int64(2*pageSize), page1)
	assertFileBytes(t, file, 0, root)
}

func TestRingCommitter(t *testing.T) {
	committer, file := newRingTestCommitter(t)
	pageSize := os.Getpagesize()
	page := []byte("ring-committer-page")
	root := []byte("ring-committer-root")
	publishTestGeneration(t, committer, 1,
		[]testPage{{offset: int64(pageSize), data: page}}, 0, root)
	if err := committer.Wait(1); err != nil {
		t.Fatal(err)
	}
	if got := committer.Stats().Backend; got != BackendIOUring {
		t.Fatalf("backend = %v, want io_uring", got)
	}
	assertFileBytes(t, file, int64(pageSize), page)
	assertFileBytes(t, file, 0, root)
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRingDataFailureDoesNotSubmitRoot(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	path := filepath.Join(t.TempDir(), "read-only-ring-store")
	wantRoot := []byte("old-root")
	if err := os.WriteFile(path, wantRoot, 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	device, err := OpenDevice(file, DeviceOptions{
		Backend: BackendIOUring, BufferCount: 2, BufferSize: os.Getpagesize(),
		QueueDepth: 8, SingleIssuer: true,
	})
	if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	copy(deviceBuffer(t, device, 0), "new-page")
	copy(deviceBuffer(t, device, 1), "new-root")
	if err := device.Commit(
		[]Write{{Offset: int64(os.Getpagesize()), Length: 8, Buffer: 0}},
		Write{Offset: 0, Length: 8, Buffer: 1},
	); err == nil {
		t.Fatal("Commit to read-only file succeeded")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(wantRoot) {
		t.Fatalf("root changed after data failure: %q", got)
	}
}

func newFixedTestRing(t *testing.T) (*Ring, *os.File) {
	t.Helper()
	ring, err := Open(Config{Entries: 8, SingleIssuer: true})
	if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	file, err := os.CreateTemp(t.TempDir(), "ring")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	if err := ring.RegisterFiles([]int{int(file.Fd())}); err != nil {
		if errors.Is(err, syscall.ENOMEM) || errors.Is(err, syscall.EPERM) {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	if err := ring.RegisterBuffers(2, syscall.Getpagesize()); err != nil {
		if errors.Is(err, syscall.ENOMEM) || errors.Is(err, syscall.EPERM) {
			t.Skip(err)
		}
		t.Fatal(err)
	}
	return ring, file
}

func newRingTestDevice(t *testing.T) (Device, *os.File) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "ring-device")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	device, err := OpenDevice(file, DeviceOptions{
		Backend: BackendIOUring, BufferCount: 3, BufferSize: os.Getpagesize(),
		QueueDepth: 8, SingleIssuer: true,
	})
	if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	return device, file
}

func newRingTestCommitter(t *testing.T) (*Committer, *os.File) {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "ring-committer")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	committer, err := NewCommitter(file, DeviceOptions{
		Backend: BackendIOUring, BufferCount: 3, BufferSize: os.Getpagesize(), QueueDepth: 8,
	}, CommitterOptions{QueueSlots: 4, MaxPagesPerBatch: 1, GroupLimit: 4})
	if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	return committer, file
}
