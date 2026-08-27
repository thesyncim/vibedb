//go:build linux && (amd64 || arm64 || riscv64 || loong64)

package storeio

import (
	"bytes"
	"errors"
	"os"
	"runtime"
	"syscall"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

// mincore inspects this exact private mapping, not process RSS, Go GC timing,
// or unrelated caches. Disable huge-page promotion so one touched base page
// cannot legitimately make an entire huge-page-sized region resident.
func coldRingTestArena(t *testing.T) []byte {
	t.Helper()
	arena, err := allocateArena(512 * os.Getpagesize())
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Madvise(arena, unix.MADV_NOHUGEPAGE); err != nil {
		_ = releaseArena(arena)
		t.Fatal(err)
	}
	return arena
}

func ringArenaResidentPages(t *testing.T, arena []byte) int {
	t.Helper()
	vector := make([]byte, len(arena)/os.Getpagesize())
	_, _, errno := syscall.Syscall(syscall.SYS_MINCORE,
		uintptr(unsafe.Pointer(&arena[0])), uintptr(len(arena)), uintptr(unsafe.Pointer(&vector[0])))
	runtime.KeepAlive(arena)
	runtime.KeepAlive(vector)
	if errno != 0 {
		t.Fatal(errno)
	}
	resident := 0
	for _, page := range vector {
		resident += int(page & 1)
	}
	return resident
}

func TestArenaReadDoesNotPrefaultColdCache(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	ring, err := Open(Config{Entries: 8, SingleIssuer: true})
	if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer ring.Close()
	if !ring.Features().AsyncRead {
		t.Skip("kernel does not support IORING_OP_READ")
	}
	file, err := os.CreateTemp(t.TempDir(), "read-cold-cache")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	want := []byte("read-only-the-addressed-page")
	if _, err := file.WriteAt(want, 0); err != nil {
		t.Fatal(err)
	}
	if err := ring.RegisterFiles([]int{int(file.Fd())}); err != nil {
		t.Fatal(err)
	}
	arena := coldRingTestArena(t)
	defer func() {
		if err := ring.Close(); err != nil {
			t.Error(err)
			return
		}
		if err := releaseArena(arena); err != nil {
			t.Error(err)
		}
	}()
	before := ringArenaResidentPages(t, arena)
	if before != 0 {
		t.Fatalf("fresh anonymous arena has %d resident pages", before)
	}
	if err := ring.useReadArena(arena); err != nil {
		t.Fatal(err)
	}
	afterBind := ringArenaResidentPages(t, arena)
	if afterBind != before || ring.buffers != 0 {
		t.Fatalf("binding faulted or registered cold cache: before=%d after=%d buffers=%d", before, afterBind, ring.buffers)
	}
	offset := len(arena) - os.Getpagesize()
	if err := ring.prepareReadArena(0, offset, len(want), 0, 19); err != nil {
		t.Fatal(err)
	}
	if err := ring.SubmitAndWait(1); err != nil {
		t.Fatal(err)
	}
	completion, ok, err := ring.Pop()
	if err != nil || !ok || completion.UserData != 19 || completion.Result != int32(len(want)) {
		t.Fatalf("read completion=%+v found=%t err=%v", completion, ok, err)
	}
	if !bytes.Equal(arena[offset:offset+len(want)], want) {
		t.Fatal("read payload changed")
	}
	afterRead := ringArenaResidentPages(t, arena)
	if afterRead != 1 {
		t.Fatalf("one-page read made %d cache pages resident", afterRead)
	}
	t.Logf("cache pages=%d resident before=%d bound=%d after-read=%d", len(arena)/os.Getpagesize(), before, afterBind, afterRead)
}

func TestFrameWriteDoesNotPrefaultColdCache(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	device, file := newRingTestDevice(t)
	ringDevice := device.(*ringDevice)
	defer device.Close()
	arena := coldRingTestArena(t)
	defer func() {
		if err := device.Close(); err != nil {
			t.Error(err)
			return
		}
		if err := releaseArena(arena); err != nil {
			t.Error(err)
		}
	}()
	before := ringArenaResidentPages(t, arena)
	if before != 0 {
		t.Fatalf("fresh anonymous arena has %d resident pages", before)
	}
	if err := ringDevice.bindFrameArena(arena, os.Getpagesize()); err != nil {
		t.Fatal(err)
	}
	afterBind := ringArenaResidentPages(t, arena)
	if afterBind != before || ringDevice.ring.buffers != 3 || len(ringDevice.ring.bufferMap) != 3*os.Getpagesize() {
		t.Fatalf("binding changed fixed staging buffers or faulted cache: before=%d after=%d buffers=%d", before, afterBind, ringDevice.ring.buffers)
	}
	want := []byte("durable-frame-only")
	lastPage := len(arena)/os.Getpagesize() - 1
	copy(arena[lastPage*os.Getpagesize():], want)
	root, err := device.Buffer(0)
	if err != nil {
		t.Fatal(err)
	}
	rootValue := []byte("fixed-root")
	copy(root, rootValue)
	page := Write{Offset: int64(os.Getpagesize()), Length: uint32(len(want)), frameIndex: uint32(lastPage), pendingFlags: pendingWriteFrameNative}
	if err := device.Commit([]Write{page}, Write{Offset: 0, Buffer: 0, Length: uint32(len(rootValue))}); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(want))
	if _, err := file.ReadAt(got, page.Offset); err != nil || !bytes.Equal(got, want) {
		t.Fatalf("frame=%q err=%v", got, err)
	}
	got = got[:len(rootValue)]
	if _, err := file.ReadAt(got, 0); err != nil || !bytes.Equal(got, rootValue) {
		t.Fatalf("root=%q err=%v", got, err)
	}
	afterWrite := ringArenaResidentPages(t, arena)
	if afterWrite != 1 {
		t.Fatalf("one-page write made %d cache pages resident", afterWrite)
	}
	pages := []Write{page}
	rootWrite := Write{Offset: 0, Buffer: 0, Length: uint32(len(rootValue))}
	if allocations := testing.AllocsPerRun(16, func() {
		if err := device.Commit(pages, rootWrite); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("steady non-fixed frame plus fixed root commit allocated %g times", allocations)
	}
	if resident := ringArenaResidentPages(t, arena); resident != afterWrite {
		t.Fatalf("repeated writes faulted unused cache pages: before=%d after=%d", afterWrite, resident)
	}
	t.Logf("cache pages=%d resident before=%d bound=%d after-durable-write=%d", len(arena)/os.Getpagesize(), before, afterBind, afterWrite)
}
