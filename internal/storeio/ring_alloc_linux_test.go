//go:build linux && (amd64 || arm64 || riscv64 || loong64)

package storeio

import (
	"os"
	"runtime"
	"testing"
)

func TestFixedWriteSteadyAllocation(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	ring, _ := newFixedTestRing(t)
	defer func() {
		if err := ring.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	const value = "failure-atomic-page"
	if allocs := testing.AllocsPerRun(100, func() {
		buffer, err := ring.Buffer(0)
		if err != nil {
			panic(err)
		}
		copy(buffer, value)
		if err := ring.PrepareWriteFixed(0, 0, len(value), 0, 43, false); err != nil {
			panic(err)
		}
		if err := ring.SubmitAndWait(1); err != nil {
			panic(err)
		}
		completion, ok, err := ring.Pop()
		if err != nil {
			panic(err)
		}
		if !ok || completion.UserData != 43 || completion.Result != int32(len(value)) {
			panic("unexpected completion")
		}
	}); allocs != 0 {
		t.Fatalf("fixed write allocations = %g, want 0", allocs)
	}
}

func TestArenaReadSteadyAllocation(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	ring, file := newFixedTestRing(t)
	defer func() {
		if err := ring.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	if !ring.Features().AsyncRead {
		t.Skip("kernel does not support IORING_OP_READ")
	}
	pageSize := os.Getpagesize()
	const value = "zero-copy-arena-read"
	if _, err := file.WriteAt([]byte(value), int64(pageSize)); err != nil {
		t.Fatal(err)
	}
	arena, err := allocateArena(pageSize)
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
	if allocs := testing.AllocsPerRun(100, func() {
		if err := ring.prepareReadArena(0, 0, len(value), int64(pageSize), 43); err != nil {
			panic(err)
		}
		if err := ring.SubmitAndWait(1); err != nil {
			panic(err)
		}
		completion, ok, err := ring.Pop()
		if err != nil {
			panic(err)
		}
		if !ok || completion.UserData != 43 || completion.Result != int32(len(value)) {
			panic("unexpected completion")
		}
	}); allocs != 0 {
		t.Fatalf("arena read allocations = %g, want 0", allocs)
	}
	if string(arena[:len(value)]) != value {
		t.Fatalf("arena read = %q, want %q", arena[:len(value)], value)
	}
}

func TestRingDeviceCommitSteadyAllocation(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	device, _ := newRingTestDevice(t)
	defer func() {
		if err := device.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()
	const page = "ring-page"
	const root = "ring-root"
	copy(deviceBuffer(t, device, 0), page)
	copy(deviceBuffer(t, device, 1), root)
	writes := [...]Write{{Offset: int64(os.Getpagesize()), Length: uint32(len(page)), Buffer: 0}}
	rootWrite := Write{Length: uint32(len(root)), Buffer: 1}
	if allocs := testing.AllocsPerRun(20, func() {
		if err := device.Commit(writes[:], rootWrite); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("ring Commit allocations = %g, want 0", allocs)
	}
}

func TestRingCommitterSteadyAllocation(t *testing.T) {
	committer, _ := newRingTestCommitter(t)
	defer committer.Close()
	pageSize := os.Getpagesize()
	var generation uint64
	if allocs := testing.AllocsPerRun(20, func() {
		generation++
		batch, err := committer.Begin(1)
		if err != nil {
			panic(err)
		}
		page, err := batch.PageBuffer(0)
		if err != nil {
			panic(err)
		}
		root, err := batch.RootBuffer()
		if err != nil {
			panic(err)
		}
		copy(page, "page")
		copy(root, "root")
		if err := batch.SetPage(0, int64(pageSize), 4); err != nil {
			panic(err)
		}
		if err := batch.SetRoot(0, 4); err != nil {
			panic(err)
		}
		if err := batch.Publish(generation); err != nil {
			panic(err)
		}
		if err := committer.Wait(generation); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("ring Begin/fill/Publish/Wait allocations = %g, want 0", allocs)
	}
}
