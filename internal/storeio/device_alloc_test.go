package storeio

import (
	"os"
	"testing"
)

func TestPortableCommitSteadyAllocation(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "portable-alloc")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	pageSize := os.Getpagesize()
	device, err := OpenDevice(file, DeviceOptions{
		Backend: BackendPortable, BufferCount: 2, BufferSize: pageSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	const page = "immutable-page"
	const root = "alternate-root"
	copy(deviceBuffer(t, device, 0), page)
	copy(deviceBuffer(t, device, 1), root)
	writes := [...]Write{{Offset: int64(pageSize), Length: uint32(len(page)), Buffer: 0}}
	rootWrite := Write{Offset: 0, Length: uint32(len(root)), Buffer: 1}
	if allocs := testing.AllocsPerRun(20, func() {
		if err := device.Commit(writes[:], rootWrite); err != nil {
			panic(err)
		}
	}); allocs != 0 {
		t.Fatalf("portable Commit allocations = %g, want 0", allocs)
	}
}
