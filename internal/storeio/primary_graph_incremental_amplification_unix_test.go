//go:build !windows

package storeio

import (
	"bytes"
	"fmt"
	"os"
	"syscall"
	"testing"
)

func TestPrimaryGraphStreamHardAmplificationAndReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("stream amplification qualification")
	}
	const rows = 100_000
	file, err := os.CreateTemp(t.TempDir(), "stream-amplification-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reservation := UnrootedGenerationReservation{
		Offset: 64 << 10, Length: 128 << 20,
		FirstLogicalID: PrimaryFirstDynamicLogicalID,
		LogicalIDCount: 1 << 20,
	}
	writer, err := NewUnrootedGenerationWriter(
		file, reservation, testStoreID, 31, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewUnrootedPrimaryGraphSink(
		writer, testStoreID, 31, reservation.FirstLogicalID,
		reservation.Offset+reservation.Length, make([]byte, 512<<10),
	)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := NewPrimaryGraphStreamBuilder(sink, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	logicalBytes := uint64(0)
	for first := 0; first < rows; first += CommonPrimaryLeafWideSlots {
		count := min(CommonPrimaryLeafWideSlots, rows-first)
		window := make([]PrimaryGraphRecord, count)
		for row := range count {
			rank := first + row
			key := []byte(fmt.Sprintf("key-%08d", rank))
			value := []byte(fmt.Sprintf(
				`{"rank":%d,"payload":"%s"}`,
				rank, bytes.Repeat([]byte{'x'}, 80),
			))
			logicalBytes += uint64(len(key) + len(value))
			window[row] = BorrowPrimaryGraphRecord(key, value)
		}
		if err := stream.StageWindow(window, nil); err != nil {
			t.Fatal(err)
		}
	}
	root, err := stream.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Sync(); err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	apparent := uint64(info.Size()) - reservation.Offset
	allocated := uint64(0)
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		allocated = uint64(stat.Blocks) * 512
	}
	deviceWrite := writer.WrittenBytes()
	if apparent > 3*logicalBytes || deviceWrite > 3*logicalBytes ||
		allocated != 0 && allocated > 4*logicalBytes {
		t.Fatalf(
			"stream amplification apparent/allocated/write=%d/%d/%d logical=%d",
			apparent, allocated, deviceWrite, logicalBytes,
		)
	}
	cache, err := NewPageCache(file, PageCacheOptions{
		PageSize: int(format0PageSize), MaxPageSize: CommonPrimaryLeafMaxExtentBytes,
		ResidentBytes: 8 << 20, StoreID: testStoreID, ReadConcurrency: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cache.Close()
	router, err := BuildResidentPrimaryRouter(
		cache, root,
		GlobalTabletCatalogBounds{
			StoreID: testStoreID, SelectedRootGeneration: 31,
			FileEnd:       reservation.Offset + reservation.Length,
			NextLogicalID: sink.BuildNextLogicalID(),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if router.Len() < rows/CommonPrimaryLeafWideSlots {
		t.Fatalf("reopened router leaves=%d", router.Len())
	}
	t.Logf(
		"stream ratios apparent=%.3fx allocated=%.3fx device-write=%.3fx",
		float64(apparent)/float64(logicalBytes),
		float64(allocated)/float64(logicalBytes),
		float64(deviceWrite)/float64(logicalBytes),
	)
}
