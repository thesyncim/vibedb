//go:build linux || darwin

package durable

import (
	"bytes"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/store"
	"golang.org/x/sys/unix"
)

func onlineCompactionAllocatedBytes(file *os.File) uint64 {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return 0
	}
	return uint64(stat.Blocks) * 512
}

func TestCompactOnlineForegroundWriteP99Bound(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "online-compact-p99-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testBatchOptions(64)
	options.ResidentBytes = 64 << 20
	options.MaxRetiredExtents = 1 << 16
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	defer collection.Close()
	for first := 0; first < 2048; first += 64 {
		if err := collection.Update(func(batch *WriteBatch) error {
			for row := first; row < first+64; row++ {
				if err := batch.Put(fmt.Appendf(nil, "seed-%08d", row), []byte(`{"v":1}`)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		<-start
		_, err := collection.CompactOnline()
		result <- err
	}()
	close(start)
	latencies := make([]time.Duration, 8)
	for row := range latencies {
		began := time.Now()
		_, err := collection.Put(
			fmt.Appendf(nil, "foreground-%08d", row), []byte(`{"v":2}`),
		)
		latencies[row] = time.Since(began)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	slices.Sort(latencies)
	p99 := latencies[len(latencies)*99/100]
	if p99 > 500*time.Millisecond {
		t.Fatalf("foreground write p99=%s (>500ms), max=%s", p99, latencies[len(latencies)-1])
	}
	t.Logf("foreground write p99=%s max=%s", p99, latencies[len(latencies)-1])
}

// TestCompactOnlineHardAmplificationGates jointly bounds the same-file peak,
// allocated blocks, device payload, and allocator-publication cadence. Mixed
// overflow values plus an exact index exercise every shipped emitter.
func TestCompactOnlineHardAmplificationGates(t *testing.T) {
	rows := 4096
	if testing.Short() {
		rows = 1024
	}
	file, err := os.CreateTemp(t.TempDir(), "online-compact-amp-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	options := testBatchOptions(128)
	options.ResidentBytes = 64 << 20
	options.MaxRetiredExtents = 1 << 17
	options.InlineValueBytes = 256
	options.Indexes = []store.IndexDefinition{{Name: "group", Paths: []string{"/group"}}}
	collection, err := Create(file, options)
	if err != nil {
		t.Fatal(err)
	}
	logical := uint64(0)
	for first := 0; first < rows; first += 128 {
		last := min(first+128, rows)
		if err := collection.Update(func(batch *WriteBatch) error {
			for row := first; row < last; row++ {
				key := fmt.Appendf(nil, "row-%08d", row)
				value := fmt.Appendf(nil, `{"group":"g%03d","pad":"`, row%257)
				if row&1 != 0 {
					value = append(value, bytes.Repeat([]byte{'x'}, 2048)...)
				}
				value = append(value, `"}`...)
				logical += uint64(len(key) + len(value))
				if err := batch.Put(key, value); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	beforeAllocated := onlineCompactionAllocatedBytes(file)
	report, err := collection.CompactOnline()
	if err != nil {
		t.Fatal(err)
	}
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	peakDelta := uint64(info.Size()) - report.SourceFileEnd
	allocated := onlineCompactionAllocatedBytes(file)
	allocatedDelta := uint64(0)
	if allocated > beforeAllocated {
		allocatedDelta = allocated - beforeAllocated
	}
	maxExtents := uint64(16) + (report.StagingAllocatedBytes+(4<<20)-1)/(4<<20)
	if peakDelta > 8*logical || allocatedDelta > 8*logical ||
		report.DeviceBytes > 12*logical || report.StagingExtentCount > maxExtents {
		t.Fatalf("online compaction amplification peak/allocated/device=%d/%d/%d logical=%d extents=%d max=%d report=%+v",
			peakDelta, allocatedDelta, report.DeviceBytes, logical,
			report.StagingExtentCount, maxExtents, report)
	}
	t.Logf("online compaction ratios peak=%.3fx allocated=%.3fx device=%.3fx extents=%d",
		float64(peakDelta)/float64(logical), float64(allocatedDelta)/float64(logical),
		float64(report.DeviceBytes)/float64(logical), report.StagingExtentCount)
}
