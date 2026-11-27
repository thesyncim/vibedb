package resourceguard

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGuardMemoryAndDiskWatermarks(t *testing.T) {
	var fakeMem atomic.Uint64
	var fakeDisk atomic.Uint64
	fakeMem.Store(50 << 20)
	fakeDisk.Store(500 << 20)

	g := New(Options{
		MaxMemoryBytes:   100 << 20,
		DiskPath:         "/data",
		MinFreeDiskBytes: 200 << 20,
		SampleInterval:   5 * time.Millisecond,
		sampleMemory:     func() uint64 { return fakeMem.Load() },
		sampleDisk:       func(string) (uint64, error) { return fakeDisk.Load(), nil },
	})

	// Under both ceilings: reads and writes admitted.
	if err := g.AdmitRead(); err != nil {
		t.Fatalf("AdmitRead under ceiling: %v", err)
	}
	if err := g.AdmitWrite(); err != nil {
		t.Fatalf("AdmitWrite under ceiling: %v", err)
	}

	// Breach disk floor: writes rejected with ErrDiskWatermarkExceeded, reads still allowed!
	fakeDisk.Store(100 << 20)
	g.Refresh()
	if err := g.AdmitRead(); err != nil {
		t.Fatalf("AdmitRead during low disk should stay allowed, got: %v", err)
	}
	if err := g.AdmitWrite(); !errors.Is(err, ErrDiskWatermarkExceeded) || !errors.Is(err, ErrResourceWatermarkExceeded) {
		t.Fatalf("AdmitWrite during low disk = %v, want ErrDiskWatermarkExceeded", err)
	}

	// Recover disk, breach memory ceiling: both reads and writes rejected with ErrMemoryWatermarkExceeded.
	fakeDisk.Store(500 << 20)
	fakeMem.Store(150 << 20)
	g.Refresh()
	if err := g.AdmitRead(); !errors.Is(err, ErrMemoryWatermarkExceeded) {
		t.Fatalf("AdmitRead over memory = %v, want ErrMemoryWatermarkExceeded", err)
	}
	if err := g.AdmitWrite(); !errors.Is(err, ErrMemoryWatermarkExceeded) {
		t.Fatalf("AdmitWrite over memory = %v, want ErrMemoryWatermarkExceeded", err)
	}

	snap := g.Snapshot()
	if snap.MemoryRejections != 2 || snap.DiskRejections != 1 || !snap.MemoryTripped || snap.DiskTripped {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
}

func TestGuardRealRuntimeAndDiskSamplingConcurrent(t *testing.T) {
	dir := t.TempDir()
	g := New(Options{
		MaxMemoryBytes:   1 << 40, // 1 TiB ceiling (never tripped)
		DiskPath:         dir,
		MinFreeDiskBytes: 1, // 1 byte floor
		SampleInterval:   time.Millisecond,
	})
	snap := g.Snapshot()
	if snap.MemoryBytes == 0 {
		t.Fatal("expected non-zero real runtime memory sample")
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if err := g.AdmitRead(); err != nil {
					t.Errorf("AdmitRead: %v", err)
					return
				}
				if err := g.AdmitWrite(); err != nil {
					t.Errorf("AdmitWrite: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
