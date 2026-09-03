//go:build linux

package seglog

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

func TestLinuxRecoveryRestoresActiveReservationBeforeStartupSync(t *testing.T) {
	for _, cut := range []string{"torn tail", "already truncated", "complete prefix"} {
		t.Run(cut, func(t *testing.T) {
			dir, path, capacity, offset := linuxReservationRecoveryFixture(t)
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			switch cut {
			case "torn tail":
				_, err = file.WriteAt([]byte{0xff}, offset)
			case "already truncated":
				// Simulate a restart crash after truncation but before restoring
				// KEEP_SIZE extents. The next replay has no tail to truncate.
				err = file.Truncate(offset)
			}
			if err := errors.Join(err, file.Close()); err != nil {
				t.Fatal(err)
			}
			physical := reservePhysicalFile
			allocations := 0
			reservePhysicalFile = func(file *os.File, capacity uint64) error {
				allocations++
				return physical(file, capacity)
			}
			t.Cleanup(func() { reservePhysicalFile = physical })
			synced := false
			recovered, err := openTestEngineWithSync(dir, func(file *os.File) error {
				if err := verifyPhysicalAllocation(file, capacity, offset); err != nil {
					return err
				}
				synced = true
				return file.Sync()
			})
			if err != nil {
				t.Fatal(err)
			}
			defer recovered.Close()
			if !synced || recovered.Sequence() != 1 {
				t.Fatalf("startup boundary lost: synced=%t sequence=%d", synced, recovered.Sequence())
			}
			want := 1
			if cut == "complete prefix" {
				want = 0
			}
			if allocations != want {
				t.Fatalf("cold recovery allocations=%d want=%d", allocations, want)
			}
		})
	}
}

func TestLinuxRecoveryRejectsMissingActiveReservation(t *testing.T) {
	for _, failure := range []error{syscall.ENOSPC, nil} {
		t.Run("allocation "+errorLabel(failure), func(t *testing.T) {
			dir, path, _, offset := linuxReservationRecoveryFixture(t)
			if err := os.Truncate(path, offset); err != nil {
				t.Fatal(err)
			}
			physical := reservePhysicalFile
			reservePhysicalFile = func(*os.File, uint64) error { return failure }
			t.Cleanup(func() { reservePhysicalFile = physical })
			synced := false
			got, err := openTestEngineWithSync(dir, func(*os.File) error { synced = true; return nil })
			want := failure
			if want == nil {
				want = ErrBounds
			}
			if got != nil || !errors.Is(err, want) || synced {
				if got != nil {
					_ = got.Close()
				}
				t.Fatalf("under-reserved writer exposed: store=%v err=%v synced=%t", got != nil, err, synced)
			}
			reservePhysicalFile = physical
			recovered, err := openTestEngine(dir)
			if err != nil {
				t.Fatalf("allocation failure damaged retry: %v", err)
			}
			if err := recovered.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func linuxReservationRecoveryFixture(t *testing.T) (dir, path string, capacity uint64, offset int64) {
	t.Helper()
	dir = t.TempDir()
	engine := newEngineAt(t, dir, 1)
	if err := engine.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{
		{GroupID: 1, Entries: []Entry{{Index: 1, Term: 1, Data: []byte("durable")}}},
	}}); err != nil {
		t.Fatal(err)
	}
	path = segmentPath(dir, engine.log.state.ActiveFileID)
	capacity, offset = engine.log.state.SegmentCapacity, int64(engine.log.activeOffset)
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, path, capacity, offset
}

func errorLabel(err error) string {
	if err == nil {
		return "short"
	}
	return err.Error()
}
