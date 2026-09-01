//go:build linux

package seglog

import (
	"encoding/binary"
	"testing"
)

// This complements the high-run-count codec/write allocation gate by keeping
// the real Linux fdatasync syscall in the measured operation.
func TestPersistWaveRealFdatasyncZeroAlloc(t *testing.T) {
	e, err := CreateEngineAuthenticated(t.TempDir(), [16]byte{1}, [32]byte{2}, 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if err = e.Reserve(4096, 32, 32); err != nil {
		t.Fatal(err)
	}
	if err = e.ReserveGroup(1, 32); err != nil {
		t.Fatal(err)
	}
	entries := []Entry{{Index: 1, Term: 1, Data: []byte("fdatasync")}}
	wave := Wave{ID: waveID(1), Batches: []ReadyBatch{{GroupID: 1, Entries: entries}}}
	if err = e.PersistWave(wave); err != nil {
		t.Fatal(err)
	}
	var runErr error
	n := uint64(1)
	allocs := testing.AllocsPerRun(5, func() {
		n++
		binary.LittleEndian.PutUint64(wave.ID[:8], n)
		entries[0].Index = n
		runErr = e.PersistWave(wave)
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	if allocs != 0 {
		t.Fatalf("real-fdatasync PersistWave allocations=%v want=0", allocs)
	}
}
