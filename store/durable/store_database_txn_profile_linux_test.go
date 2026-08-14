//go:build linux

package durable

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/storeio"
)

func TestTxnLogEnsureMintedEagerSealedProfile(t *testing.T) {
	const capacity = uint64(64 * storeio.TxnMarkerMinSectorSize)
	options := TxnLogOptions{Capacity: capacity, SealedCapacity: true}
	log, err := NewTxnLog(t.TempDir(), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })

	if err := log.EnsureMinted(); err != nil {
		if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
			t.Skipf("filesystem cannot prove strict sidecar allocation: %v", err)
		}
		t.Fatal(err)
	}
	if log.marker == nil {
		t.Fatal("EnsureMinted did not retain the sealed marker")
	}
	header := log.marker.Header()
	if !header.SealedCapacity || header.Capacity != capacity {
		t.Fatalf("sealed marker header = %+v", header)
	}
	if got := log.Options(); got != options {
		t.Fatalf("retained options = %+v, want %+v", got, options)
	}
}
