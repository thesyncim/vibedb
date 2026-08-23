package replicatedstate

import (
	"testing"
	"unsafe"
)

func TestScannedSessionScratchExcludesRetryWindowBitmap(t *testing.T) {
	const wantScratchBytes = uintptr(56)
	if got := unsafe.Sizeof(scannedSession{}); got != wantScratchBytes {
		t.Fatalf("scannedSession scratch = %d bytes, want %d", got, wantScratchBytes)
	}

	const wantRemovedBitmapBytes = uintptr(32)
	if got := unsafe.Sizeof([MaxSessionRetryWindow / 64]uint64{}); got != wantRemovedBitmapBytes {
		t.Fatalf("retry-window bitmap = %d bytes, want %d", got, wantRemovedBitmapBytes)
	}
}
