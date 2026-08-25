package replicatedstate

import (
	"testing"
	"unsafe"
)

func TestScannedSessionScratchExcludesRetryWindowBitmap(t *testing.T) {
	// Exact stable-identity verification adds only packed-arena offsets, length,
	// and ClientID; tenant bytes are not repeated in each map value.
	const wantScratchBytes = uintptr(88)
	if got := unsafe.Sizeof(scannedSession{}); got != wantScratchBytes {
		t.Fatalf("scannedSession scratch = %d bytes, want %d", got, wantScratchBytes)
	}

	const wantRemovedBitmapBytes = uintptr(32)
	if got := unsafe.Sizeof([MaxSessionRetryWindow / 64]uint64{}); got != wantRemovedBitmapBytes {
		t.Fatalf("retry-window bitmap = %d bytes, want %d", got, wantRemovedBitmapBytes)
	}
}
