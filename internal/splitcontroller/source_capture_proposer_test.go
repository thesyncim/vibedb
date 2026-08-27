package splitcontroller

import (
	"bytes"
	"testing"
)

func TestSourceCaptureAuthorityIsStableAndDistinctFromPrune(t *testing.T) {
	operation := OperationID{1, 2, 3}
	client := SourceCaptureClientID(operation)
	if client == ([16]byte{}) || client != SourceCaptureClientID(operation) ||
		client == RetainedPruneClientID(operation) {
		t.Fatalf("capture client=%x prune=%x", client, RetainedPruneClientID(operation))
	}
	tenant := SourceCaptureTenant(operation)
	if len(tenant) == 0 || !bytes.Equal(tenant, SourceCaptureTenant(operation)) ||
		bytes.Equal(tenant, RetainedPruneTenant(operation)) {
		t.Fatalf("capture tenant=%q prune=%q", tenant, RetainedPruneTenant(operation))
	}
	changed := operation
	changed[0]++
	if SourceCaptureClientID(changed) == client ||
		bytes.Equal(SourceCaptureTenant(changed), tenant) {
		t.Fatal("operation identity did not separate capture authority")
	}
}
