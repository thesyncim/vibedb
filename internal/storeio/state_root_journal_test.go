package storeio

import (
	"bytes"
	"testing"
)

// TestStateRootJournalIDRoundTrip locks the recovery-journal pairing field into
// the state-root payload: it must round-trip, occupy the first 16 bytes of the
// former reserve, and leave that reserve all-zero when unused so existing golden
// roots decode identically.
func TestStateRootJournalIDRoundTrip(t *testing.T) {
	layout, err := MutableStoreLayout(format0PageSize)
	if err != nil {
		t.Fatal(err)
	}
	var journalID [16]byte
	for i := range journalID {
		journalID[i] = byte(0xC0 + i)
	}

	root := format0EmptyState(9, format0PageSize)
	root.JournalID = journalID
	page := make([]byte, format0PageSize)
	encoded, err := encodeTestStateRootPayload(page, root, layout.DataStart)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := decodeTestStateRootPayload(encoded, root, layout.DataStart)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.JournalID != journalID {
		t.Fatalf("JournalID round trip mismatch: %x != %x", decoded.JournalID, journalID)
	}

	// The field occupies exactly the first 16 bytes of the former reserve.
	payload := encoded
	if !bytes.Equal(payload[stateRootJournalIDOffset:stateRootJournalIDEnd], journalID[:]) {
		t.Fatalf("JournalID not at expected offset [%d:%d]", stateRootJournalIDOffset, stateRootJournalIDEnd)
	}
	if !allZero(payload[stateRootReservedOffset:]) {
		t.Fatal("reserve past JournalID must remain zero")
	}

	// A store with no journal keeps those bytes zero, so pre-journal golden roots
	// are byte-identical.
	zeroRoot := format0EmptyState(9, format0PageSize)
	zeroPage := make([]byte, format0PageSize)
	zeroEncoded, err := encodeTestStateRootPayload(zeroPage, zeroRoot, layout.DataStart)
	if err != nil {
		t.Fatalf("encode zero: %v", err)
	}
	zeroPayload := zeroEncoded
	if !allZero(zeroPayload[stateRootJournalIDOffset:]) {
		t.Fatal("unused JournalID region must be zero")
	}
}
