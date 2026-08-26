package replicatedstate

import (
	"encoding/binary"
	"errors"
	"testing"

	pb "go.etcd.io/raft/v3/raftpb"
)

func TestStateRequestLedgerAccountingRoundTripAndCompactGeometry(t *testing.T) {
	compact, err := AppendState(nil, codecState())
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint16(compact[12:14]); got != stateHeaderBytes {
		t.Fatalf("empty ledger header = %d, want %d", got, stateHeaderBytes)
	}

	state := codecState()
	state.Applied = 3
	state.LastTerm = 2
	state.LastKind = RecordNormal
	state.LastEntryType = pb.EntryNormal
	state.LastEntryDigest[0] ^= 0x7f
	state.RequestLedgerRows = 3
	state.RequestLedgerResidentBytes = 1024
	state.RequestLedgerReservedBytes = 2048
	state.RequestLedgerAckRows = 1
	state.RequestLedgerAckBytes = 326
	encoded, err := AppendState(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.LittleEndian.Uint16(encoded[12:14]); got != stateRequestLedgerHeaderBytes {
		t.Fatalf("ledger header = %d, want %d", got, stateRequestLedgerHeaderBytes)
	}
	if got := binary.LittleEndian.Uint64(encoded[416:424]); got != state.RequestLedgerRows {
		t.Fatalf("ledger rows = %d, want %d", got, state.RequestLedgerRows)
	}
	if got := binary.LittleEndian.Uint64(encoded[424:432]); got != state.RequestLedgerResidentBytes {
		t.Fatalf("ledger bytes = %d, want %d", got, state.RequestLedgerResidentBytes)
	}
	if got := binary.LittleEndian.Uint64(encoded[432:440]); got != state.RequestLedgerReservedBytes {
		t.Fatalf("ledger reserved bytes = %d, want %d", got, state.RequestLedgerReservedBytes)
	}
	if got := binary.LittleEndian.Uint64(encoded[440:448]); got != state.RequestLedgerAckRows {
		t.Fatalf("ledger ack rows = %d, want %d", got, state.RequestLedgerAckRows)
	}
	if got := binary.LittleEndian.Uint64(encoded[448:456]); got != state.RequestLedgerAckBytes {
		t.Fatalf("ledger ack bytes = %d, want %d", got, state.RequestLedgerAckBytes)
	}
	decoded, err := OpenState(encoded)
	if err != nil || !equalState(decoded, state) {
		t.Fatalf("OpenState = %+v, %v", decoded, err)
	}

	missingBytes := state
	missingBytes.RequestLedgerResidentBytes = 0
	if _, err := AppendState(nil, missingBytes); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("rows without resident bytes error = %v", err)
	}
	missingRows := state
	missingRows.RequestLedgerRows = 0
	if _, err := AppendState(nil, missingRows); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("resident bytes without rows error = %v", err)
	}
	tooSmall := state
	tooSmall.RequestLedgerResidentBytes = tooSmall.RequestLedgerRows * 37
	if _, err := AppendState(nil, tooSmall); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("impossible resident bytes error = %v", err)
	}
}
