package replicatedstate

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func fenceCodecState() State {
	state := codecState()
	state.Applied = 10
	state.LastKind = RecordNormal
	state.SessionCount = 1
	state.SessionSlotCount = 4
	state.AuthorityBindingCount = 1
	return state
}

func TestStateFenceRoundTripAndCompactGeometry(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  func(*State)
	}{
		{"origin_only", func(s *State) { s.FenceOriginDigest[0] = 1 }},
		{"current_fence", func(s *State) { s.FenceOriginDigest[0], s.FenceApplied = 1, 10 }},
		{"history", func(s *State) {
			s.FenceOriginDigest[0], s.FenceApplied = 1, 7
			s.HistoricalFenceCount, s.HistoricalFenceSlots = 2, 3
		}},
		{"unfenced_only", func(s *State) { s.UnfencedSessionSlots = 4 }},
		{"combined", func(s *State) {
			s.FenceOriginDigest[0], s.FenceApplied = 1, 7
			s.HistoricalFenceCount, s.HistoricalFenceSlots, s.UnfencedSessionSlots = 2, 3, 1
			s.RelationPlacementDigest[0] = 9
			s.RequestLedgerRows, s.RequestLedgerResidentBytes = 1, 100
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := fenceCodecState()
			tc.set(&state)
			encoded, err := AppendState(nil, state)
			if err != nil {
				t.Fatal(err)
			}
			if got := binary.LittleEndian.Uint16(encoded[12:14]); got != stateFenceHeaderBytes {
				t.Fatalf("header = %d, want %d", got, stateFenceHeaderBytes)
			}
			if !bytes.Equal(encoded[536:568], state.FenceOriginDigest[:]) {
				t.Fatal("incorrect fence origin bytes")
			}
			for i, want := range []uint64{state.FenceApplied, state.HistoricalFenceCount,
				state.HistoricalFenceSlots, state.UnfencedSessionSlots} {
				if got := binary.LittleEndian.Uint64(encoded[568+i*8 : 576+i*8]); got != want {
					t.Fatalf("fence field %d = %d, want %d", i, got, want)
				}
			}
			decoded, err := OpenState(encoded)
			if err != nil || !equalState(decoded, state) {
				t.Fatalf("round trip state = %+v, err = %v", decoded, err)
			}
			reencoded, err := AppendState(nil, decoded)
			if err != nil || !bytes.Equal(encoded, reencoded) {
				t.Fatalf("noncanonical round trip: %v", err)
			}
		})
	}

	for _, placement := range []bool{false, true} {
		state := fenceCodecState()
		wantHeader := stateHeaderBytes
		if placement {
			state.RelationPlacementDigest[0] = 1
			wantHeader = stateRelationPlacementHeaderBytes
		}
		encoded, err := AppendState(nil, state)
		if err != nil {
			t.Fatal(err)
		}
		if got := binary.LittleEndian.Uint16(encoded[12:14]); int(got) != wantHeader {
			t.Fatalf("zero fence header = %d, want %d", got, wantHeader)
		}
		decoded, err := OpenState(encoded)
		if err != nil || !equalState(decoded, state) || stateHasFence(decoded) {
			t.Fatalf("zero fence round trip = %+v, err = %v", decoded, err)
		}
	}
}

func TestStateFenceRejectsInvalidCounters(t *testing.T) {
	valid := fenceCodecState()
	valid.FenceOriginDigest[0], valid.FenceApplied = 1, 7
	valid.HistoricalFenceCount, valid.HistoricalFenceSlots, valid.UnfencedSessionSlots = 2, 3, 1
	encoded, err := AppendState(nil, valid)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		offset int
		value  uint64
		set    func(*State)
	}{
		{"future_applied", 568, 11, func(s *State) { s.FenceApplied = 11 }},
		{"history_without_applied", 568, 0, func(s *State) { s.FenceApplied = 0 }},
		{"count_exceeds_slots", 576, 4, func(s *State) { s.HistoricalFenceCount = 4 }},
		{"history_exceeds_total", 584, 5, func(s *State) { s.HistoricalFenceSlots = 5 }},
		{"combined_exceeds_total", 592, 2, func(s *State) { s.UnfencedSessionSlots = 2 }},
		{"history_overflow", 584, math.MaxUint64, func(s *State) { s.HistoricalFenceSlots = math.MaxUint64 }},
		{"unfenced_overflow", 592, math.MaxUint64, func(s *State) { s.UnfencedSessionSlots = math.MaxUint64 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invalid := valid
			tc.set(&invalid)
			dst := []byte("prefix")
			got, err := AppendState(dst, invalid)
			if !errors.Is(err, ErrStateCorrupt) || !bytes.Equal(got, dst) {
				t.Fatalf("invalid encode = %x, %v", got, err)
			}
			bad := bytes.Clone(encoded)
			binary.LittleEndian.PutUint64(bad[tc.offset:tc.offset+8], tc.value)
			sealRecord(bad, stateChecksumDomain)
			if _, err := OpenState(bad); !errors.Is(err, ErrStateCorrupt) {
				t.Fatalf("invalid decode = %v", err)
			}
		})
	}
	for _, history := range []bool{false, true} {
		invalid := valid
		invalid.FenceOriginDigest = [32]byte{}
		if !history {
			invalid.HistoricalFenceCount, invalid.HistoricalFenceSlots = 0, 0
		}
		if _, err := AppendState(nil, invalid); !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("missing origin (history %t) encode error = %v", history, err)
		}
		bad := bytes.Clone(encoded)
		clear(bad[536:568])
		if !history {
			clear(bad[576:592])
		}
		sealRecord(bad, stateChecksumDomain)
		if _, err := OpenState(bad); !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("missing origin (history %t) decode error = %v", history, err)
		}
	}
}

func TestStateFenceRejectsMalformedExtension(t *testing.T) {
	state := fenceCodecState()
	state.FenceOriginDigest[0] = 1
	// A preceding populated extension must not make an empty fence extension
	// canonical: the shorter relation-placement envelope is its unique encoding.
	state.RelationPlacementDigest[0] = 1
	encoded, err := AppendState(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	empty := bytes.Clone(encoded)
	clear(empty[536:600])
	sealRecord(empty, stateChecksumDomain)
	if _, err := OpenState(empty); !errors.Is(err, ErrStateCorrupt) {
		t.Fatalf("empty fence extension error = %v", err)
	}
	for n := 0; n < len(encoded); n++ {
		if _, err := OpenState(encoded[:n]); !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("truncated length %d error = %v", n, err)
		}
	}
	for _, header := range []uint16{537, 599, 601} {
		bad := bytes.Clone(encoded)
		binary.LittleEndian.PutUint16(bad[12:14], header)
		sealRecord(bad, stateChecksumDomain)
		if _, err := OpenState(bad); !errors.Is(err, ErrStateCorrupt) {
			t.Fatalf("unknown header %d error = %v", header, err)
		}
	}
}

func TestStateFenceDoesNotAddCodecAllocations(t *testing.T) {
	base := fenceCodecState()
	fenced := base
	fenced.FenceOriginDigest[0], fenced.FenceApplied = 1, 7
	fenced.HistoricalFenceCount, fenced.HistoricalFenceSlots, fenced.UnfencedSessionSlots = 2, 3, 1
	measure := func(state State) (float64, float64) {
		buf := make([]byte, 0, MaxStateEnvelopeBytes)
		encode := testing.AllocsPerRun(100, func() {
			var err error
			buf, err = AppendState(buf[:0], state)
			if err != nil {
				t.Fatal(err)
			}
		})
		decode := testing.AllocsPerRun(100, func() {
			if _, err := OpenState(buf); err != nil {
				t.Fatal(err)
			}
		})
		return encode, decode
	}
	baseEncode, baseDecode := measure(base)
	fenceEncode, fenceDecode := measure(fenced)
	if fenceEncode != baseEncode || fenceDecode != baseDecode {
		t.Fatalf("fence allocations encode/decode = %g/%g, baseline = %g/%g",
			fenceEncode, fenceDecode, baseEncode, baseDecode)
	}
}

func TestStateFenceSystemRowCount(t *testing.T) {
	state := fenceCodecState()
	base, ok := stateSystemRowCount(state)
	if !ok {
		t.Fatal("invalid base row count")
	}
	state.HistoricalFenceCount = 2
	state.HistoricalFenceSlots = 3
	state.UnfencedSessionSlots = 1
	if got, ok := stateSystemRowCount(state); !ok || got != base+2 {
		t.Fatalf("historical row count = %d, %t, want %d, true", got, ok, base+2)
	}
	// Exercise arithmetic independently of the stricter envelope bounds.
	state.HistoricalFenceCount = math.MaxUint64 - base
	if got, ok := stateSystemRowCount(state); !ok || got != math.MaxUint64 {
		t.Fatalf("maximum row count = %d, %t", got, ok)
	}
	state.HistoricalFenceCount++
	if _, ok := stateSystemRowCount(state); ok {
		t.Fatal("accepted overflowing historical row count")
	}
}
