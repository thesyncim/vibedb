package seglog

import (
	"bytes"
	"reflect"
	"testing"
)

func TestCheckpointGroupSummaryCompactCanonicalRoundTrip(t *testing.T) {
	group := checkpointGroupSummary{GroupID: 91, Summary: sealedRunSummary{
		LastIndex: 44, LastTerm: 7,
		Hard:          HardState{Term: 7, Vote: 3, Commit: 42},
		TruncateIndex: 12, TruncateTerm: 2,
		Checkpoint:      Checkpoint{ID: [16]byte{1}, Index: 12, Term: 2},
		NodeIncarnation: 9, ReadyID: 11, ReadyDigest: [16]byte{2}, ReadyWaveID: WaveID{3},
		LatestWaveID: WaveID{4}, LatestWaveDigest: [32]byte{5}, LatestWaveSequence: 77,
	}}
	var storage [256]byte
	encoded, err := appendCheckpointGroupSummary(storage[:0], 80, group)
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewBuffer(encoded)
	got, err := readCheckpointGroupSummary(reader, 80)
	if err != nil || reader.Len() != 0 || !reflect.DeepEqual(got, group) {
		t.Fatalf("got=%+v bytes=%d err=%v", got, reader.Len(), err)
	}
	if len(encoded) >= 200 {
		t.Fatalf("compact summary encoded %d bytes", len(encoded))
	}
	for cut := range encoded {
		if _, err = readCheckpointGroupSummary(bytes.NewBuffer(encoded[:cut]), 80); err == nil {
			t.Fatalf("torn summary cut=%d accepted", cut)
		}
	}
}

func TestCheckpointGroupSummaryRejectsNoncanonicalVarint(t *testing.T) {
	// Group delta 1 encoded in two bytes is noncanonical.
	if _, err := readCheckpointGroupSummary(bytes.NewBuffer([]byte{0x81, 0x00, 0, 0, 0}), 0); err == nil {
		t.Fatal("noncanonical group delta accepted")
	}
}
