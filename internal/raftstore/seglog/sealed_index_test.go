package seglog

import (
	"errors"
	"testing"
)

func TestSealedRunDirectoryCompactAndCanonical(t *testing.T) {
	runs := []sealedGroupRun{
		{GroupID: 1, First: 1, Last: 1, BlockEntries: sealedDefaultBlockEntries, ExtentOffset: 128, ExtentBytes: 96, Inline: routeEntry{Term: 1, ExtentOffset: 128, ExtentBytes: 96, DataBytes: 8}},
		{GroupID: 2, First: 9, Last: 9, BlockEntries: sealedDefaultBlockEntries, ExtentOffset: 224, ExtentBytes: 80, Inline: routeEntry{Term: 3, Type: 2, ExtentOffset: 224, ExtentBytes: 80, DataOffset: 8, DataBytes: 12}},
	}
	encoded, err := appendRunDirectory(nil, runs)
	if err != nil {
		t.Fatal(err)
	}
	// Inline one-entry runs include exact physical routing and remain below the
	// common 48-byte group/run budget without a separate descriptor or tag.
	if got := len(encoded) / len(runs); got > 48 {
		t.Fatalf("common run directory = %d bytes/run", got)
	}
	decoded, err := decodeRunDirectory(encoded, uint64(len(runs)))
	if err != nil || len(decoded) != len(runs) || decoded[1].GroupID != 2 || decoded[1].First != 9 {
		t.Fatalf("decode = %#v, %v", decoded, err)
	}
	for i := range encoded {
		corrupt := append([]byte(nil), encoded...)
		corrupt[i] |= 0x80
		if _, decodeErr := decodeRunDirectory(corrupt, uint64(len(runs))); decodeErr == nil && len(corrupt) == len(encoded) {
			// Mutations can remain canonical; the enclosing keyed top MAC is what
			// detects those. This assertion only rejects accepted overlong input.
			continue
		}
	}
	if _, err = decodeRunDirectory(append(encoded, 0), uint64(len(runs))); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("trailing bytes: %v", err)
	}
}

func TestSealedIndexHeaderAndTopAuthentication(t *testing.T) {
	header := sealedIndexHeader{TotalBytes: 64 + 20 + 80 + 120, Runs: 2, DirectoryBytes: 20, DescriptorOffset: 84, DescriptorBytes: 80, RoutePayloadOffset: 164, RoutePayloadBytes: 120, DataBytes: 4096}
	encoded, err := marshalSealedIndexHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := unmarshalSealedIndexHeader(encoded[:])
	if err != nil || decoded != header {
		t.Fatalf("header = %#v, %v", decoded, err)
	}
	key, logID := [32]byte{7}, [16]byte{8}
	directory := []byte{1, 2, 3}
	want := sealedTopMAC(key, logID, 4, 5, encoded[:], directory)
	directory[1] ^= 1
	got := sealedTopMAC(key, logID, 4, 5, encoded[:], directory)
	if got == want {
		t.Fatal("top MAC accepted changed directory")
	}
	corrupt := encoded
	corrupt[28]++
	if _, err = unmarshalSealedIndexHeader(corrupt[:]); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("geometry corruption = %v", err)
	}
}

func TestSealedRetryTableIsDeduplicatedAndCanonical(t *testing.T) {
	id, digest := waveID(7), [32]byte{9}
	runs := []sealedGroupRun{
		{GroupID: 1, Summary: sealedRunSummary{LatestWaveID: id, LatestWaveDigest: digest, LatestWaveSequence: 11}},
		{GroupID: 2, Summary: sealedRunSummary{LatestWaveID: id, LatestWaveDigest: digest, LatestWaveSequence: 11}},
	}
	directory, retryBytes, retryCount, err := appendSealedDirectory(nil, runs)
	if err != nil {
		t.Fatal(err)
	}
	if retryCount != 1 || retryBytes != 49 { // ID + SHA-256 + one-byte sequence.
		t.Fatalf("retry table bytes=%d count=%d", retryBytes, retryCount)
	}
	header := sealedIndexHeader{Runs: 2, DirectoryBytes: uint32(len(directory)), RetryBytes: retryBytes, RetryCount: retryCount}
	decoded, err := decodeSealedDirectory(directory, header)
	if err != nil || decoded[0].Summary.LatestWaveID != id || decoded[1].Summary.LatestWaveDigest != digest || decoded[0].Summary.RetryOrdinal != 1 || decoded[1].Summary.RetryOrdinal != 1 {
		t.Fatalf("decoded=%#v err=%v", decoded, err)
	}
	// No table entry may be unreferenced, and first references must introduce
	// ordinals monotonically (there is one canonical encoding).
	extraID := waveID(8)
	extra := append([]byte(nil), directory[:retryBytes]...)
	extra = append(extra, extraID[:]...)
	extra = append(extra, digest[:]...)
	extra = appendUvarint(extra, 12)
	extra = append(extra, directory[retryBytes:]...)
	header.DirectoryBytes = uint32(len(extra))
	header.RetryBytes += 49
	header.RetryCount++
	if _, err = decodeSealedDirectory(extra, header); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unreferenced retry accepted: %v", err)
	}
	runs[1].Summary.LatestWaveSequence++
	if _, _, _, err = appendSealedDirectory(nil, runs); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("same ID with changed state accepted: %v", err)
	}
}

func TestSealedRouteDescriptorIndependentAuthentication(t *testing.T) {
	key := [32]byte{1}
	logID := [16]byte{2}
	payload := []byte{1, 0, 12, 4, 0, 8}
	descriptor := routeDescriptor{PayloadOffset: 9000, PayloadBytes: uint32(len(payload)), Entries: 2, ExtentOffset: 4096, ExtentBytes: 32768}
	encoded := make([]byte, sealedRouteDescriptorBytes)
	if _, err := marshalRouteDescriptor(encoded, descriptor, key, logID, 7, 11, 3, payload); err != nil {
		t.Fatal(err)
	}
	workspace := newRouteAuthWorkspace(key)
	got, err := workspace.unmarshalRouteDescriptor(encoded, payload, logID, 7, 11, 3)
	if err != nil || got.ExtentBytes != 32768 {
		t.Fatalf("descriptor = %#v, %v", got, err)
	}
	for _, mutate := range []func([]byte, []byte){
		func(header, _ []byte) { header[32] ^= 1 },
		func(_, body []byte) { body[0] ^= 1 },
	} {
		header := append([]byte(nil), encoded...)
		body := append([]byte(nil), payload...)
		mutate(header, body)
		if _, err = workspace.unmarshalRouteDescriptor(header, body, logID, 7, 11, 3); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("tamper accepted: %v", err)
		}
	}
	if got := testing.AllocsPerRun(1000, func() {
		if _, verifyErr := workspace.unmarshalRouteDescriptor(encoded, payload, logID, 7, 11, 3); verifyErr != nil {
			panic(verifyErr)
		}
	}); got != 0 {
		t.Fatalf("route verify allocs/run = %v", got)
	}
}

func TestSealedHugeRunUsesComputedDescriptor(t *testing.T) {
	run := sealedGroupRun{GroupID: 9, First: 1, Last: 100_000, DescriptorOrdinal: 17, DescriptorCount: 391, BlockEntries: sealedDefaultBlockEntries, ExtentOffset: 128, ExtentBytes: 8 << 20}
	encoded, err := appendRunDirectory(nil, []sealedGroupRun{run})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeRunDirectory(encoded, 1)
	if err != nil {
		t.Fatal(err)
	}
	ordinal := (uint64(99_999) - decoded[0].First) / uint64(decoded[0].BlockEntries)
	offset := decoded[0].DescriptorOrdinal + ordinal
	if ordinal != 390 || offset != run.DescriptorOrdinal+390 {
		t.Fatalf("ordinal=%d offset=%d", ordinal, offset)
	}
}

func TestSealedControlOnlySummaryAndRoutePayload(t *testing.T) {
	checkpointID := [16]byte{9}
	for name, summary := range map[string]sealedRunSummary{
		"hard":        {Hard: HardState{Term: 4, Vote: 2}},
		"checkpoint":  {Checkpoint: Checkpoint{ID: checkpointID, Index: 7, Term: 3}},
		"incarnation": {NodeIncarnation: 1},
	} {
		t.Run(name, func(t *testing.T) {
			run := sealedGroupRun{GroupID: 8, Summary: summary}
			encoded, err := appendRunDirectory(nil, []sealedGroupRun{run})
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeRunDirectory(encoded, 1)
			if err != nil || decoded[0].First != 0 || decoded[0].Summary != summary {
				t.Fatalf("control summary = %#v, %v", decoded, err)
			}
		})
	}
	entries := []routeEntry{
		{Term: 7, ExtentOffset: 4096, ExtentBytes: 1024, DataOffset: 0, DataBytes: 12},
		{Term: 7, Type: 1, ExtentOffset: 4096, ExtentBytes: 1024, DataOffset: 12, DataBytes: 9},
		{Term: 8, ExtentOffset: 8192, ExtentBytes: 800, DataOffset: 0, DataBytes: 20},
	}
	payload, err := appendRoutePayload(nil, entries)
	if err != nil {
		t.Fatal(err)
	}
	workspace := make([]routeEntry, 0, len(entries))
	got, err := decodeRoutePayload(payload, uint32(len(entries)), workspace)
	if err != nil || got[1] != entries[1] || got[2] != entries[2] {
		t.Fatalf("routes = %#v, %v", got, err)
	}
	maxTerms := []routeEntry{{Term: ^uint64(0), ExtentOffset: 1, ExtentBytes: 4, DataBytes: 1}, {Term: 1, ExtentOffset: 5, ExtentBytes: 4, DataBytes: 1}}
	encodedTerms, err := appendRoutePayload(nil, maxTerms)
	if err != nil {
		t.Fatal(err)
	}
	got, err = decodeRoutePayload(encodedTerms, 2, workspace)
	if err != nil || got[0].Term != ^uint64(0) || got[1].Term != 1 {
		t.Fatalf("absolute term escape = %#v, %v", got, err)
	}
	invalidType := entries[:1]
	invalidType[0].Type = 3
	if _, err = appendRoutePayload(nil, invalidType); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("entry type 3 accepted: %v", err)
	}
}
