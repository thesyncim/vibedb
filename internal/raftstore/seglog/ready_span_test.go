package seglog

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func readySpanFrame(t *testing.T, span, readyID uint64) []byte {
	t.Helper()
	e := &Engine{
		frameBuf:     make([]byte, 0, 1024),
		eventScratch: make([]segmentEvent, 0, 8),
	}
	wave := Wave{ID: waveID(1), Batches: []ReadyBatch{{
		GroupID:         1,
		NodeIncarnation: 1,
		ReadyID:         readyID,
		ReadySpan:       span,
		ReadyDigest:     [16]byte{2},
	}}}
	frame, _, _, err := e.prepareWave(wave, false)
	if err != nil {
		t.Fatal(err)
	}
	result := bytes.Clone(frame)
	sealWaveHeader(result, 1, wave.ID)
	return result
}

func resealUnauthenticatedFrame(frame []byte) {
	digest := sha256.Sum256(frame[72:])
	copy(frame[40:72], digest[:])
	var id WaveID
	copy(id[:], frame[16:32])
	sealWaveHeader(frame, binary.LittleEndian.Uint64(frame[8:16]), id)
}

func readySpanIdentityOffsets(frame []byte) (version, span, digest int, err error) {
	cursor := canonicalCursor{data: frame[72:]}
	if _, err = cursor.uvarint(); err != nil { // batch count
		return 0, 0, 0, err
	}
	if _, err = cursor.uvarint(); err != nil { // group delta
		return 0, 0, 0, err
	}
	flags, err := cursor.byte()
	if err != nil || flags&batchIdentity == 0 {
		return 0, 0, 0, ErrCorrupt
	}
	if _, err = cursor.uvarint(); err != nil { // incarnation
		return 0, 0, 0, err
	}
	marker := 72 + cursor.off
	if marker >= len(frame) || frame[marker] != 0 {
		return 0, 0, 0, ErrCorrupt
	}
	if _, err = cursor.uvarint(); err != nil { // reserved zero marker
		return 0, 0, 0, err
	}
	version = 72 + cursor.off
	if _, err = cursor.uvarint(); err != nil { // version
		return 0, 0, 0, err
	}
	span = 72 + cursor.off
	if _, err = cursor.uvarint(); err != nil { // span
		return 0, 0, 0, err
	}
	if _, err = cursor.uvarint(); err != nil { // final ReadyID
		return 0, 0, 0, err
	}
	digest = 72 + cursor.off
	return version, span, digest, nil
}

func TestReadySpanZeroAndOneKeepLegacyBytes(t *testing.T) {
	legacy := readySpanFrame(t, 0, 1)
	canonical := readySpanFrame(t, 1, 1)
	if !bytes.Equal(legacy, canonical) {
		t.Fatalf("zero and one span encodings differ:\nzero=%x\none=%x", legacy, canonical)
	}
	wantPayload := []byte{1, 1, batchIdentity, 1, 1}
	wantDigest := [16]byte{2}
	wantPayload = append(wantPayload, wantDigest[:]...)
	wantPayload = append(wantPayload, 0, 0)
	if !bytes.Equal(legacy[72:], wantPayload) {
		t.Fatalf("legacy payload=%x want=%x", legacy[72:], wantPayload)
	}
	_, _, batches, err := decodeWaveFrame(legacy, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(batches) != 1 || batches[0].ReadySpan != 1 || batches[0].ReadyID != 1 {
		t.Fatalf("decoded legacy batch=%+v", batches)
	}
}

func TestReadySpanRecoversFinalStateAndEntries(t *testing.T) {
	dir := t.TempDir()
	e := newEngineAt(t, dir, 1)
	if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{
		GroupID:         1,
		NodeIncarnation: 1,
		ReadyID:         1,
		ReadyDigest:     [16]byte{1},
		Entries:         []Entry{{Index: 1, Term: 1}},
	}}}); err != nil {
		t.Fatal(err)
	}
	finalDigest := [16]byte{9}
	if err := e.PersistWave(Wave{ID: waveID(2), Batches: []ReadyBatch{{
		GroupID:         1,
		NodeIncarnation: 1,
		ReadyID:         4,
		ReadySpan:       3,
		ReadyDigest:     finalDigest,
		Entries:         []Entry{{Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 1}},
	}}}); err != nil {
		t.Fatal(err)
	}
	if err := e.DeepVerify(); err != nil {
		t.Fatalf("active DeepVerify=%v", err)
	}
	if err := e.Rotate(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.WaitSeal(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err := openTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	state, ok := e.Group(1)
	if !ok || e.Sequence() != 2 || len(state.Entries) != 4 || state.NodeIncarnation != 1 || state.ReadyID != 4 || state.ReadyDigest != finalDigest {
		t.Fatalf("recovered sequence=%d state ok=%t state=%+v", e.Sequence(), ok, state)
	}
	for i, entry := range state.Entries {
		if entry.Index != uint64(i+1) || entry.Term != 1 {
			t.Fatalf("entry %d=%+v", i, entry)
		}
	}
	if err := e.DeepVerify(); err != nil {
		t.Fatalf("recovered DeepVerify=%v", err)
	}
}

func TestReadySpanNewIncarnationStartsAtSpan(t *testing.T) {
	g := &engineGroup{GroupState: GroupState{NodeIncarnation: 1, ReadyID: 27}}
	valid := ReadyBatch{NodeIncarnation: 2, ReadyID: 3, ReadySpan: 3, ReadyDigest: [16]byte{1}}
	if _, err := validateBatch(g, &valid); err != nil {
		t.Fatalf("new-incarnation span rejected: %v", err)
	}
	for _, readyID := range []uint64{1, 2, 4, 5} {
		invalid := valid
		invalid.ReadyID = readyID
		if _, err := validateBatch(g, &invalid); !errors.Is(err, ErrRaftState) {
			t.Fatalf("new-incarnation final ReadyID=%d error=%v", readyID, err)
		}
	}
}

func TestReadySpanRejectsGapsRegressionsAndOverflow(t *testing.T) {
	tests := []struct {
		name        string
		current     uint64
		span        uint64
		readyID     uint64
		incarnation uint64
	}{
		{name: "same-incarnation gap", current: 1, span: 3, readyID: 5, incarnation: 1},
		{name: "same-incarnation regression", current: 4, span: 3, readyID: 6, incarnation: 1},
		{name: "overflow", current: ^uint64(0) - 1, span: 3, readyID: 1, incarnation: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := &engineGroup{GroupState: GroupState{NodeIncarnation: 1, ReadyID: test.current}}
			batch := ReadyBatch{NodeIncarnation: test.incarnation, ReadyID: test.readyID, ReadySpan: test.span, ReadyDigest: [16]byte{1}}
			if _, err := validateBatch(g, &batch); !errors.Is(err, ErrRaftState) {
				t.Fatalf("validateBatch=%v", err)
			}
		})
	}

	e := newReservedEngine(t, 1)
	if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{
		GroupID: 1, NodeIncarnation: 1, ReadyID: 1, ReadyDigest: [16]byte{1},
	}}}); err != nil {
		t.Fatal(err)
	}
	beforeOffset, beforeSequence := e.log.activeOffset, e.sequence
	invalid := Wave{ID: waveID(2), Batches: []ReadyBatch{{
		GroupID: 1, NodeIncarnation: 1, ReadyID: 4, ReadySpan: 2, ReadyDigest: [16]byte{2},
	}}}
	if err := e.PersistWave(invalid); !errors.Is(err, ErrRaftState) {
		t.Fatalf("PersistWave gap=%v", err)
	}
	if e.log.activeOffset != beforeOffset || e.sequence != beforeSequence {
		t.Fatalf("rejected gap mutated offset/sequence=%d/%d want=%d/%d", e.log.activeOffset, e.sequence, beforeOffset, beforeSequence)
	}
}

func TestReadySpanCorruptVersionSpanAndDigestRejected(t *testing.T) {
	valid := readySpanFrame(t, 3, 3)
	versionOffset, spanOffset, digestOffset, err := readySpanIdentityOffsets(valid)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		offset int
		value  byte
	}{
		{name: "version", offset: versionOffset, value: 2},
		{name: "zero span", offset: spanOffset, value: 0},
		{name: "one span", offset: spanOffset, value: 1},
		{name: "oversized span", offset: spanOffset, value: byte(maxReadySpan + 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := bytes.Clone(valid)
			bad[test.offset] = test.value
			resealUnauthenticatedFrame(bad)
			if _, _, _, err := decodeWaveFrame(bad, 1); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("decode corrupted %s=%v", test.name, err)
			}
		})
	}

	badDigest := bytes.Clone(valid)
	for i := 0; i < 16; i++ {
		badDigest[digestOffset+i] = 0
	}
	resealUnauthenticatedFrame(badDigest)
	if _, _, _, err := decodeWaveFrame(badDigest, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decode zero ReadyDigest=%v", err)
	}

	badFrameDigest := bytes.Clone(valid)
	badFrameDigest[40] ^= 1
	binary.LittleEndian.PutUint32(badFrameDigest[32:36], crc32.Checksum(badFrameDigest[40:], crcTable))
	binary.LittleEndian.PutUint32(badFrameDigest[36:40], crc32.Checksum(badFrameDigest[:36], crcTable))
	if _, _, _, err := decodeWaveFrame(badFrameDigest, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("decode bad frame digest=%v", err)
	}
}

func TestReadySpanTornFrameDoesNotAdvanceState(t *testing.T) {
	dir := t.TempDir()
	e := newEngineAt(t, dir, 1)
	if err := e.PersistWave(Wave{ID: waveID(1), Batches: []ReadyBatch{{
		GroupID: 1, NodeIncarnation: 1, ReadyID: 1, ReadyDigest: [16]byte{1},
		Entries: []Entry{{Index: 1, Term: 1}},
	}}}); err != nil {
		t.Fatal(err)
	}
	firstOffset := e.log.activeOffset
	frame, _, _, err := e.prepareWave(Wave{ID: waveID(2), Batches: []ReadyBatch{{
		GroupID: 1, NodeIncarnation: 1, ReadyID: 4, ReadySpan: 3, ReadyDigest: [16]byte{9},
		Entries: []Entry{{Index: 2, Term: 1}, {Index: 3, Term: 1}, {Index: 4, Term: 1}},
	}}}, false)
	if err != nil {
		t.Fatal(err)
	}
	frame = bytes.Clone(frame)
	sealWaveHeader(frame, 2, waveID(2))
	cut := len(frame) - 1
	if _, err := e.log.active.WriteAt(frame[:cut], int64(e.log.activeOffset)); err != nil {
		t.Fatal(err)
	}
	if err := e.log.active.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = openTestEngine(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	state, ok := e.Group(1)
	if !ok || state.ReadyID != 1 || len(state.Entries) != 1 || e.log.activeOffset != firstOffset {
		t.Fatalf("torn frame advanced state ok=%t state=%+v offset=%d want=%d", ok, state, e.log.activeOffset, firstOffset)
	}
	if err := e.DeepVerify(); err != nil {
		t.Fatalf("DeepVerify after torn frame=%v", err)
	}
}
