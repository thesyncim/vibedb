package raftstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"os"
	"testing"
)

func testFormatState(t *testing.T) (headerState, normalizedOptions, currentState, []byte, currentState, []byte, currentState, []byte) {
	t.Helper()
	options, err := normalizeOptions(testFormatOptions())
	if err != nil {
		t.Fatal(err)
	}
	_, header, err := marshalStaticHeader(testIdentity(), testKey(), testBootstrap(), options)
	if err != nil {
		t.Fatal(err)
	}
	bootstrapPayload, _, _ := marshalBootstrap(testBootstrap(), 1)
	record, digest, _, err := marshalRecord(recordKindBootstrap, 0, 1, 0, 0, header.headerDigest, bootstrapPayload, header, options)
	if err != nil {
		t.Fatal(err)
	}
	first := initialCurrent(header, HeaderBytes+int64(len(record)), 1, digest)
	firstBytes, _, err := marshalCurrentSlot(first, 0, header)
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.generation = 2
	second.activeSlot = 1
	second.currentIncarnation = 1
	secondBytes, _, err := marshalCurrentSlot(second, 1, header)
	if err != nil {
		t.Fatal(err)
	}
	third := second
	third.generation = 3
	third.activeSlot = 0
	third.currentIncarnation = 2
	thirdBytes, _, err := marshalCurrentSlot(third, 0, header)
	if err != nil {
		t.Fatal(err)
	}
	return header, options, first, firstBytes, second, secondBytes, third, thirdBytes
}

func TestEveryShortCurrentSlotWriteFallsBackToAuthenticatedSlot(t *testing.T) {
	header, options, _, oldBytes, second, secondBytes, _, newBytes := testFormatState(t)
	file, err := os.CreateTemp(t.TempDir(), "slots")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(HeaderBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(secondBytes, StaticHeaderBytes+CurrentSlotBytes); err != nil {
		t.Fatal(err)
	}
	for prefix := 1; prefix < CurrentSlotBytes; prefix++ {
		mixed := append([]byte(nil), oldBytes...)
		copy(mixed[:prefix], newBytes[:prefix])
		if _, err := file.WriteAt(mixed, StaticHeaderBytes); err != nil {
			t.Fatal(err)
		}
		selected, _, err := recoverCurrent(file, header, options)
		if err != nil || selected.generation != second.generation {
			t.Fatalf("prefix %d selected generation %d: %v", prefix, selected.generation, err)
		}
	}
}

func TestCRCValidCurrentAuthenticationCorruptionNeverRollsBack(t *testing.T) {
	header, options, _, _, _, secondBytes, _, thirdBytes := testFormatState(t)
	file, err := os.CreateTemp(t.TempDir(), "slots")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(HeaderBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(secondBytes, StaticHeaderBytes+CurrentSlotBytes); err != nil {
		t.Fatal(err)
	}
	corrupt := append([]byte(nil), thirdBytes...)
	corrupt[currentPrefixBytes+len(header.keyID)+3] ^= 0x80
	sealSectorChecksum(corrupt)
	if _, err := file.WriteAt(corrupt, StaticHeaderBytes); err != nil {
		t.Fatal(err)
	}
	if _, _, err := recoverCurrent(file, header, options); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("authenticated corruption = %v", err)
	}
}

func TestAllZeroInactiveSlotAfterTornRewriteFallsBack(t *testing.T) {
	header, options, _, _, second, secondBytes, _, _ := testFormatState(t)
	file, err := os.CreateTemp(t.TempDir(), "slots")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := file.Truncate(HeaderBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt(secondBytes, StaticHeaderBytes+CurrentSlotBytes); err != nil {
		t.Fatal(err)
	}
	selected, recovered, err := recoverCurrent(file, header, options)
	if err != nil || !recovered || selected.generation != second.generation {
		t.Fatalf("zero inactive selected=%d recovered=%v err=%v", selected.generation, recovered, err)
	}
}

func TestRecordObjectIdentityBindsAllVariableEnvelopeFields(t *testing.T) {
	header, options, current, _, _, _, _, _ := testFormatState(t)
	payload := []byte("same-low-entropy-payload")
	base, _, _, err := marshalRecord(recordKindReady, 0, 2, 1, 1, current.chainDigest, payload, header, options)
	if err != nil {
		t.Fatal(err)
	}
	exact, _, _, err := marshalRecord(recordKindReady, 0, 2, 1, 1, current.chainDigest, payload, header, options)
	if err != nil || !bytes.Equal(base, exact) {
		t.Fatalf("exact record construction changed: %v", err)
	}
	changedPrevious := current.chainDigest
	changedPrevious[0] ^= 1
	tests := []struct {
		name                  string
		kind, flags           uint8
		sequence, incarnation uint64
		readyID               uint64
		previous              [32]byte
		payload               []byte
	}{
		{name: "kind", kind: recordKindBootstrap, sequence: 2, previous: current.chainDigest, payload: payload},
		{name: "flags", kind: recordKindReady, flags: 1, sequence: 2, incarnation: 1, readyID: 1, previous: current.chainDigest, payload: payload},
		{name: "sequence", kind: recordKindReady, sequence: 3, incarnation: 1, readyID: 1, previous: current.chainDigest, payload: payload},
		{name: "incarnation", kind: recordKindReady, sequence: 2, incarnation: 2, readyID: 1, previous: current.chainDigest, payload: payload},
		{name: "ready-id", kind: recordKindReady, sequence: 2, incarnation: 1, readyID: 2, previous: current.chainDigest, payload: payload},
		{name: "previous", kind: recordKindReady, sequence: 2, incarnation: 1, readyID: 1, previous: changedPrevious, payload: payload},
		{name: "same-length-plaintext", kind: recordKindReady, sequence: 2, incarnation: 1, readyID: 1, previous: current.chainDigest, payload: []byte("Same-low-entropy-payload")},
		{name: "lengths-and-plaintext", kind: recordKindReady, sequence: 2, incarnation: 1, readyID: 1, previous: current.chainDigest, payload: append(append([]byte(nil), payload...), '!')},
	}
	baseFingerprint := recordFingerprint(t, base, header)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed, _, _, marshalErr := marshalRecord(test.kind, test.flags, test.sequence, test.incarnation, test.readyID, test.previous, test.payload, header, options)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			assertObjectIdentityChanged(t, baseFingerprint, recordFingerprint(t, changed, header))
		})
	}
}

func TestBootstrapRecordObjectIdentityBindsContextAndPayload(t *testing.T) {
	header, options, _, _, _, _, _, _ := testFormatState(t)
	payload, _, err := marshalBootstrap(testBootstrap(), testIdentity().MemberID)
	if err != nil {
		t.Fatal(err)
	}
	base, _, _, err := marshalRecord(recordKindBootstrap, 0, 1, 0, 0, header.headerDigest, payload, header, options)
	if err != nil {
		t.Fatal(err)
	}
	exact, _, _, err := marshalRecord(recordKindBootstrap, 0, 1, 0, 0, header.headerDigest, payload, header, options)
	if err != nil || !bytes.Equal(base, exact) {
		t.Fatalf("bootstrap exact construction changed: %v", err)
	}
	changedPrevious := header.headerDigest
	changedPrevious[0] ^= 1
	changedSameLength := append([]byte(nil), payload...)
	changedSameLength[len(changedSameLength)-1] ^= 1
	variants := []struct {
		name     string
		sequence uint64
		previous [32]byte
		payload  []byte
	}{
		{name: "sequence", sequence: 2, previous: header.headerDigest, payload: payload},
		{name: "previous", sequence: 1, previous: changedPrevious, payload: payload},
		{name: "same-length-payload", sequence: 1, previous: header.headerDigest, payload: changedSameLength},
		{name: "payload-length", sequence: 1, previous: header.headerDigest, payload: append(append([]byte(nil), payload...), 1)},
	}
	baseFingerprint := recordFingerprint(t, base, header)
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			changed, _, _, marshalErr := marshalRecord(recordKindBootstrap, 0, variant.sequence, 0, 0, variant.previous, variant.payload, header, options)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			assertObjectIdentityChanged(t, baseFingerprint, recordFingerprint(t, changed, header))
		})
	}
}

func TestCurrentSlotObjectIdentityBindsAllPayloadAndSlotContext(t *testing.T) {
	header, _, _, _, baseState, _, _, _ := testFormatState(t)
	baseState.retryPresent = true
	baseState.retry = retryKey{incarnation: baseState.currentIncarnation, readyID: 1}
	baseState.retryDigest = sha256.Sum256([]byte("retry"))
	base, _, err := marshalCurrentSlot(baseState, 1, header)
	if err != nil {
		t.Fatal(err)
	}
	exact, _, err := marshalCurrentSlot(baseState, 1, header)
	if err != nil || !bytes.Equal(base, exact) {
		t.Fatalf("current exact construction changed: %v", err)
	}
	mutations := []struct {
		name   string
		mutate func(*currentState)
	}{
		{name: "wal-end", mutate: func(s *currentState) { s.walEnd += recordDamageGranule }},
		{name: "record-sequence", mutate: func(s *currentState) { s.recordSequence++ }},
		{name: "chain", mutate: func(s *currentState) { s.chainDigest[0] ^= 1 }},
		{name: "incarnation", mutate: func(s *currentState) { s.currentIncarnation++ }},
		{name: "hard-term", mutate: func(s *currentState) {
			s.hard = cloneHardState(s.hard)
			s.hard.Term = uint64Pointer(s.hard.GetTerm() + 1)
		}},
		{name: "hard-vote", mutate: func(s *currentState) { s.hard = cloneHardState(s.hard); s.hard.Vote = uint64Pointer(2) }},
		{name: "hard-commit", mutate: func(s *currentState) {
			s.hard = cloneHardState(s.hard)
			s.hard.Commit = uint64Pointer(s.hard.GetCommit() + 1)
		}},
		{name: "first", mutate: func(s *currentState) { s.first++ }},
		{name: "last", mutate: func(s *currentState) { s.last++ }},
		{name: "retry-presence", mutate: func(s *currentState) { s.retryPresent = false; s.retry = retryKey{}; s.retryDigest = [32]byte{} }},
		{name: "retry-incarnation", mutate: func(s *currentState) { s.retry.incarnation++ }},
		{name: "retry-id", mutate: func(s *currentState) { s.retry.readyID++ }},
		{name: "retry-digest", mutate: func(s *currentState) { s.retryDigest[0] ^= 1 }},
		{name: "snapshot-id", mutate: func(s *currentState) { s.snapshotID[0] ^= 1 }},
		{name: "snapshot-index", mutate: func(s *currentState) { s.snapshotIndex++ }},
		{name: "snapshot-term", mutate: func(s *currentState) { s.snapshotTerm++ }},
		{name: "snapshot-size", mutate: func(s *currentState) { s.snapshotSize++ }},
		{name: "snapshot-chunks", mutate: func(s *currentState) { s.snapshotChunks++ }},
		{name: "snapshot-digest", mutate: func(s *currentState) { s.snapshotDigest[0] ^= 1 }},
		{name: "topology-epoch", mutate: func(s *currentState) { s.topologyRecoveryEpoch++ }},
	}
	baseFingerprint := currentFingerprint(t, base, header)
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			changedState := baseState
			mutation.mutate(&changedState)
			changed, _, marshalErr := marshalCurrentSlot(changedState, 1, header)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			assertObjectIdentityChanged(t, baseFingerprint, currentFingerprint(t, changed, header))
		})
	}
	nextState := baseState
	nextState.generation++
	next, _, err := marshalCurrentSlot(nextState, 0, header)
	if err != nil {
		t.Fatal(err)
	}
	assertObjectIdentityChanged(t, baseFingerprint, currentFingerprint(t, next, header))

	payload, err := marshalCurrentPayload(baseState)
	if err != nil {
		t.Fatal(err)
	}
	leftTag := makeObjectTag(header.nonceKey, "current-slot", baseState.generation, currentTagContext(0, header.fileID), payload)
	rightTag := makeObjectTag(header.nonceKey, "current-slot", baseState.generation, currentTagContext(1, header.fileID), payload)
	left := syntheticFingerprint(header, "current-slot", baseState.generation, leftTag)
	right := syntheticFingerprint(header, "current-slot", baseState.generation, rightTag)
	if left.tag == right.tag || left.key == right.key || left.nonce == right.nonce {
		t.Fatal("slot number is not bound into current object identity")
	}
}

type objectFingerprint struct {
	tag        [32]byte
	nonce      [12]byte
	key        [32]byte
	ciphertext []byte
}

func recordFingerprint(t *testing.T, record []byte, header headerState) objectFingerprint {
	t.Helper()
	var result objectFingerprint
	copy(result.nonce[:], record[88:100])
	copy(result.tag[:], record[100:132])
	sequence := binary.LittleEndian.Uint64(record[32:40])
	result.key = deriveObjectKey(header.dataKey, "wal-record", sequence, result.tag)
	start := recordPrefixBytes + len(header.keyID)
	end := start + int(binary.LittleEndian.Uint32(record[24:28]))
	result.ciphertext = append([]byte(nil), record[start:end]...)
	return result
}

func currentFingerprint(t *testing.T, slot []byte, header headerState) objectFingerprint {
	t.Helper()
	var result objectFingerprint
	copy(result.nonce[:], slot[32:44])
	copy(result.tag[:], slot[64:96])
	generation := binary.LittleEndian.Uint64(slot[24:32])
	result.key = deriveObjectKey(header.dataKey, "current-slot", generation, result.tag)
	start := currentPrefixBytes + len(header.keyID)
	end := start + int(binary.LittleEndian.Uint32(slot[16:20]))
	result.ciphertext = append([]byte(nil), slot[start:end]...)
	return result
}

func syntheticFingerprint(header headerState, domain string, sequence uint64, tag [32]byte) objectFingerprint {
	return objectFingerprint{tag: tag, nonce: deriveObjectNonce(header.nonceKey, domain, sequence, tag), key: deriveObjectKey(header.dataKey, domain, sequence, tag)}
}

func assertObjectIdentityChanged(t *testing.T, before, after objectFingerprint) {
	t.Helper()
	if before.tag == after.tag || before.nonce == after.nonce || before.key == after.key || bytes.Equal(before.ciphertext, after.ciphertext) {
		t.Fatalf("object identity did not change completely: tag=%v nonce=%v key=%v ciphertext=%v", before.tag != after.tag, before.nonce != after.nonce, before.key != after.key, !bytes.Equal(before.ciphertext, after.ciphertext))
	}
}

func TestFormatByteGolden(t *testing.T) {
	options, err := normalizeOptions(testFormatOptions())
	if err != nil {
		t.Fatal(err)
	}
	static, header, err := marshalStaticHeader(testIdentity(), testKey(), testBootstrap(), options)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, _, _ := marshalBootstrap(testBootstrap(), 1)
	record, digest, _, err := marshalRecord(recordKindBootstrap, 0, 1, 0, 0, header.headerDigest, bootstrap, header, options)
	if err != nil {
		t.Fatal(err)
	}
	current := initialCurrent(header, HeaderBytes+int64(len(record)), 1, digest)
	slot, _, err := marshalCurrentSlot(current, 0, header)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{digestHex(static), digestHex(record), digestHex(slot)}
	// Filled from the deterministic encoder; changing any byte is a format change.
	want := []string{"8ea70ff1044428d10888243a2ecc48b15599fdc98661046641183a83ee7f18f4", "bfc4f386a60d7bd1b226448897afb3a764f672d111c3e5dbcd9f189c2e272a76", "d8759c5b741b65d41aa5cac714fc0b3322ca4a03debe985830672a828ac26fba"}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("golden %d = %s", index, got[index])
		}
	}
}

func testFormatOptions() Options {
	options := testOptions()
	options.MaxFileBytes = 32 << 20
	options.MaxRecordBytes = 4 << 20
	options.MaxLiveBytes = 8 << 20
	options.allowSmallBounds = true
	return options
}

func digestHex(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
