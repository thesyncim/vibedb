package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	sessionViewSink     SessionView
	sessionSlotViewSink SessionSlotView
	sessionCodecErrSink error
)

func sessionCodecID(seed byte) replication.ID128 {
	var id replication.ID128
	for index := range id {
		id[index] = seed + byte(index)
	}
	return id
}

func sessionCodecFingerprint(seed byte) replication.Digest {
	var digest replication.Digest
	for index := range digest {
		digest[index] = seed + byte(index)
	}
	return digest
}

func sessionCodecRecord() SessionRecord {
	return SessionRecord{
		Tenant:            []byte("tenant-a"),
		ClientID:          sessionCodecID(11),
		ClientEpoch:       7,
		RetryHome:         replication.RetryHome{0, 1, 2, 3, 4, 5, 6, 7},
		AckThrough:        11,
		HighSequence:      14,
		Status:            SessionActive,
		RetryWindow:       16,
		PhysicalSlotCount: 14,
	}
}

func sessionCodecSlot(t testing.TB) SessionSlot {
	t.Helper()
	record := sessionCodecRecord()
	fingerprint := sessionCodecFingerprint(101)
	return SessionSlot{
		Slot:                   13,
		SessionDigest:          SessionKey(record.Tenant, record.ClientID),
		ClientEpoch:            record.ClientEpoch,
		ClientSequence:         14,
		AppliedSequence:        19,
		Fingerprint:            fingerprint,
		LogicalCommandDigest:   sha256.Sum256([]byte("stable-logical-command")),
		ResultCode:             ResultApplied,
		ReplicaSetVersion:      3,
		ActivePolicyGeneration: 4,
		ProtectionEpoch:        5,
		RoutingVersion:         6,
		RouteGeneration:        7,
	}
}

func TestSessionRecordRoundTripBorrowedAndAllocationFree(t *testing.T) {
	record := sessionCodecRecord()
	encoded, err := AppendSessionRecord(nil, record)
	if err != nil {
		t.Fatalf("AppendSessionRecord: %v", err)
	}
	if len(encoded) > MaxSessionRecordBytes {
		t.Fatalf("encoded bytes = %d", len(encoded))
	}
	view, err := OpenSessionRecord(encoded)
	if err != nil {
		t.Fatalf("OpenSessionRecord: %v", err)
	}
	if view.Digest != SessionKey(record.Tenant, record.ClientID) ||
		!bytes.Equal(view.Tenant, record.Tenant) || view.ClientID != record.ClientID ||
		view.ClientEpoch != record.ClientEpoch || view.RetryHome != record.RetryHome ||
		view.AckThrough != record.AckThrough || view.HighSequence != record.HighSequence ||
		view.Status != record.Status || view.RetryWindow != record.RetryWindow ||
		view.PhysicalSlotCount != record.PhysicalSlotCount {
		t.Fatalf("round trip view = %+v", view)
	}
	if cap(view.Tenant) != len(view.Tenant) || cap(view.Bytes()) != len(encoded) ||
		&view.Tenant[0] != &encoded[sessionRecordHeaderBytes] ||
		&view.Bytes()[0] != &encoded[0] {
		t.Fatal("decoded session does not expose capacity-clamped borrowed bytes")
	}

	allocations := testing.AllocsPerRun(1000, func() {
		sessionViewSink, sessionCodecErrSink = OpenSessionRecord(encoded)
	})
	if sessionCodecErrSink != nil || allocations != 0 {
		t.Fatalf("OpenSessionRecord allocations=%v err=%v", allocations, sessionCodecErrSink)
	}

	boundary := record
	boundary.Status = SessionRetired
	boundary.HighSequence = ^uint64(0)
	boundary.AckThrough = boundary.HighSequence - 1
	boundary.RetryWindow = MaxSessionRetryWindow
	boundary.PhysicalSlotCount = MaxSessionRetryWindow
	boundaryBytes, err := AppendSessionRecord(nil, boundary)
	if err != nil {
		t.Fatalf("AppendSessionRecord boundary: %v", err)
	}
	boundaryView, err := OpenSessionRecord(boundaryBytes)
	if err != nil || boundaryView.Status != SessionRetired ||
		boundaryView.RetryWindow != MaxSessionRetryWindow ||
		boundaryView.PhysicalSlotCount != MaxSessionRetryWindow {
		t.Fatalf("boundary session = %+v,%v", boundaryView, err)
	}
	exhausted := boundary
	exhausted.Status = SessionActive
	if _, err := AppendSessionRecord(nil, exhausted); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("active terminal session = %v, want ErrSessionCorrupt", err)
	}
}

func TestSessionSlotRoundTripBorrowedAndAllocationFree(t *testing.T) {
	slot := sessionCodecSlot(t)
	encoded, err := AppendSessionSlot(nil, slot)
	if err != nil {
		t.Fatalf("AppendSessionSlot: %v", err)
	}
	if len(encoded) != MaxSessionSlotRecordBytes {
		t.Fatalf("encoded bytes = %d, want %d", len(encoded), MaxSessionSlotRecordBytes)
	}
	view, err := OpenSessionSlot(encoded)
	if err != nil {
		t.Fatalf("OpenSessionSlot: %v", err)
	}
	if view.Slot != slot.Slot || view.SessionDigest != slot.SessionDigest ||
		view.ClientEpoch != slot.ClientEpoch || view.ClientSequence != slot.ClientSequence ||
		view.AppliedSequence != slot.AppliedSequence ||
		view.Fingerprint != slot.Fingerprint ||
		view.LogicalCommandDigest != slot.LogicalCommandDigest ||
		view.ResultCode != slot.ResultCode ||
		view.ReplicaSetVersion != slot.ReplicaSetVersion ||
		view.ActivePolicyGeneration != slot.ActivePolicyGeneration ||
		view.ProtectionEpoch != slot.ProtectionEpoch ||
		view.RoutingVersion != slot.RoutingVersion ||
		view.RouteGeneration != slot.RouteGeneration {
		t.Fatalf("round trip view = %+v", view)
	}
	if cap(view.Bytes()) != len(encoded) || &view.Bytes()[0] != &encoded[0] {
		t.Fatal("decoded slot does not expose capacity-clamped borrowed bytes")
	}

	allocations := testing.AllocsPerRun(1000, func() {
		sessionSlotViewSink, sessionCodecErrSink = OpenSessionSlot(encoded)
	})
	if sessionCodecErrSink != nil || allocations != 0 {
		t.Fatalf("OpenSessionSlot allocations=%v err=%v", allocations, sessionCodecErrSink)
	}
}

func TestSessionKeysGoldenAndBounds(t *testing.T) {
	digest := SessionKey([]byte("tenant"), sessionCodecID(9))
	const want = "5dc75da7b53d9acda39520cbf89cb7a71bdfaba3a76a84a41e8dee27eb8bfefe"
	if got := hex.EncodeToString(digest[:]); got != want {
		t.Fatalf("SessionKey = %s, want %s", got, want)
	}
	if digest == SessionKey([]byte("tenant-2"), sessionCodecID(9)) ||
		digest == SessionKey([]byte("tenant"), sessionCodecID(10)) {
		t.Fatal("distinct session identities collided")
	}
	metadataKey := SessionStorageKey(digest)
	if metadataKey[0] != 1 || !bytes.Equal(metadataKey[1:], digest[:]) {
		t.Fatalf("metadata key = %x", metadataKey)
	}
	slotKey, err := SessionSlotStorageKey(digest, 255)
	if err != nil || slotKey[0] != 2 || !bytes.Equal(slotKey[1:33], digest[:]) ||
		binary.BigEndian.Uint16(slotKey[33:35]) != 255 {
		t.Fatalf("slot key = %x,%v", slotKey, err)
	}
	if _, err := SessionSlotStorageKey(digest, MaxSessionRetryWindow); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("out-of-range slot error = %v", err)
	}
	if _, err := SessionSlotStorageKey([sha256.Size]byte{}, 0); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("zero digest error = %v", err)
	}
}

func TestSessionRecordRejectsTruncationCorruptionAndInvalidInput(t *testing.T) {
	record := sessionCodecRecord()
	encoded, err := AppendSessionRecord(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	for end := 0; end < len(encoded); end++ {
		if _, err := OpenSessionRecord(encoded[:end]); !errors.Is(err, ErrSessionCorrupt) {
			t.Fatalf("truncation %d error = %v", end, err)
		}
	}
	if _, err := OpenSessionRecord(append(bytes.Clone(encoded), 0)); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("trailing byte error = %v", err)
	}
	badChecksum := bytes.Clone(encoded)
	badChecksum[len(badChecksum)-1] ^= 1
	if _, err := OpenSessionRecord(badChecksum); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("checksum error = %v", err)
	}

	for name, mutate := range map[string]func([]byte){
		"status":   func(candidate []byte) { candidate[20] = 9 },
		"reserved": func(candidate []byte) { candidate[21] = 1 },
		"digest":   func(candidate []byte) { candidate[48] ^= 1 },
		"zero-high-water": func(candidate []byte) {
			clear(candidate[88:104])
		},
		"ack": func(candidate []byte) {
			binary.LittleEndian.PutUint64(candidate[88:96], record.HighSequence)
		},
		"retirement-seal": func(candidate []byte) {
			candidate[20] = byte(SessionRetired)
		},
		"window": func(candidate []byte) {
			binary.LittleEndian.PutUint16(candidate[22:24], MaxSessionRetryWindow+1)
		},
		"zero-slots": func(candidate []byte) {
			clear(candidate[24:26])
		},
		"missing-live-slot": func(candidate []byte) {
			binary.LittleEndian.PutUint16(candidate[24:26], uint16(record.HighSequence-1))
		},
		"slots": func(candidate []byte) {
			binary.LittleEndian.PutUint16(candidate[24:26], record.RetryWindow+1)
		},
		"body-length": func(candidate []byte) {
			binary.LittleEndian.PutUint32(candidate[16:20], uint32(len(record.Tenant)+1))
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := bytes.Clone(encoded)
			mutate(candidate)
			sealRecord(candidate, sessionRecordChecksumDomain)
			if _, err := OpenSessionRecord(candidate); !errors.Is(err, ErrSessionCorrupt) {
				t.Fatalf("OpenSessionRecord error = %v", err)
			}
		})
	}

	invalid := record
	invalid.RetryWindow = MaxSessionRetryWindow + 1
	prefix := []byte("unchanged")
	got, err := AppendSessionRecord(prefix, invalid)
	if !errors.Is(err, ErrSessionCorrupt) || !bytes.Equal(got, prefix) {
		t.Fatalf("invalid append = %q,%v", got, err)
	}
}

func TestSessionSlotRejectsTruncationCorruptionAndInvalidInput(t *testing.T) {
	slot := sessionCodecSlot(t)
	encoded, err := AppendSessionSlot(nil, slot)
	if err != nil {
		t.Fatal(err)
	}
	for end := 0; end < len(encoded); end++ {
		if _, err := OpenSessionSlot(encoded[:end]); !errors.Is(err, ErrSessionCorrupt) {
			t.Fatalf("truncation %d error = %v", end, err)
		}
	}
	if _, err := OpenSessionSlot(append(bytes.Clone(encoded), 0)); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("trailing byte error = %v", err)
	}
	badChecksum := bytes.Clone(encoded)
	badChecksum[len(badChecksum)-1] ^= 1
	if _, err := OpenSessionSlot(badChecksum); !errors.Is(err, ErrSessionCorrupt) {
		t.Fatalf("checksum error = %v", err)
	}

	for name, mutate := range map[string]func([]byte){
		"slot": func(candidate []byte) {
			binary.LittleEndian.PutUint16(candidate[16:18], MaxSessionRetryWindow)
		},
		"reserved":       func(candidate []byte) { candidate[18] = 1 },
		"session-digest": func(candidate []byte) { clear(candidate[20:52]) },
		"sequence": func(candidate []byte) {
			binary.LittleEndian.PutUint64(candidate[60:68], 0)
		},
		"applied": func(candidate []byte) {
			binary.LittleEndian.PutUint64(candidate[68:76], 1)
		},
		"fingerprint":      func(candidate []byte) { clear(candidate[76:108]) },
		"logical-digest":   func(candidate []byte) { clear(candidate[108:140]) },
		"result":           func(candidate []byte) { clear(candidate[140:144]) },
		"replica-set":      func(candidate []byte) { clear(candidate[144:152]) },
		"active-policy":    func(candidate []byte) { clear(candidate[152:160]) },
		"protection":       func(candidate []byte) { clear(candidate[160:168]) },
		"routing":          func(candidate []byte) { clear(candidate[168:176]) },
		"route-generation": func(candidate []byte) { clear(candidate[176:184]) },
		"tail-reserved":    func(candidate []byte) { candidate[184] = 1 },
		"total-length": func(candidate []byte) {
			binary.LittleEndian.PutUint32(candidate[12:16], uint32(len(candidate)+1))
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := bytes.Clone(encoded)
			mutate(candidate)
			sealRecord(candidate, sessionSlotChecksumDomain)
			if _, err := OpenSessionSlot(candidate); !errors.Is(err, ErrSessionCorrupt) {
				t.Fatalf("OpenSessionSlot error = %v", err)
			}
		})
	}

	invalid := slot
	invalid.Slot = MaxSessionRetryWindow
	prefix := []byte("unchanged")
	got, err := AppendSessionSlot(prefix, invalid)
	if !errors.Is(err, ErrSessionCorrupt) || !bytes.Equal(got, prefix) {
		t.Fatalf("invalid append = %q,%v", got, err)
	}
}

func TestSessionCodecRejectsWritableAppendAliases(t *testing.T) {
	record := sessionCodecRecord()
	recordBytes := sessionRecordHeaderBytes + len(record.Tenant) + recordChecksumLen
	recordPrefix := []byte("prefix")
	recordBacking := make([]byte, len(recordPrefix), len(recordPrefix)+recordBytes)
	copy(recordBacking, recordPrefix)
	recordExpanded := recordBacking[:cap(recordBacking)]
	record.Tenant = recordExpanded[len(recordPrefix) : len(recordPrefix)+len(record.Tenant)]
	copy(record.Tenant, "tenant-a")
	gotRecord, err := AppendSessionRecord(recordBacking, record)
	if !errors.Is(err, ErrCodecAlias) || !bytes.Equal(gotRecord, recordPrefix) {
		t.Fatalf("aliased session append = %q,%v", gotRecord, err)
	}

	// An alias into the old destination is safe when append must relocate.
	relocatingTenant := append([]byte(nil), []byte("tenant-a")...)
	relocatingTenant = relocatingTenant[:len(relocatingTenant):len(relocatingTenant)]
	relocatingRecord := sessionCodecRecord()
	relocatingRecord.Tenant = relocatingTenant
	relocatedRecord, err := AppendSessionRecord(relocatingTenant, relocatingRecord)
	if err != nil {
		t.Fatalf("relocating session append: %v", err)
	}
	if _, err := OpenSessionRecord(relocatedRecord[len(relocatingTenant):]); err != nil {
		t.Fatalf("open relocated session: %v", err)
	}

	relocatingSlot := sessionCodecSlot(t)
	relocatingPrefix := []byte("prefix")
	relocatedSlot, err := AppendSessionSlot(relocatingPrefix, relocatingSlot)
	if err != nil {
		t.Fatalf("relocating slot append: %v", err)
	}
	if _, err := OpenSessionSlot(relocatedSlot[len(relocatingPrefix):]); err != nil {
		t.Fatalf("open relocated slot: %v", err)
	}
}
