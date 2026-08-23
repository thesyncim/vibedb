package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/replication"
)

const (
	sessionRecordCodecFormat = uint16(1)
	sessionRecordHeaderBytes = 128
	sessionSlotHeaderBytes   = 192

	MaxSessionRecordBytes = sessionRecordHeaderBytes +
		replication.MaxIdentityBytes + recordChecksumLen
	// Session slots are fixed width: all variable identities live once in the
	// immutable machine binding or compact session header. The retained result
	// is reconstructed canonically at lookup time.
	MaxSessionSlotRecordBytes = sessionSlotHeaderBytes + recordChecksumLen
)

var (
	ErrSessionCorrupt = errors.New("replicatedstate: corrupt session record")

	sessionRecordMagic = [8]byte{'V', 'D', 'B', 'S', 'E', 'S', 0, 0}
	sessionSlotMagic   = [8]byte{'V', 'D', 'B', 'S', 'L', 'T', 0, 0}

	sessionKeyDomain            = []byte("vibedb/replicated-state/session-key\x00")
	sessionRecordChecksumDomain = []byte(
		"vibedb/replicated-state/session-record-checksum\x00",
	)
	sessionSlotChecksumDomain = []byte(
		"vibedb/replicated-state/session-slot-checksum\x00",
	)
)

// SessionStatus records whether the current client epoch may accept another
// sequence. Zero and unknown values are invalid on disk.
type SessionStatus uint8

const (
	SessionActive SessionStatus = iota + 1
	SessionRetired
)

// SessionRecord is the compact control record for one stable
// (tenant, client ID) identity. ClientEpoch is deliberately a value rather than
// part of the storage key: retaining its high-water prevents an old epoch from
// becoming new again after release. PhysicalSlotCount counts populated ring
// keys in this exact epoch and never exceeds RetryWindow. A new epoch is opened
// only after release has removed the prior header and every slot.
type SessionRecord struct {
	Tenant            []byte
	ClientID          replication.ID128
	ClientEpoch       uint64
	RetryHome         replication.RetryHome
	AckThrough        uint64
	HighSequence      uint64
	Status            SessionStatus
	RetryWindow       uint16
	PhysicalSlotCount uint16
}

// SessionView is a strictly validated borrowed session record. Tenant and
// Bytes alias the OpenSessionRecord input, are capacity-clamped, and remain
// valid only while that input remains immutable.
type SessionView struct {
	Digest            [sha256.Size]byte
	Tenant            []byte
	ClientID          replication.ID128
	ClientEpoch       uint64
	RetryHome         replication.RetryHome
	AckThrough        uint64
	HighSequence      uint64
	Status            SessionStatus
	RetryWindow       uint16
	PhysicalSlotCount uint16
	raw               []byte
}

// Bytes returns the exact validated envelope as a capacity-clamped borrowed
// view.
func (v SessionView) Bytes() []byte { return v.raw[:len(v.raw):len(v.raw)] }

// SessionKey derives the sole session digest from its collision-verifiable
// identity. Epoch, retry home, acknowledgements, and results are verified
// values and intentionally do not create alternate session keys.
func SessionKey(tenant []byte, clientID replication.ID128) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(sessionKeyDomain)
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(tenant)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(tenant)
	binary.LittleEndian.PutUint64(length[:], uint64(len(clientID)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(clientID[:])
	var digest [sha256.Size]byte
	_ = h.Sum(digest[:0])
	return digest
}

// SessionStorageKey returns {1} || digest.
func SessionStorageKey(digest [sha256.Size]byte) [1 + sha256.Size]byte {
	var key [1 + sha256.Size]byte
	key[0] = 1
	copy(key[1:], digest[:])
	return key
}

// SessionSlotStorageKey returns {2} || digest || uint16(slot). The slot is
// encoded big-endian so a session's physical slots retain ordinal key order.
func SessionSlotStorageKey(
	digest [sha256.Size]byte,
	slot uint16,
) ([1 + sha256.Size + 2]byte, error) {
	if digest == ([sha256.Size]byte{}) || slot >= MaxSessionRetryWindow {
		return [1 + sha256.Size + 2]byte{}, fmt.Errorf(
			"%w: invalid session slot key", ErrSessionCorrupt,
		)
	}
	var key [1 + sha256.Size + 2]byte
	key[0] = 2
	copy(key[1:1+sha256.Size], digest[:])
	binary.BigEndian.PutUint16(key[1+sha256.Size:], slot)
	return key, nil
}

// AppendSessionRecord appends one strict raw binary session record. On error
// dst is unchanged. Tenant must not overlap the writable append region in
// dst's current backing array; aliases into an old backing array are safe when
// append relocates.
func AppendSessionRecord(dst []byte, record SessionRecord) ([]byte, error) {
	if err := validateSessionRecord(record); err != nil {
		return dst, err
	}
	total := sessionRecordHeaderBytes + len(record.Tenant) + recordChecksumLen
	region := writableAppendRegion(dst, total)
	if byteSlicesOverlap(region, record.Tenant) {
		return dst, ErrCodecAlias
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	digest := SessionKey(record.Tenant, record.ClientID)

	copy(frame[0:8], sessionRecordMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], sessionRecordCodecFormat)
	binary.LittleEndian.PutUint16(frame[10:12], sessionRecordHeaderBytes)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(total))
	binary.LittleEndian.PutUint32(frame[16:20], uint32(len(record.Tenant)))
	frame[20] = byte(record.Status)
	binary.LittleEndian.PutUint16(frame[22:24], record.RetryWindow)
	binary.LittleEndian.PutUint16(frame[24:26], record.PhysicalSlotCount)
	binary.LittleEndian.PutUint16(frame[26:28], uint16(len(record.Tenant)))
	copy(frame[32:48], record.ClientID[:])
	copy(frame[48:80], digest[:])
	binary.LittleEndian.PutUint64(frame[80:88], record.ClientEpoch)
	binary.LittleEndian.PutUint64(frame[88:96], record.AckThrough)
	binary.LittleEndian.PutUint64(frame[96:104], record.HighSequence)
	copy(frame[104:112], record.RetryHome[:])
	copy(frame[sessionRecordHeaderBytes:], record.Tenant)
	sealRecord(frame, sessionRecordChecksumDomain)
	return dst, nil
}

// OpenSessionRecord validates and borrows one complete raw session record. It
// allocates nothing for valid input.
func OpenSessionRecord(src []byte) (SessionView, error) {
	if len(src) < sessionRecordHeaderBytes+1+recordChecksumLen ||
		len(src) > MaxSessionRecordBytes {
		return SessionView{}, fmt.Errorf("%w: session length", ErrSessionCorrupt)
	}
	if !bytes.Equal(src[0:8], sessionRecordMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != sessionRecordCodecFormat ||
		binary.LittleEndian.Uint16(src[10:12]) != sessionRecordHeaderBytes ||
		binary.LittleEndian.Uint32(src[12:16]) != uint32(len(src)) ||
		src[21] != 0 || !allZero(src[28:32]) || !allZero(src[112:sessionRecordHeaderBytes]) ||
		!verifySessionChecksum(src, sessionRecordChecksumDomain) {
		return SessionView{}, fmt.Errorf("%w: session envelope", ErrSessionCorrupt)
	}
	tenantBytes := int(binary.LittleEndian.Uint16(src[26:28]))
	bodyBytes := int(binary.LittleEndian.Uint32(src[16:20]))
	if tenantBytes == 0 || tenantBytes > replication.MaxIdentityBytes ||
		bodyBytes != tenantBytes ||
		sessionRecordHeaderBytes+tenantBytes+recordChecksumLen != len(src) {
		return SessionView{}, fmt.Errorf("%w: session body lengths", ErrSessionCorrupt)
	}

	view := SessionView{
		Tenant:            src[sessionRecordHeaderBytes : sessionRecordHeaderBytes+tenantBytes : sessionRecordHeaderBytes+tenantBytes],
		ClientEpoch:       binary.LittleEndian.Uint64(src[80:88]),
		AckThrough:        binary.LittleEndian.Uint64(src[88:96]),
		HighSequence:      binary.LittleEndian.Uint64(src[96:104]),
		Status:            SessionStatus(src[20]),
		RetryWindow:       binary.LittleEndian.Uint16(src[22:24]),
		PhysicalSlotCount: binary.LittleEndian.Uint16(src[24:26]),
		raw:               src[:len(src):len(src)],
	}
	copy(view.ClientID[:], src[32:48])
	copy(view.Digest[:], src[48:80])
	copy(view.RetryHome[:], src[104:112])
	if err := validateSessionView(view); err != nil {
		return SessionView{}, err
	}
	return view, nil
}

func validateSessionRecord(record SessionRecord) error {
	view := SessionView{
		Digest:            SessionKey(record.Tenant, record.ClientID),
		Tenant:            record.Tenant,
		ClientID:          record.ClientID,
		ClientEpoch:       record.ClientEpoch,
		RetryHome:         record.RetryHome,
		AckThrough:        record.AckThrough,
		HighSequence:      record.HighSequence,
		Status:            record.Status,
		RetryWindow:       record.RetryWindow,
		PhysicalSlotCount: record.PhysicalSlotCount,
	}
	return validateSessionView(view)
}

func validateSessionView(view SessionView) error {
	minimumSlots := uint64(view.RetryWindow)
	if view.HighSequence < minimumSlots {
		minimumSlots = view.HighSequence
	}
	if len(view.Tenant) == 0 || len(view.Tenant) > replication.MaxIdentityBytes ||
		view.ClientID == (replication.ID128{}) || view.ClientEpoch == 0 ||
		view.Digest == ([sha256.Size]byte{}) ||
		view.HighSequence == 0 || view.AckThrough >= view.HighSequence ||
		view.RetryWindow == 0 ||
		view.RetryWindow > MaxSessionRetryWindow ||
		view.PhysicalSlotCount == 0 || view.PhysicalSlotCount > view.RetryWindow ||
		uint64(view.PhysicalSlotCount) != minimumSlots ||
		(view.Status != SessionActive && view.Status != SessionRetired) {
		return fmt.Errorf("%w: invalid session semantics", ErrSessionCorrupt)
	}
	if view.Status == SessionRetired && view.AckThrough != view.HighSequence-1 {
		return fmt.Errorf("%w: invalid session retirement seal", ErrSessionCorrupt)
	}
	if view.Status == SessionActive && view.HighSequence == math.MaxUint64 {
		return fmt.Errorf("%w: active session exhausted sequence space", ErrSessionCorrupt)
	}
	if view.Digest != SessionKey(view.Tenant, view.ClientID) {
		return fmt.Errorf("%w: session identity digest", ErrSessionCorrupt)
	}
	return nil
}

// SessionSlot is one exact retained result in a session's fixed physical ring.
// LogicalCommandDigest must be stable across non-semantic transport metadata;
// it is the authoritative exact-request discriminator paired with Fingerprint.
type SessionSlot struct {
	Slot                   uint16
	SessionDigest          [sha256.Size]byte
	ClientEpoch            uint64
	ClientSequence         uint64
	AppliedSequence        uint64
	Fingerprint            replication.Digest
	LogicalCommandDigest   [sha256.Size]byte
	ResultCode             uint32
	ReplicaSetVersion      uint64
	ActivePolicyGeneration uint64
	ProtectionEpoch        uint64
	RoutingVersion         uint64
	RouteGeneration        uint64
}

// SessionSlotView is a strictly validated fixed-width slot. Bytes aliases the
// decoder input and is capacity-clamped.
type SessionSlotView struct {
	Slot                   uint16
	SessionDigest          [sha256.Size]byte
	ClientEpoch            uint64
	ClientSequence         uint64
	AppliedSequence        uint64
	Fingerprint            replication.Digest
	LogicalCommandDigest   [sha256.Size]byte
	ResultCode             uint32
	ReplicaSetVersion      uint64
	ActivePolicyGeneration uint64
	ProtectionEpoch        uint64
	RoutingVersion         uint64
	RouteGeneration        uint64
	raw                    []byte
}

// Bytes returns the exact validated slot envelope as a borrowed view.
func (v SessionSlotView) Bytes() []byte { return v.raw[:len(v.raw):len(v.raw)] }

// AppendSessionSlot appends one strict fixed-width raw binary ring slot. On
// error dst is unchanged. With sufficient capacity it allocates zero.
func AppendSessionSlot(dst []byte, slot SessionSlot) ([]byte, error) {
	if err := validateSessionSlot(slot); err != nil {
		return dst, err
	}
	const total = MaxSessionSlotRecordBytes
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]

	copy(frame[0:8], sessionSlotMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], sessionRecordCodecFormat)
	binary.LittleEndian.PutUint16(frame[10:12], sessionSlotHeaderBytes)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(total))
	binary.LittleEndian.PutUint16(frame[16:18], slot.Slot)
	copy(frame[20:52], slot.SessionDigest[:])
	binary.LittleEndian.PutUint64(frame[52:60], slot.ClientEpoch)
	binary.LittleEndian.PutUint64(frame[60:68], slot.ClientSequence)
	binary.LittleEndian.PutUint64(frame[68:76], slot.AppliedSequence)
	copy(frame[76:108], slot.Fingerprint[:])
	copy(frame[108:140], slot.LogicalCommandDigest[:])
	binary.LittleEndian.PutUint32(frame[140:144], slot.ResultCode)
	binary.LittleEndian.PutUint64(frame[144:152], slot.ReplicaSetVersion)
	binary.LittleEndian.PutUint64(frame[152:160], slot.ActivePolicyGeneration)
	binary.LittleEndian.PutUint64(frame[160:168], slot.ProtectionEpoch)
	binary.LittleEndian.PutUint64(frame[168:176], slot.RoutingVersion)
	binary.LittleEndian.PutUint64(frame[176:184], slot.RouteGeneration)
	sealRecord(frame, sessionSlotChecksumDomain)
	return dst, nil
}

// OpenSessionSlot validates and borrows one complete raw session slot. It
// allocates nothing for valid input.
func OpenSessionSlot(src []byte) (SessionSlotView, error) {
	if len(src) != MaxSessionSlotRecordBytes {
		return SessionSlotView{}, fmt.Errorf("%w: session slot length", ErrSessionCorrupt)
	}
	if !bytes.Equal(src[0:8], sessionSlotMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != sessionRecordCodecFormat ||
		binary.LittleEndian.Uint16(src[10:12]) != sessionSlotHeaderBytes ||
		binary.LittleEndian.Uint32(src[12:16]) != uint32(len(src)) ||
		!allZero(src[18:20]) || !allZero(src[184:sessionSlotHeaderBytes]) ||
		!verifySessionChecksum(src, sessionSlotChecksumDomain) {
		return SessionSlotView{}, fmt.Errorf("%w: session slot envelope", ErrSessionCorrupt)
	}

	view := SessionSlotView{
		Slot:                   binary.LittleEndian.Uint16(src[16:18]),
		ClientEpoch:            binary.LittleEndian.Uint64(src[52:60]),
		ClientSequence:         binary.LittleEndian.Uint64(src[60:68]),
		AppliedSequence:        binary.LittleEndian.Uint64(src[68:76]),
		ResultCode:             binary.LittleEndian.Uint32(src[140:144]),
		ReplicaSetVersion:      binary.LittleEndian.Uint64(src[144:152]),
		ActivePolicyGeneration: binary.LittleEndian.Uint64(src[152:160]),
		ProtectionEpoch:        binary.LittleEndian.Uint64(src[160:168]),
		RoutingVersion:         binary.LittleEndian.Uint64(src[168:176]),
		RouteGeneration:        binary.LittleEndian.Uint64(src[176:184]),
		raw:                    src[:len(src):len(src)],
	}
	copy(view.SessionDigest[:], src[20:52])
	copy(view.Fingerprint[:], src[76:108])
	copy(view.LogicalCommandDigest[:], src[108:140])
	if err := validateSessionSlotView(view); err != nil {
		return SessionSlotView{}, err
	}
	return view, nil
}

func validateSessionSlot(slot SessionSlot) error {
	return validateSessionSlotView(SessionSlotView{
		Slot:                   slot.Slot,
		SessionDigest:          slot.SessionDigest,
		ClientEpoch:            slot.ClientEpoch,
		ClientSequence:         slot.ClientSequence,
		AppliedSequence:        slot.AppliedSequence,
		Fingerprint:            slot.Fingerprint,
		LogicalCommandDigest:   slot.LogicalCommandDigest,
		ResultCode:             slot.ResultCode,
		ReplicaSetVersion:      slot.ReplicaSetVersion,
		ActivePolicyGeneration: slot.ActivePolicyGeneration,
		ProtectionEpoch:        slot.ProtectionEpoch,
		RoutingVersion:         slot.RoutingVersion,
		RouteGeneration:        slot.RouteGeneration,
	})
}

func validateSessionSlotView(view SessionSlotView) error {
	if view.Slot >= MaxSessionRetryWindow ||
		view.SessionDigest == ([sha256.Size]byte{}) || view.ClientEpoch == 0 ||
		view.ClientSequence == 0 || view.AppliedSequence < 2 ||
		view.Fingerprint == (replication.Digest{}) ||
		view.LogicalCommandDigest == ([sha256.Size]byte{}) ||
		view.ResultCode < ResultApplied || view.ResultCode > ResultSessionOpened ||
		view.ReplicaSetVersion == 0 || view.ActivePolicyGeneration == 0 ||
		view.ProtectionEpoch == 0 || view.RoutingVersion == 0 ||
		view.RouteGeneration == 0 ||
		(view.ClientSequence == 1) != (view.ResultCode == ResultSessionOpened) ||
		view.ResultCode == ResultSessionOpened && view.AppliedSequence != view.ClientEpoch ||
		view.ResultCode != ResultSessionOpened && view.AppliedSequence <= view.ClientEpoch {
		return fmt.Errorf("%w: invalid session slot semantics", ErrSessionCorrupt)
	}
	return nil
}

// verifySessionChecksum is verifyRecord's allocation-free borrowed-decode
// form. It deliberately uses the same SHA-256 framing and domains as
// sealRecord.
func verifySessionChecksum(frame, domain []byte) bool {
	if len(frame) < recordChecksumLen {
		return false
	}
	h := sha256.New()
	_, _ = h.Write(domain)
	_, _ = h.Write(frame[:len(frame)-recordChecksumLen])
	var want [sha256.Size]byte
	_ = h.Sum(want[:0])
	return bytes.Equal(want[:], frame[len(frame)-recordChecksumLen:])
}
