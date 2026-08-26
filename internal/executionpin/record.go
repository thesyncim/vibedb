package executionpin

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
)

const RecordBytes = 576

var (
	recordMagic          = [8]byte{'V', 'E', 'P', 'R', 'E', 'C', 0, 0}
	recordChecksumDomain = []byte("vibedb/execution-pin/record-checksum\x00")
)

type Status uint8

const (
	StatusActive Status = iota + 1
	StatusReleased
	StatusExpired
)

// Record is the compact durable lifecycle. Immutable acquisition facts and
// the latest lease/terminal facts are sufficient to reconstruct transferable
// certificates without retaining their larger encoded forms.
type Record struct {
	Status                 Status
	LastOperation          Operation
	PinID                  PinID
	Binding                Binding
	AcquireAuthorityDigest Digest
	CurrentAuthorityDigest Digest

	AcquireApplied         uint64
	AcquireController      ID
	AcquireControllerEpoch uint64
	AcquireLeaseDeadline   int64

	Controller      ID
	ControllerEpoch uint64
	LeaseDeadline   int64
	LeaseRevision   uint64
	LeaseApplied    uint64

	TerminalApplied         uint64
	TerminalAuthorityDigest Digest
	PrepareTerminalDigest   Digest
	TerminalObserved        int64

	LastCommandDigest Digest
	LastApplied       uint64
}

func (record Record) Valid() bool {
	derived, err := DerivePinID(record.Binding)
	if err != nil || record.PinID != derived || record.AcquireAuthorityDigest == (Digest{}) ||
		record.CurrentAuthorityDigest == (Digest{}) ||
		record.Status < StatusActive || record.Status > StatusExpired ||
		record.LastOperation < OperationAcquire || record.LastOperation > OperationExpire ||
		record.Controller == (ID{}) || record.ControllerEpoch == 0 || record.LeaseDeadline <= 0 ||
		record.LastCommandDigest == (Digest{}) || record.LastApplied == 0 {
		return false
	}
	acquired := record.AcquireApplied != 0
	if acquired {
		if record.AcquireController == (ID{}) || record.AcquireControllerEpoch == 0 ||
			record.AcquireLeaseDeadline <= 0 || record.LeaseRevision == 0 ||
			record.LeaseApplied < record.AcquireApplied {
			return false
		}
	} else if record.AcquireController != (ID{}) || record.AcquireControllerEpoch != 0 ||
		record.AcquireLeaseDeadline != 0 || record.LeaseRevision != 0 || record.LeaseApplied != 0 {
		return false
	}
	switch record.Status {
	case StatusActive:
		if !acquired || record.TerminalApplied != 0 ||
			record.TerminalAuthorityDigest != (Digest{}) {
			return false
		}
		if record.PrepareTerminalDigest != (Digest{}) || record.TerminalObserved != 0 {
			return false
		}
		if record.LastOperation == OperationAcquire {
			return record.LastApplied == record.AcquireApplied
		}
		return (record.LastOperation == OperationRenew || record.LastOperation == OperationRecover) &&
			record.LastApplied == record.LeaseApplied
	case StatusReleased:
		return acquired && record.TerminalApplied >= record.AcquireApplied &&
			record.TerminalAuthorityDigest != (Digest{}) &&
			record.PrepareTerminalDigest != (Digest{}) && record.TerminalObserved == 0 &&
			record.LastOperation == OperationRelease && record.LastApplied == record.TerminalApplied
	case StatusExpired:
		return record.TerminalApplied != 0 && record.PrepareTerminalDigest == (Digest{}) &&
			record.TerminalAuthorityDigest != (Digest{}) &&
			record.TerminalObserved >= record.LeaseDeadline &&
			record.LastOperation == OperationExpire && record.LastApplied == record.TerminalApplied
	default:
		return false
	}
}

func AppendRecord(dst []byte, record Record) ([]byte, error) {
	if !record.Valid() {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, RecordBytes)...)
	frame := dst[start:]
	copy(frame[0:8], recordMagic[:])
	frame[8], frame[9] = byte(record.Status), byte(record.LastOperation)
	copy(frame[16:48], record.PinID[:])
	encodedBinding := appendBinding(frame[48:48], record.Binding)
	if len(encodedBinding) != bindingBytes {
		panic("executionpin: impossible binding geometry")
	}
	copy(frame[256:288], record.AcquireAuthorityDigest[:])
	copy(frame[288:320], record.CurrentAuthorityDigest[:])
	binary.LittleEndian.PutUint64(frame[320:328], record.AcquireApplied)
	copy(frame[328:344], record.AcquireController[:])
	binary.LittleEndian.PutUint64(frame[344:352], record.AcquireControllerEpoch)
	binary.LittleEndian.PutUint64(frame[352:360], uint64(record.AcquireLeaseDeadline))
	copy(frame[360:376], record.Controller[:])
	binary.LittleEndian.PutUint64(frame[376:384], record.ControllerEpoch)
	binary.LittleEndian.PutUint64(frame[384:392], uint64(record.LeaseDeadline))
	binary.LittleEndian.PutUint64(frame[392:400], record.LeaseRevision)
	binary.LittleEndian.PutUint64(frame[400:408], record.LeaseApplied)
	binary.LittleEndian.PutUint64(frame[408:416], record.TerminalApplied)
	copy(frame[416:448], record.PrepareTerminalDigest[:])
	binary.LittleEndian.PutUint64(frame[448:456], uint64(record.TerminalObserved))
	copy(frame[456:488], record.LastCommandDigest[:])
	binary.LittleEndian.PutUint64(frame[488:496], record.LastApplied)
	copy(frame[496:528], record.TerminalAuthorityDigest[:])
	sealSHA(frame, recordChecksumDomain)
	return dst, nil
}

func OpenRecord(raw []byte) (Record, error) {
	if len(raw) != RecordBytes || !bytes.Equal(raw[0:8], recordMagic[:]) ||
		!allZero(raw[10:16]) || !allZero(raw[528:544]) ||
		!verifySHA(raw, recordChecksumDomain) {
		return Record{}, ErrCorrupt
	}
	binding, ok := openBinding(raw[48:256])
	if !ok {
		return Record{}, ErrCorrupt
	}
	record := Record{Status: Status(raw[8]), LastOperation: Operation(raw[9]), Binding: binding}
	copy(record.PinID[:], raw[16:48])
	copy(record.AcquireAuthorityDigest[:], raw[256:288])
	copy(record.CurrentAuthorityDigest[:], raw[288:320])
	record.AcquireApplied = binary.LittleEndian.Uint64(raw[320:328])
	copy(record.AcquireController[:], raw[328:344])
	record.AcquireControllerEpoch = binary.LittleEndian.Uint64(raw[344:352])
	record.AcquireLeaseDeadline = int64(binary.LittleEndian.Uint64(raw[352:360]))
	copy(record.Controller[:], raw[360:376])
	record.ControllerEpoch = binary.LittleEndian.Uint64(raw[376:384])
	record.LeaseDeadline = int64(binary.LittleEndian.Uint64(raw[384:392]))
	record.LeaseRevision = binary.LittleEndian.Uint64(raw[392:400])
	record.LeaseApplied = binary.LittleEndian.Uint64(raw[400:408])
	record.TerminalApplied = binary.LittleEndian.Uint64(raw[408:416])
	copy(record.PrepareTerminalDigest[:], raw[416:448])
	record.TerminalObserved = int64(binary.LittleEndian.Uint64(raw[448:456]))
	copy(record.LastCommandDigest[:], raw[456:488])
	record.LastApplied = binary.LittleEndian.Uint64(raw[488:496])
	copy(record.TerminalAuthorityDigest[:], raw[496:528])
	if !record.Valid() {
		return Record{}, ErrCorrupt
	}
	return record, nil
}

func RecordDigest(encoded []byte) (Digest, error) {
	if _, err := OpenRecord(encoded); err != nil {
		return Digest{}, err
	}
	return Digest(sha256.Sum256(encoded)), nil
}

func sealSHA(frame []byte, domain []byte) {
	hash := sha256.New()
	_, _ = hash.Write(domain)
	_, _ = hash.Write(frame[:len(frame)-sha256.Size])
	_ = hash.Sum(frame[len(frame)-sha256.Size : len(frame)-sha256.Size])
}

func verifySHA(frame []byte, domain []byte) bool {
	if len(frame) < sha256.Size {
		return false
	}
	var digest [sha256.Size]byte
	hash := sha256.New()
	_, _ = hash.Write(domain)
	_, _ = hash.Write(frame[:len(frame)-sha256.Size])
	_ = hash.Sum(digest[:0])
	return bytes.Equal(digest[:], frame[len(frame)-sha256.Size:])
}
