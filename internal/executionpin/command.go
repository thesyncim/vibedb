package executionpin

import (
	"encoding/binary"
	"hash/crc32"
)

const CommandBytes = 436

var (
	commandMagic = [4]byte{'V', 'E', 'L', 'P'}
	castagnoli   = crc32.MakeTable(crc32.Castagnoli)
)

type Operation uint8

const (
	OperationAcquire Operation = iota + 1
	OperationRenew
	OperationRecover
	OperationRelease
	OperationExpire
)

// Command is one fixed logical pin transition. A lease is measured only in
// committed catalog-group log positions. No wall-clock value is admitted to
// this grammar: recovery/expiry is authorized by the apply index of the
// transition itself crossing the previously certified lease fence.
type Command struct {
	Operation Operation
	Binding   Binding
	PinID     PinID
	// AuthorityNode and AuthorityGeneration are the exact authenticated
	// service principal copied into the command at admission. The shard wire
	// rejects a mismatch before Raft proposal, so replicated certificates attest
	// the admitted principal rather than a caller-declared hint.
	AuthorityNode       ID
	AuthorityGeneration uint64

	ExpectedController          ID
	ExpectedControllerEpoch     uint64
	NextController              ID
	NextControllerEpoch         uint64
	ExpectedLeaseAppliedThrough uint64
	ExpectedLeaseRevision       uint64
	NextLeaseSpan               uint64

	PrepareTerminalDigest    Digest
	AcquireCertificateDigest Digest
}

func (command Command) Valid() bool {
	derived, err := DerivePinID(command.Binding)
	if err != nil || command.PinID != derived || command.AuthorityNode == (ID{}) ||
		command.AuthorityGeneration == 0 {
		return false
	}
	expected := command.ExpectedController != (ID{}) &&
		command.ExpectedControllerEpoch != 0 && command.ExpectedLeaseAppliedThrough != 0
	next := command.NextController != (ID{}) &&
		command.NextControllerEpoch != 0 && command.NextLeaseSpan != 0
	switch command.Operation {
	case OperationAcquire:
		return !expected && command.ExpectedLeaseRevision == 0 && next &&
			command.PrepareTerminalDigest == (Digest{}) &&
			command.AcquireCertificateDigest == (Digest{})
	case OperationRenew:
		return expected && command.ExpectedLeaseRevision != 0 && next && command.ExpectedController == command.NextController &&
			command.ExpectedControllerEpoch == command.NextControllerEpoch &&
			command.PrepareTerminalDigest == (Digest{}) &&
			command.AcquireCertificateDigest != (Digest{})
	case OperationRecover:
		return expected && command.ExpectedLeaseRevision != 0 && next && command.NextControllerEpoch > command.ExpectedControllerEpoch &&
			command.PrepareTerminalDigest == (Digest{}) &&
			command.AcquireCertificateDigest != (Digest{})
	case OperationRelease:
		return expected && command.ExpectedLeaseRevision != 0 && !next &&
			command.PrepareTerminalDigest != (Digest{}) &&
			command.AcquireCertificateDigest != (Digest{})
	case OperationExpire:
		return expected && command.ExpectedLeaseRevision != 0 && !next &&
			command.PrepareTerminalDigest == (Digest{}) &&
			command.AcquireCertificateDigest == (Digest{})
	default:
		return false
	}
}

func AppendCommand(dst []byte, command Command) ([]byte, error) {
	if !command.Valid() {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, CommandBytes)...)
	frame := dst[start:]
	copy(frame[0:4], commandMagic[:])
	frame[4] = byte(command.Operation)
	encodedBinding := appendBinding(frame[8:8], command.Binding)
	copy(frame[232:264], command.PinID[:])
	copy(frame[264:280], command.AuthorityNode[:])
	binary.LittleEndian.PutUint64(frame[280:288], command.AuthorityGeneration)
	copy(frame[288:304], command.ExpectedController[:])
	binary.LittleEndian.PutUint64(frame[304:312], command.ExpectedControllerEpoch)
	copy(frame[312:328], command.NextController[:])
	binary.LittleEndian.PutUint64(frame[328:336], command.NextControllerEpoch)
	binary.LittleEndian.PutUint64(frame[336:344], command.ExpectedLeaseAppliedThrough)
	binary.LittleEndian.PutUint64(frame[344:352], command.NextLeaseSpan)
	copy(frame[360:392], command.PrepareTerminalDigest[:])
	copy(frame[392:424], command.AcquireCertificateDigest[:])
	binary.LittleEndian.PutUint64(frame[424:432], command.ExpectedLeaseRevision)
	if len(encodedBinding) != bindingBytes {
		panic("executionpin: impossible binding geometry")
	}
	binary.LittleEndian.PutUint32(frame[432:436], crc32.Checksum(frame[:432], castagnoli))
	return dst, nil
}

func OpenCommand(raw []byte) (Command, error) {
	if len(raw) != CommandBytes || raw[0] != commandMagic[0] || raw[1] != commandMagic[1] ||
		raw[2] != commandMagic[2] || raw[3] != commandMagic[3] || !allZero(raw[5:8]) ||
		binary.LittleEndian.Uint32(raw[432:436]) !=
			crc32.Checksum(raw[:432], castagnoli) {
		return Command{}, ErrCorrupt
	}
	binding, ok := openBinding(raw[8:232])
	if !ok {
		return Command{}, ErrCorrupt
	}
	command := Command{Operation: Operation(raw[4]), Binding: binding}
	copy(command.PinID[:], raw[232:264])
	copy(command.AuthorityNode[:], raw[264:280])
	command.AuthorityGeneration = binary.LittleEndian.Uint64(raw[280:288])
	copy(command.ExpectedController[:], raw[288:304])
	command.ExpectedControllerEpoch = binary.LittleEndian.Uint64(raw[304:312])
	copy(command.NextController[:], raw[312:328])
	command.NextControllerEpoch = binary.LittleEndian.Uint64(raw[328:336])
	command.ExpectedLeaseAppliedThrough = binary.LittleEndian.Uint64(raw[336:344])
	command.NextLeaseSpan = binary.LittleEndian.Uint64(raw[344:352])
	if !allZero(raw[352:360]) {
		return Command{}, ErrCorrupt
	}
	copy(command.PrepareTerminalDigest[:], raw[360:392])
	copy(command.AcquireCertificateDigest[:], raw[392:424])
	command.ExpectedLeaseRevision = binary.LittleEndian.Uint64(raw[424:432])
	if !command.Valid() {
		return Command{}, ErrCorrupt
	}
	return command, nil
}
