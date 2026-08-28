// Package routegate implements the compact replicated gate that orders durable
// request pins against topology drains on one shard. Commands are fixed-size,
// canonical byte records so callers can place them directly in a Raft entry.
package routegate

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

const (
	// CommandBytes is the exact size of every replicated route-gate command.
	// A command always names at most one request or topology operation. Large
	// distributed requests therefore stream one command to each participant;
	// the grammar has no aggregate participant-count field or ceiling.
	CommandBytes = 84

	commandBodyBytes = CommandBytes - 4
)

var (
	ErrCorrupt  = errors.New("routegate: corrupt command")
	ErrTooLarge = errors.New("routegate: encoded state exceeds the admission bound")
	ErrScratch  = errors.New("routegate: insufficient caller scratch")

	commandMagic = [4]byte{'V', 'R', 'G', 'T'}
	castagnoli   = crc32.MakeTable(crc32.Castagnoli)
)

// Identity is the stable digest of one physical participant wave/attempt or
// topology operation. A long-lived logical request contract does not hold a
// route pin: it may refresh physical placement between waves. Zero is never a
// valid identity.
type Identity [32]byte

// Binding authenticates the exact physical group, member/endpoint fence, and
// command fingerprint for a participant wave, or the topology plan bound to
// an exclusive identity. Reusing an identity with another binding fails
// closed, so outcome-unknown retries cannot silently change their route.
type Binding [32]byte

// Operation identifies one deterministic state transition.
type Operation uint8

const (
	OperationInvalid Operation = iota
	OperationAcquireShared
	OperationReleaseShared
	OperationBeginExclusive
	OperationReleaseExclusive
	OperationCompactReleased
)

// Command is the construction form of one replicated transition. Epoch is the
// exact admission epoch observed before proposal. Identity and Binding must be
// nonzero except for OperationCompactReleased, where both are canonically zero.
type Command struct {
	Operation Operation
	Epoch     uint64
	Identity  Identity
	Binding   Binding
}

// AppendCommand appends one canonical fixed-size command. With sufficient dst
// capacity it allocates no memory.
func AppendCommand(dst []byte, command Command) ([]byte, error) {
	if !validCommand(command) {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, CommandBytes)...)
	copy(dst[start:start+4], commandMagic[:])
	dst[start+4] = byte(command.Operation)
	binary.LittleEndian.PutUint64(dst[start+8:start+16], command.Epoch)
	copy(dst[start+16:start+48], command.Identity[:])
	copy(dst[start+48:start+80], command.Binding[:])
	binary.LittleEndian.PutUint32(
		dst[start+commandBodyBytes:start+CommandBytes],
		crc32.Checksum(dst[start:start+commandBodyBytes], castagnoli),
	)
	return dst, nil
}

// OpenCommand authenticates and validates exactly one canonical command.
func OpenCommand(raw []byte) (Command, error) {
	if len(raw) != CommandBytes ||
		raw[0] != commandMagic[0] || raw[1] != commandMagic[1] ||
		raw[2] != commandMagic[2] || raw[3] != commandMagic[3] ||
		raw[5] != 0 || raw[6] != 0 || raw[7] != 0 ||
		binary.LittleEndian.Uint32(raw[commandBodyBytes:]) !=
			crc32.Checksum(raw[:commandBodyBytes], castagnoli) {
		return Command{}, ErrCorrupt
	}
	command := Command{
		Operation: Operation(raw[4]),
		Epoch:     binary.LittleEndian.Uint64(raw[8:16]),
	}
	copy(command.Identity[:], raw[16:48])
	copy(command.Binding[:], raw[48:80])
	if !validCommand(command) {
		return Command{}, ErrCorrupt
	}
	return command, nil
}

// ValidateCommand is the allocation-free validation-only path.
func ValidateCommand(raw []byte) error {
	_, err := OpenCommand(raw)
	return err
}

func validCommand(command Command) bool {
	if command.Epoch == 0 {
		return false
	}
	switch command.Operation {
	case OperationAcquireShared, OperationReleaseShared,
		OperationBeginExclusive, OperationReleaseExclusive:
		return command.Identity != (Identity{}) && command.Binding != (Binding{})
	case OperationCompactReleased:
		return command.Identity == (Identity{}) && command.Binding == (Binding{})
	default:
		return false
	}
}
