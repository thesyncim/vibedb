package shardservice

import (
	"bytes"
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

const replicatedRequestLedgerReadValueHeaderBytes = 12

var replicatedRequestLedgerReadValueMagic = [4]byte{'V', 'R', 'L', 'R'}

// ReplicatedRequestLedgerReadValue is the detached full-key hidden-row result.
// Value is the exact canonical requestledger record; no JSON or textual system
// relation identity crosses the native boundary.
type ReplicatedRequestLedgerReadValue struct {
	Found             bool
	AuthoritativeKind replicatedstate.RequestLedgerReadKind
	Value             []byte
}

func AppendReplicatedRequestLedgerReadValue(
	dst []byte,
	value ReplicatedRequestLedgerReadValue,
) ([]byte, error) {
	if !validReplicatedRequestLedgerReadValueParts(value) {
		return dst, ErrReplicatedWire
	}
	start := len(dst)
	dst = append(dst, make([]byte, replicatedRequestLedgerReadValueHeaderBytes)...)
	header := dst[start:]
	copy(header[:4], replicatedRequestLedgerReadValueMagic[:])
	header[4] = 1
	if value.Found {
		header[5] = 1
		header[6] = byte(value.AuthoritativeKind)
	}
	binary.BigEndian.PutUint32(header[8:12], uint32(len(value.Value)))
	return append(dst, value.Value...), nil
}

func OpenReplicatedRequestLedgerReadValue(
	raw []byte,
) (ReplicatedRequestLedgerReadValue, error) {
	if len(raw) < replicatedRequestLedgerReadValueHeaderBytes ||
		!bytes.Equal(raw[:4], replicatedRequestLedgerReadValueMagic[:]) || raw[4] != 1 ||
		raw[5] > 1 || raw[7] != 0 ||
		int(binary.BigEndian.Uint32(raw[8:12])) != len(raw)-replicatedRequestLedgerReadValueHeaderBytes {
		return ReplicatedRequestLedgerReadValue{}, ErrReplicatedWire
	}
	value := ReplicatedRequestLedgerReadValue{
		Found: raw[5] == 1, AuthoritativeKind: replicatedstate.RequestLedgerReadKind(raw[6]),
		Value: raw[replicatedRequestLedgerReadValueHeaderBytes:],
	}
	value.Value = value.Value[:len(value.Value):len(value.Value)]
	if !validReplicatedRequestLedgerReadValueParts(value) {
		return ReplicatedRequestLedgerReadValue{}, ErrReplicatedWire
	}
	return value, nil
}

func validReplicatedRequestLedgerReadValue(raw []byte) bool {
	_, err := OpenReplicatedRequestLedgerReadValue(raw)
	return err == nil
}

func validReplicatedRequestLedgerReadValueParts(value ReplicatedRequestLedgerReadValue) bool {
	if !value.Found {
		return value.AuthoritativeKind == 0 && len(value.Value) == 0
	}
	maximum := replicatedstate.RequestLedgerReadMaxBytes(value.AuthoritativeKind)
	return maximum != 0 && len(value.Value) != 0 && len(value.Value) <= maximum
}
