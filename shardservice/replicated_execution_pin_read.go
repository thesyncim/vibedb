package shardservice

import (
	"bytes"

	"github.com/thesyncim/vibedb/internal/executionpin"
)

const replicatedExecutionPinReadValueBytes = 8 + executionpin.RecordBytes

var replicatedExecutionPinReadValueMagic = [4]byte{'V', 'E', 'L', 'R'}

type ReplicatedExecutionPinReadValue struct {
	Found  bool
	Record executionpin.Record
}

func AppendReplicatedExecutionPinReadValue(
	dst []byte,
	value ReplicatedExecutionPinReadValue,
) ([]byte, error) {
	if value.Found != value.Record.Valid() {
		return dst, ErrReplicatedWire
	}
	start := len(dst)
	dst = append(dst, make([]byte, replicatedExecutionPinReadValueBytes)...)
	frame := dst[start:]
	copy(frame[:4], replicatedExecutionPinReadValueMagic[:])
	if value.Found {
		frame[4] = 1
		if _, err := executionpin.AppendRecord(frame[8:8], value.Record); err != nil {
			return dst[:start], err
		}
	}
	return dst, nil
}

func OpenReplicatedExecutionPinReadValue(raw []byte) (ReplicatedExecutionPinReadValue, error) {
	if len(raw) != replicatedExecutionPinReadValueBytes ||
		!bytes.Equal(raw[:4], replicatedExecutionPinReadValueMagic[:]) || raw[4] > 1 ||
		!allZeroReplicatedBytes(raw[5:8]) {
		return ReplicatedExecutionPinReadValue{}, ErrReplicatedWire
	}
	if raw[4] == 0 {
		if !allZeroReplicatedBytes(raw[8:]) {
			return ReplicatedExecutionPinReadValue{}, ErrReplicatedWire
		}
		return ReplicatedExecutionPinReadValue{}, nil
	}
	record, err := executionpin.OpenRecord(raw[8:])
	if err != nil {
		return ReplicatedExecutionPinReadValue{}, ErrReplicatedWire
	}
	return ReplicatedExecutionPinReadValue{Found: true, Record: record}, nil
}

func validReplicatedExecutionPinReadValue(raw []byte) bool {
	_, err := OpenReplicatedExecutionPinReadValue(raw)
	return err == nil
}

func allZeroReplicatedBytes(raw []byte) bool {
	for _, value := range raw {
		if value != 0 {
			return false
		}
	}
	return true
}
