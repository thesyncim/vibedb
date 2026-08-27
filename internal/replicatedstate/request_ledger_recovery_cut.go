package replicatedstate

import (
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	requestLedgerProgressReadHeaderBytes = 9
	MaxRequestLedgerProgressReadBytes    = requestLedgerProgressReadHeaderBytes +
		requestledger.MaxHeadRecordBytes + requestledger.MaxContinuationRecordBytes

	requestLedgerTerminalReadHeaderBytes = 21
	MaxRequestLedgerTerminalReadBytes    = requestLedgerTerminalReadHeaderBytes +
		requestledger.MaxHeadRecordBytes + requestledger.MaxContinuationRecordBytes +
		requestledger.MaxPreparedTerminalRecordBytes + requestledger.MaxSchemaPinReleaseRecordBytes
)

type RequestLedgerProgressReadValue struct {
	Head              []byte
	Continuation      []byte
	ContinuationFound bool
}

func OpenRequestLedgerProgressReadValue(raw []byte) (RequestLedgerProgressReadValue, error) {
	if len(raw) < requestLedgerProgressReadHeaderBytes ||
		len(raw) > MaxRequestLedgerProgressReadBytes || raw[0]&^byte(1) != 0 {
		return RequestLedgerProgressReadValue{}, ErrRequestLedgerRead
	}
	headBytes := uint64(binary.LittleEndian.Uint32(raw[1:5]))
	continuationBytes := uint64(binary.LittleEndian.Uint32(raw[5:9]))
	if headBytes == 0 || uint64(len(raw)) != uint64(requestLedgerProgressReadHeaderBytes)+
		headBytes+continuationBytes || (continuationBytes != 0) != (raw[0]&1 != 0) {
		return RequestLedgerProgressReadValue{}, ErrRequestLedgerRead
	}
	offset := uint64(requestLedgerProgressReadHeaderBytes)
	return RequestLedgerProgressReadValue{
		Head:              raw[offset : offset+headBytes : offset+headBytes],
		Continuation:      raw[offset+headBytes : offset+headBytes+continuationBytes : offset+headBytes+continuationBytes],
		ContinuationFound: continuationBytes != 0,
	}, nil
}

type RequestLedgerTerminalReadValue struct {
	Head              []byte
	Continuation      []byte
	ContinuationFound bool
	Prepared          []byte
	PreparedFound     bool
	SchemaPin         []byte
	SchemaPinFound    bool
	Terminal          []byte
	TerminalFound     bool
}

func OpenRequestLedgerTerminalReadValue(raw []byte) (RequestLedgerTerminalReadValue, error) {
	if len(raw) < requestLedgerTerminalReadHeaderBytes ||
		len(raw) > MaxRequestLedgerTerminalReadBytes || raw[0]&^byte(15) != 0 {
		return RequestLedgerTerminalReadValue{}, ErrRequestLedgerRead
	}
	var lengths [5]uint64
	for index := range lengths {
		lengths[index] = uint64(binary.LittleEndian.Uint32(raw[1+index*4:]))
	}
	total := uint64(requestLedgerTerminalReadHeaderBytes)
	for _, length := range lengths {
		total += length
	}
	if lengths[0] == 0 || total != uint64(len(raw)) ||
		(lengths[1] != 0) != (raw[0]&1 != 0) ||
		(lengths[2] != 0) != (raw[0]&2 != 0) ||
		(lengths[3] != 0) != (raw[0]&4 != 0) ||
		(lengths[4] != 0) != (raw[0]&8 != 0) ||
		(lengths[4] != 0 && raw[0]&7 != 0) {
		return RequestLedgerTerminalReadValue{}, ErrRequestLedgerRead
	}
	offset := uint64(requestLedgerTerminalReadHeaderBytes)
	next := func(length uint64) []byte {
		value := raw[offset : offset+length : offset+length]
		offset += length
		return value
	}
	value := RequestLedgerTerminalReadValue{Head: next(lengths[0])}
	value.Continuation, value.ContinuationFound = next(lengths[1]), lengths[1] != 0
	value.Prepared, value.PreparedFound = next(lengths[2]), lengths[2] != 0
	value.SchemaPin, value.SchemaPinFound = next(lengths[3]), lengths[3] != 0
	value.Terminal, value.TerminalFound = next(lengths[4]), lengths[4] != 0
	return value, nil
}

func requestLedgerProgressRead(
	snapshot *durable.Snapshot, dst, headRaw []byte,
	home requestledger.LedgerHome, keyDigest requestledger.Digest,
) ([]byte, error) {
	value, err := requestLedgerCutHead(dst, headRaw, requestLedgerProgressReadHeaderBytes)
	if err != nil {
		return nil, err
	}
	var key [requestledger.FixedStorageKeyBytes]byte
	return appendRequestLedgerCutRow(snapshot, value,
		requestledger.AppendContinuationKey(key[:0], home, keyDigest), 1, 5)
}

func requestLedgerTerminalRead(
	snapshot *durable.Snapshot, dst, headRaw []byte,
	home requestledger.LedgerHome, keyDigest requestledger.Digest,
) ([]byte, error) {
	value, err := requestLedgerCutHead(dst, headRaw, requestLedgerTerminalReadHeaderBytes)
	if err != nil {
		return nil, err
	}
	var key [requestledger.FixedStorageKeyBytes]byte
	value, err = appendRequestLedgerCutRow(snapshot, value,
		requestledger.AppendTerminalKey(key[:0], home, keyDigest), 8, 17)
	if err != nil || value[0]&8 != 0 {
		return value, err
	}
	value, err = appendRequestLedgerCutRow(snapshot, value,
		requestledger.AppendContinuationKey(key[:0], home, keyDigest), 1, 5)
	if err != nil {
		return nil, err
	}
	value, err = appendRequestLedgerCutRow(snapshot, value,
		requestledger.AppendPreparedTerminalKey(key[:0], home, keyDigest), 2, 9)
	if err != nil {
		return nil, err
	}
	return appendRequestLedgerCutRow(snapshot, value,
		requestledger.AppendSchemaPinReleaseKey(key[:0], home, keyDigest), 4, 13)
}

func requestLedgerCutHead(dst, headRaw []byte, headerBytes int) ([]byte, error) {
	if len(headRaw) == 0 || headerBytes+len(headRaw) > cap(dst) {
		return nil, ErrReadBufferBound
	}
	value := dst[:headerBytes+len(headRaw)]
	copy(value[headerBytes:], headRaw)
	clear(value[:headerBytes])
	binary.LittleEndian.PutUint32(value[1:5], uint32(len(headRaw)))
	return value, nil
}

func appendRequestLedgerCutRow(
	snapshot *durable.Snapshot, value, key []byte, flag byte, lengthOffset int,
) ([]byte, error) {
	start := len(value)
	next, found, err := snapshot.AppendRaw(value, key)
	if err != nil || !found {
		return next, err
	}
	if err := validateSnapshotRequestLedgerRow(key, next[start:]); err != nil {
		return nil, err
	}
	next[0] |= flag
	binary.LittleEndian.PutUint32(next[lengthOffset:lengthOffset+4], uint32(len(next)-start))
	return next, nil
}
