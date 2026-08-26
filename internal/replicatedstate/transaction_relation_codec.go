package replicatedstate

import (
	"bytes"
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/replication"
)

const (
	transactionRelationPayloadHeaderBytes = 48
	transactionRelationPayloadKeyBytes    = 19

	MaxTransactionRelationPayloadRecordBytes = transactionRelationPayloadHeaderBytes +
		replication.MaxCommandBytes + recordChecksumLen
)

var (
	transactionRelationPayloadMagic          = [8]byte{'V', 'D', 'B', 'T', 'R', 'E', 'L', 0}
	transactionRelationPayloadChecksumDomain = []byte(
		"vibedb/replicated-state/transaction-relation-checksum\x00",
	)
)

type TransactionRelationPayloadView struct {
	ID       distributedtxn.ID
	Relation replication.RelationID
	Count    uint32
	Batch    replication.RelationBatchView
	raw      []byte
}

func (view TransactionRelationPayloadView) Bytes() []byte {
	return view.raw[:len(view.raw):len(view.raw)]
}

func (view TransactionRelationPayloadView) MutationBytes() []byte {
	return view.Batch.MutationBytes()
}

func (view TransactionRelationPayloadView) StorageKey() (
	[transactionRelationPayloadKeyBytes]byte,
	error,
) {
	return TransactionRelationPayloadStorageKey(view.ID, view.Relation)
}

func TransactionRelationPayloadStorageKey(
	id distributedtxn.ID,
	relation replication.RelationID,
) ([transactionRelationPayloadKeyBytes]byte, error) {
	var key [transactionRelationPayloadKeyBytes]byte
	if id.IsZero() || !transactionRelationValid(relation) {
		return key, ErrTransactionStateCorrupt
	}
	key[0] = transactionMutationPrefix
	copy(key[1:17], id[:])
	binary.BigEndian.PutUint16(key[17:19], uint16(relation))
	return key, nil
}

func TransactionRelationPayloadResidentBytes(payloadBytes int) (uint64, error) {
	if payloadBytes <= 0 || payloadBytes > replication.MaxCommandBytes {
		return 0, ErrTransactionStateCorrupt
	}
	return transactionRelationPayloadKeyBytes + transactionRelationPayloadHeaderBytes +
		uint64(payloadBytes+recordChecksumLen), nil
}

func AppendTransactionRelationPayload(
	dst []byte,
	id distributedtxn.ID,
	batch replication.RelationBatchView,
) ([]byte, error) {
	payload := batch.MutationBytes()
	count := uint32(batch.MutationCount())
	if id.IsZero() || !transactionRelationValid(batch.Relation) || count == 0 ||
		len(payload) == 0 || len(payload) > replication.MaxCommandBytes {
		return dst, ErrTransactionStateCorrupt
	}
	if _, err := replication.OpenRelationMutationBytes(batch.Relation, count, payload); err != nil {
		return dst, ErrTransactionStateCorrupt
	}
	total := transactionRelationPayloadHeaderBytes + len(payload) + recordChecksumLen
	if byteSlicesOverlap(writableAppendRegion(dst, total), payload) {
		return dst, ErrCodecAlias
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	copy(frame[:8], transactionRelationPayloadMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], transactionCodecSentinel)
	binary.LittleEndian.PutUint16(frame[10:12], transactionRelationPayloadHeaderBytes)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(total))
	binary.LittleEndian.PutUint32(frame[16:20], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[20:24], count)
	binary.LittleEndian.PutUint16(frame[24:26], uint16(batch.Relation))
	copy(frame[32:48], id[:])
	copy(frame[transactionRelationPayloadHeaderBytes:], payload)
	sealRecord(frame, transactionRelationPayloadChecksumDomain)
	return dst, nil
}

func OpenTransactionRelationPayload(src []byte) (TransactionRelationPayloadView, error) {
	if len(src) < transactionRelationPayloadHeaderBytes+1+recordChecksumLen ||
		len(src) > MaxTransactionRelationPayloadRecordBytes ||
		!bytes.Equal(src[:8], transactionRelationPayloadMagic[:]) ||
		binary.LittleEndian.Uint16(src[8:10]) != transactionCodecSentinel ||
		binary.LittleEndian.Uint16(src[10:12]) != transactionRelationPayloadHeaderBytes ||
		binary.LittleEndian.Uint32(src[12:16]) != uint32(len(src)) ||
		!allZero(src[26:32]) ||
		!verifyRecord(src, transactionRelationPayloadChecksumDomain) {
		return TransactionRelationPayloadView{}, ErrTransactionStateCorrupt
	}
	payloadBytes := uint64(binary.LittleEndian.Uint32(src[16:20]))
	count := binary.LittleEndian.Uint32(src[20:24])
	if payloadBytes == 0 || payloadBytes > replication.MaxCommandBytes ||
		uint64(transactionRelationPayloadHeaderBytes+recordChecksumLen)+payloadBytes != uint64(len(src)) {
		return TransactionRelationPayloadView{}, ErrTransactionStateCorrupt
	}
	view := TransactionRelationPayloadView{
		Relation: replication.RelationID(binary.LittleEndian.Uint16(src[24:26])),
		Count:    count, raw: src[:len(src):len(src)],
	}
	copy(view.ID[:], src[32:48])
	end := transactionRelationPayloadHeaderBytes + int(payloadBytes)
	payload := src[transactionRelationPayloadHeaderBytes:end:end]
	batch, err := replication.OpenRelationMutationBytes(view.Relation, count, payload)
	if err != nil || view.ID.IsZero() {
		return TransactionRelationPayloadView{}, ErrTransactionStateCorrupt
	}
	view.Batch = batch
	return view, nil
}
