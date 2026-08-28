package replication

import (
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
)

// TransactionMutationBytesLayout is the complete framing metadata for one
// detached canonical relation-mutation payload. A singleton uses the same
// compact inline relation identity as Command; multiple relations carry their
// existing ordered eight-byte headers in Bytes.
type TransactionMutationBytesLayout struct {
	Bytes            int
	MutationCount    uint32
	RelationCount    uint16
	InlineRelationID RelationID
}

// MeasureTransactionMutationBytes validates and measures the exact canonical
// relation bytes used by replicated transaction commands without constructing
// a physical command envelope.
func MeasureTransactionMutationBytes(
	batches []RelationMutationBatch,
) (TransactionMutationBytesLayout, error) {
	if err := validateRelationBatches(batches); err != nil {
		return TransactionMutationBytesLayout{}, err
	}
	layout := TransactionMutationBytesLayout{RelationCount: uint16(len(batches))}
	if len(batches) == 1 {
		layout.InlineRelationID = batches[0].Relation
	}
	total := uint64(0)
	for batchIndex := range batches {
		batch := &batches[batchIndex]
		if len(batches) > 1 {
			var ok bool
			total, ok = checkedAdd(total, relationBatchHeaderBytes, MaxCommandBytes)
			if !ok {
				return TransactionMutationBytesLayout{}, ErrEnvelopeTooLarge
			}
		}
		for mutationIndex := range batch.Mutations {
			mutation := &batch.Mutations[mutationIndex]
			if err := validateMutation(*mutation); err != nil {
				return TransactionMutationBytesLayout{}, err
			}
			mutationBytes := uint64(mutationHeaderBytes + len(mutation.Key) + mutationWireValueBytes(*mutation))
			var ok bool
			total, ok = checkedAdd(total, mutationBytes, MaxCommandBytes)
			if !ok {
				return TransactionMutationBytesLayout{}, ErrEnvelopeTooLarge
			}
		}
		if len(batch.Mutations) > MaxMutations-int(layout.MutationCount) {
			return TransactionMutationBytesLayout{}, ErrEnvelopeTooLarge
		}
		layout.MutationCount += uint32(len(batch.Mutations))
	}
	layout.Bytes = int(total)
	return layout, nil
}

// AppendTransactionMutationBytes appends exactly the native relation bytes
// measured by MeasureTransactionMutationBytes. On failure dst is unchanged.
func AppendTransactionMutationBytes(
	dst []byte,
	batches []RelationMutationBatch,
) ([]byte, TransactionMutationBytesLayout, error) {
	layout, err := MeasureTransactionMutationBytes(batches)
	if err != nil {
		return dst, TransactionMutationBytesLayout{}, err
	}
	if commandOverlapsAppendRegion(dst, layout.Bytes, Command{Batches: batches}) {
		return dst, TransactionMutationBytesLayout{}, semantic("mutation input aliases destination append region")
	}
	start := len(dst)
	dst = extendZeroed(dst, layout.Bytes)
	frame := dst[start:]
	cursor := 0
	for batchIndex := range batches {
		batch := &batches[batchIndex]
		headerAt := cursor
		if layout.RelationCount > 1 {
			binary.LittleEndian.PutUint16(frame[cursor:cursor+2], uint16(batch.Relation))
			binary.LittleEndian.PutUint16(frame[cursor+2:cursor+4], uint16(len(batch.Mutations)))
			cursor += relationBatchHeaderBytes
		}
		payloadAt := cursor
		for mutationIndex := range batch.Mutations {
			mutation := &batch.Mutations[mutationIndex]
			frame[cursor] = byte(mutation.Kind)
			binary.LittleEndian.PutUint16(frame[cursor+2:cursor+4], uint16(len(mutation.Key)))
			binary.LittleEndian.PutUint32(frame[cursor+4:cursor+8], uint32(mutationWireValueBytes(*mutation)))
			cursor += mutationHeaderBytes
			cursor += copy(frame[cursor:], mutation.Key)
			if mutation.Kind == MutationDeleteDigestEqual || mutation.Kind == MutationPutDigestEqual {
				binary.LittleEndian.PutUint64(frame[cursor:cursor+8], mutation.ExpectedValueLength)
				copy(frame[cursor+8:cursor+mutationDigestCompareBytes], mutation.ExpectedValueDigest[:])
				cursor += mutationDigestCompareBytes
			}
			if mutation.Kind != MutationDeleteDigestEqual {
				cursor += copy(frame[cursor:], mutation.Value)
			}
		}
		if layout.RelationCount > 1 {
			binary.LittleEndian.PutUint32(
				frame[headerAt+4:headerAt+8], uint32(cursor-payloadAt),
			)
		}
	}
	if cursor != layout.Bytes {
		return dst[:start], TransactionMutationBytesLayout{}, ErrEnvelopeCorrupt
	}
	return dst, layout, nil
}

// TransactionMutationBytesView is a validated borrowed detached payload.
type TransactionMutationBytesView struct {
	raw    []byte
	layout TransactionMutationBytesLayout
}

// OpenTransactionMutationBytes validates detached bytes and their exact outer
// framing metadata without allocating or constructing a command envelope.
func OpenTransactionMutationBytes(
	raw []byte,
	layout TransactionMutationBytesLayout,
) (TransactionMutationBytesView, error) {
	if layout.Bytes != len(raw) || layout.Bytes <= 0 || layout.Bytes > MaxCommandBytes ||
		layout.MutationCount == 0 || layout.MutationCount > MaxMutations ||
		layout.RelationCount == 0 || layout.RelationCount > MaxRelationBatches {
		return TransactionMutationBytesView{}, ErrEnvelopeSemantic
	}
	if err := validateRelationBytes(
		raw, layout.MutationCount, layout.RelationCount, layout.InlineRelationID,
	); err != nil {
		return TransactionMutationBytesView{}, err
	}
	return TransactionMutationBytesView{
		raw: raw[:len(raw):len(raw)], layout: layout,
	}, nil
}

// RelationBatches walks the detached payload without materialization.
func (view TransactionMutationBytesView) RelationBatches() RelationBatchIterator {
	return RelationBatchIterator{
		remaining: view.layout.RelationCount,
		b:         view.raw,
		inlineID:  view.layout.InlineRelationID,
		inlineN:   view.layout.MutationCount,
	}
}

// Digest returns the same semantic mutation digest as
// TransactionMutationDigest over the decoded batches.
func (view TransactionMutationBytesView) Digest() distributedtxn.Digest {
	return transactionMutationDigestFromBytes(
		view.raw, view.layout.MutationCount,
		view.layout.RelationCount, view.layout.InlineRelationID,
	)
}
