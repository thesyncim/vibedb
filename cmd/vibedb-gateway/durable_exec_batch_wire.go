package main

import (
	"bytes"
	"errors"

	vibejson "github.com/thesyncim/vibejson"
)

var errInvalidDurableExecBatch = errors.New("gateway: invalid structured exec_batch request")

type durableExecBatchEnvelope struct {
	Identity durableExecBatchIdentity
}

var durableExecBatchFields = vibejson.MakeFieldSet(
	"op", "request_id", "issuer_epoch", "issuer_lane", "issuer_sequence",
	"issuer_authenticator", "class", "statements",
)

var durableExecBatchDecoder = func() vibejson.Decoder[durableExecBatchEnvelope] {
	decoder, err := vibejson.CompileDecoder[durableExecBatchEnvelope](vibejson.DecoderOptions{
		MaxDepth: 32, ZeroCopy: true, CaseSensitive: true, Replace: true,
	})
	if err != nil {
		panic(err)
	}
	return decoder
}()

func validateDurableExecBatchEnvelope(raw []byte) error {
	if len(raw) == 0 || len(raw) > maxServeRequestBytes {
		return errInvalidDurableExecBatch
	}
	var envelope durableExecBatchEnvelope
	if durableExecBatchDecoder.Decode(raw, &envelope) != nil ||
		!validDurableExecBatchIdentity(envelope.Identity) {
		return errInvalidDurableExecBatch
	}
	return nil
}

func (envelope *durableExecBatchEnvelope) UnmarshalVibeJSON(
	cursor vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	*envelope = durableExecBatchEnvelope{}
	if cursor.BeginObject("structured exec_batch") != nil ||
		!cursor.Field(true, durableExecBatchFields.Field(0)) {
		return cursor, errInvalidDurableExecBatch
	}
	cursor, operation, err := durableExecBatchAckString(cursor)
	if err != nil || !bytes.Equal(operation, []byte("exec_batch")) ||
		!cursor.Field(false, durableExecBatchFields.Field(1)) {
		return cursor, errInvalidDurableExecBatch
	}
	if cursor, err = decodeDurableExecBatchAckFixedHex(cursor, envelope.Identity.RequestID[:]); err != nil ||
		!cursor.Field(false, durableExecBatchFields.Field(2)) {
		return cursor, errInvalidDurableExecBatch
	}
	if cursor, envelope.Identity.IssuerEpoch, err = decodeDurableExecBatchAckUint64(cursor); err != nil ||
		!cursor.Field(false, durableExecBatchFields.Field(3)) {
		return cursor, errInvalidDurableExecBatch
	}
	if cursor, err = decodeDurableExecBatchAckFixedHex(cursor, envelope.Identity.IssuerLane[:]); err != nil ||
		!cursor.Field(false, durableExecBatchFields.Field(4)) {
		return cursor, errInvalidDurableExecBatch
	}
	if cursor, envelope.Identity.IssuerSequence, err = decodeDurableExecBatchAckUint64(cursor); err != nil ||
		!cursor.Field(false, durableExecBatchFields.Field(5)) {
		return cursor, errInvalidDurableExecBatch
	}
	if cursor, err = decodeDurableExecBatchAckFixedHex(cursor, envelope.Identity.Authenticator[:]); err != nil {
		return cursor, errInvalidDurableExecBatch
	}
	if cursor.Field(false, durableExecBatchFields.Field(6)) {
		var class []byte
		cursor, class, err = durableExecBatchAckString(cursor)
		if err != nil || !(bytes.Equal(class, []byte("interactive")) ||
			bytes.Equal(class, []byte("batch")) || bytes.Equal(class, []byte("admin"))) {
			return cursor, errInvalidDurableExecBatch
		}
	}
	if !cursor.Field(false, durableExecBatchFields.Field(7)) {
		return cursor, errInvalidDurableExecBatch
	}
	statements, err := cursor.Raw()
	if err != nil || len(statements.Bytes()) == 0 || statements.Bytes()[0] != '[' ||
		!cursor.ExpectObjectClose() || !validDurableExecBatchIdentity(envelope.Identity) {
		return cursor, errInvalidDurableExecBatch
	}
	return cursor, nil
}

func durableExecBatchRequestCandidate(raw []byte) bool {
	return exactOperationCandidate(raw, []byte(`"exec_batch"`))
}
