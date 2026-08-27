package main

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	vibejson "github.com/thesyncim/vibejson"
)

var errInvalidDurableExecBatch = errors.New("gateway: invalid structured exec_batch request")

type durableExecBatchEnvelope struct {
	Identity durableExecBatchIdentity
	Request  *serveRequest
	Scratch  *serveRequestDecodeScratch
}

var durableExecBatchFields = vibejson.MakeFieldSet(
	"op", "request_id", "installation_id", "issuer_epoch", "lane_ordinal",
	"grant_digest", "issuer_sequence", "class", "statements",
)

var durableExecBatchDecoder = func() vibejson.Decoder[durableExecBatchEnvelope] {
	decoder, err := vibejson.CompileDecoder[durableExecBatchEnvelope](vibejson.DecoderOptions{
		MaxDepth: 32, ZeroCopy: true, CaseSensitive: true,
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
	var request serveRequest
	var scratch serveRequestDecodeScratch
	if decodeDurableExecBatchRequest(raw, &request, &scratch) != nil {
		return errInvalidDurableExecBatch
	}
	return nil
}

func decodeDurableExecBatchRequest(raw []byte, request *serveRequest,
	scratch *serveRequestDecodeScratch) error {
	if request == nil || scratch == nil || len(raw) == 0 || len(raw) > maxServeRequestBytes {
		return errInvalidDurableExecBatch
	}
	resetServeRequestScratch(scratch, len(raw))
	*request = serveRequest{}
	scratch.durableTarget.Request = request
	scratch.durableTarget.Scratch = scratch
	if durableExecBatchDecoder.Decode(raw, &scratch.durableTarget) != nil ||
		!validDurableExecBatchIdentity(scratch.durableTarget.Identity) || len(request.Statements) == 0 {
		return errInvalidDurableExecBatch
	}
	request.Op = "exec_batch"
	request.wireIdentity = scratch.durableTarget.Identity
	request.wireIdentitySet = true
	return nil
}

func (envelope *durableExecBatchEnvelope) UnmarshalVibeJSON(
	cursor vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	request, scratch := envelope.Request, envelope.Scratch
	*envelope = durableExecBatchEnvelope{Request: request, Scratch: scratch}
	if request == nil || scratch == nil {
		return cursor, errInvalidDurableExecBatch
	}
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
	if cursor, err = decodeDurableExecBatchAckFixedHex(cursor, envelope.Identity.Reference.Installation[:]); err != nil ||
		!cursor.Field(false, durableExecBatchFields.Field(3)) {
		return cursor, errInvalidDurableExecBatch
	}
	if cursor, envelope.Identity.Reference.Epoch, err = decodeDurableExecBatchAckUint64(cursor); err != nil ||
		!cursor.Field(false, durableExecBatchFields.Field(4)) {
		return cursor, errInvalidDurableExecBatch
	}
	var ordinal uint64
	if cursor, ordinal, err = decodeDurableExecBatchAckUint64(cursor); err != nil ||
		ordinal >= uint64(gateway.MaxReplicatedIssuerLanes) ||
		!cursor.Field(false, durableExecBatchFields.Field(5)) {
		return cursor, errInvalidDurableExecBatch
	}
	envelope.Identity.Reference.LaneOrdinal = uint16(ordinal)
	if cursor, err = decodeDurableExecBatchAckFixedHex(cursor, envelope.Identity.Reference.GrantDigest[:]); err != nil ||
		!cursor.Field(false, durableExecBatchFields.Field(6)) {
		return cursor, errInvalidDurableExecBatch
	}
	if cursor, envelope.Identity.IssuerSequence, err = decodeDurableExecBatchAckUint64(cursor); err != nil {
		return cursor, errInvalidDurableExecBatch
	}
	if cursor.Field(false, durableExecBatchFields.Field(7)) {
		var class []byte
		cursor, class, err = durableExecBatchAckString(cursor)
		if err != nil || !(bytes.Equal(class, []byte("interactive")) ||
			bytes.Equal(class, []byte("batch")) || bytes.Equal(class, []byte("admin"))) {
			return cursor, errInvalidDurableExecBatch
		}
		request.Class = serveBorrowedString(class)
	}
	if !cursor.Field(false, durableExecBatchFields.Field(8)) {
		return cursor, errInvalidDurableExecBatch
	}
	cursor, err = decodeServeStatements(cursor, request, scratch)
	if err != nil || len(request.Statements) == 0 ||
		!cursor.ExpectObjectClose() || !validDurableExecBatchIdentity(envelope.Identity) {
		return cursor, errInvalidDurableExecBatch
	}
	return cursor, nil
}

func durableExecBatchRequestCandidate(raw []byte) bool {
	return exactOperationCandidate(raw, []byte(`"exec_batch"`))
}
