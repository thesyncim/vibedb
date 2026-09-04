package gatewayruntime

import (
	"bytes"
	"encoding/hex"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	vibejson "github.com/thesyncim/vibejson"
)

const maxDurableExecBatchAckRequestBytes = 768

var (
	errInvalidDurableExecBatchAckRequest  = errors.New("gateway: invalid durable exec_batch acknowledgement")
	errInvalidDurableExecBatchAckResponse = errors.New("gateway: invalid durable exec_batch acknowledgement response")
)

// durableExecBatchAckIdentity is the complete public, resumable identity of an
// issued request-ledger sequence. The authenticated principal and tenant are
// deliberately absent: the serving boundary supplies both from the transport
// authority and refuses a caller-provided substitute.
type durableExecBatchAckIdentity struct {
	RequestID      replication.ID128
	RequestDigest  replication.Digest
	Reference      gateway.ReplicatedIssuerReference
	IssuerSequence uint64
}

// durableExecBatchAckWireRequest proves possession of the terminal result that
// may be compacted. AckToken is an opaque fixed-width capability; no textual
// interpretation or variable-width token reaches the ledger.
type durableExecBatchAckWireRequest struct {
	Identity         durableExecBatchAckIdentity
	TerminalRevision uint64
	ResultDigest     replication.Digest
	AckToken         requestledger.AckToken
}

// durableExecBatchAckWireResponse echoes the exact resumable handle. A client
// that loses this response can safely retry the same request: the replicated
// ACK tombstone authenticates the token digest after result collection.
// Applied is the current linearizable observation, and CollectionRounds counts
// work in this invocation. Neither is part of the durable capability: elections
// may advance Applied on a write-free completed retry (CollectionRounds == 0).
type durableExecBatchAckWireResponse struct {
	durableExecBatchAckWireRequest
	Applied          uint64
	CollectionRounds uint64
}

var durableExecBatchAckRequestFields = vibejson.MakeFieldSet(
	"op",
	"request_id",
	"request_digest",
	"installation_id",
	"issuer_epoch",
	"lane_ordinal",
	"grant_digest",
	"issuer_sequence",
	"terminal_revision",
	"result_digest",
	"ack_token",
)

var durableExecBatchAckRequestDecoder = func() vibejson.Decoder[durableExecBatchAckWireRequest] {
	decoder, err := vibejson.CompileDecoder[durableExecBatchAckWireRequest](vibejson.DecoderOptions{
		MaxDepth: 2, ZeroCopy: true, CaseSensitive: true, Replace: true,
	})
	if err != nil {
		panic(err)
	}
	return decoder
}()

func decodeDurableExecBatchAckRequest(
	source []byte,
	request *durableExecBatchAckWireRequest,
) error {
	if request == nil || len(source) == 0 || len(source) > maxDurableExecBatchAckRequestBytes {
		return errInvalidDurableExecBatchAckRequest
	}
	if err := durableExecBatchAckRequestDecoder.Decode(source, request); err != nil ||
		!validDurableExecBatchAckRequest(request) {
		return errInvalidDurableExecBatchAckRequest
	}
	return nil
}

func (request *durableExecBatchAckWireRequest) UnmarshalVibeJSON(
	cursor vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	*request = durableExecBatchAckWireRequest{}
	if err := cursor.BeginObject("durable exec_batch acknowledgement"); err != nil {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	if !cursor.Field(true, durableExecBatchAckRequestFields.Field(0)) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	cursor, operation, err := durableExecBatchAckString(cursor)
	if err != nil || !bytes.Equal(operation, []byte("ack_exec_batch")) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	if !cursor.Field(false, durableExecBatchAckRequestFields.Field(1)) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	if cursor, err = decodeDurableExecBatchAckFixedHex(cursor, request.Identity.RequestID[:]); err != nil {
		return cursor, err
	}
	if !cursor.Field(false, durableExecBatchAckRequestFields.Field(2)) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	if cursor, err = decodeDurableExecBatchAckFixedHex(cursor, request.Identity.RequestDigest[:]); err != nil {
		return cursor, err
	}
	if !cursor.Field(false, durableExecBatchAckRequestFields.Field(3)) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	if cursor, err = decodeDurableExecBatchAckFixedHex(cursor, request.Identity.Reference.Installation[:]); err != nil {
		return cursor, err
	}
	if !cursor.Field(false, durableExecBatchAckRequestFields.Field(4)) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	if cursor, request.Identity.Reference.Epoch, err = decodeDurableExecBatchAckUint64(cursor); err != nil {
		return cursor, err
	}
	if !cursor.Field(false, durableExecBatchAckRequestFields.Field(5)) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	var ordinal uint64
	// Match exec_batch's zero-based lane identity without relaxing epoch/sequence.
	if cursor, ordinal, err = decodeServeUint64(cursor); err != nil ||
		ordinal >= uint64(gateway.MaxReplicatedIssuerLanes) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	request.Identity.Reference.LaneOrdinal = uint16(ordinal)
	if !cursor.Field(false, durableExecBatchAckRequestFields.Field(6)) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	if cursor, err = decodeDurableExecBatchAckFixedHex(cursor, request.Identity.Reference.GrantDigest[:]); err != nil {
		return cursor, err
	}
	if !cursor.Field(false, durableExecBatchAckRequestFields.Field(7)) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	if cursor, request.Identity.IssuerSequence, err = decodeDurableExecBatchAckUint64(cursor); err != nil {
		return cursor, err
	}
	if !cursor.Field(false, durableExecBatchAckRequestFields.Field(8)) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	if cursor, request.TerminalRevision, err = decodeDurableExecBatchAckUint64(cursor); err != nil {
		return cursor, err
	}
	if !cursor.Field(false, durableExecBatchAckRequestFields.Field(9)) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	if cursor, err = decodeDurableExecBatchAckFixedHex(cursor, request.ResultDigest[:]); err != nil {
		return cursor, err
	}
	if !cursor.Field(false, durableExecBatchAckRequestFields.Field(10)) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	if cursor, err = decodeDurableExecBatchAckFixedHex(cursor, request.AckToken[:]); err != nil {
		return cursor, err
	}
	if !cursor.ExpectObjectClose() || !validDurableExecBatchAckRequest(request) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	return cursor, nil
}

func validDurableExecBatchAckRequest(request *durableExecBatchAckWireRequest) bool {
	return request != nil &&
		request.Identity.RequestID != (replication.ID128{}) &&
		request.Identity.RequestDigest != (replication.Digest{}) &&
		request.Identity.Reference.Installation != (replication.ID128{}) &&
		request.Identity.Reference.Epoch != 0 &&
		request.Identity.Reference.LaneOrdinal < gateway.MaxReplicatedIssuerLanes &&
		request.Identity.Reference.GrantDigest != (replication.Digest{}) &&
		request.Identity.IssuerSequence != 0 &&
		request.TerminalRevision != 0 &&
		request.ResultDigest != (replication.Digest{}) &&
		request.AckToken != (requestledger.AckToken{})
}

func validDurableExecBatchAckResponse(response *durableExecBatchAckWireResponse) bool {
	return response != nil && validDurableExecBatchAckRequest(&response.durableExecBatchAckWireRequest) &&
		response.Applied != 0
}

func writeDurableExecBatchAckResponse(
	writer *vibejson.Writer,
	response *durableExecBatchAckWireResponse,
) error {
	if writer == nil || !validDurableExecBatchAckResponse(response) {
		return errInvalidDurableExecBatchAckResponse
	}
	if err := writer.BeginObject(); err != nil {
		return err
	}
	if err := writer.Key("ok"); err != nil {
		return err
	}
	if err := writer.Bool(true); err != nil {
		return err
	}
	if err := writer.Key("op"); err != nil {
		return err
	}
	if err := writer.RawUnchecked([]byte(`"ack_exec_batch"`)); err != nil {
		return err
	}
	identity := &response.Identity
	if err := writeDurableExecBatchAckHexField(writer, "request_id", identity.RequestID[:]); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckHexField(writer, "request_digest", identity.RequestDigest[:]); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckHexField(writer, "installation_id", identity.Reference.Installation[:]); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckUintField(writer, "issuer_epoch", identity.Reference.Epoch); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckUintField(writer, "lane_ordinal", uint64(identity.Reference.LaneOrdinal)); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckHexField(writer, "grant_digest", identity.Reference.GrantDigest[:]); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckUintField(writer, "issuer_sequence", identity.IssuerSequence); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckUintField(writer, "terminal_revision", response.TerminalRevision); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckHexField(writer, "result_digest", response.ResultDigest[:]); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckHexField(writer, "ack_token", response.AckToken[:]); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckUintField(writer, "applied", response.Applied); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckUintField(writer, "collection_rounds", response.CollectionRounds); err != nil {
		return err
	}
	if err := writer.EndObject(); err != nil {
		return err
	}
	if err := writer.Newline(); err != nil {
		return err
	}
	return writer.Flush()
}

func durableExecBatchAckRequestCandidate(source []byte) bool {
	index := skipNativeJSONSpace(source, 0)
	if index >= len(source) || source[index] != '{' {
		return false
	}
	index = skipNativeJSONSpace(source, index+1)
	const operationKey = `"op"`
	if len(source)-index < len(operationKey) ||
		!bytes.Equal(source[index:index+len(operationKey)], []byte(operationKey)) {
		return false
	}
	index = skipNativeJSONSpace(source, index+len(operationKey))
	if index >= len(source) || source[index] != ':' {
		return false
	}
	index = skipNativeJSONSpace(source, index+1)
	const operation = `"ack_exec_batch"`
	if len(source)-index < len(operation) ||
		!bytes.Equal(source[index:index+len(operation)], []byte(operation)) {
		return false
	}
	next := index + len(operation)
	if next == len(source) {
		return true
	}
	switch source[next] {
	case ',', '}', ' ', '\t', '\r', '\n':
		return true
	default:
		return false
	}
}

func durableExecBatchAckString(
	cursor vibejson.DecodeCursor,
) (vibejson.DecodeCursor, []byte, error) {
	raw, err := cursor.Raw()
	if err != nil {
		return cursor, nil, errInvalidDurableExecBatchAckRequest
	}
	value, ok := raw.StringBytes()
	if !ok {
		return cursor, nil, errInvalidDurableExecBatchAckRequest
	}
	return cursor, value, nil
}

func decodeDurableExecBatchAckFixedHex(
	cursor vibejson.DecodeCursor,
	destination []byte,
) (vibejson.DecodeCursor, error) {
	cursor, encoded, err := durableExecBatchAckString(cursor)
	if err != nil || len(encoded) != hex.EncodedLen(len(destination)) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	for _, value := range encoded {
		if !((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f')) {
			return cursor, errInvalidDurableExecBatchAckRequest
		}
	}
	decoded, decodeErr := hex.Decode(destination, encoded)
	if decodeErr != nil || decoded != len(destination) {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	var nonzero byte
	for _, value := range destination {
		nonzero |= value
	}
	if nonzero == 0 {
		return cursor, errInvalidDurableExecBatchAckRequest
	}
	return cursor, nil
}

func decodeDurableExecBatchAckUint64(
	cursor vibejson.DecodeCursor,
) (vibejson.DecodeCursor, uint64, error) {
	raw, err := cursor.Raw()
	if err != nil {
		return cursor, 0, errInvalidDurableExecBatchAckRequest
	}
	value, ok := raw.Uint64()
	if !ok || value == 0 {
		return cursor, 0, errInvalidDurableExecBatchAckRequest
	}
	return cursor, value, nil
}

func writeDurableExecBatchAckHexField(
	writer *vibejson.Writer,
	name string,
	value []byte,
) error {
	if err := writer.Key(name); err != nil {
		return err
	}
	return writeNativeHex(writer, value)
}

func writeDurableExecBatchAckUintField(
	writer *vibejson.Writer,
	name string,
	value uint64,
) error {
	if err := writer.Key(name); err != nil {
		return err
	}
	return writer.Uint(value)
}
