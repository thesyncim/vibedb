package main

import (
	"bytes"
	"errors"

	vibejson "github.com/thesyncim/vibejson"
)

const maxIssuerOpenRequestBytes = 96

var errInvalidIssuerOpen = errors.New("gateway: invalid issuer_open request")

type issuerOpenWireRequest struct{ Lane uint16 }

var issuerOpenFields = vibejson.MakeFieldSet("op", "lane")

var issuerOpenDecoder = func() vibejson.Decoder[issuerOpenWireRequest] {
	decoder, err := vibejson.CompileDecoder[issuerOpenWireRequest](vibejson.DecoderOptions{
		MaxDepth: 2, ZeroCopy: true, CaseSensitive: true, Replace: true,
	})
	if err != nil {
		panic(err)
	}
	return decoder
}()

func decodeIssuerOpenRequest(raw []byte, request *issuerOpenWireRequest) error {
	if request == nil || len(raw) == 0 || len(raw) > maxIssuerOpenRequestBytes ||
		issuerOpenDecoder.Decode(raw, request) != nil {
		return errInvalidIssuerOpen
	}
	return nil
}

func (request *issuerOpenWireRequest) UnmarshalVibeJSON(
	cursor vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	*request = issuerOpenWireRequest{}
	if cursor.BeginObject("issuer_open") != nil || !cursor.Field(true, issuerOpenFields.Field(0)) {
		return cursor, errInvalidIssuerOpen
	}
	cursor, operation, err := durableExecBatchAckString(cursor)
	if err != nil || !bytes.Equal(operation, []byte("issuer_open")) ||
		!cursor.Field(false, issuerOpenFields.Field(1)) {
		return cursor, errInvalidIssuerOpen
	}
	value, valueErr := cursor.Raw()
	lane, ok := value.Uint64()
	if valueErr != nil || !ok || lane > uint64(^uint16(0)) || !cursor.ExpectObjectClose() {
		return cursor, errInvalidIssuerOpen
	}
	request.Lane = uint16(lane)
	return cursor, nil
}

func issuerOpenRequestCandidate(raw []byte) bool {
	return exactOperationCandidate(raw, []byte(`"issuer_open"`))
}

func writeIssuerOpenResponse(writer *vibejson.Writer, result durableIssuerOpenResult) error {
	if writer == nil || !validDurableIssuerOpenResult(result) {
		return errInvalidIssuerOpen
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
	if err := writer.RawUnchecked([]byte(`"issuer_open"`)); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckHexField(writer, "installation_id", result.Installation[:]); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckUintField(writer, "issuer_epoch", result.Epoch); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckHexField(writer, "issuer_lane", result.Lane[:]); err != nil {
		return err
	}
	if err := writeDurableExecBatchAckHexField(writer, "issuer_authenticator", result.Authenticator[:]); err != nil {
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

func exactOperationCandidate(raw, quotedOperation []byte) bool {
	index := skipNativeJSONSpace(raw, 0)
	if index >= len(raw) || raw[index] != '{' {
		return false
	}
	index = skipNativeJSONSpace(raw, index+1)
	const operationKey = `"op"`
	if len(raw)-index < len(operationKey) || !bytes.Equal(raw[index:index+len(operationKey)], []byte(operationKey)) {
		return false
	}
	index = skipNativeJSONSpace(raw, index+len(operationKey))
	if index >= len(raw) || raw[index] != ':' {
		return false
	}
	index = skipNativeJSONSpace(raw, index+1)
	if len(raw)-index < len(quotedOperation) || !bytes.Equal(raw[index:index+len(quotedOperation)], quotedOperation) {
		return false
	}
	next := index + len(quotedOperation)
	return next == len(raw) || bytes.ContainsRune([]byte(",} \t\r\n"), rune(raw[next]))
}
