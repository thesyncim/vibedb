package main

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/internal/collectionname"
	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

var errInvalidNativeDataRequest = errors.New("gateway: invalid native data request")

type nativeDataOperation uint8

const (
	nativeDataOperationGet nativeDataOperation = iota + 1
	nativeDataOperationPut
	nativeDataOperationDelete
)

type nativeDataConsistency uint8

const (
	nativeDataLinearizable nativeDataConsistency = iota + 1
	nativeDataAtLeastApplied
)

// nativeDataWireRequest is the zero-tree public RF3 envelope. Table and
// Document alias the immutable input line; key and identities decode directly
// into fixed caller-owned storage. No document, key, or identity enters a Go
// string on this path.
type nativeDataWireRequest struct {
	Operation   nativeDataOperation
	Consistency nativeDataConsistency
	Table       []byte
	TableStore  [collectionname.MaxNameBytes * 6]byte
	Document    []byte
	Key         [replication.MaxMutationKeyBytes]byte
	KeyBytes    uint32
	RequestID   replication.ID128
	RouteID     replication.Digest
	Applied     uint64
}

func (request *nativeDataWireRequest) OrderedKey() []byte {
	if request == nil || request.KeyBytes == 0 || request.KeyBytes > uint32(len(request.Key)) {
		return nil
	}
	return request.Key[:request.KeyBytes:request.KeyBytes]
}

var nativeDataRequestFields = vibejson.MakeFieldSet(
	"op", "table", "key", "consistency", "route_id", "applied", "document", "request_id",
)

var nativeDataRequestDecoder = func() vibejson.Decoder[nativeDataWireRequest] {
	decoder, err := vibejson.CompileDecoder[nativeDataWireRequest](vibejson.DecoderOptions{
		MaxDepth: 16, ZeroCopy: true, CaseSensitive: true, Replace: true,
	})
	if err != nil {
		panic(err)
	}
	return decoder
}()

func decodeNativeDataRequest(src []byte, request *nativeDataWireRequest) error {
	if request == nil || len(src) == 0 || len(src) > replication.MaxCommandBytes {
		return errInvalidNativeDataRequest
	}
	if err := nativeDataRequestDecoder.Decode(src, request); err != nil {
		return errInvalidNativeDataRequest
	}
	return nil
}

func (request *nativeDataWireRequest) UnmarshalVibeJSON(
	cursor vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	request.Operation = 0
	request.Consistency = 0
	request.Table = nil
	request.Document = nil
	request.KeyBytes = 0
	request.RequestID = replication.ID128{}
	request.RouteID = replication.Digest{}
	request.Applied = 0
	if err := cursor.BeginObject("native data request"); err != nil {
		return cursor, errInvalidNativeDataRequest
	}
	if !cursor.Field(true, nativeDataRequestFields.Field(0)) {
		return cursor, errInvalidNativeDataRequest
	}
	cursor, op, err := nativeDataString(cursor)
	if err != nil {
		return cursor, err
	}
	switch {
	case bytes.Equal(op, []byte("get")):
		request.Operation = nativeDataOperationGet
	case bytes.Equal(op, []byte("put")):
		request.Operation = nativeDataOperationPut
	case bytes.Equal(op, []byte("delete")):
		request.Operation = nativeDataOperationDelete
	default:
		return cursor, errInvalidNativeDataRequest
	}
	if !cursor.Field(false, nativeDataRequestFields.Field(1)) {
		return cursor, errInvalidNativeDataRequest
	}
	tableValue, tableErr := cursor.Raw()
	if tableErr != nil || len(tableValue.Bytes()) > len(request.TableStore)+2 {
		return cursor, errInvalidNativeDataRequest
	}
	if request.Table, _ = tableValue.StringBytes(); request.Table == nil {
		var isString bool
		request.Table, isString, tableErr = tableValue.AppendText(request.TableStore[:0])
		if tableErr != nil || !isString {
			return cursor, errInvalidNativeDataRequest
		}
	}
	if len(request.Table) == 0 || len(request.Table) > collectionname.MaxNameBytes {
		return cursor, errInvalidNativeDataRequest
	}
	var canonicalTable [collectionname.MaxNameBytes*6 + 2]byte
	canonical := appendNativeCanonicalString(canonicalTable[:0], request.Table)
	if !bytes.Equal(canonical, tableValue.Bytes()) {
		return cursor, errInvalidNativeDataRequest
	}
	if !cursor.Field(false, nativeDataRequestFields.Field(2)) {
		return cursor, errInvalidNativeDataRequest
	}
	if cursor, err = decodeNativeDataKey(cursor, request); err != nil {
		return cursor, err
	}

	switch request.Operation {
	case nativeDataOperationGet:
		if !cursor.Field(false, nativeDataRequestFields.Field(3)) {
			return cursor, errInvalidNativeDataRequest
		}
		var consistency []byte
		cursor, consistency, err = nativeDataString(cursor)
		if err != nil {
			return cursor, err
		}
		switch {
		case bytes.Equal(consistency, []byte("linearizable")):
			request.Consistency = nativeDataLinearizable
		case bytes.Equal(consistency, []byte("at_least_applied")):
			request.Consistency = nativeDataAtLeastApplied
			if !cursor.Field(false, nativeDataRequestFields.Field(4)) {
				return cursor, errInvalidNativeDataRequest
			}
			if cursor, err = decodeNativeDataFixedHex(cursor, request.RouteID[:]); err != nil {
				return cursor, err
			}
			if !cursor.Field(false, nativeDataRequestFields.Field(5)) {
				return cursor, errInvalidNativeDataRequest
			}
			appliedValue, appliedErr := cursor.Raw()
			if appliedErr != nil {
				return cursor, errInvalidNativeDataRequest
			}
			var ok bool
			if request.Applied, ok = appliedValue.Uint64(); !ok || request.Applied == 0 {
				return cursor, errInvalidNativeDataRequest
			}
		default:
			return cursor, errInvalidNativeDataRequest
		}
	case nativeDataOperationPut:
		if !cursor.Field(false, nativeDataRequestFields.Field(6)) {
			return cursor, errInvalidNativeDataRequest
		}
		documentValue, documentErr := cursor.Raw()
		if documentErr != nil || documentValue.Kind() != document.Object ||
			len(documentValue.Bytes()) == 0 || len(documentValue.Bytes()) > replication.MaxMutationValueBytes {
			return cursor, errInvalidNativeDataRequest
		}
		request.Document = documentValue.Bytes()
		if !cursor.Field(false, nativeDataRequestFields.Field(7)) {
			return cursor, errInvalidNativeDataRequest
		}
		if cursor, err = decodeNativeDataFixedHex(cursor, request.RequestID[:]); err != nil {
			return cursor, err
		}
	case nativeDataOperationDelete:
		if !cursor.Field(false, nativeDataRequestFields.Field(7)) {
			return cursor, errInvalidNativeDataRequest
		}
		if cursor, err = decodeNativeDataFixedHex(cursor, request.RequestID[:]); err != nil {
			return cursor, err
		}
	default:
		return cursor, errInvalidNativeDataRequest
	}
	if !cursor.ExpectObjectClose() {
		return cursor, errInvalidNativeDataRequest
	}
	return cursor, nil
}

func appendNativeCanonicalString(dst, text []byte) []byte {
	const hexadecimal = "0123456789abcdef"
	dst = append(dst, '"')
	for index := 0; index < len(text); {
		value := text[index]
		switch value {
		case '"', '\\':
			dst = append(dst, '\\', value)
			index++
		case '\b':
			dst = append(dst, `\b`...)
			index++
		case '\f':
			dst = append(dst, `\f`...)
			index++
		case '\n':
			dst = append(dst, `\n`...)
			index++
		case '\r':
			dst = append(dst, `\r`...)
			index++
		case '\t':
			dst = append(dst, `\t`...)
			index++
		default:
			if value < 0x20 || value == '<' || value == '>' || value == '&' {
				dst = append(dst, '\\', 'u', '0', '0', hexadecimal[value>>4], hexadecimal[value&0xf])
				index++
				continue
			}
			if value < utf8.RuneSelf {
				dst = append(dst, value)
				index++
				continue
			}
			runeValue, width := utf8.DecodeRune(text[index:])
			if runeValue == '\u2028' || runeValue == '\u2029' {
				dst = append(dst, '\\', 'u', '2', '0', '2', hexadecimal[byte(runeValue)&0xf])
			} else {
				dst = append(dst, text[index:index+width]...)
			}
			index += width
		}
	}
	return append(dst, '"')
}

func nativeDataString(
	cursor vibejson.DecodeCursor,
) (vibejson.DecodeCursor, []byte, error) {
	raw, err := cursor.Raw()
	if err != nil {
		return cursor, nil, errInvalidNativeDataRequest
	}
	text, ok := raw.StringBytes()
	if !ok {
		return cursor, nil, errInvalidNativeDataRequest
	}
	return cursor, text, nil
}

func decodeNativeDataKey(
	cursor vibejson.DecodeCursor,
	request *nativeDataWireRequest,
) (vibejson.DecodeCursor, error) {
	cursor, encoded, err := nativeDataString(cursor)
	if err != nil || len(encoded) == 0 ||
		len(encoded) > base64.RawURLEncoding.EncodedLen(len(request.Key)) {
		return cursor, errInvalidNativeDataRequest
	}
	decoded, decodeErr := base64.RawURLEncoding.Strict().Decode(request.Key[:], encoded)
	if decodeErr != nil || decoded <= 0 || decoded > len(request.Key) ||
		base64.RawURLEncoding.EncodedLen(decoded) != len(encoded) {
		return cursor, errInvalidNativeDataRequest
	}
	request.KeyBytes = uint32(decoded)
	return cursor, nil
}

func decodeNativeDataFixedHex(
	cursor vibejson.DecodeCursor,
	destination []byte,
) (vibejson.DecodeCursor, error) {
	cursor, encoded, err := nativeDataString(cursor)
	if err != nil || len(encoded) != hex.EncodedLen(len(destination)) {
		return cursor, errInvalidNativeDataRequest
	}
	for _, value := range encoded {
		if !((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f')) {
			return cursor, errInvalidNativeDataRequest
		}
	}
	decoded, decodeErr := hex.Decode(destination, encoded)
	if decodeErr != nil || decoded != len(destination) {
		return cursor, errInvalidNativeDataRequest
	}
	var nonzero byte
	for _, value := range destination {
		nonzero |= value
	}
	if nonzero == 0 {
		return cursor, errInvalidNativeDataRequest
	}
	return cursor, nil
}
