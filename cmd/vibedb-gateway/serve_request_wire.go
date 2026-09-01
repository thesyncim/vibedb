package main

import (
	"errors"
	"unsafe"

	vibejson "github.com/thesyncim/vibejson"
)

var errInvalidServeRequest = errors.New("gateway: invalid serving request")

const maxServeWireParams = 1 << 16

// Metadata admission is a request-memory budget, not a shard/participant or
// statement-count limit. Charge conservatively for slice growth and the
// temporary parameter decode slice as well as the retained per-request slices.
const maxServeDecodeMetadataBytes = 8 << 20

const serveStatementMetadataBytes = 2 * (int(unsafe.Sizeof(serveStatement{})) + 2*4)
const serveParamMetadataBytes = 4 * int(unsafe.Sizeof(serveParam{}))

type serveParamKind uint8

const (
	serveParamNull serveParamKind = iota + 1
	serveParamBool
	serveParamNumber
	serveParamString
	serveParamDocument
)

// serveRequestDecodeScratch is owned by one client connection. Its capacities
// are reused across requests and are bounded by the one-megabyte frame, the
// explicit decode-metadata budget, and the shard codec parameter bound. Borrowed
// source spans remain valid until the next Scanner.Scan, after synchronous
// execution and response emission have completed.
type serveRequestDecodeScratch struct {
	text            []byte
	statements      []serveStatement
	topParams       []serveParam
	statementParams []serveParam
	paramDecode     []serveParam
	paramStarts     []uint32
	paramCounts     []uint32
	decodedParams   int
	metadataBytes   int
	target          serveRequestDecodeTarget
	durableTarget   durableExecBatchEnvelope
}

type serveRequestDecodeTarget struct {
	request *serveRequest
	scratch *serveRequestDecodeScratch
}

var serveRequestFields = vibejson.MakeFieldSet(
	"op", "request_id", "installation_id", "issuer_epoch", "lane_ordinal",
	"grant_digest", "issuer_sequence", "issuer_lane", "issuer_authenticator",
	"sql", "class", "max_result_bytes", "backup_id", "params", "statements",
)

var serveStatementFields = vibejson.MakeFieldSet("sql", "params")
var serveParamFields = vibejson.MakeFieldSet("kind", "bool", "text")

var serveRequestDecoder = func() vibejson.Decoder[serveRequestDecodeTarget] {
	decoder, err := vibejson.CompileDecoder[serveRequestDecodeTarget](vibejson.DecoderOptions{
		MaxDepth: 32, ZeroCopy: true, CaseSensitive: true,
	})
	if err != nil {
		panic(err)
	}
	return decoder
}()

func decodeServeRequest(src []byte, request *serveRequest, scratch *serveRequestDecodeScratch) error {
	if request == nil || scratch == nil || len(src) == 0 || len(src) > maxServeRequestBytes {
		return errInvalidServeRequest
	}
	resetServeRequestScratch(scratch, len(src))
	*request = serveRequest{}
	scratch.target.request = request
	scratch.target.scratch = scratch
	if err := serveRequestDecoder.Decode(src, &scratch.target); err != nil {
		return errInvalidServeRequest
	}
	return nil
}

func resetServeRequestScratch(scratch *serveRequestDecodeScratch, sourceBytes int) {
	scratch.text = scratch.text[:0]
	if cap(scratch.text) < sourceBytes {
		scratch.text = make([]byte, 0, sourceBytes)
	}
	scratch.statements = scratch.statements[:0]
	scratch.topParams = scratch.topParams[:0]
	scratch.statementParams = scratch.statementParams[:0]
	scratch.paramDecode = scratch.paramDecode[:0]
	scratch.paramStarts = scratch.paramStarts[:0]
	scratch.paramCounts = scratch.paramCounts[:0]
	scratch.decodedParams = 0
	scratch.metadataBytes = 0
}

func (target *serveRequestDecodeTarget) UnmarshalVibeJSON(
	cursor vibejson.DecodeCursor,
) (vibejson.DecodeCursor, error) {
	if target == nil || target.request == nil || target.scratch == nil ||
		cursor.BeginObject("serving request") != nil {
		return cursor, errInvalidServeRequest
	}
	request := target.request
	first := true
	for {
		key, ok, err := cursor.NextField(first)
		if err != nil {
			return cursor, errInvalidServeRequest
		}
		if !ok {
			break
		}
		first = false
		field, known := serveRequestFields.Lookup(key, cursor.CaseSensitive())
		if !known {
			if cursor.Skip() != nil {
				return cursor, errInvalidServeRequest
			}
			continue
		}
		switch field {
		case 0:
			var text []byte
			cursor, text, err = decodeServeText(cursor, target.scratch)
			if err != nil || !setServeOperation(request, text) {
				return cursor, errInvalidServeRequest
			}
		case 1:
			var value []byte
			cursor, value, err = decodeServeText(cursor, target.scratch)
			request.RequestID = serveBorrowedString(value)
		case 2:
			var value []byte
			cursor, value, err = decodeServeText(cursor, target.scratch)
			request.InstallationID = serveBorrowedString(value)
		case 3:
			cursor, request.IssuerEpoch, err = decodeServeUint64(cursor)
		case 4:
			var ordinal uint64
			cursor, ordinal, err = decodeServeUint64(cursor)
			if err == nil && ordinal <= uint64(^uint16(0)) {
				request.LaneOrdinal = uint16(ordinal)
			} else if err == nil {
				err = errInvalidServeRequest
			}
		case 5:
			var value []byte
			cursor, value, err = decodeServeText(cursor, target.scratch)
			request.GrantDigest = serveBorrowedString(value)
		case 6:
			cursor, request.IssuerSequence, err = decodeServeUint64(cursor)
		case 7:
			var value []byte
			cursor, value, err = decodeServeText(cursor, target.scratch)
			request.IssuerLane = serveBorrowedString(value)
		case 8:
			var value []byte
			cursor, value, err = decodeServeText(cursor, target.scratch)
			request.IssuerAuthenticator = serveBorrowedString(value)
		case 9:
			var requestSQL []byte
			cursor, requestSQL, err = decodeServeText(cursor, target.scratch)
			request.SQL = ""
			// The source or caller-owned escape arena backs SQL until this
			// request's synchronous execution completes.
			requestSQLSet(request, requestSQL)
		case 10:
			var value []byte
			cursor, value, err = decodeServeText(cursor, target.scratch)
			request.Class = serveBorrowedString(value)
		case 11:
			var value uint64
			cursor, value, err = decodeServeUint64(cursor)
			if err == nil && value <= uint64(^uint32(0)) {
				request.MaxResultBytes = uint32(value)
			} else if err == nil {
				err = errInvalidServeRequest
			}
		case 12:
			var value []byte
			cursor, value, err = decodeServeText(cursor, target.scratch)
			request.BackupID = serveBorrowedString(value)
		case 13:
			target.scratch.topParams = target.scratch.topParams[:0]
			cursor, err = decodeServeParams(cursor, target.scratch, &target.scratch.topParams)
			request.Params = target.scratch.topParams
		case 14:
			cursor, err = decodeServeStatements(cursor, request, target.scratch)
		}
		if err != nil {
			return cursor, errInvalidServeRequest
		}
	}
	return cursor, nil
}

func requestSQLSet(request *serveRequest, value []byte) {
	// Store the top-level SQL in a hidden sentinel statement slot would make
	// copies fragile. A dedicated field is kept in serveRequest (see serve.go).
	request.wireSQL = value
}

func decodeServeStatements(cursor vibejson.DecodeCursor, request *serveRequest,
	scratch *serveRequestDecodeScratch) (vibejson.DecodeCursor, error) {
	if cursor.BeginArray("serving statements") != nil {
		return cursor, errInvalidServeRequest
	}
	scratch.statements = scratch.statements[:0]
	scratch.statementParams = scratch.statementParams[:0]
	scratch.paramStarts = scratch.paramStarts[:0]
	scratch.paramCounts = scratch.paramCounts[:0]
	for first := true; ; first = false {
		more, err := cursor.NextElement(first)
		if err != nil {
			return cursor, errInvalidServeRequest
		}
		if !more {
			break
		}
		if !scratch.reserveMetadata(serveStatementMetadataBytes) {
			return cursor, errInvalidServeRequest
		}
		scratch.statements = append(scratch.statements, serveStatement{})
		scratch.paramStarts = append(scratch.paramStarts, uint32(len(scratch.statementParams)))
		scratch.paramCounts = append(scratch.paramCounts, 0)
		index := len(scratch.statements) - 1
		if cursor.BeginObject("serving statement") != nil {
			return cursor, errInvalidServeRequest
		}
		for memberFirst := true; ; memberFirst = false {
			key, ok, fieldErr := cursor.NextField(memberFirst)
			if fieldErr != nil {
				return cursor, errInvalidServeRequest
			}
			if !ok {
				break
			}
			field, known := serveStatementFields.Lookup(key, cursor.CaseSensitive())
			if !known {
				if cursor.Skip() != nil {
					return cursor, errInvalidServeRequest
				}
				continue
			}
			switch field {
			case 0:
				var value []byte
				cursor, value, fieldErr = decodeServeText(cursor, scratch)
				if fieldErr != nil {
					return cursor, errInvalidServeRequest
				}
				scratch.statements[index].wireSQL = value
			case 1:
				start := len(scratch.statementParams)
				scratch.paramDecode = scratch.paramDecode[:0]
				cursor, fieldErr = decodeServeParams(cursor, scratch, &scratch.paramDecode)
				if fieldErr != nil {
					return cursor, fieldErr
				}
				scratch.statementParams = append(scratch.statementParams, scratch.paramDecode...)
				scratch.paramStarts[index] = uint32(start)
				scratch.paramCounts[index] = uint32(len(scratch.paramDecode))
			}
		}
	}
	for index := range scratch.statements {
		start := int(scratch.paramStarts[index])
		end := start + int(scratch.paramCounts[index])
		if start < 0 || end < start || end > len(scratch.statementParams) {
			return cursor, errInvalidServeRequest
		}
		scratch.statements[index].Params = scratch.statementParams[start:end:end]
	}
	request.Statements = scratch.statements
	return cursor, nil
}

func decodeServeParams(cursor vibejson.DecodeCursor, scratch *serveRequestDecodeScratch,
	destination *[]serveParam) (vibejson.DecodeCursor, error) {
	if cursor.BeginArray("serving parameters") != nil {
		return cursor, errInvalidServeRequest
	}
	*destination = (*destination)[:0]
	for first := true; ; first = false {
		more, err := cursor.NextElement(first)
		if err != nil {
			return cursor, errInvalidServeRequest
		}
		if !more {
			break
		}
		if scratch.decodedParams >= maxServeWireParams || !scratch.reserveMetadata(serveParamMetadataBytes) {
			return cursor, errInvalidServeRequest
		}
		scratch.decodedParams++
		parameter := serveParam{}
		if cursor.BeginObject("serving parameter") != nil {
			return cursor, errInvalidServeRequest
		}
		for memberFirst := true; ; memberFirst = false {
			key, ok, fieldErr := cursor.NextField(memberFirst)
			if fieldErr != nil {
				return cursor, errInvalidServeRequest
			}
			if !ok {
				break
			}
			field, known := serveParamFields.Lookup(key, cursor.CaseSensitive())
			if !known {
				if cursor.Skip() != nil {
					return cursor, errInvalidServeRequest
				}
				continue
			}
			switch field {
			case 0:
				var value []byte
				cursor, value, fieldErr = decodeServeText(cursor, scratch)
				if fieldErr != nil {
					return cursor, errInvalidServeRequest
				}
				parameter.wireKind, parameter.Kind = serveParamKindValue(value)
			case 1:
				var raw vibejson.RawValue
				raw, fieldErr = cursor.Raw()
				if fieldErr == nil {
					var valid bool
					parameter.Bool, valid = raw.Bool()
					if !valid && !raw.IsNull() {
						fieldErr = errInvalidServeRequest
					}
				}
			case 2:
				cursor, parameter.wireText, fieldErr = decodeServeText(cursor, scratch)
			}
			if fieldErr != nil {
				return cursor, errInvalidServeRequest
			}
		}
		*destination = append(*destination, parameter)
	}
	return cursor, nil
}

func decodeServeText(cursor vibejson.DecodeCursor, scratch *serveRequestDecodeScratch) (vibejson.DecodeCursor, []byte, error) {
	raw, err := cursor.Raw()
	if err != nil {
		return cursor, nil, errInvalidServeRequest
	}
	if value, ok := raw.StringBytes(); ok {
		return cursor, value, nil
	}
	start := len(scratch.text)
	value, ok, appendErr := raw.AppendText(scratch.text)
	if appendErr != nil || !ok || len(value) > cap(scratch.text) {
		return cursor, nil, errInvalidServeRequest
	}
	scratch.text = value
	return cursor, scratch.text[start:], nil
}

func decodeServeUint64(cursor vibejson.DecodeCursor) (vibejson.DecodeCursor, uint64, error) {
	raw, err := cursor.Raw()
	if err != nil {
		return cursor, 0, errInvalidServeRequest
	}
	value, ok := raw.Uint64()
	if !ok {
		return cursor, 0, errInvalidServeRequest
	}
	return cursor, value, nil
}

func setServeOperation(request *serveRequest, value []byte) bool {
	switch string(value) {
	case "":
		request.Op = ""
	case "query":
		request.Op = "query"
	case "read_batch":
		request.Op = "read_batch"
	case "exec":
		request.Op = "exec"
	case "exec_batch":
		request.Op = "exec_batch"
	case "metrics":
		request.Op = "metrics"
	case "backup":
		request.Op = "backup"
	case "backup_status":
		request.Op = "backup_status"
	case "get":
		request.Op = "get"
	case "put":
		request.Op = "put"
	case "delete":
		request.Op = "delete"
	default:
		request.Op = serveBorrowedString(value)
	}
	return true
}

func serveParamKindValue(value []byte) (serveParamKind, string) {
	// Generic envelope fields have last-value-wins semantics. A duplicate
	// unknown kind must clear an earlier recognized enum before buildParams.
	switch string(value) {
	case "null":
		return serveParamNull, "null"
	case "bool":
		return serveParamBool, "bool"
	case "number":
		return serveParamNumber, "number"
	case "string":
		return serveParamString, "string"
	case "document":
		return serveParamDocument, "document"
	default:
		return 0, serveBorrowedString(value)
	}
}

func (scratch *serveRequestDecodeScratch) reserveMetadata(bytes int) bool {
	if bytes < 0 || bytes > maxServeDecodeMetadataBytes-scratch.metadataBytes {
		return false
	}
	scratch.metadataBytes += bytes
	return true
}

func serveBorrowedString(value []byte) string {
	if len(value) == 0 {
		return ""
	}
	return unsafe.String(unsafe.SliceData(value), len(value))
}

func (request serveRequest) sqlText() string {
	if request.SQL != "" {
		return request.SQL
	}
	return serveBorrowedString(request.wireSQL)
}

func (request serveRequest) hasSQL() bool { return request.SQL != "" || len(request.wireSQL) != 0 }

func (statement serveStatement) sqlText() string {
	if statement.SQL != "" {
		return statement.SQL
	}
	return serveBorrowedString(statement.wireSQL)
}

func (parameter serveParam) textValue() string {
	if parameter.Text != "" {
		return parameter.Text
	}
	return serveBorrowedString(parameter.wireText)
}
