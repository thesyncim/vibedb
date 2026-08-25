package main

import (
	"encoding/hex"
	"errors"
	"io"
	"strconv"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	vibejson "github.com/thesyncim/vibejson"
)

var errInvalidNativeDataResponse = errors.New("gateway: invalid native data response")

type nativeDataResponseCode uint8

const (
	nativeDataResponseInvalidRequest nativeDataResponseCode = iota + 1
	nativeDataResponseUnauthorized
	nativeDataResponseTableNotReplicated
	nativeDataResponsePositionMismatch
	nativeDataResponseReadBehind
	nativeDataResponseConflict
	nativeDataResponseStaleCatalog
	nativeDataResponseOverloaded
	nativeDataResponseUnavailable
	nativeDataResponseOutcomeUnknown
	nativeDataResponseInternal
)

type nativeDataWireResponse struct {
	Position   replication.Digest
	RequestID  replication.ID128
	Document   []byte
	Applied    uint64
	Retries    uint32
	Code       nativeDataResponseCode
	OK         bool
	Found      bool
	Retryable  bool
	HasRequest bool
	readResult gateway.ReplicatedTableReadResult
}

type nativeDataResponseScratch struct {
	prefix [256]byte
	suffix [96]byte
}

func (response *nativeDataWireResponse) release() {
	if response != nil {
		response.readResult.Release()
		response.Document = nil
	}
}

func nativeDataResponseCodeBytes(code nativeDataResponseCode) []byte {
	switch code {
	case nativeDataResponseInvalidRequest:
		return []byte("invalid_request")
	case nativeDataResponseUnauthorized:
		return []byte("unauthorized")
	case nativeDataResponseTableNotReplicated:
		return []byte("table_not_replicated")
	case nativeDataResponsePositionMismatch:
		return []byte("position_mismatch")
	case nativeDataResponseReadBehind:
		return []byte("read_behind")
	case nativeDataResponseConflict:
		return []byte("conflict")
	case nativeDataResponseStaleCatalog:
		return []byte("stale_catalog")
	case nativeDataResponseOverloaded:
		return []byte("overloaded")
	case nativeDataResponseUnavailable:
		return []byte("unavailable")
	case nativeDataResponseOutcomeUnknown:
		return []byte("outcome_unknown")
	case nativeDataResponseInternal:
		return []byte("internal")
	default:
		return nil
	}
}

func writeNativeDataResponse(writer *vibejson.Writer, response *nativeDataWireResponse) error {
	if writer == nil || !validNativeDataResponse(response) {
		return errInvalidNativeDataResponse
	}
	if err := writer.BeginObject(); err != nil {
		return err
	}
	if err := writer.Key("ok"); err != nil {
		return err
	}
	if err := writer.Bool(response.OK); err != nil {
		return err
	}
	if response.OK {
		if err := writer.Key("route_id"); err != nil {
			return err
		}
		if err := writeNativeHex(writer, response.Position[:]); err != nil {
			return err
		}
		if err := writer.Key("applied"); err != nil {
			return err
		}
		if err := writer.Uint(response.Applied); err != nil {
			return err
		}
		if err := writer.Key("found"); err != nil {
			return err
		}
		if err := writer.Bool(response.Found); err != nil {
			return err
		}
		if response.Found {
			if err := writer.Key("document"); err != nil {
				return err
			}
			// Replicated point reads return the exact document accepted by the
			// relation state machine, so this boundary does not parse or re-encode
			// the durable JSON a second time.
			if err := writer.RawUnchecked(response.Document); err != nil {
				return err
			}
		}
	} else {
		if err := writer.Key("code"); err != nil {
			return err
		}
		if err := writeNativeQuotedBytes(writer, nativeDataResponseCodeBytes(response.Code)); err != nil {
			return err
		}
		if err := writer.Key("retryable"); err != nil {
			return err
		}
		if err := writer.Bool(response.Retryable); err != nil {
			return err
		}
	}
	if response.HasRequest {
		if err := writer.Key("request_id"); err != nil {
			return err
		}
		if err := writeNativeHex(writer, response.RequestID[:]); err != nil {
			return err
		}
	}
	if response.Retries != 0 {
		if err := writer.Key("retries"); err != nil {
			return err
		}
		if err := writer.Uint(uint64(response.Retries)); err != nil {
			return err
		}
	}
	if err := writer.EndObject(); err != nil {
		return err
	}
	if err := writer.Newline(); err != nil {
		return err
	}
	return writer.Flush()
}

// writeNativeDataResponseDirect emits one response without copying Document
// into a growing connection-lifetime buffer. The fixed prefix and suffix reuse
// per-connection scratch while the already validated durable document streams
// directly to the sink. A slow sink therefore retains only the reader-owned
// document covered by its admission reservation.
func writeNativeDataResponseDirect(
	out io.Writer,
	response *nativeDataWireResponse,
	scratch *nativeDataResponseScratch,
) error {
	if out == nil || scratch == nil || !validNativeDataResponse(response) {
		return errInvalidNativeDataResponse
	}
	prefix := scratch.prefix[:0]
	prefix = append(prefix, `{"ok":`...)
	if response.OK {
		prefix = append(prefix, "true,\"route_id\":\""...)
		prefix = appendNativeResponseHex(prefix, response.Position[:])
		prefix = append(prefix, "\",\"applied\":"...)
		prefix = strconv.AppendUint(prefix, response.Applied, 10)
		prefix = append(prefix, ",\"found\":"...)
		if response.Found {
			prefix = append(prefix, "true,\"document\":"...)
		} else {
			prefix = append(prefix, "false"...)
		}
	} else {
		prefix = append(prefix, "false,\"code\":\""...)
		prefix = append(prefix, nativeDataResponseCodeBytes(response.Code)...)
		prefix = append(prefix, "\",\"retryable\":"...)
		prefix = strconv.AppendBool(prefix, response.Retryable)
	}
	if err := writeNativeResponseBytes(out, prefix); err != nil {
		return err
	}
	if response.Found {
		if err := writeNativeResponseBytes(out, response.Document); err != nil {
			return err
		}
	}
	suffix := scratch.suffix[:0]
	if response.HasRequest {
		suffix = append(suffix, ",\"request_id\":\""...)
		suffix = appendNativeResponseHex(suffix, response.RequestID[:])
		suffix = append(suffix, '"')
	}
	if response.Retries != 0 {
		suffix = append(suffix, ",\"retries\":"...)
		suffix = strconv.AppendUint(suffix, uint64(response.Retries), 10)
	}
	suffix = append(suffix, '}', '\n')
	return writeNativeResponseBytes(out, suffix)
}

func validNativeDataResponse(response *nativeDataWireResponse) bool {
	if response == nil ||
		(response.HasRequest && response.RequestID == (replication.ID128{})) {
		return false
	}
	if !response.OK {
		return nativeDataResponseCodeBytes(response.Code) != nil &&
			!response.Found && len(response.Document) == 0
	}
	return response.Code == 0 && response.Applied != 0 &&
		response.Position != (replication.Digest{}) &&
		(response.Found == (len(response.Document) != 0))
}

func appendNativeResponseHex(dst, value []byte) []byte {
	return hex.AppendEncode(dst, value)
}

func writeNativeResponseBytes(out io.Writer, value []byte) error {
	for len(value) != 0 {
		written, err := out.Write(value)
		if written > 0 {
			value = value[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func writeNativeHex(writer *vibejson.Writer, value []byte) error {
	if len(value) == 0 || len(value) > 32 {
		return errInvalidNativeDataResponse
	}
	var encoded [2 + 2*32]byte
	encoded[0] = '"'
	hex.Encode(encoded[1:], value)
	encoded[1+hex.EncodedLen(len(value))] = '"'
	return writer.RawUnchecked(encoded[:2+hex.EncodedLen(len(value))])
}

func writeNativeQuotedBytes(writer *vibejson.Writer, value []byte) error {
	if len(value) == 0 || len(value) > 32 {
		return errInvalidNativeDataResponse
	}
	var quoted [34]byte
	quoted[0] = '"'
	copy(quoted[1:], value)
	quoted[len(value)+1] = '"'
	return writer.RawUnchecked(quoted[:len(value)+2])
}
