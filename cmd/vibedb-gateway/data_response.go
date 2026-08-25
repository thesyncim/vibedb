package main

import (
	"encoding/hex"
	"errors"

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
	if writer == nil || response == nil ||
		(response.OK && (response.Code != 0 || response.Applied == 0 || response.Position == (replication.Digest{}))) ||
		(!response.OK && nativeDataResponseCodeBytes(response.Code) == nil) ||
		(response.Found && len(response.Document) == 0) ||
		(!response.Found && len(response.Document) != 0) ||
		(response.HasRequest && response.RequestID == (replication.ID128{})) {
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
