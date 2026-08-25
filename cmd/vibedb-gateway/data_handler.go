package main

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/shardservice"
)

type nativeDataReader interface {
	Read(context.Context, gateway.ReplicatedTableReadRequest) (gateway.ReplicatedTableReadResult, error)
}

func executeNativeDataRead(
	ctx context.Context,
	reader nativeDataReader,
	request *nativeDataWireRequest,
) nativeDataWireResponse {
	if ctx == nil || reader == nil || request == nil ||
		request.Operation != nativeDataOperationGet {
		return nativeDataError(nativeDataResponseInvalidRequest, false)
	}
	read := gateway.ReplicatedTableReadRequest{
		Table: request.Table,
		Key:   request.OrderedKey(),
	}
	switch request.Consistency {
	case nativeDataLinearizable:
		read.Consistency = gateway.ReplicatedDataReadLinearizable
	case nativeDataAtLeastApplied:
		read.Consistency = gateway.ReplicatedDataReadAtLeastApplied
		read.Position.RouteID = request.RouteID
		read.Position.Applied = request.Applied
	default:
		return nativeDataError(nativeDataResponseInvalidRequest, false)
	}
	result, err := reader.Read(ctx, read)
	if err != nil {
		return nativeDataResponseForError(err)
	}
	return nativeDataWireResponse{
		OK: true, Position: result.Position.RouteID, Applied: result.Position.Applied,
		Found: result.Found, Document: result.Value, Retries: uint32(result.Retries),
		readResult: result,
	}
}

func nativeDataError(code nativeDataResponseCode, retryable bool) nativeDataWireResponse {
	return nativeDataWireResponse{Code: code, Retryable: retryable}
}

func nativeDataResponseForError(err error) nativeDataWireResponse {
	if err == nil {
		return nativeDataError(nativeDataResponseInternal, false)
	}
	switch {
	case errors.Is(err, raftservice.ErrServingFence):
		// A definite data-route fence remains the public failure even when the
		// trusted topology refresh also fails validation, authorization, or
		// transport. Do not misattribute internal control-plane failures to the
		// end user.
		return nativeDataError(nativeDataResponseStaleCatalog, true)
	case errors.Is(err, gateway.ErrReplicatedDataRead):
		return nativeDataError(nativeDataResponseInvalidRequest, false)
	case errors.Is(err, gateway.ErrReplicatedTableRoute):
		return nativeDataError(nativeDataResponseTableNotReplicated, false)
	case errors.Is(err, gateway.ErrReplicatedReadPositionMismatch):
		return nativeDataError(nativeDataResponsePositionMismatch, false)
	case errors.Is(err, gateway.ErrReplicatedUnauthorized):
		return nativeDataError(nativeDataResponseUnauthorized, false)
	case errors.Is(err, gateway.ErrReplicatedReadBehind):
		return nativeDataError(nativeDataResponseReadBehind, true)
	case errors.Is(err, raftmodel.ErrAdmissionBound),
		errors.Is(err, gateway.ErrReplicatedTransportBound),
		errors.Is(err, gateway.ErrReplicatedReadAdmission):
		return nativeDataError(nativeDataResponseOverloaded, true)
	case errors.Is(err, gateway.ErrReplicatedReadBufferBound):
		return nativeDataError(nativeDataResponseUnavailable, false)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded),
		errors.Is(err, gateway.ErrNoCatalog), errors.Is(err, gateway.ErrReplicatedLeader),
		errors.Is(err, gateway.ErrReplicatedRoute):
		return nativeDataError(nativeDataResponseUnavailable, true)
	}
	var refusal *gateway.ReplicatedRefusalError
	if errors.As(err, &refusal) {
		switch refusal.Code {
		case shardservice.ReplicatedRefusalUnauthorized:
			return nativeDataError(nativeDataResponseUnauthorized, false)
		case shardservice.ReplicatedRefusalReadBehind:
			return nativeDataError(nativeDataResponseReadBehind, true)
		case shardservice.ReplicatedRefusalStaleFence:
			return nativeDataError(nativeDataResponseStaleCatalog, true)
		case shardservice.ReplicatedRefusalAdmissionBound:
			return nativeDataError(nativeDataResponseOverloaded, true)
		case shardservice.ReplicatedRefusalReadBufferBound:
			return nativeDataError(nativeDataResponseUnavailable, false)
		case shardservice.ReplicatedRefusalUnavailable,
			shardservice.ReplicatedRefusalProposalRefused:
			return nativeDataError(nativeDataResponseUnavailable, true)
		}
	}
	return nativeDataError(nativeDataResponseInternal, false)
}
