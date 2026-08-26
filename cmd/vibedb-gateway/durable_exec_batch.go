package main

import (
	"context"
	"encoding/hex"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func structuredExecBatchIdentity(request *serveRequest) (durableExecBatchIdentity, bool) {
	if request == nil || request.Op != "exec_batch" {
		return durableExecBatchIdentity{}, false
	}
	structured := request.IssuerEpoch != 0 || request.IssuerLane != "" ||
		request.IssuerSequence != 0 || request.IssuerAuthenticator != ""
	if !structured {
		return durableExecBatchIdentity{}, false
	}
	identity := durableExecBatchIdentity{
		IssuerEpoch: request.IssuerEpoch, IssuerSequence: request.IssuerSequence,
	}
	if decodeLowerFixedHex(request.RequestID, identity.RequestID[:]) != nil ||
		decodeLowerFixedHex(request.IssuerLane, identity.IssuerLane[:]) != nil ||
		decodeLowerFixedHex(request.IssuerAuthenticator, identity.Authenticator[:]) != nil ||
		!validDurableExecBatchIdentity(identity) {
		return durableExecBatchIdentity{}, false
	}
	return identity, true
}

func hasAnyStructuredExecBatchIdentity(request *serveRequest) bool {
	return request != nil && (request.RequestID != "" || request.IssuerEpoch != 0 ||
		request.IssuerLane != "" || request.IssuerSequence != 0 || request.IssuerAuthenticator != "")
}

func decodeLowerFixedHex(encoded string, destination []byte) error {
	if len(encoded) != hex.EncodedLen(len(destination)) {
		return errDurableExecBatchUnavailable
	}
	for index := range encoded {
		value := encoded[index]
		if !((value >= '0' && value <= '9') || (value >= 'a' && value <= 'f')) {
			return errDurableExecBatchUnavailable
		}
	}
	written, err := hex.Decode(destination, []byte(encoded))
	if err != nil || written != len(destination) {
		return errDurableExecBatchUnavailable
	}
	var nonzero byte
	for _, value := range destination {
		nonzero |= value
	}
	if nonzero == 0 {
		return errDurableExecBatchUnavailable
	}
	return nil
}

func executeDurableExecBatch(
	ctx context.Context,
	service durableRequestService,
	authority serviceauthz.Authority,
	request serveRequest,
) *serveResponse {
	identity, ok := structuredExecBatchIdentity(&request)
	if service == nil || !authority.Valid() || !ok {
		return &serveResponse{Error: errDurableExecBatchUnavailable.Error()}
	}
	queries, err := buildBatchQueries(request)
	if err != nil {
		return &serveResponse{Error: err.Error()}
	}
	result, err := service.ExecBatch(ctx, authority, identity, queries)
	if err != nil {
		response := &serveResponse{Error: err.Error()}
		var transactionErr *gateway.ReplicatedTransactionError
		if errors.As(err, &transactionErr) && transactionErr.ID != ([16]byte{}) {
			response.TransactionID = replication.ID128(transactionErr.ID)
			response.Committed = transactionErr.Committed
			response.OutcomeUnknown = !transactionErr.Committed
		}
		return response
	}
	if result.Result == nil || !validDurableExecBatchAckRequest(&result.Ack) {
		return &serveResponse{Error: errDurableExecBatchUnavailable.Error()}
	}
	response := encodeResult(result.Result)
	if !response.Committed || response.TransactionID == (replication.ID128{}) ||
		result.Ack.Identity.RequestID != identity.RequestID ||
		result.Ack.Identity.IssuerEpoch != identity.IssuerEpoch ||
		result.Ack.Identity.IssuerLane != identity.IssuerLane ||
		result.Ack.Identity.IssuerSequence != identity.IssuerSequence {
		return &serveResponse{Error: errDurableExecBatchUnavailable.Error()}
	}
	response.DurableAck = &result.Ack
	return response
}
