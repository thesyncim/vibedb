package main

import (
	"context"
	"encoding/hex"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func structuredExecBatchIdentity(request *serveRequest) (durableExecBatchIdentity, bool) {
	if request == nil || request.Op != "exec_batch" {
		return durableExecBatchIdentity{}, false
	}
	if request.wireIdentitySet {
		if request.IssuerLane != "" || request.IssuerAuthenticator != "" ||
			!validDurableExecBatchIdentity(request.wireIdentity) {
			return durableExecBatchIdentity{}, false
		}
		return request.wireIdentity, true
	}
	structured := request.InstallationID != "" || request.IssuerEpoch != 0 ||
		request.GrantDigest != "" || request.IssuerSequence != 0
	if !structured {
		return durableExecBatchIdentity{}, false
	}
	identity := durableExecBatchIdentity{
		Reference:      gateway.ReplicatedIssuerReference{Epoch: request.IssuerEpoch, LaneOrdinal: request.LaneOrdinal},
		IssuerSequence: request.IssuerSequence,
	}
	if decodeLowerFixedHex(request.RequestID, identity.RequestID[:]) != nil ||
		decodeLowerFixedHex(request.InstallationID, identity.Reference.Installation[:]) != nil ||
		decodeLowerFixedHex(request.GrantDigest, identity.Reference.GrantDigest[:]) != nil ||
		request.IssuerLane != "" || request.IssuerAuthenticator != "" ||
		!validDurableExecBatchIdentity(identity) {
		return durableExecBatchIdentity{}, false
	}
	return identity, true
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
		return &serveResponse{Error: err.Error()}
	}
	if result.Result == nil || !result.Direct && !validDurableExecBatchAckRequest(&result.Ack) ||
		result.Direct && result.Ack != (durableExecBatchAckWireRequest{}) {
		return &serveResponse{Error: errDurableExecBatchUnavailable.Error()}
	}
	response := encodeResult(result.Result)
	if !response.Committed || response.TransactionID == (replication.ID128{}) ||
		!result.Direct && (result.Ack.Identity.RequestID != identity.RequestID ||
			result.Ack.Identity.Reference != identity.Reference ||
			result.Ack.Identity.IssuerSequence != identity.IssuerSequence) {
		return &serveResponse{Error: errDurableExecBatchUnavailable.Error()}
	}
	if !result.Direct {
		response.DurableAck = &result.Ack
	}
	return response
}
