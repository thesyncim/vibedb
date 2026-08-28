package main

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var errInvalidDurableRequestAdapter = errors.New("gateway: invalid replicated durable request adapter")

type replicatedDurableIssuer interface {
	OpenIssuerLane(context.Context, serviceauthz.Authority, gateway.ReplicatedIssuerOpen) (gateway.ReplicatedIssuerLaneGrant, error)
	ValidateRequest(context.Context, serviceauthz.Authority, gateway.ReplicatedIssuerReference, requestledger.RequestID, uint64) (requestledger.RequestKey, error)
	ValidateAcknowledge(context.Context, serviceauthz.Authority, gateway.ReplicatedIssuerReference, requestledger.RequestID, uint64) (requestledger.RequestKey, error)
}

type replicatedDurableSQL interface {
	Execute(context.Context, requestledger.RequestKey, []byte, []gateway.Query) (gateway.DurableSQLRequestResult, error)
	Acknowledge(context.Context, gateway.DurableRequestLedgerKey, uint64, replication.Digest, gateway.DurableRequestAckToken) (gateway.DurableRequestAckResult, error)
}

// replicatedDurableRequestService is the only shipped RF3 exec_batch adapter.
// It authenticates the immutable issuer grant before constructing a key and
// never falls back to the process-local transaction registry.
type replicatedDurableRequestService struct {
	issuers replicatedDurableIssuer
	sql     replicatedDurableSQL
}

func newReplicatedDurableRequestService(
	issuers *gateway.ReplicatedIssuerAuthority,
	sql *gateway.DurableSQLRequestExecutor,
) (*replicatedDurableRequestService, error) {
	if issuers == nil || sql == nil {
		return nil, errInvalidDurableRequestAdapter
	}
	return newReplicatedDurableRequestServiceWithDependencies(issuers, sql)
}

func newReplicatedDurableRequestServiceWithDependencies(
	issuers replicatedDurableIssuer,
	sql replicatedDurableSQL,
) (*replicatedDurableRequestService, error) {
	if issuers == nil || sql == nil {
		return nil, errInvalidDurableRequestAdapter
	}
	return &replicatedDurableRequestService{issuers: issuers, sql: sql}, nil
}

func (service *replicatedDurableRequestService) OpenIssuer(
	ctx context.Context,
	authority serviceauthz.Authority,
	open gateway.ReplicatedIssuerOpen,
) (gateway.ReplicatedIssuerLaneGrant, error) {
	if service == nil || service.issuers == nil || ctx == nil || !authority.Valid() {
		return gateway.ReplicatedIssuerLaneGrant{}, errInvalidDurableRequestAdapter
	}
	return service.issuers.OpenIssuerLane(ctx, authority, open)
}

func (service *replicatedDurableRequestService) ExecBatch(
	ctx context.Context,
	authority serviceauthz.Authority,
	identity durableExecBatchIdentity,
	queries []gateway.Query,
) (durableExecBatchExecuteResult, error) {
	if service == nil || service.issuers == nil || service.sql == nil || ctx == nil ||
		!authority.Valid() || !validDurableExecBatchIdentity(identity) || len(queries) == 0 {
		return durableExecBatchExecuteResult{}, errInvalidDurableRequestAdapter
	}
	key, err := service.issuers.ValidateRequest(ctx, authority, identity.Reference,
		requestledger.RequestID(identity.RequestID), identity.IssuerSequence)
	if err != nil {
		return durableExecBatchExecuteResult{}, err
	}
	tenant, err := authenticatedIssuerTenantFor(authority)
	if err != nil {
		return durableExecBatchExecuteResult{}, err
	}
	result, err := service.sql.Execute(ctx, key, tenant[:], queries)
	if err != nil && !errors.Is(err, gateway.ErrDurableSQLAborted) {
		return durableExecBatchExecuteResult{}, err
	}
	return durableAdapterResult(identity, key, result, err)
}

func (service *replicatedDurableRequestService) ReplayBatch(ctx context.Context, authority serviceauthz.Authority, identity durableExecBatchIdentity, queries []gateway.Query) (durableExecBatchExecuteResult, bool, error) {
	recovery, ok := service.sql.(interface {
		ReplayRequest(context.Context, requestledger.RequestKey, []gateway.Query) (gateway.DurableSQLRequestResult, bool, error)
	})
	if !ok {
		return durableExecBatchExecuteResult{}, false, errInvalidDurableRequestAdapter
	}
	key, err := service.issuers.ValidateRequest(ctx, authority, identity.Reference, requestledger.RequestID(identity.RequestID), identity.IssuerSequence)
	if err != nil {
		return durableExecBatchExecuteResult{}, false, err
	}
	result, found, err := recovery.ReplayRequest(ctx, key, queries)
	if !found || err != nil && !errors.Is(err, gateway.ErrDurableSQLAborted) {
		return durableExecBatchExecuteResult{}, found, err
	}
	response, err := durableAdapterResult(identity, key, result, err)
	return response, true, err
}

func durableAdapterResult(identity durableExecBatchIdentity, key requestledger.RequestKey, result gateway.DurableSQLRequestResult, outcomeErr error) (durableExecBatchExecuteResult, error) {
	if result.Result == nil || result.Key.RequestKey != key || result.Key.Digest == (replication.Digest{}) ||
		result.TerminalRevision == 0 || result.ResultDigest == (replication.Digest{}) ||
		result.AckToken == (gateway.DurableRequestAckToken{}) {
		return durableExecBatchExecuteResult{}, errInvalidDurableRequestAdapter
	}
	ack := durableExecBatchAckWireRequest{
		Identity: durableExecBatchAckIdentity{
			RequestID: identity.RequestID, RequestDigest: result.Key.Digest,
			Reference: identity.Reference, IssuerSequence: identity.IssuerSequence,
		},
		TerminalRevision: result.TerminalRevision,
		ResultDigest:     result.ResultDigest,
		AckToken:         requestledger.AckToken(result.AckToken),
	}
	if !validDurableExecBatchAckRequest(&ack) {
		return durableExecBatchExecuteResult{}, errInvalidDurableRequestAdapter
	}
	return durableExecBatchExecuteResult{Result: result.Result, Ack: ack}, outcomeErr
}

func (service *replicatedDurableRequestService) AckExecBatch(
	ctx context.Context,
	authority serviceauthz.Authority,
	request durableExecBatchAckWireRequest,
) (durableExecBatchAckWireResponse, error) {
	if service == nil || service.issuers == nil || service.sql == nil || ctx == nil ||
		!authority.Valid() || !validDurableExecBatchAckRequest(&request) {
		return durableExecBatchAckWireResponse{}, errInvalidDurableRequestAdapter
	}
	requestKey, err := service.issuers.ValidateAcknowledge(ctx, authority,
		request.Identity.Reference, requestledger.RequestID(request.Identity.RequestID),
		request.Identity.IssuerSequence)
	if err != nil {
		return durableExecBatchAckWireResponse{}, err
	}
	key, err := gateway.NewDurableRequestLedgerKey(requestKey, request.Identity.RequestDigest)
	if err != nil {
		return durableExecBatchAckWireResponse{}, err
	}
	result, err := service.sql.Acknowledge(ctx, key, request.TerminalRevision,
		request.ResultDigest, gateway.DurableRequestAckToken(request.AckToken))
	if err != nil {
		return durableExecBatchAckWireResponse{}, err
	}
	response := durableExecBatchAckWireResponse{
		durableExecBatchAckWireRequest: request,
		Applied:                        result.Applied,
		CollectionRounds:               result.Rounds,
	}
	if !validDurableExecBatchAckResponse(&response) {
		return durableExecBatchAckWireResponse{}, errInvalidDurableRequestAdapter
	}
	return response, nil
}

var _ durableRequestService = (*replicatedDurableRequestService)(nil)
