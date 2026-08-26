package main

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var errDurableExecBatchUnavailable = errors.New("gateway: durable exec_batch service is unavailable")

// durableRequestService is the narrow shipped protocol seam. Authority is the
// certificate principal bound to the connection context. Implementations must
// validate issuer grants through cluster-visible authority; a process-local
// issuer-store secret is not sufficient for reconnect to another gateway.
type durableRequestService interface {
	OpenIssuer(context.Context, serviceauthz.Authority, uint16) (durableIssuerOpenResult, error)
	ExecBatch(context.Context, serviceauthz.Authority, durableExecBatchIdentity, []gateway.Query) (durableExecBatchExecuteResult, error)
	AckExecBatch(context.Context, serviceauthz.Authority, durableExecBatchAckWireRequest) (durableExecBatchAckWireResponse, error)
}

type durableIssuerOpenResult struct {
	Installation  replication.ID128
	Epoch         uint64
	Lane          requestledger.IssuerLane
	Authenticator replication.Digest
}

type durableExecBatchIdentity struct {
	RequestID      replication.ID128
	IssuerEpoch    uint64
	IssuerLane     requestledger.IssuerLane
	IssuerSequence uint64
	Authenticator  replication.Digest
}

type durableExecBatchExecuteResult struct {
	Result *gateway.Result
	Ack    durableExecBatchAckWireRequest
}

func validDurableIssuerOpenResult(result durableIssuerOpenResult) bool {
	return result.Installation != (replication.ID128{}) && result.Epoch != 0 &&
		result.Lane != (requestledger.IssuerLane{}) && result.Authenticator != (replication.Digest{})
}

func validDurableExecBatchIdentity(identity durableExecBatchIdentity) bool {
	return identity.RequestID != (replication.ID128{}) && identity.IssuerEpoch != 0 &&
		identity.IssuerLane != (requestledger.IssuerLane{}) && identity.IssuerSequence != 0 &&
		identity.Authenticator != (replication.Digest{})
}
