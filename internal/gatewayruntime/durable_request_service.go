package gatewayruntime

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

var errDurableExecBatchUnavailable = errors.New("gateway: durable exec_batch service is unavailable")

// durableRequestService is the narrow shipped protocol seam. Authority is the
// certificate principal bound to the connection context. Implementations must
// validate issuer grants through cluster-visible authority; a process-local
// issuer-store secret is not sufficient for reconnect to another gateway.
type durableRequestService interface {
	OpenIssuer(context.Context, serviceauthz.Authority, gateway.ReplicatedIssuerOpen) (gateway.ReplicatedIssuerLaneGrant, error)
	ExecBatch(context.Context, serviceauthz.Authority, durableExecBatchIdentity, []gateway.Query) (durableExecBatchExecuteResult, error)
	AckExecBatch(context.Context, serviceauthz.Authority, durableExecBatchAckWireRequest) (durableExecBatchAckWireResponse, error)
}

type durableExecBatchIdentity struct {
	RequestID      replication.ID128
	Reference      gateway.ReplicatedIssuerReference
	IssuerSequence uint64
}

type durableExecBatchExecuteResult struct {
	Result *gateway.Result
	Ack    durableExecBatchAckWireRequest
	Direct bool
}

func validDurableExecBatchIdentity(identity durableExecBatchIdentity) bool {
	return identity.RequestID != (replication.ID128{}) &&
		identity.Reference.Installation != (replication.ID128{}) &&
		identity.Reference.Epoch != 0 &&
		identity.Reference.LaneOrdinal < gateway.MaxReplicatedIssuerLanes &&
		identity.Reference.GrantDigest != (replication.Digest{}) && identity.IssuerSequence != 0
}
