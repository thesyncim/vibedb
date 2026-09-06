package gateway

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// ErrFrontendContinuationWireUnavailable is returned when a live accepted
// frontend session has a committed continuation credential but the selected
// native request carrier has not yet installed the corresponding outer wire
// envelope. Dropping the credential silently would turn a Draining request
// into an unauthorised legacy request, so this is deliberately fail closed.
var ErrFrontendContinuationWireUnavailable = errors.New("gateway: frontend continuation wire unavailable")

// frontendContinuationRequestCarrier is implemented by shardservice's native
// request type at the wire boundary. The method must only replace the outer
// authorization envelope; command/query bytes remain byte-for-byte unchanged.
// Keeping this as a narrow interface lets local semantic dispatch and remote
// framing share one binding without making gateway own shardservice codecs.
type frontendContinuationRequestCarrier interface {
	SetFrontendContinuation(serviceauthz.FrontendContinuationEnvelope) error
}

type frontendContinuationRequestReader interface {
	FrontendContinuation() (serviceauthz.FrontendContinuationEnvelope, bool)
}

// attachFrontendContinuation binds the dynamic accepted-socket credential to
// the exact operation/resource tuple already present in a native request. A
// missing credential is the ordinary Active/legacy path. Once the catalog has
// committed a grant, every local and remote retry must carry it or fail closed.
func attachFrontendContinuation(ctx context.Context, request *shardservice.ReplicatedRequest) error {
	if ctx == nil || request == nil {
		return ErrFrontendContinuationWireUnavailable
	}
	credential, ok := serviceauthz.FrontendContinuationFromContext(ctx)
	if !ok {
		return nil
	}
	scope, ok := replicatedContinuationScope(request)
	if !ok {
		return ErrFrontendContinuationWireUnavailable
	}
	scope.Protocol = credential.Protocol
	envelope, ok := serviceauthz.FrontendContinuationEnvelopeFromContext(ctx, scope)
	if !ok {
		return ErrFrontendContinuationWireUnavailable
	}
	carrier, ok := any(request).(frontendContinuationRequestCarrier)
	if !ok {
		return ErrFrontendContinuationWireUnavailable
	}
	if reader, readable := any(request).(frontendContinuationRequestReader); readable {
		if existing, present := reader.FrontendContinuation(); present {
			if existing == envelope {
				return nil
			}
			return ErrFrontendContinuationWireUnavailable
		}
	}
	return carrier.SetFrontendContinuation(envelope)
}

// replicatedContinuationScope maps the already authenticated native request
// union to the closed serviceauthz continuation scope. It never trusts a
// caller supplied action: capability and operation are derived from the wire
// operation selected by the gateway.
func replicatedContinuationScope(request *shardservice.ReplicatedRequest) (serviceauthz.FrontendContinuationScopeRecord, bool) {
	return shardservice.FrontendContinuationScopeForReplicatedRequest(request)
}
