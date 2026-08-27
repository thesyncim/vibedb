package splitcontroller

import (
	"bytes"
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/splitcapture"
)

// SourceCaptureActivationProposer durably settles one canonical activation on
// the source RF3 group. Implementations must retain the exact bytes across an
// outcome-unknown response; rebuilding them at a later applied cut is invalid.
type SourceCaptureActivationProposer interface {
	ProposeSourceCaptureActivation(
		context.Context, OperationID, raftservice.ServingFence, []byte,
	) error
}

// SourceCaptureActivationProposerFactory opens the operation-scoped durable
// topology session only for activation and returns its exact handle release.
type SourceCaptureActivationProposerFactory interface {
	OpenSourceCaptureActivationProposer(
		context.Context, *Plan, Observation,
	) (SourceCaptureActivationProposer, func() error, error)
}

// RF3SourceCaptureActivationProposer binds activation to the operation's
// journal-backed topology session. The same session can subsequently perform
// retained-source pruning, preserving one bounded retry ledger per split.
type RF3SourceCaptureActivationProposer struct {
	operation OperationID
	session   *gateway.NativeSession
	tenant    []byte
	clientID  replication.ID128
}

func NewRF3SourceCaptureActivationProposer(
	operation OperationID,
	session *gateway.NativeSession,
) (*RF3SourceCaptureActivationProposer, error) {
	clientID := RetainedPruneClientID(operation)
	tenant := RetainedPruneTenant(operation)
	if operation == (OperationID{}) || session == nil || clientID == (replication.ID128{}) ||
		!session.Status().Active {
		return nil, ErrInvalidPlan
	}
	return &RF3SourceCaptureActivationProposer{
		operation: operation, session: session, tenant: tenant, clientID: clientID,
	}, nil
}

func (p *RF3SourceCaptureActivationProposer) ProposeSourceCaptureActivation(
	ctx context.Context,
	operation OperationID,
	fence raftservice.ServingFence,
	body []byte,
) error {
	view, openErr := splitcapture.OpenCommand(body)
	if p == nil || ctx == nil || operation != p.operation || openErr != nil ||
		view.Operation != [32]byte(operation) ||
		!gateway.NativeSessionMatchesControlBinding(
			p.session, fence, p.tenant, p.clientID, 1,
			serviceauthz.CapabilityTopology,
		) {
		return errors.Join(ErrInvalidPlan, openErr)
	}
	var result gateway.NativeResult
	var err error
	if p.session.Status().Pending {
		pending, pendingErr := replication.OpenCommand(p.session.PendingCommand())
		if pendingErr != nil || pending.Kind() != replication.CommandSplitCaptureActivate ||
			pending.AuthorityClass != replication.CommandAuthorityTopology ||
			!gateway.NativeSessionMatchesControlBinding(
				p.session, fence, p.tenant, p.clientID, 1,
				serviceauthz.CapabilityTopology,
			) || !bytes.Equal(pending.SplitCaptureActivationBytes(), body) {
			return errors.Join(ErrInvalidPlan, pendingErr, gateway.ErrNativeCommandPending)
		}
		result, err = p.session.RetryPending(ctx)
	} else {
		result, err = p.session.SplitCaptureActivate(ctx, body)
	}
	if err != nil {
		return err
	}
	if result.Completion.ResultCode != replicatedstate.ResultApplied ||
		result.Outcome.AppliedIndex == 0 {
		return ErrTopologyConflict
	}
	return nil
}

var _ SourceCaptureActivationProposer = (*RF3SourceCaptureActivationProposer)(nil)
