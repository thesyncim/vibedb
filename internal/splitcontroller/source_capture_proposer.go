package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/splitcapture"
)

var sourceCaptureClientDomain = []byte("vibedb/split-controller/source-capture-client\x00")

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
	RetireSourceCaptureActivationSession(context.Context, *Plan, Observation) error
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
	clientID := SourceCaptureClientID(operation)
	tenant := SourceCaptureTenant(operation)
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
		pendingCapture, captureErr := pending.OpenSplitCaptureActivation()
		if pendingErr != nil || pending.Kind() != replication.CommandSplitCaptureActivate ||
			captureErr != nil || !sameSourceCaptureAuthority(pendingCapture.Command, view.Command) ||
			pending.AuthorityClass != replication.CommandAuthorityTopology ||
			!gateway.NativeSessionMatchesControlBinding(
				p.session, fence, p.tenant, p.clientID, 1,
				serviceauthz.CapabilityTopology,
			) {
			return errors.Join(ErrInvalidPlan, pendingErr, captureErr, gateway.ErrNativeCommandPending)
		}
		// The journal's exact bytes own an outcome-unknown proposal. A newer
		// coherent observation must not substitute its cut into that retry.
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

func sameSourceCaptureAuthority(left, right splitcapture.Command) bool {
	return left.Operation == right.Operation && left.PlanDigest == right.PlanDigest &&
		left.PartitionerDigest == right.PartitionerDigest && left.RelationManifestDigest == right.RelationManifestDigest &&
		left.LineageDigest == right.LineageDigest && left.BindingDigest == right.BindingDigest &&
		left.SourceGeneration == right.SourceGeneration && left.SchemaGeneration == right.SchemaGeneration &&
		bytes.Equal(left.Spec, right.Spec)
}

var _ SourceCaptureActivationProposer = (*RF3SourceCaptureActivationProposer)(nil)

// SourceCaptureClientID is distinct from retained-prune authority because
// capture is admitted on the parent route while prune runs after cutover on the
// retained child's advanced route generation.
func SourceCaptureClientID(operation OperationID) replication.ID128 {
	var input [len("vibedb/split-controller/source-capture-client\x00") + sha256.Size]byte
	copy(input[:], sourceCaptureClientDomain)
	copy(input[len(sourceCaptureClientDomain):], operation[:])
	sum := sha256.Sum256(input[:])
	var id replication.ID128
	copy(id[:], sum[:len(id)])
	return id
}

// SourceCaptureTenant returns the printable operation-scoped activation
// authority. It is stable across source leader replacement.
func SourceCaptureTenant(operation OperationID) []byte {
	const prefix = "split-capture:"
	const hex = "0123456789abcdef"
	var tenant [len(prefix) + 2*sha256.Size]byte
	copy(tenant[:], prefix)
	for index, value := range operation {
		tenant[len(prefix)+2*index] = hex[value>>4]
		tenant[len(prefix)+2*index+1] = hex[value&15]
	}
	return tenant[:]
}
