package gateway

import (
	"context"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

const DefaultDurableRequestExecutionPinSpan = uint64(1 << 20)

// DurableRequestExecutionPinSessionFactory opens the journal-backed,
// serialized session for one ledger-home group. Reopening after an
// outcome-unknown proposal must restore that session's exact pending bytes.
type DurableRequestExecutionPinSessionFactory interface {
	OpenExecutionPinSession(
		context.Context,
		DurableRequestTypedExecutionContext,
		ReplicatedRoute,
	) (*NativeSession, serviceauthz.Authority, func(), error)
	RetireTerminalExecutionPinSession(
		context.Context,
		DurableRequestTypedExecutionContext,
		ReplicatedRoute,
	) error
	RetireAcknowledgedExecutionPinSession(
		context.Context,
		executionpin.PinID,
		ReplicatedRoute,
		replication.Digest,
	) error
}

// RetireTerminal is idempotent and only removes session files after the
// caller has proved the ledger terminal and execution-pin release witnesses.
func (authority *NativeDurableRequestExecutionPinAuthority) RetireTerminal(
	ctx context.Context,
	execution DurableRequestTypedExecutionContext,
) error {
	if authority == nil || authority.sessions == nil || ctx == nil ||
		!validReplicatedRoute(execution.Home.borrowedRoute()) {
		return ErrDurableRequest
	}
	return authority.sessions.RetireTerminalExecutionPinSession(
		ctx, execution, execution.Home.borrowedRoute(),
	)
}

// RetireAcknowledged is the cross-gateway cleanup path. The replicated ACK
// permanently binds the pin and release certificate even after plan and
// terminal rows have been reclaimed, so another gateway can safely delete its
// stale local exact-retry journal without reconstructing collected recipe
// bytes.
func (authority *NativeDurableRequestExecutionPinAuthority) RetireAcknowledged(
	ctx context.Context,
	home DurableRequestLedgerHome,
	ack requestledger.AckRecord,
) error {
	if authority == nil || authority.sessions == nil || ctx == nil ||
		!validReplicatedRoute(home.borrowedRoute()) || ack.PinID == (requestledger.PinID{}) ||
		ack.PinDigest == (requestledger.Digest{}) ||
		ack.ReleaseCertificateDigest == (requestledger.Digest{}) {
		return ErrDurableRequest
	}
	pin, err := executionpin.DerivePinIDFromBindingDigest(executionpin.Digest(ack.PinDigest))
	if err != nil {
		return errors.Join(err, ErrDurableRequestConflict)
	}
	return authority.sessions.RetireAcknowledgedExecutionPinSession(
		ctx, pin, home.borrowedRoute(), replication.Digest(ack.ReleaseCertificateDigest),
	)
}

// NativeDurableRequestExecutionPinAuthority turns the clockless execution-pin
// kernel into the shipped acquire/recover boundary. A ReadIndex observation
// chooses the transition; replicated apply is the only authority that wins it.
type NativeDurableRequestExecutionPinAuthority struct {
	executor *ReplicatedExecutor
	sessions DurableRequestExecutionPinSessionFactory
	span     uint64
}

func NewNativeDurableRequestExecutionPinAuthority(
	executor *ReplicatedExecutor,
	sessions DurableRequestExecutionPinSessionFactory,
	span uint64,
) (*NativeDurableRequestExecutionPinAuthority, error) {
	if span == 0 {
		span = DefaultDurableRequestExecutionPinSpan
	}
	if executor == nil || sessions == nil || span == 0 || span > math.MaxInt64 {
		return nil, ErrDurableRequest
	}
	return &NativeDurableRequestExecutionPinAuthority{
		executor: executor, sessions: sessions, span: span,
	}, nil
}

func (authority *NativeDurableRequestExecutionPinAuthority) AcquireOrRecover(
	ctx context.Context,
	execution DurableRequestTypedExecutionContext,
) (ReplicatedRoute, executionpin.AcquireCertificate, executionpin.LeaseCertificate, error) {
	if authority == nil || authority.executor == nil || authority.sessions == nil || ctx == nil {
		return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{}, ErrDurableRequest
	}
	binding, err := BuildDurableRequestExecutionPinBinding(execution)
	if err != nil {
		return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{}, err
	}
	bindingDigest, err := executionpin.BindingDigest(binding)
	pin, pinErr := executionpin.DerivePinID(binding)
	route := execution.Home.borrowedRoute()
	if err != nil || pinErr != nil || replication.Digest(bindingDigest) != execution.Recipe.Contract.PinDigest ||
		!validReplicatedRoute(route) {
		return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{},
			errors.Join(err, pinErr, ErrDurableRequestConflict)
	}
	session, principal, releaseSession, openErr := authority.sessions.OpenExecutionPinSession(ctx, execution, route)
	if releaseSession != nil {
		defer releaseSession()
	}
	if openErr != nil || session == nil || !principal.Valid() || session.executor != authority.executor ||
		session.proposalCapability != serviceauthz.CapabilityExecutionPin ||
		!sameReplicatedCatalogRoute(session.route, route) ||
		session.retryHome != execution.Recipe.Identity.RetryHome {
		return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{},
			errors.Join(openErr, ErrDurableRequestConflict)
	}
	authorizedCtx, authErr := serviceauthz.WithAuthority(ctx, principal)
	if authErr != nil {
		return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{}, authErr
	}

	for attempt := 0; attempt < 4; attempt++ {
		read, readErr := authority.executor.ReadExecutionPin(authorizedCtx, route, ReplicatedExecutionPinRead{
			Pin: pin, MinimumApplied: 1,
		})
		if readErr != nil {
			return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{}, readErr
		}
		if read.Found {
			acquire, acquireOK := read.Record.AcquireCertificate()
			lease, leaseOK := read.Record.LeaseCertificate()
			if read.Record.Binding != binding || !acquireOK || !leaseOK {
				return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{}, ErrDurableRequestConflict
			}
			if executionpin.ValidateSideEffectFence(lease, read.Record, read.Applied) == nil {
				return route, acquire, lease, nil
			}
			if read.Record.Status != executionpin.StatusActive || read.Applied <= read.Record.LeaseAppliedThrough {
				return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{}, ErrDurableRequestConflict
			}
		}

		if session.pending {
			outer, openCommandErr := replication.OpenCommand(session.command)
			nested, nestedErr := outer.OpenExecutionPin()
			if openCommandErr != nil || nestedErr != nil || nested.PinID != pin || nested.Binding != binding ||
				(nested.Operation != executionpin.OperationAcquire && nested.Operation != executionpin.OperationRecover) {
				return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{},
					errors.Join(openCommandErr, nestedErr, ErrDurableRequestConflict)
			}
			if _, retryErr := session.RetryPending(authorizedCtx); retryErr != nil {
				return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{}, retryErr
			}
			continue
		}

		transition := executionpin.Command{
			Operation: executionpin.OperationAcquire, Binding: binding, PinID: pin,
			AuthorityNode: executionpin.ID(principal.Node), AuthorityGeneration: principal.Generation,
			NextController: executionpin.ID(principal.Node), NextControllerEpoch: 1,
			NextLeaseSpan: authority.span,
		}
		if read.Found {
			if read.Record.ControllerEpoch == math.MaxUint64 {
				return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{}, ErrDurableRequestBound
			}
			acquire, ok := read.Record.AcquireCertificate()
			if !ok {
				return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{}, ErrDurableRequestConflict
			}
			acquireDigest, digestErr := executionpin.AcquireCertificateDigest(acquire)
			if digestErr != nil {
				return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{}, digestErr
			}
			transition.Operation = executionpin.OperationRecover
			transition.ExpectedController = read.Record.Controller
			transition.ExpectedControllerEpoch = read.Record.ControllerEpoch
			transition.ExpectedLeaseAppliedThrough = read.Record.LeaseAppliedThrough
			transition.ExpectedLeaseRevision = read.Record.LeaseRevision
			transition.NextControllerEpoch = read.Record.ControllerEpoch + 1
			transition.AcquireCertificateDigest = acquireDigest
		}
		if _, proposeErr := session.ExecutionPin(authorizedCtx, transition); proposeErr != nil {
			return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{}, proposeErr
		}
	}
	return ReplicatedRoute{}, executionpin.AcquireCertificate{}, executionpin.LeaseCertificate{}, ErrDurableRequestUnresolved
}

var _ DurableRequestExecutionPinAuthority = (*NativeDurableRequestExecutionPinAuthority)(nil)
