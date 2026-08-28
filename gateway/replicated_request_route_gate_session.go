package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type durableRequestRouteGateSessions interface {
	prepareAcquire(context.Context, ReplicatedRoute, DurableRequestWave, requestledger.HeadRecord) ([]byte, requestledger.Digest, error)
	prepareRelease(context.Context, ReplicatedRoute, DurableRequestWave, requestledger.RoutePinRecord) ([]byte, error)
	cleanup(context.Context, ReplicatedRoute, DurableRequestWave, requestledger.RoutePinRecord) error
}

// The replicated RoutePin row is this session's exact-command journal. Open
// has fixed bytes and is retryable until the acquire intent exists. After that
// point recovery uses only the session epoch and sequence in that retained
// intent; it never reopens a retired SessionOpen result. No gateway-local file
// is required to recover on a different gateway.
type nativeDurableRequestRouteGateSessions struct {
	executor *ReplicatedExecutor
}

func durableRouteSessionIdentity(identity requestledger.Digest, route ReplicatedRoute, tenant []byte) (replication.ID128, error) {
	physical, err := NativeSessionJournalBinding(route, string(route.Distribution), string(route.Shard), tenant, 1, serviceauthz.CapabilityDataWrite)
	if err != nil || identity == (requestledger.Digest{}) {
		return replication.ID128{}, errors.Join(err, ErrDurableRequest)
	}
	const domain = "vibedb/request-route-session\x00"
	var raw [len(domain) + 2*sha256.Size]byte
	at := copy(raw[:], domain)
	at += copy(raw[at:], identity[:])
	copy(raw[at:], physical[:])
	digest := sha256.Sum256(raw[:])
	var id replication.ID128
	copy(id[:], digest[:len(id)])
	return id, nil
}

func (driver *nativeDurableRequestRouteGateSessions) session(route ReplicatedRoute, wave DurableRequestWave, identity requestledger.Digest) (*NativeSession, error) {
	id, err := durableRouteSessionIdentity(identity, route, wave.Tenant)
	if err != nil {
		return nil, err
	}
	return NewNativeSession(NativeSessionOptions{
		Executor: driver.executor, Route: route, Distribution: string(route.Distribution), Shard: string(route.Shard),
		Tenant: wave.Tenant, ClientID: id, RetryHome: wave.Identity.RetryHome,
		ScopedCoordination: true,
		Resolver:           BaseRelationResolver{Relation: 1}, ProposalCapability: serviceauthz.CapabilityDataWrite,
		MaxRelationBatches: 1, MaxMutations: 1,
		InitialCommandBytes: requestledger.MaxRouteGatePinCommandBytes,
		MaxCommandBytes:     requestledger.MaxRouteGatePinCommandBytes,
	})
}

func (driver *nativeDurableRequestRouteGateSessions) prepareAcquire(ctx context.Context, route ReplicatedRoute, wave DurableRequestWave, head requestledger.HeadRecord) ([]byte, requestledger.Digest, error) {
	identity, err := requestledger.DeriveRouteGateIdentity(head.KeyDigest, head.RequestDigest, head.PlanRoot, head.ContinuationDigest, wave.PinID, head.NextStepOrdinal)
	if err != nil {
		return nil, requestledger.Digest{}, err
	}
	read, err := driver.executor.ReadRouteGate(ctx, route, 1)
	if err != nil {
		return nil, requestledger.Digest{}, err
	}
	if read.Status.Drain.State == routegate.DrainPending || read.Status.Drain.State == routegate.DrainActive {
		return nil, requestledger.Digest{}, ErrDurableRequestUnresolved
	}
	wave.GateEpoch = read.Status.Epoch
	session, err := driver.session(route, wave, identity)
	if err != nil {
		return nil, requestledger.Digest{}, err
	}
	if _, err = session.Open(ctx, math.MaxInt64); err != nil {
		return nil, requestledger.Digest{}, err
	}
	return appendDurableRequestRouteGateCommand(nil, route, wave, head.KeyDigest, head.RequestDigest,
		head.PlanRoot, head.ContinuationDigest, head.NextStepOrdinal, routegate.OperationAcquireShared, session)
}

// settledSession derives state only from an authenticated ledger completion.
// It neither probes a new epoch nor replays a gate operation whose result may
// already have been acknowledged by the following session command.
func (driver *nativeDurableRequestRouteGateSessions) settledSession(route ReplicatedRoute, wave DurableRequestWave, pin requestledger.RoutePinRecord) (*NativeSession, replication.CommandView, error) {
	outer, err := replication.OpenCommand(pin.Command)
	completion, completionErr := replication.OpenCompletion(pin.Completion)
	if err != nil || completionErr != nil || !commandMatchesRoute(pin.Command, route) ||
		outer.Kind() != replication.CommandRouteGate || !bytes.Equal(outer.Tenant, wave.Tenant) ||
		outer.RetryHome != wave.Identity.RetryHome || !nativeCompletionMatches(outer, completion) ||
		!validDurableRequestSettlement(pin.Command, ReplicatedResult{Completion: pin.Completion,
			Outcome: raftserve.Outcome{AppliedIndex: completion.AppliedSequence}}) {
		return nil, replication.CommandView{}, errors.Join(err, completionErr, ErrDurableRequestConflict)
	}
	gate, err := outer.OpenRouteGate()
	identity, identityErr := requestledger.DeriveRouteGateIdentity(pin.KeyDigest, pin.RequestDigest, pin.PlanRoot, pin.PriorContinuationDigest, pin.PinID, pin.WaveOrdinal)
	if err != nil || identityErr != nil || gate.Identity != routegate.Identity(identity) {
		return nil, replication.CommandView{}, errors.Join(err, identityErr, ErrDurableRequestConflict)
	}
	session, err := driver.session(route, wave, identity)
	if session != nil {
		session.scopedCoordination = outer.AuthorityClass == replication.CommandAuthorityRouteSession
	}
	if err != nil || session.clientID != outer.ClientID || outer.ClientEpoch == 0 ||
		(outer.AuthorityClass != replication.CommandAuthorityData && outer.AuthorityClass != replication.CommandAuthorityRouteSession) ||
		(pin.Phase == requestledger.RoutePinAcquired && (gate.Operation != routegate.OperationAcquireShared || outer.ClientSequence != 2 || outer.AckThrough != 1)) ||
		(pin.Phase == requestledger.RoutePinReleased && (gate.Operation != routegate.OperationReleaseShared || outer.ClientSequence != 3 || outer.AckThrough != 2)) ||
		(pin.Phase != requestledger.RoutePinAcquired && pin.Phase != requestledger.RoutePinReleased) {
		return nil, replication.CommandView{}, errors.Join(err, ErrDurableRequestConflict)
	}
	session.phase, session.epoch, session.leaseDeadline = nativeSessionActive, outer.ClientEpoch, math.MaxInt64
	session.finishCompletion(outer, completion)
	return session, outer, nil
}

func (driver *nativeDurableRequestRouteGateSessions) prepareRelease(_ context.Context, route ReplicatedRoute, wave DurableRequestWave, pin requestledger.RoutePinRecord) ([]byte, error) {
	if pin.Phase != requestledger.RoutePinAcquired {
		return nil, ErrDurableRequestConflict
	}
	session, outer, err := driver.settledSession(route, wave, pin)
	if err != nil {
		return nil, err
	}
	gate, err := outer.OpenRouteGate()
	if err != nil {
		return nil, err
	}
	gate.Operation = routegate.OperationReleaseShared
	if err = session.prepareRouteGate(gate); err != nil {
		return nil, err
	}
	return session.PendingCommand(), nil
}

// Cleanup is derived from, and must finish before discarding, the released
// RoutePin witness. A lost retire/release response can therefore be retried
// byte-for-byte on another gateway without retaining an unbounded session map.
func (driver *nativeDurableRequestRouteGateSessions) cleanup(ctx context.Context, route ReplicatedRoute, wave DurableRequestWave, pin requestledger.RoutePinRecord) error {
	if pin.Phase != requestledger.RoutePinReleased {
		return ErrDurableRequestConflict
	}
	session, _, err := driver.settledSession(route, wave, pin)
	if err != nil {
		return err
	}
	if _, err = session.Retire(ctx); err != nil {
		if !errors.Is(err, replicatedstate.ErrRetryRetired) {
			return err
		}
		// Release may have committed and reclaimed the retire result. A retired
		// retry alone is NOT cleanup proof: submit the exact release of the
		// deterministically derived terminal command and require its authenticated
		// released outcome. It cannot release a different session epoch.
		retire := session.commandHeader(replication.CommandSessionRetire, session.epoch, 4, 3)
		session.clearPending()
		session.phase, session.terminalSequence = nativeSessionRetired, 4
		session.terminalFingerprint = nativeCommandFingerprint(retire)
	}
	_, err = session.Release(ctx)
	return err
}

func (runner *DurableRequestLifecycleRunner) cleanupRouteGateSession(ctx context.Context, wave DurableRequestWave, pin requestledger.RoutePinRecord) error {
	if runner.gateSessions == nil {
		return ErrDurableRequest
	}
	var route ReplicatedRoute
	var err error
	if resolver, ok := runner.resolver.(interface {
		resolveDurableSessionRoute(context.Context, []byte) (ReplicatedRoute, error)
	}); ok {
		route, err = resolver.resolveDurableSessionRoute(ctx, pin.Command)
	} else {
		route, err = runner.resolvePersistedRoute(ctx, wave, pin.Command)
	}
	if err != nil {
		return err
	}
	return runner.gateSessions.cleanup(ctx, route, wave, pin)
}
