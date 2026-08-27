package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"net"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type rf3RetainedPruneFactory struct {
	tls       *rafttransport.PeerTLS
	authority serviceauthz.Authority
	lease     *splitcontroller.RuntimeStoreLease
	source    *sqldriver.ReplicatedApply
}

func (factory *rf3RetainedPruneFactory) RetireSourceCaptureActivationSession(ctx context.Context, plan *splitcontroller.Plan, observed splitcontroller.Observation) error {
	if factory == nil || factory.source == nil || plan == nil || observed.Catalog == nil {
		return errRF3Serving
	}
	cut, err := factory.source.RangeSplitSnapshot()
	if err != nil {
		return err
	}
	defer cut.Close()
	activation, found, err := cut.SplitCaptureActivation()
	if err != nil || !found || activation.Command.Operation != [32]byte(plan.OperationID()) {
		return errors.Join(err, splitcontroller.ErrSourceCaptureActivation)
	}
	state := cut.State()
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, found := observed.Catalog.ResolveReplicatedRoute(distribution.DistributionName(state.Binding.Distribution), distribution.ShardID(state.Binding.Shard), replicas[:0])
	if !found {
		return errRF3Serving
	}
	pool, executor, err := newRF3SplitTopologyTransport(factory.tls)
	if err != nil {
		return err
	}
	defer pool.Close()
	authorized, err := serviceauthz.WithAuthority(ctx, factory.authority)
	if err != nil {
		return err
	}
	client := splitcontroller.SourceCaptureClientID(plan.OperationID())
	cleanup, err := gateway.NewNativeTopologySessionCleanup(gateway.NativeSessionOptions{
		Executor: executor, Route: route, Distribution: state.Binding.Distribution, Shard: state.Binding.Shard,
		Tenant: splitcontroller.SourceCaptureTenant(plan.OperationID()), ClientID: client,
		RetryHome: rf3SplitTopologyRetryHome([]byte("vibedb/split-capture/retry-home\x00"), client),
		Resolver:  gateway.BaseRelationResolver{Relation: 1}, ProposalCapability: serviceauthz.CapabilityTopology,
		MaxRelationBatches: 1, MaxMutations: 1, InitialCommandBytes: 4 << 10, MaxCommandBytes: 4 << 10,
	}, cut)
	if err != nil {
		return err
	}
	if err := cut.Close(); err != nil {
		return err
	}
	return cleanup.Run(authorized)
}

func (factory *rf3RetainedPruneFactory) OpenSourceCaptureActivationProposer(
	ctx context.Context, plan *splitcontroller.Plan, observed splitcontroller.Observation,
) (splitcontroller.SourceCaptureActivationProposer, func() error, error) {
	if factory == nil || ctx == nil || factory.tls == nil || factory.lease == nil ||
		plan == nil || observed.Catalog == nil {
		return nil, nil, errRF3Serving
	}
	session, release, err := openRF3SplitTopologySession(
		ctx, observed.Catalog, observed.SourceState.Binding,
		factory.tls, factory.authority, factory.lease,
		splitcontroller.SourceCaptureClientID(plan.OperationID()),
		splitcontroller.SourceCaptureTenant(plan.OperationID()),
		[]byte("vibedb/split-capture/retry-home\x00"), true,
	)
	if err != nil {
		return nil, nil, err
	}
	proposer, err := splitcontroller.NewRF3SourceCaptureActivationProposer(
		plan.OperationID(), session,
	)
	if err != nil {
		return nil, release, errors.Join(err, release())
	}
	return rf3AuthorizedSourceCaptureActivationProposer{
		authority: factory.authority, proposer: proposer,
	}, release, nil
}

func (factory *rf3RetainedPruneFactory) OpenRetainedPruneProposer(
	ctx context.Context, plan *splitcontroller.Plan, observed splitcontroller.Observation,
) (splitcontroller.RetainedPruneProposer, func() error, error) {
	if factory == nil || ctx == nil || factory.tls == nil || factory.lease == nil ||
		plan == nil || observed.Catalog == nil || observed.Certificate == nil {
		return nil, nil, errRF3Serving
	}
	session, release, err := openRF3SplitTopologySession(
		ctx, observed.Catalog, observed.SourceState.Binding,
		factory.tls, factory.authority, factory.lease,
		splitcontroller.RetainedPruneClientID(plan.OperationID()),
		splitcontroller.RetainedPruneTenant(plan.OperationID()),
		[]byte("vibedb/split-prune/retry-home\x00"), false,
	)
	if err != nil {
		return nil, nil, err
	}
	proposer, err := splitcontroller.NewRF3RetainedPruneProposerForPlan(
		plan, observed, session, rangesplit.DefaultRetainedPruneKeys,
		rangesplit.DefaultRetainedPruneKeyBytes,
	)
	if err != nil {
		return nil, release, errors.Join(err, release())
	}
	return rf3AuthorizedRetainedPruneProposer{authority: factory.authority, proposer: proposer}, release, nil
}

func openRF3SplitTopologySession(
	ctx context.Context,
	catalog *gateway.Snapshot,
	binding replicatedstate.Binding,
	tls *rafttransport.PeerTLS,
	authority serviceauthz.Authority,
	lease *splitcontroller.RuntimeStoreLease,
	clientID replication.ID128,
	tenant []byte,
	retryDomain []byte,
	capture bool,
) (*gateway.NativeSession, func() error, error) {
	if ctx == nil || catalog == nil || tls == nil || lease == nil ||
		clientID == (replication.ID128{}) || len(tenant) == 0 || len(retryDomain) == 0 {
		return nil, nil, errRF3Serving
	}
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := catalog.ResolveReplicatedRoute(
		distribution.DistributionName(binding.Distribution), distribution.ShardID(binding.Shard), replicas[:0],
	)
	if !ok {
		return nil, nil, errRF3Serving
	}
	journalPath, err := lease.TopologySessionJournalPath()
	if capture {
		journalPath, err = lease.CaptureSessionJournalPath()
	}
	if err != nil {
		return nil, nil, err
	}
	pool, executor, err := newRF3SplitTopologyTransport(tls)
	if err != nil {
		return nil, nil, err
	}
	release := func() error { return pool.Close() }
	retryHome := rf3SplitTopologyRetryHome(retryDomain, clientID)
	journalBinding, err := gateway.NativeSessionJournalBinding(
		route, string(binding.Distribution), string(binding.Shard), tenant, 1,
		serviceauthz.CapabilityTopology,
	)
	if err != nil {
		return nil, release, errors.Join(err, release())
	}
	journal, err := gateway.OpenNativeSessionJournal(gateway.NativeSessionJournalOptions{
		Path: journalPath, ClientID: clientID, RetryHome: retryHome,
		MaxCommandBytes: replication.MaxCommandBytes, Binding: journalBinding,
	})
	if err != nil {
		return nil, release, errors.Join(err, release())
	}
	session, err := gateway.NewNativeSession(gateway.NativeSessionOptions{
		Executor: executor, Route: route,
		Distribution: string(binding.Distribution), Shard: string(binding.Shard),
		Tenant: tenant, ClientID: clientID, RetryHome: retryHome,
		Resolver: gateway.BaseRelationResolver{Relation: 1}, Journal: journal,
		ProposalCapability: serviceauthz.CapabilityTopology,
		MaxRelationBatches: 1, MaxMutations: replication.MaxMutations,
		InitialCommandBytes: 4 << 10, MaxCommandBytes: replication.MaxCommandBytes,
	})
	if err != nil {
		return nil, release, errors.Join(err, release())
	}
	authorized, err := serviceauthz.WithAuthority(ctx, authority)
	if err != nil {
		return nil, release, errors.Join(err, release())
	}
	if session.Status().Pending {
		_, err = session.RetryPending(authorized)
	}
	if err == nil && !session.Status().Active {
		_, err = session.Open(authorized, math.MaxInt64)
	}
	if err != nil {
		return nil, release, errors.Join(err, release())
	}
	return session, release, nil
}

func newRF3SplitTopologyTransport(tls *rafttransport.PeerTLS) (*gateway.AuthenticatedReplicatedClient, *gateway.ReplicatedExecutor, error) {
	pool, err := gateway.NewAuthenticatedReplicatedClient(gateway.AuthenticatedReplicatedClientOptions{
		TLS: tls,
		Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{Timeout: rf3NetworkTimeout}).DialContext(ctx, "tcp", address)
		},
		HandshakeDeadline: func() time.Time { return time.Now().Add(rf3NetworkTimeout) },
		MaxConnections:    4, MaxPerEndpoint: 2, MaxIdlePerEndpoint: 1, MaxHandshakes: 2,
		MaxWaiters: 4, MaxIdleAge: 30 * time.Second, MaxLifetime: 10 * time.Minute,
	})
	if err != nil {
		return nil, nil, err
	}
	executor, err := gateway.NewReplicatedExecutor(pool, 3, rf3RequestTimeout)
	if err != nil {
		return nil, nil, errors.Join(err, pool.Close())
	}
	return pool, executor, nil
}

func rf3SplitTopologyRetryHome(retryDomain []byte, clientID replication.ID128) replication.RetryHome {
	retryInput := make([]byte, 0, len(retryDomain)+len(clientID))
	retryInput = append(retryInput, retryDomain...)
	retryInput = append(retryInput, clientID[:]...)
	retryDigest := sha256.Sum256(retryInput)
	var retryHome replication.RetryHome
	copy(retryHome[:], retryDigest[:len(retryHome)])
	return retryHome
}

type rf3AuthorizedRetainedPruneProposer struct {
	authority serviceauthz.Authority
	proposer  splitcontroller.RetainedPruneProposer
}

type rf3AuthorizedSourceCaptureActivationProposer struct {
	authority serviceauthz.Authority
	proposer  splitcontroller.SourceCaptureActivationProposer
}

func (proposer rf3AuthorizedSourceCaptureActivationProposer) ProposeSourceCaptureActivation(
	ctx context.Context,
	operation splitcontroller.OperationID,
	fence raftservice.ServingFence,
	body []byte,
) error {
	authorized, err := serviceauthz.WithAuthority(ctx, proposer.authority)
	if err != nil {
		return err
	}
	return proposer.proposer.ProposeSourceCaptureActivation(
		authorized, operation, fence, body,
	)
}

func (proposer rf3AuthorizedRetainedPruneProposer) ProposeRetainedPrune(
	ctx context.Context, operation splitcontroller.OperationID,
	fence raftservice.ServingFence, batch rangesplit.RetainedPruneBatch,
) error {
	authorized, err := serviceauthz.WithAuthority(ctx, proposer.authority)
	if err != nil {
		return err
	}
	return proposer.proposer.ProposeRetainedPrune(authorized, operation, fence, batch)
}

var _ splitcontroller.RetainedPruneProposerFactory = (*rf3RetainedPruneFactory)(nil)
var _ splitcontroller.SourceCaptureActivationProposerFactory = (*rf3RetainedPruneFactory)(nil)
