package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

const rf3SplitPruneLease = 5 * time.Minute

type rf3RetainedPruneFactory struct {
	tls       *rafttransport.PeerTLS
	authority serviceauthz.Authority
	lease     *splitcontroller.RuntimeStoreLease
}

func (factory *rf3RetainedPruneFactory) OpenRetainedPruneProposer(
	ctx context.Context, plan *splitcontroller.Plan, observed splitcontroller.Observation,
) (splitcontroller.RetainedPruneProposer, func() error, error) {
	if factory == nil || ctx == nil || factory.tls == nil || factory.lease == nil ||
		plan == nil || observed.Catalog == nil || observed.Certificate == nil {
		return nil, nil, errRF3Serving
	}
	binding := observed.SourceState.Binding
	var replicas [gateway.ServingReplicaCount]gateway.ReplicatedEndpoint
	route, ok := observed.Catalog.ResolveReplicatedRoute(binding.Distribution, binding.Shard, replicas[:0])
	if !ok {
		return nil, nil, errRF3Serving
	}
	journalPath, err := factory.lease.TopologySessionJournalPath()
	if err != nil {
		return nil, nil, err
	}
	pool, err := gateway.NewAuthenticatedReplicatedClient(gateway.AuthenticatedReplicatedClientOptions{
		TLS: factory.tls,
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
	release := func() error { return pool.Close() }
	executor, err := gateway.NewReplicatedExecutor(pool, 3, rf3RequestTimeout)
	if err != nil {
		return nil, release, errors.Join(err, release())
	}
	operation := plan.OperationID()
	clientID := splitcontroller.RetainedPruneClientID(operation)
	retryDigest := sha256.Sum256(append([]byte("vibedb/split-prune/retry-home\x00"), operation[:]...))
	var retryHome replication.RetryHome
	copy(retryHome[:], retryDigest[:len(retryHome)])
	tenant := splitcontroller.RetainedPruneTenant(operation)
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
	authorized, err := serviceauthz.WithAuthority(ctx, factory.authority)
	if err != nil {
		return nil, release, errors.Join(err, release())
	}
	if session.Status().Pending {
		_, err = session.RetryPending(authorized)
	}
	if err == nil && !session.Status().Active {
		_, err = session.Open(authorized, time.Now().Add(rf3SplitPruneLease).UnixNano())
	}
	if err != nil {
		return nil, release, errors.Join(err, release())
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

type rf3AuthorizedRetainedPruneProposer struct {
	authority serviceauthz.Authority
	proposer  splitcontroller.RetainedPruneProposer
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
