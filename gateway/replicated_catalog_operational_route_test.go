package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestCatalogOperationalRouteFreshProbePrefersLastObservedLeader(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	route.Distribution, route.Shard = ReplicatedCatalogDistribution, ReplicatedCatalogShard
	client := &freshLeaderHintClient{scriptedReplicatedClient: &scriptedReplicatedClient{states: states}}
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	executor.leaderHints.publish(route, route.Replicas[1], states["m2"])
	for range 4 {
		if _, err := executor.catalogOperationalRoute(t.Context(), route, nil); err != nil {
			t.Fatal(err)
		}
	}
	if len(client.probes) != 4 {
		t.Fatalf("catalog repeatedly probed partitioned first voter: %v", client.probes)
	}
	for _, member := range client.probes {
		if member != 2 {
			t.Fatalf("catalog did not freshly probe last observed leader: %v", client.probes)
		}
	}
}

func TestCatalogOperationalRouteSettlesExactLostReplyAfterPlacementChange(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixture(t)
	next := testCatalogAuthoritySnapshot(t, current.Generation()+1)
	client.unknownNext = true
	if err := authority.Publish(t.Context(), current.Generation(), next); !errors.Is(err, ErrReplicatedCatalogPending) {
		t.Fatalf("lost publication reply did not remain pending: %v", err)
	}
	retained := authority.session.PendingCommand()
	completion := bytes.Clone(client.unknownCompletion)
	client.state.Fence.Command.ReplicaSetVersion++
	client.state.Fence.Command.OwnershipEpoch++
	client.state.Fence.Command.RoutingVersion++
	client.state.Fence.Command.RouteGeneration++
	// The current handshake advances; the retained completion and admitted
	// command are still exactly the old bytes, as on a real replica replay.
	client.unknownState = client.state
	client.holdUnknown = false
	if err := authority.RetryPending(t.Context()); err != nil {
		t.Fatalf("placement change stranded retained catalog completion: %v", err)
	}
	if authority.session.Status().Pending || !bytes.Equal(retained, client.unknownCommand) ||
		!bytes.Equal(completion, client.unknownCompletion) {
		t.Fatal("catalog recovery rebuilt an admitted command or its completion")
	}
}

type catalogStoppedOldLeaderClient struct {
	*catalogAuthorityClient
	stopped        uint64
	staleProposals int
}

func (client *catalogStoppedOldLeaderClient) DoReplicated(ctx context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedPropose && endpoint.Member == client.stopped {
		client.staleProposals++
		return nil, context.DeadlineExceeded
	}
	return client.catalogAuthorityClient.DoReplicated(ctx, endpoint, request)
}

func TestCatalogFreshDiscoveryOverridesOldSessionLeaderHint(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixture(t)
	stopped := authority.session.leader.Fence.MemberID
	var replacement ReplicatedEndpoint
	for _, endpoint := range authority.route.Replicas {
		if endpoint.Member != stopped {
			replacement = endpoint
			break
		}
	}
	client.state.Fence.MemberID, client.state.LeaderID = replacement.Member, replacement.Member
	client.state.Fence.StoreID, client.state.Fence.NodeIncarnation = replacement.StoreID, replacement.NodeIncarnation
	client.state.Fence.Term++
	traced := &catalogStoppedOldLeaderClient{catalogAuthorityClient: client, stopped: stopped}
	authority.executor.client = traced
	authority.session.executor.client = traced
	if err := authority.Publish(t.Context(), current.Generation(), testCatalogAuthoritySnapshot(t, current.Generation()+1)); err != nil {
		t.Fatal(err)
	}
	if traced.staleProposals != 0 {
		t.Fatalf("fresh catalog discovery discarded in favor of stale session leader: %d proposals", traced.staleProposals)
	}
}

type catalogInitialElectionClient struct {
	ReplicatedRoundTripper
	probes int
}

func (client *catalogInitialElectionClient) DoReplicated(ctx context.Context, endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	response, err := client.ReplicatedRoundTripper.DoReplicated(ctx, endpoint, request)
	if err == nil && request.Operation == shardservice.ReplicatedProbe {
		client.probes++
		response.State.Fence.MemberID = endpoint.Member
		response.State.Fence.StoreID = endpoint.StoreID
		response.State.Fence.NodeIncarnation = endpoint.NodeIncarnation
		if client.probes <= ServingReplicaCount {
			response.State.LeaderID = 0
		}
	}
	return response, err
}

func TestCatalogOperationalRouteWaitsForInitialElectionWithinRetryBudget(t *testing.T) {
	authority, client, _ := newCatalogAuthorityFixture(t)
	election := &catalogInitialElectionClient{ReplicatedRoundTripper: client}
	authority.session.executor.client = election
	ctx, err := authority.authorizedContext(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := authority.session.catalogOperationalRoute(ctx); err != nil {
		t.Fatalf("catalog startup did not retry authenticated leaderless sweep: %v", err)
	}
	if election.probes != ServingReplicaCount+1 {
		t.Fatalf("probes=%d, want one sweep followed by elected leader", election.probes)
	}
}

type catalogReadFenceRaceClient struct {
	*catalogAuthorityClient
	changed bool
}

func (client *catalogReadFenceRaceClient) DoReplicated(ctx context.Context, endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request.Operation == shardservice.ReplicatedReadLeader && !client.changed {
		client.changed = true
		client.state.Fence.Command.ReplicaSetVersion++
		return &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedRefusal,
			Refusal: shardservice.ReplicatedRefusalStaleFence, HasState: true, State: client.state}, nil
	}
	return client.catalogAuthorityClient.DoReplicated(ctx, endpoint, request)
}

func TestCatalogOperationalReadRediscoversMembershipChangedAfterProbe(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixture(t)
	raced := &catalogReadFenceRaceClient{catalogAuthorityClient: client}
	authority.executor.client = raced
	got, err := authority.Read(t.Context())
	if err != nil || got.Generation() != current.Generation() || !raced.changed {
		t.Fatalf("catalog read stranded between probe and membership apply: %v", err)
	}
}

func TestCatalogOperationalRouteFollowsPlacementWithoutChangingBootstrapBinding(t *testing.T) {
	authority, client, current := newCatalogAuthorityFixture(t)
	bootstrap := authority.route
	client.state.Fence.Command.ReplicaSetVersion++
	if got, err := authority.Read(t.Context()); err != nil || got.Generation() != current.Generation() {
		t.Fatalf("membership step stranded catalog read: %v", err)
	}
	next := testCatalogAuthoritySnapshot(t, current.Generation()+1)
	if err := authority.Publish(t.Context(), current.Generation(), next); err != nil {
		t.Fatalf("membership step stranded catalog journal write: %v", err)
	}
	if !sameReplicatedCatalogRoute(authority.route, bootstrap) || !sameReplicatedCatalogRoute(authority.session.route, bootstrap) {
		t.Fatal("ephemeral placement discovery changed the bootstrap/journal binding")
	}
	client.state.Fence.Command.OwnershipEpoch++
	client.state.Fence.Command.RoutingVersion++
	client.state.Fence.Command.RouteGeneration++
	if _, err := authority.Read(t.Context()); err != nil {
		t.Fatalf("ownership transition stranded catalog journal: %v", err)
	}
}

func TestCatalogOperationalRouteRejectsUnrelatedAuthorityChanges(t *testing.T) {
	for name, mutate := range map[string]func(*raftservice.CommandFence){
		"schema":                func(f *raftservice.CommandFence) { f.SchemaGeneration++ },
		"manifest":              func(f *raftservice.CommandFence) { f.RelationManifestDigest[0]++ },
		"policy":                func(f *raftservice.CommandFence) { f.ActivePolicyGeneration++ },
		"protection":            func(f *raftservice.CommandFence) { f.ProtectionEpoch++ },
		"membership-regression": func(f *raftservice.CommandFence) { f.ReplicaSetVersion = 0 },
		"ownership-regression":  func(f *raftservice.CommandFence) { f.OwnershipEpoch = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			authority, client, _ := newCatalogAuthorityFixture(t)
			mutate(&client.state.Fence.Command)
			if _, err := authority.Read(t.Context()); !errors.Is(err, ErrReplicatedRoute) {
				t.Fatalf("catalog followed unrelated authority: %v", err)
			}
		})
	}
}
