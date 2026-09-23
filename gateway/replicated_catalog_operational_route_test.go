package gateway

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
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

// catalogEndpointLeaderClient returns an authenticated state for the endpoint
// that was actually probed. This models a catalog whose committed leader has
// moved to an enrolled target while the old serving seeds still answer.
type catalogEndpointLeaderClient struct {
	*catalogAuthorityClient
	leader   uint64
	parallel bool
	mu       sync.Mutex
	probes   []uint64
}

func (client *catalogEndpointLeaderClient) parallelReplicatedDiscoveryEnabled() bool {
	return client.parallel
}

func (client *catalogEndpointLeaderClient) DoReplicated(
	ctx context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	response, err := client.catalogAuthorityClient.DoReplicated(ctx, endpoint, request)
	if err != nil || response == nil {
		return response, err
	}
	if request.Operation == shardservice.ReplicatedProbe {
		client.mu.Lock()
		client.probes = append(client.probes, endpoint.Member)
		client.mu.Unlock()
	}
	response.State.Fence.MemberID = endpoint.Member
	response.State.Fence.StoreID = endpoint.StoreID
	response.State.Fence.NodeIncarnation = endpoint.NodeIncarnation
	response.State.LeaderID = client.leader
	return response, nil
}

func (client *catalogEndpointLeaderClient) probedMembers() []uint64 {
	client.mu.Lock()
	defer client.mu.Unlock()
	return append([]uint64(nil), client.probes...)
}

func TestCatalogAuthorityStartupDiscoversAuthenticatedEnrolledTarget(t *testing.T) {
	authority, client, _, current, descriptor := newRouteSeedCatalogAuthorityFixture(t)
	targetDescriptor := descriptor
	testReplicatedCatalogEnrollTarget(&targetDescriptor)
	bootstrap, err := NewSnapshotWithReplicatedMetadata(
		current.config, current.endpoints, current.Generation(), nil, nil,
		[]ReplicatedShardDescriptor{targetDescriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err = initialCatalogState(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	membership, ok := bootstrap.ResolveReplicatedMembershipRoute(
		ReplicatedCatalogDistribution, ReplicatedCatalogShard, nil,
	)
	if !ok || !membership.HasEnrolledTarget {
		t.Fatal("fixture has no enrolled target")
	}
	// Authority discovery runs before the live holder is installed. The
	// bootstrap snapshot is an attested route-seed image and is the only source
	// from which the target endpoint may be admitted at this boundary.
	authority.holder = NewCatalogHolder(nil)
	authority.session.catalogBootstrap = bootstrap
	traced := &catalogEndpointLeaderClient{
		catalogAuthorityClient: client, leader: membership.EnrolledTarget.Member,
	}
	authority.executor.client = traced
	got, err := authority.Read(t.Context())
	if err != nil {
		t.Fatalf("startup discovery rejected authenticated enrolled leader: %v", err)
	}
	if got == nil || got.Generation() != bootstrap.Generation() {
		t.Fatalf("catalog generation=%v, want %d", got, bootstrap.Generation())
	}
	seen := traced.probedMembers()
	foundTarget := false
	for _, member := range seen {
		if member == membership.EnrolledTarget.Member {
			foundTarget = true
			break
		}
	}
	if !foundTarget {
		t.Fatalf("startup discovery never probed authenticated target %d: %v", membership.EnrolledTarget.Member, seen)
	}
}

func TestCatalogOperationalRoutePromotesEnrolledLeaderWithoutDuplicateHint(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		name := map[bool]string{false: "serial", true: "parallel"}[parallel]
		t.Run(name, func(t *testing.T) {
			authority, client, _, current, descriptor := newRouteSeedCatalogAuthorityFixture(t)
			targetDescriptor := descriptor
			testReplicatedCatalogEnrollTarget(&targetDescriptor)
			bootstrap, err := NewSnapshotWithReplicatedMetadata(
				current.config, current.endpoints, current.Generation(), nil, nil,
				[]ReplicatedShardDescriptor{targetDescriptor},
			)
			if err != nil {
				t.Fatal(err)
			}
			bootstrap, err = initialCatalogState(bootstrap)
			if err != nil {
				t.Fatal(err)
			}
			var scratch [ServingReplicaCount]ReplicatedEndpoint
			bootstrapRoute, ok := bootstrap.ResolveReplicatedRoute(
				ReplicatedCatalogDistribution, ReplicatedCatalogShard, scratch[:0],
			)
			if !ok || !bootstrapRoute.hasDiscoveryReplica {
				t.Fatal("persisted enrolled route seed did not retain its authenticated target")
			}
			membership, ok := bootstrap.ResolveReplicatedMembershipRoute(
				ReplicatedCatalogDistribution, ReplicatedCatalogShard, nil,
			)
			if !ok || !membership.HasEnrolledTarget {
				t.Fatal("persisted enrolled route seed has no membership target")
			}
			target := membership.EnrolledTarget
			client.state.Fence.Command.ReplicaSetVersion = 441
			client.state.Fence.Command.OwnershipEpoch++
			client.state.Fence.Command.RoutingVersion++
			client.state.Fence.Command.RouteGeneration++
			client.state.Commit, client.state.Applied, client.state.CheckpointApplied = 441, 441, 441
			client.state.LeaderID = target.Member
			client.state.Fence.MemberID = target.Member
			client.state.Fence.StoreID = target.StoreID
			client.state.Fence.NodeIncarnation = target.NodeIncarnation
			traced := &catalogEndpointLeaderClient{
				catalogAuthorityClient: client, leader: target.Member, parallel: parallel,
			}
			authority.executor.client = traced
			authority.route = bootstrapRoute
			authority.session.route = bootstrapRoute
			authority.holder = NewCatalogHolder(nil)
			authority.session.catalogBootstrap = bootstrap

			if _, err = authority.ReadOperationIDs(t.Context()); err != nil {
				t.Fatalf("enrolled catalog leader could not resume the frozen operation directory: %v", err)
			}
			ctx, err := authority.authorizedContext(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			operational, err := authority.executor.catalogOperationalRoute(ctx, bootstrapRoute, bootstrap)
			if err != nil {
				t.Fatalf("discover enrolled catalog operational route: %v", err)
			}
			if !validReplicatedRoute(operational) || operational.hasDiscoveryReplica {
				t.Fatalf("operational route retained a duplicate transition hint: valid=%t hint=%t route=%+v",
					validReplicatedRoute(operational), operational.hasDiscoveryReplica, operational)
			}
			if !replicatedRouteContainsMember(operational, target.Member) {
				t.Fatalf("operational route omitted promoted leader %d: %+v", target.Member, operational.Replicas)
			}
		})
	}
}

func TestCatalogOperationalRouteClearsOlderHintAlreadyServingAfterNextMove(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		name := map[bool]string{false: "serial", true: "parallel"}[parallel]
		t.Run(name, func(t *testing.T) {
			authority, client, _, current, descriptor := newRouteSeedCatalogAuthorityFixture(t)

			// The route seed retained T1 as the first move's discovery hint.
			seedDescriptor := descriptor
			testReplicatedCatalogEnrollTarget(&seedDescriptor)
			bootstrap := testCatalogMembershipSnapshot(t, current, seedDescriptor)
			var scratch [ServingReplicaCount]ReplicatedEndpoint
			bootstrapRoute, ok := bootstrap.ResolveReplicatedRoute(
				ReplicatedCatalogDistribution, ReplicatedCatalogShard, scratch[:0],
			)
			if !ok || !bootstrapRoute.hasDiscoveryReplica || bootstrapRoute.discoveryReplica.Member != 4 {
				t.Fatalf("bootstrap route did not retain T1: %+v", bootstrapRoute)
			}

			// The certified current catalog has completed that move: T1 is now
			// serving and T2 is the next enrolled target. T2 is the catalog leader.
			target1 := *seedDescriptor.EnrolledTarget
			target2 := ReplicatedReplicaDescriptor{
				Member: 5, Node: [16]byte{5}, StoreID: [16]byte{15}, NodeIncarnation: 25,
				Endpoint: "ep-c", NativeEndpoint: "ep-c-native", ControlEndpoint: "ep-c-control",
			}
			currentDescriptor := descriptor
			currentDescriptor.Replicas = []ReplicatedReplicaDescriptor{
				descriptor.Replicas[0], target1, descriptor.Replicas[2],
			}
			currentDescriptor.EnrolledTarget = &target2
			currentSnapshot := testCatalogMembershipSnapshot(t, current, currentDescriptor)
			membership, ok := currentSnapshot.ResolveReplicatedMembershipRoute(
				ReplicatedCatalogDistribution, ReplicatedCatalogShard, nil,
			)
			if !ok || !replicatedRouteContainsMember(membership.Serving, target1.Member) ||
				!membership.HasEnrolledTarget || membership.EnrolledTarget.Member != target2.Member {
				t.Fatalf("fixture is not the second move: %+v ok=%v", membership, ok)
			}

			client.state.Fence.Command = membership.Serving.Command
			client.state.Commit, client.state.Applied, client.state.CheckpointApplied = 500, 500, 500
			client.state.LeaderID = target2.Member
			traced := &catalogEndpointLeaderClient{
				catalogAuthorityClient: client, leader: target2.Member, parallel: parallel,
			}
			authority.executor.client = traced
			authority.route = bootstrapRoute
			authority.holder = NewCatalogHolder(currentSnapshot)
			authority.session.catalogBootstrap = bootstrap

			ids, err := authority.ReadOperationIDs(t.Context())
			if err != nil || len(ids) != 0 {
				t.Fatalf("next operation poll through T2 ids=%v err=%v", ids, err)
			}
			foundT2 := false
			for _, member := range traced.probedMembers() {
				foundT2 = foundT2 || member == target2.Member
			}
			if !foundT2 {
				t.Fatalf("catalog sweep did not probe certified T2 %d: %v", target2.Member, traced.probedMembers())
			}
			ctx, err := authority.authorizedContext(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			operational, err := authority.executor.catalogOperationalRoute(ctx, bootstrapRoute, currentSnapshot)
			if err != nil || !validReplicatedRoute(operational) || operational.hasDiscoveryReplica ||
				!replicatedRouteContainsMember(operational, target2.Member) {
				t.Fatalf("next move route=%+v valid=%t err=%v", operational, validReplicatedRoute(operational), err)
			}
		})
	}
}

func TestCatalogOperationalRouteReadsThroughCertifiedRetiringLeader(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		name := map[bool]string{false: "serial", true: "parallel"}[parallel]
		t.Run(name, func(t *testing.T) {
			authority, client, _, current, descriptor := newRouteSeedCatalogAuthorityFixture(t)
			target := ReplicatedReplicaDescriptor{
				Member: 4, Node: [16]byte{4}, StoreID: [16]byte{14}, NodeIncarnation: 24,
				Endpoint: "ep-b", NativeEndpoint: "ep-b-native", ControlEndpoint: "ep-b-control",
			}
			retiring := descriptor.Replicas[0]
			g1Descriptor := descriptor
			g1Descriptor.Replicas = []ReplicatedReplicaDescriptor{target, descriptor.Replicas[1], descriptor.Replicas[2]}
			g1Descriptor.RetiringSource = &retiring
			g1Snapshot := testCatalogMembershipSnapshot(t, current, g1Descriptor)
			membership, ok := g1Snapshot.ResolveReplicatedMembershipRoute(
				ReplicatedCatalogDistribution, ReplicatedCatalogShard, nil,
			)
			if !ok || membership.RetiringSource.Member != retiring.Member || membership.HasEnrolledTarget {
				t.Fatalf("G+1 membership lost retiring voter: %+v ok=%v", membership, ok)
			}

			client.state.Fence.Command = membership.Serving.Command
			client.state.Commit, client.state.Applied, client.state.CheckpointApplied = 500, 500, 500
			client.state.LeaderID = retiring.Member
			traced := &catalogEndpointLeaderClient{
				catalogAuthorityClient: client, leader: retiring.Member, parallel: parallel,
			}
			authority.executor.client = traced
			authority.holder = NewCatalogHolder(g1Snapshot)
			ids, err := authority.ReadOperationIDs(t.Context())
			if err != nil || len(ids) != 0 {
				t.Fatalf("catalog point read through G+1 source ids=%v err=%v", ids, err)
			}
			foundSource := false
			for _, member := range traced.probedMembers() {
				foundSource = foundSource || member == retiring.Member
			}
			if !foundSource {
				t.Fatalf("catalog discovery omitted certified retiring source %d: %v", retiring.Member, traced.probedMembers())
			}
		})
	}
}

func TestClearPromotedCatalogDiscoveryHintUsesTheWholeEphemeralRoster(t *testing.T) {
	route, _, _ := testReplicatedRouteCommand(t)

	t.Run("same exact identity clears despite newer incarnation", func(t *testing.T) {
		candidate := route
		candidate.discoveryReplica = route.Replicas[1]
		candidate.discoveryReplica.NodeIncarnation++
		candidate.hasDiscoveryReplica = true
		if !clearPromotedCatalogDiscoveryHint(&candidate) || candidate.hasDiscoveryReplica ||
			!validReplicatedRoute(candidate) {
			t.Fatalf("exact served hint was not cleared: %+v", candidate)
		}
	})

	t.Run("distinct certified hint remains", func(t *testing.T) {
		candidate := route
		candidate.discoveryReplica = ReplicatedEndpoint{
			Member: 9, Node: [16]byte{9}, StoreID: [16]byte{19}, NodeIncarnation: 29,
			Endpoint: "ep-9", DataAddress: "data-9", NativeEndpoint: "native-9", Address: "native-addr-9",
			ControlEndpoint: "control-9", ControlAddress: "control-addr-9",
		}
		candidate.hasDiscoveryReplica = true
		if !clearPromotedCatalogDiscoveryHint(&candidate) || !candidate.hasDiscoveryReplica ||
			!validReplicatedRoute(candidate) {
			t.Fatalf("distinct control hint was discarded or invalid: %+v", candidate)
		}
	})

	t.Run("retired member on a reused node is cleared", func(t *testing.T) {
		// The group's member left this node and a new member was later placed
		// on it: the hint names a retired placement on a now-serving node.
		candidate := route
		candidate.discoveryReplica = route.Replicas[1]
		candidate.discoveryReplica.Member = 77
		candidate.discoveryReplica.NodeIncarnation++
		candidate.hasDiscoveryReplica = true
		if !clearPromotedCatalogDiscoveryHint(&candidate) || candidate.hasDiscoveryReplica ||
			!validReplicatedRoute(candidate) {
			t.Fatalf("retired hint on a reused node invalidated the route: %+v", candidate)
		}
	})

	t.Run("same member with changed node identity fails closed", func(t *testing.T) {
		candidate := route
		candidate.discoveryReplica = route.Replicas[1]
		candidate.discoveryReplica.Node = [16]byte{99}
		candidate.hasDiscoveryReplica = true
		if clearPromotedCatalogDiscoveryHint(&candidate) || !candidate.hasDiscoveryReplica {
			t.Fatalf("mismatched same-member hint was accepted: %+v", candidate)
		}
	})
}

func testCatalogMembershipSnapshot(
	t testing.TB,
	base *Snapshot,
	descriptor ReplicatedShardDescriptor,
) *Snapshot {
	t.Helper()
	manifest, ok := base.Manifest(descriptor.Distribution)
	if !ok {
		t.Fatal("catalog test manifest missing")
	}
	shard, ok := manifest.ShardInfo(0)
	if !ok || shard.ID != descriptor.Shard {
		t.Fatal("catalog test shard missing")
	}
	shard.Leaders = make([]distribution.EndpointID, len(descriptor.Replicas))
	for index, replica := range descriptor.Replicas {
		shard.Leaders[index] = replica.Endpoint
	}
	shard.Epoch++
	nextManifest, err := distribution.NewManifest(
		manifest.Distribution(), manifest.Version()+1, []distribution.Shard{shard},
	)
	if err != nil {
		t.Fatal(err)
	}
	config := base.config
	config.Manifests = append([]*distribution.Manifest(nil), base.config.Manifests...)
	config.Manifests[0] = nextManifest
	descriptor.Command.ReplicaSetVersion += 3
	descriptor.Command.OwnershipEpoch++
	descriptor.Command.RoutingVersion = uint64(nextManifest.Version())
	descriptor.Command.RouteGeneration++
	snapshot, err := NewSnapshotWithReplicatedMetadata(
		config, base.endpoints, base.Generation()+1, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = initialCatalogState(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestCatalogAuthorityStartupRejectsUnboundEnrolledLeader(t *testing.T) {
	authority, client, _, bootstrap, _ := newRouteSeedCatalogAuthorityFixture(t)
	authority.holder = NewCatalogHolder(nil)
	authority.session.catalogBootstrap = bootstrap
	traced := &catalogEndpointLeaderClient{catalogAuthorityClient: client, leader: 4}
	authority.executor.client = traced
	if _, err := authority.Read(t.Context()); !errors.Is(err, ErrReplicatedLeader) {
		t.Fatalf("unbound target leader was accepted: %v", err)
	}
	for _, member := range traced.probedMembers() {
		if member == 4 {
			t.Fatal("discovery probed an unbound target endpoint")
		}
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
