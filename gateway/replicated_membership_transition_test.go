package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

// transitionProbeClient returns an authenticated-looking handshake through
// the production probe path. That keeps the aggregation test coupled to the
// same bindReplicatedObservation checks used by real clients.
type transitionProbeClient struct {
	route    ReplicatedRoute
	observed raftservice.CommandFence
}

func (client *transitionProbeClient) ProbeReplicated(
	_ context.Context,
	route ReplicatedRoute,
	endpoint ReplicatedEndpoint,
	_ serviceauthz.Capability,
) (*shardservice.ReplicatedResponse, error) {
	state := shardservice.ReplicatedMemberState{
		Fence: shardservice.ReplicatedFence{
			Group: route.Group, AllocationGeneration: route.AllocationGeneration,
			Command: client.observed, MemberID: endpoint.Member,
			StoreID: endpoint.StoreID, NodeIncarnation: endpoint.NodeIncarnation,
			Term: 7,
		},
		LeaderID: 1, Commit: 8, Applied: 8, CheckpointApplied: 8,
	}
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
	}, nil
}

func (client *transitionProbeClient) DoReplicated(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	if request != nil && request.Operation == shardservice.ReplicatedProbe {
		return client.ProbeReplicated(ctx, client.route, endpoint, request.Capability)
	}
	return nil, ErrReplicatedRoute
}

func TestReplicatedMembershipTransitionEmitterAuthenticatesAllFences(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[0]
	base := states[endpoint.Address]
	observed := base.Fence.Command
	observed.ReplicaSetVersion++
	response := &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedHandshake, HasState: true, State: base,
	}
	response.State.Fence.Command = observed

	got, err := bindReplicatedObservation(route, endpoint, response)
	var transition *ReplicatedMembershipTransitionError
	if !errors.As(err, &transition) || !errors.Is(err, ErrReplicatedMembershipTransition) || got != (ReplicatedEndpoint{}) {
		t.Fatalf("valid transition got=%+v err=%v", got, err)
	}
	if transition.CatalogCommand != route.Command || transition.ObservedCommand != observed {
		t.Fatalf("transition evidence=%+v want catalog=%+v observed=%+v", transition, route.Command, observed)
	}
	if !isReplicatedMembershipTransitionPlanningError(err) {
		t.Fatal("authenticated transition was not planning-eligible")
	}

	for name, mutate := range map[string]func(*ReplicatedRoute, *ReplicatedEndpoint, *shardservice.ReplicatedResponse){
		"group": func(r *ReplicatedRoute, _ *ReplicatedEndpoint, response *shardservice.ReplicatedResponse) {
			response.State.Fence.Group.GroupID[0]++
		},
		"allocation": func(_ *ReplicatedRoute, _ *ReplicatedEndpoint, response *shardservice.ReplicatedResponse) {
			response.State.Fence.AllocationGeneration++
		},
		"member": func(_ *ReplicatedRoute, _ *ReplicatedEndpoint, response *shardservice.ReplicatedResponse) {
			response.State.Fence.MemberID++
		},
		"store": func(_ *ReplicatedRoute, _ *ReplicatedEndpoint, response *shardservice.ReplicatedResponse) {
			response.State.Fence.StoreID[0]++
		},
		"incarnation": func(_ *ReplicatedRoute, endpoint *ReplicatedEndpoint, response *shardservice.ReplicatedResponse) {
			response.State.Fence.NodeIncarnation = endpoint.NodeIncarnation - 1
		},
		"membership-rollback": func(r *ReplicatedRoute, _ *ReplicatedEndpoint, response *shardservice.ReplicatedResponse) {
			response.State.Fence.Command.ReplicaSetVersion = r.Command.ReplicaSetVersion - 1
		},
		"policy": func(_ *ReplicatedRoute, _ *ReplicatedEndpoint, response *shardservice.ReplicatedResponse) {
			response.State.Fence.Command.ActivePolicyGeneration++
		},
		"protection": func(_ *ReplicatedRoute, _ *ReplicatedEndpoint, response *shardservice.ReplicatedResponse) {
			response.State.Fence.Command.ProtectionEpoch++
		},
		"ownership": func(_ *ReplicatedRoute, _ *ReplicatedEndpoint, response *shardservice.ReplicatedResponse) {
			response.State.Fence.Command.OwnershipEpoch++
		},
		"schema": func(_ *ReplicatedRoute, _ *ReplicatedEndpoint, response *shardservice.ReplicatedResponse) {
			response.State.Fence.Command.SchemaGeneration++
		},
		"manifest": func(_ *ReplicatedRoute, _ *ReplicatedEndpoint, response *shardservice.ReplicatedResponse) {
			response.State.Fence.Command.RelationManifestDigest[0]++
		},
		"routing": func(_ *ReplicatedRoute, _ *ReplicatedEndpoint, response *shardservice.ReplicatedResponse) {
			response.State.Fence.Command.RoutingVersion++
		},
		"route-generation": func(_ *ReplicatedRoute, _ *ReplicatedEndpoint, response *shardservice.ReplicatedResponse) {
			response.State.Fence.Command.RouteGeneration++
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := *response
			candidate.State = response.State
			candidate.State.Fence = response.State.Fence
			candidate.State.Fence.Group = response.State.Fence.Group
			candidate.State.Fence.Command = response.State.Fence.Command
			candidate.State.Fence.Command.RelationManifestDigest = response.State.Fence.Command.RelationManifestDigest
			candidate.State.Fence.StoreID = response.State.Fence.StoreID
			candidate.State.Fence.Group.GroupID = response.State.Fence.Group.GroupID
			candidate.State.Fence.Group.ClusterID = response.State.Fence.Group.ClusterID
			candidate.State.Fence.Group.ClusterIncarnation = response.State.Fence.Group.ClusterIncarnation
			candidate.State.Fence.Group.ShardIncarnation = response.State.Fence.Group.ShardIncarnation
			candidate.State.Fence.Group.TopologyRecoveryEpoch = response.State.Fence.Group.TopologyRecoveryEpoch
			mutate(&route, &endpoint, &candidate)
			got, candidateErr := bindReplicatedObservation(route, endpoint, &candidate)
			if got != (ReplicatedEndpoint{}) || candidateErr == nil || !errors.Is(candidateErr, ErrReplicatedRoute) {
				t.Fatalf("got=%+v err=%v", got, candidateErr)
			}
			if isReplicatedMembershipTransitionPlanningError(candidateErr) {
				t.Fatalf("ineligible fence was classified as a transition: %v", candidateErr)
			}
		})
	}

	stable := route
	stable.membershipStable = true
	stableResponse := *response
	stableResponse.State = response.State
	stableResponse.State.Fence = response.State.Fence
	stableResponse.State.Fence.Command.ReplicaSetVersion = route.Command.ReplicaSetVersion + 1
	got, err = bindReplicatedObservation(stable, endpoint, &stableResponse)
	if err != nil || got.NodeIncarnation != endpoint.NodeIncarnation {
		t.Fatalf("stable higher command got=%+v err=%v", got, err)
	}
}

func TestReplicatedMembershipTransitionUnavailableStateRetainsRefusal(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[0]
	state := states[endpoint.Address]
	state.Fence.Command.ReplicaSetVersion++
	response := &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalUnavailable,
		HasState: true, State: state,
	}
	_, err := bindReplicatedObservation(route, endpoint, response)
	var refusal *ReplicatedRefusalError
	var transition *ReplicatedMembershipTransitionError
	if !errors.As(err, &refusal) || !errors.As(err, &transition) || !errors.Is(err, ErrReplicatedMembershipTransition) {
		t.Fatalf("unavailable attached state lost refusal/observation: %T %v", err, err)
	}
	if isReplicatedMembershipTransitionPlanningError(err) {
		t.Fatal("unavailable refusal became a refresh hint")
	}
}

type cyclicTransitionError struct{}

func (*cyclicTransitionError) Error() string   { return "cyclic transition wrapper" }
func (e *cyclicTransitionError) Unwrap() error { return e }
func (e *cyclicTransitionError) Is(error) bool { return false }

func TestReplicatedMembershipTransitionClassifierBoundsAndRejectsMixedTrees(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	endpoint := route.Replicas[0]
	state := states[endpoint.Address]
	state.Fence.Command.ReplicaSetVersion++
	_, hintErr := bindReplicatedObservation(route, endpoint, &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
	})
	if hintErr == nil || !isReplicatedMembershipTransitionPlanningError(hintErr) {
		t.Fatalf("hint=%v", hintErr)
	}
	for name, err := range map[string]error{
		"bare":             hintErr,
		"wrapped":          fmt.Errorf("preimage: %w", hintErr),
		"leader-aggregate": errors.Join(ErrReplicatedLeader, hintErr),
		"mixed-route":      errors.Join(ErrReplicatedLeader, hintErr, ErrReplicatedRoute),
		"mixed-context":    errors.Join(ErrReplicatedLeader, hintErr, context.Canceled),
		"mixed-refusal":    errors.Join(ErrReplicatedLeader, hintErr, &ReplicatedRefusalError{Code: shardservice.ReplicatedRefusalUnavailable}),
		"cyclic":           errors.Join(ErrReplicatedLeader, hintErr, new(cyclicTransitionError)),
	} {
		want := name == "bare" || name == "wrapped" || name == "leader-aggregate"
		if got := isReplicatedMembershipTransitionPlanningError(err); got != want {
			t.Errorf("%s classified=%t want=%t err=%v", name, got, want, err)
		}
	}

	invalid := &ReplicatedMembershipTransitionError{
		CatalogCommand: route.Command, ObservedCommand: route.Command,
	}
	for _, err := range []error{invalid, fmt.Errorf("%w", invalid), errors.Join(ErrReplicatedLeader, invalid)} {
		if isReplicatedMembershipTransitionPlanningError(err) {
			t.Fatalf("invalid hint accepted: %v", err)
		}
	}

	// The shipped SQL route is RF3. Keep this exact production aggregation shape
	// covered separately from the wider defensive bound below.
	rf3Client := &transitionProbeClient{route: route, observed: route.Command}
	rf3Client.observed.ReplicaSetVersion++
	rf3Executor, err := NewReplicatedExecutor(rf3Client, AbsoluteMaxReplicatedAttempts, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_, err = rf3Executor.ReadPoint(context.Background(), route, ReplicatedPointRead{
		Relation: 1, Key: []byte("rf3-aggregation"), MinimumApplied: 1,
		MaxValueBytes: 128, Linearizable: true,
	})
	if err == nil || !isReplicatedMembershipTransitionPlanningError(err) {
		t.Fatalf("RF3 read aggregation err=%v eligible=%t", err, isReplicatedMembershipTransitionPlanningError(err))
	}

	// Exercise the complete production read/discovery aggregation shape: all
	// sixteen attempts probe the maximum supported 64-member route, and every
	// probe passes through bindReplicatedObservation before being aggregated.
	wide := route
	wide.Replicas = make([]ReplicatedEndpoint, AbsoluteMaxReplicatedRouteMembers)
	for index := range wide.Replicas {
		wide.Replicas[index] = route.Replicas[index%len(route.Replicas)]
		wide.Replicas[index].Member = uint64(index + 1)
		wide.Replicas[index].Node[0] = byte(index + 1)
		wide.Replicas[index].StoreID[0] = byte(index + 1)
		wide.Replicas[index].NativeEndpoint = fmt.Sprintf("n-%d", index+1)
		wide.Replicas[index].Address = fmt.Sprintf("m-%d", index+1)
		wide.Replicas[index].NodeIncarnation = uint64(index + 11)
	}
	wide.membershipStable = false
	client := &transitionProbeClient{route: wide, observed: wide.Command}
	client.observed.ReplicaSetVersion++
	data, err := NewReplicatedExecutor(client, AbsoluteMaxReplicatedAttempts, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_, err = data.ReadPoint(context.Background(), wide, ReplicatedPointRead{
		Relation: 1, Key: []byte("k"), MinimumApplied: 1, MaxValueBytes: 128,
		Linearizable: true,
	})
	if err == nil || !isReplicatedMembershipTransitionPlanningError(err) {
		t.Fatalf("full read aggregation err=%v eligible=%t", err, isReplicatedMembershipTransitionPlanningError(err))
	}
}

func TestDurableSQLCatalogRefreshStateRequiresProgressAndReleasesOldLease(t *testing.T) {
	data, err := NewReplicatedExecutor(new(replicatedSQLIndexedReadClient), 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	holder := NewCatalogHolder(testSnapshot(t, 1))
	planner := NewExecutor(nil, holder, Options{Refresh: func(_ context.Context, stale uint64) (*Snapshot, error) {
		if stale != 1 {
			t.Fatalf("stale generation=%d", stale)
		}
		return testSnapshot(t, 2), nil
	}})
	lease := holder.pinCurrent()
	lease.release()
	var state = newDurableSQLCatalogRefreshState(data)
	if err := state.refreshMembership(t.Context(), planner, 1); err != nil {
		t.Fatal(err)
	}
	if state.cycles != 1 || holder.Current().Generation() != 2 {
		t.Fatalf("cycles=%d generation=%d", state.cycles, holder.Current().Generation())
	}
}

func TestDurableSQLCatalogRefreshStateSameGenerationConsumesBudget(t *testing.T) {
	data, err := NewReplicatedExecutor(new(replicatedSQLIndexedReadClient), 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	holder := NewCatalogHolder(testSnapshot(t, 1))
	calls := 0
	planner := NewExecutor(nil, holder, Options{Refresh: func(_ context.Context, stale uint64) (*Snapshot, error) {
		calls++
		return testSnapshot(t, stale), nil
	}})
	state := newDurableSQLCatalogRefreshState(data)
	err = state.refreshMembership(t.Context(), planner, 1)
	if !errors.Is(err, ErrStaleGeneration) || calls != 1 || state.cycles != 1 || holder.Current().Generation() != 1 {
		t.Fatalf("err=%v calls=%d cycles=%d generation=%d", err, calls, state.cycles, holder.Current().Generation())
	}
}

func TestDurableSQLCatalogRefreshStateCancellationStopsBeforeReplan(t *testing.T) {
	data, err := NewReplicatedExecutor(new(replicatedSQLIndexedReadClient), 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	holder := NewCatalogHolder(testSnapshot(t, 1))
	calls := 0
	ctx, cancel := context.WithCancel(t.Context())
	planner := NewExecutor(nil, holder, Options{Refresh: func(ctx context.Context, _ uint64) (*Snapshot, error) {
		calls++
		cancel()
		return testSnapshot(t, 2), nil
	}})
	state := newDurableSQLCatalogRefreshState(data)
	err = state.refreshMembership(ctx, planner, 1)
	if !errors.Is(err, context.Canceled) || calls != 1 || state.cycles != 1 || holder.Current().Generation() != 2 {
		t.Fatalf("err=%v calls=%d cycles=%d generation=%d", err, calls, state.cycles, holder.Current().Generation())
	}
}

func TestDurableSQLCatalogRefreshStateChargesAlreadyAdvancedGeneration(t *testing.T) {
	data, err := NewReplicatedExecutor(new(replicatedSQLIndexedReadClient), 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	holder := NewCatalogHolder(testSnapshot(t, 1))
	calls := 0
	planner := NewExecutor(nil, holder, Options{Refresh: func(_ context.Context, stale uint64) (*Snapshot, error) {
		calls++
		return testSnapshot(t, stale+1), nil
	}})
	state := newDurableSQLCatalogRefreshState(data)
	for attempt := 0; attempt < 2; attempt++ {
		if err := state.refreshMembership(t.Context(), planner, 1); err != nil {
			t.Fatalf("refresh attempt %d: %v", attempt+1, err)
		}
	}
	if state.cycles != 2 || calls != 1 || holder.Current().Generation() != 2 {
		t.Fatalf("advanced generation was not charged: cycles=%d calls=%d generation=%d", state.cycles, calls, holder.Current().Generation())
	}
	if err := state.refreshMembership(t.Context(), planner, 1); !errors.Is(err, ErrStaleGeneration) || state.cycles != 2 {
		t.Fatalf("refresh budget exceeded: err=%v cycles=%d", err, state.cycles)
	}
}

func TestDurableSQLCatalogRefreshStateDoesNotRetryJoinedStaleTerminal(t *testing.T) {
	data, err := NewReplicatedExecutor(new(replicatedSQLIndexedReadClient), 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	holder := NewCatalogHolder(testSnapshot(t, 1))
	terminal := errors.New("terminal catalog refresh failure")
	calls := 0
	planner := NewExecutor(nil, holder, Options{Refresh: func(_ context.Context, _ uint64) (*Snapshot, error) {
		calls++
		return nil, errors.Join(ErrStaleGeneration, terminal)
	}})
	state := newDurableSQLCatalogRefreshState(data)
	err = state.refreshMembership(t.Context(), planner, 1)
	if !errors.Is(err, ErrStaleGeneration) || !errors.Is(err, terminal) || calls != 1 || state.cycles != 1 {
		t.Fatalf("joined terminal was retried or hidden: err=%v calls=%d cycles=%d", err, calls, state.cycles)
	}
}

type membershipTransitionSQLClient struct {
	delegate        *replicatedSQLIndexedReadClient
	transition      bool
	observedCommand raftservice.CommandFence
	probes          int
}

func (client *membershipTransitionSQLClient) ProbeReplicated(
	ctx context.Context,
	route ReplicatedRoute,
	endpoint ReplicatedEndpoint,
	capability serviceauthz.Capability,
) (*shardservice.ReplicatedResponse, error) {
	client.probes++
	response, err := client.delegate.DoReplicated(ctx, endpoint, &shardservice.ReplicatedRequest{
		Operation: shardservice.ReplicatedProbe, Capability: capability,
		Fence: shardservice.ReplicatedFence{Group: route.Group, AllocationGeneration: route.AllocationGeneration},
	})
	if err != nil || !client.transition {
		if err == nil && response != nil && response.HasState && client.observedCommand.Valid() {
			response.State.Fence.Group = route.Group
			response.State.Fence.AllocationGeneration = route.AllocationGeneration
			response.State.Fence.MemberID = endpoint.Member
			response.State.Fence.StoreID = endpoint.StoreID
			response.State.Fence.NodeIncarnation = endpoint.NodeIncarnation
			response.State.Fence.Command = client.observedCommand
		}
		return response, err
	}
	response.State.Fence.Command.ReplicaSetVersion++
	return response, nil
}

func (client *membershipTransitionSQLClient) DoReplicated(
	ctx context.Context,
	endpoint ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	response, err := client.delegate.DoReplicated(ctx, endpoint, request)
	if err == nil && response != nil && response.HasState && !client.transition && client.observedCommand.Valid() {
		response.State.Fence.MemberID = endpoint.Member
		response.State.Fence.StoreID = endpoint.StoreID
		response.State.Fence.NodeIncarnation = endpoint.NodeIncarnation
		response.State.Fence.Command = client.observedCommand
	}
	return response, err
}

func membershipTransitionCatalogSnapshots(t *testing.T) (*Snapshot, *Snapshot) {
	t.Helper()
	plain, _ := replicatedSQLTransactionFixture(t, true)
	if plain == nil || plain.statistics == nil {
		t.Fatal("fixture snapshot has no statistics catalog")
	}
	descriptors := plain.replicatedDescriptors()
	found := false
	for index := range descriptors {
		if descriptors[index].Distribution == "data" && descriptors[index].Shard == "all" {
			descriptors[index].RequestLedgerRanges = []DurableRequestLedgerRangeDescriptor{{
				Identity: replication.Digest{0x91},
			}}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("SQL fixture has no data route for the request-ledger home")
	}
	stale, err := NewSnapshotWithReplicatedTableMetadata(
		plain.config, plain.endpoints, plain.Generation(), plain.indexDescriptors(),
		plain.statistics.Descriptors(), descriptors, plain.replicatedTableProfiles(),
		plain.ReplicatedTableDeclarations(),
	)
	if err != nil {
		t.Fatalf("stale catalog with request-ledger topology: %v", err)
	}
	freshDescriptors := stale.replicatedDescriptors()
	found = false
	for index := range freshDescriptors {
		if freshDescriptors[index].Distribution == "data" && freshDescriptors[index].Shard == "all" {
			freshDescriptors[index].Command.ReplicaSetVersion++
			// A ReplicaSetVersion advance must carry an actual roster cut. Keep
			// the endpoint address stable so this fixture exercises the planner's
			// refresh/replan path without growing a second transport directory,
			// while replacing the authenticated identity at one ordinal models a
			// real membership transition.
			freshDescriptors[index].Replicas[2].Member = 4
			freshDescriptors[index].Replicas[2].Node = [16]byte{4}
			freshDescriptors[index].Replicas[2].StoreID = [16]byte{14}
			freshDescriptors[index].Replicas[2].NodeIncarnation = 24
			found = true
			break
		}
	}
	if !found {
		t.Fatal("stale catalog lost data route")
	}
	fresh, err := NewSnapshotWithReplicatedTableMetadata(
		stale.config, stale.endpoints, stale.Generation()+1, stale.indexDescriptors(),
		stale.statistics.Descriptors(), freshDescriptors, stale.replicatedTableProfiles(),
		stale.ReplicatedTableDeclarations(),
	)
	if err != nil {
		t.Fatalf("fresh catalog with advanced membership: %v", err)
	}
	return stale, fresh
}

func membershipTransitionSQLFixture(
	t *testing.T,
) (*DurableSQLRequestExecutor, *membershipTransitionSQLClient, *CatalogHolder, *Snapshot, *typedServiceLedger, *typedServicePinStop, *DurableRequestLedgerTopologyHolder, requestledger.RequestKey, []byte, []Query) {
	t.Helper()
	stale, fresh := membershipTransitionCatalogSnapshots(t)
	holder := NewCatalogHolder(stale)
	planner := NewExecutor(nil, holder, Options{})
	reader, _ := attachReplicatedSQLIndexedReadClient(
		t, stale, []byte(`{"id":"message-1","n":1}`),
	)
	var workspace [ServingReplicaCount]ReplicatedEndpoint
	freshRoute, ok := fresh.ResolveReplicatedRoute("data", "all", workspace[:0])
	if !ok {
		t.Fatal("fresh catalog lost data route")
	}
	client := &membershipTransitionSQLClient{
		delegate: reader, transition: true, observedCommand: freshRoute.Command,
	}
	replicated, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	planner.refresh = func(ctx context.Context, staleGeneration uint64) (*Snapshot, error) {
		if staleGeneration != stale.Generation() {
			return nil, fmt.Errorf("unexpected stale generation %d", staleGeneration)
		}
		holder.leaseMu.Lock()
		oldLeases := holder.activeLeases[staleGeneration]
		holder.leaseMu.Unlock()
		if oldLeases != 0 {
			return nil, fmt.Errorf("stale planning lease still held: %d", oldLeases)
		}
		client.transition = false
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// The authority has already certified this membership cut before the
		// planner observes it. Install that immutable result through the same
		// test boundary used by catalog-authority refresh tests; the generic
		// catalog publisher intentionally rejects an uncertified roster change.
		installCatalogRefreshTestSnapshot(t, holder, fresh)
		return fresh, nil
	}

	topology, err := NewCatalogDurableRequestLedgerTopologyHolder(holder)
	if err != nil {
		t.Fatal(err)
	}
	ledger := new(typedServiceLedger)
	pins := new(typedServicePinStop)
	service, err := newDurableRequestService(topology, ledger, typedServiceRunnerStop{}, pins)
	if err != nil {
		t.Fatal(err)
	}
	tenant := []byte("membership-transition-tenant")
	key := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{0x41},
		Request:      requestledger.RequestID{0x42},
		TenantDigest: requestledger.Digest(sha256.Sum256(tenant)), IssuerEpoch: 7,
		IssuerLane: requestledger.IssuerLane{0x43}, IssuerSequence: 1,
	}
	queries := []Query{{
		SQL: `UPDATE messages SET n = n + 1 WHERE id = ?`, Class: ClassInteractive,
		Params: []shardservice.Param{shardservice.StringParam("message-1")},
	}}
	executor := &DurableSQLRequestExecutor{
		planner: planner, data: replicated, requests: service,
		recoveryPulses: 3, planningLeaseSpan: 64,
	}
	return executor, client, holder, fresh, ledger, pins, topology, key, tenant, queries
}

func TestDurableSQLExecuteRefreshesAuthenticatedMembershipTransitionBeforeBegin(t *testing.T) {
	executor, client, holder, fresh, ledger, pins, topology, key, tenant, queries := membershipTransitionSQLFixture(t)
	_, err := executor.Execute(t.Context(), key, tenant, queries)
	if !errors.Is(err, errTypedServicePin) {
		t.Fatalf("execution did not reach the one post-refresh admission: %v", err)
	}
	if holder.Current() == nil || holder.Current().Generation() != fresh.Generation() || topology.Current() == nil || topology.Current().Generation != fresh.Generation() ||
		client.transition || client.probes < 4 || ledger.applies != 1 || pins.called != 1 {
		t.Fatalf("refresh/replan state holder=%p fresh=%p topology=%+v transition=%t probes=%d applies=%d pins=%d",
			holder.Current(), fresh, topology.Current(), client.transition, client.probes, ledger.applies, pins.called)
	}
}

func TestDurableSQLPrepareDirectRefreshesWithoutLedgerReplay(t *testing.T) {
	executor, client, _, fresh, ledger, pins, _, key, tenant, queries := membershipTransitionSQLFixture(t)
	plan, err := (&DurableSQLRequestExecutor{planner: executor.planner, data: executor.data, singleFast: true}).PrepareDirect(
		t.Context(), key, tenant, queries,
	)
	if err != nil || plan == nil || plan.CatalogGeneration != fresh.Generation() {
		t.Fatalf("direct preimage did not refresh/replan: plan=%+v err=%v", plan, err)
	}
	if client.transition || client.probes < 4 || executor.planner.catalog.Current() == nil ||
		executor.planner.catalog.Current().Generation() != fresh.Generation() || ledger.applies != 0 || pins.called != 0 {
		t.Fatalf("direct refresh leaked ledger/admission: transition=%t probes=%d applies=%d pins=%d",
			client.transition, client.probes, ledger.applies, pins.called)
	}
}

type membershipReplayErrorLedger struct {
	typedServiceLedger
	err error
}

func (ledger *membershipReplayErrorLedger) ReadRow(
	context.Context,
	DurableRequestLedgerHome,
	DurableRequestLifecycleRead,
) (DurableRequestLifecycleRow, error) {
	ledger.reads++
	return DurableRequestLifecycleRow{}, ledger.err
}

func TestDurableSQLMembershipTransitionReplayErrorStopsBeforeRefreshOrBegin(t *testing.T) {
	executor, client, holder, _, _, pins, _, key, tenant, queries := membershipTransitionSQLFixture(t)
	const replayErrText = "retained replay unavailable"
	ledger := &membershipReplayErrorLedger{err: errors.New(replayErrText)}
	topology, err := NewCatalogDurableRequestLedgerTopologyHolder(holder)
	if err != nil {
		t.Fatal(err)
	}
	service, err := newDurableRequestService(topology, ledger, typedServiceRunnerStop{}, pins)
	if err != nil {
		t.Fatal(err)
	}
	executor.requests = service
	_, err = executor.Execute(t.Context(), key, tenant, queries)
	if !errors.Is(err, ledger.err) || !errors.Is(err, ErrDurableSQLNotAdmitted) ||
		client.transition == false || ledger.applies != 0 || pins.called != 0 || holder.Current().Generation() != 7 {
		t.Fatalf("replay error was hidden or refreshed: err=%v transition=%t reads=%d applies=%d pins=%d generation=%d",
			err, client.transition, ledger.reads, ledger.applies, pins.called, holder.Current().Generation())
	}
	if ledger.reads != 1 {
		t.Fatalf("replay error caused repeated ledger reads: %d", ledger.reads)
	}
}

func TestDurableSQLMembershipTransitionReplayFoundIsAuthoritative(t *testing.T) {
	executor, client, holder, _, _, pins, _, _, _, queries := membershipTransitionSQLFixture(t)
	targets := durableFaultTargets(t)
	requestDigest := replication.Digest(replicatedSQLTransactionRequestDigest(queries))
	base := durableFaultRequestWith(t, targets, replication.ID128{0x91}, requestDigest, 7)
	topology := durableFaultTopology(t, targets)
	current := topology.Current()
	current.Generation = 7
	if err := topology.Publish(*current); err != nil {
		t.Fatal(err)
	}
	resultRaw, err := AppendDurableRequestResult(nil, DurableRequestResult{
		Committed: true, AffectedRows: 4, Transaction: base.Program.Identity.ID,
		CatalogGeneration:       base.Program.Identity.CatalogGeneration,
		ShardsFanned:            uint64(len(base.Program.Targets)),
		TransitionTag:           base.Program.Contract.CommitTransitionTag,
		TerminalStateDigest:     base.Program.Contract.CommitTerminalStateDigest,
		TerminalContractDigest:  base.Program.Contract.TerminalContractDigest,
		RetirementWitnessDigest: base.Program.Contract.RetirementWitnessDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	ledger := &typedServiceLedger{
		head: requestledger.HeadRecord{
			Key: base.Key.RequestKey, RequestDigest: requestledger.Digest(base.Key.Digest),
			PlanRoot: requestledger.Digest{2}, Revision: 9, Phase: requestledger.PhaseTerminal,
		},
		terminal: requestledger.TerminalRecord{
			Revision: 9, Result: resultRaw, ResultDigest: requestledger.ResultDigest(resultRaw),
			RequestDigest: requestledger.Digest(base.Key.Digest), PlanRoot: requestledger.Digest{2},
			CatalogGeneration:      base.Program.Identity.CatalogGeneration,
			TerminalContractDigest: requestledger.Digest(base.Program.Contract.TerminalContractDigest),
			AckToken:               requestledger.AckToken{1},
		},
	}
	ledger.terminal.KeyDigest, err = requestledger.KeyDigest(base.Key.RequestKey)
	if err != nil {
		t.Fatal(err)
	}
	service, err := newDurableRequestService(topology, ledger, typedServiceRunnerStop{}, pins)
	if err != nil {
		t.Fatal(err)
	}
	executor.requests = service
	result, err := executor.Execute(t.Context(), base.Key.RequestKey, []byte("tenant-fault"), queries)
	if err != nil || result.Result == nil || result.Result.RowsAffected != 4 || result.Key.Digest != requestDigest {
		t.Fatalf("retained terminal was not authoritative: result=%+v err=%v", result, err)
	}
	if client.transition == false || holder.Current().Generation() != 7 || ledger.applies != 0 || pins.called != 0 {
		t.Fatalf("replay-found path refreshed/admitted: transition=%t generation=%d applies=%d pins=%d",
			client.transition, holder.Current().Generation(), ledger.applies, pins.called)
	}
}
