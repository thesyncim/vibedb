package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func testRouteReleaseReceiptCommand(t testing.TB, route ReplicatedRoute) []byte {
	t.Helper()
	gate, err := routegate.AppendCommand(nil, routegate.Command{
		Operation: routegate.OperationReleaseShared,
		Epoch:     7,
		Identity:  routegate.Identity{1},
		Binding:   routegate.Binding{2},
	})
	if err != nil {
		t.Fatal(err)
	}
	command := replication.Command{
		Kind:                  replication.CommandRouteGate,
		AuthorityClass:        replication.CommandAuthorityRouteSession,
		ClusterID:             route.Group.ClusterID,
		ClusterIncarnation:    route.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: route.Group.TopologyRecoveryEpoch,
		Distribution:          string(route.Distribution), Shard: string(route.Shard),
		AllocationGeneration: route.AllocationGeneration,
		ShardIncarnation:     route.Group.ShardIncarnation, GroupID: route.Group.GroupID,
		ReplicaSetVersion:      route.Command.ReplicaSetVersion,
		ActivePolicyGeneration: route.Command.ActivePolicyGeneration,
		ProtectionEpoch:        route.Command.ProtectionEpoch,
		OwnershipEpoch:         route.Command.OwnershipEpoch,
		SchemaGeneration:       route.Command.SchemaGeneration,
		RoutingVersion:         route.Command.RoutingVersion,
		RouteGeneration:        route.Command.RouteGeneration,
		Tenant:                 []byte("tenant"), ClientID: replication.ID128{3},
		ClientEpoch: 2, ClientSequence: 3, AckThrough: 2,
		Fingerprint: replication.Digest(sha256.Sum256([]byte("route-release-receipt"))),
		RetryHome:   replication.RetryHome{9}, RouteGate: gate,
	}
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func testRouteReleaseReceiptTarget(route ReplicatedRoute) DurableRequestLogicalTarget {
	return DurableRequestLogicalTarget{
		Distribution:           route.Distribution,
		Shard:                  route.Shard,
		RangeIdentity:          route.RangeIdentity,
		Group:                  route.Group,
		SchemaGeneration:       route.Command.SchemaGeneration,
		RelationManifestDigest: replication.Digest(route.Command.RelationManifestDigest),
		LineageDigest:          route.LineageDigest,
		ForwardingRuleDigest:   route.ForwardingRuleDigest,
	}
}

func TestCatalogReleaseReceiptRouteKeepsPhysicalSourceAcrossSchemaDrift(t *testing.T) {
	config, endpoints, descriptor, retained := testRequestLedgerCatalogInput(t, 5)
	target := testRouteReleaseReceiptTarget(retained)
	exact := testRouteReleaseReceiptCommand(t, retained)

	// Publish the real current descriptor with mutable schema/routing metadata
	// advanced while retaining the same physical Group and allocation. The
	// receipt resolver must preserve the old target's source identity while
	// returning this current outer fence.
	descriptor.Command.SchemaGeneration++
	descriptor.Command.RelationManifestDigest[0]++
	descriptor.RangeIdentity[0]++
	descriptor.LineageDigest[0]++
	descriptor.ForwardingRuleDigest[0]++
	current, err := NewSnapshotWithReplicatedMetadata(
		config, endpoints, 6, nil, nil, []ReplicatedShardDescriptor{descriptor},
	)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := NewCatalogDurableRequestRouteResolver(NewCatalogHolder(current))
	if err != nil {
		t.Fatal(err)
	}
	got, err := resolver.resolveDurableReleaseReceiptRoute(t.Context(), target, exact)
	if err != nil {
		t.Fatalf("receipt route error=%v", err)
	}
	if got.Group != retained.Group || got.AllocationGeneration != retained.AllocationGeneration ||
		got.Distribution != retained.Distribution || got.Shard != retained.Shard ||
		got.Command.SchemaGeneration == retained.Command.SchemaGeneration ||
		got.RangeIdentity == retained.RangeIdentity || got.LineageDigest == retained.LineageDigest ||
		got.ForwardingRuleDigest == retained.ForwardingRuleDigest {
		t.Fatalf("receipt route changed physical or mutable identity incorrectly: got=%+v retained=%+v", got, retained)
	}

	for name, mutate := range map[string]func(*ReplicatedRoute, *DurableRequestLogicalTarget){
		"group": func(route *ReplicatedRoute, target *DurableRequestLogicalTarget) {
			target.Group.GroupID[0]++
			_ = route
		},
		"schema": func(route *ReplicatedRoute, target *DurableRequestLogicalTarget) {
			target.SchemaGeneration++
			_ = route
		},
		"source": func(route *ReplicatedRoute, target *DurableRequestLogicalTarget) {
			route.Group = target.Group
			route.Group.ShardIncarnation[0]++
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidateTarget := target
			candidateRoute := retained
			mutate(&candidateRoute, &candidateTarget)
			candidate := exact
			if name == "group" || name == "source" {
				candidate = testRouteReleaseReceiptCommand(t, candidateRoute)
			}
			if _, err := resolver.resolveDurableReleaseReceiptRoute(t.Context(), candidateTarget, candidate); !errors.Is(err, ErrDurableRequestConflict) {
				t.Fatalf("mutated receipt identity accepted: %v", err)
			}
		})
	}
}

type routeReleaseReceiptClient struct {
	authority  serviceauthz.Authority
	states     map[string]shardservice.ReplicatedMemberState
	exact      []byte
	completion []byte
	operations []shardservice.ReplicatedOperation
	capability []serviceauthz.Capability
}

func (client *routeReleaseReceiptClient) DoReplicated(
	_ context.Context, endpoint ReplicatedEndpoint, request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	client.operations = append(client.operations, request.Operation)
	client.capability = append(client.capability, request.Capability)
	if request.Authority != client.authority {
		return nil, errors.New("receipt request lost service authority")
	}
	state, ok := client.states[endpoint.Address]
	if !ok {
		return nil, errors.New("unknown receipt endpoint")
	}
	if request.Operation == shardservice.ReplicatedProbe {
		if request.Capability != serviceauthz.CapabilityTopology {
			return nil, errors.New("receipt discovery used broad ledger probe")
		}
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
		}, nil
	}
	if request.Operation != shardservice.ReplicatedRouteSettlement ||
		request.Capability != serviceauthz.CapabilityRequestLedger ||
		request.Fence != state.Fence || !bytes.Equal(request.RouteSettlement.Command, client.exact) ||
		request.RouteSettlement.MinimumApplied != 17 {
		return nil, errors.New("receipt operation changed its fence or exact command")
	}
	value, err := shardservice.AppendReplicatedRouteSettlementValue(nil,
		shardservice.ReplicatedRouteSettlementValue{
			Mode:                      shardservice.ReplicatedRouteSettlementReadReleaseReceipt,
			CompletionAppliedSequence: 11, Completion: client.completion,
		})
	if err != nil {
		return nil, err
	}
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedRouteSettlementResult, HasState: true,
		State: state, ReadApplied: 17, Value: value,
	}, nil
}

func TestReplicatedExecutorReadsExactReleaseReceiptWithCatalogProbeScope(t *testing.T) {
	route, _, states := testReplicatedRouteCommand(t)
	for address, state := range states {
		state.Commit, state.Applied, state.CheckpointApplied = 20, 20, 20
		states[address] = state
	}
	exact := testRouteReleaseReceiptCommand(t, route)
	view, err := replication.OpenCommand(exact)
	if err != nil {
		t.Fatal(err)
	}
	completion := lifecycleRunnerCompletion(t, view)
	authority := serviceauthz.Authority{Node: [16]byte{0xa1}, Generation: 7}
	ctx, err := serviceauthz.WithAuthority(context.Background(), authority)
	if err != nil {
		t.Fatal(err)
	}
	client := &routeReleaseReceiptClient{
		authority: authority, states: states, exact: exact, completion: completion,
	}
	executor, err := NewReplicatedExecutor(client, 2, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.ReadRouteReleaseReceipt(ctx, route, exact, 17)
	if err != nil {
		t.Fatalf("receipt read error=%v", err)
	}
	if !bytes.Equal(result.Completion, completion) || result.Outcome.Code != raftserve.OutcomeCompletion ||
		result.Outcome.AppliedIndex != 17 || result.Outcome.CompletionAppliedSequence != 11 || result.Outcome.CompletionBytes != len(completion) {
		t.Fatalf("receipt result=%+v completion=%x want=%x", result.Outcome, result.Completion, completion)
	}
	if len(client.operations) != 3 || client.operations[0] != shardservice.ReplicatedProbe ||
		client.operations[1] != shardservice.ReplicatedProbe ||
		client.operations[2] != shardservice.ReplicatedRouteSettlement ||
		client.capability[0] != serviceauthz.CapabilityTopology ||
		client.capability[1] != serviceauthz.CapabilityTopology ||
		client.capability[2] != serviceauthz.CapabilityRequestLedger {
		t.Fatalf("receipt operation scopes=%v capabilities=%v", client.operations, client.capability)
	}
}

type lifecycleReceiptResolver struct {
	route     ReplicatedRoute
	strictErr bool
}

func (resolver *lifecycleReceiptResolver) ResolveDurableRequestTarget(
	context.Context, DurableRequestLogicalTarget,
) (ReplicatedRoute, error) {
	if resolver.strictErr {
		return ReplicatedRoute{}, ErrDurableRequestConflict
	}
	return cloneDurableRequestRoute(resolver.route), nil
}

func (resolver *lifecycleReceiptResolver) resolveDurableReleaseReceiptRoute(
	_ context.Context, _ DurableRequestLogicalTarget, exact []byte,
) (ReplicatedRoute, error) {
	if len(exact) == 0 {
		return ReplicatedRoute{}, ErrDurableRequestConflict
	}
	return cloneDurableRequestRoute(resolver.route), nil
}

func (resolver *lifecycleReceiptResolver) resolveDurableSessionRoute(
	_ context.Context, _ []byte,
) (ReplicatedRoute, error) {
	return cloneDurableRequestRoute(resolver.route), nil
}

type lifecycleReceiptProposer struct {
	*lifecycleRunnerProposer
	exact          []byte
	route          ReplicatedRoute
	completion     []byte
	receiptCalls   int
	receiptContext []serviceauthz.Authority
}

func (proposer *lifecycleReceiptProposer) ReadRouteReleaseReceipt(
	ctx context.Context, route ReplicatedRoute, exact []byte, minimumApplied uint64,
) (ReplicatedResult, error) {
	authority, _ := serviceauthz.FromContext(ctx)
	proposer.receiptContext = append(proposer.receiptContext, authority)
	proposer.receiptCalls++
	if !sameReplicatedCatalogRoute(route, proposer.route) || minimumApplied == 0 || !bytes.Equal(exact, proposer.exact) {
		return ReplicatedResult{}, ErrDurableRequestConflict
	}
	return ReplicatedResult{
		Outcome: raftserve.Outcome{
			Code: raftserve.OutcomeCompletion, AppliedIndex: 11,
			CompletionBytes: len(proposer.completion), CompletionAppliedSequence: 11,
		},
		Completion: bytes.Clone(proposer.completion),
	}, nil
}

func TestDurableRequestLifecycleRunnerRecordsReleasedFromExactReceipt(t *testing.T) {
	wave, initial, route := lifecycleRunnerFixture(t)
	service := serviceauthz.Authority{Node: [16]byte{0xa5}, Generation: 9}
	resolver := &lifecycleReceiptResolver{route: route}
	base := &lifecycleRunnerProposer{
		t: t, events: new(lifecycleRunnerEvents),
		faultKind: int(replication.CommandRouteGate), faultGate: routegate.OperationReleaseShared,
		attempts: make(map[replication.CommandKind][][]byte),
	}
	runner, err := NewDurableRequestLifecycleRunner(&lifecycleRunnerLedger{head: initial, events: base.events}, resolver, &ReplicatedExecutor{}, service)
	if err != nil {
		t.Fatal(err)
	}
	proposer := &lifecycleReceiptProposer{lifecycleRunnerProposer: base, route: route}
	runner.proposer, runner.pinFencer, runner.gateSessions = proposer, proposer, proposer
	ledger := runner.ledger.(*lifecycleRunnerLedger)
	if _, err := runner.RunWave(t.Context(), wave); !errors.Is(err, errLifecycleRunnerFault) {
		t.Fatalf("lost release response error=%v", err)
	}
	if ledger.route.Phase != requestledger.RoutePinReleasing || ledger.head.OutstandingRoutePinDigest == (requestledger.Digest{}) {
		t.Fatalf("lost release response did not leave canonical Releasing cut: head=%+v route=%+v", ledger.head, ledger.route)
	}
	proposer.exact = bytes.Clone(ledger.route.Command)
	view, err := replication.OpenCommand(proposer.exact)
	if err != nil {
		t.Fatal(err)
	}
	proposer.completion = lifecycleRunnerCompletion(t, view)
	resolver.strictErr = true
	base.faultKind = -1
	if _, err := runner.RunWave(t.Context(), wave); err != nil {
		t.Fatalf("receipt recovery error=%v", err)
	}
	if proposer.receiptCalls != 1 || len(proposer.receiptContext) != 1 || proposer.receiptContext[0] != service ||
		ledger.route.Phase != requestledger.RoutePinReleased || ledger.head.OutstandingRoutePinDigest != (requestledger.Digest{}) ||
		ledger.pending.Revision != 0 || !bytes.Equal(ledger.route.Command, proposer.exact) ||
		len(base.attempts[replication.CommandRouteGate]) != 2 {
		t.Fatalf("receipt release cut=%+v head=%+v pending=%+v calls=%d auth=%v proposals=%v", ledger.route, ledger.head, ledger.pending, proposer.receiptCalls, proposer.receiptContext, base.attempts[replication.CommandRouteGate])
	}
}

// retiredReleaseProposer answers the exact release re-proposal with the
// durable session-window refusal a shard returns once an earlier attempt of
// that release has applied and its sequence has been retired.
type retiredReleaseProposer struct {
	*lifecycleReceiptProposer
	retired bool
}

func (proposer *retiredReleaseProposer) Propose(
	ctx context.Context, route ReplicatedRoute, exact []byte,
) (ReplicatedResult, error) {
	if proposer.retired && bytes.Equal(exact, proposer.exact) {
		proposer.events.add("propose:retry-retired")
		return ReplicatedResult{}, &ReplicatedRefusalError{
			Code:    shardservice.ReplicatedRefusalRetryRetired,
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeRetryRetired},
		}
	}
	return proposer.lifecycleReceiptProposer.Propose(ctx, route, exact)
}

func TestDurableRequestLifecycleRunnerSettlesRetiredReleaseFromReceipt(t *testing.T) {
	wave, initial, route := lifecycleRunnerFixture(t)
	service := serviceauthz.Authority{Node: [16]byte{0xa6}, Generation: 9}
	resolver := &lifecycleReceiptResolver{route: route}
	base := &lifecycleRunnerProposer{
		t: t, events: new(lifecycleRunnerEvents),
		faultKind: int(replication.CommandRouteGate), faultGate: routegate.OperationReleaseShared,
		attempts: make(map[replication.CommandKind][][]byte),
	}
	runner, err := NewDurableRequestLifecycleRunner(&lifecycleRunnerLedger{head: initial, events: base.events}, resolver, &ReplicatedExecutor{}, service)
	if err != nil {
		t.Fatal(err)
	}
	proposer := &retiredReleaseProposer{lifecycleReceiptProposer: &lifecycleReceiptProposer{
		lifecycleRunnerProposer: base, route: route,
	}}
	runner.proposer, runner.pinFencer, runner.gateSessions = proposer, proposer, proposer
	ledger := runner.ledger.(*lifecycleRunnerLedger)
	if _, err := runner.RunWave(t.Context(), wave); !errors.Is(err, errLifecycleRunnerFault) {
		t.Fatalf("lost release response error=%v", err)
	}
	if ledger.route.Phase != requestledger.RoutePinReleasing {
		t.Fatalf("lost release response did not leave Releasing: %+v", ledger.route)
	}
	proposer.exact = bytes.Clone(ledger.route.Command)
	view, err := replication.OpenCommand(proposer.exact)
	if err != nil {
		t.Fatal(err)
	}
	proposer.completion = lifecycleRunnerCompletion(t, view)
	base.faultKind = -1
	proposer.retired = true
	// The route still resolves; only the retired session window refuses the
	// exact re-proposal.
	if _, err := runner.RunWave(t.Context(), wave); err != nil {
		t.Fatalf("retired release was not settled from its receipt: %v", err)
	}
	if proposer.receiptCalls != 1 || proposer.receiptContext[0] != service ||
		ledger.route.Phase != requestledger.RoutePinReleased ||
		ledger.head.OutstandingRoutePinDigest != (requestledger.Digest{}) || ledger.pending.Revision != 0 {
		t.Fatalf("retired release cut=%+v head=%+v pending=%+v receipts=%d", ledger.route, ledger.head, ledger.pending, proposer.receiptCalls)
	}
}
