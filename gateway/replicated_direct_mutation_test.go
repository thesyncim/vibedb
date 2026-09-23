package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestReplicatedDirectMutationRecoversLostReplyAcrossOwnershipAndPeerChange(t *testing.T) {
	route, client, _ := newRouteSessionMachine(t)
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(
		t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	tenant := []byte("lost-direct-reply")
	key := requestledger.RequestKey{
		Scope:        requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		Principal:    requestledger.PrincipalID{0x51}, Request: requestledger.RequestID{0x61},
		IssuerEpoch: 7, IssuerSequence: 9, IssuerLane: requestledger.IssuerLane{0x71},
	}
	request := ReplicatedDirectMutation{
		Key: key, RequestDigest: replication.Digest{0x81}, Tenant: tenant,
		Target: ReplicatedTransactionTarget{
			Route: route, BucketBits: 8,
			IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
			Batches: []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{
				Kind: replication.MutationPutAbsent, Key: []byte("lost-reply-row"),
				Value: []byte(`{"id":"lost-reply-row","n":1}`),
			}}}},
		},
	}
	client.hideDirect = true
	first, err := executor.DirectMutate(ctx, request)
	if !errors.Is(err, raftservice.ErrOutcomeUnknown) || first.ID != (distributedtxn.ID{}) ||
		client.state.Applied != 2 || len(client.firstDirect) == 0 {
		t.Fatalf("lost response first=%+v applied=%d err=%v", first, client.state.Applied, err)
	}

	request.Target.Route = advanceRouteSessionOwnership(t, client, route)
	if request.Target.Route.Group != route.Group || request.Target.Route.AllocationGeneration != route.AllocationGeneration ||
		request.Target.Route.Command.OwnershipEpoch != route.Command.OwnershipEpoch+1 ||
		request.Target.Route.Replicas[2].Member == route.Replicas[2].Member ||
		!directMutationRouteAdvanceAllowed(route, request.Target.Route) {
		t.Fatalf("test did not advance the same allocation to new peers: old=%+v new=%+v", route, request.Target.Route)
	}

	retried, err := executor.DirectMutate(ctx, request)
	if err != nil || !retried.Duplicate || !retried.Committed || retried.AffectedRows != 1 ||
		retried.Applied != 2 || client.state.Applied != 5 || len(client.retryDirect) == 0 {
		t.Fatalf("exact replay=%+v applied=%d first=%x retry=%x err=%v",
			retried, client.state.Applied, client.firstDirect, client.retryDirect, err)
	}
	stored, err := client.machine.PointReadInto(
		1, []byte("lost-reply-row"), client.state.Applied, replication.MaxMutationValueBytes, nil,
	)
	if err != nil || !stored.Found || !bytes.Equal(stored.Value, []byte(`{"id":"lost-reply-row","n":1}`)) {
		t.Fatalf("replayed row=%q found=%v err=%v", stored.Value, stored.Found, err)
	}
}

func TestReplicatedDirectMutationDiscoversStillServingRetiringSource(t *testing.T) {
	route, client, _ := newRouteSessionMachine(t)
	source := route.Replicas[0]
	publication, err := client.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: 2, Term: client.state.Fence.Term, Type: pb.EntryConfChangeV2,
	}, &pb.ConfState{Voters: []uint64{1, 2, 3, 4}})
	if err != nil {
		t.Fatalf("install four-voter overlap configuration: %v", err)
	}
	client.state.Applied, client.state.Commit = publication.Applied, publication.Applied
	route.Command.ReplicaSetVersion = publication.ReplicaSetVersion
	client.state.Fence.Command = route.Command

	target := ReplicatedEndpoint{
		Member: 4, Node: [16]byte{4}, StoreID: [16]byte{44}, NodeIncarnation: 14,
		Endpoint: "d4", DataAddress: "d4", NativeEndpoint: "n4", Address: "m4",
		ControlEndpoint: "c4", ControlAddress: "c4",
	}
	route.Replicas = []ReplicatedEndpoint{target, route.Replicas[1], route.Replicas[2]}
	route.discoveryReplica, route.hasDiscoveryReplica = source, true
	client.state.LeaderID = source.Member
	if !validReplicatedRoute(route) {
		t.Fatalf("constructed source-discovery route is invalid: %+v source=%+v", route, source)
	}

	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(
		t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	documentKey := []byte("retiring-source-row")
	document := []byte(`{"id":"retiring-source-row","n":1}`)
	request := ReplicatedDirectMutation{
		Key: requestledger.RequestKey{
			Scope: requestledger.ScopeAuthenticated, TenantDigest: requestledger.Digest(sha256.Sum256([]byte("transition"))),
			Principal: requestledger.PrincipalID{0x81}, Request: requestledger.RequestID{0x82},
			IssuerEpoch: 1, IssuerSequence: 1, IssuerLane: requestledger.IssuerLane{0x83},
		},
		RequestDigest: replication.Digest{0x84}, Tenant: []byte("transition"),
		Target: ReplicatedTransactionTarget{
			Route: route, BucketBits: 8,
			IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
			Batches: []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{
				Kind: replication.MutationPutAbsentOrEqual, Key: documentKey, Value: document,
			}}}},
		},
	}
	result, err := executor.DirectMutate(ctx, request)
	if err != nil || !result.Committed || result.AffectedRows != 1 || result.Applied != 3 {
		t.Fatalf("direct write through source leader result=%+v err=%v", result, err)
	}
	if len(client.proposalMembers) == 0 || client.proposalMembers[len(client.proposalMembers)-1] != source.Member {
		t.Fatalf("data proposal did not reach the still-serving source %d: proposals=%v", source.Member, client.proposalMembers)
	}
	stored, err := client.machine.PointReadInto(1, documentKey, result.Applied, replication.MaxMutationValueBytes, nil)
	if err != nil || !stored.Found || !bytes.Equal(stored.Value, document) {
		t.Fatalf("source-led write value=%q found=%v err=%v", stored.Value, stored.Found, err)
	}

	removed, err := client.machine.ApplyConfiguration(raftmodel.ApplyMeta{
		Index: client.state.Applied + 1, Term: client.state.Fence.Term, Type: pb.EntryConfChangeV2,
	}, &pb.ConfState{Voters: []uint64{2, 3, 4}})
	if err != nil {
		t.Fatalf("remove source voter: %v", err)
	}
	client.state.Applied, client.state.Commit = removed.Applied, removed.Applied
	route.Command.ReplicaSetVersion = removed.ReplicaSetVersion
	client.state.Fence.Command = route.Command
	client.state.LeaderID = source.Member // a removed member cannot remain authoritative.
	route.hasDiscoveryReplica = false
	route.discoveryReplica = ReplicatedEndpoint{}
	proposalsBeforeRemovalRetry := len(client.proposalMembers)
	request.Key.Request[0]++
	request.Key.IssuerSequence++
	request.RequestDigest[0]++
	request.Target.Route = route
	ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if _, err = executor.DirectMutate(ctx, request); !errors.Is(err, ErrReplicatedLeader) {
		t.Fatalf("G+2 route accepted a removed source leader: %v", err)
	}
	for _, member := range client.proposalMembers[proposalsBeforeRemovalRetry:] {
		if member == source.Member {
			t.Fatalf("post-remove mutation reached retired source member %d: %v", source.Member, client.proposalMembers)
		}
	}
}

func advanceRouteSessionOwnership(t *testing.T, client *routeSessionMachineClient, route ReplicatedRoute) ReplicatedRoute {
	return advanceRouteSessionOwnershipMode(t, client, route, true)
}

func advanceRouteSessionOwnershipSameRoster(t *testing.T, client *routeSessionMachineClient, route ReplicatedRoute) ReplicatedRoute {
	return advanceRouteSessionOwnershipMode(t, client, route, false)
}

func advanceRouteSessionOwnershipMode(t *testing.T, client *routeSessionMachineClient, route ReplicatedRoute, replacePeer bool) ReplicatedRoute {
	t.Helper()
	snapshot, err := client.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	initial := snapshot.Fence()
	if err = snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	expectedReplicaSetVersion := initial.ReplicaSetVersion
	transitionIndex := client.state.Applied + 1
	if replacePeer {
		publication, applyErr := client.machine.ApplyConfiguration(raftmodel.ApplyMeta{
			Index: transitionIndex, Term: client.state.Fence.Term, Type: pb.EntryConfChangeV2,
		}, &pb.ConfState{Voters: []uint64{1, 2, 4}})
		if applyErr != nil {
			t.Fatalf("replace physical member: %v", applyErr)
		}
		expectedReplicaSetVersion = publication.ReplicaSetVersion
		transitionIndex++
	}
	transition := replicatedstate.OwnershipTransition{
		From: initial.Binding, ExpectedReplicaSetVersion: expectedReplicaSetVersion,
		SourceMember: 1, TargetMember: 2,
		ToOwnershipEpoch:  initial.Binding.OwnershipEpoch + 1,
		ToRoutingVersion:  initial.Binding.RoutingVersion + 1,
		ToRouteGeneration: initial.Binding.RouteGeneration + 1,
		ToOwnedRange:      initial.Binding.OwnedRange,
	}
	command, err := replicatedstate.AppendOwnershipTransition(nil, transition)
	if err != nil {
		t.Fatalf("encode ownership transition: %v", err)
	}
	if err = client.machine.AdmitCommand(command); err != nil {
		t.Fatalf("admit ownership transition: %v", err)
	}
	publication, err := client.machine.ApplyNormal(raftmodel.ApplyMeta{
		Index: transitionIndex, Term: client.state.Fence.Term, Type: pb.EntryNormal,
	}, command)
	if err != nil {
		t.Fatalf("apply ownership transition: %v", err)
	}
	currentSnapshot, err := client.machine.Snapshot("docs")
	if err != nil {
		t.Fatal(err)
	}
	current := currentSnapshot.Fence()
	if err = currentSnapshot.Close(); err != nil {
		t.Fatal(err)
	}
	route.Command = raftservice.CommandFence{
		ReplicaSetVersion:      current.ReplicaSetVersion,
		ActivePolicyGeneration: current.Binding.ActivePolicyGeneration,
		ProtectionEpoch:        current.Binding.ProtectionEpoch,
		OwnershipEpoch:         current.Binding.OwnershipEpoch,
		SchemaGeneration:       current.Binding.SchemaGeneration,
		RelationManifestDigest: current.RelationManifestDigest,
		RoutingVersion:         current.Binding.RoutingVersion,
		RouteGeneration:        current.Binding.RouteGeneration,
	}
	if replacePeer {
		replacement := ReplicatedEndpoint{
			Member: 4, Node: [16]byte{4}, StoreID: [16]byte{44}, NodeIncarnation: 14,
			Endpoint: "m4", DataAddress: "d4", NativeEndpoint: "n4", Address: "m4",
			ControlEndpoint: "c4", ControlAddress: "c4",
		}
		route.Replicas = append(route.Replicas[:2:2], replacement)
	}
	client.state.Applied, client.state.Commit = publication.Applied, publication.Applied
	client.state.Fence.Command = route.Command
	return route
}

func TestReplicatedDirectMutationIsOneProposalWithCrossGatewayExactRetry(t *testing.T) {
	route, client, _ := newRouteSessionMachine(t)
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(
		t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	tenant := []byte("tenant")
	key := requestledger.RequestKey{
		Scope:        requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		Principal:    requestledger.PrincipalID{0x21}, Request: requestledger.RequestID{0x31},
		IssuerEpoch: 7, IssuerSequence: 1, IssuerLane: requestledger.IssuerLane{0x32},
	}
	documentKey := []byte("direct-key")
	document := []byte(`{"id":1,"state":"paid"}`)
	request := ReplicatedDirectMutation{
		Key: key, RequestDigest: replication.Digest{0x41}, Tenant: tenant,
		Target: ReplicatedTransactionTarget{
			Route: route, BucketBits: 8,
			IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
			Batches: []replication.RelationMutationBatch{{
				Relation: 1, Mutations: []replication.Mutation{{
					Kind: replication.MutationPutAbsentOrEqual, Key: documentKey, Value: document,
				}},
			}},
		},
	}
	first, err := executor.DirectMutate(ctx, request)
	if err != nil || !first.Committed || first.AffectedRows != 1 ||
		first.ResultCode != replicatedstate.ResultApplied || first.Applied != 2 ||
		client.state.Applied != 2 {
		t.Fatalf("first direct result=%+v applied=%d err=%v", first, client.state.Applied, err)
	}
	stored, err := client.machine.PointReadInto(
		1, documentKey, 2, replication.MaxMutationValueBytes, nil,
	)
	if err != nil || !stored.Found || !bytes.Equal(stored.Value, document) {
		t.Fatalf("direct value=%q found=%v err=%v", stored.Value, stored.Found, err)
	}

	// A fresh gateway can reconstruct the command solely from caller identity,
	// request digest, route, and canonical mutations. The duplicate proposal
	// returns the first entry's retained applied index and result.
	restarted, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := restarted.DirectMutate(ctx, request)
	if err != nil || !retry.Duplicate || retry.ID != first.ID || retry.Applied != first.Applied ||
		retry.Committed != first.Committed || retry.AffectedRows != first.AffectedRows ||
		retry.ResultCode != first.ResultCode || client.state.Applied != 3 {
		t.Fatalf("direct retry=%+v first=%+v applied=%d err=%v", retry, first, client.state.Applied, err)
	}

	conflicting := request
	conflicting.RequestDigest[0]++
	conflict, err := restarted.DirectMutate(ctx, conflicting)
	if !errors.Is(err, ErrReplicatedTransactionConflict) || conflict.Committed ||
		conflict.ResultCode != replicatedstate.ResultTransactionConflict ||
		client.state.Applied != 4 {
		t.Fatalf("direct conflict=%+v applied=%d err=%v", conflict, client.state.Applied, err)
	}
	stored, err = client.machine.PointReadInto(
		1, documentKey, 4, replication.MaxMutationValueBytes, nil,
	)
	if err != nil || !stored.Found || !bytes.Equal(stored.Value, document) {
		t.Fatalf("conflict changed direct value=%q found=%v err=%v", stored.Value, stored.Found, err)
	}

	next := request
	next.Key.Request[0]++
	next.Key.IssuerSequence++
	next.RequestDigest[0]++
	nextKey := []byte("direct-key-next")
	nextValue := []byte(`{"id":2,"state":"paid"}`)
	next.Target.Batches = []replication.RelationMutationBatch{{
		Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: nextKey, Value: nextValue,
		}},
	}}
	advanced, err := restarted.DirectMutate(ctx, next)
	if err != nil || !advanced.Committed || advanced.Duplicate || advanced.ID != first.ID ||
		advanced.Applied != 5 || client.state.Applied != 5 {
		t.Fatalf("advanced direct=%+v applied=%d err=%v",
			advanced, client.state.Applied, err)
	}
	stale, err := restarted.DirectMutate(ctx, request)
	if !errors.Is(err, ErrReplicatedTransactionConflict) || stale.Committed ||
		stale.ResultCode != replicatedstate.ResultTransactionConflict || client.state.Applied != 6 {
		t.Fatalf("stale direct=%+v applied=%d err=%v", stale, client.state.Applied, err)
	}
}

func TestReplicatedDirectMutationPropagatesRetainedIntentBusy(t *testing.T) {
	route, client, _ := newRouteSessionMachine(t)
	tenant := []byte("intent-busy")
	rowKey := []byte("staged-row")
	rowValue := []byte(`{"id":"staged-row","n":1}`)
	batches := []replication.RelationMutationBatch{{Relation: 1, Mutations: []replication.Mutation{{
		Kind: replication.MutationPutAbsentOrEqual, Key: rowKey, Value: rowValue,
	}}}}
	mutationDigest, err := replication.TransactionMutationDigest(batches)
	if err != nil {
		t.Fatal(err)
	}
	stageID := distributedtxn.ID{0x91}
	stage, err := (&replicatedTransactionCommandEncoder{tenant: tenant}).appendExact(
		nil, replication.RetryHome{1}, route, distributedtxn.ReplicatedCommand{
			Role: distributedtxn.ReplicatedRoleTarget, Operation: distributedtxn.ReplicatedStageTarget,
			ID: stageID, PayloadKind: distributedtxn.ReplicatedPayloadTargetStage,
			ControllerEpoch: 7, ExecutionPinDigest: distributedtxn.Digest{0x19},
			Target: distributedtxn.TransactionTargetStage{
				CoordinatorGroup:            distributedtxn.ID(route.Group.GroupID),
				CoordinatorShardIncarnation: distributedtxn.ID(route.Group.ShardIncarnation),
				CoordinatorAllocation:       route.AllocationGeneration,
				BucketBits:                  8,
				IntentScopes:                []distributedtxn.IntentScope{{Start: 0, End: 256}},
				MutationDigest:              mutationDigest,
			},
		}, batches,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.machine.AdmitCommand(stage); err != nil {
		t.Fatal(err)
	}
	stageIndex := client.state.Applied + 1
	publication, err := client.machine.ApplyNormal(raftmodel.ApplyMeta{
		Index: stageIndex, Term: client.state.Fence.Term, Type: pb.EntryNormal,
	}, stage)
	if err != nil {
		t.Fatal(err)
	}
	client.state.Applied, client.state.Commit = publication.Applied, publication.Applied

	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(
		t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	key := requestledger.RequestKey{
		Scope:        requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		Principal:    requestledger.PrincipalID{0xa1}, Request: requestledger.RequestID{0xa2},
		IssuerEpoch: 7, IssuerSequence: 1, IssuerLane: requestledger.IssuerLane{0xa3},
	}
	request := ReplicatedDirectMutation{
		Key: key, RequestDigest: replication.Digest{0xa4}, Tenant: tenant,
		Target: ReplicatedTransactionTarget{
			Route: route, BucketBits: 8,
			IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
			Batches:      batches,
		},
	}
	first, err := executor.DirectMutate(ctx, request)
	if !errors.Is(err, ErrReplicatedTransactionConflict) || first.Committed ||
		first.ResultCode != replicatedstate.ResultIntentBusy || first.ID == (distributedtxn.ID{}) {
		t.Fatalf("intent-busy direct result=%+v err=%v", first, err)
	}
	retry, err := executor.DirectMutate(ctx, request)
	if !errors.Is(err, ErrReplicatedTransactionConflict) || retry.Committed || !retry.Duplicate ||
		retry.ResultCode != replicatedstate.ResultIntentBusy || retry.ID != first.ID ||
		retry.Applied != first.Applied {
		t.Fatalf("retained intent-busy retry=%+v first=%+v err=%v", retry, first, err)
	}
}

func TestReplicatedDirectInt64DeltaConcurrentSameKeyIncrements(t *testing.T) {
	route, client, _ := newRouteSessionMachine(t)
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(
		t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	tenant := []byte("concurrent-delta-tenant")
	baseKey := requestledger.RequestKey{
		Scope:        requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		Principal:    requestledger.PrincipalID{0x51}, Request: requestledger.RequestID{0x61},
		IssuerEpoch: 7, IssuerSequence: 1, IssuerLane: requestledger.IssuerLane{0x71},
	}
	target := ReplicatedTransactionTarget{
		Route: route, BucketBits: 8,
		IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
		Batches: []replication.RelationMutationBatch{{
			Relation: 1, Mutations: []replication.Mutation{{
				Kind: replication.MutationPutAbsentOrEqual, Key: []byte("counter"),
				Value: []byte(`{"id":"counter","score":0,"keep":"x"}`),
			}},
		}},
	}
	seed := ReplicatedDirectMutation{
		Key: baseKey, RequestDigest: replication.Digest{0x81}, Tenant: tenant, Target: target,
	}
	if result, err := executor.DirectMutate(ctx, seed); err != nil || !result.Committed {
		t.Fatalf("seed result=%+v err=%v", result, err)
	}
	descriptor, err := replication.AppendJSONInt64Delta(nil, "score", 1)
	if err != nil {
		t.Fatal(err)
	}
	const increments = 16
	errs := make(chan error, increments)
	for i := 0; i < increments; i++ {
		i := i
		go func() {
			request := baseKey
			request.Request[0] = byte(0x62 + i)
			request.IssuerLane[0] = byte(0x72 + i)
			request.IssuerSequence = 1
			mutation := replication.Mutation{
				Kind: replication.MutationJSONInt64Delta, Key: []byte("counter"),
				Value: descriptor,
			}
			requestDigest := replication.Digest{byte(0x91 + i)}
			requestTarget := target
			requestTarget.Batches = []replication.RelationMutationBatch{{
				Relation: 1, Mutations: []replication.Mutation{mutation},
			}}
			result, callErr := executor.DirectMutate(ctx, ReplicatedDirectMutation{
				Key: request, RequestDigest: requestDigest, Tenant: tenant, Target: requestTarget,
			})
			if callErr != nil || !result.Committed || result.AffectedRows != 1 {
				errs <- fmt.Errorf("increment %d result=%+v err=%v", i, result, callErr)
				return
			}
			errs <- nil
		}()
	}
	for i := 0; i < increments; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	stored, err := client.machine.PointReadInto(1, []byte("counter"), client.state.Applied, replication.MaxMutationValueBytes, nil)
	if err != nil || !stored.Found || !bytes.Contains(stored.Value, []byte(`"score":16`)) ||
		!bytes.Contains(stored.Value, []byte(`"keep":"x"`)) {
		t.Fatalf("concurrent counter=%q found=%v applied=%d err=%v", stored.Value, stored.Found, client.state.Applied, err)
	}
}

func TestReplicatedDirectMultiInt64DeltaConcurrentSameKeyIncrements(t *testing.T) {
	route, client, _ := newRouteSessionMachine(t)
	executor, err := NewReplicatedExecutor(client, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := serviceauthz.WithAuthority(
		t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	tenant := []byte("concurrent-multi-delta-tenant")
	baseKey := requestledger.RequestKey{
		Scope:        requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		Principal:    requestledger.PrincipalID{0x52}, Request: requestledger.RequestID{0x62},
		IssuerEpoch: 7, IssuerSequence: 1, IssuerLane: requestledger.IssuerLane{0x72},
	}
	target := ReplicatedTransactionTarget{
		Route: route, BucketBits: 8,
		IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
		Batches: []replication.RelationMutationBatch{{
			Relation: 1, Mutations: []replication.Mutation{{
				Kind: replication.MutationPutAbsentOrEqual, Key: []byte("multi-counter"),
				Value: []byte(`{"id":"multi-counter","score":0,"total":0,"keep":"x"}`),
			}},
		}},
	}
	seed := ReplicatedDirectMutation{
		Key: baseKey, RequestDigest: replication.Digest{0x82}, Tenant: tenant, Target: target,
	}
	if result, err := executor.DirectMutate(ctx, seed); err != nil || !result.Committed {
		t.Fatalf("seed result=%+v err=%v", result, err)
	}
	descriptor, err := replication.AppendJSONInt64Deltas(nil, []replication.JSONInt64DeltaField{
		{Column: "score", Delta: 1}, {Column: "total", Delta: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	const increments = 16
	errs := make(chan error, increments)
	for i := 0; i < increments; i++ {
		i := i
		go func() {
			request := baseKey
			request.Request[0] = byte(0x63 + i)
			request.IssuerLane[0] = byte(0x73 + i)
			mutation := replication.Mutation{
				Kind: replication.MutationJSONInt64Delta, Key: []byte("multi-counter"),
				Value: descriptor,
			}
			requestDigest := replication.Digest{byte(0xa1 + i)}
			requestTarget := target
			requestTarget.Batches = []replication.RelationMutationBatch{{
				Relation: 1, Mutations: []replication.Mutation{mutation},
			}}
			result, callErr := executor.DirectMutate(ctx, ReplicatedDirectMutation{
				Key: request, RequestDigest: requestDigest, Tenant: tenant, Target: requestTarget,
			})
			if callErr != nil || !result.Committed || result.AffectedRows != 1 {
				errs <- fmt.Errorf("increment %d result=%+v err=%v", i, result, callErr)
				return
			}
			errs <- nil
		}()
	}
	for i := 0; i < increments; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	stored, err := client.machine.PointReadInto(1, []byte("multi-counter"), client.state.Applied, replication.MaxMutationValueBytes, nil)
	if err != nil || !stored.Found || !bytes.Contains(stored.Value, []byte(`"score":16`)) ||
		!bytes.Contains(stored.Value, []byte(`"total":32`)) || !bytes.Contains(stored.Value, []byte(`"keep":"x"`)) {
		t.Fatalf("concurrent multi counter=%q found=%v applied=%d err=%v", stored.Value, stored.Found, client.state.Applied, err)
	}
}
