package splitcontroller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardcontrol"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

type fixedShardControlResolver struct {
	route gateway.ReplicatedRoute
	err   error
}

func (resolver fixedShardControlResolver) ResolveShardControl(
	_ context.Context, _ ShardActionTarget, _ Action, _ shardcontrol.Request,
) (gateway.ReplicatedRoute, error) {
	return resolver.route, resolver.err
}

type scriptedShardControlClient struct {
	calls     []uint64
	responses map[uint64][]shardcontrol.Response
	errors    map[uint64][]error
}

func (client *scriptedShardControlClient) DoShardControl(
	_ context.Context, endpoint gateway.ReplicatedEndpoint, _ shardcontrol.Request,
) (shardcontrol.Response, error) {
	client.calls = append(client.calls, endpoint.Member)
	if values := client.errors[endpoint.Member]; len(values) != 0 {
		err := values[0]
		client.errors[endpoint.Member] = values[1:]
		if err != nil {
			return shardcontrol.Response{}, err
		}
	}
	values := client.responses[endpoint.Member]
	if len(values) == 0 {
		return shardcontrol.Response{}, errors.New("unreachable")
	}
	response := values[0]
	client.responses[endpoint.Member] = values[1:]
	return response, nil
}

func TestRoutedShardControlFollowsNotLeaderAndScopesHintToExactGroupFence(t *testing.T) {
	request, route := testShardControlRequestRoute()
	notLeader := shardcontrol.Response{Code: shardcontrol.ResultNotLeader,
		Operation: request.Operation, Step: request.Step, ResultDigest: [32]byte{1}}
	accepted := shardcontrol.Response{Code: shardcontrol.ResultAccepted,
		Operation: request.Operation, Step: request.Step, ResultDigest: [32]byte{2}}
	client := &scriptedShardControlClient{responses: map[uint64][]shardcontrol.Response{
		1: {notLeader}, 2: {accepted, accepted},
	}, errors: make(map[uint64][]error)}
	router, err := NewRoutedShardControl(ShardControlRouterOptions{
		Resolver: fixedShardControlResolver{route: route}, Client: client,
		MaxAttempts: 3, AttemptTimeout: time.Second, HintCapacity: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	action := Action{Kind: ActionSealSource}
	if response, executeErr := router.ExecuteShardControl(context.Background(), action, request); executeErr != nil || response.Code != shardcontrol.ResultAccepted {
		t.Fatalf("response=%+v err=%v", response, executeErr)
	}
	if response, executeErr := router.ExecuteShardControl(context.Background(), action, request); executeErr != nil || response.Code != shardcontrol.ResultAccepted {
		t.Fatalf("warm response=%+v err=%v", response, executeErr)
	}
	if len(client.calls) != 3 || client.calls[0] != 1 || client.calls[1] != 2 || client.calls[2] != 2 {
		t.Fatalf("calls=%v", client.calls)
	}

	// A catalog fence change must not consume the old group's warm hint.
	route.Command.RouteGeneration++
	request.Fence.RouteGeneration++
	payload, _ := openRemoteStepPayload(request)
	payload.Target.Authority.RouteGeneration++
	request.Payload = mustRemoteStepPayload(t, payload)
	client.responses[1] = []shardcontrol.Response{accepted}
	router.resolver = fixedShardControlResolver{route: route}
	if _, executeErr := router.ExecuteShardControl(context.Background(), action, request); executeErr != nil {
		t.Fatal(executeErr)
	}
	if client.calls[len(client.calls)-1] != 1 {
		t.Fatalf("stale hint crossed fence: calls=%v", client.calls)
	}
}

func TestRoutedShardControlRejectsRouteBeforeIOAndBoundsRetry(t *testing.T) {
	request, route := testShardControlRequestRoute()
	client := &scriptedShardControlClient{
		responses: make(map[uint64][]shardcontrol.Response),
		errors: map[uint64][]error{
			1: {errors.New("dial")}, 2: {errors.New("dial")}, 3: {errors.New("dial")},
		},
	}
	router, err := NewRoutedShardControl(ShardControlRouterOptions{
		Resolver: fixedShardControlResolver{route: route}, Client: client,
		MaxAttempts: 2, AttemptTimeout: time.Second, HintCapacity: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, executeErr := router.ExecuteShardControl(
		context.Background(), Action{Kind: ActionSealSource}, request,
	); !errors.Is(executeErr, ErrShardControlUnavailable) || len(client.calls) != 2 {
		t.Fatalf("calls=%v err=%v", client.calls, executeErr)
	}

	client.calls = nil
	payload, _ := openRemoteStepPayload(request)
	payload.Target.Authority.OwnershipEpoch++
	request.Payload = mustRemoteStepPayload(t, payload)
	if _, executeErr := router.ExecuteShardControl(
		context.Background(), Action{Kind: ActionSealSource}, request,
	); !errors.Is(executeErr, ErrShardControlRoute) || len(client.calls) != 0 {
		t.Fatalf("stale fence reached IO: calls=%v err=%v", client.calls, executeErr)
	}
}

func testShardControlRequestRoute() (shardcontrol.Request, gateway.ReplicatedRoute) {
	group := raftmember.GroupKey{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, TopologyRecoveryEpoch: 1,
		ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4},
	}
	command := raftservice.CommandFence{
		ReplicaSetVersion: 1, ActivePolicyGeneration: 2, ProtectionEpoch: 3,
		OwnershipEpoch: 4, SchemaGeneration: 5, RelationManifestDigest: [32]byte{1},
		RoutingVersion: 6, RouteGeneration: 7,
	}
	replicas := make([]gateway.ReplicatedEndpoint, 3)
	for index := range replicas {
		replicas[index] = gateway.ReplicatedEndpoint{
			Member: uint64(index + 1), Node: rafttransport.NodeID{byte(index + 1)},
			StoreID: [16]byte{byte(index + 1)}, NodeIncarnation: 1,
			ControlEndpoint: string(rune('a' + index)), ControlAddress: string(rune('x' + index)),
		}
	}
	request := shardcontrol.Request{
		Action: shardcontrol.ActionSealSource, Operation: [32]byte{1}, Step: [32]byte{2},
		PlanDigest: [32]byte{3},
		Fence: shardcontrol.Fence{
			CatalogGeneration: 1, Allocation: 9, OwnershipEpoch: command.OwnershipEpoch,
			SchemaGeneration: command.SchemaGeneration, RoutingVersion: command.RoutingVersion,
			RouteGeneration: command.RouteGeneration, ReplicaSetVersion: command.ReplicaSetVersion,
			Applied: 10,
		},
	}
	payload := remoteStepPayload{
		Action: uint8(request.Action), Target: ShardActionTarget{
			Group: group, Allocation: 9,
			Authority: sqldriver.ReplicatedAuthorityProfile{
				ActivePolicyGeneration: command.ActivePolicyGeneration,
				ProtectionEpoch:        command.ProtectionEpoch, OwnershipEpoch: command.OwnershipEpoch,
				SchemaGeneration: command.SchemaGeneration, RoutingVersion: command.RoutingVersion,
				RouteGeneration: command.RouteGeneration,
			},
			RelationManifestDigest: command.RelationManifestDigest,
		},
	}
	request.Payload = mustRemoteStepPayload(nil, payload)
	return request, gateway.ReplicatedRoute{
		Distribution: distribution.DistributionName("d"), Shard: distribution.ShardID("s"),
		Group: group, AllocationGeneration: 9, Command: command, Replicas: replicas,
	}
}

func mustRemoteStepPayload(t *testing.T, payload remoteStepPayload) []byte {
	raw, err := vibejson.Marshal(&payload)
	if err == nil {
		raw, err = vibejson.AppendCanonicalize(nil, raw)
	}
	if err != nil {
		if t != nil {
			t.Fatal(err)
		}
		panic(err)
	}
	return raw
}
