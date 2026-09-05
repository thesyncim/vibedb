package raftservice_test

import (
	"bytes"
	"context"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type semanticRF3Owner struct {
	*Owner
	t            testing.TB
	serving      atomic.Bool
	revokeAtRead atomic.Bool
	transitional atomic.Bool
}

func (owner *semanticRF3Owner) SubmitOwnedAuthorized(ctx context.Context, fence raftservice.ServingFence, command []byte, authorize raftservice.ProposalAuthorization) (raftservice.Result, error) {
	result, err := owner.Owner.SubmitOwnedAuthorized(ctx, fence, command, authorize)
	if err != nil {
		owner.t.Logf("semantic owner proposal failure: %v; outcome=%+v", err, result.Outcome)
	}
	return result, err
}

func (owner *semanticRF3Owner) ReadLinearizableDataInto(ctx context.Context, request raftservice.LinearizableDataReadRequest, cut *raftservice.LinearizableDataReadCut) error {
	if owner.revokeAtRead.Swap(false) {
		owner.serving.Store(false)
	}
	return owner.Owner.ReadLinearizableDataInto(ctx, request, cut)
}

// This uses the production SQL apply source, authenticated RF3 replication and
// real owner ReadIndex barriers. On unsupported filesystems its fixture skips;
// VIBEDB_RF3_QUORUM_QUALIFICATION=1 makes that a hard failure in Linux campaigns.
func TestRF3SemanticLocalTLSQueriesAndRevocation(t *testing.T) {
	cluster := newMultiGroupRF3Cluster(t, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	group := cluster.groups[0].key
	if err := cluster.owners[0].Campaign(ctx, group); err != nil {
		t.Fatal(err)
	}
	leader := waitRF3Leader(t, ctx, cluster.owners[:], nil, group)
	waitRF3Applied(t, ctx, cluster.owners[:], nil, group, 2)
	route := cluster.route(0)
	domain := rafttransport.TrustDomain{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation}
	credentials := newPeerServerTestAuthority(t)
	gatewayIdentity := rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{92}}
	gatewayTLS := newPeerServerTestTLS(t, credentials, gatewayIdentity)
	actor := serviceauthz.Authority{Node: rafttransport.NodeID{93}, Generation: 1}
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{
		{Node: gatewayIdentity.Node, Capabilities: serviceauthz.CapabilityDelegate},
		{Node: actor.Node, Capabilities: serviceauthz.CapabilityDataRead | serviceauthz.CapabilityDataWrite},
	})
	if err != nil {
		t.Fatal(err)
	}
	gate, _ := serviceauthz.NewGate(policy)
	ctx, err = serviceauthz.WithAuthority(ctx, actor)
	if err != nil {
		t.Fatal(err)
	}
	deadline := func() time.Time { return time.Now().Add(2 * time.Second) }
	remote, err := gateway.NewAuthenticatedReplicatedClient(gateway.AuthenticatedReplicatedClientOptions{
		TLS: gatewayTLS, Dial: func(ctx context.Context, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		},
		HandshakeDeadline: deadline, MaxConnections: 8, MaxPerEndpoint: 4, MaxIdlePerEndpoint: 2,
		MaxWaiters: 8, MaxIdleAge: time.Minute, MaxLifetime: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = remote.Close() })
	var servers [3]*shardservice.ReplicatedServer
	var tls [3]*shardservice.ReplicatedServerTLS
	var owners [3]*semanticRF3Owner
	var local *gateway.ReplicatedNodeClient
	for member := range servers {
		owner := &semanticRF3Owner{Owner: cluster.owners[member], t: t}
		owner.serving.Store(true)
		owners[member] = owner
		server, err := shardservice.NewReplicatedServer(owner, shardservice.DefaultReplicatedInFlightFrameBytes, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		servers[member] = server
		if err := server.BindAuthorization(gate, nil); err != nil {
			t.Fatal(err)
		}
		if err := server.BindServingAuthority(func(raftservice.ServingState) bool { return owner.serving.Load() }); err != nil {
			t.Fatal(err)
		}
		if err := server.BindTransitionalServingAuthority(func(_ raftservice.ServingState, request *shardservice.ReplicatedRequest) bool {
			return owner.transitional.Load() && request.Operation == shardservice.ReplicatedQueryLeader
		}); err != nil {
			t.Fatal(err)
		}
		storageTLS := newPeerServerTestTLS(t, credentials, rafttransport.PeerIdentity{TrustDomain: domain, Node: route.Replicas[member].Node})
		tls[member], err = shardservice.NewReplicatedServerTLS(storageTLS, []rafttransport.NodeID{gatewayIdentity.Node})
		if err != nil {
			t.Fatal(err)
		}
		if member == leader {
			local, err = gateway.NewReplicatedNodeClient(tls[member], gatewayTLS, server, remote)
			if err != nil {
				t.Fatal(err)
			}
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		route.Replicas[member].Address = listener.Addr().String()
		route.Replicas[member].NativeEndpoint = listener.Addr().String()
		serveCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- server.ServeAuthenticated(serveCtx, listener, tls[member], deadline, 16, 4) }()
		t.Cleanup(func() {
			stop()
			_ = listener.Close()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("native listener did not stop")
			}
		})
	}
	// Use the production gateway retry/session protocol to commit the rows.
	executor, err := gateway.NewReplicatedExecutor(local, 4, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	session, err := gateway.NewNativeSession(gateway.NativeSessionOptions{
		Executor: executor, Route: route, Distribution: string(route.Distribution), Shard: string(route.Shard),
		Tenant: []byte("tenant"), ClientID: replication.ID128{0x44}, ProposalCapability: serviceauthz.CapabilityDataWrite,
		Resolver: gateway.BaseRelationResolver{Relation: 1}, MaxRelationBatches: 4, MaxMutations: 8,
		InitialCommandBytes: 512, MaxCommandBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Open(ctx, 2_000_000_000_000_000_000); err != nil {
		t.Fatal(err)
	}
	keys := make([][]byte, 2)
	for index, id := range []string{"a", "b"} {
		var ok bool
		keys[index], ok = orderedkey.AppendJSONString(nil, []byte(`"`+id+`"`), orderedkey.Ascending)
		if !ok {
			t.Fatal("key encoding")
		}
		if _, err := session.Put(ctx, keys[index], []byte(`{"id":"`+id+`","value":""}`)); err != nil {
			t.Fatal(err)
		}
	}
	state, err := cluster.owners[leader].Probe(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	fence := state.Fence()
	nativeFence := shardservice.ReplicatedFence{Group: fence.Group, AllocationGeneration: fence.AllocationGeneration,
		Command: fence.Command, MemberID: fence.MemberID, StoreID: fence.StoreID, NodeIncarnation: fence.NodeIncarnation, Term: fence.Term}
	makeCall := func(sql string) shardservice.ReplicatedCall {
		return shardservice.ReplicatedCall{Request: shardservice.ReplicatedRequest{Operation: shardservice.ReplicatedQueryLeader,
			Authority: actor, Capability: serviceauthz.CapabilityDataRead, Fence: nativeFence, MaxValueBytes: 4096},
			SQL: &shardservice.ShardRequest{Authority: actor, SQL: sql, Distribution: route.Distribution, Shard: route.Shard,
				AllocationGeneration: distribution.ShardAllocationGeneration(route.AllocationGeneration),
				RoutingVersion:       distribution.RoutingVersion(route.Command.RoutingVersion), OwnershipEpoch: distribution.OwnershipEpoch(route.Command.OwnershipEpoch),
				MaxRows: 20, MaxResultBytes: 4096}}
	}
	endpoint := route.Replicas[leader]
	var retained *shardservice.ShardResponse
	for _, sql := range []string{
		`SELECT id, value FROM docs WHERE id = 'a'`,
		`SELECT id FROM docs WHERE id = 'missing'`,
		`SELECT id, value FROM docs ORDER BY id`,
		`SELECT value, COUNT(*) FROM docs GROUP BY value`,
		`SELECT invalid FROM missing_table`,
	} {
		call := makeCall(sql)
		before := servers[leader].Stats().Accepted
		direct, err := local.DoReplicatedCall(ctx, endpoint, &call)
		if err != nil {
			t.Fatalf("local %s: %v", sql, err)
		}
		if servers[leader].Stats().Accepted != before || len(direct.Response.Value) != 0 {
			t.Fatal("local SQL crossed a socket or nested result frame")
		}
		wire, err := remote.DoReplicatedCall(ctx, endpoint, &call)
		if err != nil || !reflect.DeepEqual(direct.SQL, wire.SQL) || direct.Response.Kind != wire.Response.Kind || direct.Response.Refusal != wire.Response.Refusal || direct.Response.ReadApplied != wire.Response.ReadApplied {
			t.Fatalf("local/TLS mismatch for %s: direct=%+v remote=%+v err=%v", sql, direct, wire, err)
		}
		if direct.SQL != nil && len(direct.SQL.Rows) != 0 && retained == nil {
			retained = direct.SQL
		}
	}
	if remote.Stats().Dials == 0 || servers[leader].Stats().SemanticDispatch == 0 {
		t.Fatal("both transport paths were not exercised")
	}
	if retained == nil || string(retained.Rows[0][0].Bytes) != `"a"` || string(retained.Rows[0][1].Bytes) != `""` {
		t.Fatalf("retained point result=%+v", retained)
	}
	for _, transport := range []gateway.ReplicatedCallRoundTripper{local, remote} {
		bounded := makeCall(`SELECT id FROM docs`)
		bounded.Request.MaxValueBytes = 1
		boundedReply, boundedErr := transport.DoReplicatedCall(ctx, endpoint, &bounded)
		if boundedErr != nil || boundedReply.Response.Kind != shardservice.ReplicatedRefusal || boundedReply.Response.Refusal != shardservice.ReplicatedRefusalReadBufferBound {
			t.Fatalf("oversized output reply=%+v err=%v", boundedReply, boundedErr)
		}
		call := makeCall(`SELECT id FROM docs`)
		call.Request.Fence.Command.SchemaGeneration++
		reply, err := transport.DoReplicatedCall(ctx, endpoint, &call)
		if err != nil || reply.Response.Kind != shardservice.ReplicatedRefusal || reply.Response.Refusal != shardservice.ReplicatedRefusalStaleFence {
			t.Fatalf("stale schema reply=%+v err=%v", reply, err)
		}
		// Revoke after Probe but before the serialized read admission. A gate
		// checked only before Probe incorrectly returns successful rows here.
		call = makeCall(`SELECT id FROM docs`)
		owners[leader].serving.Store(true)
		owners[leader].revokeAtRead.Store(true)
		reply, err = transport.DoReplicatedCall(ctx, endpoint, &call)
		if err != nil || reply.Response.Kind != shardservice.ReplicatedRefusal || reply.Response.Refusal != shardservice.ReplicatedRefusalUnavailable {
			t.Fatalf("revoked SQL reply=%+v err=%v", reply, err)
		}
		owners[leader].transitional.Store(true)
		reply, err = transport.DoReplicatedCall(ctx, endpoint, &call)
		if err != nil || reply.Response.Kind != shardservice.ReplicatedQueryResult {
			t.Fatalf("authenticated transitional SQL reply=%+v err=%v", reply, err)
		}
		owners[leader].transitional.Store(false)
		owners[leader].serving.Store(true)
	}
	// Change the stored preimage and reuse both execution paths. Retained local
	// columns/cells remain detached from earlier cursors and snapshot cuts.
	if _, err := session.Put(ctx, keys[0], []byte(`{"id":"a","value":"changed"}`)); err != nil {
		t.Fatal(err)
	}
	call := makeCall(`SELECT id, value FROM docs ORDER BY id`)
	if _, err := local.DoReplicatedCall(ctx, endpoint, &call); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(retained.Rows[0][1].Bytes, []byte(`""`)) {
		t.Fatal("retained local result changed after owner reuse")
	}
	// Public and local services remain reachable while consensus peers are
	// isolated. Neither transport may turn the retained local snapshot into a
	// successful linearizable read without a quorum proof.
	cluster.network.isolate(leader)
	for _, transport := range []gateway.ReplicatedCallRoundTripper{local, remote} {
		isolated, stop := context.WithTimeout(ctx, 100*time.Millisecond)
		reply, err := transport.DoReplicatedCall(isolated, endpoint, &call)
		stop()
		if err == nil && reply.Response.Kind == shardservice.ReplicatedQueryResult {
			t.Fatal("isolated local owner served a successful linearizable SQL read")
		}
	}
	for _, server := range servers {
		until := time.Now().Add(3 * time.Second)
		for server.Stats().InFlightFrameBytes != 0 && time.Now().Before(until) {
			time.Sleep(time.Millisecond)
		}
		if got := server.Stats().InFlightFrameBytes; got != 0 {
			t.Fatalf("reply admission leaked: %d", got)
		}
	}
}
