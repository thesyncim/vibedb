package shardservice

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func authorizationNode(marker byte) rafttransport.NodeID {
	var node rafttransport.NodeID
	node[0] = marker
	return node
}

func authorizationGate(t testing.TB, generation uint64,
	entries ...serviceauthz.Entry) *serviceauthz.Gate {
	t.Helper()
	policy, err := serviceauthz.NewPolicy(generation, entries)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := serviceauthz.NewGate(policy)
	if err != nil {
		t.Fatal(err)
	}
	return gate
}

func TestShardAuthorizationRejectsConfusedDeputyAndSeparatesRoles(t *testing.T) {
	gateway := authorizationNode(1)
	reader := authorizationNode(2)
	writer := authorizationNode(3)
	gate := authorizationGate(t, 7,
		serviceauthz.Entry{Node: gateway, Capabilities: serviceauthz.CapabilityDelegate},
		serviceauthz.Entry{Node: reader, Capabilities: serviceauthz.CapabilityDataRead},
		serviceauthz.Entry{Node: writer, Capabilities: serviceauthz.CapabilityDataWrite},
	)
	connection := &shardConn{peer: gateway, authorization: gate}
	read := &ShardRequest{Authority: serviceauthz.Authority{Node: reader, Generation: 7},
		ExecutionMode: ExecutionReadOnly}
	if !connection.authorize(read) {
		t.Fatal("reader denied read")
	}
	read.ExecutionMode = ExecutionReadWrite
	if connection.authorize(read) {
		t.Fatal("reader gained write through delegate")
	}
	read.Authority.Node = writer
	if !connection.authorize(read) {
		t.Fatal("writer denied write")
	}
	connection.peer = authorizationNode(9)
	if connection.authorize(read) {
		t.Fatal("untrusted gateway became a deputy")
	}
}

func TestReplicatedAuthorizationRotationAndRetryGeneration(t *testing.T) {
	gateway := authorizationNode(10)
	client := authorizationNode(11)
	gate := authorizationGate(t, 4,
		serviceauthz.Entry{Node: gateway, Capabilities: serviceauthz.CapabilityDelegate},
		serviceauthz.Entry{Node: client, Capabilities: serviceauthz.CapabilityDataRead},
	)
	server := &ReplicatedServer{authorization: gate}
	request := &ReplicatedRequest{Operation: ReplicatedReadLeader,
		Authority: serviceauthz.Authority{Node: client, Generation: 4}}
	if !server.authorizeReplicated(gateway, request) {
		t.Fatal("authorized read denied")
	}
	next, err := serviceauthz.NewPolicy(5, []serviceauthz.Entry{
		{Node: gateway, Capabilities: serviceauthz.CapabilityDelegate},
		{Node: client, Capabilities: serviceauthz.CapabilityDataWrite},
	})
	if err != nil || gate.Rotate(next) != nil {
		t.Fatal(err)
	}
	if server.authorizeReplicated(gateway, request) {
		t.Fatal("retry silently acquired a newer generation")
	}
	request.Authority.Generation = 5
	if server.authorizeReplicated(gateway, request) {
		t.Fatal("write-only principal gained read")
	}
}

func TestShardAuthorizationHotCheckAllocationFree(t *testing.T) {
	gateway := authorizationNode(20)
	client := authorizationNode(21)
	gate := authorizationGate(t, 9,
		serviceauthz.Entry{Node: gateway, Capabilities: serviceauthz.CapabilityDelegate},
		serviceauthz.Entry{Node: client, Capabilities: serviceauthz.CapabilityDataRead},
	)
	connection := &shardConn{peer: gateway, authorization: gate}
	request := &ShardRequest{Authority: serviceauthz.Authority{Node: client, Generation: 9}}
	if allocations := testing.AllocsPerRun(1000, func() {
		if !connection.authorize(request) {
			panic("authorization changed")
		}
	}); allocations != 0 {
		t.Fatalf("authorization allocations=%v", allocations)
	}
}
