package shardservice

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/exchange"
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
		SQL: "SELECT * FROM docs", ExecutionMode: ExecutionReadOnly}
	if !connection.authorize(read) {
		t.Fatal("reader denied read")
	}
	read.Transaction.Operation = TransactionAcquireReadFence
	if connection.authorize(read) {
		t.Fatal("reader gained write through delegate")
	}
	read.Authority.Node = writer
	if !connection.authorize(read) {
		t.Fatal("writer denied write")
	}
	read.Transaction.Operation = TransactionNone
	read.SQL = "SELECT * FROM docs"
	read.ExecutionMode = ExecutionReadWrite
	if connection.authorize(read) {
		t.Fatal("write-only principal gained SQL read through write execution mode")
	}
	read.Authority.Node = reader
	if !connection.authorize(read) {
		t.Fatal("reader denied SQL read on write execution lane")
	}
	read.SQL = `UPDATE docs SET "$doc" = ? WHERE id = ?`
	read.ExecutionMode = ExecutionReadOnly
	if connection.authorize(read) {
		t.Fatal("reader gained SQL write through read-only execution mode")
	}
	read.Authority.Node = writer
	if !connection.authorize(read) {
		t.Fatal("writer denied SQL mutation")
	}
	connection.peer = authorizationNode(9)
	if connection.authorize(read) {
		t.Fatal("untrusted gateway became a deputy")
	}
}

func TestReplicatedAuthorizationRotationAndRetryGeneration(t *testing.T) {
	gateway := authorizationNode(10)
	client := authorizationNode(11)
	writer := authorizationNode(12)
	delegateOnly := authorizationNode(13)
	operator := authorizationNode(14)
	topology := authorizationNode(15)
	gate := authorizationGate(t, 4,
		serviceauthz.Entry{Node: gateway, Capabilities: serviceauthz.CapabilityDelegate},
		serviceauthz.Entry{Node: client, Capabilities: serviceauthz.CapabilityDataRead},
		serviceauthz.Entry{Node: writer, Capabilities: serviceauthz.CapabilityDataWrite},
		serviceauthz.Entry{Node: delegateOnly, Capabilities: serviceauthz.CapabilityDelegate},
		serviceauthz.Entry{Node: operator, Capabilities: serviceauthz.CapabilityMembership},
		serviceauthz.Entry{Node: topology, Capabilities: serviceauthz.CapabilityTopology},
	)
	server := &ReplicatedServer{authorization: gate}
	request := &ReplicatedRequest{Operation: ReplicatedReadLeader,
		Authority:  serviceauthz.Authority{Node: client, Generation: 4},
		Capability: serviceauthz.CapabilityDataRead}
	if !server.authorizeReplicated(gateway, request) {
		t.Fatal("authorized read denied")
	}
	request.Capability = 0
	if server.authorizeReplicated(gateway, request) {
		t.Fatal("authenticated request selected development capability zero")
	}
	request.Capability = serviceauthz.CapabilityDataRead
	request.Operation = ReplicatedProbe
	request.Authority.Node = writer
	request.Capability = serviceauthz.CapabilityDataWrite
	if !server.authorizeReplicated(gateway, request) {
		t.Fatal("write principal denied explicit routing probe")
	}
	request.Authority.Node = delegateOnly
	if server.authorizeReplicated(gateway, request) {
		t.Fatal("delegate capability alone authorized a routing probe")
	}
	request.Authority.Node = operator
	request.Capability = serviceauthz.CapabilityMembership
	request.Operation = ReplicatedProbe
	if !server.authorizeReplicated(gateway, request) {
		t.Fatal("membership operator denied mandatory leader probe")
	}
	request.Operation = ReplicatedMembership
	if !server.authorizeReplicated(gateway, request) {
		t.Fatal("membership operator denied sealed membership request")
	}
	request.Authority.Node = writer
	if server.authorizeReplicated(gateway, request) {
		t.Fatal("data writer gained membership authority")
	}
	request.Authority.Node = delegateOnly
	if server.authorizeReplicated(gateway, request) {
		t.Fatal("delegate-only principal gained membership authority")
	}
	request.Operation = ReplicatedReadLeader
	request.Authority.Node = topology
	request.Capability = serviceauthz.CapabilityTopology
	if !server.authorizeReplicated(gateway, request) {
		t.Fatal("topology principal denied catalog read")
	}
	request.Operation = ReplicatedPropose
	if !server.authorizeReplicated(gateway, request) {
		t.Fatal("topology principal denied catalog proposal")
	}
	request.Authority.Node = writer
	if server.authorizeReplicated(gateway, request) {
		t.Fatal("ordinary data writer gained topology proposal authority")
	}
	request.Authority.Node = client
	request.Operation = ReplicatedReadLeader
	request.Capability = serviceauthz.CapabilityDataRead
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

func TestReplicatedRequestLedgerRequiresServiceControlAndDataWriterSubject(t *testing.T) {
	firstGateway := authorizationNode(31)
	replacementGateway := authorizationNode(32)
	delegateOnly := authorizationNode(33)
	client := authorizationNode(34)
	gate := authorizationGate(t, 6,
		serviceauthz.Entry{Node: firstGateway, Capabilities: serviceauthz.CapabilityDelegate | serviceauthz.CapabilityRequestLedger},
		serviceauthz.Entry{Node: replacementGateway, Capabilities: serviceauthz.CapabilityDelegate | serviceauthz.CapabilityRequestLedger},
		serviceauthz.Entry{Node: delegateOnly, Capabilities: serviceauthz.CapabilityDelegate},
		serviceauthz.Entry{Node: client, Capabilities: serviceauthz.CapabilityDataWrite},
	)
	server := &ReplicatedServer{authorization: gate}
	request := &ReplicatedRequest{
		Operation: ReplicatedPropose,
		Authority: serviceauthz.Authority{Node: firstGateway, Generation: 6},
		Capability: serviceauthz.CapabilityRequestLedger,
	}
	if !server.authorizeReplicated(firstGateway, request) {
		t.Fatal("delegated ledger service denied its control authority")
	}
	request.Authority.Node = replacementGateway
	if !server.authorizeReplicated(replacementGateway, request) {
		t.Fatal("replacement gateway could not resume the immutable inner subject")
	}
	request.Authority.Node = firstGateway
	if server.authorizeReplicated(delegateOnly, request) {
		t.Fatal("delegate-only service gained request-ledger control")
	}
	if server.authorizeReplicated(client, request) {
		t.Fatal("data writer sent raw request-ledger control directly")
	}
}

func TestSealedRequestCapabilityIgnoresCallerExecutionMode(t *testing.T) {
	checks := []struct {
		request ShardRequest
		want    serviceauthz.Capability
	}{
		{ShardRequest{SQL: "SELECT * FROM docs", ExecutionMode: ExecutionReadWrite}, serviceauthz.CapabilityDataRead},
		{ShardRequest{SQL: `UPDATE docs SET "$doc" = ? WHERE id = ?`, ExecutionMode: ExecutionReadOnly}, serviceauthz.CapabilityDataWrite},
		{ShardRequest{SQL: "CREATE TABLE docs (id TEXT)", ExecutionMode: ExecutionReadOnly}, serviceauthz.CapabilitySchema},
		{ShardRequest{SQL: "DELETE FROM docs", MutationCapture: true}, serviceauthz.CapabilityDataRead | serviceauthz.CapabilityDataWrite},
		{ShardRequest{Transaction: TransactionRequest{Operation: TransactionLookupCoordinator}, ExecutionMode: ExecutionReadWrite}, serviceauthz.CapabilityDataRead},
		{ShardRequest{Transaction: TransactionRequest{Operation: TransactionStageCoordinator}, ExecutionMode: ExecutionReadOnly}, serviceauthz.CapabilityDataWrite},
		{ShardRequest{Exchange: ExchangeRequest{Operation: ExchangePush}, ExecutionMode: ExecutionReadWrite}, serviceauthz.CapabilityDataRead},
		{ShardRequest{SQL: "DELETE FROM docs", Repartition: RepartitionRequest{Operation: exchange.ID{1}}}, serviceauthz.CapabilityDataRead | serviceauthz.CapabilityDataWrite},
	}
	for _, check := range checks {
		got, ok := sealedRequestCapability(&check.request)
		if !ok || got != check.want {
			t.Fatalf("request=%+v capability=%x present=%t want=%x", check.request, got, ok, check.want)
		}
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		got, ok := sealedRequestCapability(&checks[1].request)
		if !ok || got != serviceauthz.CapabilityDataWrite {
			panic("classification changed")
		}
	}); !raceDetectorEnabled && allocations != 0 {
		t.Fatalf("sealed request classification allocations=%v", allocations)
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
	request := &ShardRequest{Authority: serviceauthz.Authority{Node: client, Generation: 9}, SQL: "SELECT 1"}
	if allocations := testing.AllocsPerRun(1000, func() {
		if !connection.authorize(request) {
			panic("authorization changed")
		}
	}); !raceDetectorEnabled && allocations != 0 {
		t.Fatalf("authorization allocations=%v", allocations)
	}
}
