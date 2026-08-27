package raftservice_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

const multiGroupRF3RequestLedgerRangeDomain = "vibedb/test/multiraft-request-ledger-range/format-0\x00"

const multiGroupRF3DataRangeDomain = "vibedb/test/multiraft-data-range/format-0\x00"

func multiGroupRF3RequestLedgerRangeIdentity(group int) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(multiGroupRF3RequestLedgerRangeDomain))
	_, _ = hash.Write([]byte{byte(group)})
	var identity [sha256.Size]byte
	_ = hash.Sum(identity[:0])
	return identity
}

func multiGroupRF3RangeIdentity(group int) replication.Digest {
	if group == multiGroupRF3LedgerGroup {
		return replication.Digest(multiGroupRF3RequestLedgerRangeIdentity(group))
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte(multiGroupRF3DataRangeDomain))
	_, _ = hash.Write([]byte{byte(group)})
	var identity replication.Digest
	_ = hash.Sum(identity[:0])
	return identity
}

func multiGroupRF3RouteDigest(domain string, identity replication.Digest) replication.Digest {
	hash := sha256.New()
	_, _ = hash.Write([]byte(domain))
	_, _ = hash.Write(identity[:])
	var digest replication.Digest
	_ = hash.Sum(digest[:0])
	return digest
}

func multiGroupRF3RequestLedgerRoute(
	cluster *multiGroupTransactionRF3Cluster,
	group int,
) gateway.ReplicatedRoute {
	return cluster.route(group)
}

func (network *multiGroupRF3Network) nodeIsolated(node int) bool {
	network.mu.Lock()
	defer network.mu.Unlock()
	for peer := 0; peer < multiGroupRF3Voters; peer++ {
		if peer != node && network.blocked[node][peer] && network.blocked[peer][node] {
			return true
		}
	}
	return false
}

func (network *multiGroupRF3Network) heal(node int) {
	network.mu.Lock()
	defer network.mu.Unlock()
	for peer := 0; peer < multiGroupRF3Voters; peer++ {
		network.blocked[node][peer] = false
		network.blocked[peer][node] = false
	}
}

type multiGroupRequestLedgerRF3Trace struct {
	group     int
	member    int
	service   replication.ID128
	operation requestledger.Operation
	outer     []byte
	inner     []byte
	hidden    bool
}

type multiGroupRequestLedgerRF3RoundTripper struct {
	cluster *multiGroupTransactionRF3Cluster
	servers [multiGroupRF3Voters]*shardservice.ReplicatedServer

	mu            sync.Mutex
	hideOperation requestledger.Operation
	hidden        bool
	hiddenMember  int
	trace         []multiGroupRequestLedgerRF3Trace
}

func newMultiGroupRequestLedgerRF3RoundTripper(
	t testing.TB,
	cluster *multiGroupTransactionRF3Cluster,
) *multiGroupRequestLedgerRF3RoundTripper {
	t.Helper()
	client := &multiGroupRequestLedgerRF3RoundTripper{
		cluster: cluster, hiddenMember: -1,
	}
	for member := 0; member < multiGroupRF3Voters; member++ {
		server, err := shardservice.NewReplicatedServer(
			cluster.owners[member], 256<<20, 10*time.Second,
		)
		if err != nil {
			t.Fatal(err)
		}
		client.servers[member] = server
	}
	return client
}

func (client *multiGroupRequestLedgerRF3RoundTripper) armLostResponse(
	operation requestledger.Operation,
) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.hideOperation = operation
}

func (client *multiGroupRequestLedgerRF3RoundTripper) proposalTrace() []multiGroupRequestLedgerRF3Trace {
	client.mu.Lock()
	defer client.mu.Unlock()
	trace := make([]multiGroupRequestLedgerRF3Trace, len(client.trace))
	copy(trace, client.trace)
	for index := range trace {
		trace[index].outer = bytes.Clone(trace[index].outer)
		trace[index].inner = bytes.Clone(trace[index].inner)
	}
	return trace
}

func (client *multiGroupRequestLedgerRF3RoundTripper) DoReplicated(
	ctx context.Context,
	endpoint gateway.ReplicatedEndpoint,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, error) {
	member := int(endpoint.Member - 1)
	if member < 0 || member >= multiGroupRF3Voters {
		return nil, errors.New("RF3 request-ledger endpoint is outside the cluster")
	}
	group := -1
	for candidate := 0; candidate < client.cluster.groupCount; candidate++ {
		if request.Fence.Group == client.cluster.groups[candidate].key {
			group = candidate
			break
		}
	}
	if group < 0 {
		return nil, errors.New("RF3 request-ledger request names an unknown group")
	}
	if client.cluster.network.nodeIsolated(member) {
		return nil, errMultiGroupRF3Isolated
	}

	var trace multiGroupRequestLedgerRF3Trace
	ledgerProposal := false
	if request.Operation == shardservice.ReplicatedPropose {
		outer, err := replication.OpenCommand(request.Command)
		if err != nil {
			return nil, err
		}
		if outer.Kind() == replication.CommandRequestLedger {
			inner, openErr := outer.OpenRequestLedgerInto(nil)
			if openErr != nil {
				return nil, openErr
			}
			ledgerProposal = true
			trace = multiGroupRequestLedgerRF3Trace{
				group: group, member: member, service: outer.ClientID,
				operation: inner.Operation, outer: bytes.Clone(request.Command),
				inner: bytes.Clone(outer.RequestLedgerBytes()),
			}
		}
	}

	gatewayConn, serverConn := net.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- client.servers[member].ServeReplicatedConn(ctx, serverConn)
	}()
	response, err := shardservice.RoundTripReplicated(ctx, gatewayConn, request)
	_ = gatewayConn.Close()
	_ = serverConn.Close()
	var serverErr error
	select {
	case serverErr = <-done:
	case <-ctx.Done():
	}
	if err != nil {
		return nil, errors.Join(err, serverErr)
	}

	hide := false
	if ledgerProposal {
		client.mu.Lock()
		hide = !client.hidden && client.hideOperation != requestledger.OperationInvalid &&
			trace.operation == client.hideOperation && response != nil &&
			response.Kind == shardservice.ReplicatedCompletion
		if hide {
			client.hidden = true
			client.hiddenMember = member
			trace.hidden = true
		}
		client.trace = append(client.trace, trace)
		client.mu.Unlock()
	}
	if hide {
		client.cluster.network.isolate(member)
		return nil, io.ErrUnexpectedEOF
	}
	return response, nil
}

func multiGroupRF3RequestKey(sequence uint64, requestByte byte) requestledger.RequestKey {
	return requestledger.RequestKey{
		Scope:        requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest{0x31},
		Principal:    requestledger.PrincipalID{0x41},
		Request:      requestledger.RequestID{requestByte},
		IssuerEpoch:  7, IssuerSequence: sequence,
		IssuerLane: requestledger.IssuerLane{0x51},
	}
}

func multiGroupRF3RequestHead(
	t testing.TB,
	key requestledger.RequestKey,
) requestledger.HeadRecord {
	t.Helper()
	plan, err := requestledger.AppendPlan(nil, []byte("canonical multiraft request recipe"))
	if err != nil {
		t.Fatal(err)
	}
	head, err := requestledger.NewHeadWithContract(
		key, requestledger.Digest{0x61, byte(key.IssuerSequence)},
		requestledger.Digest{0x71, byte(key.IssuerSequence)}, plan,
	)
	if err != nil {
		t.Fatal(err)
	}
	return head
}

func newMultiGroupRF3DurableLedger(
	t testing.TB,
	client gateway.ReplicatedRoundTripper,
	authority serviceauthz.Authority,
) *gateway.DurableRequestLedgerRF3 {
	t.Helper()
	executor, err := gateway.NewReplicatedExecutor(client, 8, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rf3, err := gateway.NewReplicatedRequestLedgerRF3(
		gateway.ReplicatedRequestLedgerRF3Options{
			Executor: executor, Service: authority,
			ServiceTenant: []byte("internal-request-ledger"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := gateway.NewDurableRequestLedgerRF3(rf3)
	if err != nil {
		t.Fatal(err)
	}
	return ledger
}

type multiGroupRF3DurableGateway struct {
	sql      *gateway.DurableSQLRequestExecutor
	ledger   *gateway.DurableRequestLedgerRF3
	topology *gateway.DurableRequestLedgerTopologyHolder
	client   *multiGroupRequestLedgerRF3RoundTripper
}

func newMultiGroupRF3DurableGateway(
	t testing.TB,
	cluster *multiGroupTransactionRF3Cluster,
	snapshot *gateway.Snapshot,
	ackKey gateway.DurableRequestAckDerivationKey,
	principal serviceauthz.Authority,
) multiGroupRF3DurableGateway {
	t.Helper()
	client := newMultiGroupRequestLedgerRF3RoundTripper(t, cluster)
	native, err := gateway.NewReplicatedExecutorWithOptions(
		client, gateway.ReplicatedExecutorOptions{
			MaxAttempts: 1, AttemptTimeout: 10 * time.Second, LeaderHintCapacity: 16,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	rf3, err := gateway.NewReplicatedRequestLedgerRF3(
		gateway.ReplicatedRequestLedgerRF3Options{
			Executor: native, Service: principal,
			ServiceTenant: []byte("internal-request-ledger"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := gateway.NewDurableRequestLedgerRF3(rf3)
	if err != nil {
		t.Fatal(err)
	}
	catalog := gateway.NewCatalogHolder(snapshot)
	topology, err := gateway.NewCatalogDurableRequestLedgerTopologyHolder(catalog)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := gateway.NewCatalogDurableRequestRouteResolver(catalog)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := gateway.NewJournaledDurableRequestExecutionPinSessionFactory(
		native, t.TempDir(), principal,
	)
	if err != nil {
		t.Fatal(err)
	}
	pins, err := gateway.NewNativeDurableRequestExecutionPinAuthority(native, sessions, 1)
	if err != nil {
		t.Fatal(err)
	}
	waves, err := gateway.NewDurableRequestLifecycleRunner(ledger, resolver, native)
	if err != nil {
		t.Fatal(err)
	}
	payloads, err := gateway.NewDurableRequestDynamicPayloadStore(ledger)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := gateway.NewDurableRequestTerminalCoordinatorWithSessionFactory(
		ledger, native, sessions,
	)
	if err != nil {
		t.Fatal(err)
	}
	terminalAuthority, err := gateway.NewNativeDurableRequestTerminalAuthorityProvider(
		ackKey, principal,
	)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := gateway.NewDurableRequestDistributedRunner(
		ledger, resolver, waves, payloads, terminal, terminalAuthority,
	)
	if err != nil {
		t.Fatal(err)
	}
	requests, err := gateway.NewDurableRequestService(topology, ledger, runner, pins)
	if err != nil {
		t.Fatal(err)
	}
	planner := gateway.NewExecutor(nil, catalog, gateway.Options{})
	sql, err := gateway.NewDurableSQLRequestExecutor(gateway.DurableSQLRequestExecutorOptions{
		Planner: planner, ReplicatedData: native, Requests: requests,
		RecoveryPulseLimit: 4, PlanningLeaseSpan: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	return multiGroupRF3DurableGateway{
		sql: sql, ledger: ledger, topology: topology, client: client,
	}
}

func multiGroupRF3DurableCatalog(
	t testing.TB,
	cluster *multiGroupTransactionRF3Cluster,
) rf3testfixture.DurableCatalog {
	t.Helper()
	groups := make([]rf3testfixture.DurableCatalogGroup, 0, multiGroupRF3MaxGroups)
	for group := 0; group < multiGroupRF3Groups; group++ {
		groups = append(groups, rf3testfixture.DurableCatalogGroup{
			Route: cluster.route(group), Table: "orders_" + string(rune('a'+group)),
			PrimaryKey: "/id", Relation: 1,
			MaxKeyBytes:      replication.MaxMutationKeyBytes,
			MaxDocumentBytes: replication.MaxMutationValueBytes,
		})
	}
	ledgerRoute := cluster.route(multiGroupRF3LedgerGroup)
	groups = append(groups, rf3testfixture.DurableCatalogGroup{
		Route: ledgerRoute, Table: "request_ledger_home", PrimaryKey: "/home",
		LedgerRanges: []gateway.DurableRequestLedgerRangeDescriptor{{
			Identity: ledgerRoute.RangeIdentity,
		}},
	})
	fixture, err := rf3testfixture.NewDurableCatalog(rf3testfixture.DurableCatalogOptions{
		Generation: 31, Groups: groups,
		AckKey: gateway.DurableRequestAckDerivationKey{0x91},
	})
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

// TestTwoGatewayRequestLedgerRF3RecoversUnknownCreateAcrossLeaderPartition
// is the real Owner/MultiRaft/WAL/apply boundary. Gateway A loses a committed
// response as its serving member is partitioned; independently constructed
// gateway B recovers through a new leader using the same canonical inner CAS
// and a fresh gateway-service outer identity. Native TLS authorization and the
// full SQL runner are separate boundaries and are not claimed by this test.
func TestTwoGatewayRequestLedgerRF3RecoversUnknownCreateAcrossLeaderPartition(t *testing.T) {
	cluster := newMultiGroupRF3Cluster(t, multiGroupRF3MaxGroups)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	group := multiGroupRF3LedgerGroup
	if err := cluster.owners[0].Campaign(ctx, cluster.groups[group].key); err != nil {
		t.Fatal(err)
	}
	leader := waitRF3Leader(t, ctx, cluster.owners[:], nil, cluster.groups[group].key)
	route := multiGroupRF3RequestLedgerRoute(cluster, group)
	holder, err := gateway.NewDurableRequestLedgerTopologyHolder(
		gateway.DurableRequestLedgerTopology{Generation: 1,
			Ranges: []gateway.DurableRequestLedgerRange{{
				Identity: route.RangeIdentity, Route: route,
			}}},
	)
	if err != nil {
		t.Fatal(err)
	}
	key1 := multiGroupRF3RequestKey(1, 0x81)
	point, err := requestledger.Home(key1)
	if err != nil {
		t.Fatal(err)
	}
	home, _, ok := holder.Lookup(point)
	if !ok {
		t.Fatal("request-ledger topology did not cover the request home")
	}

	clientA := newMultiGroupRequestLedgerRF3RoundTripper(t, cluster)
	clientB := newMultiGroupRequestLedgerRF3RoundTripper(t, cluster)
	ledgerA := newMultiGroupRF3DurableLedger(t, clientA,
		serviceauthz.Authority{Node: [16]byte{0xa1}, Generation: 1})
	ledgerB := newMultiGroupRF3DurableLedger(t, clientB,
		serviceauthz.Authority{Node: [16]byte{0xb1}, Generation: 1})

	highwater, err := requestledger.NewIssuerHighwater(key1)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := ledgerA.ApplyCAS(ctx, home, key1, gateway.DurableRequestLifecycleCAS{
		Operation: requestledger.OperationOpenIssuerLane, Revision: 1,
		IssuerOpen: highwater,
	})
	if err != nil || opened.Ledger.ResultCode != replicatedstate.ResultApplied ||
		opened.Ledger.ExactDuplicate {
		t.Fatalf("issuer open=%+v err=%v", opened, err)
	}
	waitRF3Applied(t, ctx, cluster.owners[:], nil, cluster.groups[group].key, opened.Applied)

	key2 := multiGroupRF3RequestKey(2, 0x82)
	head2 := multiGroupRF3RequestHead(t, key2)
	gap, err := ledgerB.ApplyCAS(ctx, home, key2, gateway.DurableRequestLifecycleCAS{
		Operation: requestledger.OperationCreate, Revision: head2.Revision, Head: head2,
	})
	if err != nil || gap.Ledger.ResultCode != replicatedstate.ResultRequestLedgerConflict ||
		gap.Ledger.ExactDuplicate {
		t.Fatalf("issuer gap=%+v err=%v", gap, err)
	}

	head1 := multiGroupRF3RequestHead(t, key1)
	clientA.armLostResponse(requestledger.OperationCreate)
	_, err = ledgerA.ApplyCAS(ctx, home, key1, gateway.DurableRequestLifecycleCAS{
		Operation: requestledger.OperationCreate, Revision: head1.Revision, Head: head1,
	})
	if !errors.Is(err, raftservice.ErrOutcomeUnknown) {
		t.Fatalf("lost create error=%v", err)
	}
	if !cluster.network.nodeIsolated(leader) {
		t.Fatalf("committing leader %d was not partitioned", leader)
	}
	removed := map[int]bool{leader: true}
	candidate := (leader + 1) % multiGroupRF3Voters
	if err := cluster.owners[candidate].Campaign(ctx, cluster.groups[group].key); err != nil {
		t.Fatal(err)
	}
	newLeader := waitRF3Leader(t, ctx, cluster.owners[:], removed, cluster.groups[group].key)
	if newLeader == leader {
		t.Fatal("request-ledger failover retained the partitioned leader")
	}

	recovered, err := ledgerB.ApplyCAS(ctx, home, key1, gateway.DurableRequestLifecycleCAS{
		Operation: requestledger.OperationCreate, Revision: head1.Revision, Head: head1,
	})
	if err != nil || recovered.Ledger.ResultCode != replicatedstate.ResultApplied ||
		!recovered.Ledger.ExactDuplicate || recovered.Ledger.PlanningLeaseExpiryIndex == 0 {
		t.Fatalf("replacement create=%+v err=%v", recovered, err)
	}
	row, err := ledgerB.ReadRow(ctx, home, gateway.DurableRequestLifecycleRead{
		Key: key1, Kind: replicatedstate.RequestLedgerReadHead,
		MinimumApplied: recovered.Applied,
	})
	if err != nil || !row.Found || row.Kind != replicatedstate.RequestLedgerReadHead ||
		row.Head.Key != key1 || row.Head.KeyDigest != head1.KeyDigest ||
		row.Head.PlanningLeaseExpiryIndex != recovered.Ledger.PlanningLeaseExpiryIndex {
		t.Fatalf("replacement ReadIndex head=%+v err=%v", row, err)
	}

	accepted, err := ledgerB.ApplyCAS(ctx, home, key2, gateway.DurableRequestLifecycleCAS{
		Operation: requestledger.OperationCreate, Revision: head2.Revision, Head: head2,
	})
	if err != nil || accepted.Ledger.ResultCode != replicatedstate.ResultApplied ||
		accepted.Ledger.ExactDuplicate {
		t.Fatalf("contiguous issuer sequence=%+v err=%v", accepted, err)
	}

	traceA := clientA.proposalTrace()
	traceB := clientB.proposalTrace()
	var hidden, retry *multiGroupRequestLedgerRF3Trace
	for index := range traceA {
		if traceA[index].hidden {
			hidden = &traceA[index]
		}
	}
	if hidden != nil {
		for index := range traceB {
			if traceB[index].operation == requestledger.OperationCreate &&
				bytes.Equal(traceB[index].inner, hidden.inner) {
				retry = &traceB[index]
				break
			}
		}
	}
	if hidden == nil || retry == nil {
		t.Fatalf("missing hidden/retry trace: A=%+v B=%+v", traceA, traceB)
	}
	if bytes.Equal(hidden.outer, retry.outer) || hidden.service == retry.service ||
		!bytes.Equal(hidden.inner, retry.inner) || hidden.member == retry.member {
		t.Fatalf("gateway identity or exact inner retry drifted: hidden=%+v retry=%+v",
			*hidden, *retry)
	}
}

// TestTwoGatewayDurableSQLRF3RecoversTerminalAndAckAcrossLeaderPartitions is
// the full in-process production composition: two user-data RF3 groups, one
// dedicated request-ledger RF3 group, the shipped SQL planner, execution-pin
// journals, distributed transaction runner, terminal coordinator, and ACK
// collector. It uses real Owner/MultiRaft/WAL/apply paths. The net.Pipe native
// edge is intentionally unauthenticated; native mTLS remains a separate wire
// gate and this test does not claim a child-process deployment boundary.
func TestTwoGatewayDurableSQLRF3RecoversTerminalAndAckAcrossLeaderPartitions(t *testing.T) {
	cluster := newMultiGroupRF3Cluster(t, multiGroupRF3MaxGroups)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	for group := 0; group < cluster.groupCount; group++ {
		if err := cluster.owners[0].Campaign(ctx, cluster.groups[group].key); err != nil {
			t.Fatalf("campaign group %d: %v", group, err)
		}
		if leader := waitRF3Leader(t, ctx, cluster.owners[:], nil, cluster.groups[group].key); leader != 0 {
			t.Fatalf("group %d leader=%d, want deliberately campaigned member 0", group, leader)
		}
	}
	fixture := multiGroupRF3DurableCatalog(t, cluster)
	principalA := serviceauthz.Authority{Node: [16]byte{0xa2}, Generation: 1}
	principalB := serviceauthz.Authority{Node: [16]byte{0xb2}, Generation: 1}
	gatewayA := newMultiGroupRF3DurableGateway(
		t, cluster, fixture.Snapshot, fixture.AckKey, principalA,
	)
	gatewayB := newMultiGroupRF3DurableGateway(
		t, cluster, fixture.Snapshot, fixture.AckKey, principalB,
	)

	tenant := []byte("tenant")
	requestKey := requestledger.RequestKey{
		Scope:        requestledger.ScopeAuthenticated,
		TenantDigest: requestledger.Digest(sha256.Sum256(tenant)),
		Principal:    requestledger.PrincipalID{0xc1}, Request: requestledger.RequestID{0xd1},
		IssuerEpoch: 11, IssuerSequence: 1, IssuerLane: requestledger.IssuerLane{0xe1},
	}
	point, err := requestledger.Home(requestKey)
	if err != nil {
		t.Fatal(err)
	}
	home, _, ok := gatewayA.topology.Lookup(point)
	if !ok || home.ReplicatedRoute().Group != cluster.groups[multiGroupRF3LedgerGroup].key {
		t.Fatalf("durable catalog routed request home to %+v", home)
	}
	highwater, err := requestledger.NewIssuerHighwater(requestKey)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := gatewayA.ledger.ApplyCAS(ctx, home, requestKey, gateway.DurableRequestLifecycleCAS{
		Operation: requestledger.OperationOpenIssuerLane, Revision: 1, IssuerOpen: highwater,
	})
	if err != nil || opened.Ledger.ResultCode != replicatedstate.ResultApplied {
		t.Fatalf("open durable issuer=%+v err=%v", opened, err)
	}
	waitRF3Applied(t, ctx, cluster.owners[:], nil,
		cluster.groups[multiGroupRF3LedgerGroup].key, opened.Applied)

	queries := []gateway.Query{
		{SQL: `INSERT INTO orders_a VALUES (?)`, Class: gateway.ClassInteractive,
			Params: []shardservice.Param{shardservice.DocumentParam(`{"id":"durable-a","group":0}`)}},
		{SQL: `INSERT INTO orders_b VALUES (?)`, Class: gateway.ClassInteractive,
			Params: []shardservice.Param{shardservice.DocumentParam(`{"id":"durable-b","group":1}`)}},
	}
	// The terminal result is deliberately retained only as the test oracle: the
	// simulated caller receives no response and reconnects through gateway B.
	lost, err := gatewayA.sql.Execute(ctx, requestKey, tenant, queries)
	if err != nil || lost.Result == nil || lost.Result.RowsAffected != multiGroupRF3Groups ||
		lost.Result.ShardsFanned != multiGroupRF3Groups || lost.TerminalRevision == 0 ||
		lost.ResultDigest == (replication.Digest{}) || lost.AckToken == (gateway.DurableRequestAckToken{}) {
		t.Fatalf("gateway A durable terminal=%+v err=%v", lost, err)
	}

	terminalLeader := waitRF3Leader(t, ctx, cluster.owners[:], nil,
		cluster.groups[multiGroupRF3LedgerGroup].key)
	cluster.network.isolate(terminalLeader)
	removedTerminal := map[int]bool{terminalLeader: true}
	terminalCandidate := (terminalLeader + 1) % multiGroupRF3Voters
	if err := cluster.owners[terminalCandidate].Campaign(
		ctx, cluster.groups[multiGroupRF3LedgerGroup].key,
	); err != nil {
		t.Fatal(err)
	}
	terminalReplacement := waitRF3Leader(t, ctx, cluster.owners[:], removedTerminal,
		cluster.groups[multiGroupRF3LedgerGroup].key)
	if terminalReplacement == terminalLeader {
		t.Fatal("terminal recovery retained the partitioned ledger leader")
	}
	recovered, found, err := gatewayB.sql.Replay(ctx, lost.Key)
	if err != nil || !found || recovered.Result == nil ||
		recovered.Key != lost.Key || recovered.TerminalRevision != lost.TerminalRevision ||
		recovered.ResultDigest != lost.ResultDigest || recovered.AckToken != lost.AckToken ||
		recovered.Result.TransactionID != lost.Result.TransactionID ||
		recovered.Result.RowsAffected != lost.Result.RowsAffected {
		t.Fatalf("gateway B terminal recovery=%+v found=%v err=%v", recovered, found, err)
	}

	wrong := recovered.AckToken
	wrong[0] ^= 0xff
	if _, err = gatewayB.sql.Acknowledge(ctx, recovered.Key, recovered.TerminalRevision,
		recovered.ResultDigest, wrong,
	); !errors.Is(err, gateway.ErrDurableRequestConflict) {
		t.Fatalf("wrong ACK capability error=%v", err)
	}

	// Restore the first voter before taking the replacement leader away, so the
	// ACK fault is another genuine two-of-three failover rather than a quorum-loss
	// artifact. Waiting on one current applied cut proves the healed voter caught up.
	cluster.network.heal(terminalLeader)
	terminalState := mustRF3State(t, ctx, cluster.owners[terminalReplacement],
		cluster.groups[multiGroupRF3LedgerGroup].key)
	waitRF3Applied(t, ctx, cluster.owners[:], nil,
		cluster.groups[multiGroupRF3LedgerGroup].key, terminalState.Status.Applied)
	ackLeader := waitRF3Leader(t, ctx, cluster.owners[:], nil,
		cluster.groups[multiGroupRF3LedgerGroup].key)
	gatewayB.client.armLostResponse(requestledger.OperationAck)
	_, ackErr := gatewayB.sql.Acknowledge(ctx, recovered.Key, recovered.TerminalRevision,
		recovered.ResultDigest, recovered.AckToken)
	if !errors.Is(ackErr, raftservice.ErrOutcomeUnknown) {
		t.Fatalf("lost committed ACK error=%v", ackErr)
	}
	if !cluster.network.nodeIsolated(ackLeader) {
		t.Fatalf("ACK leader %d was not partitioned", ackLeader)
	}
	removedAck := map[int]bool{ackLeader: true}
	ackCandidate := (ackLeader + 1) % multiGroupRF3Voters
	if ackCandidate == ackLeader {
		ackCandidate = (ackCandidate + 1) % multiGroupRF3Voters
	}
	if err := cluster.owners[ackCandidate].Campaign(
		ctx, cluster.groups[multiGroupRF3LedgerGroup].key,
	); err != nil {
		t.Fatal(err)
	}
	ackReplacement := waitRF3Leader(t, ctx, cluster.owners[:], removedAck,
		cluster.groups[multiGroupRF3LedgerGroup].key)
	if ackReplacement == ackLeader {
		t.Fatal("ACK recovery retained the partitioned ledger leader")
	}

	acknowledged, err := gatewayB.sql.Acknowledge(ctx, recovered.Key,
		recovered.TerminalRevision, recovered.ResultDigest, recovered.AckToken)
	if err != nil || acknowledged.Ack.GCPhase != requestledger.AckGCComplete ||
		acknowledged.Ack.TerminalRevision != recovered.TerminalRevision ||
		acknowledged.Ack.ResultDigest != requestledger.Digest(recovered.ResultDigest) {
		t.Fatalf("recovered ACK=%+v err=%v", acknowledged, err)
	}
	proposals := len(gatewayB.client.proposalTrace())
	idempotent, err := gatewayB.sql.Acknowledge(ctx, recovered.Key,
		recovered.TerminalRevision, recovered.ResultDigest, recovered.AckToken)
	if err != nil || idempotent.Ack.AckDigest != acknowledged.Ack.AckDigest ||
		len(gatewayB.client.proposalTrace()) != proposals {
		t.Fatalf("idempotent ACK=%+v proposals=%d/%d err=%v",
			idempotent, proposals, len(gatewayB.client.proposalTrace()), err)
	}

	for group, identifier := range []string{"durable-a", "durable-b"} {
		key, keyOK := orderedkey.AppendString(nil, []byte(identifier), orderedkey.Ascending)
		if !keyOK {
			t.Fatalf("encode durable key %q", identifier)
		}
		for member := 0; member < multiGroupRF3Voters; member++ {
			if cluster.network.nodeIsolated(member) {
				continue
			}
			state := mustRF3State(t, ctx, cluster.owners[member], cluster.groups[group].key)
			row, lease, readErr := cluster.owners[member].ReadPoint(ctx, PointReadRequest{
				Fence: state.Fence(), Relation: 1, Key: key, MinimumApplied: 1,
				MaxValueBytes: replication.MaxMutationValueBytes,
			})
			if lease != nil {
				lease.Release()
			}
			if readErr != nil || !row.Found {
				t.Fatalf("group %d member %d durable row found=%v err=%v",
					group, member, row.Found, readErr)
			}
		}
	}
}
