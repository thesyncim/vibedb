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
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

const multiGroupRF3RequestLedgerRangeDomain = "vibedb/test/multiraft-request-ledger-range/format-0\x00"

func multiGroupRF3RequestLedgerRangeIdentity(group int) [sha256.Size]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte(multiGroupRF3RequestLedgerRangeDomain))
	_, _ = hash.Write([]byte{byte(group)})
	var identity [sha256.Size]byte
	_ = hash.Sum(identity[:0])
	return identity
}

func multiGroupRF3RequestLedgerRoute(
	cluster *multiGroupTransactionRF3Cluster,
	group int,
) gateway.ReplicatedRoute {
	route := cluster.route(group)
	route.RangeIdentity = replication.Digest(multiGroupRF3RequestLedgerRangeIdentity(group))
	route.LineageDigest = sha256.Sum256(append(
		[]byte("vibedb/test/multiraft-request-ledger-lineage/format-0\x00"),
		route.RangeIdentity[:]...,
	))
	route.ForwardingRuleDigest = sha256.Sum256(append(
		[]byte("vibedb/test/multiraft-request-ledger-forwarding/format-0\x00"),
		route.RangeIdentity[:]...,
	))
	return route
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
	for candidate := 0; candidate < multiGroupRF3Groups; candidate++ {
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
	if request.Operation == shardservice.ReplicatedPropose {
		outer, err := replication.OpenCommand(request.Command)
		if err != nil || outer.Kind() != replication.CommandRequestLedger {
			return nil, errors.Join(err, errors.New("RF3 request-ledger proposal is not canonical"))
		}
		inner, err := outer.OpenRequestLedgerInto(nil)
		if err != nil {
			return nil, err
		}
		trace = multiGroupRequestLedgerRF3Trace{
			group: group, member: member, service: outer.ClientID,
			operation: inner.Operation, outer: bytes.Clone(request.Command),
			inner: bytes.Clone(outer.RequestLedgerBytes()),
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
	if request.Operation == shardservice.ReplicatedPropose {
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

// TestTwoGatewayRequestLedgerRF3RecoversUnknownCreateAcrossLeaderPartition
// is the real Owner/MultiRaft/WAL/apply boundary. Gateway A loses a committed
// response as its serving member is partitioned; independently constructed
// gateway B recovers through a new leader using the same canonical inner CAS
// and a fresh gateway-service outer identity. Native TLS authorization and the
// full SQL runner are separate boundaries and are not claimed by this test.
func TestTwoGatewayRequestLedgerRF3RecoversUnknownCreateAcrossLeaderPartition(t *testing.T) {
	cluster := newMultiGroupTransactionRF3Cluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	group := 0
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
