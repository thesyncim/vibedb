package shardservice

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedagg"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/exchange"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func TestRequestFrameBytesMatchesEveryOptionalRequestEnvelope(t *testing.T) {
	base := func() ShardRequest {
		return ShardRequest{
			SQL: "SELECT ?", Distribution: "data", Shard: "all",
			AllocationGeneration: 1, RoutingVersion: 1, OwnershipEpoch: 1,
			Params: []Param{StringParam("value")}, MaxRows: 16, MaxResultBytes: 4096,
		}
	}
	key := exchange.Key{Operation: exchange.ID{1}, Stage: 1, Partition: 1, Attempt: 1}
	tests := []struct {
		name   string
		mutate func(*ShardRequest)
	}{
		{name: "authority", mutate: func(request *ShardRequest) {
			request.Authority = serviceauthz.Authority{Node: rafttransport.NodeID{1}, Generation: 1}
		}},
		{name: "position", mutate: func(request *ShardRequest) {
			request.HasMinPosition = true
			request.MinPosition = Position{Distribution: "data", Shard: "all", LogID: [16]byte{1}, Index: 1}
		}},
		{name: "scopes", mutate: func(request *ShardRequest) {
			request.BucketBits = 8
			request.AccessScopes = []distributedtxn.IntentScope{{Start: 1, End: 3}}
		}},
		{name: "read fence", mutate: func(request *ShardRequest) {
			request.ReadFenceID = distributedtxn.ID{1}
		}},
		{name: "global lookup", mutate: func(request *ShardRequest) {
			request.SQL = ""
			request.Params = nil
			request.GlobalIndexLookup = GlobalIndexLookupRequest{
				Relation: []byte("idx"), IndexID: 1, Incarnation: 1,
				KeyTuples: [][]byte{{1, 2}}, LocatorCount: 1, Unique: true,
			}
		}},
		{name: "primary keys", mutate: func(request *ShardRequest) {
			request.PrimaryKeyRead = PrimaryKeyReadRequest{
				PrimaryPath: []byte("id"), Keys: [][]byte{{1}, {2}},
			}
		}},
		{name: "mutation capture", mutate: func(request *ShardRequest) {
			request.MutationCapture = true
		}},
		{name: "mutation image capture", mutate: func(request *ShardRequest) {
			request.MutationImageCapture = true
		}},
		{name: "document scan", mutate: func(request *ShardRequest) {
			request.SQL = ""
			request.Params = nil
			request.DocumentScan = DocumentScanRequest{Relation: []byte("docs"), After: []byte{1}}
		}},
		{name: "partial aggregate", mutate: func(request *ShardRequest) {
			request.PartialAggregate = true
		}},
		{name: "row batch", mutate: func(request *ShardRequest) {
			request.RowBatch = RowBatchRequest{BatchRows: 2, BatchBytes: 128}
		}},
		{name: "repartition", mutate: func(request *ShardRequest) {
			request.Repartition = RepartitionRequest{
				Operation: exchange.ID{2}, Stage: 1, Attempt: 1,
				KeyColumns: []uint16{0}, Targets: []RepartitionTarget{{
					Address: []byte("127.0.0.1:1"), Distribution: "data", Shard: "all",
					AllocationGeneration: 1, RoutingVersion: 1, OwnershipEpoch: 1,
				}}, BlockRows: 1, BlockBytes: 64, MaxMemory: 64,
			}
		}},
		{name: "exchange push", mutate: func(request *ShardRequest) {
			request.SQL = ""
			request.Params = nil
			request.MaxRows = 0
			request.MaxResultBytes = 0
			request.ExecutionMode = ExecutionReadWrite
			request.Exchange = ExchangeRequest{Operation: ExchangePush, Key: key,
				Batch: exchange.Batch{Rows: 1, Data: []byte{3}, Final: true}}
		}},
		{name: "exchange pull", mutate: func(request *ShardRequest) {
			request.SQL = ""
			request.Params = nil
			request.MaxRows = 0
			request.MaxResultBytes = 0
			request.ExecutionMode = ExecutionReadOnly
			request.Exchange = ExchangeRequest{Operation: ExchangePull, Key: key,
				HasAck: true, AckProducer: 0, AckSequence: 1}
		}},
		{name: "exchange reduce", mutate: func(request *ShardRequest) {
			request.SQL = ""
			request.Params = nil
			request.MaxRows = 0
			request.MaxResultBytes = 0
			request.ExecutionMode = ExecutionReadWrite
			request.Exchange = ExchangeRequest{Operation: ExchangeReduce, Key: key,
				Output:    exchange.Key{Operation: key.Operation, Stage: 2, Partition: 1, Attempt: 1},
				Kinds:     []distributedagg.Kind{distributedagg.None, distributedagg.Count},
				GroupKeys: []uint16{0}, MaxStateBytes: 128, BlockRows: 1, BlockBytes: 64}
		}},
		{name: "transaction lookup", mutate: func(request *ShardRequest) {
			request.SQL = ""
			request.Params = nil
			request.Transaction = TransactionRequest{Operation: TransactionLookupCoordinator, ID: distributedtxn.ID{4}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := base()
			test.mutate(&request)
			want, err := RequestFrameBytes(&request)
			if err != nil {
				t.Fatalf("RequestFrameBytes: %v", err)
			}
			var encoded bytes.Buffer
			if err := EncodeRequest(&encoded, &request); err != nil {
				t.Fatalf("EncodeRequest: %v", err)
			}
			if got := encoded.Len(); got != want {
				t.Fatalf("frame size = %d, checked size = %d", got, want)
			}
			decoded, err := DecodeRequest(bytes.NewReader(encoded.Bytes()))
			if err != nil {
				t.Fatalf("DecodeRequest: %v", err)
			}
			var roundTrip bytes.Buffer
			if err := EncodeRequest(&roundTrip, decoded); err != nil {
				t.Fatalf("round-trip EncodeRequest: %v", err)
			}
			if !bytes.Equal(roundTrip.Bytes(), encoded.Bytes()) {
				t.Fatal("optional envelope changed its canonical bytes after decode")
			}
		})
	}
}

func TestReplicatedRequestFrameBytesMatchesNativeEncoder(t *testing.T) {
	fence := testReplicatedFence()
	authority := serviceauthz.Authority{Node: rafttransport.NodeID{31}, Generation: 17}
	batch, err := replicatedstate.AppendPointReadBatch(nil, []replicatedstate.PointRead{{
		Relation: 1, Key: []byte("a"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	requests := []*ReplicatedRequest{
		{Operation: ReplicatedProbe, Authority: authority, Capability: serviceauthz.CapabilityDataRead,
			Fence: ReplicatedFence{Group: fence.Group, AllocationGeneration: fence.AllocationGeneration}},
		{Operation: ReplicatedPropose, Authority: authority, Capability: serviceauthz.CapabilityDataWrite,
			Fence: fence, Command: testReplicatedCommand(t, fence)},
		{Operation: ReplicatedMembership, Authority: authority, Capability: serviceauthz.CapabilityMembership,
			Fence: fence, Membership: ReplicatedMembershipRequest{Kind: raftservice.MembershipAddLearner,
				TransitionID: [16]byte{3}, MetadataEpoch: 5, CatalogGeneration: 7,
				ExpectedReplicaSetVersion: 1, SourceMember: fence.MemberID, TargetMember: fence.MemberID + 1}},
		{Operation: ReplicatedReadLeader, Authority: authority, Capability: serviceauthz.CapabilityDataRead,
			Fence: fence, Relation: 1, Key: []byte{0, 1}, MinimumApplied: 7, MaxValueBytes: 4096},
		{Operation: ReplicatedReadFollower, Authority: authority, Capability: serviceauthz.CapabilityDataRead,
			Fence: fence, Relation: 2, Key: []byte{2, 1, 0}, MinimumApplied: 9, MaxValueBytes: 8192},
		{Operation: ReplicatedReadBatchLeader, Authority: authority, Capability: serviceauthz.CapabilityDataRead,
			Fence: fence, BatchRead: batch, MinimumApplied: 11, MaxValueBytes: 16384},
		{Operation: ReplicatedQueryLeader, Authority: authority, Capability: serviceauthz.CapabilityDataRead,
			Fence: fence, Query: []byte{1}, MaxValueBytes: 16384},
		{Operation: ReplicatedExecutionPinRead, Authority: authority, Capability: serviceauthz.CapabilityExecutionPin,
			Fence: fence, ExecutionPinRead: ReplicatedExecutionPinReadRequest{Pin: [32]byte{1}, MinimumApplied: 1}},
		{Operation: ReplicatedRouteGateRead, Authority: authority, Capability: serviceauthz.CapabilityDataWrite,
			Fence: fence, MinimumApplied: 1},
	}
	requests = append(requests, replicatedRecoveryRequests(fence, authority)...)
	requests = append(requests, &ReplicatedRequest{
		Operation: ReplicatedRequestLedgerRead, Authority: authority,
		Capability: serviceauthz.CapabilityRequestLedger, Fence: fence,
		RequestLedgerRead: testReplicatedRequestLedgerRead(),
	})
	for _, request := range requests {
		want, err := ReplicatedRequestFrameBytes(request)
		if err != nil {
			t.Fatalf("operation %d checked size: %v", request.Operation, err)
		}
		var encoded bytes.Buffer
		if err := EncodeReplicatedRequest(&encoded, request); err != nil {
			t.Fatalf("operation %d encode: %v", request.Operation, err)
		}
		if encoded.Len() != want {
			t.Fatalf("operation %d frame size = %d, checked size = %d", request.Operation, encoded.Len(), want)
		}
	}
}

func TestSemanticCallValidationRejectsOversizedSQLBeforeCopy(t *testing.T) {
	call := &ReplicatedCall{
		Request: ReplicatedRequest{
			Operation: ReplicatedQueryLeader, Authority: serviceauthz.Authority{
				Node: rafttransport.NodeID{31}, Generation: 17,
			}, Capability: serviceauthz.CapabilityDataRead, Fence: testReplicatedFence(),
			MaxValueBytes: MaxReplicatedSQLResultBytes,
		},
		SQL: &ShardRequest{
			Authority: serviceauthz.Authority{Node: rafttransport.NodeID{31}, Generation: 17},
			SQL:       "SELECT 1", Distribution: "data", Shard: "all",
			AllocationGeneration: 5, RoutingVersion: 1, OwnershipEpoch: 1,
		},
	}
	if _, err := ReplicatedCallFrameBytes(call); err != nil {
		t.Fatalf("valid semantic call rejected: %v", err)
	}
	call.SQL.SQL = string(bytes.Repeat([]byte{'x'}, MaxReplicatedSQLRequestBytes))
	if _, err := ReplicatedCallFrameBytes(call); err == nil {
		t.Fatal("oversized SQL accepted by checked semantic size")
	}
	if err := ValidateReplicatedCall(call); err == nil {
		t.Fatal("oversized SQL accepted by semantic validation")
	}
}

type semanticDispatchTestLease struct {
	released bool
}

func (lease *semanticDispatchTestLease) Release() {
	if lease != nil {
		lease.released = true
	}
}

func TestDetachReplicatedReplyCopiesSQLAndReleasesAdmissionLease(t *testing.T) {
	readLease := new(semanticDispatchTestLease)
	budget := replicatedFrameByteBudget{limit: 256}
	if !budget.reserve(64) {
		t.Fatal("test frame reservation failed")
	}
	responseBytes := []byte(`{"wire":true}`)
	resultBytes := []byte(`"value"`)
	lease := &replicatedReplyLease{
		reply: &ReplicatedReply{
			Response: ReplicatedResponse{Kind: ReplicatedQueryResult, Value: responseBytes},
			SQL: &ShardResponse{Kind: ResponseRows,
				Columns: []Column{{Name: "value"}},
				Rows:    [][]Cell{{{Bytes: resultBytes}}}},
		},
		readLease: readLease, frameBudget: &budget, frameBytes: 64,
	}
	detached, err := DetachReplicatedReply(lease)
	if err != nil || detached == nil || lease.Reply() != nil {
		t.Fatalf("detach = %+v, err=%v, live=%v", detached, err, lease.Reply())
	}
	if !readLease.released || budget.used.Load() != 0 {
		t.Fatalf("lease release = read:%t bytes:%d", readLease.released, budget.used.Load())
	}
	responseBytes[0] = 'X'
	resultBytes[0] = 'X'
	if detached.Response.Value[0] == 'X' || detached.SQL.Rows[0][0].Bytes[0] == 'X' {
		t.Fatal("detached reply aliases released response storage")
	}
	lease.Release()
}

func TestSemanticSQLFrameChargeMatchesWire(t *testing.T) {
	request := ShardRequest{SQL: "SELECT ?", Params: []Param{StringParam("value")},
		Authority: serviceauthz.Authority{Node: rafttransport.NodeID{1}, Generation: 1}}
	call := ReplicatedCall{Request: ReplicatedRequest{Operation: ReplicatedQueryLeader, Authority: request.Authority,
		Capability: serviceauthz.CapabilityDataRead, Fence: testReplicatedFence(), MaxValueBytes: 4096}, SQL: &request}
	var inner, outer bytes.Buffer
	if err := EncodeRequest(&inner, call.SQL); err != nil {
		t.Fatal(err)
	}
	wire := call.Request
	wire.Query = inner.Bytes()
	if err := EncodeReplicatedRequest(&outer, &wire); err != nil {
		t.Fatal(err)
	}
	got, err := ReplicatedCallFrameBytes(&call)
	if err != nil || got != outer.Len() {
		t.Fatalf("semantic charge=%d wire=%d err=%v", got, outer.Len(), err)
	}
}

type semanticServerFixture struct {
	server           *ReplicatedServer
	capability       *ReplicatedServerTLS
	storage, gateway *rafttransport.PeerTLS
	gate             *serviceauthz.Gate
	actor            serviceauthz.Authority
}

func bindSemanticServer(t *testing.T, owner replicatedOwner, timeout time.Duration) semanticServerFixture {
	t.Helper()
	fence := testReplicatedFence()
	domain := rafttransport.TrustDomain{ClusterID: fence.Group.ClusterID, ClusterIncarnation: fence.Group.ClusterIncarnation}
	authority := newShardTLSAuthority(t)
	storage := authority.profile(t, rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{91}})
	gateway := authority.profile(t, rafttransport.PeerIdentity{TrustDomain: domain, Node: rafttransport.NodeID{92}})
	capability, err := NewReplicatedServerTLS(storage, []rafttransport.NodeID{gateway.LocalIdentity().Node})
	if err != nil {
		t.Fatal(err)
	}
	actor := serviceauthz.Authority{Node: rafttransport.NodeID{93}, Generation: 1}
	all := serviceauthz.CapabilityDataRead | serviceauthz.CapabilityDataWrite | serviceauthz.CapabilityRequestLedger
	policy, err := serviceauthz.NewPolicy(1, []serviceauthz.Entry{
		{Node: gateway.LocalIdentity().Node, Capabilities: serviceauthz.CapabilityDelegate | all},
		{Node: actor.Node, Capabilities: all},
	})
	if err != nil {
		t.Fatal(err)
	}
	gate, _ := serviceauthz.NewGate(policy)
	server, err := NewReplicatedServer(owner, DefaultReplicatedInFlightFrameBytes, timeout)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.BindAuthorization(gate, nil); err != nil {
		t.Fatal(err)
	}
	if err := server.BindLocalGatewayPeerTLS(capability, gateway); err != nil {
		t.Fatal(err)
	}
	return semanticServerFixture{server, capability, storage, gateway, gate, actor}
}

func (fixture semanticServerFixture) probe() ReplicatedCall {
	fence := testReplicatedFence()
	return ReplicatedCall{Request: ReplicatedRequest{Operation: ReplicatedProbe, Authority: fixture.actor,
		Capability: serviceauthz.CapabilityDataRead, Fence: ReplicatedFence{Group: fence.Group, AllocationGeneration: fence.AllocationGeneration}}}
}

func requireSemanticResponse(t *testing.T, fixture semanticServerFixture, call ReplicatedCall, kind ReplicatedResponseKind, refusal ReplicatedRefusalCode) *ReplicatedReply {
	t.Helper()
	lease, err := fixture.server.DispatchReplicated(t.Context(), call)
	if err != nil {
		t.Fatal(err)
	}
	reply, err := DetachReplicatedReply(lease)
	if err != nil || reply.Response.Kind != kind || reply.Response.Refusal != refusal {
		t.Fatalf("reply=%+v err=%v, want kind=%d refusal=%d", reply, err, kind, refusal)
	}
	return reply
}

func TestSemanticDispatchBindsCredentialsRevocationLedgerAndRotation(t *testing.T) {
	owner := &fakeReplicatedOwner{state: testReplicatedServingState()}
	fixture := bindSemanticServer(t, owner, time.Second)
	call := fixture.probe()
	requireSemanticResponse(t, fixture, call, ReplicatedHandshake, ReplicatedRefusalNone)
	call.Request.Capability = serviceauthz.CapabilityRequestLedger
	requireSemanticResponse(t, fixture, call, ReplicatedRefusal, ReplicatedRefusalUnauthorized)
	call.Request.Authority.Node = fixture.gateway.LocalIdentity().Node
	requireSemanticResponse(t, fixture, call, ReplicatedHandshake, ReplicatedRefusalNone)
	call = fixture.probe()
	policy, _ := serviceauthz.NewPolicy(2, []serviceauthz.Entry{{Node: fixture.actor.Node, Capabilities: serviceauthz.CapabilityDataRead}})
	if err := fixture.gate.Rotate(policy); err != nil {
		t.Fatal(err)
	}
	requireSemanticResponse(t, fixture, call, ReplicatedRefusal, ReplicatedRefusalUnauthorized)
	call.Request.Authority.Generation = 2
	requireSemanticResponse(t, fixture, call, ReplicatedRefusal, ReplicatedRefusalUnauthorized)
	if owner.probeCalls.Load() != 2 {
		t.Fatalf("denied calls reached owner: %d", owner.probeCalls.Load())
	}
	policy, _ = serviceauthz.NewPolicy(3, []serviceauthz.Entry{
		{Node: fixture.gateway.LocalIdentity().Node, Capabilities: serviceauthz.CapabilityDelegate},
		{Node: fixture.actor.Node, Capabilities: serviceauthz.CapabilityDataRead},
	})
	if err := fixture.gate.Rotate(policy); err != nil {
		t.Fatal(err)
	}
	call.Request.Authority.Generation = 3
	requireSemanticResponse(t, fixture, call, ReplicatedHandshake, ReplicatedRefusalNone)
	if err := fixture.capability.Rotate(fixture.storage, []rafttransport.NodeID{fixture.gateway.LocalIdentity().Node}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.server.DispatchReplicated(t.Context(), call); !errors.Is(err, ErrReplicatedAuthentication) {
		t.Fatalf("old binding survived rotation: %v", err)
	}
	if err := fixture.server.BindLocalGatewayPeerTLS(fixture.capability, fixture.gateway); err != nil {
		t.Fatal(err)
	}
	requireSemanticResponse(t, fixture, call, ReplicatedHandshake, ReplicatedRefusalNone)
	if err := fixture.capability.Rotate(fixture.storage, []rafttransport.NodeID{fixture.actor.Node}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.server.BindLocalGatewayPeerTLS(fixture.capability, fixture.gateway); err == nil {
		t.Fatal("revoked gateway rebound")
	}
	if got := fixture.server.Stats().InFlightFrameBytes; got != 0 {
		t.Fatalf("leaked frame bytes: %d", got)
	}
}

func TestSemanticDispatchRejectsSameNodeAndUntrustedCredentials(t *testing.T) {
	fixture := bindSemanticServer(t, &fakeReplicatedOwner{state: testReplicatedServingState()}, time.Second)
	server := testReplicatedServer(&fakeReplicatedOwner{state: testReplicatedServingState()})
	if err := server.BindAuthorization(fixture.gate, nil); err != nil {
		t.Fatal(err)
	}
	self, _ := NewReplicatedServerTLS(fixture.storage, []rafttransport.NodeID{fixture.storage.LocalIdentity().Node})
	if err := server.BindLocalGatewayPeerTLS(self, fixture.storage); err == nil {
		t.Fatal("storage identity became gateway principal")
	}
	rogue := newShardTLSAuthority(t).profile(t, fixture.gateway.LocalIdentity())
	if err := server.BindLocalGatewayPeerTLS(fixture.capability, rogue); err == nil {
		t.Fatal("same NodeID from wrong roots authenticated")
	}
	other, _ := NewReplicatedServerTLS(fixture.storage, []rafttransport.NodeID{fixture.actor.Node})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := fixture.server.ServeAuthenticated(ctx, listener, other, func() time.Time { return time.Now().Add(time.Second) }, 4, 2); !errors.Is(err, ErrReplicatedAuthentication) {
		t.Fatalf("different listener/local TLS capability accepted: %v", err)
	}
}

type retainedSemanticOwner struct {
	*fakeReplicatedOwner
	command []byte
	entered chan struct{}
}

func (owner *retainedSemanticOwner) SubmitOwnedAuthorized(ctx context.Context, _ raftservice.ServingFence, command []byte, authorize raftservice.ProposalAuthorization) (raftservice.Result, error) {
	if !authorize(owner.state) {
		return raftservice.Result{State: owner.state}, raftservice.ErrServingAuthorization
	}
	if cap(command) != len(command) {
		return raftservice.Result{State: owner.state}, raftservice.ErrInvalidOwner
	}
	owner.command = command
	close(owner.entered)
	<-ctx.Done()
	return raftservice.Result{State: owner.state}, context.Cause(ctx)
}

func TestSemanticDispatchOwnsUnknownCommandAndCancelsOnRotationAndShutdown(t *testing.T) {
	for _, stopKind := range []string{"deadline", "rotation", "shutdown"} {
		t.Run(stopKind, func(t *testing.T) {
			owner := &retainedSemanticOwner{fakeReplicatedOwner: &fakeReplicatedOwner{state: testReplicatedServingState()}, entered: make(chan struct{})}
			fixture := bindSemanticServer(t, owner, 30*time.Millisecond)
			call := ReplicatedCall{Request: ReplicatedRequest{Operation: ReplicatedPropose, Authority: fixture.actor,
				Capability: serviceauthz.CapabilityDataWrite, Fence: testReplicatedFence(), Command: testReplicatedCommand(t, testReplicatedFence())}}
			original := bytes.Clone(call.Request.Command)
			var listener net.Listener
			var cancel context.CancelFunc
			var serveDone chan error
			if stopKind == "shutdown" {
				var err error
				listener, err = net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				var ctx context.Context
				ctx, cancel = context.WithCancel(t.Context())
				defer cancel()
				serveDone = make(chan error, 1)
				go func() {
					serveDone <- fixture.server.ServeAuthenticated(ctx, listener, fixture.capability, func() time.Time { return time.Now().Add(time.Second) }, 4, 2)
				}()
			}
			done := make(chan *ReplicatedReply, 1)
			go func() {
				lease, err := fixture.server.DispatchReplicated(t.Context(), call)
				if err != nil {
					done <- nil
					return
				}
				reply, _ := DetachReplicatedReply(lease)
				done <- reply
			}()
			select {
			case <-owner.entered:
			case <-time.After(time.Second):
				t.Fatal("proposal not admitted")
			}
			switch stopKind {
			case "rotation":
				if err := fixture.capability.Rotate(fixture.storage, []rafttransport.NodeID{fixture.gateway.LocalIdentity().Node}); err != nil {
					t.Fatal(err)
				}
			case "shutdown":
				cancel()
				<-serveDone
			}
			select {
			case reply := <-done:
				if reply == nil || reply.Response.Kind != ReplicatedOutcomeUnknown {
					t.Fatalf("canceled proposal=%+v", reply)
				}
			case <-time.After(time.Second):
				t.Fatal("local work ignored cancellation")
			}
			for index := range call.Request.Command {
				call.Request.Command[index] ^= 0xff
			}
			if !bytes.Equal(owner.command, original) {
				t.Fatal("SubmitOwned retained caller bytes after unknown return")
			}
			if got := fixture.server.Stats().InFlightFrameBytes; got != 0 {
				t.Fatalf("leaked frame bytes: %d", got)
			}
			if stopKind == "shutdown" {
				if _, err := fixture.server.DispatchReplicated(t.Context(), fixture.probe()); err == nil {
					t.Fatal("closed server admitted local work")
				}
			}
		})
	}
}

func TestSemanticDispatchRetainsSharedAdmissionUntilRelease(t *testing.T) {
	fixture := bindSemanticServer(t, &fakeReplicatedOwner{state: testReplicatedServingState()}, time.Second)
	call := fixture.probe()
	size, err := ReplicatedCallFrameBytes(&call)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := fixture.server.DispatchReplicated(t.Context(), call)
	if err != nil {
		t.Fatal(err)
	}
	if got := fixture.server.frames.used.Load(); got != int64(size-5) {
		t.Fatalf("frame charge=%d want=%d", got, size-5)
	}
	rest := fixture.server.frames.limit - fixture.server.frames.used.Load()
	if !fixture.server.frames.reserve(rest) {
		t.Fatal("reserve remainder")
	}
	if _, err := fixture.server.DispatchReplicated(t.Context(), call); !errors.Is(err, errFrameBudget) {
		t.Fatalf("full shared frame budget error=%v", err)
	}
	fixture.server.frames.release(rest)
	lease.Release()
	if got := fixture.server.frames.used.Load(); got != 0 {
		t.Fatalf("leaked frame bytes: %d", got)
	}
	requireSemanticResponse(t, fixture, call, ReplicatedHandshake, ReplicatedRefusalNone)
}

func TestSemanticResultEncodingAndDetachmentCopiesBorrowedStrings(t *testing.T) {
	name, message, value := []byte("name"), []byte("message"), []byte("value")
	result := &ShardResponse{Kind: ResponseRows, Columns: []Column{{Name: unsafe.String(&name[0], len(name))}}, Rows: [][]Cell{{{Bytes: value}}}}
	response := ReplicatedResponse{Kind: ReplicatedQueryResult, HasState: true, State: replicatedWireState(testReplicatedServingState()), ReadApplied: 1, sqlResult: result}
	var encoded bytes.Buffer
	if err := EncodeReplicatedResponse(&encoded, &response); err != nil {
		t.Fatal(err)
	}
	wire, err := DecodeReplicatedResponse(&encoded)
	if err != nil || len(wire.Value) == 0 {
		t.Fatalf("semantic result lost at socket boundary: %+v %v", wire, err)
	}
	decoded, err := DecodeResponse(bytes.NewReader(wire.Value))
	if err != nil || string(decoded.Rows[0][0].Bytes) != "value" {
		t.Fatalf("SQL response=%+v err=%v", decoded, err)
	}
	lease := &replicatedReplyLease{reply: &ReplicatedReply{Response: response, SQL: result}}
	detached, err := DetachReplicatedReply(lease)
	if err != nil {
		t.Fatal(err)
	}
	errorResult := cloneShardResponse(&ShardResponse{Kind: ResponseError, ErrorKind: ErrorMalformedRequest, ErrorMessage: unsafe.String(&message[0], len(message))})
	name[0], message[0], value[0] = 'X', 'X', 'X'
	if detached.SQL.Columns[0].Name != "name" || errorResult.ErrorMessage != "message" || string(detached.SQL.Rows[0][0].Bytes) != "value" {
		t.Fatal("detached result aliases released storage")
	}
	if err := ValidateReplicatedReply(detached); err != nil {
		t.Fatal(err)
	}
	invalid := *detached.SQL
	invalid.Rows = make([][]Cell, maxRows+1)
	if replicatedSemanticSQLResultValid(&invalid) {
		t.Fatal("over-limit zero-column rows accepted")
	}
	invalid = ShardResponse{Kind: ResponseError, ErrorKind: ErrorKind(255), ErrorMessage: "invalid"}
	if replicatedSemanticSQLResultValid(&invalid) {
		t.Fatal("invalid SQL error enum accepted")
	}
}

func TestSemanticDispatchDecodesDirectSQLFrame(t *testing.T) {
	result := &ShardResponse{
		Kind:    ResponseRows,
		Columns: []Column{{Name: "value"}},
		Rows:    [][]Cell{{{Bytes: []byte(`42`)}}},
	}
	var frame bytes.Buffer
	if err := EncodeResponse(&frame, result); err != nil {
		t.Fatal(err)
	}
	response := &ReplicatedResponse{
		Kind:        ReplicatedQueryResult,
		HasState:    true,
		State:       replicatedWireState(testReplicatedServingState()),
		ReadApplied: 1,
		Value:       frame.Bytes(),
	}
	reply, err := semanticReplyFromExecutedResponse(response)
	if err != nil {
		t.Fatal(err)
	}
	if len(reply.Response.Value) != 0 || reply.SQL == nil ||
		len(reply.SQL.Rows) != 1 || len(reply.SQL.Rows[0]) != 1 ||
		string(reply.SQL.Rows[0][0].Bytes) != "42" {
		t.Fatalf("decoded semantic reply = %+v", reply)
	}
	if err := ValidateReplicatedReply(reply); err != nil {
		t.Fatal(err)
	}

	bad := *response
	bad.Value = append([]byte(nil), response.Value...)
	bad.Value[len(bad.Value)-1] ^= 0xff
	if _, err := semanticReplyFromExecutedResponse(&bad); !errors.Is(err, ErrReplicatedWire) {
		t.Fatalf("corrupt direct frame error = %v", err)
	}
}

func TestSemanticDispatchChecksCredentialExpiryOnEveryCall(t *testing.T) {
	fixture := bindSemanticServer(t, &fakeReplicatedOwner{state: testReplicatedServingState()}, time.Second)
	now := shardTLSNow
	clock := func() time.Time { return now }
	credentials := newShardTLSAuthority(t)
	storage := credentials.profileClock(t, fixture.storage.LocalIdentity(), clock)
	gateway := credentials.profileClock(t, fixture.gateway.LocalIdentity(), clock)
	capability, err := NewReplicatedServerTLS(storage, []rafttransport.NodeID{gateway.LocalIdentity().Node})
	if err != nil {
		t.Fatal(err)
	}
	owner := &fakeReplicatedOwner{state: testReplicatedServingState()}
	server := testReplicatedServer(owner)
	if err := server.BindAuthorization(fixture.gate, nil); err != nil {
		t.Fatal(err)
	}
	if err := server.BindLocalGatewayPeerTLS(capability, gateway); err != nil {
		t.Fatal(err)
	}
	fixture.server = server
	requireSemanticResponse(t, fixture, fixture.probe(), ReplicatedHandshake, ReplicatedRefusalNone)
	now = now.Add(2 * time.Hour)
	if _, err := server.DispatchReplicated(t.Context(), fixture.probe()); !errors.Is(err, ErrReplicatedAuthentication) {
		t.Fatalf("expired local credential error=%v", err)
	}
	if owner.probeCalls.Load() != 1 || server.Stats().InFlightFrameBytes != 0 {
		t.Fatal("expired credential reached owner or retained credits")
	}
}

func TestSemanticSQLQuotaPreservesNativeAndHonorsMinimumDeadline(t *testing.T) {
	for _, deadlineKind := range []string{"caller", "server", "sql", "quota"} {
		t.Run(deadlineKind, func(t *testing.T) {
			state := testReplicatedServingState()
			state.Identity.Distribution, state.Identity.Shard = "data", "all"
			owner := &blockingReplicatedSQLAdmissionOwner{fakeReplicatedOwner: &fakeReplicatedOwner{state: state}, entered: make(chan struct{}, 1), release: make(chan struct{})}
			timeout := time.Second
			if deadlineKind == "server" {
				timeout = 20 * time.Millisecond
			}
			fixture := bindSemanticServer(t, owner, timeout)
			call := ReplicatedCall{Request: ReplicatedRequest{Operation: ReplicatedQueryLeader, Authority: fixture.actor, Capability: serviceauthz.CapabilityDataRead, Fence: replicatedWireState(state).Fence, MaxValueBytes: 4096},
				SQL: &ShardRequest{Authority: fixture.actor, SQL: "SELECT 1", Distribution: "data", Shard: "all", AllocationGeneration: 5, RoutingVersion: 1, OwnershipEpoch: 1, MaxResultBytes: 4096}}
			// Follow the fixture's exact serving coordinates, not a guessed SQL fence.
			call.SQL.AllocationGeneration = distribution.ShardAllocationGeneration(state.Identity.AllocationGeneration)
			call.SQL.RoutingVersion = distribution.RoutingVersion(state.Command.RoutingVersion)
			call.SQL.OwnershipEpoch = distribution.OwnershipEpoch(state.Command.OwnershipEpoch)
			ctx := t.Context()
			if deadlineKind == "caller" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
				defer cancel()
			}
			if deadlineKind == "sql" || deadlineKind == "quota" {
				call.SQL.Deadline = 20 * time.Millisecond
			}
			if deadlineKind == "quota" {
				charge := fixture.server.frames.sqlLimit()
				if !fixture.server.frames.reserveSQL(ctx, charge) {
					t.Fatal("reserve SQL quota")
				}
				defer fixture.server.frames.releaseSQL(charge)
				// The same shared budget still admits native traffic while SQL is full.
				requireSemanticResponse(t, fixture, fixture.probe(), ReplicatedHandshake, ReplicatedRefusalNone)
			}
			started := time.Now()
			lease, err := fixture.server.DispatchReplicated(ctx, call)
			if err != nil {
				t.Fatal(err)
			}
			reply, err := DetachReplicatedReply(lease)
			want := ReplicatedRefusalUnavailable
			if deadlineKind == "quota" {
				want = ReplicatedRefusalAdmissionBound
			}
			if err != nil || reply.Response.Refusal != want || time.Since(started) > 500*time.Millisecond {
				t.Fatalf("deadline reply=%+v err=%v elapsed=%v", reply, err, time.Since(started))
			}
			wantBytes := int64(0)
			if deadlineKind == "quota" {
				wantBytes = fixture.server.frames.sqlLimit()
				if len(owner.entered) != 0 {
					t.Fatal("full SQL quota reached a snapshot owner")
				}
			}
			if got := fixture.server.Stats().InFlightFrameBytes; got != wantBytes {
				t.Fatalf("leaked credits: got=%d want=%d", got, wantBytes)
			}
		})
	}
}

type semanticPoisonReadLease struct {
	data     []byte
	releases int
}

func (lease *semanticPoisonReadLease) Release() {
	lease.releases++
	for index := range lease.data {
		lease.data[index] = 'X'
	}
}

func TestSemanticPointReplyLeaseOwnsFoundMissingAndEmpty(t *testing.T) {
	for _, variant := range []string{"found", "missing", "empty"} {
		t.Run(variant, func(t *testing.T) {
			state := testReplicatedServingState()
			value := []byte("value")
			if variant != "found" {
				value = nil
			}
			readLease := &semanticPoisonReadLease{data: value}
			owner := &fakeReplicatedOwner{state: state, readResult: raftservice.PointReadResult{Found: variant != "missing", Value: value, Applied: state.Status.Applied}, readLease: readLease}
			fixture := bindSemanticServer(t, owner, time.Second)
			call := ReplicatedCall{Request: ReplicatedRequest{Operation: ReplicatedReadLeader, Authority: fixture.actor, Capability: serviceauthz.CapabilityDataRead, Fence: replicatedWireState(state).Fence, Relation: 1, Key: []byte("key"), MinimumApplied: state.Status.Applied, MaxValueBytes: 1024}}
			lease, err := fixture.server.DispatchReplicated(t.Context(), call)
			if err != nil {
				t.Fatal(err)
			}
			if readLease.releases != 0 {
				t.Fatal("read lease released before consumer detached")
			}
			reply, err := DetachReplicatedReply(lease)
			if err != nil {
				t.Fatal(err)
			}
			want := ReplicatedReadFound
			if variant == "missing" {
				want = ReplicatedReadMissing
			}
			if reply.Response.Kind != want || (variant == "found" && string(reply.Response.Value) != "value") || readLease.releases != 1 || fixture.server.frames.used.Load() != 0 {
				t.Fatalf("detached %s reply=%+v lease=%+v", variant, reply, readLease)
			}
		})
	}
}

func TestReplicatedSQLNestedLengthsRejectBeforeAllocation(t *testing.T) {
	for _, response := range []bool{false, true} {
		frame := []byte{tagRequest, 0, 0, 0, 0, wireVersion}
		if response {
			frame[0] = tagResponse
		}
		binary.BigEndian.PutUint32(frame[1:5], uint32(maxFrameBody))
		decode := func() error {
			if response {
				_, err := DecodeReplicatedSQLResponse(frame)
				return err
			}
			_, err := DecodeReplicatedSQLRequest(frame)
			return err
		}
		if err := decode(); !errors.Is(err, ErrReplicatedWire) {
			t.Fatalf("response=%v malformed nested length error=%v", response, err)
		}
		if allocations := testing.AllocsPerRun(100, func() {
			if decode() == nil {
				panic("invalid frame accepted")
			}
		}); allocations != 0 {
			t.Fatalf("response=%v allocated %g times before nested length rejection", response, allocations)
		}
	}
}
