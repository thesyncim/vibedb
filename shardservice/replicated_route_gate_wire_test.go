package shardservice

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/routegate"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"testing"
)

func (owner *fakeReplicatedOwner) ReadRouteGate(context.Context, raftservice.RouteGateReadRequest) (raftservice.RouteGateReadResult, raftservice.RouteGateReadLease, error) {
	return raftservice.RouteGateReadResult{}, nil, raftservice.ErrServingFence
}

type routeGateOwner struct {
	*fakeReplicatedOwner
	result  raftservice.RouteGateReadResult
	request raftservice.RouteGateReadRequest
	lease   raftservice.RouteGateReadLease
	err     error
}

func (o *routeGateOwner) ReadRouteGate(_ context.Context, request raftservice.RouteGateReadRequest) (raftservice.RouteGateReadResult, raftservice.RouteGateReadLease, error) {
	o.request = request
	return o.result, o.lease, o.err
}
func routeGateRequest() *ReplicatedRequest {
	return &ReplicatedRequest{Operation: ReplicatedRouteGateRead,
		Authority:  serviceauthz.Authority{Node: authorizationNode(92), Generation: 11},
		Capability: serviceauthz.CapabilityDataWrite, Fence: testReplicatedFence(), MinimumApplied: 7}
}
func TestReplicatedRouteGateReadCanonicalWireAndBounds(t *testing.T) {
	request := routeGateRequest()
	var raw, borrowed bytes.Buffer
	if err := EncodeReplicatedRequest(&raw, request); err != nil {
		t.Fatal(err)
	}
	if err := EncodeReplicatedRequestBorrowed(&borrowed, request); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw.Bytes(), borrowed.Bytes()) || raw.Len() != 5+replicatedRouteGateReadRequestBodyBytes || raw.Bytes()[0] != tagReplicatedRouteGateRead {
		t.Fatal("noncanonical request")
	}
	got, err := DecodeReplicatedRequest(bytes.NewReader(raw.Bytes()))
	if err != nil || got.Operation != request.Operation || got.Fence != request.Fence || got.Authority != request.Authority || got.MinimumApplied != 7 {
		t.Fatalf("roundtrip=%+v %v", got, err)
	}
	for _, mutate := range []func(*ReplicatedRequest){
		func(r *ReplicatedRequest) { r.MinimumApplied = 0 },
		func(r *ReplicatedRequest) { r.Capability = serviceauthz.CapabilityDataRead },
		func(r *ReplicatedRequest) { r.Capability = serviceauthz.CapabilityTopology },
		func(r *ReplicatedRequest) { r.Capability = serviceauthz.CapabilityExecutionPin },
		func(r *ReplicatedRequest) { r.Authority = serviceauthz.Authority{} },
		func(r *ReplicatedRequest) { r.Fence.Term = 0 },
		func(r *ReplicatedRequest) { r.Relation = 1 },
		func(r *ReplicatedRequest) { r.Key = []byte("hidden") },
		func(r *ReplicatedRequest) { r.MaxValueBytes = 124 },
		func(r *ReplicatedRequest) { r.Command = []byte{1} },
		func(r *ReplicatedRequest) { r.ExecutionPinRead.MinimumApplied = 1 },
	} {
		bad := *request
		mutate(&bad)
		if validReplicatedRequest(&bad) {
			t.Fatalf("invalid accepted %+v", bad)
		}
	}
	for _, size := range []uint32{249, 251, 1 << 30} {
		header := []byte{tagReplicatedRouteGateRead, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(header[1:], size+4)
		if _, err := DecodeReplicatedRequest(bytes.NewReader(header)); !errors.Is(err, ErrReplicatedWire) {
			t.Fatalf("size %d err %v", size, err)
		}
	}
	bad := append([]byte(nil), raw.Bytes()...)
	bad[6] = byte(ReplicatedReadFollower)
	if _, err := DecodeReplicatedRequest(bytes.NewReader(bad)); err == nil {
		t.Fatal("follower operation accepted under gate tag")
	}
	status := routegate.Status{Epoch: 17, Revision: 4, ActivePins: 2, ReleasedPins: 1, RetainedRecords: 3}
	value, err := AppendReplicatedRouteGateReadValue(nil, status)
	if err != nil {
		t.Fatal(err)
	}
	response := &ReplicatedResponse{Kind: ReplicatedRouteGateReadResult, HasState: true, State: replicatedWireState(testReplicatedServingState()), ReadApplied: 9, Value: value}
	raw.Reset()
	if err := EncodeReplicatedResponse(&raw, response); err != nil {
		t.Fatal(err)
	}
	maximum, err := maximumReplicatedResponseBody(request)
	if err != nil || maximum != replicatedReadResponseFixedBodyBytes+routegate.StatusBytes || raw.Len() != maximum+5 {
		t.Fatalf("max %d %v", maximum, err)
	}
	decoded, err := decodeReplicatedResponseLimit(bytes.NewReader(raw.Bytes()), maximum)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenReplicatedRouteGateReadValue(decoded.Value)
	if err != nil || opened != status {
		t.Fatalf("status %+v %v", opened, err)
	}
	header := []byte{tagReplicatedResponse, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(header[1:], uint32(maximum+5))
	if _, err := decodeReplicatedResponseLimit(bytes.NewReader(header), maximum); !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("oversized response %v", err)
	}
	response.ReadApplied = 12
	if validReplicatedResponse(response) {
		t.Fatal("response ahead of state")
	}
}
func TestReplicatedRouteGateReadAuthorizationAndOwnerFences(t *testing.T) {
	peer := authorizationNode(91)
	writer := authorizationNode(92)
	reader := authorizationNode(93)
	gate := authorizationGate(t, 11, serviceauthz.Entry{Node: peer, Capabilities: serviceauthz.CapabilityDelegate}, serviceauthz.Entry{Node: writer, Capabilities: serviceauthz.CapabilityDataWrite}, serviceauthz.Entry{Node: reader, Capabilities: serviceauthz.CapabilityDataRead})
	server := &ReplicatedServer{authorization: gate}
	request := routeGateRequest()
	if !server.authorizeReplicated(peer, request) {
		t.Fatal("writer denied")
	}
	request.Authority.Node = reader
	if server.authorizeReplicated(peer, request) {
		t.Fatal("reader gained lifecycle status")
	}
	request.Authority.Node = writer
	if server.authorizeReplicated(reader, request) {
		t.Fatal("nondelegate")
	}
	request.Authority.Generation++
	if server.authorizeReplicated(peer, request) {
		t.Fatal("stale generation")
	}
	request = routeGateRequest()
	owner := &routeGateOwner{fakeReplicatedOwner: &fakeReplicatedOwner{state: testReplicatedServingState()}, result: raftservice.RouteGateReadResult{Applied: 9, Status: routegate.Status{Epoch: 17}}}
	server = testReplicatedServer(owner)
	lease := &testPointReadLease{}
	owner.lease = lease
	response := server.executeReplicated(t.Context(), request)
	if response.Kind != ReplicatedRouteGateReadResult || !validReplicatedResponse(response) || owner.request.Fence.Term != request.Fence.Term || owner.request.MinimumApplied != 7 || owner.request.Capability != serviceauthz.CapabilityDataWrite || lease.released.Load() {
		t.Fatalf("response %+v request %+v", response, owner.request)
	}
	response.readLease.Release()
	if !lease.released.Load() {
		t.Fatal("lease not retained through response")
	}
	for _, tt := range []struct {
		err     error
		kind    ReplicatedResponseKind
		refusal ReplicatedRefusalCode
	}{
		{raftmodel.ErrNotLeader, ReplicatedNotLeader, 0},
		{raftmodel.ErrReadLeadershipLost, ReplicatedNotLeader, 0},
		{raftservice.ErrServingFence, ReplicatedRefusal, ReplicatedRefusalStaleFence},
		{replicatedstate.ErrReadBehind, ReplicatedRefusal, ReplicatedRefusalReadBehind},
		{raftservice.ErrRouteGateUnauthorized, ReplicatedRefusal, ReplicatedRefusalUnauthorized},
		{raftservice.ErrPendingReadsFull, ReplicatedRefusal, ReplicatedRefusalAdmissionBound},
	} {
		owner.err = tt.err
		owner.lease = &testPointReadLease{}
		response = server.executeReplicated(t.Context(), request)
		if response.Kind != tt.kind || response.Refusal != tt.refusal || !owner.lease.(*testPointReadLease).released.Load() {
			t.Fatalf("error %v response %+v", tt.err, response)
		}
	}
}
