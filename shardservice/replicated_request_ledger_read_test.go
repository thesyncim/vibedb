package shardservice

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func testReplicatedRequestLedgerRead() ReplicatedRequestLedgerReadRequest {
	return ReplicatedRequestLedgerReadRequest{
		Key: requestledger.RequestKey{
			Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID{7},
			Request: requestledger.RequestID{8}, TenantDigest: requestledger.Digest{9},
		},
		ExpectedRangeIdentity: requestledger.Digest{10},
		Kind:                  replicatedstate.RequestLedgerReadAck,
		MinimumApplied:        1,
		MaxBytes: uint32(replicatedstate.RequestLedgerReadMaxBytes(
			replicatedstate.RequestLedgerReadAck,
		)),
	}
}

func TestReplicatedRequestLedgerReadWireIsFixedAndFullKey(t *testing.T) {
	request := &ReplicatedRequest{
		Operation:  ReplicatedRequestLedgerRead,
		Authority:  serviceauthz.Authority{Node: rafttransport.NodeID{9}, Generation: 3},
		Capability: serviceauthz.CapabilityRequestLedger,
		Fence:      testReplicatedFence(), RequestLedgerRead: testReplicatedRequestLedgerRead(),
	}
	var raw bytes.Buffer
	if err := EncodeReplicatedRequest(&raw, request); err != nil {
		t.Fatal(err)
	}
	if raw.Bytes()[0] != tagReplicatedRequestLedgerRead ||
		raw.Len()-5 != replicatedRequestLedgerReadRequestBodyBytes {
		t.Fatalf("tag/body = %q/%d", raw.Bytes()[0], raw.Len()-5)
	}
	opened, err := DecodeReplicatedRequest(bytes.NewReader(raw.Bytes()))
	if err != nil || opened.Operation != request.Operation ||
		opened.Authority != request.Authority || opened.Capability != request.Capability ||
		opened.Fence != request.Fence || opened.RequestLedgerRead != request.RequestLedgerRead {
		t.Fatalf("round trip: opened=%+v err=%v", opened, err)
	}

	mutated := *request
	mutated.RequestLedgerRead.Key.Principal = requestledger.PrincipalID{}
	if err := EncodeReplicatedRequest(new(bytes.Buffer), &mutated); err == nil {
		t.Fatal("zero inner subject accepted")
	}
}

func TestReplicatedRequestLedgerReadValuePreservesAuthoritativeKind(t *testing.T) {
	for _, value := range []ReplicatedRequestLedgerReadValue{
		{},
		{Found: true, AuthoritativeKind: replicatedstate.RequestLedgerReadAck, Value: []byte{1, 2, 3}},
	} {
		raw, err := AppendReplicatedRequestLedgerReadValue(nil, value)
		if err != nil {
			t.Fatal(err)
		}
		opened, err := OpenReplicatedRequestLedgerReadValue(raw)
		if err != nil || opened.Found != value.Found ||
			opened.AuthoritativeKind != value.AuthoritativeKind ||
			!bytes.Equal(opened.Value, value.Value) {
			t.Fatalf("round trip: opened=%+v err=%v", opened, err)
		}
	}
}

func TestReplicatedTerminalCutFitsNativeFrameLimit(t *testing.T) {
	request := &ReplicatedRequest{
		Operation: ReplicatedRequestLedgerRead, Fence: testReplicatedFence(),
		Authority:  serviceauthz.Authority{Node: rafttransport.NodeID{9}, Generation: 3},
		Capability: serviceauthz.CapabilityRequestLedger, RequestLedgerRead: testReplicatedRequestLedgerRead(),
	}
	request.RequestLedgerRead.Kind = replicatedstate.RequestLedgerReadTerminalCut
	request.RequestLedgerRead.MaxBytes = uint32(replicatedstate.RequestLedgerReadMaxBytes(request.RequestLedgerRead.Kind))
	maximum, err := maximumReplicatedResponseBody(request)
	if err != nil {
		t.Fatal(err)
	}
	// Even a small valid reply was rejected before reading its frame header:
	// the canonical terminal-read maximum exceeded the generic codec ceiling.
	value, err := AppendReplicatedRequestLedgerReadValue(nil, ReplicatedRequestLedgerReadValue{
		Found: true, AuthoritativeKind: request.RequestLedgerRead.Kind, Value: []byte{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	response := &ReplicatedResponse{Kind: ReplicatedRequestLedgerReadResult, HasState: true,
		State: ReplicatedMemberState{Fence: request.Fence, LeaderID: request.Fence.MemberID,
			Commit: 1, Applied: 1, CheckpointApplied: 1},
		ReadApplied: 1, Value: value}
	var wire bytes.Buffer
	if err := EncodeReplicatedResponse(&wire, response); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeReplicatedResponseLimit(bytes.NewReader(wire.Bytes()), maximum); err != nil {
		t.Fatalf("valid terminal read: %v", err)
	}
	if maximum != maxFrameBody {
		t.Fatalf("frame ceiling is not the exact largest envelope: %d != %d", maximum, maxFrameBody)
	}
	var header [5]byte
	header[0] = tagReplicatedResponse
	binary.BigEndian.PutUint32(header[1:], uint32(maximum+5))
	if _, err := decodeReplicatedResponseLimit(bytes.NewReader(header[:]), maximum); !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("oversized terminal envelope was not rejected at header: %v", err)
	}
}

func TestReplicatedServerServesRequestLedgerThroughDedicatedOwnerRead(t *testing.T) {
	state := testReplicatedServingState()
	state.Status.Applied = max(state.Status.Applied, uint64(7))
	state.Status.Commit = max(state.Status.Commit, state.Status.Applied)
	owner := &fakeReplicatedOwner{
		state: state,
		requestLedgerResult: raftservice.RequestLedgerReadResult{
			Applied: 12, Found: true, AuthoritativeKind: replicatedstate.RequestLedgerReadAck,
			Value: []byte{1, 2, 3},
		},
	}
	request := &ReplicatedRequest{
		Operation:         ReplicatedRequestLedgerRead,
		Authority:         serviceauthz.Authority{Node: rafttransport.NodeID{9}, Generation: 3},
		Capability:        serviceauthz.CapabilityRequestLedger,
		Fence:             replicatedWireState(state).Fence,
		RequestLedgerRead: testReplicatedRequestLedgerRead(),
	}
	response := testReplicatedServer(owner).executeReplicated(t.Context(), request)
	if response.Kind != ReplicatedRequestLedgerReadResult || response.ReadApplied != 12 ||
		response.State.Fence != request.Fence || response.State.Applied != 12 ||
		response.State.Commit != 12 || owner.probeCalls.Load() != 0 ||
		!validReplicatedResponse(response) {
		t.Fatalf("response = %+v", response)
	}
	opened, err := OpenReplicatedRequestLedgerReadValue(response.Value)
	if err != nil || !opened.Found || opened.AuthoritativeKind != replicatedstate.RequestLedgerReadAck ||
		!bytes.Equal(opened.Value, []byte{1, 2, 3}) {
		t.Fatalf("value = %+v err=%v", opened, err)
	}
	if owner.requestLedgerRequest.Read.Key != request.RequestLedgerRead.Key ||
		owner.requestLedgerRequest.Capability != serviceauthz.CapabilityRequestLedger {
		t.Fatalf("owner request = %+v", owner.requestLedgerRequest)
	}
}
