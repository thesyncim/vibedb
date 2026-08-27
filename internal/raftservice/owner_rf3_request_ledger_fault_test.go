package raftservice_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestRequestLedgerRF3LostCallerStaysDisconnectedAcrossRetries(t *testing.T) {
	client := &multiGroupRequestLedgerRF3RoundTripper{hiddenMember: -1}
	client.armLostResponse(requestledger.OperationCreate)
	trace := multiGroupRequestLedgerRF3Trace{member: 1, operation: requestledger.OperationCreate}
	if client.recordProposal(trace, &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedNotLeader}) || client.callerDisconnected() {
		t.Fatal("fault fired before committed response")
	}
	other := trace
	other.operation = requestledger.OperationOpenIssuerLane
	if client.recordProposal(other, &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedCompletion}) || client.callerDisconnected() {
		t.Fatal("fault fired on another operation")
	}
	if !client.recordProposal(trace, &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedCompletion}) || !client.callerDisconnected() {
		t.Fatal("lost committed response did not disconnect caller")
	}
	// No cluster is installed: every production retry must fail before touching
	// another endpoint, rather than silently resolving the simulated lost call.
	for range 8 {
		if response, err := client.DoReplicated(context.Background(), gateway.ReplicatedEndpoint{}, nil); response != nil || !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("disconnected retry reached cluster: %v %v", response, err)
		}
	}
	client.reconnectCaller()
	if client.callerDisconnected() || client.recordProposal(trace, &shardservice.ReplicatedResponse{Kind: shardservice.ReplicatedCompletion}) {
		t.Fatal("explicit reconnect reinjected already-consumed fault")
	}
	if client.hiddenMember != 1 {
		t.Fatal("lost original committing member")
	}
}

func TestRequestLedgerRF3NativeDiagnosticsRetainBoundedRecentResults(t *testing.T) {
	client := &multiGroupRequestLedgerRF3RoundTripper{}
	const count = 3*multiGroupRF3NativeTraceLimit + 5
	for index := 0; index < count; index++ {
		response := &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedNotLeader, HasState: true, ReadApplied: uint64(index),
			State: shardservice.ReplicatedMemberState{
				LeaderID: 2, Applied: uint64(index), Commit: uint64(index + 1),
				Fence: shardservice.ReplicatedFence{Term: 3},
			},
		}
		client.recordNativeResult(2, 1, shardservice.ReplicatedRequestLedgerRead, response, io.ErrUnexpectedEOF)
		response.State.Applied = 0 // Recorded metadata must not alias the response.
	}
	if client.nativeCount != multiGroupRF3NativeTraceLimit || client.nativeNext != count%multiGroupRF3NativeTraceLimit {
		t.Fatalf("unbounded native diagnostics count=%d next=%d", client.nativeCount, client.nativeNext)
	}
	for ordinal := 0; ordinal < client.nativeCount; ordinal++ {
		index := (client.nativeNext - client.nativeCount + ordinal + len(client.nativeTrace)) % len(client.nativeTrace)
		trace := client.nativeTrace[index]
		want := uint64(count - multiGroupRF3NativeTraceLimit + ordinal)
		if trace.group != 2 || trace.member != 1 || trace.operation != shardservice.ReplicatedRequestLedgerRead ||
			trace.kind != shardservice.ReplicatedNotLeader || !trace.hasState || trace.leader != 2 || trace.term != 3 ||
			trace.applied != want || trace.commit != want+1 || trace.readApplied != want || !errors.Is(trace.err, io.ErrUnexpectedEOF) {
			t.Fatalf("diagnostic ordinal=%d trace=%+v want_applied=%d", ordinal, trace, want)
		}
	}
	client.recordNativeResult(2, 1, shardservice.ReplicatedProbe, nil, io.ErrUnexpectedEOF)
	last := client.nativeTrace[(client.nativeNext-1+len(client.nativeTrace))%len(client.nativeTrace)]
	if last.hasState || last.kind != 0 || last.applied != 0 || !errors.Is(last.err, io.ErrUnexpectedEOF) {
		t.Fatalf("failed native probe inherited a successful response: %+v", last)
	}
}

func TestRequestLedgerRF3SequenceFixtureFitsOrdinaryCapacity(t *testing.T) {
	key1, key2 := multiGroupRF3RequestKey(1, 0x81), multiGroupRF3RequestKey(2, 0x82)
	head1, head2 := multiGroupRF3RequestHead(t, key1), multiGroupRF3RequestHead(t, key2)
	highwater, err := requestledger.NewIssuerHighwater(key1)
	if err != nil {
		t.Fatal(err)
	}
	lane, err := requestledger.AppendIssuerHighwater(nil, highwater)
	if err != nil {
		t.Fatal(err)
	}
	consumed := uint64(len(requestledger.AppendIssuerHighwaterKey(nil, highwater.Home, highwater.IssuerDigest)) + len(lane))
	defaultConsumed := consumed
	for _, head := range []requestledger.HeadRecord{head1, head2} {
		raw, encodeErr := requestledger.AppendHead(nil, head)
		if encodeErr != nil {
			t.Fatal(encodeErr)
		}
		if _, openErr := requestledger.OpenHead(raw); openErr != nil {
			t.Fatal(openErr)
		}
		resident, reserved, reservationErr := requestledger.Reservation(head)
		if reservationErr != nil {
			t.Fatal(reservationErr)
		}
		consumed += resident + reserved
		defaults, defaultErr := requestledger.NewHeadWithContract(head.Key, head.RequestDigest, head.TerminalContractDigest, head.InlinePlan)
		if defaultErr != nil {
			t.Fatal(defaultErr)
		}
		resident, reserved, reservationErr = requestledger.Reservation(defaults)
		if reservationErr != nil {
			t.Fatal(reservationErr)
		}
		defaultConsumed += resident + reserved
	}
	const ordinary = multiGroupRF3LedgerCapacityBytes - multiGroupRF3LedgerCleanupReserveBytes
	if consumed > ordinary || defaultConsumed <= ordinary {
		t.Fatalf("fixture reservation=%d default=%d ordinary=%d", consumed, defaultConsumed, ordinary)
	}
	// Capacity correction must not bypass the contiguous issuer contract.
	if _, err := requestledger.AdmitIssuerSequence(highwater, key2, head2.RequestDigest, highwater.Revision+1); err == nil {
		t.Fatal("issuer gap accepted")
	}
	highwater, err = requestledger.AdmitIssuerSequence(highwater, key1, head1.RequestDigest, highwater.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := requestledger.AdmitIssuerSequence(highwater, key2, head2.RequestDigest, highwater.Revision+1); err != nil {
		t.Fatalf("contiguous second request rejected: %v", err)
	}
}
