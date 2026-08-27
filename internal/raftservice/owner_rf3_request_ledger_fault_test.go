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
