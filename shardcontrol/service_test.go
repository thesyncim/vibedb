package shardcontrol

import (
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

type testPeerConnection struct {
	net.Conn
	identity rafttransport.PeerIdentity
	class    rafttransport.TrafficClass
}

func (connection testPeerConnection) PeerIdentity() rafttransport.PeerIdentity {
	return connection.identity
}
func (connection testPeerConnection) TrafficClass() rafttransport.TrafficClass {
	return connection.class
}

type testExecutor struct{ calls int }

func (executor *testExecutor) ExecuteControl(
	_ context.Context, _ rafttransport.PeerIdentity, request Request,
) (Response, error) {
	executor.calls++
	digest := sha256.Sum256(append(request.Step[:], request.Payload...))
	return Response{
		Code: ResultAccepted, Operation: request.Operation, Step: request.Step,
		ResultDigest: digest, Payload: []byte(`{"durable":true}`),
	}, nil
}

func TestAuthenticatedServiceRoundTripAndActionAuthorization(t *testing.T) {
	node := rafttransport.NodeID{1}
	authorizer, err := NewAuthorizer([]ActionGrant{{
		Node: node, Actions: 1 << uint(ActionSealSource-1),
	}})
	if err != nil {
		t.Fatal(err)
	}
	executor := new(testExecutor)
	server, err := NewServer(authorizer, executor)
	if err != nil {
		t.Fatal(err)
	}
	dial := func(_ context.Context, _ string) (net.Conn, error) {
		client, rawServer := net.Pipe()
		connection := testPeerConnection{Conn: rawServer,
			identity: rafttransport.PeerIdentity{Node: node},
			class:    rafttransport.TrafficShardControl,
		}
		go func() {
			_ = server.ServeConnection(context.Background(), connection)
			_ = rawServer.Close()
		}()
		return client, nil
	}
	client, err := NewClient(dial, "member-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest()
	response, err := client.Execute(context.Background(), request)
	if err != nil || response.Code != ResultAccepted || executor.calls != 1 {
		t.Fatalf("response=%+v calls=%d err=%v", response, executor.calls, err)
	}

	request.Action = ActionPruneRetained
	if _, err = client.Execute(context.Background(), request); !errors.Is(err, ErrOutcomeUnknown) || executor.calls != 1 {
		t.Fatalf("unauthorized action err=%v calls=%d", err, executor.calls)
	}
}

func TestAuthorizerRejectsInvalidOrDuplicateCapabilities(t *testing.T) {
	for _, grants := range [][]ActionGrant{
		nil,
		{{Node: rafttransport.NodeID{1}}},
		{{Node: rafttransport.NodeID{1}, Actions: 1}, {Node: rafttransport.NodeID{1}, Actions: 2}},
	} {
		if _, err := NewAuthorizer(grants); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("grants=%+v err=%v", grants, err)
		}
	}
}
