package shardservice

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
)

type fakeReplicatedOwner struct {
	state  raftservice.ServingState
	result raftservice.Result
	err    error
}

func (owner *fakeReplicatedOwner) Probe(
	context.Context,
	raftmember.GroupKey,
) (raftservice.ServingState, error) {
	return owner.state, nil
}

func (owner *fakeReplicatedOwner) Submit(
	context.Context,
	raftservice.ServingFence,
	[]byte,
) (raftservice.Result, error) {
	return owner.result, owner.err
}

func TestReplicatedServerRoundTripCompletionAndNotLeader(t *testing.T) {
	fence := testReplicatedFence()
	command := testReplicatedCommand(t, fence)
	completion := testReplicatedCompletion(t, fence, 8)
	state := raftservice.ServingState{
		Identity: raftmember.RuntimeIdentity{
			Group: fence.Group, AllocationGeneration: fence.AllocationGeneration,
			MemberID: fence.MemberID, StoreID: fence.StoreID,
			NodeIncarnation: fence.NodeIncarnation,
		},
		Command: fence.Command,
		Status: raftmember.RuntimeStatus{
			MemberID: fence.MemberID, LeaderID: fence.MemberID, Term: fence.Term,
			Commit: 9, Applied: 8, CheckpointApplied: 7,
		},
	}
	tests := []struct {
		name   string
		owner  *fakeReplicatedOwner
		kind   ReplicatedResponseKind
		result []byte
	}{
		{name: "completion", owner: &fakeReplicatedOwner{state: state,
			result: raftservice.Result{Outcome: raftserve.Outcome{
				Code: raftserve.OutcomeCompletion, AppliedIndex: 8,
				CompletionAppliedSequence: 8, CompletionBytes: len(completion),
			}, Completion: completion}}, kind: ReplicatedCompletion, result: completion},
		{name: "not_leader", owner: &fakeReplicatedOwner{state: state,
			err: &raftservice.NotLeaderError{Status: state.Status}}, kind: ReplicatedNotLeader},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &ReplicatedServer{owner: test.owner}
			request := &ReplicatedRequest{
				Operation: ReplicatedPropose, Fence: fence, Command: command,
			}
			candidate := server.executeReplicated(context.Background(), request)
			var preflight bytes.Buffer
			if err := EncodeReplicatedResponse(&preflight, candidate); err != nil {
				t.Fatalf("server response preflight %+v: %v", candidate, err)
			}
			client, peer := net.Pipe()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- server.ServeReplicatedConn(ctx, peer) }()
			response, err := RoundTripReplicated(ctx, client, request)
			if err != nil {
				_ = client.Close()
				cancel()
				select {
				case serverErr := <-done:
					t.Fatalf("round trip: %v; server: %v", err, serverErr)
				case <-time.After(time.Second):
					t.Fatalf("round trip: %v; server did not stop", err)
				}
			}
			if response.Kind != test.kind || !bytes.Equal(response.Completion, test.result) {
				t.Fatalf("response = %+v", response)
			}
			_ = client.Close()
			cancel()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, net.ErrClosed) &&
					!errors.Is(err, context.Canceled) && !errors.Is(err, io.ErrClosedPipe) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("replicated server connection leaked")
			}
		})
	}
}

func TestReplicatedRoundTripTransportFailureRetainsExactRetryBytes(t *testing.T) {
	fence := testReplicatedFence()
	command := testReplicatedCommand(t, fence)
	client, peer := net.Pipe()
	_ = peer.Close()
	_, err := RoundTripReplicated(context.Background(), client, &ReplicatedRequest{
		Operation: ReplicatedPropose, Fence: fence, Command: command,
	})
	var unknown *raftservice.UnknownOutcomeError
	if !errors.As(err, &unknown) || !bytes.Equal(unknown.Command, command) {
		t.Fatalf("transport error = %T %v", err, err)
	}
}

func TestReplicatedServerBoundsConnectionsWithoutUserSpaceQueue(t *testing.T) {
	server := &ReplicatedServer{owner: &fakeReplicatedOwner{}}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener, 1) }()
	first, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	for deadline := time.Now().Add(time.Second); server.Stats().Active != 1; {
		if time.Now().After(deadline) {
			t.Fatalf("first connection stats = %+v", server.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	second, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_ = second.SetReadDeadline(time.Now().Add(time.Second))
	var one [1]byte
	if _, err := second.Read(one[:]); err == nil {
		t.Fatal("connection above exact bound remained open")
	}
	_ = second.Close()
	for deadline := time.Now().Add(time.Second); server.Stats().Rejected != 1; {
		if time.Now().After(deadline) {
			t.Fatalf("rejection stats = %+v", server.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bounded replicated server did not stop")
	}
}
