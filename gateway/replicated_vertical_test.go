package gateway

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

type logicalRF3Result struct {
	outcome    raftserve.Outcome
	completion []byte
	command    []byte
}

type logicalRF3Cluster struct {
	mu sync.Mutex

	states       map[uint64]shardservice.ReplicatedMemberState
	leader       uint64
	nextIndex    uint64
	openEpoch    uint64
	failMutation bool
	results      map[replication.Digest]logicalRF3Result
	attempts     [][]byte
	applications int
	kill         func()
}

func TestNativeSessionThreeEndpointFailoverRetriesExactBytesWithoutDuplicateApply(t *testing.T) {
	route, _, initial := testReplicatedRouteCommand(t)
	cluster := &logicalRF3Cluster{
		states: make(map[uint64]shardservice.ReplicatedMemberState, 3),
		leader: 2, nextIndex: 99, openEpoch: 100, failMutation: true,
		results: make(map[replication.Digest]logicalRF3Result),
	}
	listeners := make([]net.Listener, 3)
	serverDone := make([]chan error, 3)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for index := range route.Replicas {
		member := route.Replicas[index].Member
		state := initial[route.Replicas[index].Address]
		cluster.states[member] = state
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[index] = listener
		route.Replicas[index].Address = listener.Addr().String()
		serverDone[index] = make(chan error, 1)
	}
	cluster.kill = func() { _ = listeners[1].Close() }
	for index := range route.Replicas {
		member := route.Replicas[index].Member
		listener := listeners[index]
		go func() {
			serverDone[index] <- serveLogicalRF3Endpoint(ctx, listener, member, cluster)
		}()
	}
	t.Cleanup(func() {
		cancel()
		for _, listener := range listeners {
			_ = listener.Close()
		}
		for _, done := range serverDone {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("logical RF3 endpoint did not stop")
			}
		}
	})

	executor, err := NewReplicatedExecutor(TCPReplicatedClient{Dial: func(
		ctx context.Context,
		address string,
	) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "tcp", address)
	}}, 4, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewNativeSession(NativeSessionOptions{
		Executor: executor, Route: route, Distribution: "orders", Shard: "0000-ffff",
		Tenant: []byte("tenant"), ClientID: replication.ID128{0x91},
		ProposalCapability: serviceauthz.CapabilityDataWrite,
		Resolver:           BaseRelationResolver{Relation: 1},
		MaxRelationBatches: 4, MaxMutations: 8,
		InitialCommandBytes: 512, MaxCommandBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	requestCtx, requestCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer requestCancel()
	requestAuthority := serviceauthz.Authority{Generation: 1}
	requestAuthority.Node[0] = 1
	requestCtx, err = serviceauthz.WithAuthority(requestCtx, requestAuthority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Open(requestCtx, 2_000_000_000_000_000_000); err != nil {
		t.Fatalf("session open: %v", err)
	}
	put, err := session.Put(
		requestCtx, []byte{1, 2, 3}, []byte(`{"id":1,"state":"paid"}`),
	)
	if err != nil {
		t.Fatalf("Put through leader loss: %v", err)
	}
	if put.Completion.ResultCode != replicatedstate.ResultApplied || session.Status().Pending {
		t.Fatalf("Put result = %+v status = %+v", put, session.Status())
	}
	if _, err := session.Delete(requestCtx, []byte{1, 2, 3}); err != nil {
		t.Fatalf("Delete on replacement leader: %v", err)
	}

	cluster.mu.Lock()
	attempts := make([][]byte, len(cluster.attempts))
	for index := range cluster.attempts {
		attempts[index] = append([]byte(nil), cluster.attempts[index]...)
	}
	applications := cluster.applications
	leader := cluster.leader
	cluster.mu.Unlock()
	if leader != 3 || len(attempts) != 3 || applications != 2 ||
		!bytes.Equal(attempts[0], attempts[1]) || bytes.Equal(attempts[1], attempts[2]) {
		t.Fatalf("leader=%d attempts=%d applications=%d", leader, len(attempts), applications)
	}
}

func serveLogicalRF3Endpoint(
	ctx context.Context,
	listener net.Listener,
	member uint64,
	cluster *logicalRF3Cluster,
) error {
	for {
		connection, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go func() {
			defer connection.Close()
			request, decodeErr := shardservice.DecodeReplicatedRequest(connection)
			if decodeErr != nil {
				return
			}
			response, drop := cluster.execute(member, request)
			if drop {
				return
			}
			_ = shardservice.EncodeReplicatedResponse(connection, response)
		}()
	}
}

func (cluster *logicalRF3Cluster) execute(
	member uint64,
	request *shardservice.ReplicatedRequest,
) (*shardservice.ReplicatedResponse, bool) {
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	state := cluster.states[member]
	if request.Operation == shardservice.ReplicatedProbe {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedHandshake, HasState: true, State: state,
		}, false
	}
	if request.Fence != state.Fence {
		return &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalStaleFence,
			HasState: true, State: state,
		}, false
	}
	if member != cluster.leader {
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedNotLeader, HasState: true, State: state,
		}, false
	}
	command, err := replication.OpenCommand(request.Command)
	if err != nil {
		return &shardservice.ReplicatedResponse{
			Kind:     shardservice.ReplicatedRefusal,
			Refusal:  shardservice.ReplicatedRefusalUnavailable,
			HasState: true, State: state,
		}, false
	}
	if command.Kind() == replication.CommandMutationBatch {
		cluster.attempts = append(cluster.attempts, append([]byte(nil), request.Command...))
	}
	if prior, ok := cluster.results[command.Fingerprint]; ok &&
		bytes.Equal(prior.command, request.Command) {
		state = cluster.states[member]
		return &shardservice.ReplicatedResponse{
			Kind: shardservice.ReplicatedCompletion, HasState: true, State: state,
			RequestDigest: replicatedRequestDigest(request.Command),
			Outcome:       prior.outcome, Completion: append([]byte(nil), prior.completion...),
		}, false
	}

	cluster.nextIndex++
	clientEpoch := command.ClientEpoch
	appliedSequence := cluster.nextIndex
	resultCode := uint32(replicatedstate.ResultApplied)
	if command.Kind() == replication.CommandSessionOpen {
		clientEpoch = cluster.openEpoch
		appliedSequence = cluster.openEpoch
		resultCode = replicatedstate.ResultSessionOpened
	} else if command.Kind() == replication.CommandMutationBatch {
		cluster.applications++
	}
	completion, err := appendNativeSessionCompletion(
		nil, command, clientEpoch, appliedSequence, resultCode,
	)
	if err != nil {
		panic(err)
	}
	outcome := raftserve.Outcome{
		Code: raftserve.OutcomeCompletion, AppliedIndex: cluster.nextIndex,
		CompletionAppliedSequence: appliedSequence, CompletionBytes: len(completion),
	}
	cluster.results[command.Fingerprint] = logicalRF3Result{
		outcome: outcome, completion: append([]byte(nil), completion...),
		command: append([]byte(nil), request.Command...),
	}
	for replica, current := range cluster.states {
		current.Commit = cluster.nextIndex
		current.Applied = cluster.nextIndex
		current.CheckpointApplied = cluster.nextIndex
		cluster.states[replica] = current
	}
	if command.Kind() == replication.CommandMutationBatch && cluster.failMutation {
		cluster.failMutation = false
		cluster.leader = 3
		for replica, current := range cluster.states {
			current.LeaderID = cluster.leader
			current.Fence.Term++
			cluster.states[replica] = current
		}
		cluster.kill()
		return nil, true
	}
	state = cluster.states[member]
	return &shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedCompletion, HasState: true, State: state,
		RequestDigest: replicatedRequestDigest(request.Command),
		Outcome:       outcome, Completion: completion,
	}, false
}
