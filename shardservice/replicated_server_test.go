package shardservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

type fakeReplicatedOwner struct {
	state                raftservice.ServingState
	result               raftservice.Result
	err                  error
	blockSubmit          bool
	membershipErr        error
	membership           raftservice.MembershipRequest
	readResult           raftservice.PointReadResult
	readErr              error
	readLease            raftservice.PointReadLease
	readCalled           chan struct{}
	transactionResult    raftservice.TransactionReadResult
	transactionErr       error
	transactionLease     raftservice.TransactionReadLease
	transactionRequest   raftservice.TransactionReadRequest
	transactionCalled    chan struct{}
	requestLedgerResult  raftservice.RequestLedgerReadResult
	requestLedgerErr     error
	requestLedgerLease   raftservice.RequestLedgerReadLease
	requestLedgerRequest raftservice.RequestLedgerReadRequest
	probeCalls           atomic.Uint64
}

func (owner *fakeReplicatedOwner) ApplyMembership(
	_ context.Context,
	request raftservice.MembershipRequest,
) error {
	owner.membership = request
	return owner.membershipErr
}

func (owner *fakeReplicatedOwner) Probe(
	context.Context,
	raftmember.GroupKey,
) (raftservice.ServingState, error) {
	owner.probeCalls.Add(1)
	return owner.state, nil
}

func (owner *fakeReplicatedOwner) SubmitOwned(
	ctx context.Context,
	_ raftservice.ServingFence,
	_ []byte,
) (raftservice.Result, error) {
	if owner.blockSubmit {
		<-ctx.Done()
		return raftservice.Result{}, context.Cause(ctx)
	}
	return owner.result, owner.err
}

func (owner *fakeReplicatedOwner) ReadPoint(
	context.Context,
	raftservice.PointReadRequest,
) (raftservice.PointReadResult, raftservice.PointReadLease, error) {
	if owner.readCalled != nil {
		select {
		case <-owner.readCalled:
		default:
			close(owner.readCalled)
		}
	}
	return owner.readResult, owner.readLease, owner.readErr
}

func (owner *fakeReplicatedOwner) ReadTransaction(
	_ context.Context,
	request raftservice.TransactionReadRequest,
) (raftservice.TransactionReadResult, raftservice.TransactionReadLease, error) {
	owner.transactionRequest = request
	if owner.transactionCalled != nil {
		select {
		case <-owner.transactionCalled:
		default:
			close(owner.transactionCalled)
		}
	}
	return owner.transactionResult, owner.transactionLease, owner.transactionErr
}

func (owner *fakeReplicatedOwner) ReadRequestLedger(
	_ context.Context,
	request raftservice.RequestLedgerReadRequest,
) (raftservice.RequestLedgerReadResult, raftservice.RequestLedgerReadLease, error) {
	owner.requestLedgerRequest = request
	return owner.requestLedgerResult, owner.requestLedgerLease, owner.requestLedgerErr
}

type testPointReadLease struct{ released atomic.Bool }

func (lease *testPointReadLease) Release() { lease.released.Store(true) }

func testReplicatedServingState() raftservice.ServingState {
	fence := testReplicatedFence()
	return raftservice.ServingState{
		Identity: raftmember.RuntimeIdentity{Group: fence.Group,
			AllocationGeneration: fence.AllocationGeneration, MemberID: fence.MemberID,
			StoreID: fence.StoreID, NodeIncarnation: fence.NodeIncarnation},
		Command: fence.Command,
		Status: raftmember.RuntimeStatus{MemberID: fence.MemberID, LeaderID: fence.MemberID,
			Term: fence.Term, Commit: 11, Applied: 11, CheckpointApplied: 10},
	}
}

func TestReplicatedServerServesFoundEmptyReadWithoutConflatingMiss(t *testing.T) {
	state := testReplicatedServingState()
	owner := &fakeReplicatedOwner{state: state, readResult: raftservice.PointReadResult{
		Applied: 11, Found: true, Value: []byte{},
	}}
	server := &ReplicatedServer{owner: owner}
	request := &ReplicatedRequest{Operation: ReplicatedReadFollower,
		Fence: replicatedWireState(state).Fence, Relation: 1, Key: []byte("k"),
		MinimumApplied: 10, MaxValueBytes: 1024}
	response := server.executeReplicated(context.Background(), request)
	if response.Kind != ReplicatedReadFound || response.ReadApplied != 11 ||
		len(response.Value) != 0 || !validReplicatedResponse(response) {
		t.Fatalf("response=%+v", response)
	}
	owner.readResult.Found = false
	response = server.executeReplicated(context.Background(), request)
	if response.Kind != ReplicatedReadMissing || response.ReadApplied != 11 ||
		!validReplicatedResponse(response) {
		t.Fatalf("miss response=%+v", response)
	}
}

func TestReplicatedServerPreservesTypedPointReadBounds(t *testing.T) {
	state := testReplicatedServingState()
	for _, test := range []struct {
		name    string
		err     error
		refusal ReplicatedRefusalCode
	}{{"future-applied-floor", replicatedstate.ErrReadBehind, ReplicatedRefusalReadBehind},
		{"response-buffer", replicatedstate.ErrReadBufferBound, ReplicatedRefusalReadBufferBound},
		{"active-intent", replicatedstate.ErrTransactionIntentActive, ReplicatedRefusalReadIntentActive}} {
		t.Run(test.name, func(t *testing.T) {
			owner := &fakeReplicatedOwner{state: state, readErr: test.err}
			server := &ReplicatedServer{owner: owner}
			request := &ReplicatedRequest{Operation: ReplicatedReadLeader,
				Fence: replicatedWireState(state).Fence, Relation: 1, Key: []byte("k"),
				MinimumApplied: state.Status.Commit + 1, MaxValueBytes: 1024}
			response := server.executeReplicated(context.Background(), request)
			if response.Kind != ReplicatedRefusal || response.Refusal != test.refusal ||
				!response.HasState || response.State.Applied != state.Status.Applied ||
				!validReplicatedResponse(response) {
				t.Fatalf("response=%+v", response)
			}
		})
	}
}

func TestReplicatedServerHoldsReadLeaseUntilSlowClientAcceptsFrame(t *testing.T) {
	state := testReplicatedServingState()
	lease := &testPointReadLease{}
	called := make(chan struct{})
	owner := &fakeReplicatedOwner{state: state, readCalled: called,
		readResult: raftservice.PointReadResult{Applied: 11, Found: true,
			Value: bytes.Repeat([]byte{7}, 64<<10)}, readLease: lease}
	server := testReplicatedServer(owner)
	serverSide, clientSide := net.Pipe()
	defer serverSide.Close()
	defer clientSide.Close()
	done := make(chan error, 1)
	go func() { done <- server.serveReplicatedRequest(context.Background(), serverSide) }()
	request := &ReplicatedRequest{Operation: ReplicatedReadFollower,
		Fence: replicatedWireState(state).Fence, Relation: 1, Key: []byte("k"),
		MinimumApplied: 10, MaxValueBytes: 64 << 10}
	if err := EncodeReplicatedRequest(clientSide, request); err != nil {
		t.Fatal(err)
	}
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("read was not admitted")
	}
	if lease.released.Load() {
		t.Fatal("response reservation released before the blocked socket write")
	}
	response, err := DecodeReplicatedResponse(clientSide)
	if err != nil || response.Kind != ReplicatedReadFound || len(response.Value) != 64<<10 {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !lease.released.Load() {
		t.Fatal("response reservation not released after socket write")
	}
}

func testReplicatedServer(owner replicatedOwner) *ReplicatedServer {
	return &ReplicatedServer{owner: owner, requestTimeout: time.Second,
		frames: replicatedFrameByteBudget{limit: 1 << 20}}
}

func TestReplicatedServerMembershipTypedRefusals(t *testing.T) {
	fence := testReplicatedFence()
	state := raftservice.ServingState{Identity: raftmember.RuntimeIdentity{
		Group: fence.Group, AllocationGeneration: fence.AllocationGeneration,
		MemberID: fence.MemberID, StoreID: fence.StoreID, NodeIncarnation: fence.NodeIncarnation},
		Command: fence.Command, Status: raftmember.RuntimeStatus{MemberID: fence.MemberID,
			LeaderID: fence.MemberID, Term: fence.Term, Commit: 3, Applied: 3}}
	membership := ReplicatedMembershipRequest{Kind: raftservice.MembershipAddLearner,
		TransitionID: [16]byte{1}, MetadataEpoch: 2, CatalogGeneration: 3,
		ExpectedReplicaSetVersion: fence.Command.ReplicaSetVersion,
		SourceMember:              fence.MemberID, TargetMember: fence.MemberID + 1}
	tests := []struct {
		err  error
		kind ReplicatedResponseKind
		code ReplicatedRefusalCode
	}{
		{kind: ReplicatedMembershipAccepted},
		{err: raftservice.ErrMembershipUnauthorized, kind: ReplicatedRefusal,
			code: ReplicatedRefusalMembershipUnauthorized},
		{err: raftservice.ErrMembershipStale, kind: ReplicatedRefusal,
			code: ReplicatedRefusalMembershipStale},
		{err: raftservice.ErrMembershipMalformed, kind: ReplicatedRefusal,
			code: ReplicatedRefusalMembershipMalformed},
		{err: raftservice.ErrMembershipNotCaughtUp, kind: ReplicatedRefusal,
			code: ReplicatedRefusalMembershipNotCaughtUp},
	}
	for _, test := range tests {
		owner := &fakeReplicatedOwner{state: state, membershipErr: test.err}
		response := testReplicatedServer(owner).executeReplicated(context.Background(),
			&ReplicatedRequest{Operation: ReplicatedMembership, Fence: fence, Membership: membership})
		if response.Kind != test.kind || response.Refusal != test.code ||
			owner.membership.ExpectedReplicaSetVersion != membership.ExpectedReplicaSetVersion {
			t.Fatalf("err %v response=%+v request=%+v", test.err, response, owner.membership)
		}
	}
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
			server := testReplicatedServer(test.owner)
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
			wantDigest := [32]byte{}
			if test.kind == ReplicatedCompletion {
				wantDigest = sha256.Sum256(command)
			}
			if response.Kind != test.kind || response.RequestDigest != wantDigest ||
				!bytes.Equal(response.Completion, test.result) {
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

func TestReplicatedServerClosesEveryOwnerOutcomeOnTheWire(t *testing.T) {
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
		Status: raftmember.RuntimeStatus{MemberID: fence.MemberID,
			LeaderID: fence.MemberID, Term: fence.Term,
			Commit: 8, Applied: 8, CheckpointApplied: 7},
	}
	for code := raftserve.OutcomePending; code <= raftserve.OutcomeProposalAbandoned; code++ {
		result := raftservice.Result{Outcome: raftserve.Outcome{Code: code}}
		var ownerErr error
		want := ReplicatedRefusal
		wantRefusal := ReplicatedRefusalUnavailable
		switch {
		case code == raftserve.OutcomeCompletion:
			result.Outcome.AppliedIndex = 8
			result.Outcome.CompletionAppliedSequence = 8
			result.Outcome.CompletionBytes = len(completion)
			result.Completion = completion
			want = ReplicatedCompletion
			wantRefusal = ReplicatedRefusalNone
		case code > raftserve.OutcomeCompletion && code < raftserve.OutcomeProposalRefused:
			result.Outcome.AppliedIndex = 8
			ownerErr = result.Outcome.Err()
			wantRefusal = ReplicatedRefusalDeterministic
		case code == raftserve.OutcomeProposalRefused:
			ownerErr = result.Outcome.Err()
			wantRefusal = ReplicatedRefusalProposalRefused
		case code == raftserve.OutcomeNotLeader:
			ownerErr = result.Outcome.Err()
			want = ReplicatedNotLeader
			wantRefusal = ReplicatedRefusalNone
		case code == raftserve.OutcomeProposalAbandoned:
			ownerErr = result.Outcome.Err()
			want = ReplicatedOutcomeUnknown
			wantRefusal = ReplicatedRefusalNone
		default:
			ownerErr = errors.New("owner returned no terminal outcome")
		}
		server := testReplicatedServer(&fakeReplicatedOwner{
			state: state, result: result, err: ownerErr,
		})
		response := server.executeReplicated(context.Background(), &ReplicatedRequest{
			Operation: ReplicatedPropose, Fence: fence, Command: command,
		})
		var encoded bytes.Buffer
		if err := EncodeReplicatedResponse(&encoded, response); err != nil {
			t.Fatalf("outcome %d did not encode: response=%+v error=%v", code, response, err)
		}
		decoded, err := DecodeReplicatedResponse(&encoded)
		if err != nil || decoded.Kind != want || decoded.Refusal != wantRefusal {
			t.Fatalf("outcome %d = %+v error=%v want kind/refusal %d/%d",
				code, decoded, err, want, wantRefusal)
		}
	}
}

func TestReplicatedServerCountsExceptionalProposalDowngrades(t *testing.T) {
	fence := testReplicatedFence()
	command := testReplicatedCommand(t, fence)
	completion := testReplicatedCompletion(t, fence, 8)
	state := testReplicatedServingState()
	state.Status.Commit = 8
	state.Status.Applied = 8
	state.Status.CheckpointApplied = 7
	request := &ReplicatedRequest{
		Operation: ReplicatedPropose, Fence: fence, Command: command,
	}

	t.Run("invalid completion exposes exact predicate", func(t *testing.T) {
		server := testReplicatedServer(&fakeReplicatedOwner{state: state,
			result: raftservice.Result{Outcome: raftserve.Outcome{
				Code: raftserve.OutcomeCompletion, AppliedIndex: 9,
				CompletionAppliedSequence: 8, CompletionBytes: len(completion) - 1,
			}, Completion: completion}})
		response := server.executeReplicated(context.Background(), request)
		stats := server.Stats()
		if response.Kind != ReplicatedOutcomeUnknown || stats.ProposalInvalidCompletion != 1 ||
			stats.ProposalInvalidCompletionReasons != ReplicatedCompletionInvalidCompletionBytes {
			t.Fatalf("response=%+v stats=%+v", response, stats)
		}
	})

	t.Run("invalid result grammar is outcome unknown", func(t *testing.T) {
		malformed := testReplicatedCompletionWithResult(
			t, fence, 8, replicatedstate.ResultApplied,
			replicatedstate.ResultFormatMutation, nil,
		)
		server := testReplicatedServer(&fakeReplicatedOwner{state: state,
			result: raftservice.Result{Outcome: raftserve.Outcome{
				Code: raftserve.OutcomeCompletion, AppliedIndex: 8,
				CompletionAppliedSequence: 8, CompletionBytes: len(malformed),
			}, Completion: malformed}})
		response := server.executeReplicated(context.Background(), request)
		stats := server.Stats()
		if response.Kind != ReplicatedOutcomeUnknown || stats.ProposalInvalidCompletion != 1 ||
			stats.ProposalInvalidCompletionReasons != ReplicatedCompletionInvalidResult {
			t.Fatalf("response=%+v stats=%+v", response, stats)
		}
	})

	t.Run("settled completion advances response watermarks", func(t *testing.T) {
		owner := &fakeReplicatedOwner{state: state,
			result: raftservice.Result{Outcome: raftserve.Outcome{
				Code: raftserve.OutcomeCompletion, AppliedIndex: 9,
				CompletionAppliedSequence: 8, CompletionBytes: len(completion),
			}, Completion: completion}}
		server := testReplicatedServer(owner)
		response := server.executeReplicated(context.Background(), request)
		if response.Kind != ReplicatedCompletion || response.State.Applied != 9 ||
			response.State.Commit != 9 || !validReplicatedResponse(response) ||
			owner.probeCalls.Load() != 1 {
			t.Fatalf("response=%+v stats=%+v", response, server.Stats())
		}
	})

	t.Run("owner unknown remains distinct", func(t *testing.T) {
		server := testReplicatedServer(&fakeReplicatedOwner{
			state: state, err: raftservice.ErrOutcomeUnknown,
		})
		response := server.executeReplicated(context.Background(), request)
		stats := server.Stats()
		if response.Kind != ReplicatedOutcomeUnknown || stats.ProposalUnknownSubmit != 1 ||
			stats.ProposalUnknownAbandoned != 0 || stats.ProposalInvalidCompletion != 0 {
			t.Fatalf("response=%+v stats=%+v", response, stats)
		}
	})

	t.Run("abandoned remains distinct", func(t *testing.T) {
		server := testReplicatedServer(&fakeReplicatedOwner{state: state,
			result: raftservice.Result{Outcome: raftserve.Outcome{
				Code: raftserve.OutcomeProposalAbandoned,
			}}, err: raftserve.ErrProposalAbandoned})
		response := server.executeReplicated(context.Background(), request)
		stats := server.Stats()
		if response.Kind != ReplicatedOutcomeUnknown || stats.ProposalUnknownAbandoned != 1 ||
			stats.ProposalUnknownSubmit != 0 || stats.ProposalInvalidCompletion != 0 {
			t.Fatalf("response=%+v stats=%+v", response, stats)
		}
	})

	t.Run("pre-admission bound remains definite", func(t *testing.T) {
		outcome := raftserve.Outcome{Code: raftserve.OutcomeAdmissionBound}
		server := testReplicatedServer(&fakeReplicatedOwner{
			state: state, result: raftservice.Result{Outcome: outcome}, err: outcome.Err(),
		})
		response := server.executeReplicated(context.Background(), request)
		stats := server.Stats()
		if response.Kind != ReplicatedRefusal ||
			response.Refusal != ReplicatedRefusalAdmissionBound ||
			response.Outcome != (raftserve.Outcome{}) ||
			stats.ProposalInvalidDeterministic != 0 || !validReplicatedResponse(response) {
			t.Fatalf("response=%+v stats=%+v", response, stats)
		}
	})

	t.Run("invalid deterministic refusal remains distinct", func(t *testing.T) {
		outcome := raftserve.Outcome{
			Code: raftserve.OutcomeRequestConflict, AppliedIndex: 9, CompletionBytes: 1,
		}
		server := testReplicatedServer(&fakeReplicatedOwner{
			state: state, result: raftservice.Result{Outcome: outcome}, err: outcome.Err(),
		})
		response := server.executeReplicated(context.Background(), request)
		stats := server.Stats()
		if response.Kind != ReplicatedOutcomeUnknown || stats.ProposalInvalidDeterministic != 1 ||
			stats.ProposalUnknownSubmit != 0 || stats.ProposalInvalidCompletion != 0 ||
			stats.ProposalInvalidDeterministicReasons != ReplicatedDeterministicInvalidCompletionBytes ||
			stats.ProposalInvalidDeterministicCode != raftserve.OutcomeRequestConflict ||
			stats.ProposalInvalidDeterministicApplied != 9 ||
			stats.ProposalInvalidDeterministicState != 9 {
			t.Fatalf("response=%+v stats=%+v", response, stats)
		}
	})

	t.Run("settled deterministic refusal advances response watermarks", func(t *testing.T) {
		outcome := raftserve.Outcome{Code: raftserve.OutcomeRequestConflict, AppliedIndex: 9}
		server := testReplicatedServer(&fakeReplicatedOwner{
			state: state, result: raftservice.Result{Outcome: outcome}, err: outcome.Err(),
		})
		response := server.executeReplicated(context.Background(), request)
		if response.Kind != ReplicatedRefusal ||
			response.Refusal != ReplicatedRefusalDeterministic ||
			response.State.Applied != 9 || response.State.Commit != 9 ||
			!validReplicatedResponse(response) {
			t.Fatalf("response=%+v stats=%+v", response, server.Stats())
		}
	})
}

func TestReplicatedFrameBudgetRejectsBeforeAllocationAndRecovers(t *testing.T) {
	budget := &replicatedFrameByteBudget{limit: 8}
	type pipePair struct {
		reader *io.PipeReader
		writer *io.PipeWriter
	}
	pairs := make([]pipePair, 2)
	decodeErrors := make(chan error, 2)
	headerWrites := make(chan error, 2)
	frame := rawFrame(tagReplicatedRequest, []byte{1, 2, 3, 4})
	for index := range pairs {
		pairs[index].reader, pairs[index].writer = io.Pipe()
		pair := pairs[index]
		go func() {
			_, _, err := readFrameBudgeted(pair.reader, tagReplicatedRequest, budget)
			decodeErrors <- err
		}()
		go func() {
			_, err := pair.writer.Write(frame[:5])
			headerWrites <- err
		}()
	}
	for range pairs {
		if err := <-headerWrites; err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for budget.used.Load() != 8 {
		if time.Now().After(deadline) {
			t.Fatalf("reserved bytes = %d", budget.used.Load())
		}
		time.Sleep(time.Millisecond)
	}
	if _, _, err := readFrameBudgeted(
		bytes.NewReader(rawFrame(tagReplicatedRequest, []byte{9})),
		tagReplicatedRequest, budget,
	); !errors.Is(err, errFrameBudget) {
		t.Fatalf("one-over-budget decode = %v", err)
	}
	for _, pair := range pairs {
		_ = pair.writer.CloseWithError(io.ErrUnexpectedEOF)
	}
	for range pairs {
		if err := <-decodeErrors; !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("truncated reserved frame = %v", err)
		}
	}
	if budget.used.Load() != 0 {
		t.Fatalf("released bytes = %d", budget.used.Load())
	}
	body, charged, err := readFrameBudgeted(
		bytes.NewReader(frame), tagReplicatedRequest, budget,
	)
	if err != nil || charged != 4 || !bytes.Equal(body, []byte{1, 2, 3, 4}) {
		t.Fatalf("recovered decode = %v charged=%d body=%v", err, charged, body)
	}
	budget.release(charged)
	if budget.used.Load() != 0 {
		t.Fatalf("final released bytes = %d", budget.used.Load())
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
	server := testReplicatedServer(&fakeReplicatedOwner{})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.ServeLoopbackDevelopment(ctx, listener, 1) }()
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

func TestReplicatedServerRequestDeadlineBoundsSlowlorisConnection(t *testing.T) {
	server := testReplicatedServer(&fakeReplicatedOwner{})
	server.requestTimeout = 20 * time.Millisecond
	client, peer := net.Pipe()
	defer client.Close()
	done := make(chan error, 1)
	go func() {
		done <- server.ServeReplicatedConn(context.Background(), peer)
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("slowloris connection ended without its deadline error")
		}
	case <-time.After(time.Second):
		t.Fatal("slowloris connection exceeded bounded request deadline")
	}
	if stats := server.Stats(); stats.InFlightFrameBytes != 0 {
		t.Fatalf("slowloris retained frame bytes: %+v", stats)
	}
}

func TestReplicatedServerRequestDeadlineBoundsOwnerAndReleasesFrame(t *testing.T) {
	fence := testReplicatedFence()
	command := testReplicatedCommand(t, fence)
	state := raftservice.ServingState{
		Identity: raftmember.RuntimeIdentity{
			Group: fence.Group, AllocationGeneration: fence.AllocationGeneration,
			MemberID: fence.MemberID, StoreID: fence.StoreID,
			NodeIncarnation: fence.NodeIncarnation,
		},
		Command: fence.Command,
		Status: raftmember.RuntimeStatus{MemberID: fence.MemberID,
			LeaderID: fence.MemberID, Term: fence.Term,
			Commit: 8, Applied: 8, CheckpointApplied: 7},
	}
	server := testReplicatedServer(&fakeReplicatedOwner{state: state, blockSubmit: true})
	server.requestTimeout = 20 * time.Millisecond
	client, peer := net.Pipe()
	defer client.Close()
	serverDone := make(chan error, 1)
	go func() {
		defer peer.Close()
		serverDone <- server.ServeReplicatedConn(context.Background(), peer)
	}()
	started := time.Now()
	response, clientErr := RoundTripReplicated(context.Background(), client, &ReplicatedRequest{
		Operation: ReplicatedPropose, Fence: fence, Command: command,
	})
	if clientErr == nil {
		if response == nil || response.Kind != ReplicatedOutcomeUnknown {
			t.Fatalf("timed-out proposal response = %+v", response)
		}
	} else if !errors.Is(clientErr, raftservice.ErrOutcomeUnknown) {
		t.Fatalf("timed-out proposal error = %T %v", clientErr, clientErr)
	}
	select {
	case err := <-serverDone:
		if err == nil {
			t.Fatal("owner timeout ended without transport deadline error")
		}
	case <-time.After(time.Second):
		t.Fatal("owner call exceeded bounded request deadline")
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("owner timeout took %v", elapsed)
	}
	if stats := server.Stats(); stats.InFlightFrameBytes != 0 {
		t.Fatalf("owner timeout retained frame bytes: %+v", stats)
	}
}
