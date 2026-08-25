package shardservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type replicatedTLSWriteMeter struct {
	net.Conn
	writes atomic.Uint64
}

func (meter *replicatedTLSWriteMeter) Write(p []byte) (int, error) {
	meter.writes.Add(1)
	return meter.Conn.Write(p)
}

func BenchmarkReplicatedRequestTLSOneMiB(b *testing.B) {
	fence := testReplicatedFence()
	request := &ReplicatedRequest{Operation: ReplicatedPropose, Fence: fence,
		Command: testReplicatedCommandValue(b, fence, bytes.Repeat([]byte{'x'}, 1<<20))}
	authority := newShardTLSAuthority(b)
	serverIdentity := shardPeerIdentity(19, 41)
	clientIdentity := shardPeerIdentity(19, 61)
	serverProfile := authority.profile(b, serverIdentity)
	clientProfile := authority.profile(b, clientIdentity)
	deadline := func() time.Time { return time.Now().Add(5 * time.Second) }
	benchmarks := []struct {
		name   string
		encode func(io.Writer, *ReplicatedRequest) error
	}{
		{"contiguous", EncodeReplicatedRequest},
		{"borrowed_two_write_tls", EncodeReplicatedRequestBorrowed},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				b.Fatal(err)
			}
			serverDone := make(chan error, 1)
			go func() {
				raw, err := listener.Accept()
				if err != nil {
					serverDone <- err
					return
				}
				connection, err := serverProfile.Server(context.Background(), raw, rafttransport.TrafficShardNative, deadline)
				if err != nil {
					serverDone <- err
					return
				}
				_, err = io.Copy(io.Discard, connection)
				_ = connection.Close()
				serverDone <- err
			}()
			raw, err := net.Dial("tcp", listener.Addr().String())
			if err != nil {
				b.Fatal(err)
			}
			meter := &replicatedTLSWriteMeter{Conn: raw}
			connection, err := clientProfile.Client(context.Background(), meter, serverIdentity.Node, rafttransport.TrafficShardNative, deadline)
			if err != nil {
				b.Fatal(err)
			}
			baselineWrites := meter.writes.Load()
			b.ReportAllocs()
			b.SetBytes(int64(len(request.Command)))
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if err := benchmark.encode(connection, request); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(meter.writes.Load()-baselineWrites)/float64(b.N), "tlswrites/op")
			_ = connection.Close()
			_ = listener.Close()
			if err := <-serverDone; err != nil {
				b.Fatal(err)
			}
		})
	}
}

func TestReplicatedNativeWireRoundTripAndCanonicalFences(t *testing.T) {
	fence := testReplicatedFence()
	command := testReplicatedCommand(t, fence)
	authority := serviceauthz.Authority{Node: rafttransport.NodeID{31}, Generation: 17}
	for _, request := range []*ReplicatedRequest{
		{Operation: ReplicatedProbe, Authority: authority, Fence: ReplicatedFence{
			Group: fence.Group, AllocationGeneration: fence.AllocationGeneration,
		}},
		{Operation: ReplicatedPropose, Authority: authority, Fence: fence, Command: command},
		{Operation: ReplicatedReadLeader, Authority: authority, Fence: fence, Relation: 1,
			Key: []byte{0, 1}, MinimumApplied: 7, MaxValueBytes: 4096},
		{Operation: ReplicatedReadFollower, Authority: authority, Fence: fence, Relation: 2,
			Key: []byte{2, 1, 0}, MinimumApplied: 9, MaxValueBytes: 8192},
	} {
		var encoded bytes.Buffer
		if err := EncodeReplicatedRequest(&encoded, request); err != nil {
			t.Fatal(err)
		}
		var borrowed bytes.Buffer
		if err := EncodeReplicatedRequestBorrowed(&borrowed, request); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(borrowed.Bytes(), encoded.Bytes()) {
			t.Fatal("borrowed request differs from canonical contiguous frame")
		}
		decoded, err := DecodeReplicatedRequest(&encoded)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Operation != request.Operation || decoded.Authority != request.Authority || decoded.Fence != request.Fence ||
			!bytes.Equal(decoded.Command, request.Command) ||
			decoded.Relation != request.Relation || !bytes.Equal(decoded.Key, request.Key) ||
			decoded.MinimumApplied != request.MinimumApplied ||
			decoded.MaxValueBytes != request.MaxValueBytes {
			t.Fatalf("request round trip = %+v", decoded)
		}
		if len(decoded.Command) != 0 && cap(decoded.Command) != len(decoded.Command) {
			t.Fatal("decoded command retained writable trailing frame capacity")
		}
	}

	completion := testReplicatedCompletion(t, fence, 2)
	state := ReplicatedMemberState{
		Fence: fence, LeaderID: fence.MemberID, Commit: 9, Applied: 8,
		CheckpointApplied: 7,
	}
	responses := []*ReplicatedResponse{
		{Kind: ReplicatedHandshake, HasState: true, State: state},
		{Kind: ReplicatedCompletion, HasState: true, State: state,
			RequestDigest: [32]byte{1},
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion,
				AppliedIndex: 8, CompletionAppliedSequence: 2,
				CompletionBytes: len(completion)}, Completion: completion},
		{Kind: ReplicatedNotLeader, HasState: true, State: state},
		{Kind: ReplicatedOutcomeUnknown, HasState: true, State: state},
		{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalProposalRefused,
			HasState: true, State: state},
		{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalReadBehind,
			HasState: true, State: state},
		{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalReadBufferBound,
			HasState: true, State: state},
		{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalDeterministic,
			HasState: true, State: state, RequestDigest: [32]byte{1}, Outcome: raftserve.Outcome{
				Code: raftserve.OutcomeSessionEpoch, AppliedIndex: 8}},
	}
	for _, response := range responses {
		var encoded bytes.Buffer
		if err := EncodeReplicatedResponse(&encoded, response); err != nil {
			t.Fatalf("encode %+v: %v", response, err)
		}
		decoded, err := DecodeReplicatedResponse(&encoded)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Kind != response.Kind || decoded.Refusal != response.Refusal ||
			decoded.HasState != response.HasState ||
			decoded.State != response.State || decoded.Outcome != response.Outcome ||
			decoded.RequestDigest != response.RequestDigest ||
			!bytes.Equal(decoded.Completion, response.Completion) {
			t.Fatalf("response round trip = %+v, want %+v", decoded, response)
		}
	}
}

func TestReplicatedDeterministicRefusalRequiresAppliedWitness(t *testing.T) {
	state := ReplicatedMemberState{
		Fence: testReplicatedFence(), LeaderID: testReplicatedFence().MemberID,
		Commit: 8, Applied: 8, CheckpointApplied: 7,
	}
	for code := raftserve.OutcomePending; code <= raftserve.OutcomeProposalAbandoned; code++ {
		response := &ReplicatedResponse{
			Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalDeterministic,
			HasState: true, State: state, RequestDigest: [32]byte{1},
			Outcome: raftserve.Outcome{Code: code, AppliedIndex: 8},
		}
		valid := validReplicatedResponse(response)
		want := code > raftserve.OutcomeCompletion && code < raftserve.OutcomeProposalRefused
		if valid != want {
			t.Fatalf("outcome %d valid=%t want=%t", code, valid, want)
		}
	}
	valid := &ReplicatedResponse{
		Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalDeterministic,
		HasState: true, State: state, RequestDigest: [32]byte{1},
		Outcome: raftserve.Outcome{Code: raftserve.OutcomeSessionEpoch, AppliedIndex: 8},
	}
	invalid := []raftserve.Outcome{
		{Code: raftserve.OutcomeSessionEpoch},
		{Code: raftserve.OutcomeSessionEpoch, AppliedIndex: 9},
		{Code: raftserve.OutcomeSessionEpoch, AppliedIndex: 8, CompletionAppliedSequence: 1},
		{Code: raftserve.OutcomeSessionEpoch, AppliedIndex: 8, CompletionBytes: 1},
	}
	if !validReplicatedResponse(valid) {
		t.Fatal("valid applied refusal was rejected")
	}
	missingDigest := *valid
	missingDigest.RequestDigest = [32]byte{}
	if validReplicatedResponse(&missingDigest) {
		t.Fatal("applied refusal without an exact request digest was accepted")
	}
	for _, outcome := range invalid {
		candidate := *valid
		candidate.Outcome = outcome
		if validReplicatedResponse(&candidate) {
			t.Fatalf("invalid applied witness accepted: %+v", outcome)
		}
	}
}

func TestReplicatedNativeWireRejectsSQLShapedAndCrossGroupPayloads(t *testing.T) {
	fence := testReplicatedFence()
	command := testReplicatedCommand(t, fence)
	invalid := []*ReplicatedRequest{
		{Operation: ReplicatedProbe, Fence: fence},
		{Operation: ReplicatedPropose, Fence: fence},
		{Operation: ReplicatedPropose, Fence: fence, Command: []byte("INSERT INTO docs")},
	}
	changed := fence
	changed.Group.GroupID[0]++
	invalid = append(invalid, &ReplicatedRequest{
		Operation: ReplicatedPropose, Fence: changed, Command: command,
	})
	for _, request := range invalid {
		var encoded bytes.Buffer
		if err := EncodeReplicatedRequest(&encoded, request); err == nil {
			t.Fatalf("invalid request encoded: %+v", request)
		}
	}
}

func TestReplicatedPointReadWirePreservesFoundEmptyAndMiss(t *testing.T) {
	fence := testReplicatedFence()
	request := &ReplicatedRequest{
		Operation: ReplicatedReadFollower, Fence: fence, Relation: 2,
		Key: []byte{0, 1, 2}, MinimumApplied: 17, MaxValueBytes: 4096,
	}
	var encoded bytes.Buffer
	if err := EncodeReplicatedRequest(&encoded, request); err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeReplicatedRequest(bytes.NewReader(encoded.Bytes()))
	if err != nil || decoded.Operation != request.Operation || decoded.Fence != fence ||
		decoded.Relation != request.Relation || !bytes.Equal(decoded.Key, request.Key) ||
		decoded.MinimumApplied != request.MinimumApplied || decoded.MaxValueBytes != request.MaxValueBytes {
		t.Fatalf("decoded=%+v err=%v", decoded, err)
	}
	state := ReplicatedMemberState{Fence: fence, LeaderID: 9, Commit: 21, Applied: 20,
		CheckpointApplied: 19}
	for _, response := range []*ReplicatedResponse{
		{Kind: ReplicatedReadFound, HasState: true, State: state, ReadApplied: 20, Value: []byte{}},
		{Kind: ReplicatedReadMissing, HasState: true, State: state, ReadApplied: 20},
	} {
		encoded.Reset()
		if err := EncodeReplicatedResponse(&encoded, response); err != nil {
			t.Fatal(err)
		}
		got, decodeErr := DecodeReplicatedResponse(bytes.NewReader(encoded.Bytes()))
		if decodeErr != nil || got.Kind != response.Kind || got.ReadApplied != 20 || len(got.Value) != 0 {
			t.Fatalf("response=%+v err=%v", got, decodeErr)
		}
	}
}

func TestReplicatedResponseRequestBoundRejectsOversizeHeaderBeforeAllocation(t *testing.T) {
	request := &ReplicatedRequest{Operation: ReplicatedReadFollower,
		Fence: testReplicatedFence(), Relation: 1, Key: []byte("k"),
		MinimumApplied: 1, MaxValueBytes: 32}
	maximum, err := maximumReplicatedResponseBody(request)
	if err != nil {
		t.Fatal(err)
	}
	fence := request.Fence
	response := &ReplicatedResponse{Kind: ReplicatedReadFound, HasState: true,
		State: ReplicatedMemberState{Fence: fence, LeaderID: fence.MemberID,
			Commit: 9, Applied: 9, CheckpointApplied: 8},
		ReadApplied: 9, Value: bytes.Repeat([]byte{1}, 32)}
	var exact bytes.Buffer
	if err := EncodeReplicatedResponse(&exact, response); err != nil {
		t.Fatal(err)
	}
	if bodyBytes := exact.Len() - 5; bodyBytes != maximum {
		t.Fatalf("exact response body=%d, request ceiling=%d", bodyBytes, maximum)
	}
	if _, err := decodeReplicatedResponseLimit(bytes.NewReader(exact.Bytes()), maximum); err != nil {
		t.Fatalf("exact bounded response: %v", err)
	}
	response.Value = append(response.Value, 2)
	var oversized bytes.Buffer
	if err := EncodeReplicatedResponse(&oversized, response); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeReplicatedResponseLimit(bytes.NewReader(oversized.Bytes()), maximum); !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("oversized valid response error=%v", err)
	}
	var header [5]byte
	header[0] = tagReplicatedResponse
	binary.BigEndian.PutUint32(header[1:], uint32(maximum+1+4))
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := decodeReplicatedResponseLimit(bytes.NewReader(header[:]), maximum); !errors.Is(err, errFrameTooLarge) {
			panic(err)
		}
	})
	if allocs > 2 {
		t.Fatalf("oversize header control path allocated %.2f times", allocs)
	}
	var wait sync.WaitGroup
	wait.Add(32)
	for range 32 {
		go func() {
			defer wait.Done()
			if _, err := decodeReplicatedResponseLimit(bytes.NewReader(header[:]), maximum); !errors.Is(err, errFrameTooLarge) {
				t.Errorf("concurrent oversize error=%v", err)
			}
		}()
	}
	wait.Wait()
}

func BenchmarkEncodeReplicatedLargePointRead(b *testing.B) {
	fence := testReplicatedFence()
	value := bytes.Repeat([]byte{7}, replication.MaxMutationValueBytes)
	response := &ReplicatedResponse{Kind: ReplicatedReadFound, HasState: true,
		State: ReplicatedMemberState{Fence: fence, LeaderID: fence.MemberID,
			Commit: 9, Applied: 9, CheckpointApplied: 8},
		ReadApplied: 9, Value: value}
	b.ReportAllocs()
	b.SetBytes(int64(len(value)))
	for b.Loop() {
		if err := EncodeReplicatedResponse(io.Discard, response); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeReplicatedLargePointRead(b *testing.B) {
	fence := testReplicatedFence()
	response := &ReplicatedResponse{Kind: ReplicatedReadFound, HasState: true,
		State: ReplicatedMemberState{Fence: fence, LeaderID: fence.MemberID,
			Commit: 9, Applied: 9, CheckpointApplied: 8},
		ReadApplied: 9, Value: bytes.Repeat([]byte{7}, replication.MaxMutationValueBytes)}
	var encoded bytes.Buffer
	if err := EncodeReplicatedResponse(&encoded, response); err != nil {
		b.Fatal(err)
	}
	request := &ReplicatedRequest{Operation: ReplicatedReadFollower, Fence: fence,
		Relation: 1, Key: []byte("k"), MinimumApplied: 1,
		MaxValueBytes: replication.MaxMutationValueBytes}
	maximum, err := maximumReplicatedResponseBody(request)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(replication.MaxMutationValueBytes)
	b.ResetTimer()
	for b.Loop() {
		decoded, err := decodeReplicatedResponseLimit(bytes.NewReader(encoded.Bytes()), maximum)
		if err != nil || len(decoded.Value) != replication.MaxMutationValueBytes {
			b.Fatal(err)
		}
	}
}

func FuzzReplicatedNativeRequestCanonical(f *testing.F) {
	fence := testReplicatedFence()
	command := testReplicatedCommand(f, fence)
	for _, request := range []*ReplicatedRequest{
		{Operation: ReplicatedPropose, Fence: fence, Command: command},
		{Operation: ReplicatedReadLeader, Fence: fence, Relation: 1,
			Key: []byte{0, 1}, MinimumApplied: 7, MaxValueBytes: 4096},
		{Operation: ReplicatedReadFollower, Fence: fence, Relation: 2,
			Key: []byte{2, 1, 0}, MinimumApplied: 9, MaxValueBytes: 8192},
	} {
		var seed bytes.Buffer
		if err := EncodeReplicatedRequest(&seed, request); err != nil {
			f.Fatal(err)
		}
		f.Add(seed.Bytes())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		// DecodeReplicatedRequest consumes one frame from a persistent stream;
		// trailing bytes may be the next request. Canonical uniqueness therefore
		// applies only when the corpus item contains exactly one complete frame.
		if len(data) < 5 || len(data) > 1<<20 ||
			uint64(binary.BigEndian.Uint32(data[1:5]))+1 != uint64(len(data)) {
			return
		}
		request, err := DecodeReplicatedRequest(bytes.NewReader(data))
		if err != nil {
			return
		}
		var canonical bytes.Buffer
		if err := EncodeReplicatedRequest(&canonical, request); err != nil {
			t.Fatalf("accepted request did not re-encode: %v", err)
		}
		if !bytes.Equal(canonical.Bytes(), data) {
			t.Fatal("accepted request has more than one wire representation")
		}
	})
}

func FuzzReplicatedNativeResponseCanonical(f *testing.F) {
	fence := testReplicatedFence()
	state := ReplicatedMemberState{
		Fence: fence, LeaderID: fence.MemberID, Commit: 8, Applied: 8,
		CheckpointApplied: 7,
	}
	completion := testReplicatedCompletion(f, fence, 2)
	for _, response := range []*ReplicatedResponse{
		{Kind: ReplicatedCompletion, HasState: true, State: state,
			RequestDigest: [32]byte{1},
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion,
				AppliedIndex: 8, CompletionAppliedSequence: 2,
				CompletionBytes: len(completion)},
			Completion: completion},
		{Kind: ReplicatedReadFound, HasState: true, State: state,
			ReadApplied: 8, Value: []byte{0, 1, 2}},
		{Kind: ReplicatedReadMissing, HasState: true, State: state,
			ReadApplied: 8},
		{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalReadBehind,
			HasState: true, State: state},
		{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalReadBufferBound,
			HasState: true, State: state},
	} {
		var seed bytes.Buffer
		if err := EncodeReplicatedResponse(&seed, response); err != nil {
			f.Fatal(err)
		}
		f.Add(seed.Bytes())
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			return
		}
		response, err := DecodeReplicatedResponse(bytes.NewReader(data))
		if err != nil {
			return
		}
		var canonical bytes.Buffer
		if err := EncodeReplicatedResponse(&canonical, response); err != nil {
			t.Fatalf("accepted response did not re-encode: %v", err)
		}
		if !bytes.Equal(canonical.Bytes(), data) {
			t.Fatal("accepted response has more than one wire representation")
		}
	})
}

func testReplicatedFence() ReplicatedFence {
	group := raftmember.GroupKey{TopologyRecoveryEpoch: 3}
	for index := range group.ClusterID {
		group.ClusterID[index] = byte(index + 1)
		group.ClusterIncarnation[index] = byte(index + 21)
		group.ShardIncarnation[index] = byte(index + 41)
		group.GroupID[index] = byte(index + 61)
	}
	fence := ReplicatedFence{
		Group: group, AllocationGeneration: 5, MemberID: 7,
		Command: raftservice.CommandFence{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
			OwnershipEpoch: 1, SchemaGeneration: 1,
			RelationManifestDigest: [32]byte{1},
			RoutingVersion:         1, RouteGeneration: 1,
		},
		NodeIncarnation: 11, Term: 13,
	}
	fence.StoreID[0] = 9
	return fence
}

func testReplicatedCommand(t testing.TB, fence ReplicatedFence) []byte {
	return testReplicatedCommandValue(t, fence, []byte(`{"id":1}`))
}

func testReplicatedCommandValue(t testing.TB, fence ReplicatedFence, value []byte) []byte {
	t.Helper()
	command := replication.Command{
		ClusterID:             fence.Group.ClusterID,
		ClusterIncarnation:    fence.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: fence.Group.TopologyRecoveryEpoch,
		Distribution:          "orders", Shard: "0000-ffff",
		AllocationGeneration: fence.AllocationGeneration,
		ShardIncarnation:     fence.Group.ShardIncarnation, GroupID: fence.Group.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
		OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1,
		RouteGeneration: 1, Tenant: []byte("tenant"),
		ClientID: replication.ID128{1}, ClientEpoch: 2, ClientSequence: 1,
		Fingerprint: sha256.Sum256([]byte("native-wire")),
		Batches: []replication.RelationMutationBatch{{Relation: 1,
			Mutations: []replication.Mutation{{Kind: replication.MutationPut,
				Key: []byte{1}, Value: value}}}},
	}
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func testReplicatedCompletion(
	t testing.TB,
	fence ReplicatedFence,
	applied uint64,
) []byte {
	t.Helper()
	resultDigest := replication.CompletionResultDigest(1, 1, nil)
	encoded, err := replication.AppendCompletion(nil, replication.Completion{
		ClusterID:             fence.Group.ClusterID,
		ClusterIncarnation:    fence.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: fence.Group.TopologyRecoveryEpoch,
		Distribution:          "orders", Shard: "0000-ffff",
		AllocationGeneration: fence.AllocationGeneration,
		ShardIncarnation:     fence.Group.ShardIncarnation, GroupID: fence.Group.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
		RoutingVersion: 1, RouteGeneration: 1, Tenant: []byte("tenant"),
		ClientID: replication.ID128{1}, ClientEpoch: 2, ClientSequence: 1,
		Fingerprint: sha256.Sum256([]byte("native-wire")), AppliedSequence: applied,
		ResultCode: 1, ResultFormat: 1, Storage: replication.CompletionInline,
		ResultDigest: resultDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
