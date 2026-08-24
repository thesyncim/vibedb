package shardservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
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
	for _, request := range []*ReplicatedRequest{
		{Operation: ReplicatedProbe, Fence: ReplicatedFence{
			Group: fence.Group, AllocationGeneration: fence.AllocationGeneration,
		}},
		{Operation: ReplicatedPropose, Fence: fence, Command: command},
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
		if decoded.Operation != request.Operation || decoded.Fence != request.Fence ||
			!bytes.Equal(decoded.Command, request.Command) {
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
			Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion,
				AppliedIndex: 8, CompletionAppliedSequence: 2,
				CompletionBytes: len(completion)}, Completion: completion},
		{Kind: ReplicatedNotLeader, HasState: true, State: state},
		{Kind: ReplicatedOutcomeUnknown, HasState: true, State: state},
		{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalProposalRefused,
			HasState: true, State: state},
		{Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalDeterministic,
			HasState: true, State: state, Outcome: raftserve.Outcome{
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
			HasState: true, State: state,
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
		HasState: true, State: state,
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

func FuzzReplicatedNativeRequestCanonical(f *testing.F) {
	fence := testReplicatedFence()
	command := testReplicatedCommand(f, fence)
	var seed bytes.Buffer
	if err := EncodeReplicatedRequest(&seed, &ReplicatedRequest{
		Operation: ReplicatedPropose, Fence: fence, Command: command,
	}); err != nil {
		f.Fatal(err)
	}
	f.Add(seed.Bytes())
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
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
	var seed bytes.Buffer
	if err := EncodeReplicatedResponse(&seed, &ReplicatedResponse{
		Kind: ReplicatedCompletion, HasState: true, State: state,
		Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion,
			AppliedIndex: 8, CompletionAppliedSequence: 2,
			CompletionBytes: len(completion)},
		Completion: completion,
	}); err != nil {
		f.Fatal(err)
	}
	f.Add(seed.Bytes())
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
