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

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/routegate"
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
		{Operation: ReplicatedProbe, Authority: authority, Capability: serviceauthz.CapabilityDataRead, Fence: ReplicatedFence{
			Group: fence.Group, AllocationGeneration: fence.AllocationGeneration,
		}},
		{Operation: ReplicatedPropose, Authority: authority, Capability: serviceauthz.CapabilityDataWrite, Fence: fence, Command: command},
		{Operation: ReplicatedMembership, Authority: authority, Capability: serviceauthz.CapabilityMembership, Fence: fence, Membership: ReplicatedMembershipRequest{
			Kind: raftservice.MembershipAddLearner, TransitionID: [16]byte{3},
			MetadataEpoch: 5, CatalogGeneration: 7, ExpectedReplicaSetVersion: 1,
			SourceMember: fence.MemberID, TargetMember: fence.MemberID + 1,
		}},
		{Operation: ReplicatedReadLeader, Authority: authority, Capability: serviceauthz.CapabilityDataRead, Fence: fence, Relation: 1,
			Key: []byte{0, 1}, MinimumApplied: 7, MaxValueBytes: 4096},
		{Operation: ReplicatedReadFollower, Authority: authority, Capability: serviceauthz.CapabilityDataRead, Fence: fence, Relation: 2,
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
		if decoded.Operation != request.Operation || decoded.Authority != request.Authority ||
			decoded.Capability != request.Capability || decoded.Fence != request.Fence ||
			!bytes.Equal(decoded.Command, request.Command) || decoded.Membership != request.Membership ||
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
		{Kind: ReplicatedMembershipAccepted, HasState: true, State: state},
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

func BenchmarkReplicatedMembershipWire(b *testing.B) {
	request := &ReplicatedRequest{Operation: ReplicatedMembership, Fence: testReplicatedFence(),
		Membership: ReplicatedMembershipRequest{Kind: raftservice.MembershipPromoteVoter,
			TransitionID: [16]byte{4}, MetadataEpoch: 5, CatalogGeneration: 6,
			ExpectedReplicaSetVersion: 7, SourceMember: 1, TargetMember: 2}}
	var encoded bytes.Buffer
	if err := EncodeReplicatedRequest(&encoded, request); err != nil {
		b.Fatal(err)
	}
	data := encoded.Bytes()
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for range b.N {
		decoded, err := DecodeReplicatedRequest(bytes.NewReader(data))
		if err != nil || decoded.Membership.TargetMember != 2 {
			b.Fatal(err)
		}
	}
}

func TestReplicatedMembershipDecodeAllocationBound(t *testing.T) {
	request := &ReplicatedRequest{Operation: ReplicatedMembership, Fence: testReplicatedFence(),
		Membership: ReplicatedMembershipRequest{Kind: raftservice.MembershipAddLearner,
			TransitionID: [16]byte{5}, MetadataEpoch: 6, CatalogGeneration: 7,
			ExpectedReplicaSetVersion: 1, SourceMember: 7, TargetMember: 8}}
	var encoded bytes.Buffer
	if err := EncodeReplicatedRequest(&encoded, request); err != nil {
		t.Fatal(err)
	}
	if got := encoded.Len() - 5; got != replicatedMembershipRequestBodyBytes {
		t.Fatalf("membership body=%d, want exact %d", got, replicatedMembershipRequestBodyBytes)
	}
	data := encoded.Bytes()
	var reader bytes.Reader
	allocations := testing.AllocsPerRun(1000, func() {
		reader.Reset(data)
		decoded, err := DecodeReplicatedRequest(&reader)
		if err != nil || decoded.Membership.TargetMember != 8 {
			panic("membership decode")
		}
	})
	if allocations > 4 {
		t.Fatalf("membership decode allocations = %.1f, want <= 4", allocations)
	}
}

func TestReplicatedMembershipPreflightsExactBodyBeforeAllocation(t *testing.T) {
	var header [5]byte
	header[0] = tagReplicatedMembershipRequest
	binary.BigEndian.PutUint32(header[1:], uint32(4+replicatedMembershipRequestBodyBytes+1))
	var reader bytes.Reader
	allocations := testing.AllocsPerRun(1000, func() {
		reader.Reset(header[:])
		if _, err := DecodeReplicatedRequest(&reader); !errors.Is(err, ErrReplicatedWire) {
			panic("membership preflight")
		}
	})
	if allocations > 1 {
		t.Fatalf("oversized membership preflight allocations = %.1f, want <= 1", allocations)
	}
	binary.BigEndian.PutUint32(header[1:], uint32(4+replicatedMembershipRequestBodyBytes))
	badPrefix := append(header[:], replicatedWireVersion+1, byte(ReplicatedMembership))
	allocations = testing.AllocsPerRun(1000, func() {
		reader.Reset(badPrefix)
		if _, err := DecodeReplicatedRequest(&reader); !errors.Is(err, ErrReplicatedWire) {
			panic("membership prefix preflight")
		}
	})
	if allocations > 2 {
		t.Fatalf("bad membership prefix allocations = %.1f, want <= 2", allocations)
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
	authority := serviceauthz.Authority{Node: rafttransport.NodeID{9}, Generation: 1}
	invalid := []*ReplicatedRequest{
		{Operation: ReplicatedProbe, Fence: fence},
		{Operation: ReplicatedPropose, Fence: fence},
		{Operation: ReplicatedPropose, Fence: fence, Command: []byte("INSERT INTO docs")},
		{Operation: ReplicatedPropose, Authority: authority,
			Capability: serviceauthz.CapabilityDataWrite, Fence: fence,
			Command: testReplicatedTopologyCommand(t, fence)},
		{Operation: ReplicatedPropose, Authority: authority,
			Capability: serviceauthz.CapabilityTopology, Fence: fence, Command: command},
		{Operation: ReplicatedPropose, Fence: fence,
			Command: testReplicatedTopologyCommand(t, fence)},
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

func TestReplicatedProposalRecoveryCapabilityRequiresDataTransactionCommand(t *testing.T) {
	fence := testReplicatedFence()
	authority := serviceauthz.Authority{Node: rafttransport.NodeID{9}, Generation: 1}
	mutation := testReplicatedCommand(t, fence)
	transaction := testReplicatedTransactionCommandClass(
		t, fence, replication.CommandAuthorityData,
	)

	accepted := []struct {
		name       string
		authority  serviceauthz.Authority
		capability serviceauthz.Capability
		command    []byte
	}{
		{name: "plaintext mutation", command: mutation},
		{name: "data-write mutation", authority: authority,
			capability: serviceauthz.CapabilityDataWrite, command: mutation},
		{name: "plaintext transaction", command: transaction},
		{name: "data-write transaction", authority: authority,
			capability: serviceauthz.CapabilityDataWrite, command: transaction},
		{name: "recovery transaction", authority: authority,
			capability: serviceauthz.CapabilityTransactionRecovery, command: transaction},
	}
	for _, test := range accepted {
		t.Run(test.name, func(t *testing.T) {
			request := &ReplicatedRequest{
				Operation: ReplicatedPropose, Authority: test.authority,
				Capability: test.capability, Fence: fence, Command: test.command,
			}
			var encoded bytes.Buffer
			if err := EncodeReplicatedRequest(&encoded, request); err != nil {
				t.Fatalf("encode: %v", err)
			}
			decoded, err := DecodeReplicatedRequest(bytes.NewReader(encoded.Bytes()))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			command, err := replication.OpenCommand(decoded.Command)
			if err != nil || command.AuthorityClass != replication.CommandAuthorityData {
				t.Fatalf("decoded command authority=%d err=%v",
					command.AuthorityClass, err)
			}
			wantKind := replication.CommandMutationBatch
			if bytes.Equal(test.command, transaction) {
				wantKind = replication.CommandTransaction
			}
			if command.Kind() != wantKind {
				t.Fatalf("decoded command kind=%d, want %d", command.Kind(), wantKind)
			}
		})
	}

	rejected := []struct {
		name       string
		capability serviceauthz.Capability
		command    []byte
	}{
		{name: "recovery ordinary mutation",
			capability: serviceauthz.CapabilityTransactionRecovery, command: mutation},
		{name: "recovery topology transaction",
			capability: serviceauthz.CapabilityTransactionRecovery,
			command: testReplicatedTransactionCommandClass(
				t, fence, replication.CommandAuthorityTopology,
			)},
	}
	for _, test := range rejected {
		t.Run(test.name, func(t *testing.T) {
			request := &ReplicatedRequest{
				Operation: ReplicatedPropose, Authority: authority,
				Capability: test.capability, Fence: fence, Command: test.command,
			}
			var encoded bytes.Buffer
			if err := EncodeReplicatedRequest(&encoded, request); !errors.Is(err, ErrReplicatedWire) {
				t.Fatalf("encode error=%v, want %v", err, ErrReplicatedWire)
			}

			// Produce a canonical proposal with its ordinary capability, then
			// tamper only the fixed capability field. The decoder must inspect
			// the authenticated command envelope instead of trusting the wire
			// capability in isolation.
			request.Capability = serviceauthz.CapabilityDataWrite
			if command, err := replication.OpenCommand(request.Command); err != nil {
				t.Fatal(err)
			} else if command.AuthorityClass == replication.CommandAuthorityTopology {
				request.Capability = serviceauthz.CapabilityTopology
			}
			encoded.Reset()
			if err := EncodeReplicatedRequest(&encoded, request); err != nil {
				t.Fatalf("encode canonical base: %v", err)
			}
			frame := encoded.Bytes()
			const capabilityOffset = 5 + 1 + 1 + 16 + 8
			binary.BigEndian.PutUint64(frame[capabilityOffset:capabilityOffset+8],
				uint64(serviceauthz.CapabilityTransactionRecovery))
			if _, err := DecodeReplicatedRequest(bytes.NewReader(frame)); !errors.Is(err, ErrReplicatedWire) {
				t.Fatalf("decode error=%v, want %v", err, ErrReplicatedWire)
			}
		})
	}
}

func TestReplicatedProposalRequestLedgerCapabilityIsNarrow(t *testing.T) {
	fence := testReplicatedFence()
	authority := serviceauthz.Authority{Node: rafttransport.NodeID{9}, Generation: 1}
	ledger := testReplicatedRequestLedgerCommand(t, fence)
	request := &ReplicatedRequest{
		Operation: ReplicatedPropose, Authority: authority,
		Capability: serviceauthz.CapabilityRequestLedger, Fence: fence, Command: ledger,
	}
	var encoded bytes.Buffer
	if err := EncodeReplicatedRequest(&encoded, request); err != nil {
		t.Fatalf("request-ledger proposal: %v", err)
	}
	for _, capability := range []serviceauthz.Capability{
		0, serviceauthz.CapabilityDataWrite, serviceauthz.CapabilityTopology,
		serviceauthz.CapabilityTransactionRecovery,
	} {
		candidate := *request
		candidate.Capability = capability
		if capability == 0 {
			candidate.Authority = serviceauthz.Authority{}
		}
		encoded.Reset()
		if err := EncodeReplicatedRequest(&encoded, &candidate); !errors.Is(err, ErrReplicatedWire) {
			t.Fatalf("capability %d admitted ledger command: %v", capability, err)
		}
	}
	request.Command = testReplicatedCommand(t, fence)
	encoded.Reset()
	if err := EncodeReplicatedRequest(&encoded, request); !errors.Is(err, ErrReplicatedWire) {
		t.Fatalf("request-ledger capability admitted data command: %v", err)
	}

	request.Command = testReplicatedRequestLedgerCommandForPrincipal(
		t, fence, rafttransport.NodeID{8},
	)
	encoded.Reset()
	if err := EncodeReplicatedRequest(&encoded, request); err != nil {
		t.Fatalf("gateway service could not carry a distinct inner subject: %v", err)
	}
	wrongService := *request
	wrongService.Authority.Node = rafttransport.NodeID{8}
	encoded.Reset()
	if err := EncodeReplicatedRequest(&encoded, &wrongService); !errors.Is(err, ErrReplicatedWire) {
		t.Fatalf("mismatched outer service client identity admitted: %v", err)
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

func TestReplicatedProposalResponseBoundCarriesTransactionCompletion(t *testing.T) {
	fence := testReplicatedFence()
	request := &ReplicatedRequest{
		Operation: ReplicatedPropose, Fence: fence,
		Command: testReplicatedCommand(t, fence),
	}
	maximum, err := maximumReplicatedResponseBody(request)
	if err != nil {
		t.Fatal(err)
	}
	if want := replicatedResponseFixedBodyBytes + replicatedstate.MaxCompletionEnvelopeBytes; maximum != want {
		t.Fatalf("proposal response bound = %d, want %d", maximum, want)
	}

	var result [24]byte
	result[0] = byte(distributedtxn.ReplicatedRoleParticipant)
	result[1] = byte(distributedtxn.ReplicatedPrepareParticipant)
	result[2] = 2
	binary.LittleEndian.PutUint64(result[8:16], 2)
	if _, err := replicatedstate.OpenTransactionCompletionResult(
		replicatedstate.ResultApplied, result[:],
	); err != nil {
		t.Fatal(err)
	}
	completion, err := replication.AppendCompletionBytes(nil, replication.CompletionBytes{
		ClusterID:             replication.ID128(fence.Group.ClusterID),
		ClusterIncarnation:    replication.ID128(fence.Group.ClusterIncarnation),
		TopologyRecoveryEpoch: fence.Group.TopologyRecoveryEpoch,
		Distribution:          []byte("distribution"), Shard: []byte("shard"),
		AllocationGeneration:   fence.AllocationGeneration,
		ShardIncarnation:       replication.ID128(fence.Group.ShardIncarnation),
		GroupID:                replication.ID128(fence.Group.GroupID),
		ReplicaSetVersion:      fence.Command.ReplicaSetVersion,
		ActivePolicyGeneration: fence.Command.ActivePolicyGeneration,
		ProtectionEpoch:        fence.Command.ProtectionEpoch,
		RoutingVersion:         fence.Command.RoutingVersion,
		RouteGeneration:        fence.Command.RouteGeneration,
		Tenant:                 []byte("tenant"), ClientID: replication.ID128{1},
		ClientEpoch:    uint64(distributedtxn.ReplicatedRoleParticipant),
		ClientSequence: 2, Fingerprint: replication.Digest{1},
		AppliedSequence: 9,
		ResultCode:      replicatedstate.ResultApplied,
		ResultFormat:    replicatedstate.ResultFormatTransaction,
		Storage:         replication.CompletionInline, ResultLength: uint64(len(result)),
		ResultDigest: replication.CompletionResultDigest(
			replicatedstate.ResultApplied, replicatedstate.ResultFormatTransaction, result[:],
		),
		InlineResult: result[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	state := ReplicatedMemberState{
		Fence: fence, LeaderID: fence.MemberID,
		Commit: 9, Applied: 9, CheckpointApplied: 8,
	}
	response := &ReplicatedResponse{
		Kind: ReplicatedCompletion, HasState: true, State: state,
		RequestDigest: [32]byte{1}, Completion: completion,
		Outcome: raftserve.Outcome{
			Code: raftserve.OutcomeCompletion, AppliedIndex: 9,
			CompletionAppliedSequence: 9, CompletionBytes: len(completion),
		},
	}
	var encoded bytes.Buffer
	if err := EncodeReplicatedResponse(&encoded, response); err != nil {
		t.Fatal(err)
	}
	if bodyBytes := encoded.Len() - 5; bodyBytes > maximum {
		t.Fatalf("transaction response body = %d, proposal bound = %d", bodyBytes, maximum)
	}
	decoded, err := decodeReplicatedResponseLimit(bytes.NewReader(encoded.Bytes()), maximum)
	if err != nil || !bytes.Equal(decoded.Completion, completion) {
		t.Fatalf("bounded transaction completion = %dB, err=%v", len(decoded.Completion), err)
	}
}

func TestReplicatedCompletionWireValidatesFixedResultGrammar(t *testing.T) {
	fence := testReplicatedFence()
	state := ReplicatedMemberState{
		Fence: fence, LeaderID: fence.MemberID,
		Commit: 9, Applied: 9, CheckpointApplied: 8,
	}
	responseFor := func(completion []byte) *ReplicatedResponse {
		return &ReplicatedResponse{
			Kind: ReplicatedCompletion, HasState: true, State: state,
			RequestDigest: [32]byte{1}, Completion: completion,
			Outcome: raftserve.Outcome{
				Code: raftserve.OutcomeCompletion, AppliedIndex: 9,
				CompletionAppliedSequence: 9, CompletionBytes: len(completion),
			},
		}
	}

	valid := responseFor(testReplicatedCompletion(t, fence, 9))
	if !validReplicatedResponse(valid) {
		t.Fatal("canonical mutation result was rejected")
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if !validReplicatedResponse(valid) {
			panic("canonical mutation result rejected")
		}
	}); allocations != 0 {
		t.Fatalf("canonical completion validation allocations = %.1f, want 0", allocations)
	}
	gate, ok := routegate.NewMachine(1, routegate.MaxRetainedRecords)
	if !ok {
		t.Fatal("construct route gate")
	}
	gateCommand := routegate.Command{
		Operation: routegate.OperationAcquireShared, Epoch: 1,
		Identity: routegate.Identity{1}, Binding: routegate.Binding{2},
	}
	var gateResult [routegate.OutcomeBytes]byte
	gateBytes, err := routegate.AppendOutcome(gateResult[:0], gate.Apply(gateCommand))
	if err != nil {
		t.Fatal(err)
	}
	validGate := responseFor(testReplicatedCompletionWithResult(
		t, fence, 9, replicatedstate.ResultRouteGate,
		replicatedstate.ResultFormatRouteGate, gateBytes,
	))
	if !validReplicatedResponse(validGate) {
		t.Fatal("canonical route-gate result was rejected")
	}

	invalid := [][]byte{
		testReplicatedCompletionWithResult(
			t, fence, 9, replicatedstate.ResultApplied,
			replicatedstate.ResultFormatMutation, nil,
		),
		testReplicatedCompletionWithResult(
			t, fence, 9, replicatedstate.ResultStaleFence,
			replicatedstate.ResultFormatMutation, make([]byte, replicatedstate.MutationCompletionResultBytes),
		),
		testReplicatedCompletionWithResult(
			t, fence, 9, replicatedstate.ResultApplied,
			replicatedstate.ResultFormatTransaction, make([]byte, 24),
		),
		testReplicatedCompletionWithResult(
			t, fence, 9, replicatedstate.ResultApplied, 77, nil,
		),
		testReplicatedCompletionWithResult(
			t, fence, 9, replicatedstate.ResultApplied,
			replicatedstate.ResultFormatRouteGate, gateBytes,
		),
	}
	for index, completion := range invalid {
		response := responseFor(completion)
		if validReplicatedResponse(response) {
			t.Fatalf("invalid result %d was accepted", index)
		}
		if err := EncodeReplicatedResponse(io.Discard, response); !errors.Is(err, ErrReplicatedWire) {
			t.Fatalf("encode invalid result %d: %v", index, err)
		}
		raw := encodeUncheckedReplicatedResponse(t, response)
		if _, err := DecodeReplicatedResponse(bytes.NewReader(raw)); !errors.Is(err, ErrReplicatedWire) {
			t.Fatalf("decode invalid result %d: %v", index, err)
		}
	}
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
	return testReplicatedCommandClass(t, fence, value, replication.CommandAuthorityData)
}

func testReplicatedTopologyCommand(t testing.TB, fence ReplicatedFence) []byte {
	return testReplicatedCommandClass(t, fence, []byte(`{"id":1}`),
		replication.CommandAuthorityTopology)
}

func testReplicatedRequestLedgerCommand(t testing.TB, fence ReplicatedFence) []byte {
	return testReplicatedRequestLedgerCommandForPrincipal(t, fence, rafttransport.NodeID{9})
}

func testReplicatedRequestLedgerCommandForPrincipal(
	t testing.TB,
	fence ReplicatedFence,
	principal rafttransport.NodeID,
) []byte {
	t.Helper()
	key := requestledger.RequestKey{
		Scope: requestledger.ScopeAuthenticated, Principal: requestledger.PrincipalID(principal),
		Request: requestledger.RequestID{1}, TenantDigest: requestledger.Digest{1},
	}
	plan, err := requestledger.AppendPlan(nil, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	requestDigest := requestledger.Digest(sha256.Sum256([]byte("request-ledger-wire")))
	head, err := requestledger.NewHeadWithContract(key, requestDigest, requestDigest, plan)
	if err != nil {
		t.Fatal(err)
	}
	headBytes, err := requestledger.AppendHead(nil, head)
	if err != nil {
		t.Fatal(err)
	}
	home, err := requestledger.Home(key)
	if err != nil {
		t.Fatal(err)
	}
	inner, err := requestledger.AppendCommand(nil, requestledger.Command{
		Operation: requestledger.OperationCreate, Revision: head.Revision,
		KeyDigest: head.KeyDigest, RequestDigest: head.RequestDigest,
		PlanRoot: head.PlanRoot, SubjectDigest: head.TerminalContractDigest,
		Home: home, ExpectedRangeIdentity: requestledger.Digest{2}, Payload: headBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	command := replication.Command{
		Kind:           replication.CommandRequestLedger,
		AuthorityClass: replication.CommandAuthorityRequestLedger,
		ClusterID:      fence.Group.ClusterID, ClusterIncarnation: fence.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: fence.Group.TopologyRecoveryEpoch,
		Distribution:          "request-ledger", Shard: "0000-ffff",
		AllocationGeneration: fence.AllocationGeneration,
		ShardIncarnation:     fence.Group.ShardIncarnation, GroupID: fence.Group.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1,
		OwnershipEpoch: 1, SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1,
		// Proposal retry identity belongs to the internal gateway service and is
		// intentionally distinct from the forwarded end-user subject in key.
		Tenant: []byte("request-ledger"), ClientID: replication.ID128{9},
		ClientEpoch: 1, ClientSequence: 1,
		Fingerprint:   sha256.Sum256([]byte("request-ledger-wire")),
		RequestLedger: inner,
	}
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func testReplicatedTransactionCommandClass(
	t testing.TB,
	fence ReplicatedFence,
	authorityClass replication.CommandAuthorityClass,
) []byte {
	t.Helper()
	id := distributedtxn.ID{1}
	control, err := distributedtxn.AppendReplicatedCommand(nil, distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedCommitCoordinator,
		ID:        id, ExpectedRevision: 1,
		PayloadKind: distributedtxn.ReplicatedPayloadNone,
	})
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := replication.TransactionClientSequence(control)
	if err != nil {
		t.Fatal(err)
	}
	command := replication.Command{
		Kind:                   replication.CommandTransaction,
		AuthorityClass:         authorityClass,
		ClusterID:              fence.Group.ClusterID,
		ClusterIncarnation:     fence.Group.ClusterIncarnation,
		TopologyRecoveryEpoch:  fence.Group.TopologyRecoveryEpoch,
		Distribution:           "orders",
		Shard:                  "0000-ffff",
		AllocationGeneration:   fence.AllocationGeneration,
		ShardIncarnation:       fence.Group.ShardIncarnation,
		GroupID:                fence.Group.GroupID,
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: 1,
		ProtectionEpoch:        1,
		OwnershipEpoch:         1,
		SchemaGeneration:       1,
		RoutingVersion:         1,
		RouteGeneration:        1,
		Tenant:                 []byte("tenant"),
		ClientID:               replication.ID128(id),
		ClientEpoch:            1,
		ClientSequence:         sequence,
		Fingerprint:            sha256.Sum256([]byte("native-wire-transaction")),
		Transaction:            control,
	}
	encoded, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func testReplicatedCommandClass(t testing.TB, fence ReplicatedFence, value []byte,
	authorityClass replication.CommandAuthorityClass) []byte {
	t.Helper()
	command := replication.Command{
		AuthorityClass:        authorityClass,
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
	result, err := replicatedstate.AppendMutationCompletionResult(
		nil, replicatedstate.ResultApplied, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	return testReplicatedCompletionWithResult(
		t, fence, applied, replicatedstate.ResultApplied,
		replicatedstate.ResultFormatMutation, result,
	)
}

func testReplicatedCompletionWithResult(
	t testing.TB,
	fence ReplicatedFence,
	applied uint64,
	resultCode uint32,
	resultFormat uint16,
	result []byte,
) []byte {
	t.Helper()
	resultDigest := replication.CompletionResultDigest(
		resultCode, resultFormat, result,
	)
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
		ResultCode:   resultCode,
		ResultFormat: resultFormat,
		Storage:      replication.CompletionInline, ResultLength: uint64(len(result)),
		ResultDigest: resultDigest, InlineResult: result,
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func encodeUncheckedReplicatedResponse(t testing.TB, response *ReplicatedResponse) []byte {
	t.Helper()
	e := encbuf{b: make([]byte, 5, 5+replicatedResponseFixedBodyBytes+len(response.Completion))}
	e.u8(replicatedWireVersion)
	e.u8(uint8(response.Kind))
	e.u8(uint8(response.Refusal))
	e.u8(uint8(response.Outcome.Code))
	if response.HasState {
		e.u8(1)
		encodeReplicatedMemberState(&e, response.State)
	} else {
		e.u8(0)
	}
	e.u64(response.Outcome.AppliedIndex)
	e.u64(response.Outcome.CompletionAppliedSequence)
	encodeReplicatedDigest(&e, response.RequestDigest)
	e.bytes(response.Completion)
	var encoded bytes.Buffer
	if e.err != nil {
		t.Fatal(e.err)
	}
	if err := writeEncodedFrame(&encoded, tagReplicatedResponse, e.b); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}
