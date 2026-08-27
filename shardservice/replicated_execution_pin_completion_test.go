package shardservice

import (
	"bytes"
	"testing"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

type executionPinWireSample struct {
	proof   executionpin.Completion
	applied uint64
}

// Derive every positive proof through the actual transition/certificate
// implementation rather than constructing plausible-looking certificate bytes.
func executionPinWireSamples(t *testing.T) []executionPinWireSample {
	t.Helper()
	outer, err := replication.OpenCommand(testReplicatedExecutionPinCommand(t, testReplicatedFence()))
	if err != nil {
		t.Fatal(err)
	}
	acquire, err := outer.OpenExecutionPin()
	if err != nil {
		t.Fatal(err)
	}
	authority := executionpin.Digest{21}
	var samples []executionPinWireSample
	apply := func(current executionpin.Record, found bool, command executionpin.Command, index uint64) executionpin.Record {
		t.Helper()
		transition := executionpin.Apply(current, found, command, index, authority, executionpin.Digest{byte(index)})
		if transition.Reason != executionpin.ReasonApplied || !transition.Mutated {
			t.Fatalf("operation=%d transition=%+v", command.Operation, transition)
		}
		proof, err := executionpin.CompletionFromApplied(command, transition.Record, authority, index)
		if err != nil {
			t.Fatal(err)
		}
		samples = append(samples, executionPinWireSample{proof, index})
		return transition.Record
	}
	record := apply(executionpin.Record{}, false, acquire, 10)
	certificate, _ := record.AcquireCertificate()
	acquireDigest, err := executionpin.AcquireCertificateDigest(certificate)
	if err != nil {
		t.Fatal(err)
	}
	renew := acquire
	renew.Operation = executionpin.OperationRenew
	renew.ExpectedController, renew.ExpectedControllerEpoch = record.Controller, record.ControllerEpoch
	renew.ExpectedLeaseRevision, renew.ExpectedLeaseAppliedThrough = record.LeaseRevision, record.LeaseAppliedThrough
	renew.NextLeaseSpan, renew.AcquireCertificateDigest = 104, acquireDigest
	record = apply(record, true, renew, 11)
	recover := renew
	recover.Operation = executionpin.OperationRecover
	recover.ExpectedLeaseRevision, recover.ExpectedLeaseAppliedThrough = record.LeaseRevision, record.LeaseAppliedThrough
	recover.NextController, recover.NextControllerEpoch, recover.NextLeaseSpan = executionpin.ID{22}, record.ControllerEpoch+1, 4
	record = apply(record, true, recover, 116)
	release := recover
	release.Operation = executionpin.OperationRelease
	release.ExpectedController, release.ExpectedControllerEpoch = record.Controller, record.ControllerEpoch
	release.ExpectedLeaseRevision, release.ExpectedLeaseAppliedThrough = record.LeaseRevision, record.LeaseAppliedThrough
	release.NextController, release.NextControllerEpoch, release.NextLeaseSpan = executionpin.ID{}, 0, 0
	release.PrepareTerminalDigest = executionpin.Digest{23}
	apply(record, true, release, 117)
	expire := release
	expire.Operation, expire.PrepareTerminalDigest = executionpin.OperationExpire, executionpin.Digest{}
	expire.AcquireCertificateDigest = executionpin.Digest{}
	apply(record, true, expire, 121)
	// Expiry may also install the anti-resurrection tombstone before Acquire.
	apply(executionpin.Record{}, false, expire, 121)
	return samples
}

func executionPinWireEnvelope(t testing.TB, raw []byte, code uint32, applied uint64) []byte {
	t.Helper()
	command, err := replication.OpenCommand(testReplicatedExecutionPinCommand(t, testReplicatedFence()))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := replication.AppendCompletionBytes(nil, replication.CompletionBytes{
		ClusterID: command.ClusterID, ClusterIncarnation: command.ClusterIncarnation,
		TopologyRecoveryEpoch: command.TopologyRecoveryEpoch, Distribution: command.Distribution, Shard: command.Shard,
		AllocationGeneration: command.AllocationGeneration, ShardIncarnation: command.ShardIncarnation, GroupID: command.GroupID,
		ReplicaSetVersion: command.ReplicaSetVersion, ActivePolicyGeneration: command.ActivePolicyGeneration,
		ProtectionEpoch: command.ProtectionEpoch, RoutingVersion: command.RoutingVersion, RouteGeneration: command.RouteGeneration,
		Tenant: command.Tenant, ClientID: command.ClientID, ClientEpoch: command.ClientEpoch, ClientSequence: command.ClientSequence,
		Fingerprint: command.Fingerprint, RetryHome: command.RetryHome, AppliedSequence: applied,
		ResultCode: code, ResultFormat: replicatedstate.ResultFormatExecutionPin, Storage: replication.CompletionInline,
		ResultLength: uint64(len(raw)), InlineResult: raw,
		ResultDigest: replication.CompletionResultDigest(code, replicatedstate.ResultFormatExecutionPin, raw),
	})
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func TestExecutionPinCompletionNativeWireGrammar(t *testing.T) {
	for _, sample := range executionPinWireSamples(t) {
		raw, err := executionpin.AppendCompletion(nil, sample.proof)
		if err != nil {
			t.Fatal(err)
		}
		for _, code := range []uint32{replicatedstate.ResultApplied, replicatedstate.ResultIndexConflict,
			replicatedstate.ResultIntentBusy, replicatedstate.ResultTargetBound, replicatedstate.ResultStaleFence} {
			body := raw
			if code != replicatedstate.ResultApplied {
				refusal, err := executionpin.RefusalCompletion(sample.proof.Operation)
				if err != nil {
					t.Fatal(err)
				}
				body, err = executionpin.AppendCompletion(nil, refusal)
				if err != nil {
					t.Fatal(err)
				}
			}
			envelope := executionPinWireEnvelope(t, body, code, sample.applied)
			view, err := replication.OpenCompletion(envelope)
			if err != nil || !validReplicatedCompletionResult(view) {
				t.Fatalf("operation=%d code=%d valid proof rejected: %v", sample.proof.Operation, code, err)
			}
			state := replicatedStateAtApplied(replicatedWireState(testReplicatedServingState()), sample.applied)
			response := &ReplicatedResponse{Kind: ReplicatedCompletion, HasState: true, State: state,
				RequestDigest: [32]byte{1}, Completion: envelope,
				Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion, AppliedIndex: sample.applied,
					CompletionAppliedSequence: sample.applied, CompletionBytes: len(envelope)}}
			var wire bytes.Buffer
			if err := EncodeReplicatedResponse(&wire, response); err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeReplicatedResponse(&wire)
			if err != nil || !bytes.Equal(decoded.Completion, envelope) {
				t.Fatalf("fixed proof changed in native roundtrip: %v", err)
			}
			if allocations := testing.AllocsPerRun(50, func() { _ = validReplicatedCompletionResult(view) }); allocations != 0 {
				t.Fatalf("proof validation allocates: %v", allocations)
			}
		}
		assertRejected := func(body []byte, code uint32, applied uint64) {
			t.Helper()
			view, err := replication.OpenCompletion(executionPinWireEnvelope(t, body, code, applied))
			if err != nil {
				t.Fatal("negative must have an authentic outer envelope", err)
			}
			if validReplicatedCompletionResult(view) {
				t.Fatalf("operation=%d accepted malformed/spliced proof with code=%d applied=%d", sample.proof.Operation, code, applied)
			}
		}
		for _, code := range []uint32{replicatedstate.ResultIndexConflict, replicatedstate.ResultIntentBusy,
			replicatedstate.ResultTargetBound, replicatedstate.ResultStaleFence, replicatedstate.ResultSessionOpened, ^uint32(0)} {
			assertRejected(raw, code, sample.applied)
		}
		assertRejected(raw, replicatedstate.ResultApplied, sample.applied+1)
		refusal, _ := executionpin.RefusalCompletion(sample.proof.Operation)
		refusalBytes, _ := executionpin.AppendCompletion(nil, refusal)
		assertRejected(refusalBytes, replicatedstate.ResultApplied, sample.applied)
		for _, length := range []int{0, 1, executionpin.CompletionBytes - 1} {
			assertRejected(raw[:length], replicatedstate.ResultApplied, sample.applied)
		}
		assertRejected(append(bytes.Clone(raw), 0), replicatedstate.ResultApplied, sample.applied)
		for _, offset := range []int{0, 8, 10, 12, 32, executionpin.CompletionBytes - 1} {
			corrupt := bytes.Clone(raw)
			corrupt[offset] ^= 1
			assertRejected(corrupt, replicatedstate.ResultApplied, sample.applied)
		}
		wrongOperation := sample.proof
		if sample.proof.Status == executionpin.StatusActive {
			wrongOperation.Operation = executionpin.OperationRelease
		} else {
			wrongOperation.Operation = executionpin.OperationAcquire
		}
		spliced, err := executionpin.AppendCompletion(nil, wrongOperation)
		if err != nil {
			t.Fatal("operation/status splice must be canonical before wire semantic check", err)
		}
		assertRejected(spliced, replicatedstate.ResultApplied, sample.applied)
	}
}

func TestExecutionPinAppliedCompletionSurvivesNativeServerSettlement(t *testing.T) {
	sample := executionPinWireSamples(t)[0]
	proof, err := executionpin.AppendCompletion(nil, sample.proof)
	if err != nil {
		t.Fatal(err)
	}
	envelope := executionPinWireEnvelope(t, proof, replicatedstate.ResultApplied, sample.applied)
	owner := &fakeReplicatedOwner{state: testReplicatedServingState(), result: raftservice.Result{
		Completion: envelope, Outcome: raftserve.Outcome{Code: raftserve.OutcomeCompletion,
			AppliedIndex: sample.applied, CompletionAppliedSequence: sample.applied, CompletionBytes: len(envelope)},
	}}
	server := testReplicatedServer(owner)
	request := &ReplicatedRequest{Operation: ReplicatedPropose, Capability: serviceauthz.CapabilityExecutionPin,
		Fence: testReplicatedFence(), Command: testReplicatedExecutionPinCommand(t, testReplicatedFence())}
	response := server.executeReplicated(t.Context(), request)
	if response.Kind != ReplicatedCompletion || !bytes.Equal(response.Completion, envelope) || server.Stats().ProposalInvalidCompletion != 0 {
		t.Fatalf("durable valid pin proof converted to unknown: kind=%d stats=%+v", response.Kind, server.Stats())
	}
	// Re-authenticating the outer envelope must not hide corruption of an
	// inner certificate. Such a committed invariant failure remains unknown.
	proof[32] ^= 1
	owner.result.Completion = executionPinWireEnvelope(t, proof, replicatedstate.ResultApplied, sample.applied)
	response = server.executeReplicated(t.Context(), request)
	if response.Kind != ReplicatedOutcomeUnknown || server.Stats().ProposalInvalidCompletion != 1 ||
		server.Stats().ProposalInvalidCompletionReasons&ReplicatedCompletionInvalidResult == 0 {
		t.Fatalf("corrupted inner pin proof was exposed as success: kind=%d stats=%+v", response.Kind, server.Stats())
	}
}
