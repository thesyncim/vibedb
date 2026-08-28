package shardservice

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

func TestReplicatedRetryRetiredWireRequiresExactPreAdmissionShape(t *testing.T) {
	fence := testReplicatedFence()
	valid := ReplicatedResponse{
		Kind: ReplicatedRefusal, Refusal: ReplicatedRefusalRetryRetired,
		HasState: true, State: ReplicatedMemberState{Fence: fence, LeaderID: fence.MemberID,
			Commit: 9, Applied: 8, CheckpointApplied: 7},
		RequestDigest: [32]byte{1}, Outcome: raftserve.Outcome{Code: raftserve.OutcomeRetryRetired},
	}
	var encoded bytes.Buffer
	if err := EncodeReplicatedResponse(&encoded, &valid); err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeReplicatedResponse(&encoded)
	if err != nil || decoded.Refusal != valid.Refusal || decoded.Outcome != valid.Outcome ||
		decoded.RequestDigest != valid.RequestDigest || decoded.State != valid.State {
		t.Fatalf("retirement round trip = %+v, %v", decoded, err)
	}
	mutations := []struct {
		name string
		edit func(*ReplicatedResponse)
	}{
		{"missing digest", func(r *ReplicatedResponse) { r.RequestDigest = [32]byte{} }},
		{"missing state", func(r *ReplicatedResponse) { r.HasState = false; r.State = ReplicatedMemberState{} }},
		{"missing incarnation", func(r *ReplicatedResponse) { r.State.Fence.NodeIncarnation = 0 }},
		{"applied claim", func(r *ReplicatedResponse) { r.Outcome.AppliedIndex = 8 }},
		{"completion sequence", func(r *ReplicatedResponse) { r.Outcome.CompletionAppliedSequence = 8 }},
		{"completion bytes", func(r *ReplicatedResponse) { r.Outcome.CompletionBytes = 1; r.Completion = []byte{1} }},
		{"read claim", func(r *ReplicatedResponse) { r.ReadApplied = 8 }},
		{"value", func(r *ReplicatedResponse) { r.Value = []byte{1} }},
		{"applied refusal without witness", func(r *ReplicatedResponse) { r.Refusal = ReplicatedRefusalDeterministic }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			candidate := valid
			mutation.edit(&candidate)
			if validReplicatedResponse(&candidate) {
				t.Fatalf("malformed retirement accepted: %+v", candidate)
			}
		})
	}
	for code := raftserve.OutcomePending; code <= raftserve.OutcomeProposalAbandoned; code++ {
		candidate := valid
		candidate.Outcome.Code = code
		if validReplicatedResponse(&candidate) != (code == raftserve.OutcomeRetryRetired) {
			t.Fatalf("retirement accepted unrelated outcome %d", code)
		}
	}
}

func TestReplicatedServerPreservesPreAdmissionRetryRetired(t *testing.T) {
	fence := testReplicatedFence()
	request := &ReplicatedRequest{Operation: ReplicatedPropose, Fence: fence, Command: testReplicatedCommand(t, fence)}
	state := testReplicatedServingState()
	state.Status.Commit, state.Status.Applied, state.Status.CheckpointApplied = 8, 8, 7
	for _, test := range []struct {
		name    string
		outcome raftserve.Outcome
		err     error
		want    ReplicatedRefusalCode
	}{
		{"retired before admission", raftserve.Outcome{Code: raftserve.OutcomeRetryRetired}, replicatedstate.ErrRetryRetired, ReplicatedRefusalRetryRetired},
		{"retired after apply", raftserve.Outcome{Code: raftserve.OutcomeRetryRetired, AppliedIndex: 9}, replicatedstate.ErrRetryRetired, ReplicatedRefusalDeterministic},
		{"unrelated admission error", raftserve.Outcome{Code: raftserve.OutcomeSessionSequence}, replicatedstate.ErrSessionSequence, ReplicatedRefusalNone},
		{"mismatched cause", raftserve.Outcome{Code: raftserve.OutcomeRetryRetired}, replicatedstate.ErrSessionSequence, ReplicatedRefusalNone},
		{"uncertain cause", raftserve.Outcome{Code: raftserve.OutcomeRetryRetired}, errors.Join(replicatedstate.ErrRetryRetired, raftservice.ErrOutcomeUnknown), ReplicatedRefusalNone},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := testReplicatedServer(&fakeReplicatedOwner{state: state,
				result: raftservice.Result{Outcome: test.outcome}, err: test.err})
			response := server.executeReplicated(t.Context(), request)
			if test.want == ReplicatedRefusalNone {
				if response.Kind != ReplicatedOutcomeUnknown {
					t.Fatalf("unproven refusal became definite: %+v", response)
				}
				return
			}
			if response.Kind != ReplicatedRefusal || response.Refusal != test.want ||
				response.Outcome != test.outcome || response.State.Fence != request.Fence ||
				response.RequestDigest != replicatedRequestDigest(request.Command) || !validReplicatedResponse(response) {
				t.Fatalf("retirement response = %+v", response)
			}
			if test.want == ReplicatedRefusalRetryRetired &&
				(response.State.Applied != 8 || response.State.Commit != 8 || server.Stats().ProposalInvalidDeterministic != 0) {
				t.Fatalf("preadmission retirement fabricated apply or downgraded: %+v stats=%+v", response, server.Stats())
			}
		})
	}
}
