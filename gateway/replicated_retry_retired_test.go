package gateway

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestReplicatedExecutorRetryRetiredResolvesOnlyExactRequest(t *testing.T) {
	route, command, states := testReplicatedRouteCommand(t)
	state := states["m2"]
	route.Replicas = []ReplicatedEndpoint{route.Replicas[1]}
	retired := shardservice.ReplicatedResponse{
		Kind: shardservice.ReplicatedRefusal, Refusal: shardservice.ReplicatedRefusalRetryRetired,
		HasState: true, State: state, RequestDigest: replicatedRequestDigest(command),
		Outcome: raftserve.Outcome{Code: raftserve.OutcomeRetryRetired},
	}
	for _, test := range []struct {
		name  string
		edit  func(*shardservice.ReplicatedResponse)
		valid bool
	}{
		{"exact durable retirement", func(*shardservice.ReplicatedResponse) {}, true},
		{"different request", func(r *shardservice.ReplicatedResponse) { r.RequestDigest[0] ^= 1 }, false},
		{"changed term", func(r *shardservice.ReplicatedResponse) { r.State.Fence.Term++ }, false},
		{"changed incarnation", func(r *shardservice.ReplicatedResponse) { r.State.Fence.NodeIncarnation++ }, false},
		{"changed store", func(r *shardservice.ReplicatedResponse) { r.State.Fence.StoreID[0] ^= 1 }, false},
		{"fabricated apply", func(r *shardservice.ReplicatedResponse) { r.Outcome.AppliedIndex = 1 }, false},
		{"different deterministic cause", func(r *shardservice.ReplicatedResponse) { r.Outcome.Code = raftserve.OutcomeSessionSequence }, false},
		{"other preadmission refusal", func(r *shardservice.ReplicatedResponse) {
			r.Refusal = shardservice.ReplicatedRefusalAdmissionBound
			r.Outcome = raftserve.Outcome{}
			r.RequestDigest = [32]byte{}
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := retired
			test.edit(&response)
			client := &sequenceReplicatedClient{state: state, responses: []*shardservice.ReplicatedResponse{
				{Kind: shardservice.ReplicatedOutcomeUnknown, HasState: true, State: state}, &response,
			}}
			executor, err := NewReplicatedExecutor(client, 2, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			result, err := executor.Propose(t.Context(), route, command)
			if test.valid {
				var refusal *ReplicatedRefusalError
				if !errors.Is(err, replicatedstate.ErrRetryRetired) || errors.Is(err, raftservice.ErrOutcomeUnknown) ||
					!errors.As(err, &refusal) || refusal.Outcome != retired.Outcome || result.Outcome != (raftserve.Outcome{}) {
					t.Fatalf("exact retirement = %+v err=%v", result, err)
				}
			} else {
				var unknown *raftservice.UnknownOutcomeError
				if !errors.As(err, &unknown) || !bytes.Equal(unknown.Command, command) {
					t.Fatalf("unproven refusal erased earlier uncertainty: %T %v", err, err)
				}
			}
			if client.proposals != 2 {
				t.Fatalf("proposals=%d, want exact bounded retry", client.proposals)
			}
		})
	}
}
