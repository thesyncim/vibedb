package gateway

import (
	"bytes"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	pb "go.etcd.io/raft/v3/raftpb"
)

// A learner changes physical replication, not the sealed SQL participant.
// Foreground admission must work both before and after the gate command has
// been durably prepared. No catalog publication or command rewrite is allowed
// to hide the membership/catalog interval exercised by the process CI failure.
func TestDurableRequestAdmissionContinuesAcrossLearnerAddition(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		name := "before-session-open"
		if prepared {
			name = "after-gate-preparation"
		}
		t.Run(name, func(t *testing.T) {
			route, client, reopen := newRouteSessionMachine(t)
			executor, err := NewReplicatedExecutor(client, 1, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			ctx, err := serviceauthz.WithAuthority(t.Context(), serviceauthz.Authority{Node: [16]byte{7}, Generation: 1})
			if err != nil {
				t.Fatal(err)
			}
			wave, head, _ := lifecycleRunnerFixture(t)
			driver := &nativeDurableRequestRouteGateSessions{executor: executor}
			var command []byte
			var physical requestledger.Digest
			if prepared {
				command, physical, err = driver.prepareAcquire(ctx, route, wave, head)
				if err != nil {
					t.Fatal(err)
				}
			}
			exact := bytes.Clone(command)
			applied := client.state.Applied + 1
			publication, err := client.machine.ApplyConfiguration(raftmodel.ApplyMeta{
				Index: applied, Term: client.state.Fence.Term, Type: pb.EntryConfChange,
			}, &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}})
			if err != nil {
				t.Fatal(err)
			}
			client.state.Applied, client.state.Commit = applied, applied
			client.state.Fence.Command.ReplicaSetVersion = publication.ReplicaSetVersion
			reopen() // The membership cut is durable, not a process-local hint.
			if !prepared {
				command, physical, err = driver.prepareAcquire(ctx, route, wave, head)
				if err != nil {
					t.Fatalf("learner addition blocked foreground admission: %v", err)
				}
			}
			if prepared {
				client.hide = true
				if _, err = executor.Propose(ctx, route, command); err == nil {
					t.Fatal("acknowledged a lost acquire response")
				}
				reopen()
			}
			result, err := executor.Propose(ctx, route, command)
			if err != nil {
				t.Fatalf("learner addition blocked prepared gate: %v", err)
			}
			completion, err := replication.OpenCompletion(result.Completion)
			if err != nil || completion.ResultCode != replicatedstate.ResultRouteGate {
				t.Fatalf("gate result=%d err=%v", completion.ResultCode, err)
			}
			if prepared && !bytes.Equal(exact, command) {
				t.Fatal("membership recovery rewrote the retained command")
			}
			if route.Command.ReplicaSetVersion != 1 {
				t.Fatal("test rewrote the old catalog route")
			}
			pin, err := requestledger.NewRoutePinAcquiring(head, wave.PinID, wave.Binding, physical, command)
			if err != nil {
				t.Fatal(err)
			}
			pin, err = requestledger.RecordVerifiedRoutePinAcquired(pin, pin.Revision+1, result.Completion)
			if err != nil {
				t.Fatal(err)
			}
			// After catalog publication a fresh gateway must still reconstruct
			// the same release identity, then reclaim the original session.
			route.Command.ReplicaSetVersion = publication.ReplicaSetVersion
			reopen()
			driver = &nativeDurableRequestRouteGateSessions{executor: executor}
			release, err := driver.prepareRelease(ctx, route, wave, pin)
			if err != nil {
				t.Fatalf("release after catalog publication: %v", err)
			}
			releaseView, err := replication.OpenCommand(release)
			if err != nil || releaseView.ReplicaSetVersion != 1 {
				t.Fatalf("release changed original membership binding: %v", err)
			}
			pin, err = requestledger.BeginRoutePinRelease(pin, pin.Revision+1, release)
			if err != nil {
				t.Fatal(err)
			}
			result, err = executor.Propose(ctx, route, release)
			if err != nil {
				t.Fatal(err)
			}
			pin, err = requestledger.RecordVerifiedRoutePinReleased(pin, pin.Revision+1, result.Completion)
			if err != nil {
				t.Fatal(err)
			}
			if err = driver.cleanup(ctx, route, wave, pin); err != nil {
				t.Fatalf("cleanup after membership change: %v", err)
			}
			reopen()
			capacity, err := client.machine.SessionCapacityState()
			if err != nil || capacity.SessionCount != 0 || capacity.AuthorityBindingCount != 0 {
				t.Fatalf("retained route session leaked: %+v %v", capacity, err)
			}
		})
	}
}
