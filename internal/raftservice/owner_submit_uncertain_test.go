package raftservice

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestOwnerSubmitPreservesAdmittedInfrastructureOutcomesAsUnknown(t *testing.T) {
	for _, termination := range []struct {
		name  string
		cause error
		code  raftserve.OutcomeCode
		lose  bool
	}{
		{name: "leadership lost", cause: raftmodel.ErrNotLeader,
			code: raftserve.OutcomeNotLeader, lose: true},
		{name: "host closed", cause: raftserve.ErrProposalAbandoned,
			code: raftserve.OutcomeProposalAbandoned},
	} {
		t.Run(termination.name, func(t *testing.T) {
			rig := newOwnerSubmitAdmissionRig(t)
			term := electTwoVoterTestRuntime(t, rig.host, rig.identity.Group)
			command := rf3Command(rig.base, replication.CommandSessionOpen, 0, 1, nil)
			command.NextDeadlineUnixNano = 2_000_000_000_000_000_000
			commandBytes := appendRF3Command(t, command)
			ownedBytes := make([]byte, len(commandBytes))
			copy(ownedBytes, commandBytes)
			fence := ServingFence{
				Group: rig.identity.Group, AllocationGeneration: rig.identity.AllocationGeneration,
				Command: rig.commandFence, MemberID: rig.identity.MemberID,
				StoreID: rig.identity.StoreID, NodeIncarnation: rig.identity.NodeIncarnation, Term: term,
			}
			type submitResult struct {
				result Result
				err    error
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan submitResult, 2)
			go func() {
				result, err := rig.owner.Submit(ctx, fence, commandBytes)
				done <- submitResult{result: result, err: err}
			}()
			go func() {
				result, err := rig.owner.SubmitOwned(ctx, fence, ownedBytes)
				done <- submitResult{result: result, err: err}
			}()

			// Run the serialized owner handler just as Owner.Run would. The real
			// Registry-backed Host emits admission only when Runtime accepts it.
			for range 2 {
				var request ownerRequest
				select {
				case request = <-rig.owner.ingress:
				case got := <-done:
					t.Fatalf("submit failed before owner admission: result=%+v err=%v", got.result, got.err)
				case <-ctx.Done():
					t.Fatalf("owner request was not enqueued: %v", context.Cause(ctx))
				}
				if err := rig.owner.handle(request); err != nil {
					t.Fatalf("owner admitted valid session-open command: %v", err)
				}
				rig.owner.release(request.bytes)
			}

			for range 16 {
				stats := rig.registry.Stats()
				if stats.Waiters == 2 && stats.PendingAdmittedAttempts == 1 {
					break
				}
				progress, progressed, runErr := rig.host.RunOne()
				if runErr != nil {
					t.Fatalf("drive proposal admission: progress=%+v err=%v", progress, runErr)
				}
				stats = rig.registry.Stats()
				if stats.Waiters == 2 && stats.PendingAdmittedAttempts == 1 {
					break
				}
				if !progressed {
					t.Fatalf("real Host made no progress toward admission: progress=%+v registry=%+v", progress, stats)
				}
			}
			if stats := rig.registry.Stats(); stats.Waiters != 2 || stats.PendingAdmittedAttempts != 1 {
				t.Fatalf("proposal was not held as an admitted shared waiter: %+v", stats)
			}

			if termination.lose {
				// Persist the uncommitted two-voter proposal, then deliver a valid
				// higher-term heartbeat from the second voter. With no second Host
				// running, the entry cannot commit before the tracked leadership loss.
				if _, _, err := rig.host.RunOne(); err != nil {
					t.Fatalf("persist uncommitted proposal: %v", err)
				}
				for range 16 {
					if _, ok := rig.host.PopOutbound(); !ok {
						break
					}
				}
				heartbeatType := pb.MsgHeartbeat
				from, to, higherTerm := uint64(2), uint64(1), term+1
				if err := rig.host.AdoptMessage(rig.identity.Group, &pb.Message{
					Type: &heartbeatType, From: &from, To: &to, Term: &higherTerm,
				}); err != nil {
					t.Fatalf("admit higher-term heartbeat: %v", err)
				}
				for range 8 {
					stats := rig.registry.Stats()
					if stats.PendingAdmittedAttempts == 0 {
						break
					}
					if _, progressed, err := rig.host.RunOne(); err != nil {
						t.Fatalf("observe higher-term heartbeat: %v", err)
					} else if !progressed {
						t.Fatalf("host stopped before leadership termination: %+v", stats)
					}
					for range 16 {
						if _, ok := rig.host.PopOutbound(); !ok {
							break
						}
					}
				}
			} else if err := rig.host.Close(); err != nil {
				t.Fatalf("close admitted Host: %v", err)
			}

			for range 2 {
				var got submitResult
				select {
				case got = <-done:
				case <-ctx.Done():
					t.Fatalf("submit did not settle after infrastructure outcome: %v", context.Cause(ctx))
				}
				var unknown *UnknownOutcomeError
				if !errors.As(got.err, &unknown) || !errors.Is(got.err, ErrOutcomeUnknown) ||
					!errors.Is(got.err, termination.cause) {
					t.Fatalf("admitted infrastructure outcome lost uncertainty: result=%+v err=%T %v",
						got.result, got.err, got.err)
				}
				if got.result.Outcome.Code != termination.code {
					t.Fatalf("terminal outcome=%d, want %d", got.result.Outcome.Code, termination.code)
				}
				if len(unknown.Command) != len(commandBytes) || cap(unknown.Command) != len(commandBytes) ||
					!bytes.Equal(unknown.Command, commandBytes) {
					t.Fatalf("retry bytes len/cap=%d/%d, command len=%d, exact=%t",
						len(unknown.Command), cap(unknown.Command), len(commandBytes), bytes.Equal(unknown.Command, commandBytes))
				}
			}
			if stats := rig.registry.Stats(); stats.Waiters != 0 || stats.PendingAdmittedAttempts != 0 {
				t.Fatalf("settled waiters/reservation retained: %+v", stats)
			}
			rig.owner.mu.Lock()
			pendingItems, pendingBytes := rig.owner.pendingProposalItems, rig.owner.pendingProposalBytes
			ingressItems, ingressBytes := rig.owner.ingressItems, rig.owner.ingressBytes
			rig.owner.mu.Unlock()
			if pendingItems != 0 || pendingBytes != 0 || ingressItems != 0 || ingressBytes != 0 {
				t.Fatalf("Owner reservation leak: pending=%d/%d ingress=%d/%d",
					pendingItems, pendingBytes, ingressItems, ingressBytes)
			}
			if termination.lose {
				_ = rig.host.Close()
			}
		})
	}
}

type ownerSubmitAdmissionRig struct {
	owner        *Owner
	host         *multiraft.Host
	registry     *raftserve.Registry
	identity     raftmember.RuntimeIdentity
	base         sqldriver.ReplicatedShardStoreIdentity
	commandFence CommandFence
}

func newOwnerSubmitAdmissionRig(t *testing.T) ownerSubmitAdmissionRig {
	t.Helper()
	const groupIndex = 71
	runtime, base, _ := newRF3RuntimeForTestIdentityWithVoters(
		t, rf3RuntimeTestIdentity(1, groupIndex), groupIndex, false, []uint64{1, 2},
	)
	identity := runtime.Identity()
	registry, err := raftserve.NewRegistry(raftserve.Limits{
		MaxGroups: 1, MaxOutstandingIdentities: 8,
		MaxOutstandingAttempts: 8, MaxWaiters: 8,
		MaxAttemptsPerIdentity:     1,
		MaxRetainedCompletionBytes: 8 * int64(replicatedstate.MaxCompletionEnvelopeBytes),
	})
	if err != nil {
		t.Fatal(err)
	}
	host, err := registry.NewHost(rf3HostLimits())
	if err != nil {
		_ = registry.Close()
		t.Fatal(err)
	}
	if err := host.Add(runtime); err != nil {
		_ = host.Close()
		_ = registry.Close()
		t.Fatal(err)
	}
	commandFence := rf3CommandFence(identity, base)
	owner, err := NewOwner(Options{
		Registry: registry, Host: host,
		Members:       []raftmember.RuntimeIdentity{identity},
		CommandFences: []CommandFence{commandFence},
		Limits: Limits{
			MaxIngressItems: 8, MaxIngressBytes: 1 << 20,
			MaxPendingProposalItems: 8, MaxPendingProposalBytes: 1 << 20,
			MaxPendingReadItems: 8, MaxPendingReadBytes: 1 << 20,
			MaxPendingOutboundBytes: 1 << 20,
		},
	})
	if err != nil {
		_ = host.Close()
		_ = registry.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = host.Close()
		_ = registry.Close()
	})
	owner.mu.Lock()
	owner.started = true
	close(owner.ready)
	owner.mu.Unlock()
	return ownerSubmitAdmissionRig{
		owner: owner, host: host, registry: registry, identity: identity,
		base: base, commandFence: commandFence,
	}
}

func electTwoVoterTestRuntime(t *testing.T, host *multiraft.Host, group raftmember.GroupKey) uint64 {
	t.Helper()
	if err := host.RequestCampaign(group); err != nil {
		t.Fatal(err)
	}
	voteAdmitted := false
	for range 32 {
		_, _, err := host.RunOne()
		if err != nil {
			t.Fatalf("drive two-voter campaign: %v", err)
		}
		for range 16 {
			outbound, ok := host.PopOutbound()
			if !ok {
				break
			}
			if outbound.Message == nil || outbound.Message.Type == nil ||
				(*outbound.Message.Type != pb.MsgVote && *outbound.Message.Type != pb.MsgPreVote) {
				continue
			}
			responseType := pb.MsgVoteResp
			if *outbound.Message.Type == pb.MsgPreVote {
				responseType = pb.MsgPreVoteResp
			}
			if err := host.AdoptMessage(group, &pb.Message{
				Type: responseType.Enum(), From: outbound.Message.To, To: outbound.Message.From,
				Term: outbound.Message.Term, Index: outbound.Message.Index,
			}); err != nil {
				t.Fatalf("admit synthetic second-voter response: %v", err)
			}
			voteAdmitted = true
		}
		status, err := host.Status(group)
		if err != nil {
			t.Fatal(err)
		}
		if status.MemberID == 1 && status.LeaderID == 1 && status.Term != 0 {
			if !voteAdmitted {
				t.Fatal("Runtime became leader without the test vote response")
			}
			// Empty the leader's persistence phase before proposing.
			for range 16 {
				_, progressed, runErr := host.RunOne()
				if runErr != nil {
					t.Fatalf("drain leader Ready: %v", runErr)
				}
				for range 16 {
					if _, ok := host.PopOutbound(); !ok {
						break
					}
				}
				if !progressed {
					break
				}
			}
			return status.Term
		}
	}
	t.Fatal("two-voter Runtime did not become leader")
	return 0
}
