package raftservice

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

// busyProposalAdmissionHost holds the second Host turn. The first turn looks
// like a proposal progress turn, so Owner.Run performs its bounded ingress
// admission while the next Ready boundary is still pending.
type busyProposalAdmissionHost struct {
	ownerHost
	firstRunEntered    chan struct{}
	releaseFirstRunCh  chan struct{}
	secondRunEntered   chan struct{}
	releaseSecondRunCh chan struct{}
	proposalBlocked    chan struct{}
	releaseProposalCh  chan struct{}
	controlAdmitted    chan int

	blockAfter int

	mu                   sync.Mutex
	runs                 int
	proposals            []raftmember.GroupKey
	blockOnce            sync.Once
	releaseFirstRunOnce  sync.Once
	releaseSecondRunOnce sync.Once
	releaseProposalOnce  sync.Once
}

func (host *busyProposalAdmissionHost) RunOne() (multiraft.Progress, bool, error) {
	host.mu.Lock()
	host.runs++
	run := host.runs
	host.mu.Unlock()
	switch run {
	case 1:
		close(host.firstRunEntered)
		<-host.releaseFirstRunCh
		return multiraft.Progress{Kind: multiraft.ProgressProposal}, true, nil
	case 2:
		close(host.secondRunEntered)
		<-host.releaseSecondRunCh
		return multiraft.Progress{Kind: multiraft.ProgressReady,
			ReadyKind: raftmember.DrivePersisted}, true, nil
	default:
		return multiraft.Progress{}, false, nil
	}
}

func (host *busyProposalAdmissionHost) EnqueueTrackedProposal(
	group raftmember.GroupKey, _ []byte, _ multiraft.ProposalToken,
) error {
	host.mu.Lock()
	host.proposals = append(host.proposals, group)
	count := len(host.proposals)
	host.mu.Unlock()
	if count == host.blockAfter {
		host.blockOnce.Do(func() { close(host.proposalBlocked) })
		<-host.releaseProposalCh
	}
	return nil
}

func (host *busyProposalAdmissionHost) Status(raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	return raftmember.RuntimeStatus{MemberID: 1, LeaderID: 1, Term: 2}, nil
}

func (host *busyProposalAdmissionHost) RequestCampaign(raftmember.GroupKey) error {
	host.mu.Lock()
	runs := host.runs
	host.mu.Unlock()
	host.controlAdmitted <- runs
	return nil
}

func (host *busyProposalAdmissionHost) PopOutbound() (raftmember.OutboundMessage, bool) {
	return raftmember.OutboundMessage{}, false
}

func (host *busyProposalAdmissionHost) Close() error { return nil }

func (host *busyProposalAdmissionHost) releaseFirst() {
	host.releaseFirstRunOnce.Do(func() { close(host.releaseFirstRunCh) })
}

func (host *busyProposalAdmissionHost) releaseSecond() {
	host.releaseSecondRunOnce.Do(func() { close(host.releaseSecondRunCh) })
}

func (host *busyProposalAdmissionHost) releaseProposal() {
	host.releaseProposalOnce.Do(func() { close(host.releaseProposalCh) })
}

func (host *busyProposalAdmissionHost) releaseAll() {
	host.releaseFirst()
	host.releaseSecond()
	host.releaseProposal()
}

func (host *busyProposalAdmissionHost) proposalSnapshot() ([]raftmember.GroupKey, int) {
	host.mu.Lock()
	defer host.mu.Unlock()
	return append([]raftmember.GroupKey(nil), host.proposals...), host.runs
}

func newBusyProposalOwner(t *testing.T, groups []raftmember.GroupKey, host *busyProposalAdmissionHost) (
	*Owner, []CommandFence, *raftserve.Registry,
) {
	t.Helper()
	registry, err := raftserve.NewRegistry(raftserve.Limits{
		MaxGroups: len(groups), MaxOutstandingIdentities: 8,
		MaxOutstandingAttempts: 8, MaxWaiters: 8,
		MaxAttemptsPerIdentity:     1,
		MaxRetainedCompletionBytes: 8 * int64(replicatedstate.MaxCompletionEnvelopeBytes),
	})
	if err != nil {
		t.Fatal(err)
	}

	fences := make([]CommandFence, len(groups))
	members := make(map[raftmember.GroupKey]ownerMember, len(groups))
	for index, group := range groups {
		identity := raftmember.RuntimeIdentity{
			Group: group, AllocationGeneration: 1, MemberID: 1,
			StoreID: [16]byte{byte(index + 1)}, NodeIncarnation: 1,
			RelationManifestDigest: [32]byte{1},
		}
		fence := CommandFence{
			ReplicaSetVersion: 1, ActivePolicyGeneration: 1,
			ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1,
			RelationManifestDigest: [32]byte{1}, RoutingVersion: 1, RouteGeneration: 1,
		}
		fences[index] = fence
		members[group] = ownerMember{
			identity: identity, command: fence, generation: &ownerGeneration{},
		}
	}
	owner := &Owner{
		registry: registry, host: host, groups: append([]raftmember.GroupKey(nil), groups...),
		members: members,
		limits:  Limits{MaxIngressItems: 16, MaxIngressBytes: 64 << 20},
		ingress: make(chan ownerRequest, 16), ready: make(chan struct{}), done: make(chan struct{}),
		pendingReads: make(map[[16]byte]*readDelivery),
	}
	return owner, fences, registry
}

func busyProposalRequest(
	t *testing.T, group raftmember.GroupKey, fence CommandFence, storeID [16]byte,
	data []byte, reply chan ownerReply,
) ownerRequest {
	t.Helper()
	return ownerRequest{
		kind: requestProposal, group: group,
		fence: ServingFence{Group: group, AllocationGeneration: 1, Command: fence,
			MemberID: 1, StoreID: storeID, NodeIncarnation: 1, Term: 2},
		data: data, reply: reply, bytes: int64(len(data)), delivery: &proposalDelivery{},
	}
}

func busyCampaignRequest(group raftmember.GroupKey, reply chan ownerReply) ownerRequest {
	return ownerRequest{kind: requestCampaign, group: group, reply: reply}
}

func proposalAtLeastBytes(t *testing.T, group raftmember.GroupKey, sequence uint64, minimum int) []byte {
	t.Helper()
	low, high := 1, replication.MaxMutationValueBytes
	var result []byte
	for low <= high {
		valueBytes := low + (high-low)/2
		candidate := proposalPrefixValueCommand(t, group, sequence,
			bytes.Repeat([]byte{'v'}, valueBytes))
		if len(candidate) >= minimum {
			result = candidate
			high = valueBytes - 1
		} else {
			low = valueBytes + 1
		}
	}
	if len(result) < minimum {
		t.Fatalf("could not construct proposal length >= %d (got %d)", minimum, len(result))
	}
	return result
}

func proposalPrefixValueCommand(
	t *testing.T, group raftmember.GroupKey, sequence uint64, value []byte,
) []byte {
	t.Helper()
	command := replication.Command{
		Kind:                   replication.CommandMutationBatch,
		ClusterID:              replication.ID128(group.ClusterID),
		ClusterIncarnation:     replication.ID128(group.ClusterIncarnation),
		TopologyRecoveryEpoch:  group.TopologyRecoveryEpoch,
		Distribution:           "docs",
		Shard:                  "0000-ffff",
		AllocationGeneration:   1,
		ShardIncarnation:       replication.ID128(group.ShardIncarnation),
		GroupID:                replication.ID128(group.GroupID),
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: 1,
		ProtectionEpoch:        1,
		OwnershipEpoch:         1,
		SchemaGeneration:       1,
		RoutingVersion:         1,
		RouteGeneration:        1,
		Tenant:                 []byte("tenant"),
		ClientID:               replication.ID128{1},
		ClientEpoch:            1,
		ClientSequence:         sequence,
		Fingerprint:            replication.Digest{byte(sequence)},
		Batches: []replication.RelationMutationBatch{{
			Relation: 1,
			Mutations: []replication.Mutation{{
				Kind: replication.MutationPut, Key: []byte{byte(sequence)}, Value: value,
			}},
		}},
	}
	data, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestOwnerAdmitsOrdinaryProposalsBeforeReadyDrain(t *testing.T) {
	groupA := peerServerTestGroup()
	groupB := groupA
	groupB.GroupID[0] ^= 0xff
	groups := []raftmember.GroupKey{groupA, groupB}

	tests := []struct {
		name       string
		blockAfter int
		control    bool
		requests   func(t *testing.T, fences []CommandFence, replies []chan ownerReply) []ownerRequest
		wantGroups []raftmember.GroupKey
	}{
		{
			name:       "cross-group-prefix",
			blockAfter: 3,
			control:    true,
			requests: func(t *testing.T, fences []CommandFence, replies []chan ownerReply) []ownerRequest {
				return []ownerRequest{
					busyProposalRequest(t, groupA, fences[0], [16]byte{1}, proposalPrefixCommand(t, groupA, 1), replies[0]),
					busyProposalRequest(t, groupA, fences[0], [16]byte{1}, proposalPrefixCommand(t, groupA, 2), replies[1]),
					busyProposalRequest(t, groupB, fences[1], [16]byte{2}, proposalPrefixCommand(t, groupB, 3), replies[2]),
				}
			},
			wantGroups: []raftmember.GroupKey{groupA, groupA, groupB},
		},
		{
			name:       "same-group-byte-boundary",
			blockAfter: 2,
			requests: func(t *testing.T, fences []CommandFence, replies []chan ownerReply) []ownerRequest {
				second := proposalPrefixCommand(t, groupA, 5)
				first := proposalAtLeastBytes(t, groupA, 4,
					int(raftmodel.MaxProposalBatchBytes)-len(second)+1)
				if len(first) >= int(raftmodel.MaxProposalBatchBytes) ||
					len(first)+len(second) <= int(raftmodel.MaxProposalBatchBytes) {
					t.Fatalf("boundary proposals lengths=%d+%d target=%d",
						len(first), len(second), raftmodel.MaxProposalBatchBytes)
				}
				return []ownerRequest{
					busyProposalRequest(t, groupA, fences[0], [16]byte{1}, first, replies[0]),
					busyProposalRequest(t, groupA, fences[0], [16]byte{1}, second, replies[1]),
				}
			},
			wantGroups: []raftmember.GroupKey{groupA, groupA},
		},
		{
			name:       "noncandidate-oversized",
			blockAfter: 1,
			requests: func(t *testing.T, fences []CommandFence, replies []chan ownerReply) []ownerRequest {
				large := proposalAtLeastBytes(t, groupA, 6, int(raftmodel.MaxProposalBatchBytes))
				if len(large) < int(raftmodel.MaxProposalBatchBytes) {
					t.Fatalf("oversized proposal length=%d target=%d", len(large), raftmodel.MaxProposalBatchBytes)
				}
				return []ownerRequest{
					busyProposalRequest(t, groupA, fences[0], [16]byte{1}, large, replies[0]),
				}
			},
			wantGroups: []raftmember.GroupKey{groupA},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			host := &busyProposalAdmissionHost{
				firstRunEntered: make(chan struct{}), releaseFirstRunCh: make(chan struct{}),
				secondRunEntered: make(chan struct{}), releaseSecondRunCh: make(chan struct{}),
				proposalBlocked: make(chan struct{}), releaseProposalCh: make(chan struct{}),
				controlAdmitted: make(chan int, 1), blockAfter: test.blockAfter,
			}
			owner, fences, registry := newBusyProposalOwner(t, groups, host)
			t.Cleanup(func() { _ = registry.Close() })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runDone := make(chan error, 1)
			stopped := make(chan struct{})
			go func() {
				runDone <- owner.Run(ctx)
				close(stopped)
			}()
			t.Cleanup(func() {
				cancel()
				host.releaseAll()
				select {
				case <-stopped:
				case <-time.After(5 * time.Second):
					t.Errorf("Owner.Run did not stop")
				}
			})
			<-owner.ready
			<-host.firstRunEntered

			replies := make([]chan ownerReply, test.blockAfter)
			for index := range replies {
				replies[index] = make(chan ownerReply, 1)
			}
			for _, request := range test.requests(t, fences, replies) {
				if err := owner.publish(request); err != nil {
					t.Fatal(err)
				}
			}
			var controlReply chan ownerReply
			if test.control {
				controlReply = make(chan ownerReply, 1)
				if err := owner.publish(busyCampaignRequest(groupA, controlReply)); err != nil {
					t.Fatal(err)
				}
			}
			host.releaseFirst()

			select {
			case <-host.proposalBlocked:
				groupsSeen, runs := host.proposalSnapshot()
				if runs != 1 {
					t.Fatalf("ordinary proposal admitted after Host run %d, want before Ready drain", runs)
				}
				if len(groupsSeen) != len(test.wantGroups) {
					t.Fatalf("admitted groups=%v want=%v", groupsSeen, test.wantGroups)
				}
				for index := range groupsSeen {
					if groupsSeen[index] != test.wantGroups[index] {
						t.Fatalf("admitted order=%v want=%v", groupsSeen, test.wantGroups)
					}
				}
			case <-host.secondRunEntered:
				t.Fatal("ordinary proposal waited for the pending Ready drain")
			case <-time.After(5 * time.Second):
				t.Fatal("ordinary proposal was not admitted")
			}

			host.releaseProposal()
			select {
			case <-host.secondRunEntered:
			case <-time.After(5 * time.Second):
				t.Fatal("Host did not reach the Ready drain turn")
			}
			if test.control {
				select {
				case runs := <-host.controlAdmitted:
					t.Fatalf("control admitted during pending Ready turn %d", runs)
				default:
				}
			}
			host.releaseSecond()
			if test.control {
				select {
				case runs := <-host.controlAdmitted:
					if runs < 2 {
						t.Fatalf("control admitted at Host turn %d, want after Ready turn", runs)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("control did not run after Ready drain")
				}
				if reply := <-controlReply; reply.err != nil {
					t.Fatalf("control: %v", reply.err)
				}
			}

			for index, replyChannel := range replies {
				select {
				case reply := <-replyChannel:
					if reply.err != nil {
						t.Fatalf("proposal %d: %v", index, reply.err)
					}
					reply.waiter.Cancel()
				case <-time.After(5 * time.Second):
					t.Fatalf("proposal %d was not admitted", index)
				}
			}
			cancel()
			host.releaseAll()
			if err := <-runDone; !errors.Is(err, context.Canceled) {
				t.Fatalf("owner shutdown=%v", err)
			}
			if owner.ingressItems != 0 || owner.ingressBytes != 0 {
				t.Fatalf("ingress accounting after shutdown=%d/%d", owner.ingressItems, owner.ingressBytes)
			}
		})
	}
}
