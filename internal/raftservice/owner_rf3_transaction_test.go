package raftservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type transactionRF3Cluster struct {
	group      raftmember.GroupKey
	bases      [3]sqldriver.ReplicatedShardStoreIdentity
	reads      [3]*sqldriver.ReplicatedApply
	owners     [3]*Owner
	peers      [3]*AuthenticatedPeerRuntime
	contexts   [3]context.Context
	cancels    [3]context.CancelFunc
	runErrors  [3]chan error
	pulses     [3]chan struct{}
	stopPulses chan struct{}
	stopped    [3]bool
}

func newTransactionRF3Cluster(t testing.TB) *transactionRF3Cluster {
	t.Helper()
	const voters = 3
	cluster := &transactionRF3Cluster{stopPulses: make(chan struct{})}
	var runtimes [voters]*raftmember.Runtime
	for index := range voters {
		runtimes[index], cluster.bases[index], cluster.reads[index] =
			newRF3RuntimeWithGlobalIndex(t, uint64(index+1), true)
	}
	cluster.group = runtimes[0].Identity().Group

	var members [voters]rafttransport.Member
	var nodes [voters]rafttransport.NodeID
	for index := range voters {
		nodes[index][0] = byte(index + 1)
		members[index] = rafttransport.Member{
			Group: cluster.group, ReplicaSetVersion: 1,
			MemberID: uint64(index + 1), Node: nodes[index],
			Role: rafttransport.MemberVoter,
		}
	}
	registries := make(map[rafttransport.NodeID]*rafttransport.StaticRegistry, voters)
	for index := range voters {
		registry, err := rafttransport.NewStaticRegistry(
			nodes[index], members[:],
			rafttransport.Limits{MaxGroups: 1, MaxMembers: voters},
		)
		if err != nil {
			t.Fatal(err)
		}
		registries[nodes[index]] = registry
	}

	authority := newPeerServerTestAuthority(t)
	var peerTLS [voters]*rafttransport.PeerTLS
	var listeners [voters]net.Listener
	addresses := make(map[rafttransport.NodeID]string, voters)
	for index := range voters {
		peerTLS[index] = newPeerServerTestTLS(t, authority, rafttransport.PeerIdentity{
			TrustDomain: registries[nodes[index]].TrustDomain(), Node: nodes[index],
		})
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[index] = listener
		addresses[nodes[index]] = listener.Addr().String()
	}
	dial := func(ctx context.Context, node rafttransport.NodeID) (net.Conn, error) {
		address, ok := addresses[node]
		if !ok {
			return nil, rafttransport.ErrNodeNotFound
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	}
	deadline := func() time.Time { return time.Now().Add(10 * time.Second) }

	for index := range voters {
		serving, err := raftserve.NewRegistry(raftserve.Limits{
			MaxGroups: 1, MaxOutstandingIdentities: 32,
			MaxOutstandingAttempts: 64, MaxWaiters: 64,
			MaxAttemptsPerIdentity:     4,
			MaxRetainedCompletionBytes: 32 * int64(replicatedstate.MaxCompletionEnvelopeBytes),
		})
		if err != nil {
			t.Fatal(err)
		}
		host, err := serving.NewHost(rf3HostLimits())
		if err != nil {
			t.Fatal(err)
		}
		if err := host.Add(runtimes[index]); err != nil {
			t.Fatal(err)
		}
		cluster.pulses[index] = make(chan struct{}, 1)
		remoteNodes := make([]rafttransport.NodeID, 0, voters-1)
		for remote := range voters {
			if remote != index {
				remoteNodes = append(remoteNodes, nodes[remote])
			}
		}
		peer, err := NewAuthenticatedPeerRuntime(AuthenticatedPeerOptions{
			Registry: registries[nodes[index]], TLS: peerTLS[index], Dial: dial,
			Listener: listeners[index], HandshakeDeadline: deadline, MaxInboundStreams: 8,
			Owner: Options{
				Registry: serving, Host: host,
				Members:       []raftmember.RuntimeIdentity{runtimes[index].Identity()},
				CommandFences: []CommandFence{rf3CommandFence(runtimes[index].Identity(), cluster.bases[index])},
				ReadSources:   []ReadSource{cluster.reads[index]},
				Pulse:         cluster.pulses[index],
				Limits: Limits{MaxIngressItems: 128, MaxIngressBytes: 64 << 20,
					MaxPendingProposalItems: 64, MaxPendingProposalBytes: 64 << 20,
					MaxPendingReadItems: 64, MaxPendingReadBytes: 64 << 20,
					MaxPendingOutboundBytes: 64 << 20},
			},
			Transport: rafttransport.OrdinaryTransportOptions{
				Peers: remoteNodes,
				Queue: rafttransport.QueueLimits{
					PerPeerFrames: 32, PerPeerBytes: 4 << 20,
					GlobalFrames: 64, GlobalBytes: 8 << 20,
				},
				Coalesce: rafttransport.CoalesceLimits{
					MaxFrames: 8, MaxBytes: 1 << 20,
					RetainedBytes: rafttransport.DefaultRetainedFrameBytes,
				},
				Wait: rafttransport.WaitWithTimer,
				Backoff: func(failures uint32) time.Duration {
					return time.Duration(failures) * time.Millisecond
				},
				MaxReconnectDelay: time.Second, WriteDeadline: deadline,
				RetainedFrameBytes: rafttransport.DefaultRetainedFrameBytes,
			},
			Receiver: rafttransport.OrdinaryReceiverOptions{
				ReadDeadline: deadline, RetainedFrameBytes: rafttransport.DefaultRetainedFrameBytes,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		cluster.peers[index] = peer
		cluster.owners[index] = peer.Owner()
		cluster.contexts[index], cluster.cancels[index] = context.WithCancel(context.Background())
		cluster.runErrors[index] = make(chan error, 1)
	}
	for index := range voters {
		go func(index int) {
			cluster.runErrors[index] <- cluster.peers[index].Run(cluster.contexts[index])
		}(index)
	}
	for index := range voters {
		select {
		case <-cluster.peers[index].Started():
			if !cluster.peers[index].Running() {
				t.Fatalf("peer %d did not publish authenticated serving", index)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("peer %d did not start", index)
		}
	}
	go pulseRF3(cluster.stopPulses, cluster.pulses[:])
	t.Cleanup(func() { cluster.close(t, listeners[:]) })
	return cluster
}

func (cluster *transactionRF3Cluster) close(t testing.TB, listeners []net.Listener) {
	t.Helper()
	close(cluster.stopPulses)
	for index := range cluster.cancels {
		if !cluster.stopped[index] {
			cluster.stopped[index] = true
			cluster.cancels[index]()
		}
	}
	for _, listener := range listeners {
		_ = listener.Close()
	}
	for index, done := range cluster.runErrors {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Errorf("RF3 transaction owner %d did not stop", index)
		}
	}
}

func (cluster *transactionRF3Cluster) stop(t testing.TB, index int) {
	t.Helper()
	if cluster.stopped[index] {
		return
	}
	cluster.stopped[index] = true
	cluster.cancels[index]()
	select {
	case <-cluster.owners[index].Done():
	case <-time.After(10 * time.Second):
		t.Fatalf("RF3 transaction owner %d did not stop", index)
	}
}

func rf3TransactionCommand(
	t testing.TB,
	base sqldriver.ReplicatedShardStoreIdentity,
	control distributedtxn.ReplicatedCommand,
	batches []replication.RelationMutationBatch,
) []byte {
	t.Helper()
	transaction, err := distributedtxn.AppendReplicatedCommand(nil, control)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := replication.TransactionClientSequence(transaction)
	if err != nil {
		t.Fatal(err)
	}
	binding := base.Binding
	command := replication.Command{
		Kind:      replication.CommandTransaction,
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion: 1, ActivePolicyGeneration: binding.Authority.ActivePolicyGeneration,
		ProtectionEpoch: binding.Authority.ProtectionEpoch, OwnershipEpoch: binding.Authority.OwnershipEpoch,
		SchemaGeneration: binding.Authority.SchemaGeneration, RoutingVersion: binding.Authority.RoutingVersion,
		RouteGeneration: binding.Authority.RouteGeneration,
		Tenant:          []byte("tenant"), ClientID: replication.ID128(control.ID),
		ClientEpoch: uint64(control.Role), ClientSequence: sequence,
		Fingerprint: sha256.Sum256(transaction), Transaction: transaction, Batches: batches,
	}
	return appendRF3Command(t, command)
}

func submitRF3Transaction(
	t testing.TB,
	ctx context.Context,
	owner *Owner,
	group raftmember.GroupKey,
	command []byte,
) (Result, replication.CompletionView, replicatedstate.TransactionCompletionResult) {
	t.Helper()
	state, err := owner.Probe(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	result, err := owner.Submit(ctx, state.Fence(), command)
	if err != nil {
		t.Fatal(err)
	}
	completion, err := replication.OpenCompletion(result.Completion)
	if err != nil {
		t.Fatal(err)
	}
	transaction, err := replicatedstate.OpenTransactionCompletionResult(
		completion.ResultCode, completion.InlineResult,
	)
	if err != nil {
		t.Fatal(err)
	}
	return result, completion, transaction
}

func TestRF3TransactionSurvivesLeaderLossAndPublishesRelationBundleAtomically(t *testing.T) {
	cluster := newTransactionRF3Cluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cluster.owners[0].Campaign(ctx, cluster.group); err != nil {
		t.Fatal(err)
	}
	leader := waitRF3Leader(t, ctx, cluster.owners[:], nil, cluster.group)

	open := rf3Command(cluster.bases[leader], replication.CommandSessionOpen, 0, 1, nil)
	open.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	openResult, err := cluster.owners[leader].Submit(
		ctx, mustRF3State(t, ctx, cluster.owners[leader], cluster.group).Fence(),
		appendRF3Command(t, open),
	)
	if err != nil {
		t.Fatal(err)
	}
	openCompletion, err := replication.OpenCompletion(openResult.Completion)
	if err != nil {
		t.Fatal(err)
	}

	baseKey, ok := orderedkey.AppendJSONString(nil, []byte(`"txn-doc"`), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode transaction base key")
	}
	document := []byte(`{"id":"txn-doc","email":"txn@example.com"}`)
	globalKey := []byte{0x91, 0x01, 't'}
	globalValue := []byte(`["txn-doc"]`)
	batches := []replication.RelationMutationBatch{
		{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: baseKey, Value: document,
		}}},
		{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey, Value: globalValue,
		}}},
	}
	mutationDigest, err := replication.TransactionMutationDigest(batches)
	if err != nil {
		t.Fatal(err)
	}
	id := distributedtxn.ID{0x74, 0x78, 0x6e, 0x2d, 0x72, 0x66, 0x33, 1}
	coordinatorRecord, err := distributedtxn.AppendCoordinator(nil, distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: 1, RecoveryDeadline: time.Now().Add(time.Minute).UnixNano(),
		Participants: []distributedtxn.ParticipantRef{{
			Distribution:         []byte(cluster.bases[leader].Binding.Distribution),
			Shard:                []byte(cluster.bases[leader].Binding.Shard),
			RoutingVersion:       uint64(cluster.bases[leader].Binding.Authority.RoutingVersion),
			AllocationGeneration: cluster.bases[leader].Binding.AllocationGeneration,
			OwnershipEpoch:       uint64(cluster.bases[leader].Binding.Authority.OwnershipEpoch),
			MutationDigest:       mutationDigest, State: distributedtxn.ParticipantStaged,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	stageCoordinator := rf3TransactionCommand(t, cluster.bases[leader], distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedStageCoordinator,
		ID: id, PayloadKind: distributedtxn.ReplicatedPayloadCoordinator, Payload: coordinatorRecord,
	}, nil)
	_, _, _ = submitRF3Transaction(t, ctx, cluster.owners[leader], cluster.group, stageCoordinator)
	stageParticipant := rf3TransactionCommand(t, cluster.bases[leader], distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleParticipant, Operation: distributedtxn.ReplicatedStageParticipant,
		ID: id, PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
		Participant: distributedtxn.ParticipantStage{
			CoordinatorGroup:            distributedtxn.ID(cluster.bases[leader].Binding.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(cluster.bases[leader].Binding.ShardIncarnation),
			CoordinatorAllocation:       cluster.bases[leader].Binding.AllocationGeneration,
			BucketBits:                  8, IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
			MutationDigest: mutationDigest,
		},
	}, batches)
	staged, _, _ := submitRF3Transaction(t, ctx, cluster.owners[leader], cluster.group, stageParticipant)
	waitRF3Applied(t, ctx, cluster.owners[:], nil, cluster.group, staged.Outcome.AppliedIndex)

	follower := (leader + 1) % len(cluster.owners)
	if _, lease, _, readErr := readRF3PointAtFreshFence(
		t, ctx, cluster.owners[follower], cluster.reads[follower], cluster.group,
		PointReadRequest{Relation: 1, Key: baseKey, MinimumApplied: staged.Outcome.AppliedIndex,
			MaxValueBytes: replication.MaxMutationValueBytes},
	); !errors.Is(readErr, replicatedstate.ErrTransactionIntentActive) || lease != nil {
		t.Fatalf("staged point read lease=%T err=%v", lease, readErr)
	}
	blockedPut := rf3Command(cluster.bases[leader], replication.CommandMutationBatch,
		openCompletion.ClientEpoch, 2, []replication.Mutation{{
			Kind: replication.MutationPut, Key: baseKey, Value: []byte(`{"id":"txn-doc","email":"blocked"}`),
		}})
	blocked, err := cluster.owners[leader].Submit(
		ctx, mustRF3State(t, ctx, cluster.owners[leader], cluster.group).Fence(),
		appendRF3Command(t, blockedPut),
	)
	if err != nil {
		t.Fatal(err)
	}
	blockedCompletion, err := replication.OpenCompletion(blocked.Completion)
	if err != nil || blockedCompletion.ResultCode != replicatedstate.ResultIntentBusy {
		t.Fatalf("blocked write completion=%+v err=%v", blockedCompletion, err)
	}

	prepare := rf3TransactionCommand(t, cluster.bases[leader], distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleParticipant, Operation: distributedtxn.ReplicatedPrepareParticipant,
		ID: id, ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	_, _, _ = submitRF3Transaction(t, ctx, cluster.owners[leader], cluster.group, prepare)
	commit := rf3TransactionCommand(t, cluster.bases[leader], distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedCommitCoordinator,
		ID: id, ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	committed, committedEnvelope, _ := submitRF3Transaction(
		t, ctx, cluster.owners[leader], cluster.group, commit,
	)
	waitRF3Applied(t, ctx, cluster.owners[:], nil, cluster.group, committed.Outcome.AppliedIndex)

	// Model a response lost after quorum/apply: discard the first result, kill
	// that leader, and resolve the exact canonical command on the new leader.
	cluster.stop(t, leader)
	removed := map[int]bool{leader: true}
	if err := cluster.owners[(leader+1)%len(cluster.owners)].Campaign(ctx, cluster.group); err != nil {
		t.Fatal(err)
	}
	newLeader := waitRF3Leader(t, ctx, cluster.owners[:], removed, cluster.group)
	commitRetry, commitRetryEnvelope, _ := submitRF3Transaction(
		t, ctx, cluster.owners[newLeader], cluster.group, commit,
	)
	if !bytes.Equal(commitRetry.Completion, committed.Completion) ||
		commitRetryEnvelope.ResultCode != committedEnvelope.ResultCode {
		t.Fatal("commit retry after leader loss changed the deterministic outcome")
	}

	apply := rf3TransactionCommand(t, cluster.bases[newLeader], distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleParticipant, Operation: distributedtxn.ReplicatedApplyParticipant,
		ID: id, ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	applied, _, appliedResult := submitRF3Transaction(t, ctx, cluster.owners[newLeader], cluster.group, apply)
	if !appliedResult.AffectedRowsValid || appliedResult.AffectedRows != 1 {
		t.Fatalf("apply result=%+v", appliedResult)
	}
	appliedRetry, _, retryResult := submitRF3Transaction(t, ctx, cluster.owners[newLeader], cluster.group, apply)
	if !bytes.Equal(appliedRetry.Completion, applied.Completion) || retryResult != appliedResult {
		t.Fatalf("apply retry changed result: first=%+v retry=%+v", appliedResult, retryResult)
	}
	waitRF3Applied(t, ctx, cluster.owners[:], removed, cluster.group, applied.Outcome.AppliedIndex)

	follower = (newLeader + 1) % len(cluster.owners)
	if follower == leader {
		follower = (follower + 1) % len(cluster.owners)
	}
	if _, lease, _, readErr := readRF3PointAtFreshFence(
		t, ctx, cluster.owners[follower], cluster.reads[follower], cluster.group,
		PointReadRequest{Relation: 2, Key: globalKey, MinimumApplied: applied.Outcome.AppliedIndex,
			MaxValueBytes: replication.MaxMutationValueBytes},
	); !errors.Is(readErr, replicatedstate.ErrTransactionIntentActive) || lease != nil {
		t.Fatalf("applied-before-release index read lease=%T err=%v", lease, readErr)
	}

	release := rf3TransactionCommand(t, cluster.bases[newLeader], distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleParticipant, Operation: distributedtxn.ReplicatedReleaseParticipant,
		ID: id, ExpectedRevision: 3, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	released, _, _ := submitRF3Transaction(t, ctx, cluster.owners[newLeader], cluster.group, release)
	retire := rf3TransactionCommand(t, cluster.bases[newLeader], distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedRetireCoordinator,
		ID: id, ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	retired, _, _ := submitRF3Transaction(t, ctx, cluster.owners[newLeader], cluster.group, retire)
	waitRF3Applied(t, ctx, cluster.owners[:], removed, cluster.group,
		max(released.Outcome.AppliedIndex, retired.Outcome.AppliedIndex))

	baseRead, baseLease, _, err := readRF3PointAtFreshFence(
		t, ctx, cluster.owners[follower], cluster.reads[follower], cluster.group,
		PointReadRequest{Relation: 1, Key: baseKey, MinimumApplied: released.Outcome.AppliedIndex,
			MaxValueBytes: replication.MaxMutationValueBytes},
	)
	if err != nil || !baseRead.Found || !bytes.Equal(baseRead.Value, document) {
		t.Fatalf("released base read=%+v err=%v", baseRead, err)
	}
	baseLease.Release()
	indexRead, indexLease, _, err := readRF3PointAtFreshFence(
		t, ctx, cluster.owners[follower], cluster.reads[follower], cluster.group,
		PointReadRequest{Relation: 2, Key: globalKey, MinimumApplied: released.Outcome.AppliedIndex,
			MaxValueBytes: replication.MaxMutationValueBytes},
	)
	if err != nil || !indexRead.Found || !bytes.Equal(indexRead.Value, globalValue) {
		t.Fatalf("released index read=%+v err=%v", indexRead, err)
	}
	indexLease.Release()
}

func mustRF3State(
	t testing.TB,
	ctx context.Context,
	owner *Owner,
	group raftmember.GroupKey,
) ServingState {
	t.Helper()
	state, err := owner.Probe(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	return state
}
