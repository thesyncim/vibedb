package raftservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
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
				TransactionRecoverySources: []TransactionRecoverySource{
					cluster.reads[index],
				},
				Pulse: cluster.pulses[index],
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
	control.ControllerEpoch = 1
	control.ExecutionPinDigest = distributedtxn.Digest(sha256.Sum256(control.ID[:]))
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

func rf3TransactionCoordinatorRecord(
	t testing.TB,
	base sqldriver.ReplicatedShardStoreIdentity,
	id distributedtxn.ID,
	digest distributedtxn.Digest,
) []byte {
	t.Helper()
	record, err := distributedtxn.AppendCoordinator(nil, distributedtxn.CoordinatorRecord{
		ID: id, State: distributedtxn.CoordinatorStaging, Revision: 1,
		CatalogGeneration: 1, RecoveryDeadline: int64(distributedtxn.MaxRecoveryPulses),
		Targets: []distributedtxn.TransactionTargetRef{{
			Distribution: []byte(base.Binding.Distribution), Shard: []byte(base.Binding.Shard),
			RoutingVersion:       base.Binding.Authority.RoutingVersion,
			AllocationGeneration: base.Binding.AllocationGeneration,
			OwnershipEpoch:       base.Binding.Authority.OwnershipEpoch,
			MutationDigest:       digest, State: distributedtxn.TargetStaged,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func rf3TransactionGlobalKey(t testing.TB, relation sqldriver.ReplicatedShardRelationIdentity) []byte {
	t.Helper()
	key, err := distribution.CurrentTupleCodec.AppendTuple(nil,
		[]distribution.Scalar{distribution.NewString("txn@example.com")})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := relation.GlobalIndexStorageKeyPoint(key); !ok {
		t.Fatalf("fixture global key is invalid for retained relation: %x", key)
	}
	return key
}

// Keep the shared transaction fixtures qualified on hosts where strict
// allocation prevents starting the physical RF3 cluster.
func TestRF3TransactionCommandFixturesPreflight(t *testing.T) {
	base := sqldriver.ReplicatedShardStoreIdentity{Binding: sqldriver.ReplicatedShardStoreBinding{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, TopologyRecoveryEpoch: 3,
		Distribution: "orders", Shard: "all", AllocationGeneration: 7,
		ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4},
		Authority: sqldriver.ReplicatedAuthorityProfile{
			ActivePolicyGeneration: 5, ProtectionEpoch: 7, OwnershipEpoch: 11,
			SchemaGeneration: 13, RoutingVersion: 17, RouteGeneration: 19,
		},
	}}
	id := distributedtxn.ID{1, 2, 3}
	relation := sqldriver.ReplicatedShardRelationIdentity{
		Relation: 2, Kind: sqldriver.ReplicatedShardRelationGlobalIndex,
		IndexID: 41, Incarnation: 7, LocatorCount: 1, Unique: true,
		KeyEncoding: sqldriver.ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
		TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
		BucketBits: distribution.DefaultVirtualBucketBits,
	}
	globalKey := rf3TransactionGlobalKey(t, relation)
	if _, ok := relation.GlobalIndexStorageKeyPoint([]byte{0x91, 0x01, 't'}); ok {
		t.Fatal("obsolete handwritten key unexpectedly accepted")
	}
	batches := []replication.RelationMutationBatch{
		{Relation: 1, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: []byte("base"), Value: []byte(`{"id":"base"}`),
		}}},
		{Relation: 2, Mutations: []replication.Mutation{{
			Kind: replication.MutationPutAbsentOrEqual, Key: globalKey, Value: []byte(`["base"]`),
		}}},
	}
	digest, err := replication.TransactionMutationDigest(batches)
	if err != nil {
		t.Fatal(err)
	}
	record := rf3TransactionCoordinatorRecord(t, base, id, digest)
	coordinator, err := distributedtxn.OpenCoordinator(record)
	if err != nil || coordinator.RecoveryDeadline != int64(distributedtxn.MaxRecoveryPulses) {
		t.Fatalf("logical recovery budget: %+v, %v", coordinator, err)
	}
	retirement, err := distributedtxn.AppendReplicatedRetirementSummary(nil,
		distributedtxn.ReplicatedRetirementSummary{AffectedRows: 1, AffectedRowsValid: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, control := range []distributedtxn.ReplicatedCommand{
		{Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedStageCoordinator,
			ID: id, PayloadKind: distributedtxn.ReplicatedPayloadCoordinator, Payload: record},
		{Role: distributedtxn.ReplicatedRoleTarget, Operation: distributedtxn.ReplicatedStageTarget,
			ID: id, PayloadKind: distributedtxn.ReplicatedPayloadTargetStage,
			Target: distributedtxn.TransactionTargetStage{
				CoordinatorGroup:            distributedtxn.ID(base.Binding.GroupID),
				CoordinatorShardIncarnation: distributedtxn.ID(base.Binding.ShardIncarnation),
				CoordinatorAllocation:       base.Binding.AllocationGeneration, BucketBits: 8,
				IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}}, MutationDigest: digest,
			}},
		{Role: distributedtxn.ReplicatedRoleTarget, Operation: distributedtxn.ReplicatedPrepareTarget,
			ID: id, ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone},
		{Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedCommitCoordinator,
			ID: id, ExpectedRevision: 1, PayloadKind: distributedtxn.ReplicatedPayloadNone},
		{Role: distributedtxn.ReplicatedRoleTarget, Operation: distributedtxn.ReplicatedApplyTarget,
			ID: id, ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone},
		{Role: distributedtxn.ReplicatedRoleTarget, Operation: distributedtxn.ReplicatedReleaseTarget,
			ID: id, ExpectedRevision: 3, PayloadKind: distributedtxn.ReplicatedPayloadNone},
		{Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedRetireCoordinator,
			ID: id, ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadRetirement, Payload: retirement},
	} {
		var mutations []replication.RelationMutationBatch
		if control.Operation == distributedtxn.ReplicatedStageTarget {
			mutations = batches
		}
		outer, err := replication.OpenCommand(rf3TransactionCommand(t, base, control, mutations))
		if err != nil {
			t.Fatal(err)
		}
		inner, err := distributedtxn.OpenReplicatedCommand(outer.TransactionBytes())
		if err != nil || inner.ControllerEpoch != 1 || inner.ExecutionPinDigest != distributedtxn.Digest(sha256.Sum256(id[:])) {
			t.Fatalf("fenced operation %d: %+v, %v", control.Operation, inner, err)
		}
	}
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
	if err != nil || completion.ResultCode != replicatedstate.ResultApplied {
		t.Fatalf("transaction completion=%+v err=%v", completion, err)
	}
	transaction, err := replicatedstate.OpenTransactionCompletionResult(
		completion.ResultCode, completion.InlineResult,
	)
	if err != nil {
		t.Fatal(err)
	}
	return result, completion, transaction
}

// submitRF3TransactionAtCurrentLeader routes one already-canonical command
// through a freshly observed leader. A leader can lose its lease between the
// status probe and Submit while the surviving voters are electing; retry only
// that documented routing failure with the exact same command bytes. This
// keeps the transaction identity and duplicate-result assertions intact.
func submitRF3TransactionAtCurrentLeader(
	t testing.TB,
	ctx context.Context,
	cluster *transactionRF3Cluster,
	removed map[int]bool,
	group raftmember.GroupKey,
	command []byte,
) (int, Result, replication.CompletionView, replicatedstate.TransactionCompletionResult) {
	t.Helper()
	const maxRoutingAttempts = 3
	for attempt := 0; attempt < maxRoutingAttempts; attempt++ {
		leader := waitRF3Leader(t, ctx, cluster.owners[:], removed, group)
		state, err := cluster.owners[leader].Probe(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		result, err := cluster.owners[leader].Submit(ctx, state.Fence(), command)
		if errors.Is(err, raftmodel.ErrNotLeader) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		completion, err := replication.OpenCompletion(result.Completion)
		if err != nil || completion.ResultCode != replicatedstate.ResultApplied {
			t.Fatalf("transaction completion=%+v err=%v", completion, err)
		}
		transaction, err := replicatedstate.OpenTransactionCompletionResult(
			completion.ResultCode, completion.InlineResult,
		)
		if err != nil {
			t.Fatal(err)
		}
		return leader, result, completion, transaction
	}
	t.Fatalf("RF3 transaction routing failed after %d exact-command attempts", maxRoutingAttempts)
	return -1, Result{}, replication.CompletionView{}, replicatedstate.TransactionCompletionResult{}
}

func selectRF3Follower(t testing.TB, leader int, removed map[int]bool, voters int) int {
	t.Helper()
	for offset := 1; offset < voters; offset++ {
		candidate := (leader + offset) % voters
		if !removed[candidate] {
			return candidate
		}
	}
	t.Fatalf("RF3 follower selection: leader=%d removed=%v", leader, removed)
	return -1
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
	if err != nil || openCompletion.ResultCode != replicatedstate.ResultSessionOpened {
		t.Fatalf("session completion=%+v err=%v", openCompletion, err)
	}

	baseKey, ok := orderedkey.AppendJSONString(nil, []byte(`"txn-doc"`), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode transaction base key")
	}
	document, err := vibejson.AppendCanonicalize(
		nil, []byte(`{"id":"txn-doc","email":"txn@example.com"}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	globalKey := rf3TransactionGlobalKey(t, cluster.bases[leader].Relations[1])
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
	coordinatorRecord := rf3TransactionCoordinatorRecord(t, cluster.bases[leader], id, mutationDigest)
	stageCoordinator := rf3TransactionCommand(t, cluster.bases[leader], distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedStageCoordinator,
		ID: id, PayloadKind: distributedtxn.ReplicatedPayloadCoordinator, Payload: coordinatorRecord,
	}, nil)
	_, _, _ = submitRF3Transaction(t, ctx, cluster.owners[leader], cluster.group, stageCoordinator)
	stageTarget := rf3TransactionCommand(t, cluster.bases[leader], distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleTarget, Operation: distributedtxn.ReplicatedStageTarget,
		ID: id, PayloadKind: distributedtxn.ReplicatedPayloadTargetStage,
		Target: distributedtxn.TransactionTargetStage{
			CoordinatorGroup:            distributedtxn.ID(cluster.bases[leader].Binding.GroupID),
			CoordinatorShardIncarnation: distributedtxn.ID(cluster.bases[leader].Binding.ShardIncarnation),
			CoordinatorAllocation:       cluster.bases[leader].Binding.AllocationGeneration,
			BucketBits:                  8, IntentScopes: []distributedtxn.IntentScope{{Start: 0, End: 256}},
			MutationDigest: mutationDigest,
		},
	}, batches)
	staged, _, _ := submitRF3Transaction(t, ctx, cluster.owners[leader], cluster.group, stageTarget)
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
		Role: distributedtxn.ReplicatedRoleTarget, Operation: distributedtxn.ReplicatedPrepareTarget,
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
	var commitRetry Result
	var commitRetryEnvelope replication.CompletionView
	newLeader, commitRetry, commitRetryEnvelope, _ = submitRF3TransactionAtCurrentLeader(
		t, ctx, cluster, removed, cluster.group, commit,
	)
	if !bytes.Equal(commitRetry.Completion, committed.Completion) ||
		commitRetryEnvelope.ResultCode != committedEnvelope.ResultCode {
		t.Fatal("commit retry after leader loss changed the deterministic outcome")
	}

	apply := rf3TransactionCommand(t, cluster.bases[newLeader], distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleTarget, Operation: distributedtxn.ReplicatedApplyTarget,
		ID: id, ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	newLeader, applied, _, appliedResult := submitRF3TransactionAtCurrentLeader(
		t, ctx, cluster, removed, cluster.group, apply,
	)
	if !appliedResult.AffectedRowsValid || appliedResult.AffectedRows != 1 {
		t.Fatalf("apply result=%+v", appliedResult)
	}
	newLeader, appliedRetry, _, retryResult := submitRF3TransactionAtCurrentLeader(
		t, ctx, cluster, removed, cluster.group, apply,
	)
	if !bytes.Equal(appliedRetry.Completion, applied.Completion) || retryResult != appliedResult {
		t.Fatalf("apply retry changed result: first=%+v retry=%+v", appliedResult, retryResult)
	}
	waitRF3Applied(t, ctx, cluster.owners[:], removed, cluster.group, applied.Outcome.AppliedIndex)

	newLeader = waitRF3Leader(t, ctx, cluster.owners[:], removed, cluster.group)
	follower = selectRF3Follower(t, newLeader, removed, len(cluster.owners))
	if _, lease, _, readErr := readRF3PointAtFreshFence(
		t, ctx, cluster.owners[follower], cluster.reads[follower], cluster.group,
		PointReadRequest{Relation: 2, Key: globalKey, MinimumApplied: applied.Outcome.AppliedIndex,
			MaxValueBytes: replication.MaxMutationValueBytes},
	); !errors.Is(readErr, replicatedstate.ErrTransactionIntentActive) || lease != nil {
		t.Fatalf("applied-before-release index read lease=%T err=%v", lease, readErr)
	}

	release := rf3TransactionCommand(t, cluster.bases[newLeader], distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleTarget, Operation: distributedtxn.ReplicatedReleaseTarget,
		ID: id, ExpectedRevision: 3, PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}, nil)
	var released Result
	newLeader, released, _, _ = submitRF3TransactionAtCurrentLeader(
		t, ctx, cluster, removed, cluster.group, release,
	)
	retirement, err := distributedtxn.AppendReplicatedRetirementSummary(
		nil,
		distributedtxn.ReplicatedRetirementSummary{AffectedRows: 1, AffectedRowsValid: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	retire := rf3TransactionCommand(t, cluster.bases[newLeader], distributedtxn.ReplicatedCommand{
		Role: distributedtxn.ReplicatedRoleCoordinator, Operation: distributedtxn.ReplicatedRetireCoordinator,
		ID: id, ExpectedRevision: 2, PayloadKind: distributedtxn.ReplicatedPayloadRetirement,
		Payload: retirement,
	}, nil)
	_, retired, _, _ := submitRF3TransactionAtCurrentLeader(
		t, ctx, cluster, removed, cluster.group, retire,
	)
	waitRF3Applied(t, ctx, cluster.owners[:], removed, cluster.group,
		max(released.Outcome.AppliedIndex, retired.Outcome.AppliedIndex))

	newLeader = waitRF3Leader(t, ctx, cluster.owners[:], removed, cluster.group)
	follower = selectRF3Follower(t, newLeader, removed, len(cluster.owners))
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

func TestRF3TransactionRecoveryReadIsLeaderOnlyAndSurvivesGatewayReplacement(t *testing.T) {
	cluster := newTransactionRF3Cluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := cluster.owners[0].Campaign(ctx, cluster.group); err != nil {
		t.Fatal(err)
	}
	leader := waitRF3Leader(t, ctx, cluster.owners[:], nil, cluster.group)

	id := distributedtxn.ID{0x72, 0x65, 0x63, 0x6f, 0x76, 0x65, 0x72, 0x79, 1}
	record := rf3TransactionCoordinatorRecord(t, cluster.bases[leader], id,
		distributedtxn.Digest(sha256.Sum256([]byte("recovery-read"))))
	stage := rf3TransactionCommand(t, cluster.bases[leader], distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedStageCoordinator,
		ID:        id, PayloadKind: distributedtxn.ReplicatedPayloadCoordinator, Payload: record,
	}, nil)
	staged, _, _ := submitRF3Transaction(t, ctx, cluster.owners[leader], cluster.group, stage)
	waitRF3Applied(t, ctx, cluster.owners[:], nil, cluster.group, staged.Outcome.AppliedIndex)

	read := replicatedstate.TransactionRecoveryReadRequest{
		Kind: replicatedstate.TransactionRecoveryLookupCoordinator, ID: id,
		MinimumApplied: staged.Outcome.AppliedIndex, MaxRows: 1,
		MaxBytes: uint32(replicatedstate.TransactionRecoverySummaryBytes +
			distributedtxn.MaxCoordinatorRecordBytes),
	}
	leaderState := mustRF3State(t, ctx, cluster.owners[leader], cluster.group)
	request := TransactionReadRequest{
		Fence: leaderState.Fence(), Capability: serviceauthz.CapabilityTransactionRecovery,
		Read: read,
	}
	for _, capability := range []serviceauthz.Capability{
		serviceauthz.CapabilityDataRead,
		serviceauthz.CapabilityDataWrite,
		serviceauthz.CapabilityTopology,
		serviceauthz.CapabilityDataRead | serviceauthz.CapabilityTransactionRecovery,
	} {
		request.Capability = capability
		if result, lease, readErr := cluster.owners[leader].ReadTransaction(ctx, request); !errors.Is(readErr, ErrTransactionRecoveryUnauthorized) || lease != nil ||
			len(result.Records) != 0 {
			t.Fatalf("capability %x recovery result=%+v lease=%T err=%v",
				capability, result, lease, readErr)
		}
	}
	request.Capability = serviceauthz.CapabilityTransactionRecovery
	result, lease, err := cluster.owners[leader].ReadTransaction(ctx, request)
	if err != nil || lease == nil || !result.Complete ||
		result.Applied < staged.Outcome.AppliedIndex || len(result.Records) != 1 ||
		result.Records[0].ID != id || !bytes.Equal(result.Records[0].Payload, record) {
		t.Fatalf("leader recovery result=%+v lease=%T err=%v", result, lease, err)
	}
	lease.Release()

	follower := (leader + 1) % len(cluster.owners)
	followerState := mustRF3State(t, ctx, cluster.owners[follower], cluster.group)
	request.Fence = followerState.Fence()
	if result, deniedLease, readErr := cluster.owners[follower].ReadTransaction(ctx, request); !errors.Is(readErr, raftmodel.ErrNotLeader) || deniedLease != nil ||
		len(result.Records) != 0 {
		t.Fatalf("follower recovery result=%+v lease=%T err=%v", result, deniedLease, readErr)
	}

	formerFence := leaderState.Fence()
	cluster.stop(t, leader)
	request.Fence = formerFence
	if result, deniedLease, readErr := cluster.owners[leader].ReadTransaction(ctx, request); !errors.Is(readErr, ErrOwnerClosed) || deniedLease != nil || len(result.Records) != 0 {
		t.Fatalf("former leader recovery result=%+v lease=%T err=%v",
			result, deniedLease, readErr)
	}
	removed := map[int]bool{leader: true}
	if err := cluster.owners[follower].Campaign(ctx, cluster.group); err != nil {
		t.Fatal(err)
	}
	newLeader := waitRF3Leader(t, ctx, cluster.owners[:], removed, cluster.group)
	request.Fence = mustRF3State(t, ctx, cluster.owners[newLeader], cluster.group).Fence()
	result, lease, err = cluster.owners[newLeader].ReadTransaction(ctx, request)
	if err != nil || lease == nil || len(result.Records) != 1 ||
		result.Records[0].ID != id || !bytes.Equal(result.Records[0].Payload, record) {
		t.Fatalf("replacement recovery result=%+v lease=%T err=%v", result, lease, err)
	}
	lease.Release()
}

func TestRF3IsolatedLeaderCannotCompleteTransactionRecoveryRead(t *testing.T) {
	cluster := newTransactionRF3Cluster(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := cluster.owners[0].Campaign(ctx, cluster.group); err != nil {
		t.Fatal(err)
	}
	leader := waitRF3Leader(t, ctx, cluster.owners[:], nil, cluster.group)
	leaderState := mustRF3State(t, ctx, cluster.owners[leader], cluster.group)
	for index := range cluster.owners {
		if index != leader {
			cluster.stop(t, index)
		}
	}

	readCtx, readCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer readCancel()
	result, lease, err := cluster.owners[leader].ReadTransaction(
		readCtx,
		TransactionReadRequest{
			Fence: leaderState.Fence(), Capability: serviceauthz.CapabilityTransactionRecovery,
			Read: replicatedstate.TransactionRecoveryReadRequest{
				Kind: replicatedstate.TransactionRecoveryLookupCoordinator,
				ID:   distributedtxn.ID{1}, MinimumApplied: max(uint64(1), leaderState.Status.Applied),
				MaxRows: 1, MaxBytes: uint32(replicatedstate.TransactionRecoverySummaryBytes +
					distributedtxn.MaxCoordinatorRecordBytes),
			},
		},
	)
	if !errors.Is(err, context.DeadlineExceeded) || lease != nil || len(result.Records) != 0 {
		t.Fatalf("isolated leader recovery result=%+v lease=%T err=%v", result, lease, err)
	}
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
