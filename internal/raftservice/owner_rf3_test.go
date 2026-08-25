package raftservice

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/storeio"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestAuthenticatedThreeVoterServingPutSurvivesLeaderLossAndExactRetry(t *testing.T) {
	const voters = 3
	runtimes := make([]*raftmember.Runtime, voters)
	bases := make([]sqldriver.ReplicatedShardStoreIdentity, voters)
	readSources := make([]*sqldriver.ReplicatedApply, voters)
	for index := range voters {
		runtimes[index], bases[index], readSources[index] = newRF3Runtime(t, uint64(index+1))
	}
	portableManifest := runtimes[0].Identity().RelationManifestDigest
	for index := 1; index < voters; index++ {
		if bases[index].RelationManifestDigest == bases[0].RelationManifestDigest {
			t.Fatal("independent replicas unexpectedly retained one local catalog manifest")
		}
		if runtimes[index].Identity().RelationManifestDigest != portableManifest {
			t.Fatalf(
				"replica %d portable manifest = %x, want %x",
				index+1, runtimes[index].Identity().RelationManifestDigest, portableManifest,
			)
		}
	}
	group := runtimes[0].Identity().Group

	members := make([]rafttransport.Member, voters)
	nodes := make([]rafttransport.NodeID, voters)
	for index := range voters {
		nodes[index][0] = byte(index + 1)
		members[index] = rafttransport.Member{
			Group: group, ReplicaSetVersion: 1,
			MemberID: uint64(index + 1), Node: nodes[index],
			Role: rafttransport.MemberVoter,
		}
	}
	registries := make(map[rafttransport.NodeID]*rafttransport.StaticRegistry, voters)
	for index := range voters {
		registry, err := rafttransport.NewStaticRegistry(
			nodes[index], members,
			rafttransport.Limits{MaxGroups: 1, MaxMembers: voters},
		)
		if err != nil {
			t.Fatal(err)
		}
		registries[nodes[index]] = registry
	}

	authority := newPeerServerTestAuthority(t)
	peerTLS := make([]*rafttransport.PeerTLS, voters)
	listeners := make([]net.Listener, voters)
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
	t.Cleanup(func() {
		for _, listener := range listeners {
			_ = listener.Close()
		}
	})
	dial := func(ctx context.Context, node rafttransport.NodeID) (net.Conn, error) {
		address, ok := addresses[node]
		if !ok {
			return nil, rafttransport.ErrNodeNotFound
		}
		return (&net.Dialer{}).DialContext(ctx, "tcp", address)
	}
	deadline := func() time.Time { return time.Now().Add(10 * time.Second) }

	owners := make([]*Owner, voters)
	contexts := make([]context.Context, voters)
	cancels := make([]context.CancelFunc, voters)
	runErrors := make([]chan error, voters)
	pulses := make([]chan struct{}, voters)
	peers := make([]*AuthenticatedPeerRuntime, voters)
	for index := range voters {
		serving, err := raftserve.NewRegistry(raftserve.Limits{
			MaxGroups: 1, MaxOutstandingIdentities: 32,
			MaxOutstandingAttempts: 64, MaxWaiters: 64,
			MaxAttemptsPerIdentity:     4,
			MaxRetainedCompletionBytes: 32 * int64(replication.MaxEmptyResultCompletionEnvelopeBytes),
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
		pulses[index] = make(chan struct{}, 1)
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
				CommandFences: []CommandFence{rf3CommandFence(runtimes[index].Identity(), bases[index])},
				ReadSources:   []ReadSource{readSources[index]},
				Pulse:         pulses[index],
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
		peers[index] = peer
		owners[index] = peer.Owner()
		contexts[index], cancels[index] = context.WithCancel(context.Background())
		runErrors[index] = make(chan error, 1)
	}
	for index := range voters {
		go func(index int) { runErrors[index] <- peers[index].Run(contexts[index]) }(index)
	}
	for index := range voters {
		select {
		case <-peers[index].Started():
			if !peers[index].Running() {
				t.Fatalf("peer %d did not publish authenticated serving", index)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("peer %d did not start", index)
		}
	}
	stopPulses := make(chan struct{})
	go pulseRF3(stopPulses, pulses)
	t.Cleanup(func() {
		close(stopPulses)
		for _, cancel := range cancels {
			cancel()
		}
		for _, done := range runErrors {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("RF3 owner did not stop")
			}
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := owners[0].Campaign(ctx, group); err != nil {
		t.Fatal(err)
	}
	leader := waitRF3Leader(t, ctx, owners, nil, group)
	leaderState, err := owners[leader].Probe(ctx, group)
	if err != nil {
		t.Fatal(err)
	}

	open := rf3Command(bases[leader], replication.CommandSessionOpen, 0, 1, nil)
	open.NextDeadlineUnixNano = 2_000_000_000_000_000_000
	openData := appendRF3Command(t, open)
	openResult, err := owners[leader].Submit(ctx, leaderState.Fence(), openData)
	if err != nil {
		t.Fatalf("session open: %v", err)
	}
	openCompletion, err := replication.OpenCompletion(openResult.Completion)
	if err != nil || openCompletion.ClientEpoch == 0 {
		t.Fatalf("session open completion = %+v, %v", openCompletion, err)
	}

	key, ok := orderedkey.AppendJSONString(nil, []byte(`"alpha"`), orderedkey.Ascending)
	if !ok {
		t.Fatal("encode primary key")
	}
	put := rf3Command(bases[leader], replication.CommandMutationBatch,
		openCompletion.ClientEpoch, 2, []replication.Mutation{{
			Kind: replication.MutationPut, Key: key,
			Value: []byte(`{"id":"alpha","value":1}`),
		}})
	putData := appendRF3Command(t, put)
	acknowledged, err := owners[leader].Submit(ctx, leaderState.Fence(), putData)
	if err != nil {
		t.Fatalf("RF3 Put: %v", err)
	}
	waitRF3Applied(t, ctx, owners, nil, group, acknowledged.Outcome.AppliedIndex)
	follower := (leader + 1) % voters
	followerRead, followerLease, followerState, err := readRF3PointAtFreshFence(
		t, ctx, owners[follower], readSources[follower], group, PointReadRequest{Relation: 1, Key: key,
			MinimumApplied: acknowledged.Outcome.AppliedIndex,
			MaxValueBytes:  replication.MaxMutationValueBytes,
		})
	if err != nil || !followerRead.Found ||
		!bytes.Equal(followerRead.Value, []byte(`{"id":"alpha","value":1}`)) {
		t.Fatalf("applied-bounded follower read=%+v err=%v", followerRead, err)
	}
	followerLease.Release()
	var lease PointReadLease
	if _, lease, _, err := readRF3PointAtFreshFence(t, ctx, owners[follower], readSources[follower], group, PointReadRequest{
		Relation: 1, Key: key,
		MinimumApplied: acknowledged.Outcome.AppliedIndex,
		MaxValueBytes:  len(followerRead.Value),
	}); !errors.Is(err, replicatedstate.ErrReadBufferBound) {
		t.Fatalf("request-relative response bound error=%v", err)
	} else if lease != nil {
		t.Fatal("response-bound refusal returned a lease")
	}
	if _, lease, followerState, err = readRF3PointAtFreshFence(t, ctx, owners[follower], readSources[follower], group, PointReadRequest{
		Relation: 1, Key: key,
		MinimumApplied: followerRead.Applied + 1,
		MaxValueBytes:  replication.MaxMutationValueBytes,
	}); !errors.Is(err, replicatedstate.ErrReadBehind) {
		t.Fatalf("future follower floor error=%v", err)
	} else if lease != nil {
		t.Fatal("failed follower read returned a lease")
	}
	staleFence := followerState.Fence()
	staleFence.Command.RelationManifestDigest[0] ^= 1
	if _, lease, err := owners[follower].ReadPoint(ctx, PointReadRequest{
		Fence: staleFence, Relation: 1, Key: key,
		MinimumApplied: acknowledged.Outcome.AppliedIndex,
		MaxValueBytes:  replication.MaxMutationValueBytes,
	}); !errors.Is(err, ErrServingFence) {
		t.Fatalf("stale manifest fence error=%v", err)
	} else if lease != nil {
		t.Fatal("stale manifest read returned a lease")
	}
	staleFence = followerState.Fence()
	staleFence.Command.ReplicaSetVersion++
	if _, lease, err := owners[follower].ReadPoint(ctx, PointReadRequest{
		Fence: staleFence, Relation: 1, Key: key,
		MinimumApplied: acknowledged.Outcome.AppliedIndex,
		MaxValueBytes:  replication.MaxMutationValueBytes,
	}); !errors.Is(err, ErrServingFence) {
		t.Fatalf("stale replica-set fence error=%v", err)
	} else if lease != nil {
		t.Fatal("stale replica-set read returned a lease")
	}
	leader = waitRF3Leader(t, ctx, owners, nil, group)
	linearRead, linearLease, leaderState, err := readRF3PointAtFreshFence(
		t, ctx, owners[leader], readSources[leader], group, PointReadRequest{Relation: 1, Key: key,
			MinimumApplied: acknowledged.Outcome.AppliedIndex,
			MaxValueBytes:  replication.MaxMutationValueBytes, Linearizable: true,
		})
	if err != nil || !linearRead.Found || !bytes.Equal(linearRead.Value, followerRead.Value) {
		t.Fatalf("ReadIndex leader read=%+v err=%v", linearRead, err)
	}
	linearLease.Release()
	staleTerm := leaderState.Fence()
	staleTerm.Term--
	if _, lease, err := owners[leader].ReadPoint(ctx, PointReadRequest{
		Fence: staleTerm, Relation: 1, Key: key,
		MinimumApplied: linearRead.Applied, MaxValueBytes: replication.MaxMutationValueBytes,
		Linearizable: true,
	}); !errors.Is(err, raftmodel.ErrNotLeader) || errors.Is(err, ErrServingFence) {
		t.Fatalf("stale read term lease=%T error=%v", lease, err)
	} else if lease != nil {
		t.Fatal("stale read term returned a lease")
	}
	if _, lease, leaderState, err = readRF3PointAtFreshFence(t, ctx, owners[leader], readSources[leader], group, PointReadRequest{
		Relation: 1, Key: key,
		MinimumApplied: linearRead.Applied + 1,
		MaxValueBytes:  replication.MaxMutationValueBytes, Linearizable: true,
	}); !errors.Is(err, replicatedstate.ErrReadBehind) {
		t.Fatalf("future ReadIndex floor error=%v", err)
	} else if lease != nil {
		t.Fatal("future ReadIndex floor returned a lease")
	}

	// Kill the exact serving leader after its acknowledged response. Remaining
	// voters elect another leader and replaying identical bytes returns the same
	// deterministic completion without duplicating the mutation.
	cancels[leader]()
	select {
	case <-owners[leader].Done():
	case <-ctx.Done():
		t.Fatalf("leader owner stop: %v", context.Cause(ctx))
	}
	if err := owners[(leader+1)%voters].Campaign(ctx, group); err != nil {
		t.Fatal(err)
	}
	newLeader := waitRF3Leader(t, ctx, owners, map[int]bool{leader: true}, group)
	newState, err := owners[newLeader].Probe(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	retried, err := owners[newLeader].Submit(ctx, newState.Fence(), putData)
	if err != nil {
		t.Fatalf("exact retry after leader loss: %v", err)
	}
	if !bytes.Equal(retried.Completion, acknowledged.Completion) {
		t.Fatal("exact retry returned a different deterministic completion")
	}
}

func rf3CommandFence(
	runtime raftmember.RuntimeIdentity,
	base sqldriver.ReplicatedShardStoreIdentity,
) CommandFence {
	authority := base.Binding.Authority
	return CommandFence{
		ReplicaSetVersion: 1, ActivePolicyGeneration: authority.ActivePolicyGeneration,
		ProtectionEpoch: authority.ProtectionEpoch, OwnershipEpoch: authority.OwnershipEpoch,
		SchemaGeneration:       authority.SchemaGeneration,
		RelationManifestDigest: runtime.RelationManifestDigest,
		RoutingVersion:         authority.RoutingVersion,
		RouteGeneration:        authority.RouteGeneration,
	}
}

func pulseRF3(stop <-chan struct{}, pulses []chan struct{}) {
	// Preserve enough wall time between logical ticks for strict WAL sync and
	// authenticated vote delivery; a scheduler-scale logical tick creates
	// election storms on valid but slower filesystems.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			for _, pulse := range pulses {
				select {
				case pulse <- struct{}{}:
				default:
				}
			}
		}
	}
}

func waitRF3Leader(
	t testing.TB,
	ctx context.Context,
	owners []*Owner,
	removed map[int]bool,
	group raftmember.GroupKey,
) int {
	t.Helper()
	for ctx.Err() == nil {
		leader := -1
		var term uint64
		consistent := true
		for index, owner := range owners {
			if removed[index] {
				continue
			}
			state, err := owner.Probe(ctx, group)
			status := state.Status
			if err != nil || status.LeaderID == 0 {
				consistent = false
				break
			}
			candidate := int(status.LeaderID - 1)
			if removed[candidate] {
				consistent = false
				break
			}
			if leader == -1 {
				leader = candidate
				term = status.Term
			} else if leader != candidate || term != status.Term {
				consistent = false
				break
			}
		}
		if consistent && leader >= 0 {
			state, err := owners[leader].Probe(ctx, group)
			status := state.Status
			if err == nil && status.MemberID == status.LeaderID && status.Term == term {
				return leader
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("RF3 leader election: %v", context.Cause(ctx))
	return -1
}

func readRF3PointAtFreshFence(
	t testing.TB,
	ctx context.Context,
	owner *Owner,
	source ReadSource,
	group raftmember.GroupKey,
	request PointReadRequest,
) (PointReadResult, PointReadLease, ServingState, error) {
	t.Helper()
	for ctx.Err() == nil {
		state, err := owner.Probe(ctx, group)
		if err != nil {
			return PointReadResult{}, nil, ServingState{}, err
		}
		request.Fence = state.Fence()
		result, lease, err := owner.ReadPoint(ctx, request)
		if errors.Is(err, ErrServingFence) {
			if lease != nil {
				t.Fatal("stale serving fence returned a read lease")
			}
			refreshed, probeErr := owner.Probe(ctx, group)
			if probeErr != nil {
				return PointReadResult{}, nil, ServingState{}, probeErr
			}
			if refreshed.Fence() == request.Fence {
				direct, directErr := source.PointReadInto(
					request.Relation, request.Key, request.MinimumApplied,
					request.MaxValueBytes, nil,
				)
				t.Fatalf("unchanged serving fence rejected: request=%+v snapshot=%+v direct=%v",
					request.Fence, direct.Fence, directErr)
			}
			// Let the pulse/transport goroutines settle the term observed by
			// Probe; a tight test retry loop can otherwise create its own
			// scheduler starvation on small CI runners.
			time.Sleep(time.Millisecond)
			continue
		}
		return result, lease, state, err
	}
	return PointReadResult{}, nil, ServingState{}, context.Cause(ctx)
}

func waitRF3Applied(
	t testing.TB,
	ctx context.Context,
	owners []*Owner,
	removed map[int]bool,
	group raftmember.GroupKey,
	index uint64,
) {
	t.Helper()
	for ctx.Err() == nil {
		complete := true
		for member, owner := range owners {
			if removed[member] {
				continue
			}
			state, err := owner.Probe(ctx, group)
			status := state.Status
			if err != nil || status.Applied < index {
				complete = false
				break
			}
		}
		if complete {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("RF3 apply %d: %v", index, context.Cause(ctx))
}

func appendRF3Command(t testing.TB, command replication.Command) []byte {
	t.Helper()
	data, err := replication.AppendCommand(nil, command)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func rf3Command(
	base sqldriver.ReplicatedShardStoreIdentity,
	kind replication.CommandKind,
	epoch, sequence uint64,
	mutations []replication.Mutation,
) replication.Command {
	binding := base.Binding
	command := replication.Command{
		Kind:      kind,
		ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch,
		Distribution:          binding.Distribution, Shard: binding.Shard,
		AllocationGeneration: binding.AllocationGeneration,
		ShardIncarnation:     binding.ShardIncarnation, GroupID: binding.GroupID,
		ReplicaSetVersion:      1,
		ActivePolicyGeneration: binding.Authority.ActivePolicyGeneration,
		ProtectionEpoch:        binding.Authority.ProtectionEpoch,
		OwnershipEpoch:         binding.Authority.OwnershipEpoch,
		SchemaGeneration:       binding.Authority.SchemaGeneration,
		RoutingVersion:         binding.Authority.RoutingVersion,
		RouteGeneration:        binding.Authority.RouteGeneration,
		Tenant:                 []byte("tenant"), ClientID: replication.ID128{0x44},
		ClientEpoch: epoch, ClientSequence: sequence,
		Fingerprint: sha256.Sum256([]byte{byte(kind), byte(sequence)}),
	}
	if len(mutations) != 0 {
		command.Batches = []replication.RelationMutationBatch{{Relation: 1, Mutations: mutations}}
	}
	return command
}

func newRF3Runtime(
	t testing.TB,
	memberID uint64,
) (*raftmember.Runtime, sqldriver.ReplicatedShardStoreIdentity, *sqldriver.ReplicatedApply) {
	t.Helper()
	identity := raftstore.Identity{
		Distribution: "orders", Shard: "0000-ffff",
		AllocationGeneration: 7, MemberID: memberID,
	}
	for index := range identity.ClusterID {
		identity.ClusterID[index] = byte(index + 1)
		identity.ClusterIncarnation[index] = byte(index + 21)
		identity.ShardIncarnation[index] = byte(index + 41)
		identity.GroupID[index] = byte(index + 61)
		identity.StoreID[index] = byte(index+81) ^ byte(memberID)
	}
	key := raftstore.Key{ID: "rf3-serving-key", Wrapped: []byte("opaque-wrapped-key")}
	for index := range key.Material {
		key.Material[index] = byte(index + 1)
	}
	baseIndex, baseTerm := uint64(1), uint64(1)
	wal, err := raftstore.Create(
		filepath.Join(t.TempDir(), "member.wal"), identity, key,
		raftstore.Bootstrap{TopologyRecoveryEpoch: 3, Snapshot: &pb.Snapshot{
			Data: []byte("rf3-serving-bootstrap"),
			Metadata: &pb.SnapshotMetadata{Index: &baseIndex, Term: &baseTerm,
				ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}},
		}},
		raftstore.Options{MaxFileBytes: 256 << 20,
			MaxRecordBytes: raftstore.DefaultMaxRecordBytes, MaxRecords: 4096,
			MaxEntries: 16384, MaxLiveBytes: raftstore.DefaultMaxLiveBytes},
	)
	if err != nil {
		t.Fatal(err)
	}
	database, err := sqldriver.InitializeShardStore(
		filepath.Join(t.TempDir(), "member.vdb"), sqldriver.ShardStoreBinding{
			Distribution:         distribution.DistributionName(identity.Distribution),
			Shard:                distribution.ShardID(identity.Shard),
			AllocationGeneration: distribution.ShardAllocationGeneration(identity.AllocationGeneration),
		},
	)
	if err != nil {
		_ = wal.Close()
		t.Fatal(err)
	}
	session, err := database.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := session.Prepare(context.Background(), `CREATE TABLE docs (PRIMARY KEY (id))`)
	if err == nil {
		_, err = prepared.Exec(context.Background(), nil)
	}
	if prepared != nil {
		err = errors.Join(err, prepared.Close())
	}
	err = errors.Join(err, session.Close())
	if err != nil {
		t.Fatal(err)
	}
	authority := sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: 5, ProtectionEpoch: 7, OwnershipEpoch: 11,
		SchemaGeneration: 13, RoutingVersion: 17, RouteGeneration: 19,
	}
	base, err := raftmember.BindPreparedSQL(wal, database, authority, "docs")
	if errors.Is(err, storeio.ErrStrictAllocationUnsupported) {
		_ = database.Close()
		_ = wal.Close()
		t.Skipf("strict allocation unsupported: %v", err)
	}
	if err != nil {
		t.Fatal(err)
	}
	apply, _, err := raftmember.OpenPreparedApply(
		wal, database, authority, base, sqldriver.ReplicatedApplyOptions{
			MaxSessions: 32, RetryWindow: 8,
			TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20},
			Placement: sqldriver.ReplicatedPlacementProfile{
				Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "/id",
				TupleVersion:  distribution.CurrentTupleVersion,
				MapperVersion: distribution.NativeMapperVersion,
				Range:         distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := wal.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	runtime, err := raftmember.AdoptRuntime(wal, database, apply)
	if err != nil {
		t.Fatal(err)
	}
	return runtime, base, apply
}

func rf3HostLimits() multiraft.Limits {
	return multiraft.Limits{
		MaxGroups: 1, MaxQueueItems: 256, MaxQueueBytes: 128 << 20,
		MaxGroupItems: 256, MaxGroupBytes: 128 << 20,
		MaxOutboxItems: 256, MaxOutboxBytes: 128 << 20,
		MaxPendingTicks: 16,
	}
}
