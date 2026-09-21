package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestRF3DynamicLearnerCanceledStageReleasesStorage(t *testing.T) {
	f := newRF3NodeRecoveryFixture(t)
	bootstrap := rf3testfixture.InitialBootstrap([]uint64{1, 2, 3})
	options := rf3testfixture.MemberOptions{Root: t.TempDir(), Table: "docs",
		CreateTable: `CREATE TABLE docs (PRIMARY KEY (id))`, Identity: rf3CommandStoreIdentity(1),
		Key: f.key, Bootstrap: bootstrap, Authority: rf3CommandAuthority(), Apply: f.applyOptions}
	source, err := rf3testfixture.PrepareMember(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	conf := &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
	if _, err = source.Apply.ApplyConfiguration(raftmodel.ApplyMeta{Index: 2, Term: 1, Type: pb.EntryConfChange}, conf); err != nil {
		t.Fatal(err)
	}
	cut, err := source.Apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var payload bytes.Buffer
	manifest, err := replicatedstate.WriteSnapshotArtifact(&payload, cut, replicatedstate.SnapshotArtifactOptions{})
	if err = errors.Join(err, cut.Close()); err != nil {
		t.Fatal(err)
	}
	options.Root, options.Identity.MemberID, options.Identity.StoreID = t.TempDir(), 4, [16]byte{94}
	target, err := rf3testfixture.PrepareSnapshotTarget(options)
	if err != nil {
		t.Fatal(err)
	}
	if err = target.Close(); err != nil {
		t.Fatal(err)
	}
	descriptor := snapshottransfer.Descriptor{Group: groupFromBinding(target.Base.Binding), SourceMember: 1,
		TargetMember: 4, TargetStore: target.Base.Binding.StoreID, TargetIncarnation: 1,
		SchemaGeneration: target.Base.Binding.Authority.SchemaGeneration, ReplicaSetVersion: manifest.State.ReplicaSetVersion,
		SnapshotIndex: manifest.State.Applied, SnapshotTerm: manifest.State.LastTerm, Lineage: manifest.State.LastEntryDigest,
		ArtifactHash: sha256.Sum256(payload.Bytes()), ArtifactBytes: uint64(payload.Len()), ChunkBytes: snapshottransfer.MinChunkBytes}
	repository, err := snapshottransfer.OpenRepository(t.TempDir(), snapshottransfer.Limits{MaxArtifacts: 1, MaxArtifactBytes: 1 << 20, MaxDiskBytes: 2 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	for offset := uint64(0); offset < descriptor.ArtifactBytes; {
		end := min(offset+uint64(descriptor.ChunkBytes), descriptor.ArtifactBytes)
		chunk := payload.Bytes()[offset:end]
		offset, _, err = repository.Append(descriptor, offset, chunk, sha256.Sum256(chunk))
		if err != nil {
			t.Fatal(err)
		}
	}
	cursor, err := replicatedstate.OpenSnapshotCursorStore(filepath.Join(t.TempDir(), "cursor"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cursor.Close() })
	journal, err := replicaaction.OpenFileJournal(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = journal.Close() })
	config := migrationbudget.DefaultConfig()
	config.MaxActive = 1
	budget, err := migrationbudget.New(config)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := budget.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	intent := rf3RecoveryEnrollmentIntent()
	intent.Group = descriptor.Group
	intent.Target.Member, intent.Target.StoreID, intent.Target.NodeIncarnation = 4, descriptor.TargetStore, 1
	proof := rf3ReservationProof(intent)
	intent.Proof = &proof
	installer := &rf3DynamicLearnerInstaller{intent: intent, proof: proof, reservationRoot: options.Root,
		repository: repository, cursor: cursor, base: target.Base, applyIdentity: target.ApplyIdentity,
		staticBootstrap: bootstrap.Snapshot, factory: &rf3DynamicLearnerFactory{
			owner: &rf3NodeOwner{store: f.store}, runtime: &rf3NodeRuntime{actionJournal: journal}, budget: budget,
			deadline: func() time.Time { return time.Now().Add(time.Second) },
		}}
	t.Cleanup(func() { _ = installer.close() })
	// Hold the active permit so cancellation happens after the real SQL stage
	// opens, without racing a timer against filesystem setup. Each retry must
	// reach that same boundary and release every exclusive storage owner.
	for attempt := 0; attempt < 2; attempt++ {
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() { _, err := installer.InstallPublishedLearner(ctx, descriptor); result <- err }()
		deadline := time.Now().Add(5 * time.Second)
		for budget.Metrics().Waiting == 0 && time.Now().Before(deadline) {
			select {
			case err := <-result:
				cancel()
				t.Fatalf("attempt %d did not reach staged receive: %v", attempt, err)
			default:
				time.Sleep(time.Millisecond)
			}
		}
		waiting := budget.Metrics().Waiting
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) || waiting != 1 {
			t.Fatalf("attempt %d cancellation: waiting=%d error=%v", attempt, waiting, err)
		}
		reopened, err := sqldriver.OpenReplicatedSnapshotTarget(target.SQLPath, target.Base, target.ApplyIdentity)
		if err != nil {
			t.Fatalf("attempt %d retained snapshot writer: %v", attempt, err)
		}
		if err = reopened.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRF3RemovalSnapshotRestoresSurvivorCatchupAuthority(t *testing.T) {
	f := newRF3NodeRecoveryFixture(t)
	source, err := rf3testfixture.PrepareMember(rf3testfixture.MemberOptions{
		Root: t.TempDir(), Table: "docs", CreateTable: `CREATE TABLE docs (PRIMARY KEY (id))`,
		Identity: rf3CommandStoreIdentity(1), Key: f.key,
		Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
		Authority: rf3CommandAuthority(), Apply: f.applyOptions,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = source.Close() })
	for index, conf := range []*pb.ConfState{
		{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}},
		{Voters: []uint64{1, 2, 3, 4}},
		{Voters: []uint64{2, 3, 4}},
	} {
		if _, err := source.Apply.ApplyConfiguration(raftmodel.ApplyMeta{Index: uint64(index + 2), Term: 2, Type: pb.EntryConfChange}, conf); err != nil {
			t.Fatal(err)
		}
	}
	cut, err := source.Apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "removed.snapshot")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = replicatedstate.WriteSnapshotArtifact(file, cut, replicatedstate.SnapshotArtifactOptions{})
	if err = errors.Join(err, cut.Close(), file.Close(), source.Close()); err != nil {
		t.Fatal(err)
	}
	file, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := replicatedstate.VerifySnapshotArtifact(file, replicatedstate.SnapshotArtifactCallbacks{})
	if err = errors.Join(err, file.Close()); err != nil {
		t.Fatal(err)
	}
	if manifest.State.ReplicaSetVersion != 4 {
		t.Fatalf("lost durable membership: %+v", manifest.State)
	}
	// Construct a new transport solely from the recovered snapshot authority.
	// The surviving voter missed the removal commit; no process-local previous
	// view, cached grant, or hand-written predecessor may be needed to resume it.
	state := manifest.State
	group := raftmember.GroupKey{ClusterID: state.Binding.ClusterID,
		ClusterIncarnation:    state.Binding.ClusterIncarnation,
		TopologyRecoveryEpoch: state.Binding.TopologyRecoveryEpoch,
		ShardIncarnation:      state.Binding.ShardIncarnation, GroupID: state.Binding.GroupID}
	openRegistry := func(local byte, version uint64, voters []uint64) *rafttransport.StaticRegistry {
		t.Helper()
		members := make([]rafttransport.Member, 4)
		for i := range members {
			id := uint64(i + 1)
			role := rafttransport.MemberEnrolled
			if slices.Contains(voters, id) {
				role = rafttransport.MemberVoter
			}
			members[i] = rafttransport.Member{Group: group, ReplicaSetVersion: version,
				MemberID: id, Node: rafttransport.NodeID{byte(id)}, Role: role}
		}
		registry, err := rafttransport.NewStaticRegistry(rafttransport.NodeID{local}, members,
			rafttransport.Limits{MaxGroups: 1, MaxMembers: 4})
		if err != nil {
			t.Fatal(err)
		}
		return registry
	}
	leader := openRegistry(2, state.ReplicaSetVersion, state.ConfState.Voters)
	if err := leader.PublishCommittedAuthority(group, state.ReplicaSetVersion, state.ConfState); err != nil {
		t.Fatal(err)
	}
	follower := openRegistry(3, 3, []uint64{1, 2, 3, 4})
	removed := openRegistry(1, 3, []uint64{1, 2, 3, 4})
	send := func(sender, receiver *rafttransport.StaticRegistry, kind pb.MessageType) error {
		t.Helper()
		from, to := uint64(sender.LocalNode()[0]), uint64(receiver.LocalNode()[0])
		message := &pb.Message{Type: kind.Enum(), From: proto.Uint64(from), To: proto.Uint64(to),
			Term: proto.Uint64(2), LogTerm: proto.Uint64(2), Index: proto.Uint64(state.Applied), Commit: proto.Uint64(state.Applied)}
		frame, _, err := sender.EncodeOutbound(nil, raftmember.OutboundMessage{Group: group, From: from, To: to, Message: message})
		if err != nil {
			t.Fatal(err)
		}
		_, err = receiver.DecodeInbound(rafttransport.PeerIdentity{TrustDomain: sender.TrustDomain(), Node: sender.LocalNode()}, frame)
		return err
	}
	if err := send(leader, follower, pb.MsgHeartbeat); err != nil {
		t.Fatalf("snapshot cannot deliver missing removal commit: %v", err)
	}
	for _, kind := range []pb.MessageType{pb.MsgHeartbeatResp, pb.MsgAppResp} {
		if err := send(follower, leader, kind); err != nil {
			t.Fatalf("snapshot cannot receive surviving voter progress: %v", err)
		}
		if err := send(removed, leader, kind); !errors.Is(err, rafttransport.ErrUnauthorized) {
			t.Fatalf("removed member regained progress authority: %v", err)
		}
	}
	for _, kind := range []pb.MessageType{pb.MsgApp, pb.MsgVote} {
		if err := send(follower, leader, kind); err != nil {
			t.Fatalf("surviving voter cannot reconcile its Raft state: %v", err)
		}
	}
}
