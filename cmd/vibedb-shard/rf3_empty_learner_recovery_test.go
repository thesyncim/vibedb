package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/membershipgrant"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
)

func rf3RecoveryEnrollmentIntent() gateway.GroupEnrollmentIntent {
	intent := gateway.GroupEnrollmentIntent{
		IntentID: [32]byte{10}, Group: raftmember.GroupKey{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}},
		Distribution: "data", Shard: "0", AllocationGeneration: 1, CatalogGeneration: 1,
		Source:               gateway.ReplicaIdentity{Member: 1, Node: rafttransport.NodeID{5}, NodeIncarnation: 1, StoreID: [16]byte{7}, Endpoint: "source-data", NativeEndpoint: "source-native", ControlEndpoint: "source-control"},
		Target:               gateway.ReplicaIdentity{Member: 4, Node: rafttransport.NodeID{6}, NodeIncarnation: 1, StoreID: [16]byte{8}, Endpoint: "target-data", NativeEndpoint: "target-native", ControlEndpoint: "target-control"},
		SnapshotSourceMember: 1, ExpectedRosterDigest: replication.Digest{13}, ExpectedDescriptorDigest: replication.Digest{14}, ExpectedManifestDigest: replication.Digest{15},
		ExpectedCommand:    raftservice.CommandFence{ReplicaSetVersion: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1, SchemaGeneration: 1, RelationManifestDigest: replication.Digest{9}, RoutingVersion: 1, RouteGeneration: 1},
		TargetNodeRevision: 1, State: gateway.EnrollmentPrepared, Revision: 1, ExpectedCatalogHeadDigest: replication.Digest{15},
	}
	proof := rf3ReservationProof(intent)
	intent.Proof = &proof
	return intent
}

func TestRF3NodeControlAdoptionReceiptRestoresReceiver(t *testing.T) {
	intent := rf3RecoveryEnrollmentIntent()
	proof := *intent.Proof
	if !intent.Valid() || !proof.Valid() {
		t.Fatal("invalid test intent")
	}
	root := t.TempDir()
	reservation := rf3EnrollmentReservationPath(root, intent.IntentID)
	if err := os.MkdirAll(reservation, 0700); err != nil {
		t.Fatal(err)
	}
	receipt := rf3EnrollmentReceiverReceipt{Kind: rf3EnrollmentPayloadKind, IntentID: intent.IntentID, IntentDigest: intent.Digest(), Group: intent.Group, TargetMember: intent.Target.Member, TargetNode: intent.Target.Node, TargetNodeIncarnation: intent.Target.NodeIncarnation, TargetStoreID: intent.Target.StoreID, ProofDigest: proof.EnrollmentDigest}
	raw, err := vibejson.Marshal(&receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeRF3DurableMarker(filepath.Join(reservation, rf3EnrollmentReceiverFile), raw); err != nil {
		t.Fatal(err)
	}
	activationErr := errors.New("receiver unavailable")
	calls := 0
	adopter := &rf3NodeControlAdopter{NodeRoot: root, ActivateReceiver: func(_ context.Context, got gateway.GroupEnrollmentIntent, actual gateway.PreparedReplicaProof) error {
		calls++
		if got.Digest() != intent.Digest() || actual != proof {
			t.Fatal("wrong activation identity")
		}
		return activationErr
	}}
	if found, err := adopter.ObserveAdopted(t.Context(), intent, proof); found || !errors.Is(err, activationErr) || calls != 1 {
		t.Fatalf("durable receipt hid lost receiver: found=%t err=%v calls=%d", found, err, calls)
	}
	activationErr = nil
	if found, err := adopter.ObserveAdopted(t.Context(), intent, proof); !found || err != nil || calls != 2 {
		t.Fatalf("receiver was not restored: found=%t err=%v calls=%d", found, err, calls)
	}
}

func TestRF3DynamicLearnerRecoveryBoundsLiveGroupsInsteadOfHistory(t *testing.T) {
	root := t.TempDir()
	for index := 0; index <= maxRF3ManifestGroups; index++ {
		path := filepath.Join(root, "enrollments", fmt.Sprintf("%064x", index+1))
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	factory := &rf3DynamicLearnerFactory{root: root, runtime: &rf3EmptyNodeRuntime{reader: new(nodecontrol.IntentReaderSlot)}}
	if err := factory.Recover(t.Context()); err != nil {
		t.Fatalf("historical reservations prevented startup: %v", err)
	}
}

type rf3EnrollmentRecoveryReadFunc func(context.Context, [32]byte) (nodecontrol.BootstrapReadReply, error)

func (read rf3EnrollmentRecoveryReadFunc) ReadEnrollmentRecovery(ctx context.Context, id [32]byte) (nodecontrol.BootstrapReadReply, error) {
	return read(ctx, id)
}

func (rf3EnrollmentRecoveryReadFunc) ReadEnrollmentIntent(context.Context, [32]byte) (gateway.GroupEnrollmentIntent, error) {
	return gateway.GroupEnrollmentIntent{}, nodecontrol.ErrBootstrapReadUnavailable
}

func TestRF3DynamicLearnerRecoverySkipsOnlyCertifiedMissingHistory(t *testing.T) {
	for _, test := range []struct {
		name   string
		count  int
		change func(*nodecontrol.BootstrapReadReply) error
		want   error
	}{
		{name: "more retired reservations than live capacity", count: maxRF3ManifestGroups + 1},
		{name: "reader unavailable", count: 1, change: func(*nodecontrol.BootstrapReadReply) error { return nodecontrol.ErrBootstrapReadUnavailable }, want: nodecontrol.ErrBootstrapReadUnavailable},
		{name: "missing witnesses", count: 1, change: func(reply *nodecontrol.BootstrapReadReply) error {
			reply.CatalogHeadDigest = replication.Digest{}
			return nil
		}, want: nodecontrol.ErrStale},
		{name: "different request", count: 1, change: func(reply *nodecontrol.BootstrapReadReply) error { reply.IntentID = [32]byte{99}; return nil }, want: nodecontrol.ErrStale},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			intent := rf3RecoveryEnrollmentIntent()
			for index := 0; index < test.count; index++ {
				intent.IntentID = [32]byte{byte(index + 1)}
				proof := rf3ReservationProof(intent)
				intent.Proof = &proof
				reservation := rf3EnrollmentReservationPath(root, intent.IntentID)
				if err := os.MkdirAll(reservation, 0700); err != nil {
					t.Fatal(err)
				}
				descriptor := snapshottransfer.Descriptor{Group: intent.Group, SourceMember: 1, TargetMember: 4, TargetStore: intent.Target.StoreID, TargetIncarnation: 1, SchemaGeneration: 1, ReplicaSetVersion: 1, SnapshotIndex: 1, SnapshotTerm: 1, Lineage: [32]byte{1}, ArtifactHash: [32]byte{2}, ArtifactBytes: 4096, ChunkBytes: 4096}
				if err := persistRF3EnrollmentDescriptor(reservation, intent, descriptor); err != nil {
					t.Fatal(err)
				}
			}
			node := gateway.NodeRecord{NodeID: intent.Target.Node, Incarnation: 1, ServiceKeyDigest: replication.Digest{1},
				DataEndpoint: intent.Target.Endpoint, NativeEndpoint: intent.Target.NativeEndpoint, ControlEndpoint: intent.Target.ControlEndpoint,
				DataAddress: string(intent.Target.Endpoint), NativeAddress: string(intent.Target.NativeEndpoint), ControlAddress: string(intent.Target.ControlEndpoint),
				FailureDomain: "test", Roles: gateway.NodeRoleStorage, Lifecycle: gateway.NodeActive, Revision: 1, CatalogGeneration: 1}
			calls := 0
			slot := new(nodecontrol.IntentReaderSlot)
			if err := slot.Set(rf3EnrollmentRecoveryReadFunc(func(_ context.Context, id [32]byte) (nodecontrol.BootstrapReadReply, error) {
				calls++
				reply := nodecontrol.BootstrapReadReply{Nonce: [16]byte{1}, Operation: nodecontrol.OpReadOwnEnrollmentRecovery,
					PhysicalNode: node.NodeID, Incarnation: node.Incarnation, IntentID: id, IntentMissing: true, Node: node,
					DirectoryCutRevision: 1, DirectoryCutDigest: replication.Digest{1}, CatalogGeneration: 1,
					CatalogHeadDigest: replication.Digest{2}, EnrollmentDirectoryDigest: replication.Digest{3}}
				if !reply.EnrollmentMissing() {
					t.Fatal("invalid absence fixture")
				}
				if test.change != nil {
					err := test.change(&reply)
					return reply, err
				}
				return reply, nil
			})); err != nil {
				t.Fatal(err)
			}
			factory := &rf3DynamicLearnerFactory{root: root, runtime: &rf3EmptyNodeRuntime{reader: slot}}
			if err := factory.Recover(t.Context()); !errors.Is(err, test.want) {
				t.Fatalf("recovery=%v want=%v", err, test.want)
			}
			if calls != test.count {
				t.Fatalf("read calls=%d want=%d", calls, test.count)
			}
			entries, err := os.ReadDir(filepath.Join(root, "enrollments"))
			if err != nil || len(entries) != test.count {
				t.Fatalf("retained artifacts=%d err=%v", len(entries), err)
			}
		})
	}
}

func TestRF3DynamicLearnerRecoversRegistrationBeforeRuntimeReceipt(t *testing.T) {
	f := newRF3NodeRecoveryFixtureWithLearner(t, true)
	if _, err := f.store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	intent := rf3RecoveryEnrollmentIntent()
	intent.Group = groupFromBinding(f.bases[0].Binding)
	intent.Target.Node = rafttransport.NodeID(f.node.NodeID)
	intent.Target.StoreID = f.bases[0].Binding.StoreID
	group, _ := f.store.GroupByID(intent.Group.GroupID)
	database, apply, err := openRF3SelectedLog(f.paths[0], group, f.bases[0], f.applies[0])
	if err != nil {
		t.Fatal(err)
	}
	profile, err := apply.CapacityQualificationProfile()
	if err != nil {
		t.Fatal(err)
	}
	intent.ExpectedCommand.RelationManifestDigest = profile.RelationManifestDigest
	if err := errors.Join(apply.Close(), database.Close()); err != nil {
		t.Fatal(err)
	}
	proof := rf3ReservationProof(intent)
	intent.Proof = &proof
	descriptor := snapshottransfer.Descriptor{Group: intent.Group, SourceMember: 1, TargetMember: 4, TargetStore: intent.Target.StoreID, TargetIncarnation: 1, SchemaGeneration: 1, ReplicaSetVersion: 1, SnapshotIndex: 1, SnapshotTerm: 1, Lineage: [32]byte{1}, ArtifactHash: [32]byte{2}, ArtifactBytes: 4096, ChunkBytes: 4096}
	if !intent.Valid() || !proof.Valid() || !descriptor.Valid() || !rf3BootstrapIntentProofMatches(intent, proof) {
		t.Fatalf("invalid recovery fixture intent=%t proof=%t descriptor=%t command=%+v", intent.Valid(), proof.Valid(), descriptor.Valid(), intent.ExpectedCommand)
	}
	reservation := filepath.Dir(f.paths[0])
	for restart := 0; restart < 2; restart++ {
		f.reopen(t)
		owner, err := newRF3NodeOwner(f.store)
		if err != nil {
			t.Fatal(err)
		}
		installer := &rf3DynamicLearnerInstaller{factory: &rf3DynamicLearnerFactory{owner: owner, deadline: func() time.Time { return time.Now().Add(time.Minute) }}, intent: intent, proof: proof, reservationRoot: reservation, base: f.bases[0], applyIdentity: f.applies[0], spec: nodecontrol.PreparationSpec{InitialVoters: [3]nodecontrol.PreparationMember{{MemberID: 1}, {MemberID: 2}, {MemberID: 3}}}}
		if restart == 1 {
			// Placement may already advertise a later schema while this exact
			// replica still needs to replay it from its retained local generation.
			installer.recovery = &nodecontrol.BootstrapReadReply{CurrentRoute: &gateway.ReplicatedMembershipRoute{Serving: gateway.ReplicatedRoute{Command: intent.ExpectedCommand}}}
			installer.recovery.CurrentRoute.Serving.Command.SchemaGeneration++
			installer.recovery.CurrentRoute.Serving.Command.RelationManifestDigest = replication.Digest{99}
		}
		runtime, apply, found, err := installer.recoverRuntime(t.Context(), descriptor)
		if err != nil || !found || runtime == nil || apply == nil {
			_ = owner.Close()
			t.Fatalf("restart=%d recovered=%t err=%v", restart, found, err)
		}
		if identity, found, err := readRF3EnrollmentRuntime(reservation, intent, proof, descriptor); err != nil || !found || identity != runtime.Identity() {
			t.Fatalf("registration receipt was not repaired: identity=%+v found=%t err=%v", identity, found, err)
		}
		if err := errors.Join(runtime.Close(), owner.Close()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRF3DynamicLearnerCompletedMissingRegistrationFailsClosed(t *testing.T) {
	f := newRF3NodeRecoveryFixtureWithLearner(t, true)
	owner, err := newRF3NodeOwner(f.store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	intent := rf3RecoveryEnrollmentIntent()
	intent.Group.GroupID = [16]byte{99}
	proof := rf3ReservationProof(intent)
	intent.Proof = &proof
	descriptor := snapshottransfer.Descriptor{Group: intent.Group, SourceMember: 1, TargetMember: 4, TargetStore: intent.Target.StoreID, TargetIncarnation: 1, SchemaGeneration: 1, ReplicaSetVersion: 1, SnapshotIndex: 1, SnapshotTerm: 1, Lineage: [32]byte{1}, ArtifactHash: [32]byte{2}, ArtifactBytes: 4096, ChunkBytes: 4096}
	installer := &rf3DynamicLearnerInstaller{factory: &rf3DynamicLearnerFactory{owner: owner},
		intent: intent, proof: proof, reservationRoot: t.TempDir()}
	if runtime, apply, found, err := installer.recoverRuntime(t.Context(), descriptor); err != nil || found || runtime != nil || apply != nil {
		t.Fatalf("in-progress transfer absence: found=%t err=%v", found, err)
	}
	// This marker is set by factory recovery only after a fresh placement cut
	// proves the completed target still serves. Its data must already exist.
	installer.recovery = &nodecontrol.BootstrapReadReply{}
	if runtime, apply, found, err := installer.recoverRuntime(t.Context(), descriptor); !errors.Is(err, nodecontrol.ErrJournalCorrupt) || found || runtime != nil || apply != nil {
		t.Fatalf("completed serving replica absence: found=%t err=%v", found, err)
	}
}

func TestRF3RecoveredCommandRetainsLocalSchemaUntilReplay(t *testing.T) {
	current := rf3RecoveryEnrollmentIntent().ExpectedCommand
	current.SchemaGeneration = 2
	localManifest := replication.Digest{99}
	got, err := rf3RecoveredCommand(current, 1, localManifest)
	if err != nil || got.SchemaGeneration != 1 || got.RelationManifestDigest != localManifest {
		t.Fatalf("restore local schema: command=%+v err=%v", got, err)
	}
	got.SchemaGeneration, got.RelationManifestDigest = current.SchemaGeneration, current.RelationManifestDigest
	if got != current {
		t.Fatal("recovery changed current placement fences")
	}
	if _, err = rf3RecoveredCommand(current, 2, localManifest); !errors.Is(err, nodecontrol.ErrStale) {
		t.Fatalf("accepted conflicting current schema: %v", err)
	}
	if _, err = rf3RecoveredCommand(current, 3, localManifest); !errors.Is(err, nodecontrol.ErrStale) {
		t.Fatalf("accepted schema beyond current authority: %v", err)
	}
}

func TestRF3DynamicLearnerRecoveryPreservesAdvancedCheckpoint(t *testing.T) {
	initial := &pb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}
	descriptor := snapshottransfer.Descriptor{SnapshotIndex: 10, SnapshotTerm: 2}
	for _, test := range []struct {
		name          string
		index, term   uint64
		state         *pb.ConfState
		receipt, want bool
	}{
		{"registration cut without receipt", 10, 2, initial, false, true},
		{"older checkpoint", 9, 2, initial, true, false},
		{"conflicting registration", 10, 3, initial, true, false},
		{"conflicting membership", 10, 2, &pb.ConfState{Voters: []uint64{1, 2, 4}}, true, false},
		{"checkpoint after promotion", 20, 3, &pb.ConfState{Voters: []uint64{2, 3, 4}}, true, true},
		{"unproved advanced checkpoint", 20, 3, initial, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			checkpoint := &pb.Snapshot{Metadata: &pb.SnapshotMetadata{Index: &test.index, Term: &test.term, ConfState: test.state}}
			if got := rf3RecoveredCheckpointMatches(checkpoint, descriptor, initial, test.receipt); got != test.want {
				t.Fatalf("matches=%t want=%t", got, test.want)
			}
		})
	}
}

func TestRF3RecoveredRosterUsesLocalMembershipAndCurrentPlacement(t *testing.T) {
	intent := rf3RecoveryEnrollmentIntent()
	descriptor := snapshottransfer.Descriptor{Group: intent.Group, TargetMember: 4, ReplicaSetVersion: 2}
	spec := nodecontrol.PreparationSpec{InitialVoters: [3]nodecontrol.PreparationMember{
		{MemberID: 1, Node: intent.Source.Node}, {MemberID: 2, Node: rafttransport.NodeID{2}}, {MemberID: 3, Node: rafttransport.NodeID{3}},
	}, Target: nodecontrol.PreparationMember{MemberID: 4, Node: intent.Target.Node}}
	cut := nodecontrol.BootstrapReadReply{Nonce: [16]byte{1}, Operation: nodecontrol.OpReadOwnEnrollmentRecovery,
		PhysicalNode: intent.Target.Node, Incarnation: intent.Target.NodeIncarnation, IntentID: intent.IntentID, Intent: intent, IntentDigest: intent.Digest(),
		DirectoryCutRevision: 1, DirectoryCutDigest: replication.Digest{1}, CatalogGeneration: 1, CatalogHeadDigest: replication.Digest{2}, EnrollmentDirectoryDigest: replication.Digest{3},
		CurrentRoute: &gateway.ReplicatedMembershipRoute{Serving: gateway.ReplicatedRoute{Distribution: intent.Distribution, Shard: intent.Shard,
			Group: intent.Group, AllocationGeneration: uint64(intent.AllocationGeneration), Command: intent.ExpectedCommand}},
	}
	cut.CurrentRoute.Serving.Command.ReplicaSetVersion = 20
	for _, id := range []uint64{2, 3, 4} {
		member := gateway.ReplicatedEndpoint{Member: id, Node: rafttransport.NodeID{byte(id)}, NodeIncarnation: 1, StoreID: [16]byte{byte(id)},
			Endpoint: fmt.Sprintf("peer-%d", id), NativeEndpoint: fmt.Sprintf("native-%d", id), ControlEndpoint: fmt.Sprintf("control-%d", id)}
		if id == 4 {
			member.Node, member.StoreID = intent.Target.Node, intent.Target.StoreID
			member.Endpoint, member.NativeEndpoint, member.ControlEndpoint = string(intent.Target.Endpoint), string(intent.Target.NativeEndpoint), string(intent.Target.ControlEndpoint)
		}
		member.DataAddress, member.Address, member.ControlAddress = member.Endpoint, member.NativeEndpoint, member.ControlEndpoint
		node := gateway.NodeRecord{NodeID: member.Node, Incarnation: 1, ServiceKeyDigest: replication.Digest{byte(id)},
			DataEndpoint: distribution.EndpointID(member.Endpoint), NativeEndpoint: distribution.EndpointID(member.NativeEndpoint), ControlEndpoint: distribution.EndpointID(member.ControlEndpoint),
			DataAddress: member.Endpoint, NativeAddress: member.NativeEndpoint, ControlAddress: member.ControlEndpoint,
			FailureDomain: "test", Roles: gateway.NodeRoleStorage, Lifecycle: gateway.NodeActive, Revision: 1, CatalogGeneration: 1}
		cut.CurrentRoute.Serving.Replicas = append(cut.CurrentRoute.Serving.Replicas, member)
		cut.CurrentNodes = append(cut.CurrentNodes, node)
		if id == 4 {
			cut.Node = node
		}
	}
	if !cut.TargetServing() {
		t.Fatal("invalid current placement fixture")
	}
	for _, conf := range []*pb.ConfState{{Voters: []uint64{1, 2, 3}, Learners: []uint64{4}}, {Voters: []uint64{1, 2, 3, 4}}, {Voters: []uint64{2, 3, 4}}} {
		publication := raftmodel.Publication{Applied: 25, ReplicaSetVersion: 10, ConfState: conf}
		roster, command, err := rf3RecoveredRoster(spec, descriptor, publication, intent.ExpectedCommand, &cut, membershipgrant.Grant{})
		if err != nil || command != cut.CurrentRoute.Serving.Command {
			t.Fatalf("recover roster=%+v command=%+v err=%v", roster, command, err)
		}
		var restored pb.ConfState
		for _, member := range roster {
			if member.ReplicaSetVersion != publication.ReplicaSetVersion {
				t.Fatal("catalog membership replaced durable local publication")
			}
			switch member.Role {
			case rafttransport.MemberVoter:
				restored.Voters = append(restored.Voters, member.MemberID)
			case rafttransport.MemberLearner:
				restored.Learners = append(restored.Learners, member.MemberID)
			}
		}
		if err := restored.Equivalent(conf); err != nil {
			t.Fatal(err)
		}
	}
}
