package main

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
)

func rf3RetirementRecoveryRecord(intent gateway.GroupEnrollmentIntent) replicaaction.Record {
	return replicaaction.Record{
		Request: replicaaction.Request{
			Operation: [32]byte{31}, Step: [32]byte{32}, Kind: replicaaction.SourceRetirement,
			Fence: raftservice.ServingFence{Group: intent.Group,
				AllocationGeneration: uint64(intent.AllocationGeneration), Command: intent.ExpectedCommand,
				MemberID: intent.Target.Member, StoreID: intent.Target.StoreID, NodeIncarnation: 2, Term: 3},
			SourceMember: intent.Target.Member, TargetMember: 5,
		},
		Revision: 1, State: replicaaction.Running,
	}
}

func TestRF3RetiredLearnerRecoveryDoesNotReopenPriorMembership(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "crash before close", true: "completed close"}[terminal], func(t *testing.T) {
			intent := rf3RecoveryEnrollmentIntent()
			intent.State = gateway.EnrollmentEnrolled
			intent.Receipt = &gateway.CertifiedEnrollmentReceipt{
				IntentID: intent.IntentID, IntentDigest: intent.Digest(), BaseCatalogGeneration: intent.CatalogGeneration,
				BaseCatalogHeadDigest: intent.ExpectedCatalogHeadDigest, BaseDescriptorDigest: intent.ExpectedDescriptorDigest,
				PublicationPredecessorGeneration: intent.CatalogGeneration,
				PublicationPredecessorHeadDigest: intent.ExpectedCatalogHeadDigest,
				EnrolledCatalogGeneration:        intent.CatalogGeneration + 1,
				EnrolledCatalogHeadDigest:        replication.Digest{21}, EnrolledDescriptorDigest: replication.Digest{22},
				Target: intent.Target, InitialReplicaSetVersion: intent.ExpectedCommand.ReplicaSetVersion,
				GrantDigest: replication.Digest{23}, TransitionID: gateway.EnrollmentTransitionDigest(intent),
			}
			if !intent.Valid() {
				t.Fatal("invalid enrollment fixture")
			}
			root := t.TempDir()
			reservation := rf3EnrollmentReservationPath(root, intent.IntentID)
			if err := os.MkdirAll(reservation, 0700); err != nil {
				t.Fatal(err)
			}
			descriptor := snapshottransfer.Descriptor{Group: intent.Group, SourceMember: 1, TargetMember: 4,
				TargetStore: intent.Target.StoreID, TargetIncarnation: 1, SchemaGeneration: 1, ReplicaSetVersion: 1,
				SnapshotIndex: 1, SnapshotTerm: 1, Lineage: [32]byte{1}, ArtifactHash: [32]byte{2},
				ArtifactBytes: 4096, ChunkBytes: 4096}
			if err := persistRF3EnrollmentDescriptor(reservation, intent, descriptor); err != nil {
				t.Fatal(err)
			}
			journalPath := t.TempDir()
			journal, err := replicaaction.OpenFileJournal(journalPath, 8)
			if err != nil {
				t.Fatal(err)
			}
			record := rf3RetirementRecoveryRecord(intent)
			if err = journal.PublishReplicaAction(t.Context(), 0, record); err != nil {
				t.Fatal(err)
			}
			record.Revision, record.State = 2, replicaaction.RetirementAuthorized
			if err = journal.PublishReplicaAction(t.Context(), 1, record); err != nil {
				t.Fatal(err)
			}
			if terminal {
				record.Revision, record.State = 3, replicaaction.Complete
				if err = journal.PublishReplicaAction(t.Context(), 2, record); err != nil {
					t.Fatal(err)
				}
			}
			for restart := 0; restart < 2; restart++ {
				if err = journal.Close(); err != nil {
					t.Fatal(err)
				}
				journal, err = replicaaction.OpenFileJournal(journalPath, 8)
				if err != nil {
					t.Fatal(err)
				}
				slot := new(nodecontrol.IntentReaderSlot)
				if err = slot.Set(rf3EnrollmentRecoveryReadFunc(func(context.Context, [32]byte) (nodecontrol.BootstrapReadReply, error) {
					// Catalog placement may still contain the source until retirement
					// returns. That older placement cannot undo the local tombstone.
					return nodecontrol.BootstrapReadReply{Intent: intent}, nil
				})); err != nil {
					t.Fatal(err)
				}
				factory := &rf3DynamicLearnerFactory{root: root,
					runtime: &rf3EmptyNodeRuntime{reader: slot, actionJournal: journal}}
				// No storage or receiver is configured: crossing into registration
				// would fail. Recovery must skip before either resource is opened.
				if err = factory.Recover(t.Context()); err != nil {
					t.Fatalf("restart %d restored retired source: %v", restart, err)
				}
				if err = factory.Register(t.Context(), intent, *intent.Proof, descriptor); !errors.Is(err, nodecontrol.ErrConflict) {
					t.Fatalf("restart %d admitted old bootstrap: %v", restart, err)
				}
				installer := &rf3DynamicLearnerInstaller{factory: factory, intent: intent,
					installed: &raftmember.RuntimeIdentity{Group: intent.Group, MemberID: intent.Target.Member,
						StoreID: intent.Target.StoreID, NodeIncarnation: uint64(3 + restart)}}
				if err = installer.RecoverInstalled(t.Context(), descriptor); !errors.Is(err, nodecontrol.ErrConflict) {
					t.Fatalf("restart %d resumed retired installer: %v", restart, err)
				}
			}
			if err = journal.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRF3SourceRetirementOnlyExcludesExactStorage(t *testing.T) {
	intent := rf3RecoveryEnrollmentIntent()
	record := rf3RetirementRecoveryRecord(intent)
	record.Revision, record.State = 2, replicaaction.RetirementAuthorized
	records := []replicaaction.Record{record}
	if !rf3ReplicaSourceRetired(records, intent.Group, intent.Target.Member, intent.Target.StoreID, uint64(intent.AllocationGeneration)) {
		t.Fatal("exact retired source not excluded")
	}
	otherGroup := intent.Group
	otherGroup.ShardIncarnation[0]++
	otherStore := intent.Target.StoreID
	otherStore[0]++
	for _, changed := range []struct {
		group      raftmember.GroupKey
		member     uint64
		store      [16]byte
		allocation uint64
	}{
		{otherGroup, intent.Target.Member, intent.Target.StoreID, uint64(intent.AllocationGeneration)},
		{intent.Group, intent.Target.Member + 1, intent.Target.StoreID, uint64(intent.AllocationGeneration)},
		{intent.Group, intent.Target.Member, otherStore, uint64(intent.AllocationGeneration)},
		{intent.Group, intent.Target.Member, intent.Target.StoreID, uint64(intent.AllocationGeneration) + 1},
	} {
		if rf3ReplicaSourceRetired(records, changed.group, changed.member, changed.store, changed.allocation) {
			t.Fatalf("retirement excluded another storage identity: %+v", changed)
		}
	}
}
