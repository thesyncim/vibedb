package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson"
)

func TestRF3AdoptionRecoveryBeforeFirstSnapshotDescriptor(t *testing.T) {
	intent := rf3RecoveryEnrollmentIntent()
	intent.State = gateway.EnrollmentEnrolled
	intent.Receipt = &gateway.CertifiedEnrollmentReceipt{IntentID: intent.IntentID, IntentDigest: intent.Digest(),
		BaseCatalogGeneration: intent.CatalogGeneration, BaseCatalogHeadDigest: intent.ExpectedCatalogHeadDigest,
		BaseDescriptorDigest: intent.ExpectedDescriptorDigest, PublicationPredecessorGeneration: intent.CatalogGeneration,
		PublicationPredecessorHeadDigest: intent.ExpectedCatalogHeadDigest, EnrolledCatalogGeneration: intent.CatalogGeneration + 1,
		EnrolledCatalogHeadDigest: replication.Digest{21}, EnrolledDescriptorDigest: replication.Digest{22}, Target: intent.Target,
		InitialReplicaSetVersion: intent.ExpectedCommand.ReplicaSetVersion, GrantDigest: replication.Digest{23}, TransitionID: gateway.EnrollmentTransitionDigest(intent)}
	if !intent.Valid() {
		t.Fatal("invalid intent")
	}
	root := t.TempDir()
	reservation := rf3EnrollmentReservationPath(root, intent.IntentID)
	if err := os.MkdirAll(reservation, 0700); err != nil {
		t.Fatal(err)
	}
	receipt := rf3EnrollmentReceiverReceipt{Kind: rf3EnrollmentPayloadKind, IntentID: intent.IntentID, IntentDigest: intent.Digest(),
		Group: intent.Group, TargetMember: intent.Target.Member, TargetNode: intent.Target.Node, TargetNodeIncarnation: intent.Target.NodeIncarnation,
		TargetStoreID: intent.Target.StoreID, ProofDigest: intent.Proof.EnrollmentDigest}
	raw, _ := vibejson.Marshal(&receipt)
	if err := writeRF3DurableMarker(filepath.Join(reservation, rf3EnrollmentReceiverFile), raw); err != nil {
		t.Fatal(err)
	}
	journal, err := replicaaction.OpenFileJournal(t.TempDir(), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	for reopen := 0; reopen < 2; reopen++ {
		reader := new(nodecontrol.IntentReaderSlot)
		if err := reader.Set(rf3EnrollmentRecoveryReadFunc(func(_ context.Context, id [32]byte) (nodecontrol.BootstrapReadReply, error) {
			if id != intent.IntentID {
				t.Fatal("wrong recovery identity")
			}
			return nodecontrol.BootstrapReadReply{Intent: intent}, nil
		})); err != nil {
			t.Fatal(err)
		}
		receivers, err := newRF3DynamicBootstrapRegistry(rafttransport.TrustDomain{ClusterID: intent.Group.ClusterID, ClusterIncarnation: intent.Group.ClusterIncarnation}, func() time.Time { return time.Now().Add(time.Second) }, 1)
		if err != nil {
			t.Fatal(err)
		}
		factory := &rf3DynamicLearnerFactory{root: root, runtime: &rf3NodeRuntime{reader: reader, receivers: receivers, actionJournal: journal}}
		if err := factory.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
		got, found := receivers.reservations[intent.Group]
		if !found || got.intent.Digest() != intent.Digest() || got.proof != *intent.Proof || len(receivers.services) != 0 {
			t.Fatal("did not restore exact pre-Raft reservation")
		}
	}
}
