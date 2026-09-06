package nodecontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
)

type testDirectory struct {
	intent gateway.GroupEnrollmentIntent
	err    error
}

func (directory *testDirectory) ReadEnrollmentIntent(_ context.Context, id [32]byte) (gateway.GroupEnrollmentIntent, error) {
	if directory.err != nil {
		return gateway.GroupEnrollmentIntent{}, directory.err
	}
	if id != directory.intent.IntentID {
		return gateway.GroupEnrollmentIntent{}, errors.New("wrong intent")
	}
	return directory.intent, nil
}

type testPreparer struct {
	proof       gateway.PreparedReplicaProof
	observed    bool
	prepareCall int
	observeCall int
}

func (preparer *testPreparer) Prepare(context.Context, gateway.GroupEnrollmentIntent, []byte) (gateway.PreparedReplicaProof, error) {
	preparer.prepareCall++
	return preparer.proof, nil
}

func (preparer *testPreparer) ObservePrepared(context.Context, gateway.GroupEnrollmentIntent, []byte) (gateway.PreparedReplicaProof, bool, error) {
	preparer.observeCall++
	return preparer.proof, preparer.observed, nil
}

type testAdopter struct {
	adoptCall   int
	observeCall int
	observed    bool
}

func (adopter *testAdopter) Adopt(context.Context, gateway.GroupEnrollmentIntent, gateway.PreparedReplicaProof) error {
	adopter.adoptCall++
	return nil
}

func (adopter *testAdopter) ObserveAdopted(context.Context, gateway.GroupEnrollmentIntent, gateway.PreparedReplicaProof) (bool, error) {
	adopter.observeCall++
	return adopter.observed, nil
}

func testIntent(payload []byte, state gateway.EnrollmentState) gateway.GroupEnrollmentIntent {
	var group raftmemberGroupForTest
	group.ClusterID[0], group.ClusterIncarnation[0], group.ShardIncarnation[0], group.GroupID[0] = 1, 2, 3, 4
	group.TopologyRecoveryEpoch = 8
	sourceNode, targetNode := rafttransport.NodeID{5}, rafttransport.NodeID{6}
	var sourceStore, targetStore [16]byte
	sourceStore[0], targetStore[0] = 7, 8
	source := gateway.ReplicaIdentity{Member: 1, Node: sourceNode, NodeIncarnation: 1, StoreID: sourceStore, Endpoint: "source-data", NativeEndpoint: "source-native", ControlEndpoint: "source-control"}
	target := gateway.ReplicaIdentity{Member: 4, Node: targetNode, NodeIncarnation: 2, StoreID: targetStore, Endpoint: "target-data", NativeEndpoint: "target-native", ControlEndpoint: "target-control"}
	command := raftservice.CommandFence{ReplicaSetVersion: 3, ActivePolicyGeneration: 4, ProtectionEpoch: 5, OwnershipEpoch: 6, SchemaGeneration: 7, RelationManifestDigest: replication.Digest{9}, RoutingVersion: 8, RouteGeneration: 9}
	intent := gateway.GroupEnrollmentIntent{
		IntentID: [32]byte{10}, Group: group.GroupKey(), Distribution: "orders", Shard: "s1",
		AllocationGeneration: 11, CatalogGeneration: 12, ReplicaOrdinal: 1, Source: source,
		SnapshotSourceMember: 1, Target: target, ExpectedRosterDigest: replication.Digest{13},
		ExpectedDescriptorDigest: replication.Digest{14}, ExpectedManifestDigest: replication.Digest(sha256.Sum256(payload)),
		ExpectedCommand: command, TargetNodeRevision: 15, State: state, Revision: 1,
		ExpectedCatalogHeadDigest: replication.Digest{15},
	}
	if state == gateway.EnrollmentReserved {
		intent.PreparationClaim = gateway.EnrollmentPreparationClaim(intent)
	}
	if state >= gateway.EnrollmentPrepared {
		proof := testProof(intent)
		intent.Proof = &proof
	}
	if state >= gateway.EnrollmentEnrolled {
		intent.Receipt = &gateway.CertifiedEnrollmentReceipt{
			IntentID: intent.IntentID, IntentDigest: intent.Digest(),
			BaseCatalogGeneration:            intent.CatalogGeneration,
			BaseCatalogHeadDigest:            intent.ExpectedCatalogHeadDigest,
			BaseDescriptorDigest:             intent.ExpectedDescriptorDigest,
			PublicationPredecessorGeneration: intent.CatalogGeneration,
			PublicationPredecessorHeadDigest: intent.ExpectedCatalogHeadDigest,
			EnrolledCatalogGeneration:        intent.CatalogGeneration + 1,
			EnrolledCatalogHeadDigest:        replication.Digest{16},
			EnrolledDescriptorDigest:         replication.Digest{17},
			Target:                           intent.Target,
			InitialReplicaSetVersion:         intent.ExpectedCommand.ReplicaSetVersion,
			GrantDigest:                      replication.Digest{18},
			TransitionID:                     gateway.EnrollmentTransitionDigest(intent),
		}
	}
	return intent
}

// Keeping this local alias avoids importing raftmember in the fixture's field
// construction while preserving the production GroupKey shape.
type raftmemberGroupForTest struct {
	ClusterID, ClusterIncarnation [16]byte
	TopologyRecoveryEpoch         uint64
	ShardIncarnation, GroupID     [16]byte
}

func (group raftmemberGroupForTest) GroupKey() (result raftmember.GroupKey) {
	result.ClusterID, result.ClusterIncarnation = group.ClusterID, group.ClusterIncarnation
	result.TopologyRecoveryEpoch = group.TopologyRecoveryEpoch
	result.ShardIncarnation, result.GroupID = group.ShardIncarnation, group.GroupID
	return result
}

func testProof(intent gateway.GroupEnrollmentIntent) gateway.PreparedReplicaProof {
	proof := gateway.PreparedReplicaProof{
		IntentID: intent.IntentID, Group: intent.Group, Distribution: intent.Distribution, Shard: intent.Shard,
		ReplicaOrdinal: intent.ReplicaOrdinal, TargetMember: intent.Target.Member, TargetNode: intent.Target.Node,
		TargetNodeIncarnation: intent.Target.NodeIncarnation, TargetStoreID: intent.Target.StoreID,
		TargetEndpoint: intent.Target.Endpoint, TargetNativeEndpoint: intent.Target.NativeEndpoint,
		TargetControlEndpoint: intent.Target.ControlEndpoint, ExpectedRosterDigest: intent.ExpectedRosterDigest,
		ExpectedDescriptorDigest: intent.ExpectedDescriptorDigest, ExpectedManifestDigest: intent.ExpectedManifestDigest,
		RelationManifestDigest: intent.ExpectedCommand.RelationManifestDigest, DescriptorDigest: intent.ExpectedDescriptorDigest,
		ManifestDigest: intent.ExpectedManifestDigest, Command: intent.ExpectedCommand,
		AllocationGeneration: intent.AllocationGeneration, CatalogGeneration: intent.CatalogGeneration,
		AppliedIndex: 20, ReplicaSetVersion: intent.ExpectedCommand.ReplicaSetVersion,
		CertifiedDirectoryRevision: intent.TargetNodeRevision,
	}
	proof.EnrollmentDigest = proof.ComputedEnrollmentDigest()
	return proof
}

func newTestService(t testing.TB, directory *testDirectory, preparer *testPreparer, adopter *testAdopter, journal Journal) *Service {
	t.Helper()
	service, err := NewService(ServiceOptions{
		Reader: directory, Journal: journal, Preparer: preparer, Adopter: adopter,
		Authorize:       func(rafttransport.PeerIdentity, Request) bool { return true },
		ValidatePayload: func(context.Context, gateway.GroupEnrollmentIntent, []byte) error { return nil },
		LocalNode:       rafttransport.NodeID{6}, LocalIncarnation: 2,
		ReadDeadline:  func() time.Time { return time.Now().Add(time.Minute) },
		WriteDeadline: func() time.Time { return time.Now().Add(time.Minute) }, MaxConcurrent: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestRequestAndResponseRoundTrip(t *testing.T) {
	payload := []byte(`{"root":"/private/node/group-0"}`)
	intent := testIntent(payload, gateway.EnrollmentReserved)
	request, err := NewRequest(PhasePrepare, intent, payload)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendRequest(nil, request)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenRequest(raw)
	if err != nil || opened.Digest() != request.Digest() || !bytes.Equal(opened.Payload, payload) {
		t.Fatalf("request round trip: %#v %v", opened, err)
	}
	proof := testProof(intent)
	record := Record{IntentID: intent.IntentID, PreparationDigest: request.Digest(), Revision: 2, State: StatePrepared, Proof: proof}
	raw, err = AppendResponse(nil, record)
	if err != nil {
		t.Fatal(err)
	}
	openedRecord, err := OpenResponse(raw)
	if err != nil || openedRecord != record {
		t.Fatalf("response round trip: %#v %v", openedRecord, err)
	}
}

func TestServiceRequiresCommittedIntentAndExactPayload(t *testing.T) {
	payload := []byte(`{"root":"/private/node/group-0"}`)
	directory := &testDirectory{intent: testIntent(payload, gateway.EnrollmentReserved)}
	preparer := &testPreparer{proof: testProof(directory.intent)}
	adopter := new(testAdopter)
	service := newTestService(t, directory, preparer, adopter, NewMemoryJournal())
	request, err := NewRequest(PhasePrepare, directory.intent, payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Execute(context.Background(), request); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if preparer.prepareCall != 1 || preparer.observeCall != 1 {
		t.Fatalf("prepare calls=%d observe=%d", preparer.prepareCall, preparer.observeCall)
	}
	request.Payload = []byte(`{"root":"/other"}`)
	if _, err = service.Execute(context.Background(), request); !errors.Is(err, ErrStale) {
		t.Fatalf("tampered payload error=%v", err)
	}
	directory.err = errors.New("directory unavailable")
	if _, err = service.Execute(context.Background(), request); !errors.Is(err, ErrStale) {
		t.Fatalf("directory error=%v", err)
	}
}

func TestServiceAdoptsOnlyAfterMembershipAndResumesExactCrashWindow(t *testing.T) {
	payload := []byte(`{"root":"/private/node/group-0"}`)
	directory := &testDirectory{intent: testIntent(payload, gateway.EnrollmentReserved)}
	proof := testProof(directory.intent)
	preparer := &testPreparer{proof: proof}
	adopter := new(testAdopter)
	journal := NewMemoryJournal()
	service := newTestService(t, directory, preparer, adopter, journal)
	prepare, err := NewRequest(PhasePrepare, directory.intent, payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Execute(context.Background(), prepare); err != nil {
		t.Fatal(err)
	}
	// A prepared artifact alone cannot be adopted before the replicated grant.
	adoptBeforeGrant, err := NewRequest(PhaseAdopt, directory.intent, payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.Execute(context.Background(), adoptBeforeGrant); !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("pre-grant adoption error=%v", err)
	}
	directory.intent = testIntent(payload, gateway.EnrollmentEnrolled)
	if _, err = service.Execute(context.Background(), adoptBeforeGrant); err != nil {
		t.Fatalf("adopt: %v", err)
	}
	if adopter.adoptCall != 1 || adopter.observeCall != 1 {
		t.Fatalf("adopt calls=%d observe=%d", adopter.adoptCall, adopter.observeCall)
	}
	if _, err = service.Execute(context.Background(), adoptBeforeGrant); err != nil {
		t.Fatalf("idempotent adopt: %v", err)
	}
	if adopter.adoptCall != 1 {
		t.Fatalf("idempotent adopt repeated callback: %d", adopter.adoptCall)
	}
	completed := directory.intent
	completed.State = gateway.EnrollmentComplete
	completed.MoveOperationID = [32]byte{99}
	directory.intent = completed
	if _, err = service.Execute(context.Background(), adoptBeforeGrant); err != nil {
		t.Fatalf("completed read-only retry: %v", err)
	}
	if adopter.adoptCall != 1 {
		t.Fatalf("completed intent resurrected runtime: %d", adopter.adoptCall)
	}
}

func TestFileJournalSurvivesReopenAndRejectsInvalidTransition(t *testing.T) {
	payload := []byte(`{"root":"/private/node/group-0"}`)
	intent := testIntent(payload, gateway.EnrollmentReserved)
	proof := testProof(intent)
	request, err := NewRequest(PhasePrepare, intent, payload)
	if err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	journal, err := NewFileJournal(directory)
	if err != nil {
		t.Fatal(err)
	}
	preparing := Record{IntentID: intent.IntentID, PreparationDigest: request.Digest(), Revision: 1, State: StatePreparing}
	if err = journal.Publish(context.Background(), 0, preparing); err != nil {
		t.Fatal(err)
	}
	prepared := preparing
	prepared.Revision, prepared.State, prepared.Proof = 2, StatePrepared, proof
	if err = journal.Publish(context.Background(), 1, prepared); err != nil {
		t.Fatal(err)
	}
	invalid := prepared
	invalid.Revision = 3
	if err = journal.Publish(context.Background(), 2, invalid); !errors.Is(err, ErrConflict) {
		t.Fatalf("invalid transition error=%v", err)
	}
	if err = journal.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewFileJournal(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Read(context.Background(), intent.IntentID)
	if err != nil || got != prepared {
		t.Fatalf("reopened record=%#v err=%v", got, err)
	}
}
