package gateway

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestReplicaHealthKeysMatchSQLDocumentPrimaryKeys(t *testing.T) {
	_, _, catalog := newCatalogAuthorityFixture(t)
	group := catalog.ReplicatedShardDescriptors()[0].Group
	for suspect := uint64(1); suspect <= 256; suspect++ {
		record, page := replicatedFailureKeys(group, suspect)
		id := replicatedFailureDocumentID(group, suspect)
		if !bytes.Equal(record[:], fixedControlPlaneKey(id[:])) {
			t.Fatal("health record key is not its canonical SQL primary key")
		}
		digest := replicatedFailureDigest(group, suspect)
		index := digest[0] & (replicatedFailurePageCount - 1)
		encoded, err := appendReplicatedFailurePage(nil, index, nil)
		if err != nil {
			t.Fatal(err)
		}
		pageID, _, ok := openFixedControlPlaneDocument(encoded, 15)
		if !ok || !bytes.Equal(page[:], fixedControlPlaneKey(pageID)) {
			t.Fatal("health page key is not its canonical SQL primary key")
		}
	}
}

func TestReplicatedFailureAuthorityRequiresQuorumWindowAndSurvivesRestart(t *testing.T) {
	authority, _, catalog := newCatalogAuthorityFixture(t)
	descriptor := catalog.ReplicatedShardDescriptors()[0]
	revision := testReplicaHealthRevision(descriptor, catalog.Generation())

	partitioned := revision
	partitioned.Attestations = partitioned.Attestations[:1]
	if err := authority.PublishReplicaHealthRevision(context.Background(), partitioned); !errors.Is(err, ErrReplicatedFailureAuthority) {
		t.Fatalf("partitioned revision=%v", err)
	}
	for number := uint64(1); number <= FailureConfirmationRevisions; number++ {
		revision.Revision = number
		revision.CommitIndex = 40 + number
		synchronizeReplicaHealthAttestations(&revision)
		if err := authority.PublishReplicaHealthRevision(context.Background(), revision); err != nil {
			t.Fatalf("publish revision %d: %v", number, err)
		}
		// Exact delivery replay is idempotent and does not advance the window.
		if err := authority.PublishReplicaHealthRevision(context.Background(), revision); err != nil {
			t.Fatalf("replay revision %d: %v", number, err)
		}
		count := 0
		err := authority.VisitReplicaFailureCertificates(context.Background(), catalog,
			func(certificate ReplicatedFailureCertificate) error {
				count++
				if certificate.FirstRevision != 1 || certificate.ConfirmedRevision != number ||
					certificate.CommitIndex != revision.CommitIndex || certificate.LeaderTerm != revision.LeaderTerm ||
					certificate.SuspectIncarnation != revision.SuspectIncarnation || len(certificate.Confirmations) != 2 {
					t.Fatalf("wrong certificate: %+v", certificate)
				}
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		if number == FailureConfirmationRevisions {
			want = 1
		}
		if count != want {
			t.Fatalf("revision %d certificates=%d want=%d", number, count, want)
		}
	}

	// A separately constructed authority reopens the same canonical state; no
	// gateway-local timer or in-memory streak is needed after restart.
	restarted := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(catalog), 0x96)
	count := 0
	if err := restarted.VisitReplicaFailureCertificates(context.Background(), catalog,
		func(ReplicatedFailureCertificate) error { count++; return nil }); err != nil || count != 1 {
		t.Fatalf("restart visit count=%d err=%v", count, err)
	}
}

func TestReplicatedFailureAuthorityFencesStaleLeaderTermCatalogAndIncarnation(t *testing.T) {
	authority, _, catalog := newCatalogAuthorityFixture(t)
	descriptor := catalog.ReplicatedShardDescriptors()[0]
	base := testReplicaHealthRevision(descriptor, catalog.Generation())
	tests := []struct {
		name string
		edit func(*ReplicaHealthRevision)
	}{
		{"catalog", func(revision *ReplicaHealthRevision) { revision.CatalogGeneration-- }},
		{"rsv", func(revision *ReplicaHealthRevision) { revision.ReplicaSetVersion++ }},
		{"stale leader", func(revision *ReplicaHealthRevision) { revision.LeaderMember = revision.SuspectMember }},
		{"suspect incarnation", func(revision *ReplicaHealthRevision) { revision.SuspectIncarnation++ }},
		{"reporter incarnation", func(revision *ReplicaHealthRevision) { revision.Attestations[0].NodeIncarnation++ }},
		{"reporter cut", func(revision *ReplicaHealthRevision) { revision.Attestations[0].CommitIndex++ }},
		{"zero term", func(revision *ReplicaHealthRevision) { revision.LeaderTerm = 0 }},
		{"zero commit", func(revision *ReplicaHealthRevision) { revision.CommitIndex = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			revision := base
			revision.Attestations = append([]ReplicaHealthAttestation(nil), base.Attestations...)
			test.edit(&revision)
			if err := authority.PublishReplicaHealthRevision(context.Background(), revision); !errors.Is(err, ErrReplicatedFailureAuthority) {
				t.Fatalf("accepted fenced revision: %v", err)
			}
		})
	}
}

func TestReplicatedFailureAuthorityTermChangeRestartsWindowAndExactGC(t *testing.T) {
	authority, _, catalog := newCatalogAuthorityFixture(t)
	descriptor := catalog.ReplicatedShardDescriptors()[0]
	revision := testReplicaHealthRevision(descriptor, catalog.Generation())
	for number := uint64(1); number <= 2; number++ {
		revision.Revision = number
		revision.CommitIndex += number
		synchronizeReplicaHealthAttestations(&revision)
		if err := authority.PublishReplicaHealthRevision(context.Background(), revision); err != nil {
			t.Fatal(err)
		}
	}
	revision.Revision = 3
	revision.LeaderTerm++
	revision.CommitIndex++
	synchronizeReplicaHealthAttestations(&revision)
	if err := authority.PublishReplicaHealthRevision(context.Background(), revision); err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := authority.VisitReplicaFailureCertificates(context.Background(), catalog,
		func(ReplicatedFailureCertificate) error { count++; return nil }); err != nil || count != 0 {
		t.Fatalf("term change retained stale window count=%d err=%v", count, err)
	}
	if err := authority.DeleteReplicaHealthRecord(context.Background(), revision.Group,
		revision.SuspectMember, revision.Revision-1); !errors.Is(err, ErrReplicatedCatalogConflict) {
		t.Fatalf("stale GC=%v", err)
	}
	if err := authority.DeleteReplicaHealthRecord(context.Background(), revision.Group,
		revision.SuspectMember, revision.Revision); err != nil {
		t.Fatal(err)
	}
	key, pageKey := replicatedFailureKeys(revision.Group, revision.SuspectMember)
	if result, err := authority.readRaw(context.Background(), key[:], maxReplicatedFailureRecordBytes); err != nil || result.Found {
		t.Fatalf("record retained found=%v err=%v", result.Found, err)
	}
	page, err := authority.readRaw(context.Background(), pageKey[:], maxReplicatedFailurePageBytes)
	if err != nil || !page.Found {
		t.Fatalf("page absent err=%v", err)
	}
	identities, err := openReplicatedFailurePage(pageKey.bucket(), page.Value)
	if err != nil || len(identities) != 0 {
		t.Fatalf("page identities=%d err=%v", len(identities), err)
	}
}

func testReplicaHealthRevision(descriptor ReplicatedShardDescriptor, generation uint64) ReplicaHealthRevision {
	suspect := descriptor.Replicas[2]
	attestations := make([]ReplicaHealthAttestation, 0, 2)
	for _, replica := range descriptor.Replicas[:2] {
		attestations = append(attestations, ReplicaHealthAttestation{Member: replica.Member,
			Node: replica.Node, NodeIncarnation: replica.NodeIncarnation, Failed: true})
	}
	revision := ReplicaHealthRevision{Distribution: descriptor.Distribution, Shard: descriptor.Shard,
		Group: descriptor.Group, CatalogGeneration: generation,
		ReplicaSetVersion: descriptor.Command.ReplicaSetVersion, Revision: 1,
		LeaderMember: descriptor.Replicas[0].Member, LeaderTerm: 7, CommitIndex: 40,
		SuspectMember: suspect.Member, SuspectNode: suspect.Node,
		SuspectIncarnation: suspect.NodeIncarnation, Attestations: attestations}
	synchronizeReplicaHealthAttestations(&revision)
	return revision
}

func synchronizeReplicaHealthAttestations(revision *ReplicaHealthRevision) {
	for index := range revision.Attestations {
		revision.Attestations[index].CatalogGeneration = revision.CatalogGeneration
		revision.Attestations[index].ReplicaSetVersion = revision.ReplicaSetVersion
		revision.Attestations[index].LeaderMember = revision.LeaderMember
		revision.Attestations[index].LeaderTerm = revision.LeaderTerm
		revision.Attestations[index].CommitIndex = revision.CommitIndex
	}
}

func TestReplicatedFailureAuthorityReopensMonotonicRevision(t *testing.T) {
	authority, _, catalog := newCatalogAuthorityFixture(t)
	descriptor := catalog.ReplicatedShardDescriptors()[0]
	if revision, err := authority.ReadReplicaHealthRevision(
		context.Background(), descriptor.Group, descriptor.Replicas[0].Member,
	); err != nil || revision != 0 {
		t.Fatalf("initial revision=%d err=%v", revision, err)
	}
	health := testReplicaHealthRevision(descriptor, catalog.Generation())
	health.Revision = 1
	if err := authority.PublishReplicaHealthRevision(context.Background(), health); err != nil {
		t.Fatal(err)
	}
	restarted := newCatalogAuthorityPeer(t, authority, NewCatalogHolder(catalog), 0x97)
	if revision, err := restarted.ReadReplicaHealthRevision(
		context.Background(), health.Group, health.SuspectMember,
	); err != nil || revision != 1 {
		t.Fatalf("reopened revision=%d err=%v", revision, err)
	}
}

func TestReplicatedFailureAuthorityAcceptsFullAgreementLeaderClear(t *testing.T) {
	authority, _, catalog := newCatalogAuthorityFixture(t)
	descriptor := catalog.ReplicatedShardDescriptors()[0]
	leader := descriptor.Replicas[0]
	revision := ReplicaHealthRevision{
		Distribution: descriptor.Distribution, Shard: descriptor.Shard, Group: descriptor.Group,
		CatalogGeneration: catalog.Generation(), ReplicaSetVersion: descriptor.Command.ReplicaSetVersion,
		Revision: 1, LeaderMember: leader.Member, LeaderTerm: 7, CommitIndex: 40,
		SuspectMember: leader.Member, SuspectNode: leader.Node,
		SuspectIncarnation: leader.NodeIncarnation,
	}
	for _, replica := range descriptor.Replicas {
		revision.Attestations = append(revision.Attestations, ReplicaHealthAttestation{
			Member: replica.Member, Node: replica.Node, NodeIncarnation: replica.NodeIncarnation,
		})
	}
	synchronizeReplicaHealthAttestations(&revision)
	if err := authority.PublishReplicaHealthRevision(context.Background(), revision); err != nil {
		t.Fatal(err)
	}
	status, err := authority.ReadReplicaHealthRevisionStatus(t.Context(), revision.Group, revision.SuspectMember)
	if err != nil || status.Revision != 1 || !status.AlreadyHealthy(revision) {
		t.Fatalf("retained healthy state=%+v err=%v", status, err)
	}
	revision.CatalogGeneration++
	if status.AlreadyHealthy(revision) {
		t.Fatal("suppressed new catalog generation")
	}
	revision.CatalogGeneration--
	revision.SuspectIncarnation++
	if status.AlreadyHealthy(revision) {
		t.Fatal("suppressed new incarnation")
	}
	revision.SuspectIncarnation--
	revision.Attestations[0].Failed = true
	if status.AlreadyHealthy(revision) {
		t.Fatal("suppressed failed observation")
	}
}
