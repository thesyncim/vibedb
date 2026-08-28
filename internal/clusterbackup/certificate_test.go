package clusterbackup

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

func filled32(value byte) (out [32]byte) {
	for index := range out {
		out[index] = value
	}
	return out
}
func filled16(value byte) (out [16]byte) {
	for index := range out {
		out[index] = value
	}
	return out
}

func backupGroup(value byte) raftmember.GroupKey {
	return raftmember.GroupKey{ClusterID: filled16(1), ClusterIncarnation: filled16(2),
		TopologyRecoveryEpoch: 3, ShardIncarnation: filled16(value), GroupID: filled16(value)}
}
func backupCut(value byte) GroupCut {
	return GroupCut{Group: backupGroup(value), SourceMember: 1, SchemaGeneration: 2,
		ReplicaSetVersion: 3, SnapshotIndex: uint64(10 + value), SnapshotTerm: 4,
		Lineage: filled32(value), RelationManifestDigest: filled32(value + 10),
		ArtifactHash: filled32(value + 20), ArtifactBytes: uint64(100 + value),
		ArtifactManifestDigest: filled32(value + 30)}
}

func TestCertificateCanonicalRoundTripAndCompleteCatalogInventory(t *testing.T) {
	cuts := []GroupCut{backupCut(1), backupCut(2)}
	authority := CatalogCut{Generation: 7, Digest: filled32(9), PolicyGeneration: 8,
		Groups: []raftmember.GroupKey{cuts[0].Group, cuts[1].Group}}
	certificate, err := Certify(filled32(8), authority, cuts)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendCertificate(nil, certificate)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenCertificate(raw)
	if err != nil {
		t.Fatal(err)
	}
	reencoded, err := AppendCertificate(nil, opened)
	if err != nil || !bytes.Equal(raw, reencoded) || opened.Digest != certificate.Digest {
		t.Fatalf("noncanonical replay err=%v", err)
	}

	if _, err = Certify(filled32(8), authority, cuts[:1]); !errors.Is(err, ErrCatalogCut) {
		t.Fatalf("partial inventory err=%v", err)
	}
	reordered := []GroupCut{cuts[1], cuts[0]}
	if _, err = Certify(filled32(8), authority, reordered); !errors.Is(err, ErrCatalogCut) {
		t.Fatalf("reordered inventory err=%v", err)
	}
}

func TestCertificateRejectsEveryCorruptionTruncationAndTrailingByte(t *testing.T) {
	cut := backupCut(1)
	certificate, err := Certify(filled32(8), CatalogCut{Generation: 7, Digest: filled32(9),
		PolicyGeneration: 8, Groups: []raftmember.GroupKey{cut.Group}}, []GroupCut{cut})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := AppendCertificate(nil, certificate)
	for _, offset := range []int{0, 20, HeaderBytes, len(raw) - 1} {
		corrupt := bytes.Clone(raw)
		corrupt[offset] ^= 1
		if _, err = OpenCertificate(corrupt); !errors.Is(err, ErrCertificate) {
			t.Fatalf("corruption offset %d err=%v", offset, err)
		}
	}
	if _, err = OpenCertificate(raw[:len(raw)-1]); !errors.Is(err, ErrCertificate) {
		t.Fatal("truncation accepted")
	}
	if _, err = OpenCertificate(append(bytes.Clone(raw), 0)); !errors.Is(err, ErrCertificate) {
		t.Fatal("trailing byte accepted")
	}
}

func TestRestoreAdmissionRequiresExactCompleteArtifactEvidenceAndMintsNoServingIdentity(t *testing.T) {
	cuts := []GroupCut{backupCut(1), backupCut(2)}
	certificate, err := Certify(filled32(8), CatalogCut{Generation: 7, Digest: filled32(9),
		PolicyGeneration: 8, Groups: []raftmember.GroupKey{cuts[0].Group, cuts[1].Group}}, cuts)
	if err != nil {
		t.Fatal(err)
	}
	evidence := make([]ArtifactEvidence, len(cuts))
	for index, cut := range cuts {
		evidence[index] = ArtifactEvidence{Group: cut.Group,
			SnapshotIndex: cut.SnapshotIndex, SnapshotTerm: cut.SnapshotTerm, Lineage: cut.Lineage,
			RelationManifestDigest: cut.RelationManifestDigest, ArtifactHash: cut.ArtifactHash,
			ArtifactBytes: cut.ArtifactBytes, ArtifactManifestDigest: cut.ArtifactManifestDigest}
	}
	permit, err := AdmitRestore(certificate, evidence, filled32(40), filled16(41), filled16(42))
	if err != nil || permit.Groups != 2 || permit.CertificateDigest != certificate.Digest {
		t.Fatalf("permit=%+v err=%v", permit, err)
	}
	bad := append([]ArtifactEvidence(nil), evidence...)
	bad[1].ArtifactBytes++
	if _, err = AdmitRestore(certificate, bad, filled32(40), filled16(41), filled16(42)); !errors.Is(err, ErrArtifactEvidence) {
		t.Fatalf("corrupt evidence err=%v", err)
	}
	if _, err = AdmitRestore(certificate, evidence[:1], filled32(40), filled16(41), filled16(42)); !errors.Is(err, ErrArtifactEvidence) {
		t.Fatalf("partial evidence err=%v", err)
	}
	mutated := certificate
	mutated.Groups = append([]GroupCut(nil), certificate.Groups...)
	mutated.Groups[0].ArtifactBytes++
	if _, err = AdmitRestore(mutated, evidence, filled32(40), filled16(41), filled16(42)); !errors.Is(err, ErrArtifactEvidence) {
		t.Fatalf("mutated certificate err=%v", err)
	}
	if _, err = AdmitRestore(certificate, evidence, filled32(40), certificate.Groups[0].Group.ClusterID,
		certificate.Groups[0].Group.ClusterIncarnation); !errors.Is(err, ErrArtifactEvidence) {
		t.Fatalf("copied source identity err=%v", err)
	}
}

func TestGroupCutComesFromCompleteVerifierManifestNotLearnerDescriptor(t *testing.T) {
	manifest := replicatedstate.SnapshotArtifactManifest{EncodedBytes: 4096,
		RelationManifestDigest: filled32(4), Digest: filled32(5), State: replicatedstate.State{
			Binding: replicatedstate.Binding{ClusterID: replication.ID128(filled16(1)),
				ClusterIncarnation: replication.ID128(filled16(2)), TopologyRecoveryEpoch: 3,
				ShardIncarnation: replication.ID128(filled16(4)), GroupID: replication.ID128(filled16(5)),
				SchemaGeneration: 6}, Applied: 7, LastTerm: 8, LastEntryDigest: filled32(9),
			ReplicaSetVersion: 10}}
	cut, err := GroupCutFromVerifiedArtifact(11, manifest, filled32(12), 4096)
	if err != nil || cut.SnapshotIndex != 7 || cut.SchemaGeneration != 6 || cut.ArtifactBytes != 4096 {
		t.Fatalf("cut=%+v err=%v", cut, err)
	}
	manifest.Seeded = true
	if _, err = GroupCutFromVerifiedArtifact(11, manifest, filled32(12), 4096); !errors.Is(err, ErrArtifactEvidence) {
		t.Fatalf("seeded manifest err=%v", err)
	}
	manifest.Seeded = false
	if _, err = GroupCutFromVerifiedArtifact(11, manifest, filled32(12), 4095); !errors.Is(err, ErrArtifactEvidence) {
		t.Fatalf("wrong artifact bytes err=%v", err)
	}
}

func TestGroupCutAcceptsAuthenticatedSingletonWithoutBundleManifestDigest(t *testing.T) {
	manifest := replicatedstate.SnapshotArtifactManifest{EncodedBytes: 4096,
		Digest: filled32(5), State: replicatedstate.State{
			Binding: replicatedstate.Binding{ClusterID: replication.ID128(filled16(1)),
				ClusterIncarnation: replication.ID128(filled16(2)), TopologyRecoveryEpoch: 3,
				ShardIncarnation: replication.ID128(filled16(4)), GroupID: replication.ID128(filled16(5)),
				SchemaGeneration: 6}, Applied: 7, LastTerm: 8, LastEntryDigest: filled32(9),
			ReplicaSetVersion: 10}}
	cut, err := GroupCutFromVerifiedArtifact(11, manifest, filled32(12), 4096)
	if err != nil || !cut.Valid() || cut.RelationManifestDigest != ([32]byte{}) {
		t.Fatalf("cut=%+v err=%v", cut, err)
	}
}

func BenchmarkCertificateOpen64Groups(b *testing.B) {
	cuts := make([]GroupCut, 64)
	groups := make([]raftmember.GroupKey, 64)
	for index := range cuts {
		cuts[index] = backupCut(byte(index + 1))
		groups[index] = cuts[index].Group
	}
	certificate, err := Certify(filled32(8), CatalogCut{Generation: 7, Digest: filled32(9), PolicyGeneration: 8, Groups: groups}, cuts)
	if err != nil {
		b.Fatal(err)
	}
	raw, _ := AppendCertificate(nil, certificate)
	workspace := make([]GroupCut, len(cuts))
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for b.Loop() {
		if _, err := OpenCertificateInto(raw, workspace); err != nil {
			b.Fatal(err)
		}
	}
}

func FuzzOpenCertificate(f *testing.F) {
	cut := backupCut(1)
	certificate, _ := Certify(filled32(8), CatalogCut{Generation: 7, Digest: filled32(9),
		PolicyGeneration: 8, Groups: []raftmember.GroupKey{cut.Group}}, []GroupCut{cut})
	raw, _ := AppendCertificate(nil, certificate)
	f.Add(raw)
	f.Fuzz(func(t *testing.T, candidate []byte) {
		opened, err := OpenCertificate(candidate)
		if err != nil {
			return
		}
		reencoded, err := AppendCertificate(nil, opened)
		if err != nil || !bytes.Equal(candidate, reencoded) {
			t.Fatalf("accepted noncanonical certificate")
		}
	})
}
