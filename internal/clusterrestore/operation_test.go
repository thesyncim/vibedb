package clusterrestore

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func filled16(value byte) (result [16]byte) {
	for index := range result {
		result[index] = value
	}
	return
}
func filled32(value byte) (result [sha256.Size]byte) {
	for index := range result {
		result[index] = value
	}
	return
}

func restoreOperationFixture(t *testing.T, groups int) Operation {
	t.Helper()
	cuts := make([]clusterbackup.GroupCut, groups)
	inventory := make([]raftmember.GroupKey, groups)
	evidence := make([]clusterbackup.ArtifactEvidence, groups)
	for index := range cuts {
		value := byte(index + 1)
		group := raftmember.GroupKey{ClusterID: filled16(1), ClusterIncarnation: filled16(2),
			TopologyRecoveryEpoch: 1, ShardIncarnation: filled16(10 + value), GroupID: filled16(20 + value)}
		cut := clusterbackup.GroupCut{Group: group, SourceMember: 1, SchemaGeneration: 3,
			ReplicaSetVersion: 4, SnapshotIndex: uint64(100 + index), SnapshotTerm: 7,
			Lineage: filled32(30 + value), RelationManifestDigest: filled32(40 + value),
			ArtifactHash: filled32(50 + value), ArtifactBytes: uint64(4096 + index),
			ArtifactManifestDigest: filled32(60 + value)}
		cuts[index], inventory[index] = cut, group
		evidence[index] = clusterbackup.ArtifactEvidence{Group: group,
			SnapshotIndex: cut.SnapshotIndex, SnapshotTerm: cut.SnapshotTerm, Lineage: cut.Lineage,
			RelationManifestDigest: cut.RelationManifestDigest, ArtifactHash: cut.ArtifactHash,
			ArtifactBytes: cut.ArtifactBytes, ArtifactManifestDigest: cut.ArtifactManifestDigest}
	}
	certificate, err := clusterbackup.Certify(filled32(70), clusterbackup.CatalogCut{
		Generation: 9, Digest: filled32(71), PolicyGeneration: 5, Groups: inventory}, cuts)
	if err != nil {
		t.Fatal(err)
	}
	permit, err := clusterbackup.AdmitRestore(certificate, evidence, filled32(72), filled16(73), filled16(74))
	if err != nil {
		t.Fatal(err)
	}
	targets := make([]TargetGroup, groups)
	for index := range targets {
		targets[index].Group = raftmember.GroupKey{ClusterID: permit.TargetClusterID,
			ClusterIncarnation: permit.TargetClusterIncarnation, TopologyRecoveryEpoch: 1,
			ShardIncarnation: filled16(byte(80 + index)), GroupID: filled16(byte(90 + index))}
		for replica := range targets[index].Replicas {
			node := rafttransport.NodeID(filled16(byte(100 + index*3 + replica)))
			targets[index].Replicas[replica] = ReplicaIdentity{Member: uint64(replica + 1),
				Node: node, Store: filled16(byte(120 + index*3 + replica)), NodeIncarnation: 1}
		}
	}
	operation, err := NewOperation(permit, certificate, 0, 1, filled32(75), filled32(76), filled32(77), targets)
	if err != nil {
		t.Fatal(err)
	}
	return operation
}

func TestOperationCanonicalRoundTripBindsFreshCompleteInventory(t *testing.T) {
	operation := restoreOperationFixture(t, 3)
	raw, err := AppendOperation(nil, operation)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenOperation(raw)
	if err != nil || opened.Digest != operation.Digest || len(opened.Targets) != 3 {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	reencoded, err := AppendOperation(nil, opened)
	if err != nil || !bytes.Equal(raw, reencoded) {
		t.Fatalf("canonical=%t err=%v", bytes.Equal(raw, reencoded), err)
	}
}

func TestOperationRejectsCorruptionPartialAndReusedIdentity(t *testing.T) {
	operation := restoreOperationFixture(t, 2)
	raw, _ := AppendOperation(nil, operation)
	for _, invalid := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0)} {
		if _, err := OpenOperation(invalid); err == nil {
			t.Fatal("accepted malformed operation")
		}
	}
	corrupt := bytes.Clone(raw)
	corrupt[len(corrupt)/2] ^= 0x80
	if _, err := OpenOperation(corrupt); err == nil {
		t.Fatal("accepted corrupted operation")
	}
	targets := cloneTargets(operation.Targets)
	targets[1].Replicas[0].Node = targets[0].Replicas[0].Node
	if _, err := NewOperation(operation.Permit, operation.Certificate, 0, 1,
		operation.BuildGrammarDigest, operation.TargetPolicyDigest, operation.TargetCatalogDigest, targets); err == nil {
		t.Fatal("accepted duplicate node identity")
	}
	targets = cloneTargets(operation.Targets)
	targets[0].Group.ClusterID = operation.Certificate.Groups[0].Group.ClusterID
	targets[0].Group.ClusterIncarnation = operation.Certificate.Groups[0].Group.ClusterIncarnation
	if _, err := NewOperation(operation.Permit, operation.Certificate, 0, 1,
		operation.BuildGrammarDigest, operation.TargetPolicyDigest, operation.TargetCatalogDigest, targets); err == nil {
		t.Fatal("accepted source trust domain")
	}
	targets = cloneTargets(operation.Targets)
	targets[0].Group.GroupID = operation.Certificate.Groups[1].Group.GroupID
	if _, err := NewOperation(operation.Permit, operation.Certificate, 0, 1,
		operation.BuildGrammarDigest, operation.TargetPolicyDigest, operation.TargetCatalogDigest, targets); err == nil {
		t.Fatal("accepted reused source group identity")
	}
}
