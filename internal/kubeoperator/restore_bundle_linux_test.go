//go:build linux

package kubeoperator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/clusterbackup"
	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
	pb "go.etcd.io/raft/v3/raftpb"
)

func TestRestoreReplicaFactoryBindsExactBundle(t *testing.T) {
	template := validRestoreTestTemplate()
	template.Apply.MaxSessions, template.Apply.RetryWindow = 128, 8
	template.Apply.MaxCollections, template.Apply.MaxDocuments, template.Apply.MaxBytes = 16, 256, 384<<20
	template.DDL = append(template.DDL,
		"CREATE INDEX by_email ON items (email)",
		"CREATE TABLE email_claims (PRIMARY KEY (key))",
	)
	template.GlobalIndexes = []restoreGlobalIndexTemplate{{
		Relation: 2, Table: "email_claims", IndexID: 41, Incarnation: 7,
		LocatorCount: 1, Unique: true, KeyEncoding: uint8(sqldriver.ReplicatedRelationKeyCanonicalTuple),
		KeyArity: 1, TupleVersion: uint32(distribution.CurrentTupleVersion),
		MapperVersion: uint32(distribution.NativeMapperVersion), BucketBits: distribution.DefaultVirtualBucketBits,
	}}
	root := t.TempDir()
	factory := restoreReplicaFactory{root: root, template: template}
	operation := clusterrestore.Operation{Digest: sha256.Sum256([]byte("bundle-operation"))}
	binding := sqldriver.ReplicatedShardStoreBinding{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, TopologyRecoveryEpoch: 1,
		Distribution: template.Distribution, Shard: template.Shard,
		AllocationGeneration: template.AllocationGeneration,
		ShardIncarnation:     [16]byte{3}, GroupID: [16]byte{4}, MemberID: 1, StoreID: [16]byte{5},
		Authority: sqldriver.ReplicatedAuthorityProfile{
			ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1,
			SchemaGeneration: 1, RoutingVersion: 1, RouteGeneration: 1,
		},
	}
	directory := filepath.Join(root, "replica")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	_, database, identity, err := factory.openOrCreateRoot(
		filepath.Join(directory, "member.vdb"), filepath.Join(directory, "allocation.vibejson"),
		operation, 0, 0, binding,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if identity.RelationCount != 2 || identity.RelationManifestDigest == ([32]byte{}) ||
		identity.Relations[1].Kind != sqldriver.ReplicatedShardRelationGlobalIndex ||
		identity.Relations[1].IndexID != 41 || identity.Relations[1].Table != "email_claims" {
		t.Fatalf("identity=%+v", identity)
	}
	options, err := restoreApplyOptions(template.Apply)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap := restoreRF3Bootstrap(operation, 0, [32]byte{1})
	apply, _, err := database.OpenReplicatedApply(identity, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apply.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	if _, err = apply.ApplyConfiguration(raftmodel.ApplyMeta{Index: 2, Term: 1, Type: pb.EntryConfChange}, bootstrap.Metadata.ConfState); err != nil {
		t.Fatal(err)
	}
	cut, err := apply.SnapshotArtifactCut()
	if err != nil {
		t.Fatal(err)
	}
	var artifact bytes.Buffer
	manifest, err := replicatedstate.WriteSnapshotArtifact(&artifact, cut, replicatedstate.SnapshotArtifactOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err = cut.Close(); err != nil {
		t.Fatal(err)
	}
	if err = apply.Close(); err != nil {
		t.Fatal(err)
	}
	if err = database.Close(); err != nil {
		t.Fatal(err)
	}
	groupCut, err := clusterbackup.GroupCutFromVerifiedArtifact(1, manifest, sha256.Sum256(artifact.Bytes()), uint64(artifact.Len()))
	if err != nil {
		t.Fatal(err)
	}
	sourceGroup := raftmember.GroupKey{ClusterID: binding.ClusterID, ClusterIncarnation: binding.ClusterIncarnation,
		TopologyRecoveryEpoch: binding.TopologyRecoveryEpoch, ShardIncarnation: binding.ShardIncarnation, GroupID: binding.GroupID}
	certificate, err := clusterbackup.Certify([32]byte{9}, clusterbackup.CatalogCut{Generation: 1, Digest: [32]byte{10}, PolicyGeneration: 1, Groups: []raftmember.GroupKey{sourceGroup}}, []clusterbackup.GroupCut{groupCut})
	if err != nil {
		t.Fatal(err)
	}
	target := clusterrestore.TargetGroup{Group: raftmember.GroupKey{ClusterID: [16]byte{11}, ClusterIncarnation: [16]byte{12}, TopologyRecoveryEpoch: 2, ShardIncarnation: [16]byte{13}, GroupID: [16]byte{14}}}
	for i := range target.Replicas {
		target.Replicas[i] = clusterrestore.ReplicaIdentity{Member: uint64(i + 1), Node: rafttransport.NodeID{byte(20 + i)}, Store: [16]byte{byte(30 + i)}, NodeIncarnation: 1}
	}
	evidence := []clusterbackup.ArtifactEvidence{{Group: groupCut.Group, SnapshotIndex: groupCut.SnapshotIndex, SnapshotTerm: groupCut.SnapshotTerm,
		Lineage: groupCut.Lineage, RelationManifestDigest: groupCut.RelationManifestDigest, ArtifactHash: groupCut.ArtifactHash,
		ArtifactBytes: groupCut.ArtifactBytes, ArtifactManifestDigest: groupCut.ArtifactManifestDigest}}
	permit, err := clusterbackup.AdmitRestore(certificate, evidence, [32]byte{15}, target.Group.ClusterID, target.Group.ClusterIncarnation)
	if err != nil {
		t.Fatal(err)
	}
	schemaRaw, err := vibejson.Marshal(&restoreSchemaSet{Format: 1, Groups: []restoreSchemaSlot{{Ordinal: 0, Schema: template}}})
	if err != nil {
		t.Fatal(err)
	}
	operation, err = clusterrestore.NewOperation(permit, certificate, 0, 1, [32]byte{16}, [32]byte{17}, sha256.Sum256(schemaRaw), []clusterrestore.TargetGroup{target})
	if err != nil {
		t.Fatal(err)
	}
	restoredRoot := filepath.Join(t.TempDir(), "restored")
	if err = os.Mkdir(restoredRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	config := RestoreGroupConfig{Root: restoredRoot, Template: schemaRaw, Operation: operation, Artifact: bytes.NewReader(artifact.Bytes())}
	first, err := RestoreGroup(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	memberRoot := filepath.Join(restoreGroupDirectory(restoredRoot, 0), "replica-1")
	state, err := OpenRestoredReplicaState(memberRoot)
	if err != nil {
		t.Fatal(err)
	}
	held, _, err := sqldriver.OpenReplicatedShardStoreWithApplyForSettlement(filepath.Join(memberRoot, "member.vdb"), state.Identity, options)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	config.Artifact = bytes.NewReader(artifact.Bytes())
	replayed, err := RestoreGroup(context.Background(), config)
	if err != nil || replayed.Witness != first.Witness {
		t.Fatalf("read-only sealed replay=%+v err=%v", replayed, err)
	}
	receiptPath := filepath.Join(memberRoot, "activation.vibejson")
	receipt, err := os.ReadFile(receiptPath)
	if err != nil {
		t.Fatal(err)
	}
	receipt[len(receipt)/2] ^= 1
	if err = os.WriteFile(receiptPath, receipt, 0o600); err != nil {
		t.Fatal(err)
	}
	config.Artifact = bytes.NewReader(artifact.Bytes())
	if _, err = RestoreGroup(context.Background(), config); err == nil {
		t.Fatal("corrupt sealed receipt accepted")
	}
}
