package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type rf3DynamicTemplateFixture struct {
	manifest    rf3Manifest
	preparation splitcontroller.ChildPreparation
	source      raftmember.GroupKey
	registry    rf3ManifestSplitChildRegistry
	bootstrap   *pb.Snapshot
	peers       []rafttransport.PhysicalPeer
}

func testRF3DynamicTemplateFixture(t *testing.T) rf3DynamicTemplateFixture {
	t.Helper()
	root := t.TempDir()
	operation := splitcontroller.OperationID{1}
	descriptor := autosplit.SplitChild{
		Range: distribution.KeyRange{Start: distribution.KeyspacePoint{0x80}, End: distribution.KeyspaceEnd{Max: true}},
		Shard: "child", AllocationGeneration: 8, OwnershipEpoch: 1,
		Leaders: []distribution.EndpointID{"child-native-1", "child-native-2", "child-native-3"},
	}
	limits := durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 64 << 20}
	registry := rf3ManifestSplitChildRegistry{
		Root: filepath.Join(root, "split-children"), MaxOperations: maxRF3SplitChildOperations,
		StageCheckpointBytes: 64 << 20, Table: "docs", CreateTable: "CREATE TABLE docs (PRIMARY KEY (id))",
		WAL: rf3ManifestSplitChildWAL{KeyID: "child-key", KeyMaterialPath: filepath.Join(root, "wal-key"),
			Options: raftstore.Options{MaxFileBytes: 256 << 20, MaxRecordBytes: 16 << 20, MaxRecords: 4096, MaxEntries: 4096, MaxLiveBytes: 128 << 20}},
		Apply: rf3ManifestSplitChildApply{
			MaxSessions: 32, RetryWindow: 8, TxnLimits: limits, Format: sqldriver.ReplicatedPlacementProfileFormat,
			ShardKey: "/id", TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
		},
		ReplicaSetVersion: 1, MemberCount: 3,
	}
	registry.StaticBootstrapPath = filepath.Join(registry.Root, "static-bootstrap.pb")
	authority := sqldriver.ReplicatedAuthorityProfile{
		ActivePolicyGeneration: 1, ProtectionEpoch: 1, OwnershipEpoch: 1,
		SchemaGeneration: 1, RoutingVersion: 12, RouteGeneration: 20,
	}
	identity := raftstore.Identity{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}, Distribution: "orders", Shard: "child",
		AllocationGeneration: 8, ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}, MemberID: 1, StoreID: [16]byte{5},
	}
	replicas := make([]splitcontroller.ChildReplicaTarget, 3)
	peers := make([]rafttransport.PhysicalPeer, 3)
	for index := range replicas {
		number := strconv.Itoa(index + 1)
		replicaWAL := identity
		replicaWAL.MemberID = uint64(index + 1)
		replicaWAL.StoreID = [16]byte{byte(5 + index)}
		binding, err := raftmember.BindingForNewWAL(replicaWAL, 1, authority)
		if err != nil {
			t.Fatal(err)
		}
		localRegistry := registry
		if index != 0 {
			localRegistry.Root = filepath.Join(root, "peer-"+number, "split-children")
		}
		paths, err := localRegistry.childPaths(operation, 1)
		if err != nil {
			t.Fatal(err)
		}
		sqlIdentity, err := sqldriver.NewReplicatedChildShardStoreIdentity(sqldriver.ShardStoreIdentity{
			Distribution: "orders", Shard: "child", AllocationGeneration: 8, LogID: [16]byte{byte(60 + index)},
		}, binding, "docs", strings.Repeat(number, 64), "/id", sqldriver.ReplicatedShardStoreLimits{
			MaxKeyBytes: 256, MaxDocumentBytes: 4 << 20, MaxBatchDocuments: 64, MaxBatchBytes: replication.MaxCommandBytes + 64*256,
		})
		if err != nil {
			t.Fatal(err)
		}
		applyIdentity, err := sqldriver.NewReplicatedChildApplyIdentity(sqlIdentity, strings.Repeat("a"+number, 32), strings.Repeat("b"+number, 32), sqldriver.ReplicatedApplyOptions{
			MaxSessions: registry.Apply.MaxSessions, RetryWindow: registry.Apply.RetryWindow, TxnLimits: limits,
			Placement: sqldriver.ReplicatedPlacementProfile{
				Format: registry.Apply.Format, ShardKey: registry.Apply.ShardKey, Range: descriptor.Range,
				TupleVersion: registry.Apply.TupleVersion, MapperVersion: registry.Apply.MapperVersion,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		replicas[index] = splitcontroller.ChildReplicaTarget{
			Member: replicaWAL.MemberID, Node: [16]byte{byte(20 + index)}, StoreID: replicaWAL.StoreID, NodeIncarnation: 1,
			Endpoint: distribution.EndpointID("child-peer-" + number), NativeEndpoint: descriptor.Leaders[index],
			ControlEndpoint: distribution.EndpointID("child-control-" + number),
			PeerAddress:     "127.0.0.1:" + strconv.Itoa(1100+index*3), NativeAddress: "127.0.0.1:" + strconv.Itoa(1101+index*3),
			ControlAddress: "127.0.0.1:" + strconv.Itoa(1102+index*3), SnapshotAddress: "127.0.0.1:" + strconv.Itoa(9000+index),
			WAL: replicaWAL, RuntimeRoot: paths.Root, WALPath: paths.WAL, SQLPath: paths.Database,
			SQL: sqlIdentity, Apply: applyIdentity,
			CertificateDigest: sha256.Sum256([]byte("replica-certificate-" + number)),
		}
		registry.Members[index] = rf3ManifestMember{
			MemberID: replicaWAL.MemberID, NodeID: replicas[index].Node, StoreID: replicaWAL.StoreID,
			PeerAddress: replicas[index].PeerAddress, NativeAddress: replicas[index].NativeAddress,
		}
		peers[index] = rafttransport.PhysicalPeer{
			NodeID: replicas[index].Node, TrustDomain: rafttransport.TrustDomain{
				ClusterID: identity.ClusterID, ClusterIncarnation: identity.ClusterIncarnation,
			}, Incarnation: 1, Revision: 1, ServiceKeyDigest: sha256.Sum256([]byte("peer-key-" + number)),
			Endpoint: replicas[index].PeerAddress, State: rafttransport.PeerEnrolled,
		}
	}
	relationDigest, err := sqldriver.ReplicatedRelationManifestDigest(replicas[0].SQL)
	if err != nil {
		t.Fatal(err)
	}
	target := splitcontroller.ChildTarget{
		Child: 1, Endpoint: descriptor.Leaders[0], Replicas: replicas, ReplicaSetVersion: 1,
		RelationManifestDigest: relationDigest,
		WAL:                    identity, TopologyRecoveryEpoch: 1, Authority: authority, SQL: replicas[0].SQL.Clone(),
	}
	preparation, err := splitcontroller.NewChildPreparation(operation, [32]byte{9}, descriptor, "docs", target, 0)
	if err != nil {
		t.Fatal(err)
	}
	index, term := uint64(47), uint64(19)
	return rf3DynamicTemplateFixture{
		manifest: rf3Manifest{NodeLog: &rf3NodeLogManifest{}, NodeIncarnation: 1, Digest: [32]byte{1},
			ReplicaControl: rf3ManifestReplicaControl{SourceDataRoot: root}},
		preparation: preparation, registry: registry, peers: peers,
		source: raftmember.GroupKey{ClusterID: identity.ClusterID, ClusterIncarnation: identity.ClusterIncarnation,
			TopologyRecoveryEpoch: 1, ShardIncarnation: [16]byte{7}, GroupID: [16]byte{8}},
		bootstrap: &pb.Snapshot{Data: []byte("retained-parent-bootstrap"), Metadata: &pb.SnapshotMetadata{
			Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
		}},
	}
}

func assertRF3DynamicTemplateResources(t *testing.T, got rf3SplitChildResources, fixture rf3DynamicTemplateFixture) {
	t.Helper()
	digest, err := splitcontroller.ChildPreparationDigest(fixture.preparation)
	if err != nil {
		t.Fatal(err)
	}
	if got.Slot != rf3DynamicTemplateSlot || got.SourceGroup != fixture.source || got.PreparationDigest != digest ||
		!reflect.DeepEqual(got.Registry, fixture.registry) || !proto.Equal(got.Bootstrap, fixture.bootstrap) || !reflect.DeepEqual(got.Peers, fixture.peers) {
		t.Fatalf("recovered template changed source, schema, roster, bootstrap, or preparation: %+v", got)
	}
}

func TestRF3DynamicChildTemplateCrashBeforeRenameAndReopen(t *testing.T) {
	fixture := testRF3DynamicTemplateFixture(t)
	catalog, err := openRF3DynamicChildTemplateCatalog(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	operation, child := [32]byte(fixture.preparation.OperationID()), fixture.preparation.Child()
	if _, found, err := catalog.Resolve(operation, child, fixture.preparation.ReplicaTarget()); err != nil || found {
		t.Fatalf("unpublished template resolved: found=%v err=%v", found, err)
	}
	resources, err := catalog.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers)
	if err != nil {
		t.Fatal(err)
	}
	assertRF3DynamicTemplateResources(t, resources, fixture)
	if second, err := openRF3DynamicChildTemplateCatalog(fixture.manifest); err == nil {
		_ = second.Close()
		t.Fatal("second writer admitted")
	}
	if err := catalog.Close(); err != nil {
		t.Fatal(err)
	}
	operationRoot := filepath.Join(fixture.manifest.ReplicaControl.SourceDataRoot, "dynamic-child-templates", hex.EncodeToString(operation[:]))
	// A crash before rename leaves arbitrary bytes in the temporary file. They
	// cannot replace a committed record or authorize a different child.
	for _, name := range []string{"child-1.tmp", "child-2.tmp"} {
		if err := os.WriteFile(filepath.Join(operationRoot, name), []byte("partial write"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	catalog, err = openRF3DynamicChildTemplateCatalog(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	resources, found, err := catalog.Resolve(operation, child, fixture.preparation.ReplicaTarget())
	if err != nil || !found {
		t.Fatalf("committed template lost across restart: found=%v err=%v", found, err)
	}
	assertRF3DynamicTemplateResources(t, resources, fixture)
	if _, found, err := catalog.Read(operation, 2); err != nil || found {
		t.Fatalf("temporary child became authoritative: found=%v err=%v", found, err)
	}
	if _, err := catalog.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err != nil {
		t.Fatalf("exact retry after interrupted publication: %v", err)
	}
}

func TestRF3DynamicChildTemplateRejectsChangedPublication(t *testing.T) {
	for name, mutate := range map[string]func(*rf3DynamicTemplateFixture){
		"schema": func(f *rf3DynamicTemplateFixture) {
			f.registry.SchemaStatements = []string{"CREATE INDEX docs_value ON docs (id)"}
		},
		"table":             func(f *rf3DynamicTemplateFixture) { f.registry.CreateTable += "; CREATE TABLE forged (id TEXT)" },
		"apply":             func(f *rf3DynamicTemplateFixture) { f.registry.Apply.MaxSessions++ },
		"roster":            func(f *rf3DynamicTemplateFixture) { f.registry.Members[1].StoreID[0]++ },
		"key":               func(f *rf3DynamicTemplateFixture) { f.registry.WAL.KeyID = "substituted-key" },
		"source":            func(f *rf3DynamicTemplateFixture) { f.source.GroupID[0]++ },
		"bootstrap-data":    func(f *rf3DynamicTemplateFixture) { f.bootstrap.Data[0] ^= 1 },
		"bootstrap-roster":  func(f *rf3DynamicTemplateFixture) { f.bootstrap.Metadata.ConfState.Voters[0] = 4 },
		"peer-key":          func(f *rf3DynamicTemplateFixture) { f.peers[0].ServiceKeyDigest[0]++ },
		"peer-incarnation":  func(f *rf3DynamicTemplateFixture) { f.peers[0].Incarnation++ },
		"peer-trust-domain": func(f *rf3DynamicTemplateFixture) { f.peers[0].TrustDomain.ClusterIncarnation[0]++ },
		"peer-address":      func(f *rf3DynamicTemplateFixture) { f.peers[0].Endpoint = "127.0.0.1:8888" },
	} {
		t.Run(name, func(t *testing.T) {
			fixture := testRF3DynamicTemplateFixture(t)
			catalog, err := openRF3DynamicChildTemplateCatalog(fixture.manifest)
			if err != nil {
				t.Fatal(err)
			}
			defer catalog.Close()
			if _, err := catalog.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err != nil {
				t.Fatal(err)
			}
			changed := fixture
			changed.registry.SchemaStatements = append([]string(nil), fixture.registry.SchemaStatements...)
			changed.bootstrap = proto.Clone(fixture.bootstrap).(*pb.Snapshot)
			changed.peers = append([]rafttransport.PhysicalPeer(nil), fixture.peers...)
			mutate(&changed)
			if _, err := catalog.Publish(changed.preparation, changed.source, changed.registry, changed.bootstrap, changed.peers); err == nil {
				t.Fatal("changed publication replaced immutable template")
			}
			got, found, err := catalog.Resolve(fixture.preparation.OperationID(), fixture.preparation.Child(), fixture.preparation.ReplicaTarget())
			if err != nil || !found {
				t.Fatalf("rejected publication lost original template: found=%v err=%v", found, err)
			}
			assertRF3DynamicTemplateResources(t, got, fixture)
		})
	}
}

func TestRF3DynamicChildTemplateRejectsReplicaSubstitution(t *testing.T) {
	fixture := testRF3DynamicTemplateFixture(t)
	catalog, err := openRF3DynamicChildTemplateCatalog(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	if _, err := catalog.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*splitcontroller.ChildReplicaTarget){
		"member":           func(r *splitcontroller.ChildReplicaTarget) { r.Member++ },
		"node":             func(r *splitcontroller.ChildReplicaTarget) { r.Node[0]++ },
		"store":            func(r *splitcontroller.ChildReplicaTarget) { r.StoreID[0]++ },
		"incarnation":      func(r *splitcontroller.ChildReplicaTarget) { r.NodeIncarnation++ },
		"peer-endpoint":    func(r *splitcontroller.ChildReplicaTarget) { r.Endpoint += "-forged" },
		"native-address":   func(r *splitcontroller.ChildReplicaTarget) { r.NativeAddress = "127.0.0.1:8888" },
		"control-address":  func(r *splitcontroller.ChildReplicaTarget) { r.ControlAddress = "127.0.0.1:8888" },
		"snapshot-address": func(r *splitcontroller.ChildReplicaTarget) { r.SnapshotAddress = "127.0.0.1:8888" },
		"wal-path":         func(r *splitcontroller.ChildReplicaTarget) { r.WALPath += ".forged" },
		"sql-path":         func(r *splitcontroller.ChildReplicaTarget) { r.SQLPath += ".forged" },
		"schema":           func(r *splitcontroller.ChildReplicaTarget) { r.SQL.RelationManifestDigest[0]++ },
		"apply":            func(r *splitcontroller.ChildReplicaTarget) { r.Apply.ValidationDigest[0]++ },
		"certificate":      func(r *splitcontroller.ChildReplicaTarget) { r.CertificateDigest[0]++ },
	} {
		t.Run(name, func(t *testing.T) {
			target := fixture.preparation.ReplicaTarget()
			mutate(&target)
			if _, _, err := catalog.Resolve(fixture.preparation.OperationID(), fixture.preparation.Child(), target); err == nil {
				t.Fatal("template authorized a different replica identity")
			}
		})
	}
}

func TestRF3DynamicChildTemplateRejectsChangedNodeIncarnation(t *testing.T) {
	fixture := testRF3DynamicTemplateFixture(t)
	catalog, err := openRF3DynamicChildTemplateCatalog(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Close(); err != nil {
		t.Fatal(err)
	}
	fixture.manifest.NodeIncarnation++
	catalog, err = openRF3DynamicChildTemplateCatalog(fixture.manifest)
	if err != nil {
		return
	}
	defer catalog.Close()
	if _, _, err := catalog.Resolve(fixture.preparation.OperationID(), fixture.preparation.Child(), fixture.preparation.ReplicaTarget()); err == nil {
		t.Fatal("old node incarnation authorized a recovered child")
	}
}

func TestRF3DynamicChildTemplateRejectsSymlinks(t *testing.T) {
	for _, name := range []string{"catalog-root", "operation", "record", "temporary"} {
		t.Run(name, func(t *testing.T) {
			fixture := testRF3DynamicTemplateFixture(t)
			outside := t.TempDir()
			sentinel := filepath.Join(outside, "sentinel")
			original := []byte("foreign file must stay untouched")
			if err := os.WriteFile(sentinel, original, 0o600); err != nil {
				t.Fatal(err)
			}
			catalogRoot := filepath.Join(fixture.manifest.ReplicaControl.SourceDataRoot, "dynamic-child-templates")
			if name == "catalog-root" {
				if err := os.Symlink(outside, catalogRoot); err != nil {
					t.Fatal(err)
				}
				if catalog, err := openRF3DynamicChildTemplateCatalog(fixture.manifest); err == nil {
					_ = catalog.Close()
					t.Fatal("symlinked catalog root admitted")
				}
				return
			}
			catalog, err := openRF3DynamicChildTemplateCatalog(fixture.manifest)
			if err != nil {
				t.Fatal(err)
			}
			defer catalog.Close()
			operation := fixture.preparation.OperationID()
			operationRoot := filepath.Join(catalogRoot, hex.EncodeToString(operation[:]))
			if name == "operation" {
				if err := os.Symlink(outside, operationRoot); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.Mkdir(operationRoot, 0o700); err != nil {
					t.Fatal(err)
				}
				filename := "child-1.template"
				if name == "temporary" {
					filename = "child-1.tmp"
				}
				if err := os.Symlink(sentinel, filepath.Join(operationRoot, filename)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := catalog.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err == nil && name != "temporary" {
				t.Fatal("publication followed a symlink")
			}
			if name != "temporary" {
				if _, _, err := catalog.Read(operation, fixture.preparation.Child()); err == nil {
					t.Fatal("recovery followed a symlink")
				}
			}
			if got, err := os.ReadFile(sentinel); err != nil || !bytes.Equal(got, original) {
				t.Fatalf("foreign file changed: %q %v", got, err)
			}
		})
	}
}

func writeRF3DynamicTemplateReceipt(t *testing.T, fixture rf3DynamicTemplateFixture, preparation splitcontroller.ChildPreparation) splitcontroller.ChildPrepareReceipt {
	t.Helper()
	receipt, err := splitcontroller.NewChildPrepareReceipt(preparation, preparation.ReplicaTarget())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := splitcontroller.AppendChildPrepareReceipt(nil, receipt)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := fixture.registry.childPaths(preparation.OperationID(), preparation.Child())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(paths.Root, 0o700); err != nil {
		t.Fatal(err)
	}
	for path, data := range map[string][]byte{
		filepath.Join(paths.Root, rf3ChildPrepareReceiptName): raw,
		paths.Database: nil, paths.WAL: nil,
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return receipt
}

func TestRF3DynamicChildTemplateRecoversAdoptedGroupOnOriginallyEmptyNode(t *testing.T) {
	fixture := testRF3DynamicTemplateFixture(t)
	if groups := fixture.manifest.groupBundles(); len(groups) != 0 {
		t.Fatalf("fixture has %d startup groups", len(groups))
	}
	inventory, err := openRF3AdoptedGroupInventory(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inventory.templates.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err != nil {
		t.Fatal(err)
	}
	target := fixture.preparation.ReplicaTarget()
	// A durable preparation template alone does not certify adoption.
	if groups, err := inventory.recoveryGroups(target.Node); err != nil || len(groups) != 0 {
		t.Fatalf("template alone adopted child: groups=%d err=%v", len(groups), err)
	}
	receipt := writeRF3DynamicTemplateReceipt(t, fixture, fixture.preparation)
	entry := rf3AdoptedGroupEntry{
		operation: receipt.Operation, receipt: receipt.ReceiptDigest, plan: [32]byte{1},
		certificate: target.CertificateDigest, cutover: [32]byte{2}, group: rf3DynamicTemplateSlot, child: uint64(receipt.Child),
	}
	if err := inventory.record(entry); err != nil {
		t.Fatal(err)
	}
	if err := inventory.Close(); err != nil {
		t.Fatal(err)
	}
	inventory, err = openRF3AdoptedGroupInventory(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer inventory.Close()
	groups, err := inventory.recoveryGroups(target.Node)
	if err != nil || len(groups) != 1 {
		t.Fatalf("adopted child recovery: groups=%d err=%v", len(groups), err)
	}
	got := groups[0]
	if got.bundle.Route.Group != groupFromBinding(target.SQL.Binding) || got.bundle.Route.MemberID != target.Member ||
		got.bundle.Route.StoreID != target.StoreID || got.bundle.Route.MemberRoot != target.RuntimeRoot ||
		got.bundle.SQL.Path != target.SQLPath || got.bundle.WAL.Path != target.WALPath ||
		got.bundle.Members != fixture.registry.Members || got.bundle.MemberCount != fixture.registry.MemberCount ||
		!reflect.DeepEqual(got.bundle.ChildRegistry, fixture.registry) || !got.base.Equal(target.SQL) ||
		!reflect.DeepEqual(got.apply, target.Apply) || got.runtimeDigest != target.CertificateDigest {
		t.Fatalf("adopted recovery lost exact dynamic child: %+v", got)
	}
	wrongNode := target.Node
	wrongNode[0]++
	if _, err := inventory.recoveryGroups(wrongNode); err == nil {
		t.Fatal("adopted child recovered as a different physical node")
	}
}

func TestRF3DynamicChildTemplateRejectsReceiptForDifferentPreparation(t *testing.T) {
	fixture := testRF3DynamicTemplateFixture(t)
	inventory, err := openRF3AdoptedGroupInventory(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inventory.templates.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err != nil {
		t.Fatal(err)
	}
	// A perfectly valid receipt for another preparation of the same replica
	// cannot replace the receipt authorized by the immutable template.
	changed, err := splitcontroller.NewChildPreparation(fixture.preparation.OperationID(), [32]byte{10},
		fixture.preparation.Descriptor(), fixture.preparation.Collection(), fixture.preparation.Target(), fixture.preparation.ReplicaIndex())
	if err != nil {
		t.Fatal(err)
	}
	receipt := writeRF3DynamicTemplateReceipt(t, fixture, changed)
	entry := rf3AdoptedGroupEntry{
		operation: receipt.Operation, receipt: receipt.ReceiptDigest, plan: [32]byte{1},
		certificate: receipt.Target.CertificateDigest, cutover: [32]byte{2}, group: rf3DynamicTemplateSlot, child: uint64(receipt.Child),
	}
	if err := inventory.record(entry); err != nil {
		t.Fatal(err)
	}
	if err := inventory.Close(); err != nil {
		t.Fatal(err)
	}
	inventory, err = openRF3AdoptedGroupInventory(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer inventory.Close()
	if _, err := inventory.recoveryGroups(receipt.Target.Node); err == nil {
		t.Fatal("changed preparation receipt recovered as the adopted child")
	}
}

func TestRF3DynamicChildTemplateRejectsUnboundAdoptionRecord(t *testing.T) {
	fixture := testRF3DynamicTemplateFixture(t)
	inventory, err := openRF3AdoptedGroupInventory(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer inventory.Close()
	target := fixture.preparation.ReplicaTarget()
	entry := rf3AdoptedGroupEntry{
		operation: fixture.preparation.OperationID(), receipt: [32]byte{1}, plan: [32]byte{2},
		certificate: target.CertificateDigest, cutover: [32]byte{3}, group: rf3DynamicTemplateSlot, child: uint64(fixture.preparation.Child()),
	}
	if err := inventory.record(entry); err == nil {
		t.Fatal("adoption recorded without a durable dynamic template")
	}
	if _, err := inventory.templates.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*rf3AdoptedGroupEntry){
		"startup-slot":    func(entry *rf3AdoptedGroupEntry) { entry.group = 0 },
		"certificate":     func(entry *rf3AdoptedGroupEntry) { entry.certificate[0]++ },
		"other-operation": func(entry *rf3AdoptedGroupEntry) { entry.operation[0]++ },
		"other-child":     func(entry *rf3AdoptedGroupEntry) { entry.child = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := entry
			mutate(&changed)
			if err := inventory.record(changed); err == nil {
				t.Fatal("unbound adoption recorded")
			}
		})
	}
	if inventory.liveCount() != 0 {
		t.Fatal("rejected adoption consumed a live group slot")
	}
}

func TestRF3DynamicChildTemplateUncertainPublicationRequiresReopen(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(strconv.Itoa(failAt), func(t *testing.T) {
			fixture := testRF3DynamicTemplateFixture(t)
			catalog, err := openRF3DynamicChildTemplateCatalog(fixture.manifest)
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected directory sync failure")
			syncDirectory, calls := catalog.syncDirectory, 0
			catalog.syncDirectory = func(root *os.Root) error {
				calls++
				if calls == failAt {
					return injected
				}
				return syncDirectory(root)
			}
			if _, err := catalog.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); !errors.Is(err, injected) {
				t.Fatalf("uncertain publication result: %v", err)
			}
			operation, child := fixture.preparation.OperationID(), fixture.preparation.Child()
			if _, _, err := catalog.Read(operation, child); err == nil {
				t.Fatal("uncertain handle authorized a template")
			}
			if _, err := catalog.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err == nil {
				t.Fatal("uncertain handle accepted a retry without reopen")
			}
			if err := catalog.Close(); err != nil {
				t.Fatal(err)
			}
			catalog, err = openRF3DynamicChildTemplateCatalog(fixture.manifest)
			if err != nil {
				t.Fatal(err)
			}
			defer catalog.Close()
			resources, found, err := catalog.Resolve(operation, child, fixture.preparation.ReplicaTarget())
			// The first sync is before writing the record; the second is after
			// rename, when recovery must honor the exact surviving commit.
			if err != nil || found != (failAt == 2) {
				t.Fatalf("reopen resolved the wrong publication cut: found=%v err=%v", found, err)
			}
			if found {
				assertRF3DynamicTemplateResources(t, resources, fixture)
			}
		})
	}
}

func TestRF3DynamicChildTemplateRejectsInvalidInitialPublication(t *testing.T) {
	for name, mutate := range map[string]func(*rf3DynamicTemplateFixture){
		"schema": func(f *rf3DynamicTemplateFixture) {
			f.registry.SchemaStatements = []string{"CREATE INDEX forged ON docs (id)"}
		},
		"roster":            func(f *rf3DynamicTemplateFixture) { f.registry.Members[1].NodeID[0]++ },
		"source-cluster":    func(f *rf3DynamicTemplateFixture) { f.source.ClusterID[0]++ },
		"bootstrap-roster":  func(f *rf3DynamicTemplateFixture) { f.bootstrap.Metadata.ConfState.Voters[0] = 4 },
		"peer-key":          func(f *rf3DynamicTemplateFixture) { f.peers[0].ServiceKeyDigest = [32]byte{} },
		"peer-incarnation":  func(f *rf3DynamicTemplateFixture) { f.peers[0].Incarnation++ },
		"peer-count":        func(f *rf3DynamicTemplateFixture) { f.peers = f.peers[:2] },
		"peer-trust-domain": func(f *rf3DynamicTemplateFixture) { f.peers[0].TrustDomain.ClusterIncarnation[0]++ },
	} {
		t.Run(name, func(t *testing.T) {
			fixture := testRF3DynamicTemplateFixture(t)
			catalog, err := openRF3DynamicChildTemplateCatalog(fixture.manifest)
			if err != nil {
				t.Fatal(err)
			}
			defer catalog.Close()
			mutate(&fixture)
			if _, err := catalog.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err == nil {
				t.Fatal("invalid initial template became authoritative")
			}
			if _, found, err := catalog.Read(fixture.preparation.OperationID(), fixture.preparation.Child()); err != nil || found {
				t.Fatalf("invalid publication left authoritative state: found=%v err=%v", found, err)
			}
		})
	}
}

func TestRF3DynamicChildTemplateDistinguishesPhysicalAndRuntimeIncarnations(t *testing.T) {
	fixture := testRF3DynamicTemplateFixture(t)
	fixture.manifest.NodeIncarnation = 2
	fixture.peers[0].Incarnation = 2
	if fixture.preparation.ReplicaTarget().NodeIncarnation != 1 {
		t.Fatal("fixture runtime incarnation changed")
	}
	catalog, err := openRF3DynamicChildTemplateCatalog(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := catalog.Publish(fixture.preparation, fixture.source, fixture.registry, fixture.bootstrap, fixture.peers); err != nil {
		t.Fatalf("valid replacement physical incarnation cannot prepare new child: %v", err)
	}
	if err := catalog.Close(); err != nil {
		t.Fatal(err)
	}
	catalog, err = openRF3DynamicChildTemplateCatalog(fixture.manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer catalog.Close()
	resources, found, err := catalog.Resolve(fixture.preparation.OperationID(), fixture.preparation.Child(), fixture.preparation.ReplicaTarget())
	if err != nil || !found {
		t.Fatalf("valid physical/runtime incarnation pair lost at reopen: found=%v err=%v", found, err)
	}
	assertRF3DynamicTemplateResources(t, resources, fixture)
}
