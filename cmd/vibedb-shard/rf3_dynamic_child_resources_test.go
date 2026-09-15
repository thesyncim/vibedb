package main

import (
	"crypto/sha256"
	"net"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// The reservation was issued for original voters 1/2/3 and learner 4. The
// actual local group has since committed the stable voter set 2/3/4. Child
// preparation must use that live source, even though the node manifest is empty.
func testRF3MovedChildResources(t *testing.T) (*rf3DynamicChildResources, splitcontroller.ChildPreparation,
	*rf3SchemaGeneration, *rf3AdoptedTestOwner,
) {
	t.Helper()
	f := newRF3NodeRecoveryFixtureWithLearner(t, true)
	if _, err := f.store.BeginIncarnations([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	log, _ := f.store.GroupByID(f.boots[0].Descriptor.GroupID)
	db, apply, err := openRF3SelectedLog(f.paths[0], log, f.bases[0], f.applies[0])
	if err != nil {
		t.Fatal(err)
	}
	owner := &dynamicSchemaOwner{log: log, db: db, apply: apply, identity: dynamicSchemaIdentity(t, apply, 1)}
	t.Cleanup(func() { _ = owner.apply.Close(); _ = owner.db.Close() })
	schemas, err := newRF3SchemaActivator(owner, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	spec := dynamicSchemaSpec(t, f, owner)
	memberRoot := filepath.Dir(f.paths[0])
	if err := schemas.RegisterDynamic(owner.identity, apply, spec, memberRoot, log); err != nil {
		t.Fatal(err)
	}
	state, err := schemas.generation(schemainstall.Request{Group: owner.identity.Group})
	if err != nil {
		t.Fatal(err)
	}
	publication, err := apply.ApplyConfiguration(raftmodel.ApplyMeta{Index: 2, Term: 2, Type: pb.EntryConfChangeV2},
		&pb.ConfState{Voters: []uint64{2, 3, 4}})
	if err != nil {
		t.Fatal(err)
	}
	fixture := testRF3DynamicTemplateFixture(t)
	target := fixture.preparation.Target()
	members := make([]rafttransport.Member, len(target.Replicas))
	peers := make([]rafttransport.PhysicalPeer, len(target.Replicas))
	for index := range target.Replicas {
		replica := &target.Replicas[index]
		replica.Member, replica.WAL.MemberID = uint64(index+2), uint64(index+2)
		replica.WAL.Distribution = owner.identity.Distribution
		if index == 2 {
			replica.Node = rafttransport.NodeID(f.node.NodeID)
			paths, err := (rf3ManifestSplitChildRegistry{Root: filepath.Join(memberRoot, "split-children")}).childPaths(fixture.preparation.OperationID(), fixture.preparation.Child())
			if err != nil {
				t.Fatal(err)
			}
			replica.RuntimeRoot, replica.SQLPath, replica.WALPath = paths.Root, paths.Database, paths.WAL
		}
		binding, err := raftmember.BindingForNewWAL(replica.WAL, 1, target.Authority)
		if err != nil {
			t.Fatal(err)
		}
		storages := make([]string, f.bases[0].RelationCount)
		for relation := range storages {
			storages[relation] = strings.Repeat(strconv.Itoa(index+1), 62) + strconv.Itoa(relation+10)
		}
		replica.SQL, err = sqldriver.NewReplicatedChildShardStoreBundleIdentity(sqldriver.ShardStoreIdentity{
			Distribution: distribution.DistributionName(replica.WAL.Distribution), Shard: distribution.ShardID(replica.WAL.Shard),
			AllocationGeneration: distribution.ShardAllocationGeneration(replica.WAL.AllocationGeneration), LogID: [16]byte{byte(70 + index)},
		}, binding, f.bases[0], storages)
		if err != nil {
			t.Fatal(err)
		}
		options := f.applyOptions
		options.Placement.Range = fixture.preparation.Descriptor().Range
		replica.Apply, err = sqldriver.NewReplicatedChildApplyIdentity(replica.SQL, strings.Repeat(strconv.Itoa(index+4), 64), strings.Repeat(strconv.Itoa(index+7), 64), options)
		if err != nil {
			t.Fatal(err)
		}
		members[index] = rafttransport.Member{Group: owner.identity.Group, ReplicaSetVersion: publication.ReplicaSetVersion,
			MemberID: replica.Member, Node: replica.Node, Role: rafttransport.MemberVoter}
		peers[index] = rafttransport.PhysicalPeer{NodeID: replica.Node,
			TrustDomain: rafttransport.TrustDomain{ClusterID: f.node.ClusterID, ClusterIncarnation: f.node.ClusterIncarnation},
			Incarnation: 1, Revision: uint64(index + 7), ServiceKeyDigest: sha256.Sum256([]byte("live-peer-" + strconv.Itoa(index))),
			Endpoint: replica.PeerAddress, State: rafttransport.PeerEnrolled}
	}
	target.WAL, target.SQL = target.Replicas[0].WAL, target.Replicas[0].SQL.Clone()
	target.RelationManifestDigest, err = sqldriver.ReplicatedRelationManifestDigest(target.SQL)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err := splitcontroller.NewChildPreparation(fixture.preparation.OperationID(), fixture.preparation.AllocationDigest(),
		fixture.preparation.Descriptor(), "docs", target, 2)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := rafttransport.NewStaticRegistryWithPhysicalPeers(rafttransport.NodeID(f.node.NodeID), members, peers,
		rafttransport.Limits{MaxGroups: 4, MaxMembers: 12})
	if err != nil {
		t.Fatal(err)
	}
	observations := &rf3AdoptedTestOwner{observation: raftservice.ReplicaObservation{Identity: owner.identity,
		Publication: publication, State: replicatedstate.State{Binding: replicatedstate.Binding{
			SchemaGeneration: 1, ActivePolicyGeneration: 1, ProtectionEpoch: 1}}}}
	manifest := fixture.manifest
	manifest.ReplicaControl.SourceDataRoot = filepath.Dir(f.path)
	manifest.NodeLog.KeyID, manifest.NodeLog.KeyMaterialPath = f.key.ID, filepath.Join(filepath.Dir(f.path), "physical-node-key")
	manifest.SplitControl.MaxOperations = 2
	catalog, err := openRF3DynamicChildTemplateCatalog(manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = catalog.Close() })
	resources, err := newRF3DynamicChildResources(manifest, &rf3NodeOwner{store: f.store}, schemas, observations, registry, catalog)
	if err != nil {
		t.Fatal(err)
	}
	return resources, preparation, state, observations
}

func TestRF3DynamicChildResourcesFreezeMovedSourceAndExactRetry(t *testing.T) {
	resources, preparation, state, owner := testRF3MovedChildResources(t)
	if len(resources.manifest.groupBundles()) != 0 {
		t.Fatal("fixture node unexpectedly has startup groups")
	}
	prepared, err := resources.PrepareResources(t.Context(), preparation)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(prepared.Bootstrap.GetMetadata().GetConfState().Voters, []uint64{2, 3, 4}) ||
		prepared.Bootstrap.GetMetadata().GetIndex() != 1 || prepared.Registry.StaticBootstrapPath != "" ||
		prepared.Registry.Root != filepath.Join(state.manifest.Route.MemberRoot, "split-children") ||
		prepared.Registry.WAL.KeyID != resources.manifest.NodeLog.KeyID ||
		prepared.Registry.WAL.KeyMaterialPath != resources.manifest.NodeLog.KeyMaterialPath || prepared.Registry.MaxOperations != 2 {
		t.Fatal("child inherited stale bootstrap, source roster, paths, key, or limits")
	}
	for index, member := range prepared.Registry.Members[:prepared.Registry.MemberCount] {
		if member.MemberID != uint64(index+2) || prepared.Peers[index].Revision != uint64(index+7) ||
			member.StoreID != preparation.Target().Replicas[index].StoreID {
			t.Fatal("frozen child roster lost committed membership or exact peer pins")
		}
	}
	state.mu.Lock()
	state.quiesced = true
	state.mu.Unlock()
	owner.observation.Identity.NodeIncarnation++
	if got, err := resources.PrepareResources(t.Context(), preparation); err != nil || !reflect.DeepEqual(got, prepared) {
		t.Fatalf("exact admitted retry consulted departed source: %v", err)
	}
	changed := preparation.Target()
	changed.Replicas[0].CertificateDigest[0]++
	other, err := splitcontroller.NewChildPreparation(preparation.OperationID(), preparation.AllocationDigest(), preparation.Descriptor(),
		preparation.Collection(), changed, preparation.ReplicaIndex())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resources.PrepareResources(t.Context(), other); err == nil {
		t.Fatal("same local replica accepted altered remote preparation")
	}
	if err := resources.Close(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := resources.catalog.Read(preparation.OperationID(), preparation.Child()); err != nil || !found {
		t.Fatalf("resource close closed shared inventory catalog: %v", err)
	}
}

func TestRF3DynamicChildResourcesRejectUnpublishedOrChangedSource(t *testing.T) {
	resources, preparation, state, owner := testRF3MovedChildResources(t)
	original := owner.observation
	for name, mutate := range map[string]func(){
		"runtime":            func() { owner.observation.Identity.NodeIncarnation++ },
		"schema":             func() { owner.observation.State.Binding.SchemaGeneration++ },
		"policy":             func() { owner.observation.State.Binding.ActivePolicyGeneration++ },
		"protection":         func() { owner.observation.State.Binding.ProtectionEpoch++ },
		"membership-version": func() { owner.observation.Publication.ReplicaSetVersion++ },
		"membership":         func() { owner.observation.Publication.ConfState = &pb.ConfState{Voters: []uint64{1, 2, 3}} },
		"quiesced":           func() { state.quiesced = true },
	} {
		t.Run(name, func(t *testing.T) {
			owner.observation = original
			state.quiesced = false
			mutate()
			if _, err := resources.PrepareResources(t.Context(), preparation); err == nil {
				t.Fatal("unpublished or changed source supplied child authority")
			}
			if _, found, err := resources.ReadResources(preparation.OperationID(), preparation.Child()); err != nil || found {
				t.Fatalf("rejected source left immutable child authority: found=%v err=%v", found, err)
			}
		})
	}
}

func TestRF3DynamicChildPreparerRecoversOnEmptyManifest(t *testing.T) {
	resources, preparation, state, _ := testRF3MovedChildResources(t)
	target := preparation.ReplicaTarget()
	address := func(value string) net.Addr {
		result, err := net.ResolveTCPAddr("tcp", value)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	open := func() *rf3GroupChildPreparer {
		p, err := newRF3GroupChildPreparer(resources.manifest, target.Node, address(target.PeerAddress), address(target.NativeAddress),
			address(target.ControlAddress), address(target.SnapshotAddress), resources)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	preparer := open()
	receipt, err := preparer.PrepareChild(t.Context(), preparation)
	if err != nil {
		_ = preparer.Close()
		t.Fatal(err)
	}
	if err := preparer.Close(); err != nil {
		t.Fatal(err)
	}
	state.quiesced = true
	preparer = open()
	defer preparer.Close()
	retried, err := preparer.PrepareChild(t.Context(), preparation)
	if err != nil || !reflect.DeepEqual(retried, receipt) {
		t.Fatalf("restart lost exact child receipt: %v", err)
	}
}

func TestRF3DynamicChildBootstrapRejectsRosterSubstitution(t *testing.T) {
	fixture := testRF3DynamicTemplateFixture(t)
	bootstrap, err := rf3DynamicSplitChildBootstrap(fixture.preparation.Target(), fixture.registry.Members[:])
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(bootstrap.Metadata.ConfState, &pb.ConfState{Voters: []uint64{1, 2, 3}}) {
		t.Fatal("wrong child bootstrap")
	}
	fixture.registry.Members[1].NodeID[0]++
	if _, err := rf3DynamicSplitChildBootstrap(fixture.preparation.Target(), fixture.registry.Members[:]); err == nil {
		t.Fatal("substituted physical node became bootstrap member")
	}
}
