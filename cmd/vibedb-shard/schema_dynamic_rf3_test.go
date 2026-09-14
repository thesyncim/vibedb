package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type dynamicSchemaArtifact string

func (path dynamicSchemaArtifact) OpenArtifact(schemainstall.Request) (*os.File, error) {
	return os.Open(string(path))
}

// This owner persists and applies the one test transition in the real shared
// node log. Quorum behavior is covered by the schema process qualification.
type dynamicSchemaOwner struct {
	rf3SchemaOwner
	log       *raftstore.GroupView
	db        *sqldriver.Database
	apply     *sqldriver.ReplicatedApply
	identity  raftmember.RuntimeIdentity
	committed bool
}

func (o *dynamicSchemaOwner) Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error) {
	profile, err := o.apply.CapacityQualificationProfile()
	if err != nil {
		return raftservice.ServingState{}, err
	}
	return raftservice.ServingState{Command: commandFenceFromPublication(profile.Binding.Authority, o.identity, o.apply.Published().ReplicaSetVersion)}, nil
}

func (o *dynamicSchemaOwner) ObserveSchemaTransition(context.Context, raftmember.GroupKey, []byte) (bool, error) {
	return o.committed, nil
}

func (o *dynamicSchemaOwner) ProposeSchemaTransition(_ context.Context, _ raftservice.ServingFence, raw []byte) error {
	index, term, kind := o.apply.Applied()+1, uint64(2), pb.EntryNormal
	if err := o.log.Persist(raftmodel.PersistBatch{NodeIncarnation: o.identity.NodeIncarnation,
		ReadyID: 1, MustSync: true, HardState: &pb.HardState{Term: &term, Commit: &index},
		Entries: []*pb.Entry{{Index: &index, Term: &term, Type: &kind, Data: raw}},
	}); err != nil {
		return err
	}
	_, err := o.apply.ApplyNormal(raftmodel.ApplyMeta{Index: index, Term: term, Type: kind}, raw)
	o.committed = err == nil
	return err
}

func (o *dynamicSchemaOwner) QuiesceCommittedSchemaGeneration(context.Context, raftmember.GroupKey, []byte) error {
	return errors.Join(o.apply.Close(), o.db.Close())
}

func (o *dynamicSchemaOwner) InstallSchemaGeneration(_ context.Context, _ raftmember.GroupKey,
	db *sqldriver.Database, apply *sqldriver.ReplicatedApply,
	_ sqldriver.ReplicatedShardStoreIdentity, _ sqldriver.ReplicatedApplyIdentity,
) error {
	profile, err := apply.CapacityQualificationProfile()
	if err != nil {
		return err
	}
	o.db, o.apply = db, apply
	o.identity.RelationManifestDigest = profile.RelationManifestDigest
	return nil
}

func dynamicSchemaIdentity(t *testing.T, apply *sqldriver.ReplicatedApply, incarnation uint64) raftmember.RuntimeIdentity {
	t.Helper()
	profile, err := apply.CapacityQualificationProfile()
	if err != nil {
		t.Fatal(err)
	}
	b := profile.Binding
	return raftmember.RuntimeIdentity{Group: groupFromBinding(b), Distribution: b.Distribution, Shard: b.Shard,
		AllocationGeneration: b.AllocationGeneration, MemberID: b.MemberID, StoreID: b.StoreID,
		NodeIncarnation: incarnation, RelationManifestDigest: profile.RelationManifestDigest}
}

func dynamicSchemaSpec(t *testing.T, f *rf3NodeRecoveryFixture, owner *dynamicSchemaOwner) nodecontrol.PreparationSpec {
	t.Helper()
	serving, err := owner.Probe(t.Context(), owner.identity.Group)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, err := proto.MarshalOptions{Deterministic: true}.Marshal(f.boots[0].Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	member := func(id uint64) nodecontrol.PreparationMember {
		return nodecontrol.PreparationMember{MemberID: id, Node: rafttransport.NodeID{byte(id + 10)},
			PeerEndpoint: distribution.EndpointID(fmt.Sprintf("peer-%d", id)), NativeEndpoint: distribution.EndpointID(fmt.Sprintf("native-%d", id)),
			ControlEndpoint: distribution.EndpointID(fmt.Sprintf("control-%d", id)), PeerAddress: "127.0.0.1:10001",
			NativeAddress: "127.0.0.1:10002", ControlAddress: "127.0.0.1:10003", SnapshotAddress: "127.0.0.1:10004",
			ServiceKeyDigest: [32]byte{byte(id)}, NodeIncarnation: 1, NodeRevision: 1}
	}
	target := member(4)
	target.Node = rafttransport.NodeID(f.node.NodeID)
	return nodecontrol.PreparationSpec{Kind: nodecontrol.PreparationSpecKind, Group: owner.identity.Group,
		Distribution: distribution.DistributionName(owner.identity.Distribution), Shard: distribution.ShardID(owner.identity.Shard),
		AllocationGeneration: distribution.ShardAllocationGeneration(owner.identity.AllocationGeneration), SourceCommand: serving.Command,
		LogicalSchemaDigest: owner.identity.RelationManifestDigest, InitialVoters: [3]nodecontrol.PreparationMember{member(1), member(2), member(3)},
		Target: target, TargetStoreID: owner.identity.StoreID, TargetNodeIncarnation: 1,
		Table: "docs", CreateTable: "CREATE TABLE docs (PRIMARY KEY (id))",
		SourceBootstrap: bootstrap, SourceBootstrapDigest: sha256.Sum256(bootstrap),
		Log: nodecontrol.PreparationLogProfile{MaxFileBytes: 1 << 20, MaxRecordBytes: 1 << 16, MaxRecords: 64, MaxEntries: 64, MaxLiveBytes: 1 << 20},
		Apply: nodecontrol.PreparationApplyProfile{MaxSessions: f.applyOptions.MaxSessions, RetryWindow: f.applyOptions.RetryWindow,
			MaxCollections: f.applyOptions.TxnLimits.MaxCollections, MaxDocuments: f.applyOptions.TxnLimits.MaxDocuments,
			MaxBytes: f.applyOptions.TxnLimits.MaxBytes, ShardKey: "/id"}}
}

func TestRF3DynamicSchemaBuildActivateAndRecover(t *testing.T) {
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
	activator, err := newRF3SchemaActivator(owner, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	spec := dynamicSchemaSpec(t, f, owner)
	root := filepath.Dir(f.paths[0])
	identity := owner.identity
	if _, err := activator.generation(schemainstall.Request{Group: identity.Group}); !errors.Is(err, schemainstall.ErrInvalid) {
		t.Fatalf("unadopted group was visible: %v", err)
	}
	foreign := identity
	foreign.StoreID[0]++
	if err := activator.RegisterDynamic(foreign, apply, spec, root, log); !errors.Is(err, schemainstall.ErrConflict) {
		t.Fatalf("foreign identity accepted: %v", err)
	}
	wrongSpec := spec
	wrongSpec.Target.Node[0]++
	if err := activator.RegisterDynamic(identity, apply, wrongSpec, root, log); !errors.Is(err, schemainstall.ErrConflict) {
		t.Fatalf("foreign physical node accepted: %v", err)
	}
	wrongSpec = spec
	wrongSpec.SourceCommand.RelationManifestDigest[0]++
	if err := activator.RegisterDynamic(identity, apply, wrongSpec, root, log); !errors.Is(err, schemainstall.ErrConflict) {
		t.Fatalf("foreign same-generation schema accepted: %v", err)
	}
	wrongLog, _ := f.store.GroupByID(f.boots[1].Descriptor.GroupID)
	if err := activator.RegisterDynamic(identity, apply, spec, root, wrongLog); !errors.Is(err, schemainstall.ErrConflict) {
		t.Fatalf("foreign shared log accepted: %v", err)
	}
	if err := activator.RegisterDynamic(identity, apply, spec, root, log); err != nil {
		t.Fatal(err)
	}
	state, err := activator.generation(schemainstall.Request{Group: identity.Group})
	if err != nil {
		t.Fatal(err)
	}
	refreshedLog, _ := f.store.GroupByID(identity.Group.GroupID)
	if err := activator.RegisterDynamic(identity, apply, spec, root, refreshedLog); err != nil || activator.groups[identity.Group] != state {
		t.Fatalf("exact retry replaced live state: %v", err)
	}
	spec.SourceBootstrap[0] ^= 1
	if state.preparation.SourceBootstrap[0] == spec.SourceBootstrap[0] {
		t.Fatal("registration retained caller-owned bootstrap bytes")
	}
	spec.SourceBootstrap[0] ^= 1
	const sql = "ALTER TABLE docs ADD COLUMN marker TEXT"
	request := schemainstall.BuildRequest{Operation: [32]byte{71}, Group: identity.Group,
		AllocationGeneration: 1, FromSchemaGeneration: 1, FromRelationManifestDigest: identity.RelationManifestDigest,
		SourceApplied: apply.Applied(), SQLBytes: uint64(len(sql)), SQLDigest: sha256.Sum256([]byte(sql))}
	target, err := activator.BuildSchema(t.Context(), request, sql)
	if err != nil {
		t.Fatalf("adopted schema build: %v", err)
	}
	artifact := filepath.Join(t.TempDir(), "catalog")
	if err := os.WriteFile(artifact, target.Catalog, 0600); err != nil {
		t.Fatal(err)
	}
	activator.files = dynamicSchemaArtifact(artifact)
	install := schemainstall.Request{Operation: request.Operation, Group: identity.Group, AllocationGeneration: 1,
		FromSchemaGeneration: 1, FromRelationManifestDigest: request.FromRelationManifestDigest,
		ToSchemaGeneration: target.Proof.Catalog.SchemaGeneration, ToRelationManifestDigest: target.Proof.Catalog.RelationManifestDigest,
		ApplyContractDigest: target.Proof.ApplyContract, BundleDigest: target.Proof.Catalog.Digest, BundleBytes: uint64(len(target.Catalog))}
	witness, err := activator.Stage(t.Context(), install, install.BundleDigest, "")
	if err != nil {
		t.Fatalf("adopted schema preparation: %v", err)
	}
	installation := schemainstall.InstallationDigest(install, schemainstall.MaterializedArtifactDigest(install.BundleDigest, witness))
	authorization := schemainstall.Authorization{Operation: request.Operation, TargetCatalogGeneration: 2,
		TargetCatalogDigest: [32]byte{72}, PreparedGroupCount: 1, PreparedGroupRoot: [32]byte{73}, ContractDigest: schemainstall.ContractDigest()}
	if err := activator.Activate(t.Context(), install, authorization, installation, ""); err != nil {
		t.Fatalf("adopted schema activation: %v", err)
	}
	if active, err := activator.ObserveActive(t.Context(), install, authorization, installation, ""); err != nil || !active {
		t.Fatalf("adopted schema not active: %t %v", active, err)
	}
	if state.apply != owner.apply || state.base.Binding.Authority.SchemaGeneration != 2 ||
		state.identity.RelationManifestDigest != install.ToRelationManifestDigest || state.wal != log {
		t.Fatal("activation did not update the shared live schema directory")
	}
	donor := rf3DynamicDonorCut{state: state}
	donorFence, err := donor.SnapshotAuthorizationFence()
	if err != nil || donorFence.Binding.SchemaGeneration != 2 || donorFence.RelationManifestDigest != install.ToRelationManifestDigest {
		t.Fatalf("adopted donor retained closed predecessor generation: %+v %v", donorFence, err)
	}
	cut, err := donor.SnapshotArtifactCut()
	if err != nil {
		t.Fatalf("adopted donor cannot pin activated schema: %v", err)
	}
	cutFence := cut.Fence()
	if err := cut.Close(); err != nil {
		t.Fatal(err)
	}
	if cutFence != donorFence {
		t.Fatal("adopted donor authorization and pinned schema disagree")
	}
	if err := activator.UnregisterDynamic(identity); !errors.Is(err, schemainstall.ErrConflict) {
		t.Fatalf("stale rollback removed activated generation: %v", err)
	}
	if err := activator.UnregisterDynamic(state.identity); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.apply.CapacityQualificationProfile(); err != nil {
		t.Fatalf("metadata removal closed owner handles: %v", err)
	}
	if err := errors.Join(owner.apply.Close(), owner.db.Close()); err != nil {
		t.Fatal(err)
	}
	f.reopen(t)
	log, _ = f.store.GroupByID(identity.Group.GroupID)
	base, applyID, db, apply, err := openRF3RetainedApply(f.paths[0], log, f.bases[0], f.applies[0])
	if err != nil {
		t.Fatal(err)
	}
	owner.db, owner.apply = db, apply
	owner.identity = dynamicSchemaIdentity(t, apply, 1)
	if err := activator.RegisterDynamic(owner.identity, apply, spec, root, log); err != nil {
		t.Fatalf("register recovered successor from original reservation: %v", err)
	}
	state = activator.groups[identity.Group]
	if !state.base.Equal(base) || state.applyID != applyID || state.base.Binding.Authority.SchemaGeneration != 2 {
		t.Fatal("recovery registered the original reservation schema")
	}
}

func TestRF3SchemaRetirementRemovesOnlyExactReplica(t *testing.T) {
	identity := raftmember.RuntimeIdentity{Group: raftmember.GroupKey{GroupID: [16]byte{1}},
		MemberID: 2, StoreID: [16]byte{3}, NodeIncarnation: 4, RelationManifestDigest: [32]byte{5}}
	for _, dynamic := range []bool{false, true} {
		t.Run(fmt.Sprintf("dynamic=%t", dynamic), func(t *testing.T) {
			state := &rf3SchemaGeneration{identity: identity}
			state.identity.RelationManifestDigest = [32]byte{6}
			if dynamic {
				state.preparation = new(nodecontrol.PreparationSpec)
			}
			activator := &rf3SchemaActivator{groups: map[raftmember.GroupKey]*rf3SchemaGeneration{identity.Group: state}}
			for _, field := range []string{"member", "store", "incarnation"} {
				stale := identity
				switch field {
				case "member":
					stale.MemberID++
				case "store":
					stale.StoreID[0]++
				case "incarnation":
					stale.NodeIncarnation++
				}
				if removed, err := activator.RemoveRetired(stale); removed || !errors.Is(err, schemainstall.ErrConflict) || activator.groups[identity.Group] != state {
					t.Fatalf("retirement for another %s removed live replica: %v", field, err)
				}
			}
			if removed, err := activator.RemoveRetired(identity); err != nil || !removed || len(activator.groups) != 0 {
				t.Fatalf("schema successor retained after exact replica retirement: %v", err)
			}
			if removed, err := activator.RemoveRetired(identity); err != nil || removed {
				t.Fatalf("retirement replay: %v", err)
			}
		})
	}
}
