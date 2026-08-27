package splitcontroller

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/splitcapture"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestReplicatedSplitAuthenticatesDistinctSourceChildAndLocalSchemas(t *testing.T) {
	plan, catalog, source := testReplicatedProjectionPlan(t)
	target := plan.targets[1]
	if plan.relationDigest != source.Command.RelationManifestDigest || target.RelationManifestDigest == plan.relationDigest ||
		target.SQL.RelationManifestDigest == target.RelationManifestDigest {
		t.Fatal("source artifact, child serving, or replica-local identity domains were conflated")
	}
	for name, mutate := range map[string]func(*Plan){
		"source group":             func(p *Plan) { p.sourceAuthority.Group.GroupID[0]++ },
		"source schema generation": func(p *Plan) { p.sourceAuthority.Command.SchemaGeneration++ },
		"source machine digest":    func(p *Plan) { p.sourceAuthority.Command.RelationManifestDigest[0]++ },
		"source immutable range":   func(p *Plan) { p.sourceAuthority.Schema.Placement.Range.Start[0] = 1 },
		"source local identity":    func(p *Plan) { p.sourceAuthority.Schema.SQL.RelationManifestDigest[0]++ },
	} {
		t.Run(name, func(t *testing.T) {
			copy := *plan
			authority := *plan.sourceAuthority
			authority.Schema = clonePlanSourceSchema(authority.Schema)
			copy.sourceAuthority = &authority
			mutate(&copy)
			if copy.validateReplicatedSourceSchema() == nil {
				t.Fatal("foreign source schema accepted")
			}
		})
	}
	for name, mutate := range map[string]func(*ChildTarget){
		"copied source digest": func(c *ChildTarget) { c.RelationManifestDigest = plan.relationDigest },
		"replica local digest": func(c *ChildTarget) { c.RelationManifestDigest = c.SQL.RelationManifestDigest },
		"wrong placement":      func(c *ChildTarget) { c.Replicas[1].Apply.Placement.ShardKey = "/other" },
		"wrong schema":         func(c *ChildTarget) { c.Replicas[1].SQL.Binding.Authority.SchemaGeneration++ },
	} {
		t.Run(name, func(t *testing.T) {
			copy := cloneChildTarget(target)
			mutate(&copy)
			if plan.validateReplicatedChildSchema(copy) == nil {
				t.Fatal("foreign child schema accepted")
			}
		})
	}
	raw, err := AppendPlanIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenPlanIntent(raw, catalog)
	if err != nil || !samePlanSourceAuthority(*plan.sourceAuthority, *recovered.sourceAuthority) {
		t.Fatalf("source authority lost on restart: %v", err)
	}
	again, err := AppendPlanIntent(nil, catalog, recovered)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatalf("noncanonical replay: %v", err)
	}
}

func TestReplicatedSplitUsesAppliedSourceFenceNotCatalogCAS(t *testing.T) {
	plan, catalog, _ := testReplicatedProjectionPlanWithFence(t, 10, 17)
	state := testSourceState(plan)
	state.Binding.RoutingVersion, state.Binding.RouteGeneration = 10, 17
	if plan.current != 19 || plan.source.RoutingVersion != 11 || !plan.sourceBindingInitial(state.Binding) {
		t.Fatal("unchanged source incorrectly required unrelated catalog publication")
	}
	if err := plan.validateSourceObservation(Observation{Catalog: catalog, SourceState: state}); err != nil {
		t.Fatal(err)
	}
	raw, err := plan.AppendSourceCaptureActivation(nil, state)
	if err != nil {
		t.Fatal(err)
	}
	command, err := splitcapture.OpenCommand(raw)
	if err != nil || command.SourceGeneration != 17 || command.RelationManifestDigest != plan.relationDigest {
		t.Fatalf("capture used catalog CAS instead of applied fence: %+v %v", command, err)
	}
	for _, mutate := range []func(*replicatedstate.Binding){
		func(b *replicatedstate.Binding) { b.RoutingVersion = 11 },
		func(b *replicatedstate.Binding) { b.RouteGeneration = 19 },
		func(b *replicatedstate.Binding) { b.GroupID[0]++ },
		func(b *replicatedstate.Binding) { b.SchemaGeneration++ },
		func(b *replicatedstate.Binding) { b.ProtectionEpoch++ },
	} {
		copy := state
		mutate(&copy.Binding)
		if plan.sourceBindingInitial(copy.Binding) {
			t.Fatal("wrong source fence accepted")
		}
		if _, err := plan.AppendSourceCaptureActivation(nil, copy); err == nil {
			t.Fatal("wrong source activated capture")
		}
	}
	descriptors, err := plan.projectReplicatedSplitDescriptors(catalog, [32]byte{9})
	if err != nil {
		t.Fatal(err)
	}
	next, err := gateway.BuildManifestTransitionsWithReplicatedMetadata(catalog, []*distribution.Manifest{plan.targetManifest}, plan.next, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := AppendPlanIntent(nil, catalog, plan)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenPlanIntent(intent, next)
	if err != nil || !samePlanSourceAuthority(*plan.sourceAuthority, *recovered.sourceAuthority) {
		t.Fatalf("publication lost exact original source fence: %v", err)
	}
}

func TestOperationNamespacePrecedesPreparedRuntimeIdentities(t *testing.T) {
	plan, catalog, _, split := testPlan(t)
	id, err := OperationIDForSplit(catalog.Generation(), split, plan.partitioner)
	if err != nil || id != plan.OperationID() {
		t.Fatalf("preallocated operation differs: %x %v", id, err)
	}
	next, err := OperationIDForSplit(catalog.Generation()+1, split, plan.partitioner)
	if err != nil || next == id {
		t.Fatalf("different catalog CAS reused operation: %v", err)
	}
	for _, generation := range []uint64{0, ^uint64(0)} {
		if _, err := OperationIDForSplit(generation, split, plan.partitioner); err == nil {
			t.Fatal("invalid CAS accepted")
		}
	}
}

func bindProjectionSourceAndChildSchemas(t testing.TB, source *gateway.ReplicatedShardDescriptor, target *ChildTarget) PlanSourceSchema {
	t.Helper()
	group, command := source.Group, source.Command
	binding := sqldriver.ReplicatedShardStoreBinding{ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch, Distribution: string(source.Distribution), Shard: string(source.Shard),
		AllocationGeneration: uint64(source.AllocationGeneration), ShardIncarnation: group.ShardIncarnation, GroupID: group.GroupID,
		MemberID: source.Replicas[0].Member, StoreID: source.Replicas[0].StoreID,
		Authority: sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: command.ActivePolicyGeneration, ProtectionEpoch: command.ProtectionEpoch,
			OwnershipEpoch: command.OwnershipEpoch, SchemaGeneration: command.SchemaGeneration, RoutingVersion: command.RoutingVersion, RouteGeneration: command.RouteGeneration}}
	base, err := sqldriver.NewReplicatedChildShardStoreIdentity(sqldriver.ShardStoreIdentity{Distribution: source.Distribution,
		Shard: source.Shard, AllocationGeneration: source.AllocationGeneration, LogID: testID(99)}, binding, "docs", fmt.Sprintf("%064x", 11), "/tenant",
		sqldriver.ReplicatedShardStoreLimits{MaxKeyBytes: 256, MaxDocumentBytes: 1 << 20, MaxBatchDocuments: 64, MaxBatchBytes: 16<<20 + 64*256})
	if err != nil {
		t.Fatal(err)
	}
	schema := PlanSourceSchema{SQL: base, Placement: sqldriver.ReplicatedPlacementProfile{Format: sqldriver.ReplicatedPlacementProfileFormat,
		ShardKey: "/tenant", TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
		Range: distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}}}}
	source.Command.RelationManifestDigest, err = sqldriver.ReplicatedSchemaManifest(schema.SQL, schema.Placement, nil)
	if err != nil {
		t.Fatal(err)
	}
	source.LogicalSchemaDigest, err = sqldriver.ReplicatedRelationManifestDigest(schema.SQL)
	if err != nil {
		t.Fatal(err)
	}
	for i := range target.Replicas {
		replica := &target.Replicas[i]
		local := sqldriver.ShardStoreIdentity{Distribution: distribution.DistributionName(replica.WAL.Distribution), Shard: distribution.ShardID(replica.WAL.Shard),
			AllocationGeneration: distribution.ShardAllocationGeneration(replica.WAL.AllocationGeneration), LogID: replica.SQL.LogID}
		replica.SQL, err = sqldriver.NewReplicatedChildShardStoreBundleIdentity(local, replica.SQL.Binding, schema.SQL, []string{fmt.Sprintf("%064x", 20+i)})
		if err != nil {
			t.Fatal(err)
		}
		placement := schema.Placement
		placement.Range = replica.Apply.Placement.Range
		replica.Apply, err = sqldriver.NewReplicatedChildApplyIdentity(replica.SQL, fmt.Sprintf("%064x", 30+i), fmt.Sprintf("%064x", 40+i),
			sqldriver.ReplicatedApplyOptions{MaxSessions: 32, RetryWindow: 8, TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20}, Placement: placement})
		if err != nil {
			t.Fatal(err)
		}
		digest, err := sqldriver.ReplicatedSchemaManifest(replica.SQL, placement, nil)
		if err != nil {
			t.Fatal(err)
		}
		if i > 0 && target.RelationManifestDigest != digest {
			t.Fatal("replica-local stores changed portable serving schema")
		}
		target.RelationManifestDigest = digest
	}
	target.SQL = target.Replicas[0].SQL.Clone()
	return schema
}
