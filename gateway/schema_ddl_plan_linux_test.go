//go:build linux

package gateway

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

func schemaDDLPlanFixture(t *testing.T, sql string, indexed bool) (*Snapshot, []SchemaDDLReplicaBuild, [2]string) {
	t.Helper()
	fixture, _, keys := replicatedSQLSplitTransactionFixture(t)
	descriptors := fixture.ReplicatedShardDescriptors()
	profile := fixture.replicatedTableProfiles()[0]
	create := "CREATE TABLE messages (id TEXT PRIMARY KEY, name TEXT NOT NULL, city TEXT)"
	if strings.Contains(sql, "IF NOT EXISTS department") {
		create = "CREATE TABLE messages (id TEXT PRIMARY KEY, name TEXT NOT NULL, city TEXT, department TEXT)"
	}
	var builds []SchemaDDLReplicaBuild
	for i := range descriptors {
		d := &descriptors[i]
		manifest, _ := fixture.Manifest(d.Distribution)
		_, shard := manifestShardOrdinal(manifest, d.Shard)
		for j := range d.Replicas {
			r := &d.Replicas[j]
			r.StoreID = [16]byte{byte(1 + i*ServingReplicaCount + j)}
			db, err := sqldriver.InitializeShardStore(filepath.Join(t.TempDir(), "schema.vdb"), sqldriver.ShardStoreBinding{
				Distribution: d.Distribution, Shard: d.Shard, AllocationGeneration: d.AllocationGeneration})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			s, err := db.NewSession(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			statements := []string{create}
			if indexed {
				statements = append(statements, "CREATE INDEX by_city ON messages (city)")
			}
			for _, ddl := range statements {
				statement, err := s.Prepare(t.Context(), ddl)
				if err != nil {
					t.Fatal(err)
				}
				_, err = statement.Exec(t.Context(), nil)
				if err = errors.Join(err, statement.Close()); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			c := d.Command
			binding := sqldriver.ReplicatedShardStoreBinding{ClusterID: d.Group.ClusterID, ClusterIncarnation: d.Group.ClusterIncarnation,
				TopologyRecoveryEpoch: d.Group.TopologyRecoveryEpoch, Distribution: string(d.Distribution), Shard: string(d.Shard),
				AllocationGeneration: uint64(d.AllocationGeneration), ShardIncarnation: d.Group.ShardIncarnation, GroupID: d.Group.GroupID, MemberID: r.Member, StoreID: r.StoreID,
				Authority: sqldriver.ReplicatedAuthorityProfile{ActivePolicyGeneration: c.ActivePolicyGeneration, ProtectionEpoch: c.ProtectionEpoch,
					OwnershipEpoch: c.OwnershipEpoch, SchemaGeneration: c.SchemaGeneration, RoutingVersion: c.RoutingVersion, RouteGeneration: c.RouteGeneration}}
			base, err := db.BindReplicatedShardStore(binding, "messages")
			if err != nil {
				t.Fatal(err)
			}
			index, term := uint64(1), uint64(1)
			bootstrap := &pb.Snapshot{Data: []byte("schema-plan-fixture"), Metadata: &pb.SnapshotMetadata{Index: &index, Term: &term, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}}}}
			apply, _, err := db.OpenReplicatedApply(base, bootstrap, sqldriver.ReplicatedApplyOptions{MaxSessions: 96, RetryWindow: 8,
				TxnLimits: durable.TxnLimits{MaxCollections: 16, MaxDocuments: 2048, MaxBytes: 256 << 20},
				Placement: sqldriver.ReplicatedPlacementProfile{Format: sqldriver.ReplicatedPlacementProfileFormat, ShardKey: "/id", TupleVersion: distribution.CurrentTupleVersion,
					MapperVersion: distribution.NativeMapperVersion, Range: shard.Range}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = apply.Close() })
			if _, err := apply.InstallSnapshot(bootstrap); err != nil {
				t.Fatal(err)
			}
			machine, err := apply.RangeSplitRelationManifestDigest()
			if err != nil {
				t.Fatal(err)
			}
			logical, err := sqldriver.ReplicatedRelationManifestDigest(base)
			if err != nil {
				t.Fatal(err)
			}
			d.Command.RelationManifestDigest, d.LogicalSchemaDigest = machine, logical
			profile.LogicalSchemaDigest = logical
			profile.MaxKeyBytes, profile.MaxDocumentBytes = uint16(base.UserLimits.MaxKeyBytes), uint32(base.UserLimits.MaxDocumentBytes)
			request := schemainstall.BuildRequest{Operation: [32]byte{9}, Group: d.Group, AllocationGeneration: d.AllocationGeneration,
				FromSchemaGeneration: d.Command.SchemaGeneration, FromRelationManifestDigest: machine, SourceApplied: 1, SQLBytes: uint64(len(sql)), SQLDigest: sha256.Sum256([]byte(sql))}
			target, err := apply.BuildReplicatedSchemaDDLTarget(t.Context(), 1, sql)
			if err != nil {
				t.Fatal(err)
			}
			builds = append(builds, SchemaDDLReplicaBuild{Node: r.Node, Member: r.Member, Request: request, Target: target})
		}
	}
	var indexes []IndexDescriptor
	if indexed {
		indexes = []IndexDescriptor{{IndexID: 7, Incarnation: 3, Table: "messages", Name: "by_city", Paths: []string{"/city"}, Flags: IndexLocal, Lifecycle: IndexReady}}
	}
	// Retain an unrelated ledger distribution, including its durable request
	// range identity, so this is a persistable catalog rather than a fragment.
	ledger, _ := testRequestLedgerCatalogSnapshot(t, fixture.Generation())
	config := cloneConfig(fixture.config)
	ledgerConfig := cloneConfig(ledger.config)
	ledgerConfig.Placements[0].Table = "ledger_control"
	config.Distributions = append(config.Distributions, ledgerConfig.Distributions...)
	config.Placements = append(config.Placements, ledgerConfig.Placements...)
	config.Manifests = append(config.Manifests, ledgerConfig.Manifests...)
	endpoints := cloneEndpoints(fixture.endpoints)
	for key, address := range ledger.endpoints {
		endpoints[key] = address
	}
	ledgerDescriptors := ledger.replicatedDescriptors()
	ledgerDescriptors[0].Group.GroupID[0] ^= 0x80
	ledgerDescriptors[0].Group.ShardIncarnation[0] ^= 0x80
	descriptors = append(descriptors, ledgerDescriptors...)
	base, err := NewSnapshotWithReplicatedTableMetadata(config, endpoints, fixture.Generation(), indexes, nil, descriptors, []ReplicatedTableProfile{profile},
		[]ReplicatedTableDeclaration{{Table: "messages", CreateTable: create}})
	if err != nil {
		t.Fatal(err)
	}
	return base, builds, keys
}

func TestSchemaDDLPlanBuildsCompleteDistributedCatalog(t *testing.T) {
	for _, test := range []struct {
		sql     string
		indexed bool
		indexes int
		noOp    bool
	}{
		{"CREATE INDEX by_city ON messages (city)", false, 1, false},
		{"CREATE INDEX by_name_city ON messages (name, city)", true, 2, false},
		{"DROP INDEX by_city", true, 0, false},
		{"TRUNCATE messages", true, 1, false},
		{"ALTER TABLE messages ADD COLUMN department TEXT", false, 0, false},
		{"ALTER TABLE messages ADD COLUMN IF NOT EXISTS department TEXT", false, 0, true},
		{"CREATE INDEX IF NOT EXISTS by_city ON messages (city)", true, 1, true},
	} {
		t.Run(test.sql, func(t *testing.T) {
			base, builds, keys := schemaDDLPlanFixture(t, test.sql, test.indexed)
			before, err := schemaRolloutCatalogDocument(base)
			if err != nil {
				t.Fatal(err)
			}
			target, plans, err := BuildReplicatedSchemaDDLPlan(base, [32]byte{9}, "messages", test.sql, builds)
			if err != nil {
				t.Fatal(err)
			}
			if len(target.indexDescriptors()) != test.indexes {
				t.Fatal("incorrect target indexes")
			}
			if test.noOp {
				if target != base || len(plans) != 0 {
					t.Fatal("no-op changed catalog")
				}
				return
			}
			declarationsChanged := strings.HasPrefix(test.sql, "ALTER TABLE")
			if len(plans) != 6 || target.Generation() != base.Generation()+1 ||
				(reflect.DeepEqual(target.ReplicatedTableDeclarations(), base.ReplicatedTableDeclarations()) == declarationsChanged) {
				t.Fatal("incomplete plan or lost declarations")
			}
			if declarationsChanged {
				info, ok := target.declaredTableInfo("messages")
				if !ok || len(info.Columns) != 4 || info.Columns[3].Path != "/department" {
					t.Fatalf("ALTER declaration = %+v", info)
				}
			}
			beforeLedger, afterLedger := base.replicatedDescriptors()[2], target.replicatedDescriptors()[2]
			if !reflect.DeepEqual(beforeLedger, afterLedger) || !reflect.DeepEqual(base.config, target.config) || !reflect.DeepEqual(base.endpoints, target.endpoints) {
				t.Fatal("DDL changed unrelated routing or durable ledger identity")
			}
			if test.indexed {
				for _, index := range target.indexDescriptors() {
					if index.Name == "by_city" && (index.IndexID != 7 || index.Incarnation != 4) {
						t.Fatal("rebuilt index lost its identity fence")
					}
				}
			}
			for i, key := range keys {
				encoded, _ := orderedkey.AppendString(nil, []byte(key), orderedkey.Ascending)
				var scratch [replication.MaxMutationKeyBytes + 16]byte
				var replicas [ServingReplicaCount]ReplicatedEndpoint
				resolved, ok := target.ResolveReplicatedTableKey([]byte("messages"), encoded, scratch[:0], replicas[:0])
				if !ok || resolved.Route.Group != builds[i*ServingReplicaCount].Request.Group || resolved.Profile.SchemaGeneration != base.replicatedTableProfiles()[0].SchemaGeneration+1 {
					t.Fatal("DDL changed range routing or lost new schema fence")
				}
			}
			// Arrival order is not operation identity. Parallel responses must
			// produce the same catalog and replica plan on recovery.
			slices.Reverse(builds)
			again, againPlans, err := BuildReplicatedSchemaDDLPlan(base, [32]byte{9}, "messages", test.sql, builds)
			after, _ := schemaRolloutCatalogDocument(target)
			againRaw, _ := schemaRolloutCatalogDocument(again)
			if err != nil || !bytes.Equal(after, againRaw) || !reflect.DeepEqual(plans, againPlans) {
				t.Fatalf("unstable plan: %v", err)
			}
			unchanged, _ := schemaRolloutCatalogDocument(base)
			if !bytes.Equal(before, unchanged) {
				t.Fatal("mutated serving catalog")
			}
		})
	}
}

func TestSchemaDDLPlanRejectsIncompleteOrSubstitutedReceipts(t *testing.T) {
	const sql = "CREATE INDEX by_city ON messages (city)"
	base, builds, _ := schemaDDLPlanFixture(t, sql, false)
	for _, mutate := range []func([]SchemaDDLReplicaBuild) []SchemaDDLReplicaBuild{
		func(b []SchemaDDLReplicaBuild) []SchemaDDLReplicaBuild { return b[:len(b)-1] },
		func(b []SchemaDDLReplicaBuild) []SchemaDDLReplicaBuild { b[1] = b[0]; return b },
		func(b []SchemaDDLReplicaBuild) []SchemaDDLReplicaBuild { b[0].Request.Operation[0]++; return b },
		func(b []SchemaDDLReplicaBuild) []SchemaDDLReplicaBuild {
			b[0].Request.FromRelationManifestDigest[0]++
			return b
		},
		func(b []SchemaDDLReplicaBuild) []SchemaDDLReplicaBuild { b[0].Request.SQLDigest[0]++; return b },
		func(b []SchemaDDLReplicaBuild) []SchemaDDLReplicaBuild { b[0].Target = b[1].Target; return b },
		func(b []SchemaDDLReplicaBuild) []SchemaDDLReplicaBuild { b[0].Target.NoOp = true; return b },
	} {
		if _, _, err := BuildReplicatedSchemaDDLPlan(base, [32]byte{9}, "messages", sql, mutate(slices.Clone(builds))); err == nil {
			t.Fatal("invalid build receipt accepted")
		}
	}
}

func TestSchemaDDLPlanReconcilesAppliedDescriptorsWithMissingPortableMetadata(t *testing.T) {
	const sql = "CREATE INDEX by_city ON messages (city)"
	base, builds, _ := schemaDDLPlanFixture(t, sql, false)
	applied, _, err := BuildReplicatedSchemaDDLPlan(base, [32]byte{9}, "messages", sql, builds)
	if err != nil {
		t.Fatal(err)
	}
	partial, err := NewSnapshotWithReplicatedTableMetadata(
		cloneConfig(base.config), cloneEndpoints(base.endpoints), applied.Generation(),
		base.indexDescriptors(), base.statistics.Descriptors(), applied.replicatedDescriptors(),
		applied.replicatedTableProfiles(), base.ReplicatedTableDeclarations(),
	)
	if err != nil {
		t.Fatal(err)
	}
	reconciled, plans, matched, err := ReconcileAppliedReplicatedSchemaDDLCatalog(
		partial, [32]byte{9}, "messages", sql, builds,
	)
	if err != nil || !matched || len(plans) != len(builds) || reconciled.Generation() != partial.Generation()+1 ||
		len(reconciled.indexDescriptors()) != 1 || reconciled.indexDescriptors()[0].Name != "by_city" ||
		!reflect.DeepEqual(reconciled.replicatedDescriptors(), partial.replicatedDescriptors()) ||
		!reflect.DeepEqual(reconciled.replicatedTableProfiles(), partial.replicatedTableProfiles()) {
		t.Fatalf("reconcile plans=%d matched=%t err=%v indexes=%+v", len(plans), matched, err, reconciled.indexDescriptors())
	}
	for _, plan := range plans {
		if plan.Request.Operation != ([32]byte{9}) ||
			plan.Request.FromSchemaGeneration+1 != plan.Request.ToSchemaGeneration ||
			plan.Request.ToRelationManifestDigest == ([32]byte{}) ||
			len(plan.Bundle) == 0 || uint64(len(plan.Bundle)) != plan.Request.BundleBytes {
			t.Fatalf("recovery plan does not retain the exact source/target cut: %+v", plan)
		}
	}
	if _, _, matched, err := ReconcileAppliedReplicatedSchemaDDLCatalog(base, [32]byte{9}, "messages", sql, builds); err != nil || matched {
		t.Fatalf("ordinary source rollout treated as repair: matched=%t err=%v", matched, err)
	}
	already, plans, matched, err := ReconcileAppliedReplicatedSchemaDDLCatalog(
		reconciled, [32]byte{9}, "messages", sql, builds,
	)
	if err != nil || !matched || already != reconciled || len(plans) != len(builds) {
		t.Fatalf("exact recovered request was not idempotent: plans=%d matched=%t same=%t err=%v", len(plans), matched, already == reconciled, err)
	}
	tables := replicatedTableInfos(reconciled, reconciled.ReplicatedTableProfiles())
	if len(tables) != 1 || len(tables[0].Indexes) != 1 || tables[0].Indexes[0].Name != "by_city" ||
		!reflect.DeepEqual(tables[0].Indexes[0].Paths, []string{"/city"}) {
		t.Fatalf("PostgreSQL discovery lost recovered index: %+v", tables)
	}
}
