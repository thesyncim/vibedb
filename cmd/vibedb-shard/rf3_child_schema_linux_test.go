//go:build linux

package main

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// Mandatory Linux test: creates real sealed SQL/apply source bundles, allocates
// exact destination identities, and exercises the shipped child SQL preparer.
func TestRF3ChildSQLProvisionsExactSingletonLocalGlobalBundles(t *testing.T) {
	for _, kind := range []string{"singleton", "local", "base-local-global"} {
		t.Run(kind, func(t *testing.T) {
			options := rf3testfixture.DurableGatewayMemberProfiles()[rf3testfixture.DurableGatewayDataAGroup]
			if kind == "singleton" {
				options.SchemaStatements, options.GlobalIndexes = nil, nil
			}
			if kind == "local" {
				options.SchemaStatements, options.GlobalIndexes = options.SchemaStatements[:1], nil
			}
			options.Root = t.TempDir()
			options.Identity = raftstore.Identity{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
				ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}, Distribution: "orders", Shard: "source",
				AllocationGeneration: 1, MemberID: 1, StoreID: [16]byte{5}}
			options.Bootstrap = rf3testfixture.InitialBootstrap([]uint64{1, 2, 3})
			options.WAL, options.Key = rf3testfixture.DurableGatewayWALOptions(), raftstore.Key{ID: "child-test-key", Wrapped: []byte("wrapped")}
			options.Key.Material[0] = 9
			prepared, err := rf3testfixture.PrepareMember(options)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "base-local-global" {
				description, err := sqldriver.DescribeReplicatedSchemaCatalog(prepared.SQLPath)
				if err != nil {
					t.Fatal(err)
				}
				registry, err := refreshRF3SplitChildSchema(rf3ManifestSplitChildRegistry{
					Table: options.Table, CreateTable: options.CreateTable,
					SchemaStatements: options.SchemaStatements, GlobalIndexes: options.GlobalIndexes,
				}, description)
				if err != nil {
					t.Fatalf("refresh multi-relation child schema: %v", err)
				}
				if !slices.Contains(registry.SchemaStatements, options.SchemaStatements[1]) {
					t.Fatal("refresh discarded colocated global-index table DDL")
				}
			}
			source := prepared.Base
			if err := prepared.Close(); err != nil {
				t.Fatal(err)
			}
			binding := source.Binding
			binding.Shard, binding.GroupID, binding.ShardIncarnation = "child", [16]byte{11}, [16]byte{12}
			binding.AllocationGeneration, binding.StoreID = 2, [16]byte{13}
			local := sqldriver.ShardStoreIdentity{Distribution: "orders", Shard: "child", AllocationGeneration: 2, LogID: [16]byte{14}}
			storages := make([]string, source.RelationCount)
			for i := range storages {
				storages[i] = strings.Repeat(fmt.Sprintf("%02x", 32+i), 32)
			}
			base, err := sqldriver.NewReplicatedChildShardStoreBundleIdentity(local, binding, source, storages)
			if err != nil {
				t.Fatal(err)
			}
			applyOptions := options.Apply
			applyOptions.Placement.Range.Start = distribution.KeyspacePoint{0x80}
			apply, err := sqldriver.NewReplicatedChildApplyIdentity(base, strings.Repeat("41", 32), strings.Repeat("42", 32), applyOptions)
			if err != nil {
				t.Fatal(err)
			}
			target := splitcontroller.ChildReplicaTarget{SQL: base, Apply: apply}
			template := rf3ManifestSplitChildRegistry{Table: options.Table, CreateTable: options.CreateTable,
				SchemaStatements: options.SchemaStatements, GlobalIndexes: options.GlobalIndexes}
			for _, partial := range []string{"fresh", "base-ddl", "bound-before-reservation"} {
				path := filepath.Join(t.TempDir(), "child.vdb")
				if partial != "fresh" {
					// Simulate a process dying after base DDL, before any index/global
					// DDL. Resume must authenticate and finish this exact schema.
					db, err := sqldriver.InitializeShardStoreIdentity(path, local)
					if err != nil {
						t.Fatal(err)
					}
					session, err := db.NewSession(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					statements := []string{options.CreateTable}
					if partial == "bound-before-reservation" {
						statements = append(statements, options.SchemaStatements...)
					}
					for _, ddl := range statements {
						statement, err := session.Prepare(context.Background(), ddl)
						if err != nil {
							t.Fatal(err)
						}
						if _, err = statement.Exec(context.Background(), nil); err != nil {
							t.Fatal(err)
						}
						if err = statement.Close(); err != nil {
							t.Fatal(err)
						}
					}
					if err = session.Close(); err != nil {
						t.Fatal(err)
					}
					if partial == "bound-before-reservation" {
						if got, err := db.BindReplicatedShardStoreBundleIdentity(base, options.GlobalIndexes); err != nil || !got.Equal(base) {
							t.Fatalf("interrupted bound child: %v", err)
						}
					}
					if err = db.Close(); err != nil {
						t.Fatal(err)
					}
				}
				for attempt := 0; attempt < 2; attempt++ {
					if err := prepareRF3ChildSQL(context.Background(), template, path, target); err != nil {
						t.Fatalf("partial=%v attempt=%d: %v", partial, attempt, err)
					}
					if err := verifyRF3PreparedChildSQL(path, target); err != nil {
						t.Fatal(err)
					}
				}
				foreign := template
				foreign.CreateTable = "CREATE TABLE orders_a (PRIMARY KEY (foreign))"
				if err := prepareRF3ChildSQL(context.Background(), foreign, path, target); err == nil {
					t.Fatal("exact existing child bypassed foreign schema rejection")
				}
			}
		})
	}
}
