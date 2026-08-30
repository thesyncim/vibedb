package gateway

import (
	"errors"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

func TestDistributedSchemaDDLRejectsUniqueIndexAtEveryAdmissionBoundary(t *testing.T) {
	const text = `CREATE UNIQUE INDEX by_email ON messages (email)`
	snapshot := testSnapshot(t, 7)
	operation := [32]byte{1}

	checks := []struct {
		name string
		call func() error
	}{
		{"resolve", func() error {
			_, err := ResolveReplicatedSchemaDDLTable(snapshot, text)
			return err
		}},
		{"plan", func() error {
			_, _, err := BuildReplicatedSchemaDDLPlan(snapshot, operation, "messages", text, nil)
			return err
		}},
		{"reconcile", func() error {
			_, _, err := ReconcileAppliedReplicatedSchemaDDLCatalog(snapshot, operation, "messages", text, nil)
			return err
		}},
		{"index plan", func() error {
			_, _, err := schemaDDLPlanIndexes(snapshot, "messages", text)
			return err
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			var unsupported *sqlast.FeatureNotSupportedError
			if err := check.call(); !errors.As(err, &unsupported) {
				t.Fatalf("error = %T %v, want *sql.FeatureNotSupportedError", err, err)
			}
		})
	}
}

func TestReplicatedChildSchemaRejectsLocalUniqueIndexButKeepsGlobalUniqueDescriptor(t *testing.T) {
	const createTable = `CREATE TABLE messages (PRIMARY KEY (tenant_id))`
	if err := sqldriver.ValidateReplicatedChildSchemaDefinition(
		"messages", "/tenant_id", createTable,
		[]string{`CREATE UNIQUE INDEX by_email ON messages (email)`}, nil,
	); !errors.Is(err, sqldriver.ErrReplicatedShardStoreProfile) {
		t.Fatalf("local unique child index error = %v, want ErrReplicatedShardStoreProfile", err)
	}
	if err := sqldriver.ValidateReplicatedChildSchemaDefinition(
		"messages", "/tenant_id", createTable,
		[]string{`CREATE INDEX by_email ON messages (email)`}, nil,
	); err != nil {
		t.Fatalf("ordinary local child index rejected: %v", err)
	}

	config, endpoints := globalIndexCatalog(t)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 8, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		t.Fatal(err)
	}
	tables := replicatedTableInfos(snapshot, []ReplicatedTableProfile{{
		Table: "messages", PrimaryKey: "/tenant_id",
	}})
	if len(tables) != 1 || len(tables[0].Indexes) != 1 ||
		tables[0].Indexes[0].Name != "by_email" || !tables[0].Indexes[0].Unique {
		t.Fatalf("global unique introspection = %+v", tables)
	}
}
