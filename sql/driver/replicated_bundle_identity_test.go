package driver

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

func TestReplicatedGlobalIndexRelationCanonicalProvisioningJSON(t *testing.T) {
	want := ReplicatedGlobalIndexRelation{
		Relation: 2, Table: "emails", IndexID: 41, Incarnation: 7, LocatorCount: 1,
		Unique: true, KeyEncoding: ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
		TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
		BucketBits: distribution.DefaultVirtualBucketBits,
	}
	encoded, err := vibejson.Marshal(&want)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"relation", "table", "index_id", "incarnation", "locator_count", "unique", "key_encoding", "key_arity", "tuple_version", "mapper_version", "bucket_bits"} {
		if !bytes.Contains(encoded, []byte(`"`+name+`":`)) {
			t.Fatalf("missing canonical field %s: %s", name, encoded)
		}
	}
	var got ReplicatedGlobalIndexRelation
	if err := vibejson.Unmarshal(encoded, &got); err != nil || got != want {
		t.Fatalf("provisioning roundtrip = %+v, %v", got, err)
	}
}

func prepareReservedReplicatedBundle(t *testing.T) (string, *Database, ReplicatedShardStoreIdentity, []ReplicatedGlobalIndexRelation) {
	t.Helper()
	path, db, binding, local := prepareReplicatedTestRoot(t, "reserved-bundle", false)
	t.Cleanup(func() { _ = db.Close() })
	session, err := db.NewSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`CREATE INDEX by_email ON docs (email)`,
		`CREATE TABLE emails (PRIMARY KEY (key))`,
		`CREATE TABLE names (PRIMARY KEY (key))`,
	} {
		if err := testRuntimeExec(session, statement, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	globals := []ReplicatedGlobalIndexRelation{
		{Relation: 2, Table: "emails", IndexID: 41, Incarnation: 7, LocatorCount: 1, Unique: true,
			KeyEncoding: ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
			TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
			BucketBits: distribution.DefaultVirtualBucketBits},
		{Relation: 3, Table: "names", IndexID: 42, Incarnation: 8, LocatorCount: 2,
			KeyEncoding: ReplicatedRelationKeyCanonicalTuple, KeyArity: 2,
			TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
			BucketBits: distribution.DefaultVirtualBucketBits},
	}
	expected := ReplicatedShardStoreIdentity{
		Format: ReplicatedShardStoreFormat, Binding: binding, LogID: local.LogID,
		UserTable: "docs", UserPrimaryKey: "/id", UserStorage: strings.Repeat("a1", 32),
		Sidecars: canonicalReplicatedShardStoreSidecars(), RelationCount: 3,
		RelationSchemaGeneration: binding.Authority.SchemaGeneration,
		Relations:                make([]ReplicatedShardRelationIdentity, 3),
	}
	for i, name := range []string{"docs", "emails", "names"} {
		table := db.connector.db.tables[name]
		options, err := durable.NormalizeOptions(durableOptions(table))
		if err != nil {
			t.Fatal(err)
		}
		relation := ReplicatedShardRelationIdentity{
			Relation: uint16(i + 1), Table: name, Storage: strings.Repeat([]string{"a1", "b2", "c3"}[i], 32),
			Limits: ReplicatedShardStoreLimits{MaxKeyBytes: options.MaxKeyBytes, MaxDocumentBytes: options.MaxDocumentBytes,
				MaxBatchDocuments: options.MaxBatchDocuments, MaxBatchBytes: options.MaxBatchBytes},
		}
		if i == 0 {
			relation.Kind = ReplicatedShardRelationJSON
			relation.LocalIndexDigest = replicatedLocalIndexDigest(table.meta.Indexes)
			expected.UserLimits = relation.Limits
		} else {
			global := globals[i-1]
			relation.Kind = ReplicatedShardRelationGlobalIndex
			relation.IndexID, relation.Incarnation = global.IndexID, global.Incarnation
			relation.LocatorCount, relation.Unique = global.LocatorCount, global.Unique
			relation.KeyEncoding, relation.KeyArity = global.KeyEncoding, global.KeyArity
			relation.TupleVersion, relation.MapperVersion = global.TupleVersion, global.MapperVersion
			relation.BucketBits = global.BucketBits
		}
		expected.Relations[i] = relation
	}
	expected.RelationManifestDigest = replicatedRelationManifestDigest(expected)
	if err := validateReplicatedShardStoreIdentity(expected); err != nil {
		t.Fatalf("fixture identity: %v", err)
	}
	return path, db, expected, globals
}

func TestBindReplicatedShardStoreBundleIdentityExactRetryAndReopen(t *testing.T) {
	path, db, expected, globals := prepareReservedReplicatedBundle(t)
	got, err := db.BindReplicatedShardStoreBundleIdentity(expected, globals)
	skipReplicatedStrictAllocationUnsupported(t, db, got, err)
	if err != nil || !got.Equal(expected) {
		t.Fatalf("exact bind = %+v, %v", got, err)
	}
	// Returned storage descriptors must not alias the retained caller identity.
	got.Relations[1].Storage = "changed"
	got, err = db.BindReplicatedShardStoreBundleIdentity(expected, globals)
	if err != nil || !got.Equal(expected) {
		t.Fatalf("exact retry = %+v, %v", got, err)
	}
	foreign := ownedReplicatedShardStoreIdentity(expected)
	foreign.Relations[2].Storage = strings.Repeat("d4", 32)
	foreign.RelationManifestDigest = replicatedRelationManifestDigest(foreign)
	if _, err := db.BindReplicatedShardStoreBundleIdentity(foreign, globals); !errors.Is(err, ErrReplicatedShardStoreIdentityMismatch) {
		t.Fatalf("foreign retry = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStore(path, expected)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err = reopened.BindReplicatedShardStoreBundleIdentity(expected, globals)
	if err != nil || !got.Equal(expected) {
		t.Fatalf("reopened retry = %+v, %v", got, err)
	}
}

func TestBindReplicatedShardStoreBundleIdentityRejectsBeforeMaterialization(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ReplicatedShardStoreIdentity, []ReplicatedGlobalIndexRelation)
	}{
		{"foreign_log", func(i *ReplicatedShardStoreIdentity, _ []ReplicatedGlobalIndexRelation) { i.LogID[0] ^= 1 }},
		{"foreign_shard", func(i *ReplicatedShardStoreIdentity, _ []ReplicatedGlobalIndexRelation) {
			i.Binding.Shard += "-foreign"
		}},
		{"primary_key", func(i *ReplicatedShardStoreIdentity, _ []ReplicatedGlobalIndexRelation) { i.UserPrimaryKey = "/other" }},
		{"local_index", func(i *ReplicatedShardStoreIdentity, _ []ReplicatedGlobalIndexRelation) {
			i.Relations[0].LocalIndexDigest[0] ^= 1
		}},
		{"global_limits", func(i *ReplicatedShardStoreIdentity, _ []ReplicatedGlobalIndexRelation) {
			i.Relations[2].Limits.MaxDocumentBytes--
		}},
		{"base_limits", func(i *ReplicatedShardStoreIdentity, _ []ReplicatedGlobalIndexRelation) {
			i.UserLimits.MaxDocumentBytes--
			i.Relations[0].Limits = i.UserLimits
		}},
		{"duplicate_storage", func(i *ReplicatedShardStoreIdentity, _ []ReplicatedGlobalIndexRelation) {
			i.Relations[2].Storage = i.Relations[1].Storage
		}},
		{"global_index", func(i *ReplicatedShardStoreIdentity, _ []ReplicatedGlobalIndexRelation) { i.Relations[2].IndexID++ }},
		{"global_incarnation", func(i *ReplicatedShardStoreIdentity, _ []ReplicatedGlobalIndexRelation) { i.Relations[2].Incarnation++ }},
		{"global_locators", func(i *ReplicatedShardStoreIdentity, _ []ReplicatedGlobalIndexRelation) {
			i.Relations[2].LocatorCount++
		}},
		{"global_unique", func(i *ReplicatedShardStoreIdentity, _ []ReplicatedGlobalIndexRelation) { i.Relations[2].Unique = true }},
		{"global_arity", func(i *ReplicatedShardStoreIdentity, _ []ReplicatedGlobalIndexRelation) { i.Relations[2].KeyArity++ }},
		{"global_bucket_bits", func(i *ReplicatedShardStoreIdentity, _ []ReplicatedGlobalIndexRelation) { i.Relations[2].BucketBits-- }},
		{"global_spec", func(_ *ReplicatedShardStoreIdentity, g []ReplicatedGlobalIndexRelation) { g[1].IndexID++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path, db, expected, globals := prepareReservedReplicatedBundle(t)
			test.mutate(&expected, globals)
			expected.RelationManifestDigest = replicatedRelationManifestDigest(expected)
			if err := validateReplicatedShardStoreIdentity(expected); err != nil {
				t.Fatalf("test requires a structurally valid mismatched identity: %v", err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			_, err = db.BindReplicatedShardStoreBundleIdentity(expected, globals)
			if !errors.Is(err, ErrReplicatedShardStoreIdentityMismatch) {
				t.Fatalf("mismatched identity = %v", err)
			}
			assertReservedBundleUnchanged(t, db, path, before, nil)
		})
	}
}

func TestBindReplicatedShardStoreBundleIdentityRejectsReservedNamespaceCollision(t *testing.T) {
	for _, kind := range []string{"table", "journal", "catalog"} {
		t.Run(kind, func(t *testing.T) {
			path, db, expected, globals := prepareReservedReplicatedBundle(t)
			core := db.connector.db
			reserved := filepath.Join(core.dataDir, expected.Relations[2].Storage+".vjc")
			if kind == "catalog" {
				expected.Relations[2].Storage = core.tables["names"].meta.Storage
				expected.RelationManifestDigest = replicatedRelationManifestDigest(expected)
			} else {
				if kind == "journal" {
					reserved = durable.RecoveryJournalPath(reserved)
				}
				if err := os.WriteFile(reserved, []byte("foreign storage must survive"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(core.dataDir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.BindReplicatedShardStoreBundleIdentity(expected, globals); !errors.Is(err, ErrReplicatedShardStoreIdentityMismatch) {
				t.Fatalf("reserved namespace collision = %v", err)
			}
			assertReservedBundleUnchanged(t, db, path, before, entries)
			if kind != "catalog" {
				content, err := os.ReadFile(reserved)
				if err != nil || string(content) != "foreign storage must survive" {
					t.Fatalf("foreign file modified: %q, %v", content, err)
				}
			}
		})
	}
}

func TestBindReplicatedShardStoreBundleIdentityRejectsMalformedManifest(t *testing.T) {
	path, db, expected, globals := prepareReservedReplicatedBundle(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*ReplicatedShardStoreIdentity)
	}{
		{"format", func(i *ReplicatedShardStoreIdentity) { i.Format++ }},
		{"zero_log", func(i *ReplicatedShardStoreIdentity) { i.LogID = [16]byte{} }},
		{"zero_group", func(i *ReplicatedShardStoreIdentity) { i.Binding.GroupID = [16]byte{} }},
		{"sidecars", func(i *ReplicatedShardStoreIdentity) { i.Sidecars.TransactionMarkerBytes++ }},
		{"schema_generation", func(i *ReplicatedShardStoreIdentity) { i.RelationSchemaGeneration++ }},
		{"manifest_digest", func(i *ReplicatedShardStoreIdentity) { i.RelationManifestDigest[0] ^= 1 }},
		{"relation_count", func(i *ReplicatedShardStoreIdentity) { i.RelationCount++ }},
		{"base_storage", func(i *ReplicatedShardStoreIdentity) { i.UserStorage = strings.Repeat("d4", 32) }},
		{"global_storage", func(i *ReplicatedShardStoreIdentity) { i.Relations[2].Storage = "../foreign" }},
		{"global_encoding", func(i *ReplicatedShardStoreIdentity) { i.Relations[2].KeyEncoding++ }},
		{"global_tuple_version", func(i *ReplicatedShardStoreIdentity) { i.Relations[2].TupleVersion++ }},
		{"global_mapper_version", func(i *ReplicatedShardStoreIdentity) { i.Relations[2].MapperVersion++ }},
		{"sparse_relations", func(i *ReplicatedShardStoreIdentity) { i.Relations[2].Relation++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := ownedReplicatedShardStoreIdentity(expected)
			test.mutate(&bad)
			if _, err := db.BindReplicatedShardStoreBundleIdentity(bad, globals); err == nil {
				t.Fatal("malformed identity accepted")
			}
			assertReservedBundleUnchanged(t, db, path, before, nil)
		})
	}
}

func assertReservedBundleUnchanged(t *testing.T, db *Database, path string, before []byte, entries []os.DirEntry) {
	t.Helper()
	core := db.connector.db
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("catalog changed: %v", err)
	}
	if core.catalog.ReplicatedShardStore != nil || core.catalogWritePending || core.txnLog.Options() != (durable.TxnLogOptions{}) {
		t.Fatal("rejected identity changed publication/transaction state")
	}
	for name, table := range core.tables {
		if table.collection != nil || table.file != nil || table.meta.Materialized || table.meta.SealedRecoveryJournalBytes != 0 {
			t.Fatalf("rejected identity materialized %s", name)
		}
	}
	afterEntries, err := os.ReadDir(core.dataDir)
	if err != nil || len(entries) != len(afterEntries) {
		t.Fatalf("namespace changed: %v, %v", afterEntries, err)
	}
	for i := range entries {
		if entries[i].Name() != afterEntries[i].Name() {
			t.Fatal("namespace entries changed")
		}
	}
}
