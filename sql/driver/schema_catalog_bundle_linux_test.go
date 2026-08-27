//go:build linux

package driver

import (
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

// Run the complete persisted target, commit, interrupted namespace promotion,
// reopen, continued apply and source-drain path with a real exact-index bundle.
// Linux strict-allocation failures are fatal, never silently skipped.
func TestReplicatedSchemaBaseLocalGlobalBundleRollover(t *testing.T) {
	path, database, binding, _ := prepareReplicatedTestRoot(t, "schema-indexed-bundle", false)
	session, err := database.NewSession(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"CREATE INDEX by_email ON docs (email)",
		"CREATE TABLE email_claims (PRIMARY KEY (key))",
	} {
		if err := testRuntimeExec(session, statement, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	identity, err := database.BindReplicatedShardStoreBundle(binding, "docs", []ReplicatedGlobalIndexRelation{{
		Relation: 2, Table: "email_claims", IndexID: 41, Incarnation: 7, LocatorCount: 1, Unique: true,
		KeyEncoding: ReplicatedRelationKeyCanonicalTuple, KeyArity: 1,
		TupleVersion: distribution.CurrentTupleVersion, MapperVersion: distribution.NativeMapperVersion,
		BucketBits: distribution.DefaultVirtualBucketBits,
	}})
	if err != nil {
		t.Fatal(err)
	}
	testReplicatedSchemaTargetRollover(t, path, database, identity)
}
