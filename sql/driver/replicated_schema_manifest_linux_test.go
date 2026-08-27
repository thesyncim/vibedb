//go:build linux

package driver

import (
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestReplicatedSchemaManifestMatchesLiveBundleMachine(t *testing.T) {
	_, database, expected, globals := prepareReservedReplicatedBundle(t)
	base, err := database.BindReplicatedShardStoreBundleIdentity(expected, globals)
	if err != nil {
		t.Fatal(err)
	}
	options := testReplicatedApplyOptions()
	apply, _, err := database.OpenReplicatedApply(base, testReplicatedApplyBootstrap(), options)
	if err != nil {
		t.Fatal(err)
	}
	defer apply.Close()
	actual, err := apply.RangeSplitRelationManifestDigest()
	if err != nil {
		t.Fatal(err)
	}
	expectedDigest, err := ReplicatedSchemaManifest(base, options.Placement, []store.IndexDefinition{{Name: "by_email", Paths: []string{"/email"}}})
	if err != nil || expectedDigest != actual {
		t.Fatalf("cold/live bundle manifest differ: cold=%x live=%x err=%v", expectedDigest, actual, err)
	}
}
