package driver

import (
	"crypto/sha256"
	"strings"
	"testing"
)

func TestInitialReplicatedRelationManifestMatchesServingIdentity(t *testing.T) {
	digest, limits, err := InitialReplicatedRelationManifest("documents")
	if err != nil {
		t.Fatal(err)
	}
	wantLimits := ReplicatedShardStoreLimits{
		MaxKeyBytes:       replicatedMaxKeyBytes,
		MaxDocumentBytes:  replicatedMaxDocumentBytes,
		MaxBatchDocuments: replicatedMaxDistinctMutations,
		MaxBatchBytes:     replicatedMaxBatchBytes,
	}
	identity := ReplicatedShardStoreIdentity{
		RelationCount: 1, RelationSchemaGeneration: 1,
		Relations: []ReplicatedShardRelationIdentity{{
			Relation: 1, Kind: ReplicatedShardRelationJSON, Table: "documents", Limits: wantLimits,
		}},
	}
	want := replicatedRelationApplyManifestDigest(identity)
	if digest == ([sha256.Size]byte{}) || digest != want || limits != wantLimits {
		t.Fatalf("initial manifest digest=%x limits=%+v want=%x,%+v", digest, limits, want, wantLimits)
	}
	second, secondLimits, err := InitialReplicatedRelationManifest("documents")
	if err != nil || second != digest || secondLimits != limits {
		t.Fatalf("initial manifest is not deterministic: %x %+v %v", second, secondLimits, err)
	}
}

func TestInitialReplicatedRelationManifestRejectsInvalidTable(t *testing.T) {
	for _, table := range []string{"", "documents\x00other", strings.Repeat("x", 1<<20)} {
		if digest, limits, err := InitialReplicatedRelationManifest(table); err == nil ||
			digest != ([sha256.Size]byte{}) || limits != (ReplicatedShardStoreLimits{}) {
			t.Fatalf("accepted table=%q digest=%x limits=%+v err=%v", table, digest, limits, err)
		}
	}
}
