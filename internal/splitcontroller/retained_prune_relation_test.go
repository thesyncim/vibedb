package splitcontroller

import (
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestRetainedPruneRelationsMustBeExactDenseBundle(t *testing.T) {
	for _, relations := range [][]replication.RelationID{nil, {2}, {2, 3}} {
		if !validRetainedPruneRelations(relations) {
			t.Fatalf("dense bundle rejected: %v", relations)
		}
	}
	for _, relations := range [][]replication.RelationID{{0}, {1}, {3}, {2, 2}, {3, 2}, {2, 4}} {
		if validRetainedPruneRelations(relations) {
			t.Fatalf("noncanonical bundle accepted: %v", relations)
		}
	}
	tooMany := make([]replication.RelationID, replication.MaxRelationsPerBundle)
	for index := range tooMany {
		tooMany[index] = replication.RelationID(index + 2)
	}
	if validRetainedPruneRelations(tooMany) {
		t.Fatal("oversized bundle accepted")
	}
}

func TestRetainedPruneIndexOnlyPendingBindsExactRelationAndRawValue(t *testing.T) {
	fence := retainedPruneTestFence()
	clientID := RetainedPruneClientID(OperationID{9})
	proof := retainedPruneTestProof(replication.Digest{8})
	mutation := replication.Mutation{Kind: replication.MutationDeleteDigestEqual,
		Key: []byte("same-key"), ExpectedValueLength: 19, ExpectedValueDigest: replication.Digest{7}}
	raw := retainedPruneTestCommand(t, fence, clientID, 2, proof, []replication.Mutation{mutation})
	want := []gateway.NativeMutation{{Relation: 2, Kind: mutation.Kind, Key: mutation.Key,
		ExpectedValueLength: mutation.ExpectedValueLength, ExpectedValueDigest: mutation.ExpectedValueDigest}}
	if !retainedPrunePendingMatches(raw, fence, clientID, 2, proof, want) {
		t.Fatal("exact index-only retry rejected")
	}
	if retainedPrunePendingMatches(raw, fence, clientID, 1, proof, want) {
		t.Fatal("index retry accepted as base batch")
	}
	want[0].Relation = 1
	if retainedPrunePendingMatches(raw, fence, clientID, 2, proof, want) {
		t.Fatal("same key in another relation matched")
	}
	want[0].Relation = 2
	want[0].ExpectedValueDigest[0]++
	if retainedPrunePendingMatches(raw, fence, clientID, 2, proof, want) {
		t.Fatal("different stored index value matched")
	}
}
