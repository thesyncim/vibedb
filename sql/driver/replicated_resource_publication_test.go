package driver

import (
	"github.com/thesyncim/vibedb/internal/replication"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestReplicatedResourceStatsCarriesItsApplyPublication(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "capacity-publication")
	bootstrap := testReplicatedApplyBootstrap()
	claim, _, err := database.OpenReplicatedApply(base, bootstrap, testReplicatedApplyOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer claim.Close()
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	epoch := applyReplicatedApplySessionOpen(t, claim, base, 2)
	before, err := claim.ResourceStats()
	if err != nil {
		t.Fatal(err)
	}
	document := []byte(`{"id":"capacity","value":1}`)
	command := testReplicatedApplyCommand(base, epoch, 2, replication.Mutation{Kind: replication.MutationPut, Key: testReplicatedApplyKey(t, database, document), Value: document})
	publication, err := claim.ApplyNormal(testReplicatedApplyMeta(3), command)
	if err != nil {
		t.Fatal(err)
	}
	after, err := claim.ResourceStats()
	if err != nil {
		t.Fatal(err)
	}
	if before.Publication.Applied != 2 || after.Publication.Applied != publication.Applied || after.Publication.DataChainDigest != publication.DataChainDigest || !proto.Equal(after.Publication.ConfState, publication.ConfState) || after.RelationCount == 0 {
		t.Fatalf("before=%+v after=%+v want=%+v", before.Publication, after.Publication, publication)
	}
}
