package shardservice

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/internal/clusterrestore"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
)

func TestRestoredShardCannotServeWithoutCatalogObservedAuthority(t *testing.T) {
	server := testReplicatedServer(&fakeReplicatedOwner{})
	if err := server.BindRestoreServingAuthority(nil); err == nil {
		t.Fatal("nil restore authority admitted")
	}
	// The ordinary live-serving predicate remains closed until a validated
	// authority is supplied; peer Raft operation is not represented here.
	server.serving = func(raftservice.ServingState) bool { return false }
	response := server.executeReplicated(context.Background(), &ReplicatedRequest{
		Operation: ReplicatedProbe, Fence: ReplicatedFence{Group: raftmember.GroupKey{GroupID: [16]byte{1}}},
	})
	if response.Kind != ReplicatedRefusal || response.Refusal != ReplicatedRefusalUnavailable {
		t.Fatalf("response=%+v", response)
	}
	var _ *clusterrestore.ServingAuthority
}
