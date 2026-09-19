package gateway

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestReplicatedControlDirectoryAcceptsCatalogOnlyGenerationAdvance(t *testing.T) {
	node := scalingTestNodeRecord(rafttransport.NodeID{0xe8}, 1, NodeActive, 1)
	node.CatalogGeneration = 1
	initial := ReplicatedControlDirectorySnapshot{
		Revision: 1, CatalogGeneration: 1, Nodes: []NodeRecord{node},
	}
	directory, err := NewReplicatedControlDirectory(initial)
	if err != nil {
		t.Fatalf("new control directory: %v", err)
	}
	advanced := initial
	advanced.CatalogGeneration = 2
	if err := directory.Apply(advanced); err != nil {
		t.Fatalf("catalog-only generation advance: %v", err)
	}
	if directory.Revision() != initial.Revision || directory.CatalogGeneration() != advanced.CatalogGeneration {
		t.Fatalf("effective directory coordinate revision=%d generation=%d", directory.Revision(), directory.CatalogGeneration())
	}
	if got := directory.Nodes(); len(got) != 1 || got[0] != node {
		t.Fatalf("catalog-only advance rewrote physical records: %+v", got)
	}
	rollback := advanced
	rollback.CatalogGeneration = 1
	if err := directory.Apply(rollback); !errors.Is(err, ErrReplicatedControlRevision) {
		t.Fatalf("catalog generation rollback=%v, want ErrReplicatedControlRevision", err)
	}
}
