package gateway

import (
	"errors"
	"sync"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
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

func TestReplicatedControlDirectoryAtomicApplyRollbackOnFailure(t *testing.T) {
	nodeA := scalingTestNodeRecord(rafttransport.NodeID{0x10}, 1, NodeActive, 1)
	nodeA.CatalogGeneration = 1
	directory, err := NewReplicatedControlDirectory(ReplicatedControlDirectorySnapshot{
		Revision: 1, CatalogGeneration: 1, Nodes: []NodeRecord{nodeA},
	})
	if err != nil {
		t.Fatalf("new control directory: %v", err)
	}

	// Build a revision-2 snapshot where nodeB (sorted before nodeA) is valid and
	// new, while nodeA conflicts on ControlAddress for the same (NodeID, Incarnation).
	nodeB := scalingTestNodeRecord(rafttransport.NodeID{0x05}, 1, NodeActive, 2)
	nodeB.CatalogGeneration = 1
	conflictingA := nodeA
	conflictingA.Revision = 2
	conflictingA.ControlAddress = "127.0.0.1:59999"

	err = directory.Apply(ReplicatedControlDirectorySnapshot{
		Revision: 2, CatalogGeneration: 1, Nodes: []NodeRecord{nodeB, conflictingA},
	})
	if !errors.Is(err, ErrReplicatedControlDirectory) {
		t.Fatalf("conflicting Apply error = %v, want ErrReplicatedControlDirectory", err)
	}

	// Directory state must be completely unchanged — no partial leak of nodeB.
	if directory.Revision() != 1 || directory.CatalogGeneration() != 1 {
		t.Fatalf("failed Apply advanced coordinates: rev=%d gen=%d", directory.Revision(), directory.CatalogGeneration())
	}
	if got := directory.Nodes(); len(got) != 1 || got[0] != nodeA {
		t.Fatalf("failed Apply mutated Nodes(): %+v", got)
	}
	if got := directory.ShardControlEndpoints(); len(got) != 1 || got[0].Node != nodeA.NodeID {
		t.Fatalf("failed Apply leaked nodeB into ShardControlEndpoints(): %+v", got)
	}
}

func TestReplicatedControlDirectoryReturnedSlicesAreIsolated(t *testing.T) {
	nodeA := scalingTestNodeRecord(rafttransport.NodeID{0x01}, 1, NodeActive, 1)
	nodeB := scalingTestNodeRecord(rafttransport.NodeID{0x02}, 1, NodeActive, 1)
	nodeA.CatalogGeneration, nodeB.CatalogGeneration = 1, 1
	directory, err := NewReplicatedControlDirectory(ReplicatedControlDirectorySnapshot{
		Revision: 1, CatalogGeneration: 1, Nodes: []NodeRecord{nodeA, nodeB},
	})
	if err != nil {
		t.Fatalf("new control directory: %v", err)
	}

	nodes := directory.Nodes()
	nodes[0] = NodeRecord{}
	if got := directory.Nodes(); len(got) != 2 || got[0] != nodeA || got[1] != nodeB {
		t.Fatalf("mutating Nodes() corrupted directory: %+v", got)
	}

	shards := directory.ShardControlEndpoints()
	shards[0] = ReplicatedEndpoint{}
	if got := directory.ShardControlEndpoints(); len(got) != 2 || got[0].Node != nodeA.NodeID {
		t.Fatalf("mutating ShardControlEndpoints() corrupted directory: %+v", got)
	}
}

func TestReplicatedControlDirectoryConcurrentReadersAndWriters(t *testing.T) {
	nodeA := scalingTestNodeRecord(rafttransport.NodeID{0x01}, 1, NodeActive, 1)
	nodeB := scalingTestNodeRecord(rafttransport.NodeID{0x02}, 1, NodeActive, 1)
	nodeA.CatalogGeneration, nodeB.CatalogGeneration = 1, 1
	directory, err := NewReplicatedControlDirectory(ReplicatedControlDirectorySnapshot{
		Revision: 1, CatalogGeneration: 1, Nodes: []NodeRecord{nodeA, nodeB},
	})
	if err != nil {
		t.Fatalf("new control directory: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var lastRev, lastGen uint64
			for {
				select {
				case <-stop:
					return
				default:
				}
				rev := directory.Revision()
				gen := directory.CatalogGeneration()
				if rev < lastRev || gen < lastGen {
					t.Errorf("monotonicity violated: rev %d < %d or gen %d < %d", rev, lastRev, gen, lastGen)
					return
				}
				lastRev, lastGen = rev, gen
				if len(directory.Nodes()) == 0 {
					t.Error("Nodes() returned empty cut")
					return
				}
				if len(directory.ShardControlEndpoints()) < 2 {
					t.Error("ShardControlEndpoints() lost historical endpoints")
					return
				}
				_ = directory.GatewayControlEndpoints()
				_ = directory.GatewayControlEndpointsWithHistory()
			}
		}()
	}

	for rev := uint64(2); rev <= 64; rev++ {
		nA := scalingTestNodeRecord(rafttransport.NodeID{0x01}, 1, NodeActive, rev)
		nB := scalingTestNodeRecord(rafttransport.NodeID{0x02}, 1, NodeActive, rev)
		nA.CatalogGeneration, nB.CatalogGeneration = rev, rev
		if err := directory.Apply(ReplicatedControlDirectorySnapshot{
			Revision: rev, CatalogGeneration: rev, Nodes: []NodeRecord{nA, nB},
		}); err != nil {
			t.Fatalf("Apply rev %d: %v", rev, err)
		}
		// Also exercise catalog-only advance at same revision.
		if err := directory.Apply(ReplicatedControlDirectorySnapshot{
			Revision: rev, CatalogGeneration: rev + 1, Nodes: []NodeRecord{nA, nB},
		}); err != nil {
			t.Fatalf("Apply catalog-only rev %d: %v", rev, err)
		}
	}
	close(stop)
	wg.Wait()
}

func TestReplicatedControlDirectoryZeroValueAndReplacementProof(t *testing.T) {
	var directory ReplicatedControlDirectory
	if directory.Revision() != 0 || directory.CatalogGeneration() != 0 ||
		directory.Nodes() != nil || directory.ShardControlEndpoints() != nil ||
		directory.GatewayControlEndpoints() != nil || directory.GatewayControlEndpointsWithHistory() != nil {
		t.Fatal("zero-valued ReplicatedControlDirectory must report empty state")
	}

	node := scalingTestNodeRecord(rafttransport.NodeID{0x22}, 2, NodeActive, 1)
	node.CatalogGeneration = 1
	proof1 := &NodeReplacementProof{
		PredecessorIncarnation:      1,
		PredecessorServiceKeyDigest: replication.Digest{0x99},
		IntentID:                    [32]byte{0x44},
		ProofDigest:                 replication.Digest{0x55},
	}
	node.Replacement = proof1
	if err := directory.Apply(ReplicatedControlDirectorySnapshot{
		Revision: 1, CatalogGeneration: 1, Nodes: []NodeRecord{node},
	}); err != nil {
		t.Fatalf("Apply on zero-valued directory: %v", err)
	}

	// Mutating the caller's Replacement proof or returned Nodes()[0].Replacement
	// must not corrupt internal immutable state.
	proof1.PredecessorIncarnation = 999
	got := directory.Nodes()
	if len(got) != 1 || got[0].Replacement == nil || got[0].Replacement.PredecessorIncarnation != 1 {
		t.Fatalf("caller mutation leaked into directory: %+v", got)
	}
	got[0].Replacement.PredecessorIncarnation = 888
	if got2 := directory.Nodes(); got2[0].Replacement.PredecessorIncarnation != 1 {
		t.Fatalf("returned Nodes() mutation leaked into directory: %+v", got2[0].Replacement)
	}

	// Catalog-only generation advance at the same revision with a freshly
	// allocated equal Replacement proof (as produced by JSON catalog reads)
	// must succeed rather than failing on pointer inequality.
	replayed := node
	replayed.Replacement = &NodeReplacementProof{
		PredecessorIncarnation:      1,
		PredecessorServiceKeyDigest: replication.Digest{0x99},
		IntentID:                    [32]byte{0x44},
		ProofDigest:                 replication.Digest{0x55},
	}
	if err := directory.Apply(ReplicatedControlDirectorySnapshot{
		Revision: 1, CatalogGeneration: 2, Nodes: []NodeRecord{replayed},
	}); err != nil {
		t.Fatalf("catalog-only advance with distinct Replacement pointer failed: %v", err)
	}
	if directory.CatalogGeneration() != 2 {
		t.Fatalf("CatalogGeneration() = %d, want 2", directory.CatalogGeneration())
	}
}

func scalingTestGatewayNodeRecord(id byte, incarnation uint64, lifecycle NodeLifecycle, rev uint64, gwAddr string) NodeRecord {
	node := scalingTestNodeRecord(rafttransport.NodeID{id}, incarnation, lifecycle, rev)
	node.CatalogGeneration = 1
	node.Roles |= NodeRoleGateway
	node.GatewayEndpoint = distribution.EndpointID("gw-" + string([]byte{'a' + id%26}))
	node.GatewayAddress = gwAddr
	node.Gateway = GatewayIdentity{
		NodeID:            node.NodeID,
		Incarnation:       incarnation,
		ServiceKeyDigest:  replication.Digest{id, 0x01},
		ServiceID:         [16]byte{id, 0x02},
		SessionID:         [16]byte{id, 0x03},
		SessionRevision:   1,
		ParticipantDigest: replication.Digest{id, 0x04},
	}
	return node
}

func TestReplicatedControlDirectoryGatewayEndpointsRollbackAndIsolation(t *testing.T) {
	gwA := scalingTestGatewayNodeRecord(0x10, 1, NodeActive, 1, "127.0.0.1:9101")
	directory, err := NewReplicatedControlDirectory(ReplicatedControlDirectorySnapshot{
		Revision: 1, CatalogGeneration: 1, Nodes: []NodeRecord{gwA},
	})
	if err != nil {
		t.Fatalf("new control directory: %v", err)
	}

	// Returned gateway slices must be isolated from caller mutation.
	gws := directory.GatewayControlEndpoints()
	if len(gws) != 1 || gws[0].Address != "127.0.0.1:9101" {
		t.Fatalf("unexpected GatewayControlEndpoints: %+v", gws)
	}
	gws[0].Address = "mutated"
	if got := directory.GatewayControlEndpoints(); len(got) != 1 || got[0].Address != "127.0.0.1:9101" {
		t.Fatalf("mutating GatewayControlEndpoints corrupted directory: %+v", got)
	}
	hist := directory.GatewayControlEndpointsWithHistory()
	hist[0].Address = "mutated"
	if got := directory.GatewayControlEndpointsWithHistory(); len(got) != 1 || got[0].Address != "127.0.0.1:9101" {
		t.Fatalf("mutating GatewayControlEndpointsWithHistory corrupted directory: %+v", got)
	}

	// A mid-snapshot gateway conflict on gwA must roll back gwB (sorted earlier).
	gwB := scalingTestGatewayNodeRecord(0x05, 1, NodeActive, 2, "127.0.0.1:9102")
	conflictingA := gwA
	conflictingA.Revision = 2
	conflictingA.GatewayAddress = "127.0.0.1:59998"
	if err := directory.Apply(ReplicatedControlDirectorySnapshot{
		Revision: 2, CatalogGeneration: 1, Nodes: []NodeRecord{gwB, conflictingA},
	}); !errors.Is(err, ErrReplicatedControlDirectory) {
		t.Fatalf("conflicting gateway Apply = %v, want ErrReplicatedControlDirectory", err)
	}
	if got := directory.GatewayControlEndpointsWithHistory(); len(got) != 1 || got[0].Member.Node != gwA.NodeID {
		t.Fatalf("failed gateway Apply leaked gwB into history: %+v", got)
	}

	// Decommissioning gwA removes it from current GatewayControlEndpoints()
	// while retaining it in GatewayControlEndpointsWithHistory().
	decommA := gwA
	decommA.Revision = 2
	decommA.Lifecycle = NodeDecommissioned
	decommA.RetirementScanDigest = replication.Digest{0x77}
	decommA.RetirementScanDirectoryRevision = 2
	decommA.RetirementScanCutRevision = 2
	if err := directory.Apply(ReplicatedControlDirectorySnapshot{
		Revision: 2, CatalogGeneration: 1, Nodes: []NodeRecord{decommA},
	}); err != nil {
		t.Fatalf("decommission Apply: %v", err)
	}
	if got := directory.GatewayControlEndpoints(); got == nil || len(got) != 0 {
		t.Fatalf("decommissioned GatewayControlEndpoints() = %#v, want non-nil empty", got)
	}
	if got := directory.GatewayControlEndpointsWithHistory(); len(got) != 1 || got[0].Member.Node != gwA.NodeID {
		t.Fatalf("decommissioned GatewayControlEndpointsWithHistory() = %+v, want gwA retained", got)
	}
}

func BenchmarkReplicatedControlDirectoryRevisionParallel(b *testing.B) {
	nodes := make([]NodeRecord, 8)
	for i := range nodes {
		n := scalingTestNodeRecord(rafttransport.NodeID{byte(i + 1)}, 1, NodeActive, 1)
		n.CatalogGeneration = 1
		nodes[i] = n
	}
	directory, err := NewReplicatedControlDirectory(ReplicatedControlDirectorySnapshot{
		Revision: 1, CatalogGeneration: 1, Nodes: nodes,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if directory.Revision() == 0 {
				b.Fatal("unexpected zero revision")
			}
		}
	})
}

func BenchmarkReplicatedControlDirectoryNodesParallel(b *testing.B) {
	nodes := make([]NodeRecord, 8)
	for i := range nodes {
		n := scalingTestNodeRecord(rafttransport.NodeID{byte(i + 1)}, 1, NodeActive, 1)
		n.CatalogGeneration = 1
		nodes[i] = n
	}
	directory, err := NewReplicatedControlDirectory(ReplicatedControlDirectorySnapshot{
		Revision: 1, CatalogGeneration: 1, Nodes: nodes,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if len(directory.Nodes()) != 8 {
				b.Fatal("unexpected node count")
			}
		}
	})
}

func BenchmarkReplicatedControlDirectoryShardEndpointsParallel(b *testing.B) {
	nodes := make([]NodeRecord, 8)
	for i := range nodes {
		n := scalingTestNodeRecord(rafttransport.NodeID{byte(i + 1)}, 1, NodeActive, 1)
		n.CatalogGeneration = 1
		nodes[i] = n
	}
	directory, err := NewReplicatedControlDirectory(ReplicatedControlDirectorySnapshot{
		Revision: 1, CatalogGeneration: 1, Nodes: nodes,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if len(directory.ShardControlEndpoints()) != 8 {
				b.Fatal("unexpected endpoint count")
			}
		}
	})
}
