//go:build linux

package rf3testfixture

import (
	"errors"
	"os"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestColdProcessTargetDoesNotRequireServingApply(t *testing.T) {
	profile := DurableGatewayMemberProfiles()[DurableGatewayLedgerGroup]
	identity := raftstore.Identity{Distribution: "request-ledger", Shard: "all", AllocationGeneration: 1,
		MemberID: 4, ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}, StoreID: [16]byte{5}}
	listeners := ProcessListeners{Peer: "127.0.0.1:1", Native: "127.0.0.1:2", Snapshot: "127.0.0.1:3", Control: "127.0.0.1:4"}
	prepared, err := PrepareColdProcessTarget(ProcessMemberOptions{
		Root: t.TempDir(), Table: profile.Table, CreateTable: profile.CreateTable,
		Identity: identity, Key: raftstore.Key{ID: "cold", Wrapped: []byte("wrapped"), Material: [32]byte{6}},
		WAL: DurableGatewayWALOptions(), Bootstrap: InitialBootstrap([]uint64{1, 2, 3}),
		Authority: profile.Authority, Apply: profile.Apply, Listeners: listeners,
		Credential: Credential{Certificate: "/cert", Key: "/key"}, Roots: "/roots", AuthorizationPolicy: "/policy",
		Nodes: [3]rafttransport.NodeID{{1}, {2}, {3}}, PeerAddresses: [3]string{"127.0.0.1:11", "127.0.0.1:12", "127.0.0.1:13"},
		Target: &ProcessTarget{MemberID: 4, NodeID: [16]byte{4}, StoreID: identity.StoreID, NodeIncarnation: 1, Listeners: listeners},
	}, [16]byte{2}, "127.0.0.1:23", 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.LogicalSchemaDigest == ([32]byte{}) || prepared.RelationManifestDigest != ([32]byte{}) {
		t.Fatal("cold reservation must carry a logical schema, not fabricated live machine authority")
	}
	if _, err := os.Stat(prepared.WALPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cold reservation created a WAL before certified snapshot installation: %v", err)
	}
	for _, path := range []string{prepared.ManifestPath, prepared.BootstrapManifestPath, prepared.StaticBootstrapPath, prepared.SQLPath} {
		if info, err := os.Stat(path); err != nil || info.Size() == 0 {
			t.Fatalf("cold reservation lost %s: %v", path, err)
		}
	}
}
