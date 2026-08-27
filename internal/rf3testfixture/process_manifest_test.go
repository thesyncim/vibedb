package rf3testfixture

import (
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	vibejson "github.com/thesyncim/vibejson"
)

func TestProcessMemberManifestIsCanonicalJSONWithColdEnrollment(t *testing.T) {
	root := t.TempDir()
	var nodes [3]rafttransport.NodeID
	for index := range nodes {
		nodes[index][0] = byte(index + 1)
	}
	identity := raftstore.Identity{Distribution: "catalog", Shard: "controlplane",
		AllocationGeneration: 7, MemberID: 1, StoreID: [16]byte{11},
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		ShardIncarnation: [16]byte{3}, GroupID: [16]byte{4}}
	controlRoot := filepath.Join(root, "shared-control")
	options := ProcessMemberOptions{Root: root, ControlRoot: controlRoot, Identity: identity,
		Key: raftstore.Key{ID: "rf3-command-key"},
		WAL: raftstore.Options{MaxFileBytes: 1, MaxRecordBytes: 2, MaxRecords: 3,
			MaxEntries: 4, MaxLiveBytes: 5}, Bootstrap: InitialBootstrap([]uint64{1, 2, 3}),
		Listeners: ProcessListeners{Peer: "127.0.0.1:1", Native: "127.0.0.1:2",
			Snapshot: "127.0.0.1:3", Control: "127.0.0.1:4"},
		Credential: Credential{Certificate: "/cert", Key: "/key"}, Roots: "/roots",
		AuthorizationPolicy: "/policy", Nodes: nodes,
		PeerAddresses: [3]string{"127.0.0.1:1", "127.0.0.1:5", "127.0.0.1:6"},
		Target: &ProcessTarget{MemberID: 4, NodeID: [16]byte{9}, StoreID: [16]byte{10},
			NodeIncarnation: 11, Listeners: ProcessListeners{Peer: "127.0.0.1:7",
				Native: "127.0.0.1:8", Snapshot: "127.0.0.1:9", Control: "127.0.0.1:10"}},
	}
	prepared := &PreparedMember{WALPath: filepath.Join(root, "member.wal"),
		SQLPath: filepath.Join(root, "member.vdb")}
	raw := processMemberManifest(options, prepared,
		filepath.Join(root, "identity.json"), filepath.Join(root, "apply.json"), filepath.Join(root, "key"))
	document, err := vibejson.Parse(raw)
	if err != nil {
		t.Fatalf("generated invalid process manifest: %s: %v", raw, err)
	}
	for _, field := range []string{"enrolled_target", "replica_control", "split_control"} {
		if _, found := document.Get(field); !found {
			t.Fatalf("incomplete process manifest: missing %q", field)
		}
	}
	route, _ := document.Get("route")
	if got := processManifestTestText(t, route, "member_root"); got != root {
		t.Fatalf("member_root=%q want data root %q", got, root)
	}
	replicaControl, _ := document.Get("replica_control")
	if got := processManifestTestText(t, replicaControl, "source_data_root"); got != controlRoot {
		t.Fatalf("source_data_root=%q want shared control root %q", got, controlRoot)
	}
	splitControl, _ := document.Get("split_control")
	registry, found := splitControl.Get("child_registry")
	if !found {
		t.Fatal("split control has no child registry")
	}
	if got := processManifestTestText(t, registry, "root"); got != filepath.Join(controlRoot, "split-children") {
		t.Fatalf("split child root=%q", got)
	}
	childWAL, found := registry.Get("wal")
	if !found {
		t.Fatal("split child registry has no WAL")
	}
	if got := processManifestTestText(t, childWAL, "key_material_path"); got != filepath.Join(controlRoot, "wal-key") {
		t.Fatalf("split child WAL key=%q", got)
	}
}

func processManifestTestText(t testing.TB, document vibejson.Value, key string) string {
	t.Helper()
	value, found := document.Get(key)
	if !found {
		t.Fatalf("process manifest has no %q", key)
	}
	text, ok := value.Text()
	if !ok {
		t.Fatalf("process manifest %q is not text", key)
	}
	return text
}
