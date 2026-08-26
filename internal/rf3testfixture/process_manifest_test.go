package rf3testfixture

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
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
	options := ProcessMemberOptions{Root: root, Identity: identity,
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
	if !json.Valid(raw) {
		t.Fatalf("generated invalid process manifest: %s", raw)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil || document["enrolled_target"] == nil ||
		document["replica_control"] == nil || document["split_control"] == nil {
		t.Fatalf("incomplete process manifest: keys=%v err=%v", document, err)
	}
}
