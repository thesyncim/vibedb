//go:build darwin || linux

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
)

// Compose the actual cold fixture renderers through the same two strict
// parsers and member combiner consumed by bootstrap-rf3, without allocating a
// WAL. Multi-group manifests deliberately have no singleton MemberManifest.
func TestColdTargetBootstrapGroupManifestPreflight(t *testing.T) {
	root := t.TempDir()
	first, second := rf3EnrolledGroupIdentities()
	identities := []raftstore.Identity{first, second}
	nodes := rf3CommandNodes()
	peers := [3]string{"127.0.0.1:31001", "127.0.0.1:31002", "127.0.0.1:31003"}
	targetNode := rafttransport.NodeID{0xd1}
	listeners := rf3ManifestListeners{Peer: "127.0.0.1:31004", Native: "127.0.0.1:32004", Snapshot: "127.0.0.1:33004", Control: "127.0.0.1:34004"}
	paths := make([]string, len(identities))
	for i, source := range identities {
		groupRoot := filepath.Join(root, fmt.Sprintf("group-%d", i))
		if err := os.Mkdir(groupRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		options := rf3ColdTargetOptions{Root: groupRoot,
			Group: raftmember.GroupKey{ClusterID: source.ClusterID, ClusterIncarnation: source.ClusterIncarnation,
				TopologyRecoveryEpoch: 1, ShardIncarnation: source.ShardIncarnation, GroupID: source.GroupID},
			Distribution: source.Distribution, Shard: source.Shard, AllocationGeneration: source.AllocationGeneration,
			Target:    rf3ManifestEnrolledTarget{MemberID: 4, NodeID: targetNode, StoreID: [16]byte{byte(200 + i)}, NodeIncarnation: 9},
			Listeners: listeners, SourceNode: nodes[0], SourceSnapshotAddress: "127.0.0.1:33001", MaxArtifactBytes: 1 << 30}
		identity := rf3ColdTargetIdentity(t, options)
		memberPath := filepath.Join(groupRoot, "member.vibejson")
		member := rf3CommandManifestDocument(filepath.Join(groupRoot, "wal"), filepath.Join(groupRoot, "member.vdb"),
			filepath.Join(groupRoot, "sql-identity"), filepath.Join(groupRoot, "apply-identity"), filepath.Join(groupRoot, "wal-key"),
			listeners.Peer, listeners.Native, listeners.Snapshot, listeners.Control,
			rf3testfixture.Credential{Certificate: filepath.Join(root, "cert"), Key: filepath.Join(root, "key")},
			filepath.Join(root, "roots"), filepath.Join(root, "policy"), rf3testfixture.DurableGatewayWALOptions(), nodes, peers, identity, options.Group.TopologyRecoveryEpoch)
		member = rf3CommandEnrollTarget(member, targetNode, options.Target.StoreID, options.Target.NodeIncarnation,
			listeners.Peer, listeners.Native, listeners.Snapshot, listeners.Control)
		if err := os.WriteFile(memberPath, member, 0o600); err != nil {
			t.Fatal(err)
		}
		paths[i] = filepath.Join(groupRoot, "bootstrap.vibejson")
		bootstrap := rf3ColdTargetBootstrapDocument(options, memberPath, filepath.Join(groupRoot, "static-bootstrap.pb"))
		if err := os.WriteFile(paths[i], bootstrap, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifest, err := parseBootstrapRF3Manifest(combineBootstrapRF3ProcessGroups(t, paths...))
	if err != nil || manifest.MemberManifest != "" || len(manifest.Groups) != 2 {
		t.Fatalf("invalid multi-group fixture shape: %+v %v", manifest, err)
	}
	members := make([]rf3Manifest, len(manifest.groupBundles()))
	for i, bundle := range manifest.groupBundles() {
		members[i], err = loadRF3Manifest(bundle.MemberManifest)
		if err != nil {
			t.Fatalf("exact member group %d: %v", i, err)
		}
		if members[i].Route.Shard != identities[i].Shard || members[i].Route.Group.GroupID != identities[i].GroupID ||
			members[i].Route.MemberID != 4 || members[i].EnrolledTarget == nil || members[i].EnrolledTarget.NodeID != targetNode ||
			bundle.SourceNode != nodes[0] || bundle.MemberManifest == "" {
			t.Fatalf("group %d lost source or target identity", i)
		}
	}
	combined, err := combineColdRF3MemberManifests(members)
	if err != nil || len(combined.Groups) != 2 {
		t.Fatalf("shipped cold member composition: %v", err)
	}
}
