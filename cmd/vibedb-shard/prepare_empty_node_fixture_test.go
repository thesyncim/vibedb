package main

import (
	"bytes"
	"encoding/asn1"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	"github.com/thesyncim/vibejson"
)

func TestEmptyNodePreparationFixtureMatchesShippedGrammar(t *testing.T) {
	root := filepath.Join(t.TempDir(), "empty-node")
	nodes := []rafttransport.NodeID{{1}, {2}, {3}, {4}}
	credentials, roots, err := rf3testfixture.WriteCredentials(filepath.Dir(root), asn1.ObjectIdentifier{1, 3, 6, 1, 4, 1, 32473, 1, 1}, rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(root+".node-key", bytes.Repeat([]byte{7}, 32), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := rf3testfixture.EmptyNodePreparationManifest(rf3testfixture.EmptyNodeOptions{
		Root: root, NodeIncarnation: 7,
		Key:        raftstore.Key{ID: "node-key", Wrapped: []byte("wrapped")},
		Listeners:  rf3testfixture.ProcessListeners{Peer: "127.0.0.1:21001", Native: "127.0.0.1:21002", Snapshot: "127.0.0.1:21003", Control: "127.0.0.1:21004"},
		Credential: credentials[0], Roots: roots, AuthorizationPolicy: "/policy",
		GrantNodes: nodes, GatewaySeeds: []nodecontrol.BootstrapGatewaySeed{{NodeID: nodes[0], Incarnation: 1, ControlAddress: "127.0.0.1:22001", SPKIPinDigest: replication.Digest{1}}},
	}, root+".node-key")
	if err != nil {
		t.Fatal(err)
	}
	var input prepareRF3NodeManifest
	if err := vibejson.Unmarshal(raw, &input); err != nil {
		t.Fatalf("fixture input does not decode as prepare-node-rf3: %v\n%s", err, raw)
	}
	canonical, err := vibejson.Marshal(&input)
	if err != nil || !bytes.Equal(raw, canonical) {
		t.Fatalf("fixture input is not canonical: %v\n%s", err, raw)
	}
	if input.Root != root || input.Services == nil || input.Services.NodeIncarnation != 7 || input.NodeLog.Path != filepath.Join(root, "node-log") ||
		input.Services == nil || len(input.Groups) != 0 || input.Services.ReplicaControl.SourceDataRoot != root ||
		len(input.Services.GatewaySeeds) != 1 || input.Services.GatewaySeeds[0].NodeID != nodes[0] {
		t.Fatalf("empty fixture lost explicit node grammar: %+v", input)
	}
	if err := provisionRF3Node(input); err != nil {
		t.Fatalf("provision empty node: %v", err)
	}
	manifest, err := loadRF3Manifest(filepath.Join(root, "serve-rf3.vibejson"))
	if err != nil {
		t.Fatalf("load prepared empty node: %v", err)
	}
	if manifest.Groups == nil || len(manifest.Groups) != 0 || manifest.NodeIncarnation != 7 {
		t.Fatal("prepared capacity node must retain an explicit empty group array and its incarnation")
	}
}
