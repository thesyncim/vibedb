package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibejson"
)

func TestDevClusterManifestResumeIsCanonicalAndDoesNotReprovision(t *testing.T) {
	root := t.TempDir()
	manifest := devClusterManifest{
		Format: devClusterFormat, Nodes: devClusterRF3,
		ClientEndpoint: "127.0.0.1:24000", CatalogPath: filepath.Join(root, "catalog.vibejson"),
		GatewayCertificate: filepath.Join(root, "gateway-cert.pem"), GatewayKey: filepath.Join(root, "gateway-key.pem"),
		Roots: filepath.Join(root, "roots.pem"), AuthorizationPolicy: filepath.Join(root, "policy.vibejson"),
		GatewayNode: "01010101010101010101010101010101", Members: make([]devClusterMember, devClusterRF3), LedgerMembers: make([]devClusterMember, devClusterRF3),
	}
	paths := []string{manifest.CatalogPath, manifest.GatewayCertificate, manifest.GatewayKey, manifest.Roots, manifest.AuthorizationPolicy}
	for index := range manifest.Members {
		manifest.Members[index] = devClusterMember{Member: uint64(index + 1), Node: "1111111111111111111111111111111" + string(rune('1'+index)), Store: "2222222222222222222222222222222" + string(rune('1'+index)), Peer: "127.0.0.1:2500" + string(rune('1'+index)), Native: "127.0.0.1:2510" + string(rune('1'+index)), Snapshot: "127.0.0.1:2520" + string(rune('1'+index)), Control: "127.0.0.1:2530" + string(rune('1'+index)), ServeManifest: filepath.Join(root, "member-"+string(rune('1'+index)), "serve-rf3.vibejson")}
		manifest.LedgerMembers[index] = devClusterMember{Member: uint64(index + 1), Node: manifest.Members[index].Node, Store: "3333333333333333333333333333333" + string(rune('1'+index)), Peer: "127.0.0.1:2600" + string(rune('1'+index)), Native: "127.0.0.1:2610" + string(rune('1'+index)), Snapshot: "127.0.0.1:2620" + string(rune('1'+index)), Control: "127.0.0.1:2630" + string(rune('1'+index)), ServeManifest: filepath.Join(root, "ledger-member-"+string(rune('1'+index)), "serve-rf3.vibejson")}
		paths = append(paths, manifest.Members[index].ServeManifest, manifest.LedgerMembers[index].ServeManifest)
	}
	for _, path := range paths {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("retained"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := vibejson.Marshal(&manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "cluster.vibejson"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := ensureDevCluster(devClusterOptions{root: root, replicas: 3, shardBinary: "/does/not/run"})
	if err != nil {
		t.Fatal(err)
	}
	again, err := vibejson.Marshal(&loaded)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatalf("resume changed manifest: %v", err)
	}
}

func TestDevClusterManifestAcceptsOnlyExplicitRF1OrRF3(t *testing.T) {
	root := t.TempDir()
	base := devClusterManifest{
		Format: devClusterFormat, Nodes: devClusterRF1, ClientEndpoint: "127.0.0.1:24000",
		CatalogPath: filepath.Join(root, "catalog.vibejson"), GatewayCertificate: filepath.Join(root, "gateway-cert.pem"),
		GatewayKey: filepath.Join(root, "gateway-key.pem"), Roots: filepath.Join(root, "roots.pem"),
		AuthorizationPolicy: filepath.Join(root, "policy.vibejson"), GatewayNode: "01010101010101010101010101010101",
		Members:       []devClusterMember{{Member: 1, Node: "11111111111111111111111111111111", Store: "22222222222222222222222222222222", Peer: "127.0.0.1:25001", Native: "127.0.0.1:25101", Snapshot: "127.0.0.1:25201", Control: "127.0.0.1:25301", ServeManifest: filepath.Join(root, "member-1", "serve-rf3.vibejson")}},
		LedgerMembers: []devClusterMember{{Member: 1, Node: "11111111111111111111111111111111", Store: "33333333333333333333333333333333", Peer: "127.0.0.1:26001", Native: "127.0.0.1:26101", Snapshot: "127.0.0.1:26201", Control: "127.0.0.1:26301", ServeManifest: filepath.Join(root, "ledger-member-1", "serve-rf3.vibejson")}},
	}
	if !validDevManifest(base, root) {
		t.Fatal("explicit RF1 manifest rejected")
	}
	withoutLedger := base
	withoutLedger.LedgerMembers = nil
	if validDevManifest(withoutLedger, root) {
		t.Fatal("manifest without a dedicated request-ledger group accepted")
	}
	reusedStore := base
	reusedStore.LedgerMembers = append([]devClusterMember(nil), base.LedgerMembers...)
	reusedStore.LedgerMembers[0].Store = reusedStore.Members[0].Store
	if validDevManifest(reusedStore, root) {
		t.Fatal("request-ledger group reused the catalog store")
	}
	base.Nodes = 2
	if validDevManifest(base, root) {
		t.Fatal("RF2 manifest accepted")
	}
}

func TestClusterDevReplicaFlagsRejectAmbiguity(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	for _, args := range [][]string{
		{"--root", root, "--replicas", "2"},
		{"--root", root, "--nodes", "0"},
		{"--root", root, "--replicas", "1", "--nodes", "3"},
	} {
		if code := runClusterDev(args); code != 2 {
			t.Fatalf("runClusterDev(%v) = %d, want usage error", args, code)
		}
	}
}

func TestDevPolicyIsAcceptedByProductionAuthorizationLoader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.vibejson")
	var nodes [4]rafttransport.NodeID
	for index := range nodes {
		nodes[index][0] = byte(index + 1)
	}
	if err := writeDevPolicy(path, nodes[:]); err != nil {
		t.Fatal(err)
	}
	policy, err := serviceauthz.LoadFile(path)
	if err != nil || policy.Generation() != 1 || len(policy.Nodes()) != len(nodes) {
		t.Fatalf("policy=%+v err=%v", policy, err)
	}
}

func TestDevChildShutdownDoesNotLeakProcess(t *testing.T) {
	child, err := startDevChild("/bin/sh", []string{"-c", "echo READY >&2; trap 'exit 0' TERM; while :; do sleep 1; done"}, "READY")
	if err != nil {
		t.Fatal(err)
	}
	if err := waitDevReady(t.Context(), child); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	stopDevChildren([]*devChild{child})
	if child.command.ProcessState == nil || time.Since(started) > 5*time.Second {
		t.Fatalf("child not reaped promptly: %+v", child.command.ProcessState)
	}
}

func TestReserveDevPortsUsesDistinctLoopbackEndpoints(t *testing.T) {
	ports, err := reserveDevPorts(13)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]struct{}, len(ports))
	for _, address := range ports {
		if _, exists := seen[address]; exists {
			t.Fatalf("duplicate %q", address)
		}
		seen[address] = struct{}{}
	}
}

func TestDevEndpointAndIdentityValidationIsCanonical(t *testing.T) {
	for _, address := range []string{"localhost:1234", "[::1]:1234", "127.0.0.1:01234", "127.0.0.1:0", "127.0.0.1:http"} {
		if validDevLoopbackAddress(address) {
			t.Fatalf("noncanonical endpoint %q accepted", address)
		}
	}
	if !validDevLoopbackAddress("127.0.0.1:1234") {
		t.Fatal("canonical loopback endpoint rejected")
	}
	if _, err := decodeDev16("00000000000000000000000000000000"); err == nil {
		t.Fatal("zero identity accepted")
	}
}

func TestDevLogicalAuthorityDerivationIsDeterministicAndDomainSeparated(t *testing.T) {
	group := raftmember.GroupKey{
		ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
		TopologyRecoveryEpoch: 3, ShardIncarnation: [16]byte{4}, GroupID: [16]byte{5},
	}
	relation := [32]byte{6}
	rangeIdentity, lineage, forwarding := deriveDevLogicalRangeAuthority(
		group, distribution.DistributionName("request-ledger"), distribution.ShardID("all"), relation,
	)
	againRange, againLineage, againForwarding := deriveDevLogicalRangeAuthority(
		group, distribution.DistributionName("request-ledger"), distribution.ShardID("all"), relation,
	)
	if rangeIdentity == (replication.Digest{}) || lineage == (replication.Digest{}) ||
		forwarding == (replication.Digest{}) || rangeIdentity != againRange ||
		lineage != againLineage || forwarding != againForwarding ||
		rangeIdentity == lineage || rangeIdentity == forwarding || lineage == forwarding {
		t.Fatalf("derived authorities are zero, unstable, or aliased: %x %x %x", rangeIdentity, lineage, forwarding)
	}
	home := deriveDevLedgerHomeIdentity(group, rangeIdentity, relation)
	if home == (replication.Digest{}) || home == rangeIdentity ||
		home != deriveDevLedgerHomeIdentity(group, rangeIdentity, relation) {
		t.Fatalf("ledger home identity = %x", home)
	}
	changed := group
	changed.GroupID[0]++
	changedRange, _, _ := deriveDevLogicalRangeAuthority(
		changed, distribution.DistributionName("request-ledger"), distribution.ShardID("all"), relation,
	)
	if changedRange == rangeIdentity {
		t.Fatal("authenticated group change did not change range authority")
	}
	relation[0]++
	changedRange, _, _ = deriveDevLogicalRangeAuthority(
		group, distribution.DistributionName("request-ledger"), distribution.ShardID("all"), relation,
	)
	if changedRange == rangeIdentity {
		t.Fatal("relation-manifest change did not change range authority")
	}
}

func TestClusterDevRejectsRelativeRootBeforeResolvingChildren(t *testing.T) {
	if status := runClusterDev([]string{"--root", "relative"}); status != 2 {
		t.Fatalf("status=%d", status)
	}
}

func TestResolveDevBinaryRejectsExecutableDirectory(t *testing.T) {
	if _, err := resolveDevBinary(t.TempDir(), "ignored"); err == nil {
		t.Fatal("executable directory accepted as a child binary")
	}
}

func TestDevSupervisorObservesChildExit(t *testing.T) {
	child, err := startDevChild("/bin/sh", []string{"-c", "echo READY >&2; exit 17"}, "READY")
	if err != nil {
		t.Fatal(err)
	}
	exits := make(chan devChildExit, 1)
	watchDevChildExit(exits, "shard member 1", child)
	if err := waitDevReadyOrExit(context.Background(), child, exits); err != nil {
		// An immediate exit is also a correct supervisor result.
		return
	}
	select {
	case exit := <-exits:
		if exit.name != "shard member 1" || exit.err == nil {
			t.Fatalf("exit=%+v", exit)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not observe child exit")
	}
}
