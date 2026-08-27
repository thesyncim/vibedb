package main

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/rf3testfixture"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// Exercise the same four schemas, full singleton renderer, structural
// composition, strict shipped parser, and retained-template comparison used by
// the external gateway gates. No WAL, socket, environment opt-in, or Linux-only
// allocation is needed to discover a heterogeneous-profile composition bug.
func TestDurableGatewayFourRoleManifestPreflight(t *testing.T) {
	root := t.TempDir()
	profiles := rf3testfixture.DurableGatewayMemberProfiles()
	var options [rf3testfixture.DurableGatewayGroups]rf3testfixture.ProcessMemberOptions
	documents := make([][]byte, len(profiles))
	nodes := [3]rafttransport.NodeID{{1}, {2}, {3}}
	peers := [3]string{"127.0.0.1:21001", "127.0.0.1:21002", "127.0.0.1:21003"}
	for group, profile := range profiles {
		options[group] = rf3testfixture.ProcessMemberOptions{
			Root: filepath.Join(root, fmt.Sprintf("group-%d", group)), ControlRoot: filepath.Join(root, "control"),
			Table: profile.Table, CreateTable: profile.CreateTable,
			SchemaStatements: profile.SchemaStatements, GlobalIndexes: profile.GlobalIndexes,
			Authority: profile.Authority, Apply: profile.Apply,
			Identity: raftstore.Identity{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2},
				ShardIncarnation: [16]byte{byte(group + 10)}, GroupID: [16]byte{byte(group + 20)},
				Distribution: profile.Table, Shard: "all", AllocationGeneration: 1,
				MemberID: 1, StoreID: [16]byte{30}},
			Key: raftstore.Key{ID: "preflight-key"}, WAL: rf3testfixture.DurableGatewayWALOptions(),
			Bootstrap: rf3testfixture.InitialBootstrap([]uint64{1, 2, 3}),
			Listeners: rf3testfixture.ProcessListeners{Peer: peers[0], Native: "127.0.0.1:22001",
				Snapshot: "127.0.0.1:23001", Control: "127.0.0.1:24001"},
			Credential: rf3testfixture.Credential{Certificate: filepath.Join(root, "node.crt"), Key: filepath.Join(root, "node.key")},
			Roots:      filepath.Join(root, "roots.crt"), AuthorizationPolicy: filepath.Join(root, "policy.vibejson"),
			Nodes: nodes, PeerAddresses: peers,
		}
		documents[group] = rf3testfixture.ProcessMemberManifest(options[group])
		if _, err := parseRF3Manifest(documents[group]); err != nil {
			t.Fatalf("strict shipped parser rejected singleton role %d: %v", group, err)
		}
	}
	combined, err := rf3testfixture.CombineProcessManifests(documents...)
	if err != nil {
		t.Fatalf("combine complete heterogeneous role manifests: %v", err)
	}
	manifest, err := parseRF3Manifest(combined)
	if err != nil {
		t.Fatalf("strict shipped parser rejected combined role manifests: %v", err)
	}
	bundles := manifest.groupBundles()
	if len(bundles) != len(profiles) {
		t.Fatalf("parsed groups=%d want=%d", len(bundles), len(profiles))
	}
	seenRoots := make(map[string]bool, len(profiles))
	for group, profile := range profiles {
		single := manifest.withGroup(bundles[group])
		registry := single.SplitControl.ChildRegistry
		apply := profile.Apply
		retained := sqldriver.ReplicatedApplyIdentity{
			MaxSessions: apply.MaxSessions, RetryWindow: apply.RetryWindow,
			TxnLimits: apply.TxnLimits, Placement: apply.Placement,
			RequestLedgerCapacityBytes:       apply.RequestLedgerCapacityBytes,
			RequestLedgerCleanupReserveBytes: apply.RequestLedgerCleanupReserveBytes,
			RequestLedgerRangeStart:          apply.RequestLedgerRangeStart,
			RequestLedgerRangeEnd:            apply.RequestLedgerRangeEnd,
			RequestLedgerRangeIdentity:       apply.RequestLedgerRangeIdentity,
		}
		base := sqldriver.ReplicatedShardStoreIdentity{UserTable: profile.Table,
			UserPrimaryKey: profile.Apply.Placement.ShardKey}
		if !rf3SplitChildTemplateMatchesRetained(registry, base, retained) {
			t.Fatalf("group %d template does not match exact shared retained profile", group)
		}
		wantRoot := filepath.Join(options[group].Root, "split-children")
		if registry.Root != wantRoot || seenRoots[registry.Root] ||
			registry.StaticBootstrapPath != filepath.Join(wantRoot, "static-bootstrap.pb") {
			t.Fatalf("group %d child root/bootstrap aliases or drifts: %+v", group, registry)
		}
		seenRoots[registry.Root] = true
		other := profiles[(group+1)%len(profiles)]
		base.UserTable = other.Table
		if rf3SplitChildTemplateMatchesRetained(registry, base, retained) {
			t.Fatalf("group %d accepts a foreign group's retained table", group)
		}
	}
}
