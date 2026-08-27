package main

import (
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

func TestRF3ChildPreparerAcceptsOnlyExactLocalManifestSlot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "split-children")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	node := rafttransport.NodeID{1}
	template := rf3ManifestSplitChildRegistry{
		Root: root, MaxOperations: 2, MemberCount: 1,
		Members: [rf3ManifestMembers]rf3ManifestMember{{
			MemberID: 2, NodeID: node, PeerAddress: "127.0.0.1:1101",
		}},
	}
	registry, err := newRF3SplitChildPathRegistry(template)
	if err != nil {
		t.Fatal(err)
	}
	preparer, err := newRF3ChildPreparer(
		registry, node,
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1101},
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1102},
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1103},
		&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1104},
	)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := registry.acquire([32]byte{7}, 1)
	if err != nil {
		t.Fatal(err)
	}
	target := splitcontroller.ChildReplicaTarget{
		Member: 2, Node: node, StoreID: [16]byte{3},
		Endpoint: "127.0.0.1:1101", NativeEndpoint: "127.0.0.1:1102",
		ControlEndpoint: "127.0.0.1:1103", SnapshotAddress: "127.0.0.1:1104", RuntimeRoot: paths.Root,
		SQLPath: paths.Database, WALPath: paths.WAL,
	}
	target.WAL.MemberID, target.WAL.StoreID = target.Member, target.StoreID
	target.SQL.Binding.MemberID, target.SQL.Binding.StoreID = target.Member, target.StoreID
	if !preparer.matchesLocalTarget(target, paths) {
		t.Fatal("exact local target rejected")
	}
	for name, mutate := range map[string]func(*splitcontroller.ChildReplicaTarget){
		"node":     func(candidate *splitcontroller.ChildReplicaTarget) { candidate.Node[0]++ },
		"member":   func(candidate *splitcontroller.ChildReplicaTarget) { candidate.Member++ },
		"sql path": func(candidate *splitcontroller.ChildReplicaTarget) { candidate.SQLPath += ".forged" },
		"control":  func(candidate *splitcontroller.ChildReplicaTarget) { candidate.ControlEndpoint = "127.0.0.1:9999" },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := target
			mutate(&candidate)
			if preparer.matchesLocalTarget(candidate, paths) {
				t.Fatal("substituted target accepted")
			}
		})
	}
	// Production catalogs use logical endpoint names, not listener addresses.
	target.Endpoint, target.NativeEndpoint, target.ControlEndpoint = "data-1-peer", "data-1-native", "data-1-control"
	target.PeerAddress, target.NativeAddress, target.ControlAddress = "127.0.0.1:1101", "127.0.0.1:1102", "127.0.0.1:1103"
	if !preparer.matchesLocalTarget(target, paths) {
		t.Fatal("logical endpoints with exact local transport addresses rejected")
	}
	for name, mutate := range map[string]func(*splitcontroller.ChildReplicaTarget){
		"peer address":    func(candidate *splitcontroller.ChildReplicaTarget) { candidate.PeerAddress = "127.0.0.1:9999" },
		"native address":  func(candidate *splitcontroller.ChildReplicaTarget) { candidate.NativeAddress = "127.0.0.1:9999" },
		"control address": func(candidate *splitcontroller.ChildReplicaTarget) { candidate.ControlAddress = "127.0.0.1:9999" },
		"partial addresses": func(candidate *splitcontroller.ChildReplicaTarget) {
			candidate.NativeAddress = ""
		},
		"unresolved logical endpoints": func(candidate *splitcontroller.ChildReplicaTarget) {
			candidate.PeerAddress, candidate.NativeAddress, candidate.ControlAddress = "", "", ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			candidate := target
			mutate(&candidate)
			if preparer.matchesLocalTarget(candidate, paths) {
				t.Fatal("substituted transport destination accepted")
			}
		})
	}
}

func TestEnsureRF3PreparedChildDirectoriesRejectsSymlink(t *testing.T) {
	root := filepath.Join(t.TempDir(), "split-children")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	operation := filepath.Join(root, "operation")
	if err := os.Symlink(outside, operation); err != nil {
		t.Fatal(err)
	}
	if err := ensureRF3PreparedChildDirectories(root, filepath.Join(operation, "child-1")); err == nil {
		t.Fatal("symlinked allocation root accepted")
	}
}
