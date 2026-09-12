//go:build darwin || linux

package main

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

func prepareRF3FaultNodeMember(t testing.TB, fixture *rf3FaultFixture, member int, root, policy string, key []byte, geometry raftstore.Options) {
	t.Helper()
	keySource := filepath.Join(fixture.root, fmt.Sprintf("node-key-input-%d", member))
	if err := os.WriteFile(keySource, key, 0o600); err != nil {
		t.Fatal(err)
	}
	identity, authority := rf3CommandStoreIdentity(uint64(member+1)), fixture.authority
	input := prepareRF3Manifest{
		Root: filepath.Join(root, "group-0"), Distribution: identity.Distribution, Shard: identity.Shard,
		ClusterID: idString(identity.ClusterID[:]), ClusterIncarnation: idString(identity.ClusterIncarnation[:]),
		TopologyRecoveryEpoch: fixture.group.TopologyRecoveryEpoch, AllocationGeneration: identity.AllocationGeneration,
		ShardIncarnation: idString(identity.ShardIncarnation[:]), GroupID: idString(identity.GroupID[:]), MemberID: identity.MemberID, StoreID: idString(identity.StoreID[:]),
		Table: gateway.ReplicatedCatalogTable, CreateTable: `CREATE TABLE controlplane (PRIMARY KEY (id))`,
		Authority: prepareRF3Authority{ActivePolicyGeneration: authority.ActivePolicyGeneration, ProtectionEpoch: authority.ProtectionEpoch, OwnershipEpoch: authority.OwnershipEpoch, SchemaGeneration: authority.SchemaGeneration, RoutingVersion: authority.RoutingVersion, RouteGeneration: authority.RouteGeneration},
		WAL:       prepareRF3WAL{KeyID: "rf3-command-key", KeyMaterialPath: keySource, WrappedKey: "explicit-test-wrapped-key", MaxFileBytes: geometry.MaxFileBytes, MaxRecordBytes: geometry.MaxRecordBytes, MaxRecords: geometry.MaxRecords, MaxEntries: geometry.MaxEntries, MaxLiveBytes: geometry.MaxLiveBytes},
		Apply:     prepareRF3Apply{MaxSessions: 32, RetryWindow: 8, MaxCollections: 16, MaxDocuments: 1024, MaxBytes: 384 << 20, ShardKey: gateway.ReplicatedCatalogPrimaryKey},
		Listeners: rf3ManifestListeners{Peer: fixture.peerAddresses[member], Native: fixture.nativeAddresses[member], Snapshot: fixture.snapshotAddresses[member], Control: fixture.controlAddresses[member]},
		TLS:       rf3ManifestTLS{PeerKeys: rf3CommandPeerKeys(fixture.credentials[member]), Certificate: fixture.credentials[member].Certificate, Key: fixture.credentials[member].Key, Roots: fixture.roots, IdentityOID: rf3CommandIdentityOID.String()}, AuthorizationPolicy: policy,
		SplitControl: prepareRF3SplitControl{MaxRecords: 4096, MaxFileBytes: 64 << 20, MaxChildOperations: 8, StageCheckpointBytes: 32 << 20},
	}
	for i, node := range fixture.nodes {
		input.Members = append(input.Members, prepareRF3Member{MemberID: uint64(i + 1), NodeID: idString(node[:]), PeerAddress: fixture.peerRoutes[member][i]})
		input.SplitControl.Grants = append(input.SplitControl.Grants, prepareRF3ActionGrant{NodeID: idString(node[:]), Actions: ^uint16(0)})
	}
	node := prepareRF3NodeManifest{Root: root, NodeLog: rf3NodeLogManifest{Format: 1, Path: filepath.Join(root, "node-log"), KeyID: input.WAL.KeyID, KeyMaterialPath: keySource, Options: raftstore.NodeStoreOptions{MaxGroups: 8}}, Groups: []prepareRF3Manifest{input}}
	if err := provisionRF3Node(node); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(input.Root, "member.wal")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("node fixture created a per-group WAL: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(input.Root, "sql-identity.vibejson"))
	if err != nil {
		t.Fatal(err)
	}
	var base sqldriver.ReplicatedShardStoreIdentity
	if err := vibejson.Unmarshal(raw, &base); err != nil {
		t.Fatal(err)
	}
	maximum := base.UserLimits.MaxDocumentBytes
	if maximum <= 0 || maximum > replication.MaxMutationValueBytes || member > 0 && fixture.maxReadValueBytes != uint32(maximum) {
		t.Fatal("inconsistent node fault read response bound")
	}
	fixture.maxReadValueBytes = uint32(maximum)
	fixture.walPaths[member] = node.NodeLog.Path
	fixture.manifestPaths[member] = filepath.Join(root, "serve-rf3.vibejson")
}

func (fixture *rf3FaultFixture) allocatedLogBytes(t testing.TB) int64 {
	t.Helper()
	if !fixture.nodeLog {
		return rf3FaultWALAllocatedBytes(t, fixture.walPaths)
	}
	// Node logs include segments, reserves, descriptor catalogs and checkpoints.
	// Count every physical inode, including directory blocks, exactly once.
	type inode struct{ device, number uint64 }
	seen := make(map[inode]bool)
	var total int64
	for _, root := range fixture.walPaths {
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			info, err := entry.Info()
			if err != nil {
				return err
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok || (!info.Mode().IsRegular() && !info.IsDir()) || stat.Blocks < 0 || stat.Blocks > math.MaxInt64/512 {
				return fmt.Errorf("invalid node allocation evidence %q", path)
			}
			id := inode{uint64(stat.Dev), uint64(stat.Ino)}
			if !seen[id] {
				bytes := stat.Blocks * 512
				if bytes > math.MaxInt64-total {
					return fmt.Errorf("node allocation overflow")
				}
				seen[id] = true
				total += bytes
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return total
}
