package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/hotshard"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
)

func TestDevPhysicalPlacementIsDeterministicAndCoversSixNodes(t *testing.T) {
	placements := [3][]int{}
	seen := make(map[int]bool)
	for group := range placements {
		placements[group] = devPhysicalPlacement(devClusterPhysicalNodes6, group)
		if len(placements[group]) != devClusterRF3 {
			t.Fatalf("group %d placement=%v", group, placements[group])
		}
		for _, node := range placements[group] {
			seen[node] = true
		}
	}
	if len(seen) != devClusterPhysicalNodes6 {
		t.Fatalf("placement union=%v, want every physical node", seen)
	}
	if placements[0][2] != placements[1][0] || placements[1][2] != placements[2][0] {
		t.Fatalf("placements do not overlap deterministically: %v", placements)
	}
	for group := range placements {
		again := devPhysicalPlacement(devClusterPhysicalNodes6, group)
		for index := range again {
			if again[index] != placements[group][index] {
				t.Fatalf("group %d changed from %v to %v", group, placements[group], again)
			}
		}
	}
}

func TestInitializeDevPhysicalClusterPersistsPerFrontendGatewayConfig(t *testing.T) {
	for _, physical := range []int{devClusterPhysicalNodes3, devClusterPhysicalNodes6} {
		t.Run(filepath.Base(filepath.Join("nodes", string(rune('0'+physical)))), func(t *testing.T) {
			root := t.TempDir()
			manifest, err := initializeDevCluster(devClusterOptions{root: root, replicas: devClusterRF3, physicalNodes: physical, shardBinary: "/usr/bin/true"}, filepath.Join(root, "cluster.vibejson"))
			if !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("initialize physical=%d error=%v", physical, err)
			}
			if manifest.PhysicalNodes != uint8(physical) || len(manifest.NodeManifests) != physical || !validDevManifest(manifest, root) {
				t.Fatalf("invalid physical manifest=%+v", manifest)
			}
			seenGateway := make(map[string]bool)
			seenSession := make(map[string]bool)
			for index, node := range manifest.NodeManifests {
				if len(node.Groups) == 0 || node.ServeManifest != filepath.Join(root, "node-"+itoa(index+1), "serve-rf3.vibejson") {
					t.Fatalf("node %d inventory=%+v", index, node)
				}
				raw, readErr := os.ReadFile(filepath.Join(root, "node-"+itoa(index+1)) + ".prepare-node.vibejson")
				if readErr != nil {
					t.Fatal(readErr)
				}
				var input devPrepareNodeManifest
				if unmarshalErr := vibejson.Unmarshal(raw, &input); unmarshalErr != nil || input.Gateway == nil {
					t.Fatalf("node %d input gateway=%+v err=%v", index, input.Gateway, unmarshalErr)
				}
				if input.Gateway.CatalogClientID == node.Node || input.Gateway.CatalogClientID == "" || seenGateway[input.Gateway.CatalogClientID] {
					t.Fatalf("node %d gateway identity=%q", index, input.Gateway.CatalogClientID)
				}
				seenGateway[input.Gateway.CatalogClientID] = true
				if input.Gateway.CatalogSessionJournal == "" || seenSession[input.Gateway.CatalogSessionJournal] {
					t.Fatalf("node %d session journal=%q", index, input.Gateway.CatalogSessionJournal)
				}
				seenSession[input.Gateway.CatalogSessionJournal] = true
				if input.Gateway.TLS.Certificate == node.Certificate || input.Gateway.TLS.Certificate == "" {
					t.Fatalf("node %d reused storage TLS certificate", index)
				}
				canonical, marshalErr := vibejson.Marshal(&input)
				if marshalErr != nil || string(canonical) != string(raw) {
					t.Fatalf("node %d input not canonical: %v", index, marshalErr)
				}
			}
		})
	}
}

func itoa(value int) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	result := make([]byte, 0, 3)
	for value > 0 {
		result = append([]byte{digits[value%10]}, result...)
		value /= 10
	}
	return string(result)
}

func TestDevClusterSupervisorLockRetainsOneInode(t *testing.T) {
	root := t.TempDir()
	unlock, err := lockDevCluster(root)
	if err != nil {
		t.Fatal(err)
	}
	first, err := os.Stat(filepath.Join(root, devClusterLockName))
	if err != nil {
		t.Fatal(err)
	}
	if competing, err := lockDevCluster(root); !errors.Is(err, storeio.ErrWriterLocked) {
		if competing != nil {
			competing()
		}
		t.Fatalf("competing supervisor error=%v", err)
	}
	unlock()
	unlock, err = lockDevCluster(root)
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	second, err := os.Stat(filepath.Join(root, devClusterLockName))
	if err != nil || !os.SameFile(first, second) {
		t.Fatalf("lock inode replaced: %v", err)
	}
}

func TestDevPhysicalPGEndpointsAreExplicitAndUnambiguous(t *testing.T) {
	for _, tc := range []struct {
		single, multiple string
		physical         int
	}{
		{"127.0.0.1:5000", "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003", 3},
		{"", "127.0.0.1:5001,127.0.0.1:5002", 3},
		{"", "127.0.0.1:5001,127.0.0.1:5001,127.0.0.1:5003", 3},
		{"", "127.0.0.1:5001,,127.0.0.1:5003", 3},
		{"0.0.0.0:5001", "", 3},
		{"127.0.0.1:05001", "", 3},
	} {
		if _, err := devPhysicalPGListens(tc.single, tc.multiple, 3, tc.physical); err == nil {
			t.Fatalf("accepted endpoints %+v", tc)
		}
	}
	if values, err := devPhysicalPGListens("", "127.0.0.1:5001,[::1]:5002,127.0.0.2:5003", 3, 3); err != nil || len(values) != 3 {
		t.Fatalf("valid explicit endpoints: %v %v", values, err)
	}
}

func TestDevPhysicalPlannedPlacementAndCapacity(t *testing.T) {
	for _, physical := range []int{3, 6} {
		t.Run(fmt.Sprint(physical), func(t *testing.T) {
			root := t.TempDir()
			cluster, err := initializeDevPhysicalCluster(devClusterOptions{root: root, replicas: 3, physicalNodes: physical, shardBinary: "/usr/bin/true"}, filepath.Join(root, "cluster.vibejson"))
			if !errors.Is(err, os.ErrNotExist) {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(cluster.HotShardCapacity)
			if err != nil {
				t.Fatal(err)
			}
			capacity, err := hotshard.OpenStaticCapacityConfig(raw)
			if err != nil || len(capacity.Nodes) != physical {
				t.Fatalf("capacity nodes=%d error=%v", len(capacity.Nodes), err)
			}
			var inventory devTableInventory
			seen := map[string]bool{}
			for ordinal := range 3 {
				table, err := planDevPhysicalTable(cluster, fmt.Sprintf("table_%d", ordinal), "/id", fmt.Sprintf("CREATE TABLE table_%d (PRIMARY KEY (id))", ordinal), uint64(ordinal))
				if err != nil {
					t.Fatal(err)
				}
				inventory.Tables = append(inventory.Tables, table)
				inventory.NextPlacement++
				if err := reserveDevPhysicalTablePlans(&cluster, inventory); err != nil {
					t.Fatal(err)
				}
				for _, target := range table.PhysicalNodes {
					known := map[string]bool{}
					for _, role := range [][]devClusterMember{cluster.Members, cluster.LedgerMembers, cluster.DataMembers} {
						hosted := false
						for _, member := range role {
							hosted = hosted || member.Node == target
						}
						if hosted {
							for _, member := range role {
								known[member.Node] = true
							}
						}
					}
					for _, peer := range table.PhysicalNodes {
						if !known[peer] {
							t.Fatalf("placement gives node %s an unregistered Raft peer %s", target, peer)
						}
					}
				}
				for _, node := range table.PhysicalNodes {
					seen[node] = true
				}
			}
			if len(seen) != physical {
				t.Fatalf("table placement covers %d physical nodes, want %d", len(seen), physical)
			}
			bad := inventory
			bad.Tables = append([]devTableProvision(nil), inventory.Tables...)
			bad.Tables[0].GroupRoots[0] = bad.Tables[0].GroupRoots[1]
			if err := reserveDevPhysicalTablePlans(&cluster, bad); err == nil {
				t.Fatal("accepted a substituted physical root")
			}
			for index := range cluster.NodeManifests {
				node := &cluster.NodeManifests[index]
				for len(node.Groups) < devPhysicalMaxGroups {
					node.Groups = append(node.Groups, filepath.Join(filepath.Dir(node.ServeManifest), fmt.Sprintf("group-%d", len(node.Groups))))
				}
			}
			before, _ := vibejson.Marshal(&cluster)
			if _, err := planDevPhysicalTable(cluster, "full", "/id", "CREATE TABLE full (PRIMARY KEY (id))", inventory.NextPlacement); err == nil {
				t.Fatal("accepted group above per-node capacity")
			}
			after, _ := vibejson.Marshal(&cluster)
			if !bytes.Equal(before, after) {
				t.Fatal("capacity rejection changed cluster inventory")
			}
		})
	}
}

// This opt-in test invokes the shipped Linux preparer, including its strict
// gateway parser and real NodeStore creation. It exercises recovery at the
// durable inventory boundary and after a partially prepared RF3 group.
func TestDevPhysicalRealPreparationRecoversExactPlans(t *testing.T) {
	binary := os.Getenv("VIBEDB_PHYSICAL_TEST_SHARD_BINARY")
	if binary == "" {
		t.Skip("set VIBEDB_PHYSICAL_TEST_SHARD_BINARY to a real Linux vibedb-shard binary")
	}
	if runtime.GOOS != "linux" {
		t.Fatal("real physical preparation requires Linux")
	}
	for _, physical := range []int{3, 6} {
		t.Run(fmt.Sprint(physical), func(t *testing.T) {
			root := t.TempDir()
			options := devClusterOptions{root: root, replicas: 3, physicalNodes: physical, shardBinary: binary}
			cluster, err := ensureDevCluster(options)
			if err != nil {
				t.Fatal(err)
			}
			initial := make(map[string][]byte)
			gatewayBytes := make(map[string][]byte)
			for index, node := range cluster.NodeManifests {
				raw, err := os.ReadFile(node.ServeManifest)
				if err != nil {
					t.Fatal(err)
				}
				initial[node.ServeManifest] = raw
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(raw, &fields); err != nil {
					t.Fatal(err)
				}
				gatewayBytes[node.ServeManifest] = fields["gateway"]
				controlRaw, err := os.ReadFile(devPhysicalControlPath(cluster, index))
				if err != nil {
					t.Fatal(err)
				}
				var control devReplicaControlManifest
				if err := vibejson.Unmarshal(controlRaw, &control); err != nil || control.LocalGateway.Node != node.GatewayNode || len(control.GatewayEndpoints) != physical || len(control.ShardEndpoints) != physical {
					t.Fatalf("node control inventory: %+v %v", control, err)
				}
			}
			// A command wrapper fails before member two. Member one and the
			// complete plan are durable; restart must retain every identity.
			wrapper := filepath.Join(t.TempDir(), "fail-second-member")
			script := "#!/bin/sh\ncase \"$3\" in *member-2.vibejson) exit 79;; esac\nexec '" + strings.ReplaceAll(binary, "'", "'\\''") + "' \"$@\"\n"
			if err := os.WriteFile(wrapper, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			schema := filepath.Join(root, "test-table.sql")
			if err := os.WriteFile(schema, []byte("CREATE TABLE recover_alpha (value TEXT, PRIMARY KEY (id))"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := ensureDevTables(root, wrapper, &cluster, schema); err == nil {
				t.Fatal("injected member preparation failure did not fail")
			}
			inventoryPath := filepath.Join(root, "tables.vibejson")
			planned, err := os.ReadFile(inventoryPath)
			if err != nil {
				t.Fatal(err)
			}
			var inventory devTableInventory
			if err := vibejson.Unmarshal(planned, &inventory); err != nil || len(inventory.Tables) != 1 {
				t.Fatalf("durable planned inventory: %+v %v", inventory, err)
			}
			if _, err := os.Stat(filepath.Join(inventory.Tables[0].GroupRoots[0], "sql-identity.vibejson")); err != nil {
				t.Fatal("first member not durably prepared", err)
			}
			cluster, err = ensureDevCluster(options)
			if err != nil {
				t.Fatal("physical restart failed", err)
			}
			if err := ensureDevTables(root, binary, &cluster, ""); err != nil {
				t.Fatal("resume failed", err)
			}
			after, err := os.ReadFile(inventoryPath)
			if err != nil || !bytes.Equal(planned, after) {
				t.Fatalf("restart replaced the planned identities: %v", err)
			}
			alpha := inventory.Tables[0]
			alphaMembers, alphaGroup, err := devPhysicalTableMembers(cluster, alpha)
			if err != nil {
				t.Fatal(err)
			}
			// Hold the exact SQL writer locks a live serve-node owns. The next
			// CREATE must validate alpha's completed proof without reopening it.
			for _, member := range alphaMembers {
				lock, err := os.OpenFile(filepath.Join(member.GroupRoot, "member.vdb.lock"), os.O_RDWR, 0o600)
				if err != nil {
					t.Fatal(err)
				}
				if err := storeio.LockWriter(lock); err != nil {
					lock.Close()
					t.Fatal(err)
				}
				t.Cleanup(func() {
					storeio.UnlockWriter(lock)
					lock.Close()
				})
			}
			if _, err := buildDevPhysicalTableProvision(alpha, alphaMembers, alphaGroup, false); !errors.Is(err, storeio.ErrWriterLocked) {
				t.Fatalf("cold validation did not observe the live writer lock: %v", err)
			}
			foreignIdentity, err := os.ReadFile(filepath.Join(alpha.GroupRoots[1], "sql-identity.vibejson"))
			if err != nil {
				t.Fatal(err)
			}
			for _, mismatch := range []struct {
				name, path string
				replace    func([]byte) []byte
			}{
				{"fragment_schema", filepath.Join(root, alpha.artifactStem()+"-catalog.vibejson"), func(raw []byte) []byte {
					return bytes.ReplaceAll(raw, []byte("value TEXT"), []byte("value INTEGER"))
				}},
				{"preparation", filepath.Join(root, "prepare-"+alpha.artifactStem()+"-member-1.vibejson"), func(raw []byte) []byte {
					return bytes.ReplaceAll(raw, []byte(alpha.Stores[0]), []byte(alpha.Stores[1]))
				}},
				{"store_binding", filepath.Join(alpha.GroupRoots[0], "sql-identity.vibejson"), func([]byte) []byte {
					return foreignIdentity
				}},
				{"apply_identity", filepath.Join(alpha.GroupRoots[0], "apply-identity.vibejson"), func([]byte) []byte {
					return []byte("{}")
				}},
			} {
				t.Run(mismatch.name, func(t *testing.T) {
					original, err := os.ReadFile(mismatch.path)
					if err != nil {
						t.Fatal(err)
					}
					changed := mismatch.replace(original)
					if bytes.Equal(changed, original) {
						t.Fatal("mismatch did not change the retained proof")
					}
					if err := os.WriteFile(mismatch.path, changed, 0o600); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						if err := os.WriteFile(mismatch.path, original, 0o600); err != nil {
							t.Fatal(err)
						}
					})
					if err := ensureDevTables(root, binary, &cluster, ""); err == nil {
						t.Fatal("accepted a substituted completed preparation witness")
					}
					retained, err := os.ReadFile(mismatch.path)
					if err != nil || !bytes.Equal(retained, changed) {
						t.Fatalf("repaired a mismatched completed witness: %v", err)
					}
				})
			}
			for _, name := range []string{"recover_beta", "recover_gamma"} {
				if err := os.WriteFile(schema, []byte("CREATE TABLE "+name+" (value TEXT, PRIMARY KEY (id))"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := ensureDevTables(root, binary, &cluster, schema); err != nil {
					t.Fatal(err)
				}
			}
			raw, _ := os.ReadFile(inventoryPath)
			if err := vibejson.Unmarshal(raw, &inventory); err != nil {
				t.Fatal(err)
			}
			catalog, err := gateway.LoadSnapshot(cluster.CatalogPath)
			if err != nil {
				t.Fatal(err)
			}
			seen := map[string]bool{}
			for _, table := range inventory.Tables {
				members, group, err := devPhysicalTableMembers(cluster, table)
				if err != nil {
					t.Fatal(err)
				}
				for _, member := range members {
					seen[member.Node] = true
				}
				path, err := devTableCatalogPath(root, table.Table)
				if err != nil || path != filepath.Join(root, table.artifactStem()+"-catalog.vibejson") {
					t.Fatalf("DDL fragment=%q %v", path, err)
				}
				fragment, _ := os.ReadFile(path)
				addition, err := gateway.OpenReplicatedTableProvision(fragment)
				if err != nil || addition.ReplicatedShardDescriptors()[0].Group != group {
					t.Fatalf("fragment group differs: %v", err)
				}
				catalog, err = gateway.BuildReplicatedTableAddition(catalog, addition)
				if err != nil {
					t.Fatal("rotated table endpoint collision", err)
				}
			}
			if len(seen) != physical {
				t.Fatalf("prepared table placements cover %d/%d nodes", len(seen), physical)
			}
			for _, node := range cluster.NodeManifests {
				raw, _ := os.ReadFile(node.ServeManifest)
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(raw, &fields); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(gatewayBytes[node.ServeManifest], fields["gateway"]) {
					t.Fatal("live append changed gateway configuration")
				}
				digest := sha256.Sum256(initial[node.ServeManifest])
				retained, err := os.ReadFile(filepath.Join(filepath.Dir(node.ServeManifest), "prepared-manifests", hex.EncodeToString(digest[:])+".vibejson"))
				if err != nil || !bytes.Equal(retained, initial[node.ServeManifest]) {
					t.Fatalf("predecessor not retained: %v", err)
				}
			}
			if _, err := ensureDevCluster(options); err != nil {
				t.Fatal("completed table recovery changed initial catalog", err)
			}
		})
	}
}
