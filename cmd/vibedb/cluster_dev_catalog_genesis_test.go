package main

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson"
)

func TestWriteDevPhysicalCatalogGenesisPlansIsImmutableAndVoterScoped(t *testing.T) {
	root := t.TempDir()
	catalogPath := filepath.Join(root, "catalog.vibejson")
	directoryPath := filepath.Join(root, "initial-node-directory.vibejson")
	manifest, err := distribution.NewManifest(
		distribution.DistributionName("catalog"), 1,
		[]distribution.Shard{{
			ID:                   distribution.ShardID("control"),
			AllocationGeneration: 1,
			Range:                distribution.KeyRange{End: distribution.KeyspaceEnd{Max: true}},
			Leaders:              []distribution.EndpointID{"catalog"},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := gateway.NewSnapshot(distribution.ClusterConfig{
		Distributions: []distribution.DistributionSpec{{
			Name:          distribution.DistributionName("catalog"),
			Arity:         1,
			MapperVersion: distribution.NativeMapperVersion,
		}},
		Manifests: []*distribution.Manifest{manifest},
	}, map[distribution.EndpointID]string{"catalog": "127.0.0.1:7001"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	catalogRaw, err := gateway.AppendSnapshotDocument(nil, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(catalogPath, catalogRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	nodeID := rafttransport.NodeID{1}
	record := gateway.NodeRecord{
		NodeID:            nodeID,
		Incarnation:       1,
		ServiceKeyDigest:  replication.Digest{1},
		DataEndpoint:      "data-1",
		NativeEndpoint:    "native-1",
		ControlEndpoint:   "control-1",
		DataAddress:       "127.0.0.1:7101",
		NativeAddress:     "127.0.0.1:7201",
		ControlAddress:    "127.0.0.1:7301",
		FailureDomain:     "fd-1",
		Roles:             gateway.NodeRoleStorage | gateway.NodeRoleCatalog,
		Lifecycle:         gateway.NodeActive,
		Revision:          1,
		CatalogGeneration: 1,
	}
	if !record.Valid() {
		t.Fatal("test node record is invalid")
	}
	records := []gateway.NodeRecord{record}
	directoryRaw, err := vibejson.Marshal(&records)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(directoryPath, directoryRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	nodeHex := hex.EncodeToString(nodeID[:])
	storeID := strings.Repeat("02", 16)
	config := devCatalogGenesisConfig{
		PlanPath:                 filepath.Join(root, "catalog-genesis-node-1.vibejson"),
		CatalogPath:              catalogPath,
		InitialNodeDirectoryPath: directoryPath,
		SessionJournal:           filepath.Join(root, "node-1", "catalog-session"),
		ClientID:                 nodeHex,
		RetryHome:                "0102030405060708",
		Distribution:             string(gateway.ReplicatedCatalogDistribution),
		Shard:                    string(gateway.ReplicatedCatalogShard),
		ClusterID:                strings.Repeat("03", 16),
		ClusterIncarnation:       strings.Repeat("04", 16),
		TopologyRecoveryEpoch:    1,
		AllocationGeneration:     1,
		ShardIncarnation:         strings.Repeat("05", 16),
		GroupID:                  strings.Repeat("06", 16),
		MemberID:                 1,
		StoreID:                  storeID,
		NodeID:                   nodeHex,
		NodeIncarnation:          1,
		Relation:                 1,
	}
	serveManifest := filepath.Join(root, "node-1", "serve-rf3.vibejson")
	preparePath := filepath.Dir(serveManifest) + ".prepare-node.vibejson"
	prepareRaw, err := vibejson.Marshal(&devPrepareNodeManifest{CatalogGenesis: &config})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(preparePath, prepareRaw, 0o600); err != nil {
		t.Fatal(err)
	}
	cluster := devClusterManifest{
		CatalogPath: catalogPath,
		Members:     []devClusterMember{{Member: 1, Node: nodeHex, Store: storeID}},
		NodeManifests: []devPhysicalNode{{
			Node: nodeHex, ServeManifest: serveManifest,
		}},
	}
	if err := writeDevPhysicalCatalogGenesisPlans(cluster); err != nil {
		t.Fatal(err)
	}
	first, err := os.ReadFile(config.PlanPath)
	if err != nil {
		t.Fatal(err)
	}
	var plan devCatalogGenesisPlan
	if err := vibejson.Unmarshal(first, &plan); err != nil {
		t.Fatal(err)
	}
	canonical, err := vibejson.Marshal(&plan)
	if err != nil || !bytes.Equal(first, canonical) {
		t.Fatalf("plan is not canonical: %v", err)
	}
	if plan.Format != devCatalogGenesisPlanFormat || plan.CatalogPath != catalogPath ||
		plan.InitialNodeDirectoryPath != directoryPath || plan.CombinedDigest == "" {
		t.Fatalf("incomplete plan=%+v", plan)
	}
	if err := writeDevPhysicalCatalogGenesisPlans(cluster); err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	second, err := os.ReadFile(config.PlanPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("identical physical plan changed on retry")
	}
}
