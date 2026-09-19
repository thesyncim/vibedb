package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibejson"
)

const devCatalogGenesisPlanFormat = 1

// devCatalogGenesisPlan is an immutable witness over the two operator input
// files consumed by the private storage-side genesis lane. The route and
// local identity remain in devCatalogGenesisConfig; ConfigDigest binds those
// fields to this sidecar so changing either input cannot reuse a prior plan.
type devCatalogGenesisPlan struct {
	Format                     uint16 `json:"format"`
	CatalogPath                string `json:"catalog_path"`
	InitialNodeDirectoryPath   string `json:"initial_node_directory"`
	CatalogDigest              string `json:"catalog_digest"`
	InitialNodeDirectoryDigest string `json:"initial_node_directory_digest"`
	ConfigDigest               string `json:"config_digest"`
	CombinedDigest             string `json:"combined_digest"`
}

func writeDevPhysicalCatalogGenesisPlans(cluster devClusterManifest) error {
	if cluster.CatalogPath == "" || len(cluster.NodeManifests) == 0 {
		return errDevCluster
	}
	catalogRaw, err := readDevFile(cluster.CatalogPath, gateway.RestoreCatalogReadAdmissionBytes)
	if err != nil {
		return err
	}
	snapshot, err := gateway.LoadSnapshot(cluster.CatalogPath)
	if err != nil || snapshot.Generation() != 1 {
		return errors.Join(errDevCluster, err)
	}
	directoryPath := filepath.Join(filepath.Dir(cluster.CatalogPath), "initial-node-directory.vibejson")
	directoryRaw, err := readDevFile(directoryPath, 4<<20)
	if err != nil {
		return err
	}
	var records []gateway.NodeRecord
	if err := vibejson.Unmarshal(directoryRaw, &records); err != nil || len(records) == 0 {
		return errors.Join(errDevCluster, err)
	}
	canonicalDirectory, err := vibejson.Marshal(&records)
	if err != nil || !bytes.Equal(canonicalDirectory, directoryRaw) {
		return errors.Join(errDevCluster, err)
	}
	if _, err := gateway.BuildReplicatedCatalogGenesisMutations(snapshot, records); err != nil {
		return errors.Join(errDevCluster, err)
	}

	catalogDigest := sha256.Sum256(catalogRaw)
	directoryDigest := sha256.Sum256(directoryRaw)
	combinedDigest := devCatalogGenesisCombinedDigest(catalogRaw, directoryRaw)
	for index, node := range cluster.NodeManifests {
		inputPath := filepath.Dir(node.ServeManifest) + ".prepare-node.vibejson"
		inputRaw, err := readDevFile(inputPath, 4<<20)
		if err != nil {
			return err
		}
		var input devPrepareNodeManifest
		if err := vibejson.Unmarshal(inputRaw, &input); err != nil {
			return errors.Join(errDevCluster, err)
		}
		config := input.CatalogGenesis
		if config == nil {
			continue
		}
		if err := validateDevCatalogGenesisConfig(cluster, index, node, config, directoryPath); err != nil {
			return err
		}
		configRaw, err := vibejson.Marshal(config)
		if err != nil {
			return err
		}
		configDigest := sha256.Sum256(configRaw)
		plan := devCatalogGenesisPlan{
			Format:                     devCatalogGenesisPlanFormat,
			CatalogPath:                cluster.CatalogPath,
			InitialNodeDirectoryPath:   directoryPath,
			CatalogDigest:              hex.EncodeToString(catalogDigest[:]),
			InitialNodeDirectoryDigest: hex.EncodeToString(directoryDigest[:]),
			ConfigDigest:               hex.EncodeToString(configDigest[:]),
			CombinedDigest:             hex.EncodeToString(combinedDigest[:]),
		}
		planRaw, err := vibejson.Marshal(&plan)
		if err != nil {
			return err
		}
		if config.PlanPath == "" {
			return errDevCluster
		}
		if err := writeDevFileOnce(config.PlanPath, planRaw); err != nil {
			return err
		}
	}
	return syncDevDir(filepath.Dir(cluster.CatalogPath))
}

func validateDevCatalogGenesisConfig(
	cluster devClusterManifest,
	index int,
	node devPhysicalNode,
	config *devCatalogGenesisConfig,
	directoryPath string,
) error {
	if config == nil || index < 0 || index >= len(cluster.NodeManifests) ||
		config.CatalogPath != cluster.CatalogPath ||
		config.InitialNodeDirectoryPath != directoryPath ||
		!filepath.IsAbs(config.PlanPath) || !filepath.IsAbs(config.SessionJournal) ||
		config.ClientID != node.Node || config.NodeID != node.Node ||
		config.Distribution != string(gateway.ReplicatedCatalogDistribution) ||
		config.Shard != string(gateway.ReplicatedCatalogShard) || config.Relation == 0 ||
		config.NodeIncarnation == 0 || config.MemberID == 0 || config.StoreID == "" ||
		config.RetryHome == "" || config.ClusterID == "" || config.ClusterIncarnation == "" ||
		config.ShardIncarnation == "" || config.GroupID == "" ||
		config.TopologyRecoveryEpoch == 0 || config.AllocationGeneration == 0 {
		return fmt.Errorf("%w: invalid catalog genesis config for node %d", errDevCluster, index+1)
	}
	found := false
	for _, member := range cluster.Members {
		if member.Node != node.Node {
			continue
		}
		if found || member.Member != config.MemberID || member.Store != config.StoreID {
			return fmt.Errorf("%w: catalog genesis member mismatch for node %d", errDevCluster, index+1)
		}
		found = true
	}
	if !found {
		return fmt.Errorf("%w: non-voter catalog genesis config for node %d", errDevCluster, index+1)
	}
	return nil
}

func devCatalogGenesisCombinedDigest(catalog, directory []byte) [32]byte {
	hash := sha256.New()
	hash.Write([]byte("vibedb/catalog-genesis-plan\x00"))
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(catalog)))
	hash.Write(length[:])
	hash.Write(catalog)
	binary.BigEndian.PutUint64(length[:], uint64(len(directory)))
	hash.Write(length[:])
	hash.Write(directory)
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}
