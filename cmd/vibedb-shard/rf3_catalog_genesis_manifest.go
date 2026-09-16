package main

import (
	"bytes"
	"encoding/hex"
	"math"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibejson"
)

// rf3CatalogGenesisConfig is the physical-only, closed provisioning lane
// carried beside the optional embedded gateway. Its sidecar is produced by
// cluster preparation after the catalog and initial node directory exist;
// runtime validates that witness before allowing the private initializer to
// use the route.
type rf3CatalogGenesisConfig struct {
	PlanPath                 string `json:"plan_path"`
	CatalogPath              string `json:"catalog_path"`
	InitialNodeDirectoryPath string `json:"initial_node_directory"`
	SessionJournal           string `json:"session_journal"`
	ClientID                 string `json:"client_id"`
	RetryHome                string `json:"retry_home"`
	Distribution             string `json:"distribution"`
	Shard                    string `json:"shard"`
	ClusterID                string `json:"cluster_id"`
	ClusterIncarnation       string `json:"cluster_incarnation"`
	TopologyRecoveryEpoch    uint64 `json:"topology_recovery_epoch"`
	AllocationGeneration     uint64 `json:"allocation_generation"`
	ShardIncarnation         string `json:"shard_incarnation"`
	GroupID                  string `json:"group_id"`
	MemberID                 uint64 `json:"member_id"`
	StoreID                  string `json:"store_id"`
	NodeID                   string `json:"node_id"`
	NodeIncarnation          uint64 `json:"node_incarnation"`
	Relation                 uint64 `json:"relation"`
}

func parseRF3ManifestCatalogGenesis(node vibejson.Node) (*rf3CatalogGenesisConfig, error) {
	raw := node.Raw().Bytes()
	var result rf3CatalogGenesisConfig
	if err := vibejson.Unmarshal(raw, &result); err != nil {
		return nil, errInvalidRF3Manifest
	}
	canonical, err := vibejson.Marshal(&result)
	if err != nil || !bytes.Equal(canonical, raw) {
		return nil, errInvalidRF3Manifest
	}
	if !validRF3CatalogGenesisConfig(result) {
		return nil, errInvalidRF3Manifest
	}
	return &result, nil
}

func validRF3CatalogGenesisConfig(config rf3CatalogGenesisConfig) bool {
	paths := [...]string{config.PlanPath, config.CatalogPath, config.InitialNodeDirectoryPath, config.SessionJournal}
	for _, path := range paths {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return false
		}
	}
	if config.PlanPath == config.CatalogPath || config.PlanPath == config.InitialNodeDirectoryPath ||
		config.PlanPath == config.SessionJournal || config.CatalogPath == config.InitialNodeDirectoryPath ||
		config.CatalogPath == config.SessionJournal || config.InitialNodeDirectoryPath == config.SessionJournal ||
		config.ClientID == "" || config.RetryHome == "" || config.Distribution == "" || config.Shard == "" ||
		config.TopologyRecoveryEpoch == 0 || config.AllocationGeneration == 0 || config.MemberID == 0 ||
		config.NodeIncarnation == 0 || config.Relation == 0 || config.Relation > math.MaxUint16 {
		return false
	}
	for _, value := range []string{config.ClusterID, config.ClusterIncarnation, config.ShardIncarnation, config.GroupID, config.StoreID, config.NodeID} {
		if !validRF3FixedHex(value, 16) {
			return false
		}
	}
	return validRF3FixedHex(config.ClientID, 16) && validRF3FixedHex(config.RetryHome, 8)
}

func validRF3FixedHex(value string, bytesCount int) bool {
	if len(value) != bytesCount*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == bytesCount && !bytes.Equal(decoded, make([]byte, bytesCount)) && hex.EncodeToString(decoded) == value
}

func (config rf3CatalogGenesisConfig) nodeID() (rafttransport.NodeID, bool) {
	var result rafttransport.NodeID
	if !decodeRF3FixedHex(config.NodeID, result[:], false) {
		return result, false
	}
	return result, true
}
