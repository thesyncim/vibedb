package main

import (
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibejson"
)

// Canonical command-boundary types match the shard's fresh node preparation
// grammar. The shared geometry is independent from legacy per-range WAL bounds.
type devNodeLogManifest struct {
	Format          uint16                     `json:"format"`
	Path            string                     `json:"path"`
	KeyID           string                     `json:"key_id"`
	KeyMaterialPath string                     `json:"key_material_path"`
	Options         raftstore.NodeStoreOptions `json:"options"`
}

type devPrepareNodeManifest struct {
	Root    string               `json:"root"`
	NodeLog devNodeLogManifest   `json:"node_log"`
	Groups  []devPrepareManifest `json:"groups"`
}

func prepareDevNode(binary, memberPreparation string) error {
	raw, err := readDevFile(memberPreparation, 1<<20)
	if err != nil {
		return err
	}
	var group devPrepareManifest
	if err := vibejson.Unmarshal(raw, &group); err != nil {
		return err
	}
	if group.DevelopmentOnly || filepath.Base(group.Root) != "group-0" {
		return errDevCluster
	}
	root := filepath.Dir(group.Root)
	node := devPrepareNodeManifest{Root: root, NodeLog: devNodeLogManifest{Format: 1, Path: filepath.Join(root, "node-log"), KeyID: group.WAL.KeyID, KeyMaterialPath: group.WAL.KeyMaterialPath, Options: raftstore.NodeStoreOptions{MaxGroups: 64}}, Groups: []devPrepareManifest{group}}
	raw, err = vibejson.Marshal(&node)
	if err != nil {
		return err
	}
	path := memberPreparation + ".node"
	if err := writeDevFileOnce(path, raw); err != nil {
		return err
	}
	return runDevCommand(binary, "prepare-node-rf3", "-manifest", path)
}
