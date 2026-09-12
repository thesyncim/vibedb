package main

import (
	"bytes"
	"errors"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/migrationbudget"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibejson"
)

// The node log is an explicit process-level durability choice. Group routes and
// SQL identities still describe independent replicated members; their former
// range-WAL paths are never opened in this mode.
type rf3NodeLogManifest struct {
	Format uint16 `json:"format"`
	Path   string `json:"path"`
	KeyID  string `json:"key_id"`
	// WrappedKey is opaque key-provider metadata retained by a newly-created
	// physical node log.  Existing manifests may omit it because a restart
	// can recover the metadata from the authenticated node log header.
	WrappedKey      string                     `json:"wrapped_key,omitempty"`
	KeyMaterialPath string                     `json:"key_material_path"`
	Options         raftstore.NodeStoreOptions `json:"options"`
}

func parseRF3NodeLogManifest(node vibejson.Node) (*rf3NodeLogManifest, error) {
	var result rf3NodeLogManifest
	raw := node.Raw().Bytes()
	if err := vibejson.Unmarshal(raw, &result); err != nil {
		return nil, errors.Join(errInvalidRF3Manifest, err)
	}
	canonical, err := vibejson.Marshal(&result)
	if err != nil || !bytes.Equal(raw, canonical) || result.Format != 1 || result.KeyID == "" || len(result.KeyID) > maxRF3ManifestStringBytes {
		return nil, errInvalidRF3Manifest
	}
	for _, path := range []string{result.Path, result.KeyMaterialPath} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || len(path) > maxRF3ManifestStringBytes {
			return nil, errInvalidRF3Manifest
		}
	}
	if result.Path == result.KeyMaterialPath {
		return nil, errInvalidRF3Manifest
	}
	return &result, nil
}

func openRF3NodeOwner(manifest rf3Manifest, profile *rafttransport.PeerTLS, budgets ...*migrationbudget.Budget) (*rf3NodeOwner, error) {
	if manifest.NodeLog == nil {
		return nil, nil
	}
	config := manifest.NodeLog
	key, err := loadRF3WALKey(config.KeyID, config.KeyMaterialPath)
	if err != nil {
		return nil, err
	}
	defer clear(key.Material[:])
	local := profile.LocalIdentity()
	identity := raftstore.NodeIdentity{ClusterID: local.TrustDomain.ClusterID, ClusterIncarnation: local.TrustDomain.ClusterIncarnation, NodeID: [16]byte(local.Node)}
	store, err := raftstore.OpenNodeStore(config.Path, identity, key, config.Options)
	if err != nil {
		return nil, err
	}
	owner, err := newRF3NodeOwner(store, budgets...)
	if err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return owner, nil
}
