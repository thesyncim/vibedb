package main

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibejson"
)

var errInvalidBootstrapRF3Manifest = errors.New("vibedb-shard: invalid RF3 bootstrap manifest")

type bootstrapRF3Manifest struct {
	MemberManifest        string
	ControlListener       string
	SourceNode            rafttransport.NodeID
	SourceSnapshotAddress string
	RepositoryPath        string
	CursorPath            string
	JournalPath           string
	StaticBootstrapPath   string
	MaxArtifactBytes      uint64
}

func loadBootstrapRF3Manifest(path string) (bootstrapRF3Manifest, error) {
	raw, err := readRF3ManifestFile(path)
	if err != nil {
		return bootstrapRF3Manifest{}, errors.Join(errInvalidBootstrapRF3Manifest, err)
	}
	return parseBootstrapRF3Manifest(raw)
}

func parseBootstrapRF3Manifest(raw []byte) (bootstrapRF3Manifest, error) {
	if len(raw) == 0 || len(raw) > maxRF3ManifestBytes {
		return bootstrapRF3Manifest{}, errInvalidBootstrapRF3Manifest
	}
	document, err := vibejson.ParseOptions(raw, vibejson.Options{ZeroCopy: true, MaxDepth: 2})
	if err != nil {
		return bootstrapRF3Manifest{}, errors.Join(errInvalidBootstrapRF3Manifest, err)
	}
	fields, ok := document.Node().ObjectIter()
	if !ok {
		return bootstrapRF3Manifest{}, errInvalidBootstrapRF3Manifest
	}
	var manifest bootstrapRF3Manifest
	strings := []*string{
		&manifest.MemberManifest, &manifest.ControlListener,
		&manifest.SourceSnapshotAddress, &manifest.RepositoryPath,
		&manifest.CursorPath, &manifest.JournalPath, &manifest.StaticBootstrapPath,
	}
	names := [...]string{
		`"member_manifest"`, `"control_listener"`, `"source_snapshot_address"`,
		`"repository_path"`, `"cursor_path"`, `"journal_path"`,
		`"static_bootstrap_path"`,
	}
	for index := range strings {
		key, node, present := fields.Next()
		if !present || !bytes.Equal(key.Raw().Bytes(), []byte(names[index])) {
			return bootstrapRF3Manifest{}, errInvalidBootstrapRF3Manifest
		}
		value, valueErr := rf3ManifestString(node, maxRF3ManifestStringBytes)
		if valueErr != nil {
			return bootstrapRF3Manifest{}, errors.Join(errInvalidBootstrapRF3Manifest, valueErr)
		}
		*strings[index] = value
		if index == 1 {
			key, node, present = fields.Next()
			if !present || !bytes.Equal(key.Raw().Bytes(), []byte(`"source_node"`)) {
				return bootstrapRF3Manifest{}, errInvalidBootstrapRF3Manifest
			}
			manifest.SourceNode, valueErr = rf3ManifestNodeID(node)
			if valueErr != nil {
				return bootstrapRF3Manifest{}, errors.Join(errInvalidBootstrapRF3Manifest, valueErr)
			}
		}
	}
	key, node, present := fields.Next()
	if !present || !bytes.Equal(key.Raw().Bytes(), []byte(`"max_artifact_bytes"`)) {
		return bootstrapRF3Manifest{}, errInvalidBootstrapRF3Manifest
	}
	manifest.MaxArtifactBytes, err = rf3ManifestPositiveUint64(node)
	if err != nil {
		return bootstrapRF3Manifest{}, errors.Join(errInvalidBootstrapRF3Manifest, err)
	}
	if _, _, extra := fields.Next(); extra {
		return bootstrapRF3Manifest{}, errInvalidBootstrapRF3Manifest
	}
	return manifest, nil
}
