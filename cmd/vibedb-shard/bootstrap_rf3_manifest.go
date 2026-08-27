package main

import (
	"bytes"
	"encoding/hex"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibejson"
)

var errInvalidBootstrapRF3Manifest = errors.New("vibedb-shard: invalid RF3 bootstrap manifest")

const maxBootstrapRF3ManifestGroups = 64

type bootstrapRF3Manifest struct {
	MemberManifest        string
	ControlListener       string
	SourceNode            rafttransport.NodeID
	SourceSnapshotAddress string
	RepositoryPath        string
	CursorPath            string
	JournalPath           string
	StaticBootstrapPath   string
	WALWrappedKey         []byte
	MaxArtifactBytes      uint64
	Groups                []bootstrapRF3ManifestGroup
}

type bootstrapRF3ManifestGroup struct {
	MemberManifest        string
	SourceNode            rafttransport.NodeID
	SourceSnapshotAddress string
	RepositoryPath        string
	CursorPath            string
	JournalPath           string
	StaticBootstrapPath   string
	WALWrappedKey         []byte
	MaxArtifactBytes      uint64
}

func (manifest bootstrapRF3Manifest) groupBundles() []bootstrapRF3ManifestGroup {
	if len(manifest.Groups) != 0 {
		return manifest.Groups
	}
	return []bootstrapRF3ManifestGroup{{
		MemberManifest: manifest.MemberManifest, SourceNode: manifest.SourceNode,
		SourceSnapshotAddress: manifest.SourceSnapshotAddress,
		RepositoryPath:        manifest.RepositoryPath, CursorPath: manifest.CursorPath,
		JournalPath: manifest.JournalPath, StaticBootstrapPath: manifest.StaticBootstrapPath,
		MaxArtifactBytes: manifest.MaxArtifactBytes, WALWrappedKey: manifest.WALWrappedKey,
	}}
}

func (manifest bootstrapRF3Manifest) withGroup(group bootstrapRF3ManifestGroup) bootstrapRF3Manifest {
	return bootstrapRF3Manifest{
		MemberManifest: group.MemberManifest, ControlListener: manifest.ControlListener,
		SourceNode: group.SourceNode, SourceSnapshotAddress: group.SourceSnapshotAddress,
		RepositoryPath: group.RepositoryPath, CursorPath: group.CursorPath,
		JournalPath: group.JournalPath, StaticBootstrapPath: group.StaticBootstrapPath,
		MaxArtifactBytes: group.MaxArtifactBytes, WALWrappedKey: group.WALWrappedKey,
	}
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
	document, err := vibejson.ParseOptions(raw, vibejson.Options{ZeroCopy: true, MaxDepth: 3})
	if err != nil {
		return bootstrapRF3Manifest{}, errors.Join(errInvalidBootstrapRF3Manifest, err)
	}
	fields, ok := document.Node().ObjectIter()
	if !ok {
		return bootstrapRF3Manifest{}, errInvalidBootstrapRF3Manifest
	}
	key, node, present := fields.Next()
	if !present {
		return bootstrapRF3Manifest{}, errInvalidBootstrapRF3Manifest
	}
	var manifest bootstrapRF3Manifest
	if bytes.Equal(key.Raw().Bytes(), []byte(`"control_listener"`)) {
		if manifest.ControlListener, err = rf3ManifestString(node, maxRF3ManifestStringBytes); err != nil {
			return bootstrapRF3Manifest{}, errors.Join(errInvalidBootstrapRF3Manifest, err)
		}
		key, node, present = fields.Next()
		if !present || !bytes.Equal(key.Raw().Bytes(), []byte(`"groups"`)) {
			return bootstrapRF3Manifest{}, errInvalidBootstrapRF3Manifest
		}
		if manifest.Groups, err = parseBootstrapRF3ManifestGroups(node); err != nil {
			return bootstrapRF3Manifest{}, errors.Join(errInvalidBootstrapRF3Manifest, err)
		}
	} else {
		if !bytes.Equal(key.Raw().Bytes(), []byte(`"member_manifest"`)) {
			return bootstrapRF3Manifest{}, errInvalidBootstrapRF3Manifest
		}
		var group bootstrapRF3ManifestGroup
		if group.MemberManifest, err = rf3ManifestString(node, maxRF3ManifestStringBytes); err != nil {
			return bootstrapRF3Manifest{}, errors.Join(errInvalidBootstrapRF3Manifest, err)
		}
		key, node, present = fields.Next()
		if !present || !bytes.Equal(key.Raw().Bytes(), []byte(`"control_listener"`)) {
			return bootstrapRF3Manifest{}, errInvalidBootstrapRF3Manifest
		}
		if manifest.ControlListener, err = rf3ManifestString(node, maxRF3ManifestStringBytes); err != nil {
			return bootstrapRF3Manifest{}, errors.Join(errInvalidBootstrapRF3Manifest, err)
		}
		if group, err = parseBootstrapRF3ManifestGroupFields(&fields, group); err != nil {
			return bootstrapRF3Manifest{}, errors.Join(errInvalidBootstrapRF3Manifest, err)
		}
		manifest = manifest.withGroup(group)
	}
	if _, _, extra := fields.Next(); extra {
		return bootstrapRF3Manifest{}, errInvalidBootstrapRF3Manifest
	}
	return manifest, nil
}

func parseBootstrapRF3ManifestGroups(node vibejson.Node) ([]bootstrapRF3ManifestGroup, error) {
	count, ok := node.ArrayLen()
	if !ok || count < 1 || count > maxBootstrapRF3ManifestGroups {
		return nil, errInvalidBootstrapRF3Manifest
	}
	groups := make([]bootstrapRF3ManifestGroup, 0, count)
	values, _ := node.ArrayIter()
	for index := 0; index < count; index++ {
		node, present := values.Next()
		if !present {
			return nil, errInvalidBootstrapRF3Manifest
		}
		fields, ok := node.ObjectIter()
		if !ok {
			return nil, errInvalidBootstrapRF3Manifest
		}
		key, value, present := fields.Next()
		if !present || !bytes.Equal(key.Raw().Bytes(), []byte(`"member_manifest"`)) {
			return nil, errInvalidBootstrapRF3Manifest
		}
		memberManifest, err := rf3ManifestString(value, maxRF3ManifestStringBytes)
		if err != nil {
			return nil, err
		}
		group, err := parseBootstrapRF3ManifestGroupFields(&fields, bootstrapRF3ManifestGroup{
			MemberManifest: memberManifest,
		})
		if err != nil {
			return nil, err
		}
		groups = append(groups, group)
	}
	if _, extra := values.Next(); extra {
		return nil, errInvalidBootstrapRF3Manifest
	}
	return groups, nil
}

func parseBootstrapRF3ManifestGroupFields(fields *vibejson.ObjectIter, group bootstrapRF3ManifestGroup) (bootstrapRF3ManifestGroup, error) {
	key, node, present := fields.Next()
	if !present || !bytes.Equal(key.Raw().Bytes(), []byte(`"source_node"`)) {
		return bootstrapRF3ManifestGroup{}, errInvalidBootstrapRF3Manifest
	}
	var err error
	if group.SourceNode, err = rf3ManifestNodeID(node); err != nil {
		return bootstrapRF3ManifestGroup{}, err
	}
	strings := []*string{
		&group.SourceSnapshotAddress, &group.RepositoryPath, &group.CursorPath,
		&group.JournalPath, &group.StaticBootstrapPath,
	}
	names := [...]string{
		`"source_snapshot_address"`, `"repository_path"`, `"cursor_path"`,
		`"journal_path"`, `"static_bootstrap_path"`,
	}
	for index := range strings {
		key, node, present = fields.Next()
		if !present || !bytes.Equal(key.Raw().Bytes(), []byte(names[index])) {
			return bootstrapRF3ManifestGroup{}, errInvalidBootstrapRF3Manifest
		}
		if *strings[index], err = rf3ManifestString(node, maxRF3ManifestStringBytes); err != nil {
			return bootstrapRF3ManifestGroup{}, err
		}
	}
	key, node, present = fields.Next()
	if !present || !bytes.Equal(key.Raw().Bytes(), []byte(`"wal_wrapped_key"`)) {
		return bootstrapRF3ManifestGroup{}, errInvalidBootstrapRF3Manifest
	}
	wrapped, err := rf3ManifestString(node, 2*raftstore.MaxWrappedKeyBytes)
	if err != nil {
		return bootstrapRF3ManifestGroup{}, err
	}
	group.WALWrappedKey, err = hex.DecodeString(wrapped)
	if err != nil || len(group.WALWrappedKey) == 0 || hex.EncodeToString(group.WALWrappedKey) != wrapped {
		return bootstrapRF3ManifestGroup{}, errInvalidBootstrapRF3Manifest
	}
	key, node, present = fields.Next()
	if !present || !bytes.Equal(key.Raw().Bytes(), []byte(`"max_artifact_bytes"`)) {
		return bootstrapRF3ManifestGroup{}, errInvalidBootstrapRF3Manifest
	}
	if group.MaxArtifactBytes, err = rf3ManifestPositiveUint64(node); err != nil {
		return bootstrapRF3ManifestGroup{}, err
	}
	if _, _, extra := fields.Next(); extra {
		return bootstrapRF3ManifestGroup{}, errInvalidBootstrapRF3Manifest
	}
	return group, nil
}
