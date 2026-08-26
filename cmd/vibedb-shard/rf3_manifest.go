package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicaaction"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
	"github.com/thesyncim/vibejson"
)

const (
	maxRF3ManifestBytes       = 4 << 20
	maxRF3ManifestStringBytes = 4 << 10
	rf3ManifestMembers        = 3
	maxRF3ManifestGroups      = 64
)

var errInvalidRF3Manifest = errors.New("vibedb-shard: invalid RF3 manifest")

type rf3Manifest struct {
	WAL                 rf3ManifestWAL
	SQL                 rf3ManifestSQL
	Listeners           rf3ManifestListeners
	TLS                 rf3ManifestTLS
	AuthorizationPolicy string
	ReplicaControl      rf3ManifestReplicaControl
	Members             [rf3ManifestMembers]rf3ManifestMember
	EnrolledTarget      *rf3ManifestEnrolledTarget
	Groups              []rf3ManifestGroup
}

type rf3ManifestGroup struct {
	WAL            rf3ManifestWAL
	SQL            rf3ManifestSQL
	Members        [rf3ManifestMembers]rf3ManifestMember
	EnrolledTarget *rf3ManifestEnrolledTarget
}

func (manifest rf3Manifest) groupBundles() []rf3ManifestGroup {
	if len(manifest.Groups) != 0 {
		return manifest.Groups
	}
	return []rf3ManifestGroup{{WAL: manifest.WAL, SQL: manifest.SQL,
		Members: manifest.Members, EnrolledTarget: manifest.EnrolledTarget}}
}

func (manifest rf3Manifest) withGroup(group rf3ManifestGroup) rf3Manifest {
	return rf3Manifest{WAL: group.WAL, SQL: group.SQL, Listeners: manifest.Listeners,
		TLS: manifest.TLS, AuthorizationPolicy: manifest.AuthorizationPolicy,
		ReplicaControl: manifest.ReplicaControl,
		Members:        group.Members, EnrolledTarget: group.EnrolledTarget}
}

// rf3ManifestReplicaControl fixes every local disk and memory bound used by
// replica movement. Source fields are retained even before a target is
// enrolled so provisioning does not rewrite the control grammar mid-move.
type rf3ManifestReplicaControl struct {
	ActionJournalPath      string
	MaxActionRecords       int
	SourceDataRoot         string
	SourceJournalPath      string
	MaxSourceRecords       int
	SourceRepositoryPath   string
	MaxSourceArtifacts     int
	MaxSourceArtifactBytes uint64
	MaxSourceDiskBytes     uint64
	SourceChunkBytes       uint32
	MaxSourceConcurrent    int
}

type rf3ManifestWAL struct {
	Path            string
	KeyID           string
	KeyMaterialPath string
	Options         raftstore.Options
}

type rf3ManifestSQL struct {
	Path              string
	IdentityPath      string
	ApplyIdentityPath string
}

type rf3ManifestListeners struct {
	Peer     string `json:"peer"`
	Native   string `json:"native"`
	Snapshot string `json:"snapshot"`
	Control  string `json:"control"`
}

type rf3ManifestTLS struct {
	Certificate string `json:"certificate"`
	Key         string `json:"key"`
	Roots       string `json:"roots"`
	IdentityOID string `json:"identity_oid"`
}

type rf3ManifestMember struct {
	MemberID    uint64
	NodeID      rafttransport.NodeID
	PeerAddress string
}

// rf3ManifestEnrolledTarget is the one replacement identity provisioning has
// enrolled beside the stable serving RF3. Keeping it separate from Members is
// intentional: merely retaining this descriptor must not make the target a
// voter, a transport member, or a data-serving endpoint.
type rf3ManifestEnrolledTarget struct {
	MemberID        uint64
	NodeID          rafttransport.NodeID
	StoreID         [16]byte
	NodeIncarnation uint64
	PeerAddress     string
	NativeAddress   string
	SnapshotAddress string
	ControlAddress  string
}

// loadRF3Manifest reads one exact, bounded startup manifest. The grammar is
// deliberately order-sensitive: accepting aliases, reordered members, or
// escaped field names would give provisioning systems more than one spelling
// for the same serving authority.
func loadRF3Manifest(path string) (rf3Manifest, error) {
	data, err := readRF3ManifestFile(path)
	if err != nil {
		return rf3Manifest{}, err
	}
	return parseRF3Manifest(data)
}

func readRF3ManifestFile(path string) ([]byte, error) {
	if path == "" {
		return nil, errInvalidRF3Manifest
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Join(errInvalidRF3Manifest, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() <= 0 || info.Size() > maxRF3ManifestBytes {
		return nil, errors.Join(errInvalidRF3Manifest, err)
	}
	data := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, errors.Join(errInvalidRF3Manifest, err)
	}
	var trailing [1]byte
	if count, readErr := file.Read(trailing[:]); count != 0 || !errors.Is(readErr, io.EOF) {
		return nil, errInvalidRF3Manifest
	}
	return data, nil
}

func parseRF3Manifest(data []byte) (rf3Manifest, error) {
	if len(data) == 0 || len(data) > maxRF3ManifestBytes {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	document, err := vibejson.ParseOptions(data, vibejson.Options{ZeroCopy: true, MaxDepth: 5})
	if err != nil {
		return rf3Manifest{}, errors.Join(errInvalidRF3Manifest, err)
	}
	fields, ok := document.Node().ObjectIter()
	if !ok {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	var manifest rf3Manifest
	key, node, present := fields.Next()
	if !present {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	if bytes.Equal(key.Raw().Bytes(), []byte(`"listeners"`)) {
		if manifest.Listeners, err = parseRF3ManifestListeners(node); err != nil {
			return rf3Manifest{}, err
		}
		node, err = nextRF3Field(&fields, `"tls"`)
		if err != nil {
			return rf3Manifest{}, err
		}
		if manifest.TLS, err = parseRF3ManifestTLS(node); err != nil {
			return rf3Manifest{}, err
		}
		node, err = nextRF3Field(&fields, `"authorization_policy"`)
		if err != nil {
			return rf3Manifest{}, err
		}
		if manifest.AuthorizationPolicy, err = rf3ManifestString(node, maxRF3ManifestStringBytes); err != nil {
			return rf3Manifest{}, err
		}
		node, err = nextRF3Field(&fields, `"replica_control"`)
		if err != nil {
			return rf3Manifest{}, err
		}
		if manifest.ReplicaControl, err = parseRF3ManifestReplicaControl(node); err != nil {
			return rf3Manifest{}, err
		}
		node, err = nextRF3Field(&fields, `"groups"`)
		if err != nil {
			return rf3Manifest{}, err
		}
		if manifest.Groups, err = parseRF3ManifestGroups(node); err != nil {
			return rf3Manifest{}, err
		}
		controlPaths := [...]string{manifest.ReplicaControl.ActionJournalPath, manifest.ReplicaControl.SourceJournalPath, manifest.ReplicaControl.SourceRepositoryPath}
		for _, group := range manifest.Groups {
			for _, path := range [...]string{group.WAL.Path, group.WAL.KeyMaterialPath, group.SQL.Path, group.SQL.IdentityPath, group.SQL.ApplyIdentityPath} {
				for _, control := range controlPaths {
					if path == control {
						return rf3Manifest{}, errInvalidRF3Manifest
					}
				}
			}
		}
		if _, _, extra := fields.Next(); extra {
			return rf3Manifest{}, errInvalidRF3Manifest
		}
		return manifest, nil
	}
	if !bytes.Equal(key.Raw().Bytes(), []byte(`"wal"`)) {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	if manifest.WAL, err = parseRF3ManifestWAL(node); err != nil {
		return rf3Manifest{}, err
	}
	node, err = nextRF3Field(&fields, `"sql"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.SQL, err = parseRF3ManifestSQL(node); err != nil {
		return rf3Manifest{}, err
	}
	node, err = nextRF3Field(&fields, `"listeners"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.Listeners, err = parseRF3ManifestListeners(node); err != nil {
		return rf3Manifest{}, err
	}
	node, err = nextRF3Field(&fields, `"tls"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.TLS, err = parseRF3ManifestTLS(node); err != nil {
		return rf3Manifest{}, err
	}
	node, err = nextRF3Field(&fields, `"authorization_policy"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.AuthorizationPolicy, err = rf3ManifestString(node, maxRF3ManifestStringBytes); err != nil {
		return rf3Manifest{}, err
	}
	node, err = nextRF3Field(&fields, `"replica_control"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.ReplicaControl, err = parseRF3ManifestReplicaControl(node); err != nil {
		return rf3Manifest{}, err
	}
	node, err = nextRF3Field(&fields, `"members"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.Members, err = parseRF3ManifestMembers(node); err != nil {
		return rf3Manifest{}, err
	}
	key, node, present = fields.Next()
	if present {
		if !bytes.Equal(key.Raw().Bytes(), []byte(`"enrolled_target"`)) {
			return rf3Manifest{}, errInvalidRF3Manifest
		}
		target, err := parseRF3ManifestEnrolledTarget(node, manifest.Members)
		if err != nil {
			return rf3Manifest{}, err
		}
		manifest.EnrolledTarget = &target
	}
	if _, _, extra := fields.Next(); extra {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	return manifest, nil
}

func parseRF3ManifestGroups(node vibejson.Node) ([]rf3ManifestGroup, error) {
	count, ok := node.ArrayLen()
	if !ok || count < 1 || count > maxRF3ManifestGroups {
		return nil, errInvalidRF3Manifest
	}
	iter, _ := node.ArrayIter()
	groups := make([]rf3ManifestGroup, 0, count)
	paths := make(map[string]struct{}, count*5)
	nodes := make(map[rafttransport.NodeID]string, rf3ManifestMembers)
	addresses := make(map[string]rafttransport.NodeID, rf3ManifestMembers)
	for index := 0; index < count; index++ {
		value, present := iter.Next()
		if !present {
			return nil, errInvalidRF3Manifest
		}
		group, err := parseRF3ManifestGroup(value)
		if err != nil {
			return nil, err
		}
		for _, path := range [...]string{group.WAL.Path, group.WAL.KeyMaterialPath, group.SQL.Path, group.SQL.IdentityPath, group.SQL.ApplyIdentityPath} {
			if _, duplicate := paths[path]; duplicate {
				return nil, errInvalidRF3Manifest
			}
			paths[path] = struct{}{}
		}
		for _, member := range group.Members {
			if address, found := nodes[member.NodeID]; found && address != member.PeerAddress {
				return nil, errInvalidRF3Manifest
			}
			if node, found := addresses[member.PeerAddress]; found && node != member.NodeID {
				return nil, errInvalidRF3Manifest
			}
			nodes[member.NodeID], addresses[member.PeerAddress] = member.PeerAddress, member.NodeID
		}
		groups = append(groups, group)
	}
	if _, extra := iter.Next(); extra {
		return nil, errInvalidRF3Manifest
	}
	return groups, nil
}

func parseRF3ManifestGroup(node vibejson.Node) (rf3ManifestGroup, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestGroup{}, errInvalidRF3Manifest
	}
	var group rf3ManifestGroup
	value, err := nextRF3Field(&fields, `"wal"`)
	if err != nil {
		return group, err
	}
	if group.WAL, err = parseRF3ManifestWAL(value); err != nil {
		return group, err
	}
	value, err = nextRF3Field(&fields, `"sql"`)
	if err != nil {
		return group, err
	}
	if group.SQL, err = parseRF3ManifestSQL(value); err != nil {
		return group, err
	}
	value, err = nextRF3Field(&fields, `"members"`)
	if err != nil {
		return group, err
	}
	if group.Members, err = parseRF3ManifestMembers(value); err != nil {
		return group, err
	}
	key, value, present := fields.Next()
	if present {
		if !bytes.Equal(key.Raw().Bytes(), []byte(`"enrolled_target"`)) {
			return group, errInvalidRF3Manifest
		}
		target, err := parseRF3ManifestEnrolledTarget(value, group.Members)
		if err != nil {
			return group, err
		}
		group.EnrolledTarget = &target
	}
	if _, _, extra := fields.Next(); extra {
		return group, errInvalidRF3Manifest
	}
	return group, nil
}

func parseRF3ManifestReplicaControl(node vibejson.Node) (rf3ManifestReplicaControl, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
	}
	var result rf3ManifestReplicaControl
	stringsOut := []*string{
		&result.ActionJournalPath, &result.SourceDataRoot,
		&result.SourceJournalPath, &result.SourceRepositoryPath,
	}
	stringNames := [...]string{
		`"action_journal_path"`, `"source_data_root"`,
		`"source_journal_path"`, `"source_repository_path"`,
	}
	value, err := nextRF3Field(&fields, stringNames[0])
	if err != nil {
		return result, err
	}
	if *stringsOut[0], err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"max_action_records"`)
	if err != nil {
		return result, err
	}
	if result.MaxActionRecords, err = rf3ManifestPositiveInt(value); err != nil ||
		result.MaxActionRecords > replicaaction.AbsoluteMaxReplicaActionRecords {
		return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
	}
	for index := 1; index < len(stringsOut); index++ {
		value, err = nextRF3Field(&fields, stringNames[index])
		if err != nil {
			return result, err
		}
		if *stringsOut[index], err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
			return result, err
		}
		if index == 2 {
			value, err = nextRF3Field(&fields, `"max_source_records"`)
			if err != nil {
				return result, err
			}
			if result.MaxSourceRecords, err = rf3ManifestPositiveInt(value); err != nil ||
				result.MaxSourceRecords > snapshottransfer.AbsoluteMaxSourceRecords {
				return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
			}
		}
	}
	positiveInts := []*int{&result.MaxSourceArtifacts, &result.MaxSourceConcurrent}
	positiveIntNames := [...]string{`"max_source_artifacts"`, `"max_source_concurrent"`}
	for index := range positiveInts {
		value, err = nextRF3Field(&fields, positiveIntNames[index])
		if err != nil {
			return result, err
		}
		if *positiveInts[index], err = rf3ManifestPositiveInt(value); err != nil {
			return result, err
		}
	}
	uint64Out := []*uint64{&result.MaxSourceArtifactBytes, &result.MaxSourceDiskBytes}
	uint64Names := [...]string{`"max_source_artifact_bytes"`, `"max_source_disk_bytes"`}
	for index := range uint64Out {
		value, err = nextRF3Field(&fields, uint64Names[index])
		if err != nil {
			return result, err
		}
		if *uint64Out[index], err = rf3ManifestPositiveUint64(value); err != nil {
			return result, err
		}
	}
	value, err = nextRF3Field(&fields, `"source_chunk_bytes"`)
	if err != nil {
		return result, err
	}
	chunk, err := rf3ManifestPositiveUint64(value)
	if err != nil || chunk > math.MaxUint32 {
		return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
	}
	result.SourceChunkBytes = uint32(chunk)
	if _, _, extra := fields.Next(); extra ||
		result.MaxSourceArtifacts > 4096 ||
		result.MaxSourceConcurrent > snapshottransfer.AbsoluteMaxSourceConcurrency ||
		result.SourceChunkBytes < snapshottransfer.MinChunkBytes ||
		result.SourceChunkBytes > snapshottransfer.AbsoluteMaxChunkBytes ||
		result.MaxSourceArtifactBytes == 0 ||
		result.MaxSourceDiskBytes < result.MaxSourceArtifactBytes {
		return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
	}
	paths := [...]string{
		result.ActionJournalPath, result.SourceDataRoot,
		result.SourceJournalPath, result.SourceRepositoryPath,
	}
	for index, path := range paths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
		}
		for prior := 0; prior < index; prior++ {
			if path == paths[prior] {
				return rf3ManifestReplicaControl{}, errInvalidRF3Manifest
			}
		}
	}
	return result, nil
}

func parseRF3ManifestWAL(node vibejson.Node) (rf3ManifestWAL, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestWAL{}, errInvalidRF3Manifest
	}
	var result rf3ManifestWAL
	value, err := nextRF3Field(&fields, `"path"`)
	if err != nil {
		return result, err
	}
	if result.Path, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"key_id"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.KeyID, err = rf3ManifestString(value, raftstore.MaxKeyIDBytes); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"key_material_path"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.KeyMaterialPath, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"max_file_bytes"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.Options.MaxFileBytes, err = rf3ManifestPositiveInt64(value); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"max_record_bytes"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.Options.MaxRecordBytes, err = rf3ManifestPositiveInt(value); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"max_records"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.Options.MaxRecords, err = rf3ManifestPositiveUint64(value); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"max_entries"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.Options.MaxEntries, err = rf3ManifestPositiveUint64(value); err != nil {
		return rf3ManifestWAL{}, err
	}
	value, err = nextRF3Field(&fields, `"max_live_bytes"`)
	if err != nil {
		return rf3ManifestWAL{}, err
	}
	if result.Options.MaxLiveBytes, err = rf3ManifestPositiveInt64(value); err != nil {
		return rf3ManifestWAL{}, err
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestWAL{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestSQL(node vibejson.Node) (rf3ManifestSQL, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestSQL{}, errInvalidRF3Manifest
	}
	var result rf3ManifestSQL
	values := []*string{&result.Path, &result.IdentityPath, &result.ApplyIdentityPath}
	names := [...]string{`"path"`, `"identity_path"`, `"apply_identity_path"`}
	for index := range names {
		value, err := nextRF3Field(&fields, names[index])
		if err != nil {
			return rf3ManifestSQL{}, err
		}
		if *values[index], err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
			return rf3ManifestSQL{}, err
		}
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestSQL{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestListeners(node vibejson.Node) (rf3ManifestListeners, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestListeners{}, errInvalidRF3Manifest
	}
	var result rf3ManifestListeners
	value, err := nextRF3Field(&fields, `"peer"`)
	if err != nil {
		return result, err
	}
	if result.Peer, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestListeners{}, err
	}
	value, err = nextRF3Field(&fields, `"native"`)
	if err != nil {
		return rf3ManifestListeners{}, err
	}
	if result.Native, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestListeners{}, err
	}
	value, err = nextRF3Field(&fields, `"snapshot"`)
	if err != nil {
		return result, err
	}
	if result.Snapshot, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestListeners{}, err
	}
	value, err = nextRF3Field(&fields, `"control"`)
	if err != nil {
		return result, err
	}
	if result.Control, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestListeners{}, err
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestListeners{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestTLS(node vibejson.Node) (rf3ManifestTLS, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestTLS{}, errInvalidRF3Manifest
	}
	var result rf3ManifestTLS
	values := []*string{&result.Certificate, &result.Key, &result.Roots, &result.IdentityOID}
	names := [...]string{`"certificate"`, `"key"`, `"roots"`, `"identity_oid"`}
	for index := range names {
		value, err := nextRF3Field(&fields, names[index])
		if err != nil {
			return rf3ManifestTLS{}, err
		}
		if *values[index], err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
			return rf3ManifestTLS{}, err
		}
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestTLS{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestMembers(node vibejson.Node) ([rf3ManifestMembers]rf3ManifestMember, error) {
	var result [rf3ManifestMembers]rf3ManifestMember
	count, ok := node.ArrayLen()
	if !ok || count != len(result) {
		return result, errInvalidRF3Manifest
	}
	members, _ := node.ArrayIter()
	for index := range result {
		node, present := members.Next()
		if !present {
			return [rf3ManifestMembers]rf3ManifestMember{}, errInvalidRF3Manifest
		}
		member, err := parseRF3ManifestMember(node)
		if err != nil || index > 0 && member.MemberID <= result[index-1].MemberID {
			return [rf3ManifestMembers]rf3ManifestMember{}, errInvalidRF3Manifest
		}
		for prior := 0; prior < index; prior++ {
			if member.NodeID == result[prior].NodeID || member.PeerAddress == result[prior].PeerAddress {
				return [rf3ManifestMembers]rf3ManifestMember{}, errInvalidRF3Manifest
			}
		}
		result[index] = member
	}
	if _, extra := members.Next(); extra {
		return [rf3ManifestMembers]rf3ManifestMember{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestMember(node vibejson.Node) (rf3ManifestMember, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestMember{}, errInvalidRF3Manifest
	}
	var result rf3ManifestMember
	value, err := nextRF3Field(&fields, `"member_id"`)
	if err != nil {
		return result, err
	}
	if result.MemberID, err = rf3ManifestPositiveUint64(value); err != nil {
		return rf3ManifestMember{}, err
	}
	value, err = nextRF3Field(&fields, `"node_id"`)
	if err != nil {
		return rf3ManifestMember{}, err
	}
	if result.NodeID, err = rf3ManifestNodeID(value); err != nil {
		return rf3ManifestMember{}, err
	}
	value, err = nextRF3Field(&fields, `"peer_address"`)
	if err != nil {
		return rf3ManifestMember{}, err
	}
	if result.PeerAddress, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return rf3ManifestMember{}, err
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestMember{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestEnrolledTarget(
	node vibejson.Node,
	members [rf3ManifestMembers]rf3ManifestMember,
) (rf3ManifestEnrolledTarget, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestEnrolledTarget{}, errInvalidRF3Manifest
	}
	var result rf3ManifestEnrolledTarget
	value, err := nextRF3Field(&fields, `"member_id"`)
	if err != nil {
		return result, err
	}
	if result.MemberID, err = rf3ManifestPositiveUint64(value); err != nil {
		return rf3ManifestEnrolledTarget{}, err
	}
	value, err = nextRF3Field(&fields, `"node_id"`)
	if err != nil {
		return rf3ManifestEnrolledTarget{}, err
	}
	if result.NodeID, err = rf3ManifestNodeID(value); err != nil {
		return rf3ManifestEnrolledTarget{}, err
	}
	value, err = nextRF3Field(&fields, `"store_id"`)
	if err != nil {
		return result, err
	}
	store, err := rf3ManifestNodeID(value)
	if err != nil {
		return result, err
	}
	result.StoreID = [16]byte(store)
	value, err = nextRF3Field(&fields, `"node_incarnation"`)
	if err != nil {
		return result, err
	}
	if result.NodeIncarnation, err = rf3ManifestPositiveUint64(value); err != nil {
		return result, err
	}
	values := []*string{
		&result.PeerAddress, &result.NativeAddress,
		&result.SnapshotAddress, &result.ControlAddress,
	}
	names := [...]string{
		`"peer_address"`, `"native_address"`, `"snapshot_address"`, `"control_address"`,
	}
	for index := range names {
		value, err = nextRF3Field(&fields, names[index])
		if err != nil {
			return rf3ManifestEnrolledTarget{}, err
		}
		if *values[index], err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
			return rf3ManifestEnrolledTarget{}, err
		}
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestEnrolledTarget{}, errInvalidRF3Manifest
	}
	for _, member := range members {
		if result.MemberID == member.MemberID || result.NodeID == member.NodeID {
			return rf3ManifestEnrolledTarget{}, errInvalidRF3Manifest
		}
	}
	addresses := [...]string{
		result.PeerAddress, result.NativeAddress, result.SnapshotAddress, result.ControlAddress,
	}
	for index := range addresses {
		for _, member := range members {
			if addresses[index] == member.PeerAddress {
				return rf3ManifestEnrolledTarget{}, errInvalidRF3Manifest
			}
		}
		for prior := 0; prior < index; prior++ {
			if addresses[index] == addresses[prior] {
				return rf3ManifestEnrolledTarget{}, errInvalidRF3Manifest
			}
		}
	}
	return result, nil
}

func nextRF3Field(fields *vibejson.ObjectIter, canonical string) (vibejson.Node, error) {
	key, value, ok := fields.Next()
	if !ok || !bytes.Equal(key.Raw().Bytes(), []byte(canonical)) {
		return vibejson.Node{}, errInvalidRF3Manifest
	}
	return value, nil
}

func rf3ManifestString(node vibejson.Node, maximum int) (string, error) {
	value, ok := node.StringBytes()
	if !ok || len(value) == 0 || len(value) > maximum || bytes.IndexByte(value, 0) >= 0 {
		return "", errInvalidRF3Manifest
	}
	return strings.Clone(string(value)), nil
}

func rf3ManifestPositiveUint64(node vibejson.Node) (uint64, error) {
	value, ok := node.Uint64()
	if !ok || value == 0 {
		return 0, errInvalidRF3Manifest
	}
	return value, nil
}

func rf3ManifestPositiveInt64(node vibejson.Node) (int64, error) {
	value, err := rf3ManifestPositiveUint64(node)
	if err != nil || value > math.MaxInt64 {
		return 0, errInvalidRF3Manifest
	}
	return int64(value), nil
}

func rf3ManifestPositiveInt(node vibejson.Node) (int, error) {
	value, err := rf3ManifestPositiveUint64(node)
	if err != nil || value > uint64(math.MaxInt) {
		return 0, errInvalidRF3Manifest
	}
	return int(value), nil
}

func rf3ManifestNodeID(node vibejson.Node) (rafttransport.NodeID, error) {
	var result rafttransport.NodeID
	raw := node.Raw().Bytes()
	if len(raw) != 2+hex.EncodedLen(len(result)) || raw[0] != '"' || raw[len(raw)-1] != '"' {
		return result, errInvalidRF3Manifest
	}
	encoded := raw[1 : len(raw)-1]
	for _, character := range encoded {
		if character >= '0' && character <= '9' || character >= 'a' && character <= 'f' {
			continue
		}
		return rafttransport.NodeID{}, errInvalidRF3Manifest
	}
	if _, err := hex.Decode(result[:], encoded); err != nil || result == (rafttransport.NodeID{}) {
		return rafttransport.NodeID{}, errInvalidRF3Manifest
	}
	return result, nil
}
