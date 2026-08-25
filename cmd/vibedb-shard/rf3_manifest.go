package main

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"os"
	"strings"

	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibejson"
)

const (
	maxRF3ManifestBytes       = 64 << 10
	maxRF3ManifestStringBytes = 4 << 10
	rf3ManifestMembers        = 3
)

var errInvalidRF3Manifest = errors.New("vibedb-shard: invalid RF3 manifest")

type rf3Manifest struct {
	WAL                 rf3ManifestWAL
	SQL                 rf3ManifestSQL
	Listeners           rf3ManifestListeners
	TLS                 rf3ManifestTLS
	AuthorizationPolicy string
	Members             [rf3ManifestMembers]rf3ManifestMember
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
	Peer   string
	Native string
}

type rf3ManifestTLS struct {
	Certificate string
	Key         string
	Roots       string
	IdentityOID string
}

type rf3ManifestMember struct {
	MemberID    uint64
	NodeID      rafttransport.NodeID
	PeerAddress string
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
	document, err := vibejson.ParseOptions(data, vibejson.Options{ZeroCopy: true, MaxDepth: 4})
	if err != nil {
		return rf3Manifest{}, errors.Join(errInvalidRF3Manifest, err)
	}
	fields, ok := document.Node().ObjectIter()
	if !ok {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	var manifest rf3Manifest

	node, err := nextRF3Field(&fields, `"wal"`)
	if err != nil {
		return rf3Manifest{}, err
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
	node, err = nextRF3Field(&fields, `"members"`)
	if err != nil {
		return rf3Manifest{}, err
	}
	if manifest.Members, err = parseRF3ManifestMembers(node); err != nil {
		return rf3Manifest{}, err
	}
	if _, _, extra := fields.Next(); extra {
		return rf3Manifest{}, errInvalidRF3Manifest
	}
	return manifest, nil
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
