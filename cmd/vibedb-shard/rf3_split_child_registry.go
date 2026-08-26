package main

import (
	"encoding/hex"
	"errors"
	"math"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

const (
	maxRF3SplitChildOperations = 64
	maxRF3SplitChildDDLBytes   = 64 << 10
	maxRF3StageCheckpointBytes = 1 << 30
)

var errRF3SplitChildRegistryBound = errors.New("vibedb-shard: split child registry bound exceeded")

// rf3ManifestSplitChildRegistry is a bounded provisioning template, not a
// list of speculative operations. The replicated split intent supplies exact
// child identities; this template freezes the local schema, capacity, key,
// bootstrap, and RF3 enrollment used when one admitted operation receives a
// slot. Preparation creates only Root and StaticBootstrapPath.
type rf3ManifestSplitChildRegistry struct {
	Root                 string
	MaxOperations        int
	StageCheckpointBytes uint64
	Table                string
	CreateTable          string
	WAL                  rf3ManifestSplitChildWAL
	Apply                rf3ManifestSplitChildApply
	StaticBootstrapPath  string
	ReplicaSetVersion    uint64
	Members              [rf3ManifestMembers]rf3ManifestMember
	MemberCount          uint8
}

type rf3ManifestSplitChildWAL struct {
	KeyID           string
	KeyMaterialPath string
	Options         raftstore.Options
}

type rf3ManifestSplitChildApply struct {
	MaxSessions   uint64
	RetryWindow   uint16
	TxnLimits     durable.TxnLimits
	Format        uint16
	ShardKey      string
	TupleVersion  distribution.TupleVersion
	MapperVersion distribution.MapperVersion
}

// rf3SplitChildPaths are the only local artifact names an admitted operation
// may use. Derivation is injective and contains no caller-controlled strings.
type rf3SplitChildPaths struct {
	Root     string
	Database string
	WAL      string
}

// rf3SplitChildPathRegistry is the fixed resident admission index for active
// split operations. Restart reconstructs it from the bounded replicated
// operation set; it never scans Root and never allocates a map proportional to
// historical operations.
type rf3SplitChildPathRegistry struct {
	mu       sync.Mutex
	template rf3ManifestSplitChildRegistry
	slots    [maxRF3SplitChildOperations][32]byte
}

func newRF3SplitChildPathRegistry(
	template rf3ManifestSplitChildRegistry,
) (*rf3SplitChildPathRegistry, error) {
	if template.MaxOperations <= 0 || template.MaxOperations > maxRF3SplitChildOperations {
		return nil, errInvalidRF3Manifest
	}
	if _, err := template.childPaths([32]byte{1}, 0); err != nil {
		return nil, err
	}
	return &rf3SplitChildPathRegistry{template: template}, nil
}

func (registry *rf3SplitChildPathRegistry) acquire(
	operation [32]byte,
	child uint8,
) (rf3SplitChildPaths, error) {
	if registry == nil || operation == ([32]byte{}) {
		return rf3SplitChildPaths{}, errInvalidRF3Manifest
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	empty := -1
	for index := 0; index < registry.template.MaxOperations; index++ {
		if registry.slots[index] == operation {
			return registry.template.childPaths(operation, child)
		}
		if empty < 0 && registry.slots[index] == ([32]byte{}) {
			empty = index
		}
	}
	if empty < 0 {
		return rf3SplitChildPaths{}, errRF3SplitChildRegistryBound
	}
	paths, err := registry.template.childPaths(operation, child)
	if err != nil {
		return rf3SplitChildPaths{}, err
	}
	registry.slots[empty] = operation
	return paths, nil
}

func (registry *rf3SplitChildPathRegistry) release(operation [32]byte) bool {
	if registry == nil || operation == ([32]byte{}) {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for index := 0; index < registry.template.MaxOperations; index++ {
		if registry.slots[index] == operation {
			registry.slots[index] = [32]byte{}
			return true
		}
	}
	return false
}

func (registry rf3ManifestSplitChildRegistry) childPaths(
	operation [32]byte,
	child uint8,
) (rf3SplitChildPaths, error) {
	if !filepath.IsAbs(registry.Root) || filepath.Clean(registry.Root) != registry.Root ||
		registry.Root == string(filepath.Separator) || operation == ([32]byte{}) ||
		child >= autosplit.MaxSplitChildren {
		return rf3SplitChildPaths{}, errInvalidRF3Manifest
	}
	var encoded [64]byte
	hex.Encode(encoded[:], operation[:])
	root := filepath.Join(registry.Root, string(encoded[:]), "child-"+strconv.Itoa(int(child)))
	return rf3SplitChildPaths{
		Root: root, Database: filepath.Join(root, "stage.vdb"), WAL: filepath.Join(root, "child.wal"),
	}, nil
}

func parseRF3ManifestSplitChildRegistry(node vibejson.Node) (rf3ManifestSplitChildRegistry, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestSplitChildRegistry{}, errInvalidRF3Manifest
	}
	var result rf3ManifestSplitChildRegistry
	value, err := nextRF3Field(&fields, `"root"`)
	if err != nil {
		return result, err
	}
	if result.Root, err = rf3ManifestCanonicalAbsolutePath(value); err != nil ||
		result.Root == string(filepath.Separator) {
		return rf3ManifestSplitChildRegistry{}, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"max_operations"`)
	if err != nil {
		return result, err
	}
	if result.MaxOperations, err = rf3ManifestPositiveInt(value); err != nil ||
		result.MaxOperations > maxRF3SplitChildOperations {
		return rf3ManifestSplitChildRegistry{}, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"stage_checkpoint_bytes"`)
	if err != nil {
		return result, err
	}
	if result.StageCheckpointBytes, err = rf3ManifestPositiveUint64(value); err != nil ||
		result.StageCheckpointBytes < rangesplit.MaxChildArtifactChunkBytes ||
		result.StageCheckpointBytes > maxRF3StageCheckpointBytes {
		return rf3ManifestSplitChildRegistry{}, errInvalidRF3Manifest
	}
	value, err = nextRF3Field(&fields, `"table"`)
	if err != nil {
		return result, err
	}
	if result.Table, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"create_table"`)
	if err != nil {
		return result, err
	}
	if result.CreateTable, err = rf3ManifestString(value, maxRF3SplitChildDDLBytes); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"wal"`)
	if err != nil {
		return result, err
	}
	if result.WAL, err = parseRF3ManifestSplitChildWAL(value); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"apply"`)
	if err != nil {
		return result, err
	}
	if result.Apply, err = parseRF3ManifestSplitChildApply(value); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"static_bootstrap_path"`)
	if err != nil {
		return result, err
	}
	if result.StaticBootstrapPath, err = rf3ManifestCanonicalAbsolutePath(value); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"replica_set_version"`)
	if err != nil {
		return result, err
	}
	if result.ReplicaSetVersion, err = rf3ManifestPositiveUint64(value); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"members"`)
	if err != nil {
		return result, err
	}
	var count uint8
	if result.Members, count, err = parseRF3ManifestMembers(value, true); err != nil {
		return rf3ManifestSplitChildRegistry{}, errInvalidRF3Manifest
	}
	result.MemberCount = count
	if _, _, extra := fields.Next(); extra ||
		result.StaticBootstrapPath != filepath.Join(result.Root, "static-bootstrap.pb") {
		return rf3ManifestSplitChildRegistry{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestSplitChildWAL(node vibejson.Node) (rf3ManifestSplitChildWAL, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestSplitChildWAL{}, errInvalidRF3Manifest
	}
	var result rf3ManifestSplitChildWAL
	value, err := nextRF3Field(&fields, `"key_id"`)
	if err != nil {
		return result, err
	}
	if result.KeyID, err = rf3ManifestString(value, raftstore.MaxKeyIDBytes); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"key_material_path"`)
	if err != nil {
		return result, err
	}
	if result.KeyMaterialPath, err = rf3ManifestCanonicalAbsolutePath(value); err != nil {
		return result, err
	}
	values := []struct {
		name string
		set  func(v vibejson.Node) error
	}{
		{`"max_file_bytes"`, func(v vibejson.Node) error {
			result.Options.MaxFileBytes, err = rf3ManifestPositiveInt64(v)
			return err
		}},
		{`"max_record_bytes"`, func(v vibejson.Node) error {
			result.Options.MaxRecordBytes, err = rf3ManifestPositiveInt(v)
			return err
		}},
		{`"max_records"`, func(v vibejson.Node) error { result.Options.MaxRecords, err = rf3ManifestPositiveUint64(v); return err }},
		{`"max_entries"`, func(v vibejson.Node) error { result.Options.MaxEntries, err = rf3ManifestPositiveUint64(v); return err }},
		{`"max_live_bytes"`, func(v vibejson.Node) error {
			result.Options.MaxLiveBytes, err = rf3ManifestPositiveInt64(v)
			return err
		}},
	}
	for _, field := range values {
		value, err = nextRF3Field(&fields, field.name)
		if err != nil || field.set(value) != nil {
			return rf3ManifestSplitChildWAL{}, errInvalidRF3Manifest
		}
	}
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestSplitChildWAL{}, errInvalidRF3Manifest
	}
	return result, nil
}

func parseRF3ManifestSplitChildApply(node vibejson.Node) (rf3ManifestSplitChildApply, error) {
	fields, ok := node.ObjectIter()
	if !ok {
		return rf3ManifestSplitChildApply{}, errInvalidRF3Manifest
	}
	var result rf3ManifestSplitChildApply
	value, err := nextRF3Field(&fields, `"max_sessions"`)
	if err != nil {
		return result, err
	}
	if result.MaxSessions, err = rf3ManifestPositiveUint64(value); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"retry_window"`)
	if err != nil {
		return result, err
	}
	retry, err := rf3ManifestPositiveUint64(value)
	if err != nil || retry > math.MaxUint16 {
		return rf3ManifestSplitChildApply{}, errInvalidRF3Manifest
	}
	result.RetryWindow = uint16(retry)
	value, err = nextRF3Field(&fields, `"max_collections"`)
	if err != nil {
		return result, err
	}
	if result.TxnLimits.MaxCollections, err = rf3ManifestPositiveInt(value); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"max_documents"`)
	if err != nil {
		return result, err
	}
	if result.TxnLimits.MaxDocuments, err = rf3ManifestPositiveInt(value); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"max_bytes"`)
	if err != nil {
		return result, err
	}
	if result.TxnLimits.MaxBytes, err = rf3ManifestPositiveInt64(value); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"format"`)
	if err != nil {
		return result, err
	}
	format, ok := value.Uint64()
	if !ok || format > math.MaxUint16 || format != uint64(sqldriver.ReplicatedPlacementProfileFormat) {
		return rf3ManifestSplitChildApply{}, errInvalidRF3Manifest
	}
	result.Format = uint16(format)
	value, err = nextRF3Field(&fields, `"shard_key"`)
	if err != nil {
		return result, err
	}
	if result.ShardKey, err = rf3ManifestString(value, maxRF3ManifestStringBytes); err != nil {
		return result, err
	}
	value, err = nextRF3Field(&fields, `"tuple_version"`)
	if err != nil {
		return result, err
	}
	tuple, err := rf3ManifestPositiveUint64(value)
	if err != nil || tuple != uint64(distribution.CurrentTupleVersion) {
		return rf3ManifestSplitChildApply{}, errInvalidRF3Manifest
	}
	result.TupleVersion = distribution.TupleVersion(tuple)
	value, err = nextRF3Field(&fields, `"mapper_version"`)
	if err != nil {
		return result, err
	}
	mapper, err := rf3ManifestPositiveUint64(value)
	if err != nil || mapper != uint64(distribution.NativeMapperVersion) {
		return rf3ManifestSplitChildApply{}, errInvalidRF3Manifest
	}
	result.MapperVersion = distribution.MapperVersion(mapper)
	if _, _, extra := fields.Next(); extra {
		return rf3ManifestSplitChildApply{}, errInvalidRF3Manifest
	}
	return result, nil
}

func rf3ManifestCanonicalAbsolutePath(node vibejson.Node) (string, error) {
	value, err := rf3ManifestString(node, maxRF3ManifestStringBytes)
	if err != nil || !filepath.IsAbs(value) || filepath.Clean(value) != value {
		return "", errInvalidRF3Manifest
	}
	return value, nil
}

func validateRF3SplitChildRegistryRoster(
	registry rf3ManifestSplitChildRegistry,
	groups []rf3ManifestGroup,
) error {
	if len(groups) == 0 {
		return errInvalidRF3Manifest
	}
	for _, group := range groups {
		if group.MemberCount != registry.MemberCount {
			return errInvalidRF3Manifest
		}
		for index := range registry.MemberCount {
			if registry.Members[index] != group.Members[index] {
				return errInvalidRF3Manifest
			}
		}
	}
	return nil
}
