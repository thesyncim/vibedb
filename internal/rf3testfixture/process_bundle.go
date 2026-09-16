package rf3testfixture

import (
	"bytes"
	"errors"
	"path/filepath"
	"strconv"

	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	vibejson "github.com/thesyncim/vibejson"
)

var ErrProcessManifestBundle = errors.New("rf3 process fixture: invalid manifest bundle")

// maxProcessManifestBundleBytes matches the shipped serve-rf3 manifest read
// bound. Composition rejects oversized input before indexing it and also caps
// the aggregate source retained while constructing one canonical bundle.
const maxProcessManifestBundleBytes = 4 << 20

var processManifestCommonFields = [...]string{
	"listeners",
	"tls",
	"authorization_policy",
	"replica_control",
	"split_control",
}

var processManifestGroupFields = [...]string{
	"wal",
	"sql",
	"route",
	"child_registry",
	"members",
	"enrolled_target",
}

// ProcessNodeLogManifest is the physical-node portion of the managed RF3
// manifest. It is kept in the fixture package so external-process tests and
// operator compositions use the same canonical node-log grammar as
// prepare-node-rf3 without importing command-private types.
type ProcessNodeLogManifest struct {
	Format          uint16                     `json:"format"`
	Path            string                     `json:"path"`
	KeyID           string                     `json:"key_id"`
	WrappedKey      string                     `json:"wrapped_key,omitempty"`
	KeyMaterialPath string                     `json:"key_material_path"`
	Options         raftstore.NodeStoreOptions `json:"options"`
}

// ProcessNodeMetadata carries the immutable physical identity that must
// survive singleton-group composition. CatalogGenesis is an already
// canonical JSON object because its command-owned type lives in
// cmd/vibedb-shard; the composition helper validates and places it at the
// exact grammar position rather than decoding it into a second type.
type ProcessNodeMetadata struct {
	NodeLog              ProcessNodeLogManifest
	NodeIncarnation      uint64
	GatewaySeeds         []nodecontrol.BootstrapGatewaySeed
	CanonicalSourceSeeds []nodecontrol.BootstrapGatewaySeed
	CatalogGenesis       []byte
}

// CombineProcessManifestsWithNodeMetadata composes prepared singleton groups
// and prefixes the complete physical-node identity. The physical node log,
// incarnation, and authenticated source pins are common to every group;
// catalog genesis is inserted after split_control, where the serve-rf3 parser
// requires it. This is the fixture-side counterpart of prepare-node-rf3's
// atomic physical publication and deliberately has no catalog bootstrap
// fallback.
func CombineProcessManifestsWithNodeMetadata(
	metadata ProcessNodeMetadata, documents ...[]byte,
) ([]byte, error) {
	if err := validateProcessNodeMetadata(metadata); err != nil {
		return nil, err
	}
	bundle, err := CombineProcessManifests(documents...)
	if err != nil {
		return nil, err
	}
	nodeLog, err := vibejson.Marshal(&metadata.NodeLog)
	if err != nil {
		return nil, errors.Join(ErrProcessManifestBundle, err)
	}
	prefix := make([]byte, 0, len(bundle)+len(nodeLog)+512)
	prefix = append(prefix, `{"node_log":`...)
	prefix = append(prefix, nodeLog...)
	prefix = append(prefix, `,"node_incarnation":`...)
	prefix = strconv.AppendUint(prefix, metadata.NodeIncarnation, 10)
	if len(metadata.GatewaySeeds) != 0 {
		prefix = append(prefix, `,"bootstrap_gateway_seeds":`...)
		seeds, marshalErr := vibejson.Marshal(&metadata.GatewaySeeds)
		if marshalErr != nil {
			return nil, errors.Join(ErrProcessManifestBundle, marshalErr)
		}
		prefix = append(prefix, seeds...)
	}
	if len(metadata.CanonicalSourceSeeds) != 0 {
		prefix = append(prefix, `,"canonical_source_seeds":`...)
		seeds, marshalErr := vibejson.Marshal(&metadata.CanonicalSourceSeeds)
		if marshalErr != nil {
			return nil, errors.Join(ErrProcessManifestBundle, marshalErr)
		}
		prefix = append(prefix, seeds...)
	}
	prefix = append(prefix, ',')
	if len(bundle) < 2 || bundle[0] != '{' {
		return nil, ErrProcessManifestBundle
	}
	prefix = append(prefix, bundle[1:]...)
	if len(metadata.CatalogGenesis) == 0 {
		return prefix, nil
	}
	position := bytes.Index(prefix, []byte(`,"groups":`))
	if position < 0 {
		return nil, ErrProcessManifestBundle
	}
	result := make([]byte, 0, len(prefix)+len(metadata.CatalogGenesis)+20)
	result = append(result, prefix[:position]...)
	result = append(result, `,"catalog_genesis":`...)
	result = append(result, metadata.CatalogGenesis...)
	result = append(result, prefix[position:]...)
	if len(result) > maxProcessManifestBundleBytes {
		return nil, ErrProcessManifestBundle
	}
	return result, nil
}

func validateProcessNodeMetadata(metadata ProcessNodeMetadata) error {
	log := metadata.NodeLog
	if log.Format != 1 || log.KeyID == "" || metadata.NodeIncarnation == 0 ||
		!filepath.IsAbs(log.Path) || filepath.Clean(log.Path) != log.Path ||
		!filepath.IsAbs(log.KeyMaterialPath) || filepath.Clean(log.KeyMaterialPath) != log.KeyMaterialPath ||
		log.Path == "/" || log.KeyMaterialPath == "/" || log.Path == log.KeyMaterialPath {
		return ErrProcessManifestBundle
	}
	if len(metadata.GatewaySeeds) == 0 && len(metadata.CanonicalSourceSeeds) == 0 {
		return ErrProcessManifestBundle
	}
	for _, seeds := range [][]nodecontrol.BootstrapGatewaySeed{metadata.GatewaySeeds, metadata.CanonicalSourceSeeds} {
		if len(seeds) > nodecontrol.MaxBootstrapGatewaySeeds {
			return ErrProcessManifestBundle
		}
		seen := make(map[rafttransport.NodeID]struct{}, len(seeds))
		for _, seed := range seeds {
			if !seed.Valid() {
				return ErrProcessManifestBundle
			}
			if _, found := seen[seed.NodeID]; found {
				return ErrProcessManifestBundle
			}
			seen[seed.NodeID] = struct{}{}
		}
	}
	if len(metadata.CatalogGenesis) != 0 {
		document, err := vibejson.Parse(metadata.CatalogGenesis)
		if err != nil {
			return errors.Join(ErrProcessManifestBundle, err)
		}
		if _, ok := document.Object(); !ok {
			return ErrProcessManifestBundle
		}
		canonical, err := document.MarshalJSON()
		if err != nil || !bytes.Equal(canonical, metadata.CatalogGenesis) {
			return errors.Join(ErrProcessManifestBundle, err)
		}
	}
	return nil
}

// CombineProcessManifests composes independently prepared singleton groups
// into the strict serve-rf3 multi-group grammar. The common listener, TLS,
// authorization, and control journal/grants must be byte-identical after
// vibejson canonicalization. Every group retains its exact split-child schema
// and apply template alongside its WAL, SQL, route, and membership state.
func CombineProcessManifests(documents ...[]byte) ([]byte, error) {
	if len(documents) < 2 || len(documents) > 64 {
		return nil, ErrProcessManifestBundle
	}
	totalBytes := 0
	for _, document := range documents {
		if len(document) == 0 || len(document) > maxProcessManifestBundleBytes ||
			totalBytes > maxProcessManifestBundleBytes-len(document) {
			return nil, ErrProcessManifestBundle
		}
		totalBytes += len(document)
	}
	groups := make([]processManifestFields, len(documents))
	var common processManifestFields
	var maxOperations uint64
	for index, document := range documents {
		fields, err := openProcessManifestFields(document)
		if err != nil {
			return nil, err
		}
		shared, registry, bound, err := splitProcessControl(fields.values["split_control"])
		if err != nil {
			return nil, err
		}
		fields.values["split_control"] = shared
		fields.values["child_registry"] = registry
		if bound > maxOperations {
			maxOperations = bound
		}
		if index == 0 {
			common = fields.commonOnly()
		} else if !common.sameCommon(fields) {
			return nil, ErrProcessManifestBundle
		}
		groups[index] = fields.groupOnly()
	}
	// One process-wide operation budget, not the sum of all group budgets.
	shared := common.values["split_control"]
	shared = append(shared[:len(shared)-1:len(shared)-1], `,"max_operations":`...)
	shared = strconv.AppendUint(shared, maxOperations, 10)
	common.values["split_control"] = append(shared, '}')

	var output bytes.Buffer
	writer := vibejson.NewWriter(&output)
	if err := writer.BeginObject(); err != nil {
		return nil, err
	}
	for _, name := range processManifestCommonFields {
		if err := writer.Key(name); err != nil {
			return nil, err
		}
		if err := writer.RawUnchecked(common.values[name]); err != nil {
			return nil, err
		}
	}
	if err := writer.Key("groups"); err != nil {
		return nil, err
	}
	if err := writer.BeginArray(); err != nil {
		return nil, err
	}
	for _, group := range groups {
		if err := writer.BeginObject(); err != nil {
			return nil, err
		}
		for _, name := range processManifestGroupFields {
			value, found := group.values[name]
			if !found {
				continue
			}
			if err := writer.Key(name); err != nil {
				return nil, err
			}
			if err := writer.RawUnchecked(value); err != nil {
				return nil, err
			}
		}
		if err := writer.EndObject(); err != nil {
			return nil, err
		}
	}
	if err := writer.EndArray(); err != nil {
		return nil, err
	}
	if err := writer.EndObject(); err != nil {
		return nil, err
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	if output.Len() == 0 || output.Len() > maxProcessManifestBundleBytes {
		return nil, ErrProcessManifestBundle
	}
	return output.Bytes(), nil
}

type processManifestFields struct {
	values map[string][]byte
}

func splitProcessControl(raw []byte) (shared, registry []byte, maxOperations uint64, err error) {
	parsed, parseErr := vibejson.Parse(raw)
	if parseErr != nil {
		return nil, nil, 0, errors.Join(ErrProcessManifestBundle, parseErr)
	}
	members, ok := parsed.Object()
	if !ok || len(members) != 5 {
		return nil, nil, 0, ErrProcessManifestBundle
	}
	names := [...]string{"journal_path", "max_records", "max_file_bytes", "grants", "child_registry"}
	values := make(map[string][]byte, len(names))
	for _, member := range members {
		allowed := false
		for _, name := range names {
			allowed = allowed || member.Key == name
		}
		if _, duplicate := values[member.Key]; duplicate || !allowed {
			return nil, nil, 0, ErrProcessManifestBundle
		}
		encoded, marshalErr := member.Value.MarshalJSON()
		if marshalErr != nil {
			return nil, nil, 0, errors.Join(ErrProcessManifestBundle, marshalErr)
		}
		values[member.Key] = encoded
	}
	child, _ := parsed.Get("child_registry")
	bound, _ := child.Get("max_operations")
	maxOperations, ok = bound.Uint64()
	if !ok || maxOperations == 0 || maxOperations > 64 {
		return nil, nil, 0, ErrProcessManifestBundle
	}
	shared = append(shared, '{')
	for index, name := range names[:4] {
		if index != 0 {
			shared = append(shared, ',')
		}
		shared = append(shared, '"')
		shared = append(shared, name...)
		shared = append(shared, '"', ':')
		shared = append(shared, values[name]...)
	}
	shared = append(shared, '}')
	return shared, values["child_registry"], maxOperations, nil
}

func openProcessManifestFields(document []byte) (processManifestFields, error) {
	parsed, err := vibejson.Parse(document)
	if err != nil {
		return processManifestFields{}, errors.Join(ErrProcessManifestBundle, err)
	}
	members, ok := parsed.Object()
	if !ok {
		return processManifestFields{}, ErrProcessManifestBundle
	}
	fields := processManifestFields{values: make(map[string][]byte, len(members))}
	for _, member := range members {
		if _, duplicate := fields.values[member.Key]; duplicate ||
			!processManifestFieldAllowed(member.Key) {
			return processManifestFields{}, ErrProcessManifestBundle
		}
		raw, marshalErr := member.Value.MarshalJSON()
		if marshalErr != nil {
			return processManifestFields{}, errors.Join(ErrProcessManifestBundle, marshalErr)
		}
		fields.values[member.Key] = raw
	}
	for _, name := range processManifestCommonFields {
		if len(fields.values[name]) == 0 {
			return processManifestFields{}, ErrProcessManifestBundle
		}
	}
	for _, name := range [...]string{"wal", "sql", "route", "members"} {
		if len(fields.values[name]) == 0 {
			return processManifestFields{}, ErrProcessManifestBundle
		}
	}
	return fields, nil
}

func processManifestFieldAllowed(name string) bool {
	if name == "child_registry" {
		return false // accepted only nested under singleton split_control
	}
	for _, candidate := range processManifestCommonFields {
		if name == candidate {
			return true
		}
	}
	for _, candidate := range processManifestGroupFields {
		if name == candidate {
			return true
		}
	}
	return false
}

func (fields processManifestFields) commonOnly() processManifestFields {
	common := processManifestFields{values: make(map[string][]byte, len(processManifestCommonFields))}
	for _, name := range processManifestCommonFields {
		common.values[name] = fields.values[name]
	}
	return common
}

func (fields processManifestFields) groupOnly() processManifestFields {
	group := processManifestFields{values: make(map[string][]byte, len(processManifestGroupFields))}
	for _, name := range processManifestGroupFields {
		if value, found := fields.values[name]; found {
			group.values[name] = value
		}
	}
	return group
}

func (fields processManifestFields) sameCommon(other processManifestFields) bool {
	for _, name := range processManifestCommonFields {
		if !bytes.Equal(fields.values[name], other.values[name]) {
			return false
		}
	}
	return true
}
