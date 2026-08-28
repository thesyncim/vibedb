package driver

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/storeio"
	"github.com/thesyncim/vibejson"
)

// Catalog metadata has one canonical, bounded vibejson grammar. Unknown,
// duplicate (including escape-equivalent), missing, oversized, and trailing
// input fail closed.
type catalogFileVibe catalogFile
type shardStoreIdentityVibe ShardStoreIdentity
type shardStoreFenceVibe ShardStoreFence

const (
	maxCanonicalJSONStringExpansion = 6
	maxShardStoreIdentityJSONBytes  = 2*(maxCanonicalJSONStringExpansion*maxCatalogTableNameBytes+2) + 256
	maxShardStoreFenceJSONBytes     = 256
	// The deepest canonical catalog value is currently six containers (root,
	// tables, table metadata, schema, fields, field metadata). Two spare levels
	// keep the bound explicit without coupling additions to that exact shape.
	maxCatalogJSONDepth = 8
	// Any decoded catalog map key is bounded by maxCatalogTableNameBytes.
	// Six source bytes per decoded byte is JSON's worst escape expansion.
	maxCatalogEncodedKeyBytes = 6 * maxCatalogTableNameBytes
)

var catalogVibeDecoder = func() vibejson.Decoder[catalogFileVibe] {
	decoder, err := vibejson.CompileDecoder[catalogFileVibe](vibejson.DecoderOptions{
		MaxDepth:      maxCatalogJSONDepth,
		CaseSensitive: true,
		Replace:       true,
	})
	if err != nil {
		panic("driver: compile SQL catalog decoder: " + err.Error())
	}
	return decoder
}()

var catalogVibeEncoder = func() vibejson.Encoder[catalogFileVibe] {
	encoder, err := vibejson.CompileEncoder[catalogFileVibe](vibejson.EncoderOptions{})
	if err != nil {
		panic("driver: compile SQL catalog encoder: " + err.Error())
	}
	return encoder
}()

var (
	catalogRootFields        = vibejson.MakeFieldSet("version", "tables", "views", "shard_store", "shard_store_fence", "replicated_shard_store", "replicated_apply", "replicated_child_apply")
	tableMetaFields          = vibejson.MakeFieldSet("primary_key", "schema", "indexes", "storage", "materialized", "sealed_recovery_journal_bytes")
	viewMetaFields           = vibejson.MakeFieldSet("query", "columns", "outputs", "view_dependencies", "table_dependencies")
	schemaMetaFields         = vibejson.MakeFieldSet("root", "fields")
	schemaFieldFields        = vibejson.MakeFieldSet("path", "types", "required")
	indexMetaFields          = vibejson.MakeFieldSet("name", "paths")
	shardStoreIdentityFields = vibejson.MakeFieldSet("distribution", "shard", "allocation_generation", "log_id")
	shardStoreFenceFields    = vibejson.MakeFieldSet("ownership_epoch", "routing_version")
)

func (i *shardStoreIdentityVibe) MarshalVibeJSON(w vibejson.TrustedAppender) vibejson.TrustedAppender {
	return appendShardStoreIdentity(w, ShardStoreIdentity(*i))
}

func appendShardStoreIdentity(w vibejson.TrustedAppender, i ShardStoreIdentity) vibejson.TrustedAppender {
	w = w.RawUnchecked(`{"distribution":`).String(string(i.Distribution)).RawUnchecked(`,"shard":`).String(string(i.Shard)).RawUnchecked(`,"allocation_generation":`).Uint(uint64(i.AllocationGeneration)).RawUnchecked(`,"log_id":`)
	w = appendReplicatedHexString(w, i.LogID[:])
	return w.RawByteUnchecked('}')
}

func (i *shardStoreIdentityVibe) UnmarshalVibeJSON(c vibejson.DecodeCursor) (vibejson.DecodeCursor, error) {
	var decoded ShardStoreIdentity
	if err := decodeShardStoreIdentityVibe(&c, &decoded); err != nil {
		return c, err
	}
	*i = shardStoreIdentityVibe(decoded)
	return c, nil
}

func decodeShardStoreIdentityVibe(c *vibejson.DecodeCursor, dst *ShardStoreIdentity) error {
	if err := c.BeginObject("shard store identity"); err != nil {
		return errors.New("vibedb: SQL catalog shard store identity must be a JSON object")
	}
	var decoded ShardStoreIdentity
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := shardStoreIdentityFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("shard store identity", name)
		}
		if err := markCatalogField(&seen, index, "shard store identity", name); err != nil {
			return err
		}
		switch index {
		case 0:
			var value string
			err = c.String(&value)
			decoded.Distribution = distribution.DistributionName(strings.Clone(value))
		case 1:
			var value string
			err = c.String(&value)
			decoded.Shard = distribution.ShardID(strings.Clone(value))
		case 2:
			var value uint64
			err = c.Uint64(&value)
			decoded.AllocationGeneration = distribution.ShardAllocationGeneration(value)
		case 3:
			err = decodeReplicatedLowerHex(c, decoded.LogID[:], "vibedb: shard store log id must contain exactly 128 bits of lowercase hexadecimal", "vibedb: shard store log id")
		}
		if err != nil {
			return err
		}
	}
	names := [...]string{"distribution", "shard", "allocation_generation", "log_id"}
	if err := requireReplicatedFields(seen, names[:], "shard store identity"); err != nil {
		return err
	}
	if err := validateShardStoreIdentity(decoded); err != nil {
		return err
	}
	*dst = decoded
	return nil
}

func appendShardStoreFence(w vibejson.TrustedAppender, f ShardStoreFence) vibejson.TrustedAppender {
	return w.RawUnchecked(`{"ownership_epoch":`).Uint(uint64(f.OwnershipEpoch)).RawUnchecked(`,"routing_version":`).Uint(uint64(f.RoutingVersion)).RawByteUnchecked('}')
}

func (f *shardStoreFenceVibe) MarshalVibeJSON(w vibejson.TrustedAppender) vibejson.TrustedAppender {
	return appendShardStoreFence(w, ShardStoreFence(*f))
}

func (f *shardStoreFenceVibe) UnmarshalVibeJSON(c vibejson.DecodeCursor) (vibejson.DecodeCursor, error) {
	var decoded ShardStoreFence
	if err := decodeShardStoreFenceVibe(&c, &decoded); err != nil {
		return c, err
	}
	*f = shardStoreFenceVibe(decoded)
	return c, nil
}

func decodeShardStoreFenceVibe(c *vibejson.DecodeCursor, dst *ShardStoreFence) error {
	if err := c.BeginObject("shard store fence"); err != nil {
		return errors.New("vibedb: SQL catalog shard store fence must be a JSON object")
	}
	var decoded ShardStoreFence
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := shardStoreFenceFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("shard store fence", name)
		}
		if err := markCatalogField(&seen, index, "shard store fence", name); err != nil {
			return err
		}
		if index == 0 {
			var value uint64
			err = c.Uint64(&value)
			decoded.OwnershipEpoch = distribution.OwnershipEpoch(value)
		} else {
			var value uint64
			err = c.Uint64(&value)
			decoded.RoutingVersion = distribution.RoutingVersion(value)
		}
		if err != nil {
			return err
		}
	}
	names := [...]string{"ownership_epoch", "routing_version"}
	if err := requireReplicatedFields(seen, names[:], "shard store fence"); err != nil {
		return err
	}
	if err := validateShardStoreFence(decoded); err != nil {
		return err
	}
	*dst = decoded
	return nil
}

func (c catalogFile) MarshalJSON() ([]byte, error) {
	bound, err := catalogSizeUpperBound(c)
	if err != nil {
		return nil, err
	}
	return appendCatalogJSON(make([]byte, 0, bound), c)
}

func appendCatalogJSON(dst []byte, catalog catalogFile) ([]byte, error) {
	encoded := catalogFileVibe(catalog)
	return catalogVibeEncoder.AppendJSON(dst, &encoded)
}

func (c *catalogFile) UnmarshalJSON(data []byte) error {
	var decoded catalogFileVibe
	if err := decodeCatalogJSON(data, &decoded); err != nil {
		return err
	}
	*c = catalogFile(decoded)
	return nil
}

func decodeCatalogJSON(data []byte, dst *catalogFileVibe) error {
	if len(data) > maxCatalogBytes {
		return catalogSizeError(len(data))
	}
	if err := preflightCatalogJSON(data); err != nil {
		return err
	}
	return catalogVibeDecoder.Decode(data, dst)
}

// preflightCatalogJSON is an allocation-free structural admission pass. The
// compiled decoder remains the syntax authority; this pass exists so an
// attacker cannot make it build a huge escaped-key arena or walk thousands of
// nested containers before the catalog grammar rejects the value.
func preflightCatalogJSON(data []byte) error {
	var containers [maxCatalogJSONDepth]byte
	var objectNeedsKey [maxCatalogJSONDepth]bool
	depth := 0
	for offset := 0; offset < len(data); offset++ {
		switch data[offset] {
		case '"':
			key := depth != 0 && containers[depth-1] == '{' && objectNeedsKey[depth-1]
			start := offset + 1
			offset++
			for ; offset < len(data); offset++ {
				if key && offset-start > maxCatalogEncodedKeyBytes {
					return errors.New("vibedb: SQL catalog member name exceeds its encoded byte bound")
				}
				if data[offset] == '\\' {
					offset++
					continue
				}
				if data[offset] == '"' {
					break
				}
			}
			if offset >= len(data) {
				return errors.New("vibedb: SQL catalog contains an unterminated string")
			}
		case '{', '[':
			if depth == maxCatalogJSONDepth {
				return fmt.Errorf("vibedb: SQL catalog exceeds the maximum JSON depth of %d", maxCatalogJSONDepth)
			}
			containers[depth] = data[offset]
			objectNeedsKey[depth] = data[offset] == '{'
			depth++
		case '}', ']':
			if depth != 0 {
				depth--
			}
		case ':':
			if depth != 0 && containers[depth-1] == '{' {
				objectNeedsKey[depth-1] = false
			}
		case ',':
			if depth != 0 && containers[depth-1] == '{' {
				objectNeedsKey[depth-1] = true
			}
		}
	}
	return nil
}

func (c *catalogFileVibe) MarshalVibeJSON(w vibejson.TrustedAppender) vibejson.TrustedAppender {
	catalog := catalogFile(*c)
	w = w.RawUnchecked(`{"version":`).Int(int64(catalog.Version)).RawUnchecked(`,"tables":`)
	w = appendCatalogTables(w, catalog.Tables)
	if len(catalog.Views) != 0 {
		w = w.RawUnchecked(`,"views":`)
		w = appendCatalogViews(w, catalog.Views)
	}
	if catalog.ShardStore != nil {
		w = w.RawUnchecked(`,"shard_store":`)
		w = appendShardStoreIdentity(w, *catalog.ShardStore)
	}
	if catalog.ShardStoreFence != nil {
		w = w.RawUnchecked(`,"shard_store_fence":`)
		w = appendShardStoreFence(w, *catalog.ShardStoreFence)
	}
	if catalog.ReplicatedShardStore != nil {
		w = w.RawUnchecked(`,"replicated_shard_store":`)
		encoded := replicatedStoreIdentityVibe(*catalog.ReplicatedShardStore)
		w = encoded.MarshalVibeJSON(w)
	}
	if catalog.ReplicatedApply != nil {
		w = w.RawUnchecked(`,"replicated_apply":`)
		encoded := replicatedApplyMetaVibe(*catalog.ReplicatedApply)
		w = encoded.MarshalVibeJSON(w)
	}
	if catalog.ReplicatedChildApply != nil {
		w = w.RawUnchecked(`,"replicated_child_apply":`)
		encoded := replicatedApplyMetaVibe(*catalog.ReplicatedChildApply)
		w = encoded.MarshalVibeJSON(w)
	}
	return w.RawByteUnchecked('}')
}

func appendCatalogTables(w vibejson.TrustedAppender, values map[string]*tableMeta) vibejson.TrustedAppender {
	w = w.RawByteUnchecked('{')
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	slices.Sort(names)
	for ordinal, name := range names {
		if ordinal != 0 {
			w = w.RawByteUnchecked(',')
		}
		w = w.String(name).RawByteUnchecked(':')
		if values[name] == nil {
			w = w.RawUnchecked("null")
		} else {
			w = appendTableMeta(w, *values[name])
		}
	}
	return w.RawByteUnchecked('}')
}

func appendCatalogViews(w vibejson.TrustedAppender, values map[string]*viewMeta) vibejson.TrustedAppender {
	w = w.RawByteUnchecked('{')
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	slices.Sort(names)
	for ordinal, name := range names {
		if ordinal != 0 {
			w = w.RawByteUnchecked(',')
		}
		w = w.String(name).RawByteUnchecked(':')
		if values[name] == nil {
			w = w.RawUnchecked("null")
		} else {
			w = appendViewMeta(w, *values[name])
		}
	}
	return w.RawByteUnchecked('}')
}

func appendTableMeta(w vibejson.TrustedAppender, m tableMeta) vibejson.TrustedAppender {
	w = w.RawUnchecked(`{"primary_key":`).String(m.PrimaryKey)
	if m.Schema != nil {
		w = w.RawUnchecked(`,"schema":`)
		w = appendSchemaMeta(w, *m.Schema)
	}
	if len(m.Indexes) != 0 {
		w = w.RawUnchecked(`,"indexes":[`)
		for i := range m.Indexes {
			if i != 0 {
				w = w.RawByteUnchecked(',')
			}
			w = appendIndexMeta(w, m.Indexes[i])
		}
		w = w.RawByteUnchecked(']')
	}
	if m.Storage != "" {
		w = w.RawUnchecked(`,"storage":`).String(m.Storage)
	}
	if m.Materialized {
		w = w.RawUnchecked(`,"materialized":true`)
	}
	if m.SealedRecoveryJournalBytes != 0 {
		w = w.RawUnchecked(`,"sealed_recovery_journal_bytes":`).Uint(m.SealedRecoveryJournalBytes)
	}
	return w.RawByteUnchecked('}')
}

func appendViewMeta(w vibejson.TrustedAppender, m viewMeta) vibejson.TrustedAppender {
	w = w.RawUnchecked(`{"query":`).String(m.Query)
	if len(m.Columns) != 0 {
		w = w.RawUnchecked(`,"columns":`)
		w = appendStringArray(w, m.Columns)
	}
	w = w.RawUnchecked(`,"outputs":`)
	w = appendStringArray(w, m.Outputs)
	if len(m.ViewDependencies) != 0 {
		w = w.RawUnchecked(`,"view_dependencies":`)
		w = appendStringArray(w, m.ViewDependencies)
	}
	if len(m.TableDependencies) != 0 {
		w = w.RawUnchecked(`,"table_dependencies":`)
		w = appendStringArray(w, m.TableDependencies)
	}
	return w.RawByteUnchecked('}')
}

func appendSchemaMeta(w vibejson.TrustedAppender, m schemaMeta) vibejson.TrustedAppender {
	w = w.RawUnchecked(`{"root":`).Uint(uint64(m.Root))
	if len(m.Fields) != 0 {
		w = w.RawUnchecked(`,"fields":[`)
		for i, f := range m.Fields {
			if i != 0 {
				w = w.RawByteUnchecked(',')
			}
			w = w.RawUnchecked(`{"path":`).String(f.Path).RawUnchecked(`,"types":`).Uint(uint64(f.Types))
			if f.Required {
				w = w.RawUnchecked(`,"required":true`)
			}
			w = w.RawByteUnchecked('}')
		}
		w = w.RawByteUnchecked(']')
	}
	return w.RawByteUnchecked('}')
}

func appendIndexMeta(w vibejson.TrustedAppender, m indexMeta) vibejson.TrustedAppender {
	w = w.RawUnchecked(`{"name":`).String(m.Name).RawUnchecked(`,"paths":`)
	w = appendStringArray(w, m.Paths)
	return w.RawByteUnchecked('}')
}

func appendStringArray(w vibejson.TrustedAppender, values []string) vibejson.TrustedAppender {
	w = w.RawByteUnchecked('[')
	for i, value := range values {
		if i != 0 {
			w = w.RawByteUnchecked(',')
		}
		w = w.String(value)
	}
	return w.RawByteUnchecked(']')
}

func (c *catalogFileVibe) UnmarshalVibeJSON(cursor vibejson.DecodeCursor) (vibejson.DecodeCursor, error) {
	var decoded catalogFile
	if err := decodeCatalogFile(&cursor, &decoded); err != nil {
		return cursor, err
	}
	*c = catalogFileVibe(decoded)
	return cursor, nil
}

func decodeCatalogFile(c *vibejson.DecodeCursor, dst *catalogFile) error {
	if err := c.BeginObject("SQL catalog root"); err != nil {
		return errors.New("vibedb: SQL catalog root must be a JSON object")
	}
	var decoded catalogFile
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := catalogRootFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("root", name)
		}
		if err := markCatalogField(&seen, index, "root", name); err != nil {
			return err
		}
		switch index {
		case 0:
			null, nullErr := c.Null()
			if nullErr != nil {
				return nullErr
			}
			if null {
				return errors.New("vibedb: SQL catalog version must not be null")
			}
			var value int64
			err = c.Int64(&value)
			if err == nil && (value < 0 || int64(int(value)) != value) {
				err = errors.New("vibedb: SQL catalog version is out of range")
			}
			decoded.Version = int(value)
		case 1:
			err = decodeCatalogTables(c, &decoded.Tables)
		case 2:
			err = decodeCatalogViews(c, &decoded.Views)
		case 3:
			decoded.ShardStore = new(ShardStoreIdentity)
			err = checkReplicatedValueByteBound(c, maxShardStoreIdentityJSONBytes, "vibedb: shard store identity exceeds its byte bound")
			if err == nil {
				err = decodeShardStoreIdentityVibe(c, decoded.ShardStore)
			}
		case 4:
			decoded.ShardStoreFence = new(ShardStoreFence)
			err = checkReplicatedValueByteBound(c, maxShardStoreFenceJSONBytes, "vibedb: shard store fence exceeds its byte bound")
			if err == nil {
				err = decodeShardStoreFenceVibe(c, decoded.ShardStoreFence)
			}
		case 5:
			decoded.ReplicatedShardStore = new(ReplicatedShardStoreIdentity)
			err = checkReplicatedValueByteBound(c, maxReplicatedStoreJSONBytes, "vibedb: replicated shard store identity exceeds its byte bound")
			if err == nil {
				err = decodeReplicatedStoreIdentityVibe(c, decoded.ReplicatedShardStore)
			}
		case 6:
			decoded.ReplicatedApply = new(replicatedApplyMeta)
			err = decodeReplicatedApplyMetaVibe(c, decoded.ReplicatedApply)
		case 7:
			decoded.ReplicatedChildApply = new(replicatedApplyMeta)
			err = decodeReplicatedApplyMetaVibe(c, decoded.ReplicatedChildApply)
		}
		if err != nil {
			return err
		}
	}
	if seen&1 == 0 {
		return fmt.Errorf("vibedb: SQL catalog root is missing member %q", "version")
	}
	if seen&2 == 0 {
		return fmt.Errorf("vibedb: SQL catalog root is missing member %q", "tables")
	}
	if decoded.ShardStoreFence != nil && decoded.ShardStore == nil {
		return errors.New("vibedb: SQL catalog shard store fence requires a shard store identity")
	}
	if err := validateReplicatedCatalog(decoded); err != nil {
		return fmt.Errorf("vibedb: SQL catalog replicated shard store: %w", err)
	}
	*dst = decoded
	return nil
}

func decodeCatalogTables(c *vibejson.DecodeCursor, dst *map[string]*tableMeta) error {
	if err := c.BeginObject("SQL catalog tables"); err != nil {
		return errors.New("vibedb: SQL catalog tables must be a JSON object")
	}
	decoded := make(map[string]*tableMeta)
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if _, duplicate := decoded[name]; duplicate {
			return duplicateCatalogMember("tables", name)
		}
		if len(name) > maxCatalogTableNameBytes {
			return errors.New("vibedb: SQL catalog table name exceeds its byte bound")
		}
		if err := checkCatalogTableCount(len(decoded) + 1); err != nil {
			return err
		}
		null, err := c.Null()
		if err != nil {
			return err
		}
		if null {
			decoded[strings.Clone(name)] = nil
			continue
		}
		meta := new(tableMeta)
		if err := decodeTableMetaVibe(c, meta); err != nil {
			return fmt.Errorf("table %q: %w", name, err)
		}
		decoded[strings.Clone(name)] = meta
	}
	*dst = decoded
	return nil
}

func decodeCatalogViews(c *vibejson.DecodeCursor, dst *map[string]*viewMeta) error {
	if err := c.BeginObject("SQL catalog views"); err != nil {
		return errors.New("vibedb: SQL catalog views must be a JSON object")
	}
	decoded := make(map[string]*viewMeta)
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if _, duplicate := decoded[name]; duplicate {
			return duplicateCatalogMember("views", name)
		}
		if len(name) > maxCatalogTableNameBytes {
			return errors.New("vibedb: SQL catalog view name exceeds its byte bound")
		}
		if err := checkCatalogViewCount(len(decoded) + 1); err != nil {
			return err
		}
		null, err := c.Null()
		if err != nil {
			return err
		}
		if null {
			decoded[strings.Clone(name)] = nil
			continue
		}
		meta := new(viewMeta)
		if err := decodeViewMetaVibe(c, meta); err != nil {
			return fmt.Errorf("view %q: %w", name, err)
		}
		decoded[strings.Clone(name)] = meta
	}
	*dst = decoded
	return nil
}

func decodeTableMetaVibe(c *vibejson.DecodeCursor, dst *tableMeta) error {
	if err := c.BeginObject("SQL catalog table metadata"); err != nil {
		return errors.New("vibedb: SQL catalog table metadata must be a JSON object")
	}
	var decoded tableMeta
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := tableMetaFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("table metadata", name)
		}
		if err := markCatalogField(&seen, index, "table metadata", name); err != nil {
			return err
		}
		switch index {
		case 0:
			err = decodeBoundedCatalogString(c, &decoded.PrimaryKey, maxCatalogBytes, "primary key")
		case 1:
			decoded.Schema = new(schemaMeta)
			err = decodeSchemaMetaVibe(c, decoded.Schema)
		case 2:
			err = decodeIndexListVibe(c, &decoded.Indexes)
		case 3:
			err = decodeBoundedCatalogString(c, &decoded.Storage, maxCatalogBytes, "storage identity")
		case 4:
			err = c.Bool(&decoded.Materialized)
		case 5:
			err = c.Uint64(&decoded.SealedRecoveryJournalBytes)
		}
		if err != nil {
			return err
		}
	}
	*dst = decoded
	return nil
}

func decodeViewMetaVibe(c *vibejson.DecodeCursor, dst *viewMeta) error {
	if err := c.BeginObject("SQL catalog view metadata"); err != nil {
		return errors.New("vibedb: SQL catalog view metadata must be a JSON object")
	}
	var decoded viewMeta
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := viewMetaFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("view metadata", name)
		}
		if err := markCatalogField(&seen, index, "view metadata", name); err != nil {
			return err
		}
		switch index {
		case 0:
			err = decodeBoundedCatalogString(c, &decoded.Query, maxCatalogViewQueryBytes, "view query")
		case 1:
			err = decodeStringListVibe(c, &decoded.Columns, maxCatalogViewColumns, "view columns")
		case 2:
			err = decodeStringListVibe(c, &decoded.Outputs, maxCatalogViewColumns, "view outputs")
		case 3:
			err = decodeStringListVibe(c, &decoded.ViewDependencies, maxCatalogViewDependencies, "view dependencies")
		case 4:
			err = decodeStringListVibe(c, &decoded.TableDependencies, maxCatalogViewDependencies, "table dependencies")
		}
		if err != nil {
			return err
		}
	}
	if seen&1 == 0 {
		return fmt.Errorf("vibedb: SQL catalog view metadata is missing member %q", "query")
	}
	if seen&(1<<2) == 0 {
		return fmt.Errorf("vibedb: SQL catalog view metadata is missing member %q", "outputs")
	}
	*dst = decoded
	return nil
}

func decodeSchemaMetaVibe(c *vibejson.DecodeCursor, dst *schemaMeta) error {
	if err := c.BeginObject("SQL catalog schema"); err != nil {
		return errors.New("vibedb: SQL catalog schema must be a JSON object")
	}
	var decoded schemaMeta
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := schemaMetaFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("schema", name)
		}
		if err := markCatalogField(&seen, index, "schema", name); err != nil {
			return err
		}
		if index == 0 {
			err = c.Uint16(&decoded.Root)
		} else {
			err = decodeSchemaFieldsVibe(c, &decoded.Fields)
		}
		if err != nil {
			return err
		}
	}
	*dst = decoded
	return nil
}

func decodeSchemaFieldsVibe(c *vibejson.DecodeCursor, dst *[]schemaFieldMeta) error {
	if err := c.BeginArray("SQL catalog schema fields"); err != nil {
		return errors.New("vibedb: SQL catalog schema fields must be a JSON array")
	}
	decoded := make([]schemaFieldMeta, 0)
	for first := true; ; first = false {
		more, err := c.NextElement(first)
		if err != nil {
			return err
		}
		if !more {
			break
		}
		if len(decoded) >= storeio.PageCatalogMaxSchemaFields {
			return fmt.Errorf("vibedb: SQL catalog schema fields exceeds the format limit of %d", storeio.PageCatalogMaxSchemaFields)
		}
		var field schemaFieldMeta
		if err := decodeSchemaFieldVibe(c, &field); err != nil {
			return err
		}
		decoded = append(decoded, field)
	}
	*dst = decoded
	return nil
}

func decodeSchemaFieldVibe(c *vibejson.DecodeCursor, dst *schemaFieldMeta) error {
	if err := c.BeginObject("SQL catalog schema field"); err != nil {
		return errors.New("vibedb: SQL catalog schema field must be a JSON object")
	}
	var decoded schemaFieldMeta
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := schemaFieldFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("schema field", name)
		}
		if err := markCatalogField(&seen, index, "schema field", name); err != nil {
			return err
		}
		switch index {
		case 0:
			err = decodeBoundedCatalogString(c, &decoded.Path, maxCatalogBytes, "schema field path")
		case 1:
			err = c.Uint16(&decoded.Types)
		case 2:
			err = c.Bool(&decoded.Required)
		}
		if err != nil {
			return err
		}
	}
	*dst = decoded
	return nil
}

func decodeIndexListVibe(c *vibejson.DecodeCursor, dst *[]indexMeta) error {
	if err := c.BeginArray("SQL catalog indexes"); err != nil {
		return errors.New("vibedb: SQL catalog indexes must be a JSON array")
	}
	decoded := make([]indexMeta, 0)
	for first := true; ; first = false {
		more, err := c.NextElement(first)
		if err != nil {
			return err
		}
		if !more {
			break
		}
		if len(decoded) >= storeio.PageCatalogMaxLogicalIndexes {
			return fmt.Errorf("vibedb: SQL catalog indexes exceeds the format limit of %d", storeio.PageCatalogMaxLogicalIndexes)
		}
		var index indexMeta
		if err := decodeIndexMetaVibe(c, &index); err != nil {
			return err
		}
		decoded = append(decoded, index)
	}
	*dst = decoded
	return nil
}

func decodeIndexMetaVibe(c *vibejson.DecodeCursor, dst *indexMeta) error {
	if err := c.BeginObject("SQL catalog index"); err != nil {
		return errors.New("vibedb: SQL catalog index must be a JSON object")
	}
	var decoded indexMeta
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		index, known := indexMetaFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("index", name)
		}
		if err := markCatalogField(&seen, index, "index", name); err != nil {
			return err
		}
		if index == 0 {
			err = decodeBoundedCatalogString(c, &decoded.Name, maxCatalogBytes, "index name")
		} else {
			err = decodeStringListVibe(c, &decoded.Paths, storeio.PageCatalogMaxIndexColumns, "index paths")
		}
		if err != nil {
			return err
		}
	}
	*dst = decoded
	return nil
}

func decodeStringListVibe(c *vibejson.DecodeCursor, dst *[]string, limit int, kind string) error {
	if err := c.BeginArray("SQL catalog " + kind); err != nil {
		return fmt.Errorf("vibedb: SQL catalog %s must be a JSON array", kind)
	}
	decoded := make([]string, 0)
	for first := true; ; first = false {
		more, err := c.NextElement(first)
		if err != nil {
			return err
		}
		if !more {
			break
		}
		if len(decoded) >= limit {
			return fmt.Errorf("vibedb: SQL catalog %s exceeds the format limit of %d", kind, limit)
		}
		var value string
		if err := decodeBoundedCatalogString(c, &value, maxCatalogBytes, kind); err != nil {
			return err
		}
		decoded = append(decoded, value)
	}
	*dst = decoded
	return nil
}

func decodeBoundedCatalogString(c *vibejson.DecodeCursor, dst *string, maximum int, kind string) error {
	encodedMaximum := maximum*maxCanonicalJSONStringExpansion + 2
	if err := checkReplicatedValueByteBound(c, encodedMaximum, fmt.Sprintf("vibedb: SQL catalog %s exceeds its encoded byte bound", kind)); err != nil {
		return err
	}
	var decoded string
	if err := c.String(&decoded); err != nil {
		return err
	}
	if len(decoded) > maximum {
		return fmt.Errorf("vibedb: SQL catalog %s exceeds its decoded byte bound", kind)
	}
	*dst = strings.Clone(decoded)
	return nil
}

func markCatalogField(seen *uint64, index int, kind, name string) error {
	bit := uint64(1) << uint(index)
	if *seen&bit != 0 {
		return duplicateCatalogMember(kind, name)
	}
	*seen |= bit
	return nil
}

func duplicateCatalogMember(kind, name string) error {
	return fmt.Errorf("vibedb: SQL catalog %s has duplicate member %q", kind, name)
}

func unknownCatalogMember(kind, name string) error {
	return fmt.Errorf("vibedb: SQL catalog %s has unknown member %q", kind, name)
}
