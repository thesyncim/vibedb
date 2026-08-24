package vitessroute

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibejson"
	"github.com/thesyncim/vibejson/document"
)

// ErrUnsupportedVSchema is the sentinel every strict-subset rejection matches
// under errors.Is.
var ErrUnsupportedVSchema = errors.New("vitessroute: unsupported VSchema")

// ConfigError reports why the strict loader rejected a VSchema. It wraps
// ErrUnsupportedVSchema. The loader never silently ignores an unknown or
// unsupported field: it fails closed with a reason.
type ConfigError struct {
	Reason string
}

func (e *ConfigError) Error() string { return "vitessroute: unsupported VSchema: " + e.Reason }

func (e *ConfigError) Unwrap() error { return ErrUnsupportedVSchema }

// Upstream multicol param names, matching the pinned Vitess release exactly.
const (
	paramColumnCount  = "column_count"
	paramColumnBytes  = "column_bytes"
	paramColumnVindex = "column_vindex"

	maxVSchemaJSONBytes = 1 << 20
	maxVSchemaJSONDepth = 8
)

// The JSON subset. Every struct disallows unknown fields during decode, so a
// reference table, sequence, lookup owner, routing rule, auto-increment clause,
// or any other unsupported construct is rejected rather than silently dropped.

type rawKeyspace struct {
	Sharded  *bool                `json:"sharded"`
	Vindexes map[string]rawVindex `json:"vindexes"`
	Tables   map[string]rawTable  `json:"tables"`
}

type rawVindex struct {
	Type   string            `json:"type"`
	Params map[string]string `json:"params"`
}

type rawTable struct {
	ColumnVindexes []rawColumnVindex `json:"column_vindexes"`
}

type rawColumnVindex struct {
	Column  string   `json:"column"`
	Columns []string `json:"columns"`
	Name    string   `json:"name"`
}

var rawKeyspaceDecoder = func() vibejson.Decoder[rawKeyspace] {
	decoder, err := vibejson.CompileDecoder[rawKeyspace](vibejson.DecoderOptions{
		MaxDepth:              maxVSchemaJSONDepth,
		DisallowUnknownFields: true,
		CaseSensitive:         true,
		Replace:               true,
	})
	if err != nil {
		panic("vitessroute: compile VSchema decoder: " + err.Error())
	}
	return decoder
}()

// Keyspace is a strictly validated subset of a Vitess VSchema for one sharded
// keyspace. It resolves each supported vindex to a distribution.Mapper and each
// table to its single primary vindex. No upstream Vitess type is part of its
// API.
type Keyspace struct {
	vindexes map[string]distribution.Mapper
	tables   map[string]string // table name -> vindex name
}

// LoadKeyspace parses and strictly validates a single sharded-keyspace VSchema.
// It accepts only the exact upstream JSON fields for the xxhash and multicol
// vindexes and rejects everything else — unknown fields, unsharded keyspaces,
// lookup/owned vindexes, sequences, reference tables, routing rules,
// auto-increment behavior, cross-keyspace plans, and any destination width
// other than the fixed 8 bytes — with a typed *ConfigError.
func LoadKeyspace(data []byte) (*Keyspace, error) {
	if len(data) > maxVSchemaJSONBytes {
		return nil, &ConfigError{Reason: fmt.Sprintf("VSchema JSON exceeds %d bytes", maxVSchemaJSONBytes)}
	}
	if !utf8.Valid(data) {
		return nil, &ConfigError{Reason: "malformed VSchema JSON: invalid UTF-8"}
	}
	if bytes.IndexByte(data, 0) >= 0 {
		return nil, &ConfigError{Reason: "malformed VSchema JSON: NUL byte"}
	}
	if err := validateVSchemaJSON(data); err != nil {
		return nil, &ConfigError{Reason: "malformed VSchema JSON: " + err.Error()}
	}

	var raw rawKeyspace
	if err := rawKeyspaceDecoder.Decode(data, &raw); err != nil {
		return nil, &ConfigError{Reason: "malformed VSchema JSON: " + err.Error()}
	}
	return buildKeyspace(raw)
}

func buildKeyspace(raw rawKeyspace) (*Keyspace, error) {
	if raw.Sharded == nil || !*raw.Sharded {
		return nil, &ConfigError{Reason: `keyspace must set "sharded": true`}
	}

	ks := &Keyspace{
		vindexes: make(map[string]distribution.Mapper, len(raw.Vindexes)),
		tables:   make(map[string]string, len(raw.Tables)),
	}

	for name, v := range raw.Vindexes {
		m, err := buildVindex(v)
		if err != nil {
			return nil, err
		}
		ks.vindexes[name] = m
	}

	for tname, t := range raw.Tables {
		if len(t.ColumnVindexes) != 1 {
			return nil, &ConfigError{Reason: fmt.Sprintf("table %q must declare exactly one column_vindex", tname)}
		}
		cv := t.ColumnVindexes[0]
		m, ok := ks.vindexes[cv.Name]
		if !ok {
			return nil, &ConfigError{Reason: fmt.Sprintf("table %q references undefined vindex %q", tname, cv.Name)}
		}
		ncols, err := columnVindexArity(cv)
		if err != nil {
			return nil, err
		}
		if ncols != m.Arity() {
			return nil, &ConfigError{Reason: fmt.Sprintf("table %q binds %d column(s) to vindex %q of arity %d", tname, ncols, cv.Name, m.Arity())}
		}
		ks.tables[tname] = cv.Name
	}

	return ks, nil
}

// validateVSchemaJSON performs the canonical checks that a typed decoder does
// not own: duplicate-member rejection and NUL-free decoded strings. The index
// is byte-backed and bounded; decoded-key hashes are computed directly from
// string tokens, so identities are never materialized as Go strings.
func validateVSchemaJSON(data []byte) error {
	needed, err := vibejson.RequiredIndexEntries(data)
	if err != nil {
		return err
	}
	index, err := vibejson.BuildIndexOptions(data, make([]vibejson.IndexEntry, needed), document.IndexOptions{
		MaxDepth: maxVSchemaJSONDepth,
		HashKeys: false,
	})
	if err != nil {
		return err
	}
	return validateVSchemaNode(index.Root())
}

func validateVSchemaNode(node vibejson.Node) error {
	switch node.Kind() {
	case document.String:
		if jsonStringContainsNUL(node) {
			return errors.New("NUL in decoded string")
		}
	case document.Array:
		iter, _ := node.ArrayIter()
		for value, ok := iter.Next(); ok; value, ok = iter.Next() {
			if err := validateVSchemaNode(value); err != nil {
				return err
			}
		}
	case document.Object:
		count, _ := node.ObjectLen()
		seen := make(map[uint32]vibejson.Node, count)
		iter, _ := node.ObjectIter()
		for key, value, ok := iter.Next(); ok; key, value, ok = iter.Next() {
			if jsonStringContainsNUL(key) {
				return errors.New("NUL in decoded object member")
			}
			hash := hashDecodedJSONString(key)
			if previous, exists := seen[hash]; exists {
				if duplicateVSchemaKey(node, key, previous) {
					return fmt.Errorf("duplicate object member %s", key.Raw().Bytes())
				}
			} else {
				seen[hash] = key
			}
			if err := validateVSchemaNode(value); err != nil {
				return err
			}
		}
	}
	return nil
}

func duplicateVSchemaKey(object, current, first vibejson.Node) bool {
	currentRaw := current.Raw().Bytes()
	if vibejson.RawJSONStringEqual(currentRaw, current.Entry.Flags(), first.Raw().Bytes(), first.Entry.Flags()) {
		return true
	}
	// A hash collision is rare, but all earlier keys in the same object must be
	// checked so a later duplicate of either colliding spelling is still closed.
	iter, _ := object.ObjectIter()
	for key, _, ok := iter.Next(); ok && key.Entry != current.Entry; key, _, ok = iter.Next() {
		if hashDecodedJSONString(key) == hashDecodedJSONString(current) &&
			vibejson.RawJSONStringEqual(currentRaw, current.Entry.Flags(), key.Raw().Bytes(), key.Entry.Flags()) {
			return true
		}
	}
	return false
}

func hashDecodedJSONString(node vibejson.Node) uint32 {
	const (
		offset32 = uint32(2166136261)
		prime32  = uint32(16777619)
	)
	raw := node.Raw().Bytes()
	hash := offset32
	if node.Entry.Flags()&vibejson.TapeFlagEscaped == 0 {
		for _, b := range raw[1 : len(raw)-1] {
			hash = (hash ^ uint32(b)) * prime32
		}
		return hash
	}
	iter := vibejson.JSONStringByteIter{Raw: raw[1 : len(raw)-1]}
	for b, ok := iter.Next(); ok; b, ok = iter.Next() {
		hash = (hash ^ uint32(b)) * prime32
	}
	return hash
}

func jsonStringContainsNUL(node vibejson.Node) bool {
	raw := node.Raw().Bytes()
	if node.Entry.Flags()&vibejson.TapeFlagEscaped == 0 {
		return bytes.IndexByte(raw[1:len(raw)-1], 0) >= 0
	}
	iter := vibejson.JSONStringByteIter{Raw: raw[1 : len(raw)-1]}
	for b, ok := iter.Next(); ok; b, ok = iter.Next() {
		if b == 0 {
			return true
		}
	}
	return false
}

// buildVindex resolves one supported vindex definition to a mapper.
func buildVindex(v rawVindex) (distribution.Mapper, error) {
	switch v.Type {
	case "":
		return nil, &ConfigError{Reason: `vindex is missing a "type"`}
	case "xxhash":
		if len(v.Params) != 0 {
			return nil, &ConfigError{Reason: "xxhash vindex accepts no params"}
		}
		return NewXXHashMapper(), nil
	case "multicol":
		return buildMultiCol(v.Params)
	default:
		return nil, &ConfigError{Reason: fmt.Sprintf("unsupported vindex type %q (only xxhash and multicol are supported)", v.Type)}
	}
}

// buildMultiCol validates the multicol params against the strict profile and
// returns the corresponding mapper. Only column_count, column_bytes, and
// column_vindex are accepted; the sub-vindexes must all be xxhash; and the
// resolved widths must total exactly 8 bytes.
func buildMultiCol(params map[string]string) (distribution.Mapper, error) {
	for k := range params {
		switch k {
		case paramColumnCount, paramColumnBytes, paramColumnVindex:
		default:
			return nil, &ConfigError{Reason: fmt.Sprintf("multicol vindex has unsupported param %q", k)}
		}
	}

	countStr, ok := params[paramColumnCount]
	if !ok {
		return nil, &ConfigError{Reason: "multicol requires the column_count param"}
	}
	count, err := strconv.Atoi(countStr)
	if err != nil {
		return nil, &ConfigError{Reason: "column_count is not an integer"}
	}
	if count < 1 || count > distribution.KeyspaceWidth {
		return nil, &ConfigError{Reason: "column_count must be between 1 and 8"}
	}

	vindexStr, ok := params[paramColumnVindex]
	if !ok {
		return nil, &ConfigError{Reason: "multicol requires the column_vindex param"}
	}
	subVindexes := strings.Split(vindexStr, ",")
	if len(subVindexes) != count {
		return nil, &ConfigError{Reason: "column_vindex must list exactly column_count sub-vindexes"}
	}
	for _, name := range subVindexes {
		if strings.TrimSpace(name) != "xxhash" {
			return nil, &ConfigError{Reason: "multicol sub-vindexes must all be xxhash"}
		}
	}

	bytesStr, present := params[paramColumnBytes]
	widths, err := resolveColumnBytes(count, bytesStr, present)
	if err != nil {
		return nil, err
	}
	return NewMultiColMapper(widths)
}

// columnVindexArity reports how many shard-key columns a table's column_vindex
// binds. It requires exactly one of the mutually exclusive "column" (single) or
// "columns" (multi) forms.
func columnVindexArity(cv rawColumnVindex) (int, error) {
	hasSingle := cv.Column != ""
	hasMulti := cv.Columns != nil
	switch {
	case hasSingle && hasMulti:
		return 0, &ConfigError{Reason: `column_vindex must set either "column" or "columns", not both`}
	case hasSingle:
		return 1, nil
	case hasMulti:
		if len(cv.Columns) == 0 {
			return 0, &ConfigError{Reason: `column_vindex "columns" must be non-empty`}
		}
		for _, c := range cv.Columns {
			if c == "" {
				return 0, &ConfigError{Reason: `column_vindex "columns" must not contain an empty name`}
			}
		}
		return len(cv.Columns), nil
	default:
		return 0, &ConfigError{Reason: `column_vindex must set "column" or "columns"`}
	}
}

// Mapper returns the distribution.Mapper bound to table, or a typed *ConfigError
// if the table is not defined in the keyspace.
func (k *Keyspace) Mapper(table string) (distribution.Mapper, error) {
	name, ok := k.tables[table]
	if !ok {
		return nil, &ConfigError{Reason: fmt.Sprintf("table %q is not defined in the keyspace", table)}
	}
	return k.vindexes[name], nil
}

// Vindex returns the distribution.Mapper for a named vindex, or a typed
// *ConfigError if it is not defined in the keyspace.
func (k *Keyspace) Vindex(name string) (distribution.Mapper, error) {
	m, ok := k.vindexes[name]
	if !ok {
		return nil, &ConfigError{Reason: fmt.Sprintf("vindex %q is not defined in the keyspace", name)}
	}
	return m, nil
}

// Tables returns the names of the tables bound in the keyspace, sorted.
func (k *Keyspace) Tables() []string {
	names := make([]string, 0, len(k.tables))
	for name := range k.tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
