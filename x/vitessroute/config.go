package vitessroute

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/thesyncim/vibedb/distribution"
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
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var raw rawKeyspace
	if err := dec.Decode(&raw); err != nil {
		return nil, &ConfigError{Reason: "malformed VSchema JSON: " + err.Error()}
	}
	if dec.More() {
		return nil, &ConfigError{Reason: "trailing data after the VSchema JSON object"}
	}

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
