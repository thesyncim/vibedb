package pgwire

import (
	"context"
	_ "embed"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// These are the installed JetBrains 2026.2 PostgreSQL-16 full/fragment
// introspection requests. They are a closed protocol contract, not a SQL
// evaluator. Whitespace/comments may vary; every semantic token must match.
// Only schema IDs and explicitly marked relation-name lists are captured.
// Recognition runs exclusively after normal SQL preparation refuses a query.
//
//go:embed catalog_goland_queries.json
var golandQueriesJSON string

//go:embed catalog_jdbc_queries.json
var jdbcQueriesJSON string

// Incremental requests are discovery refresh hints, not PostgreSQL MVCC:
// return the complete bounded snapshot, with NULL object state numbers.
//
//go:embed catalog_goland_incremental_queries.json
var golandIncrementalQueriesJSON string

//go:embed catalog_goland_aux_queries.json
var golandAuxQueriesJSON string

type discoveryShape struct {
	Name    string
	SQL     string
	Columns []string
	tokens  []string
}

type discoveryQuery struct {
	shape  *discoveryShape
	filter discoveryFilter
	tokens []string
}

var discoveryShapes = func() []discoveryShape {
	var shapes []discoveryShape
	if err := json.Unmarshal([]byte(golandQueriesJSON), &shapes); err != nil {
		panic(err)
	}
	var jdbc []discoveryShape
	if err := json.Unmarshal([]byte(jdbcQueriesJSON), &jdbc); err != nil {
		panic(err)
	}
	shapes = append(shapes, jdbc...)
	var incremental []discoveryShape
	if err := json.Unmarshal([]byte(golandIncrementalQueriesJSON), &incremental); err != nil {
		panic(err)
	}
	shapes = append(shapes, incremental...)
	var aux []discoveryShape
	if err := json.Unmarshal([]byte(golandAuxQueriesJSON), &aux); err != nil {
		panic(err)
	}
	shapes = append(shapes, aux...)
	for i := range shapes {
		shapes[i].tokens, _ = discoveryTokens(shapes[i].SQL, nil)
	}
	return shapes
}()

// A finite scanner, deliberately incapable of accepting unrecognized SQL.
// Quoted tokens keep their case and escapes; comments cannot join tokens.
func discoveryTokens(sql string, check func() error) ([]string, error) {
	if len(sql) > 128<<10 {
		return nil, nil
	}
	var out []string
	for i := 0; i < len(sql); {
		if check != nil {
			if err := check(); err != nil {
				return nil, err
			}
		}
		c, start := sql[i], i
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
			i++
			continue
		}
		if strings.HasPrefix(sql[i:], "--") {
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if strings.HasPrefix(sql[i:], "/*") {
			i += 2
			depth := 1
			for i < len(sql) && depth > 0 {
				if strings.HasPrefix(sql[i:], "/*") {
					depth++
					i += 2
				} else if strings.HasPrefix(sql[i:], "*/") {
					depth--
					i += 2
				} else {
					i++
				}
			}
			if depth != 0 {
				return nil, nil
			}
			continue
		}
		if c == '\'' || c == '"' {
			i++
			closed := false
			for i < len(sql) {
				if sql[i] == c {
					i++
					if i < len(sql) && sql[i] == c {
						i++
						continue
					}
					closed = true
					break
				}
				i++
			}
			if !closed {
				return nil, nil
			}
			out = append(out, sql[start:i])
			continue
		}
		if c == '$' && i+1 < len(sql) && sql[i+1] >= '0' && sql[i+1] <= '9' {
			i += 2
			for i < len(sql) && sql[i] >= '0' && sql[i] <= '9' {
				i++
			}
			out = append(out, sql[start:i])
			continue
		}
		marker := ""
		for _, m := range []string{":schema_name", ":table_name", ":table_pattern", ":column_pattern", ":state"} {
			if strings.HasPrefix(sql[i:], m) {
				marker = m
				break
			}
		}
		if marker != "" {
			out = append(out, marker)
			i += len(marker)
			continue
		}
		if strings.HasPrefix(sql[i:], ":[*f_names]") {
			out = append(out, ":[*f_names]")
			i += len(":[*f_names]")
			continue
		}
		if strings.HasPrefix(sql[i:], ":[*schema_ids]") {
			out = append(out, ":[*schema_ids]")
			i += len(":[*schema_ids]")
			continue
		}
		if strings.HasPrefix(sql[i:], ":schema_id") {
			out = append(out, ":schema_id")
			i += len(":schema_id")
			continue
		}
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c == '_' || c >= '0' && c <= '9' {
			i++
			for i < len(sql) && (sql[i] >= 'a' && sql[i] <= 'z' || sql[i] >= 'A' && sql[i] <= 'Z' || sql[i] >= '0' && sql[i] <= '9' || sql[i] == '_' || sql[i] == '$') {
				i++
			}
			out = append(out, strings.ToLower(sql[start:i]))
			continue
		}
		if strings.HasPrefix(sql[i:], "::") {
			out = append(out, "::")
			i += 2
			continue
		}
		out = append(out, sql[i:i+1])
		i++
	}
	if len(out) > 0 && out[len(out)-1] == ";" {
		out = out[:len(out)-1]
	}
	return out, nil
}

type discoveryFilter struct {
	schema                                                         string
	names                                                          []string
	schemaName, tableName, tablePattern, columnPattern             string
	hasSchemaName, hasTableName, hasTablePattern, hasColumnPattern bool
}

func matchDiscovery(tokens, pattern []string) (discoveryFilter, bool) {
	var f discoveryFilter
	i := 0
	for _, want := range pattern {
		if i >= len(tokens) {
			return f, false
		}
		switch want {
		case ":state":
			v := strings.Trim(tokens[i], "'")
			if v != "null" && !strings.HasPrefix(v, "$") {
				if _, err := strconv.ParseUint(v, 10, 32); err != nil {
					return f, false
				}
			}
			i++
		case ":schema_name", ":table_name", ":table_pattern", ":column_pattern":
			t := tokens[i]
			if len(t) < 2 || t[0] != '\'' {
				return f, false
			}
			v := strings.ReplaceAll(t[1:len(t)-1], "''", "'")
			switch want {
			case ":schema_name":
				f.schemaName = v
				f.hasSchemaName = true
			case ":table_name":
				f.tableName = v
				f.hasTableName = true
			case ":table_pattern":
				f.tablePattern = v
				f.hasTablePattern = true
			case ":column_pattern":
				f.columnPattern = v
				f.hasColumnPattern = true
			}
			i++
		case ":schema_id", ":[*schema_ids]":
			if strings.HasPrefix(tokens[i], "$") {
				i++
				continue
			}
			v := strings.Trim(tokens[i], "'")
			if _, err := strconv.ParseUint(v, 10, 32); err != nil {
				return f, false
			}
			if f.schema != "" && f.schema != v {
				return f, false
			}
			f.schema = v
			i++
		case ":[*f_names]":
			// JetBrains renders an empty fragment selection as IN (NULL).
			// It matches no relation, not every relation.
			if tokens[i] == "null" {
				if f.names != nil && len(f.names) != 0 {
					return f, false
				}
				f.names = []string{}
				i++
				continue
			}
			var names []string
			unbound := false
			for {
				if i < len(tokens) && strings.HasPrefix(tokens[i], "$") {
					unbound = true
					names = append(names, tokens[i])
					i++
					if i >= len(tokens) || tokens[i] != "," {
						break
					}
					i++
					continue
				}
				if i >= len(tokens) || len(tokens[i]) < 2 || tokens[i][0] != '\'' {
					return f, false
				}
				names = append(names, strings.ReplaceAll(tokens[i][1:len(tokens[i])-1], "''", "'"))
				i++
				if i >= len(tokens) || tokens[i] != "," {
					break
				}
				i++
			}
			if unbound {
				continue
			} // compare values after Bind, not parameter numbers
			if f.names != nil && strings.Join(f.names, "\x00") != strings.Join(names, "\x00") {
				return f, false
			}
			f.names = names
		default:
			if tokens[i] != want {
				return f, false
			}
			i++
		}
	}
	return f, i == len(tokens)
}

func (f discoveryFilter) includes(name string) bool {
	if f.hasSchemaName && !discoveryLike("public", f.schemaName) {
		return false
	}
	if f.hasTableName && f.tableName != name {
		return false
	}
	if f.hasTablePattern && !discoveryLike(name, f.tablePattern) {
		return false
	}
	if f.schema != "" && f.schema != "2200" {
		return false
	}
	if f.names == nil {
		return true
	}
	for _, n := range f.names {
		if n == name {
			return true
		}
	}
	return false
}

func (s *session) discoveryShim(text string, check func() error) (*fixedResult, bool, error) {
	tokens, err := discoveryTokens(text, check)
	if err != nil {
		return nil, false, err
	}
	if len(tokens) == 0 {
		return nil, false, nil
	}
	for i := range discoveryShapes {
		shape := &discoveryShapes[i]
		if check != nil {
			if err := check(); err != nil {
				return nil, false, err
			}
		}
		filter, ok := matchDiscovery(tokens, shape.tokens)
		if !ok {
			continue
		}
		return &fixedResult{cols: discoveryResultColumns(shape), discovery: &discoveryQuery{shape: shape, filter: filter, tokens: tokens}}, true, nil
	}
	return nil, false, nil
}

func (s *session) executeDiscovery(q *discoveryQuery, args []any) (*fixedResult, error) {
	f := q.filter
	if !strings.HasPrefix(q.shape.Name, "JDBC ") && len(args) > 0 {
		tokens := append([]string(nil), q.tokens...)
		for i, v := range tokens {
			if len(v) > 1 && v[0] == '$' {
				n, e := strconv.Atoi(v[1:])
				if e != nil || n < 1 || n > len(args) {
					return nil, newError(sqlstateInvalidParameterValue, "invalid metadata parameter")
				}
				if args[n-1] == nil {
					tokens[i] = "null"
					continue
				}
				value, ok := discoveryArgument(args[n-1])
				if !ok {
					return nil, newError(sqlstateInvalidParameterValue, "metadata parameter must be a string or integer")
				}
				tokens[i] = "'" + strings.ReplaceAll(value, "'", "''") + "'"
			}
		}
		var ok bool
		f, ok = matchDiscovery(tokens, q.shape.tokens)
		if !ok {
			return nil, newError(sqlstateInvalidParameterValue, "metadata arguments do not match the discovery contract")
		}
	}
	tables, err := s.sql.Tables(context.Background())
	if err != nil {
		return nil, err
	}
	if len(tables) > 16384 {
		return nil, newError(sqlstateProgramLimitExceeded, "catalog discovery exceeds its table bound")
	}
	objects := 0
	for _, t := range tables {
		objects += len(t.Columns) + len(t.Indexes) + 2
		if objects > 65536 {
			return nil, newError(sqlstateProgramLimitExceeded, "catalog discovery exceeds its object bound")
		}
	}
	oids := make(map[string]uint32, len(tables))
	for _, t := range tables {
		oid := s.server.discoveryOID(t.Name)
		if oid == 0 {
			return nil, newError(sqlstateProgramLimitExceeded, "catalog identity budget exhausted")
		}
		oids[t.Name] = oid
		for _, idx := range append([]sqldriver.IndexInfo{{Name: t.Name + "_pkey"}}, t.Indexes...) {
			if s.server.discoveryOID("index\x00"+t.Name+"\x00"+idx.Name) == 0 {
				return nil, newError(sqlstateProgramLimitExceeded, "catalog identity budget exhausted")
			}
		}
	}
	a := catalogAnswer{database: s.database, user: s.user, tables: tables, oidMap: oids}
	if strings.HasPrefix(q.shape.Name, "JDBC ") {
		return s.answerJDBCDiscovery(q.shape, q.filter, &a, args)
	}
	return s.answerDiscovery(q.shape, f, &a)
}

func discoveryArgument(v any) (string, bool) {
	switch v := v.(type) {
	case query.Number:
		return string(v), true
	case *query.Number:
		return string(*v), true
	case string:
		return v, true
	case *string:
		return *v, true
	case int64:
		return strconv.FormatInt(v, 10), true
	case *int64:
		return strconv.FormatInt(*v, 10), true
	}
	return "", false
}

// JDBC metadata patterns use SQL LIKE, not glob or regular expressions.
// This bounded dynamic program has no exponential wildcard backtracking.
func discoveryLike(value, pattern string) bool {
	if pattern == "%" {
		return true
	}
	if !strings.ContainsAny(pattern, "%_\\") {
		return value == pattern
	}
	v, p := []rune(value), []rune(pattern)
	if len(v) > 4096 || len(p) > 4096 {
		return false
	}
	row := make([]bool, len(v)+1)
	row[0] = true
	for i := 0; i < len(p); i++ {
		c := p[i]
		literal := false
		if c == '\\' {
			i++
			if i == len(p) {
				return false
			}
			c = p[i]
			literal = true
		}
		if c == '%' && !literal {
			for j := 1; j <= len(v); j++ {
				row[j] = row[j] || row[j-1]
			}
			continue
		}
		for j := len(v); j > 0; j-- {
			row[j] = row[j-1] && (c == v[j-1] || c == '_' && !literal)
		}
		row[0] = false
	}
	return row[len(v)]
}

// Schemaless tables expose their real key and the whole-document pseudo-column,
// never fields inferred by scanning arbitrary data. All path projections are
// JSON on this wire adapter, including paths constrained to a scalar type.
func discoveryColumns(t *sqldriver.TableInfo) []sqldriver.ColumnInfo {
	cols := append([]sqldriver.ColumnInfo(nil), t.Columns...)
	found := false
	for _, c := range cols {
		if c.Path == t.PrimaryKey {
			found = true
		}
	}
	if !found && t.PrimaryKey != "" {
		cols = append(cols, sqldriver.ColumnInfo{Path: t.PrimaryKey, Required: true})
	}
	return append(cols, sqldriver.ColumnInfo{Path: "/$doc", Required: true})
}

func discoveryResultColumns(shape *discoveryShape) []column {
	cols := textCols(shape.Columns...)
	if shape.Name == "GoLand search path" {
		cols[1].typ = columnType{oid: 1009, size: -1}
	}
	if strings.HasPrefix(shape.Name, "JDBC ") {
		jdbcDiscoveryColumnTypes(cols)
		return cols
	}
	// Numbers and booleans have truthful wire types. State numbers are NULL:
	// this adapter has no PostgreSQL transaction IDs.
	for i := range cols {
		n := cols[i].name
		if strings.HasSuffix(n, "_id") || strings.HasSuffix(n, "_number") || n == "id" || n == "oid" || n == "indexrelid" || n == "schemaid" || n == "majoroid" || n == "position" || n == "column_position" || n == "col_idx" || n == "type_mod" || n == "column_options" || n == "startup_time" || n == "current_txid" {
			cols[i].typ = typeInt8
		}
		if strings.HasPrefix(n, "is_") || strings.HasPrefix(n, "can_") || n == "usesuper" || n == "mandatory" || n == "in_key" || n == "allow_connections" || n == "table_with_oids" || n == "column_is_inherited" || n == "column_is_dropped" || n == "nulls_not_distinct" || n == "no_inherit" {
			cols[i].typ = typeBool
		}
	}
	return cols
}

func (s *session) answerDiscovery(shape *discoveryShape, f discoveryFilter, a *catalogAnswer) (*fixedResult, error) {
	cols := discoveryResultColumns(shape)
	var rows [][]*string
	var buildErr error
	var bytes int64
	add := func(values map[string]string) {
		if buildErr != nil {
			return
		}
		if err := s.cancellationError(); err != nil {
			buildErr = err
			return
		}
		charge := int64(24 + len(cols)*16)
		for _, v := range values {
			charge += int64(len(v))
		}
		if (s.server.opts.MaxResultRows >= 0 && len(rows) >= s.server.opts.MaxResultRows) || (s.server.opts.MaxResultBytes >= 0 && charge > s.server.opts.MaxResultBytes-bytes) {
			buildErr = newError(sqlstateProgramLimitExceeded, "catalog discovery exceeds its result budget")
			return
		}
		bytes += charge
		row := make([]*string, len(cols))
		for i, c := range cols {
			if v, ok := values[c.name]; ok {
				row[i] = strPtr(v)
			}
		}
		rows = append(rows, row)
	}
	switch shape.Name {
	case "GoLand search path":
		add(map[string]string{"a": a.database, "b": "{public}"})
	case "ServerStartupTime":
		add(map[string]string{"startup_time": strconv.FormatInt((s.server.started.UnixMilli()+500)/1000, 10)})
	case "IsSuperUser":
		add(map[string]string{"usesuper": "f"})
	case "ListDatabases":
		add(map[string]string{"id": "1", "name": a.database, "is_template": "f", "allow_connections": "t", "owner": a.user})
	case "ListSchemas":
		add(map[string]string{"id": "2200", "name": "public", "owner": a.user})
	case "StateNumber", "CurrentXid":
		add(nil)
	case "RetrieveTimeZones":
		add(map[string]string{"name": "UTC", "is_dst": "f"})
	case "RetrieveAccessMethods":
		add(map[string]string{"access_method_id": "9000", "access_method_name": "exact", "access_method_type": "i"})
	case "RetrieveExistentAccessMethods":
		add(map[string]string{"oid": "9000"})
	case "RetrieveExistentTables", "RetrieveTables", "L1_ListAllExistentObjects", "L1_ListAllMajorNames", "RetrieveColumns", "L1_RetrieveColumnNames", "RetrieveExistentIndices", "RetrieveIndices", "RetrieveIndexColumns", "RetrieveExistentConstraints", "RetrieveConstraints":
		for ti := range a.tables {
			t := &a.tables[ti]
			if !f.includes(t.Name) {
				continue
			}
			oid, _ := a.oidByName(t.Name)
			id := strconv.FormatUint(uint64(oid), 10)
			switch shape.Name {
			case "RetrieveExistentTables":
				add(map[string]string{"oid": id})
			case "RetrieveTables":
				add(map[string]string{"table_kind": "r", "table_name": t.Name, "table_id": id, "table_with_oids": "f", "tablespace_id": "0", "persistence": "p", "is_partition": "f", "am_id": "0", "owner": a.user})
			case "L1_ListAllExistentObjects", "L1_ListAllMajorNames":
				add(map[string]string{"oid": id, "schemaid": "2200", "kind": "r", "name": t.Name})
			case "RetrieveColumns", "L1_RetrieveColumnNames":
				for i, c := range discoveryColumns(t) {
					pos := strconv.Itoa(i + 1)
					mandatory := "f"
					if c.Required && c.Types&sqlast.TypeNull == 0 {
						mandatory = "t"
					}
					add(map[string]string{"table_id": id, "column_position": pos, "column_name": pointerDisplayName(c.Path), "type_mod": "-1", "dimensions_number": "0", "type_spec": "json", "type_id": "114", "mandatory": mandatory, "column_is_inherited": "f", "column_is_dropped": "f", "identity_kind": "", "generated": "", "schemaid": "2200", "majoroid": id, "kind": "r", "position": pos, "name": pointerDisplayName(c.Path)})
				}
			default:
				indices := append([]sqldriver.IndexInfo{{Name: t.Name + "_pkey", Paths: []string{t.PrimaryKey}}}, t.Indexes...)
				for ii, index := range indices {
					// The table's reserved block separates index/constraint identities.
					indexID := strconv.FormatUint(uint64(s.server.discoveryOID("index\x00"+t.Name+"\x00"+index.Name)), 10)
					primary := "f"
					if ii == 0 {
						primary = "t"
					}
					switch shape.Name {
					case "RetrieveExistentIndices":
						add(map[string]string{"indexrelid": indexID})
					case "RetrieveIndices":
						add(map[string]string{"table_id": id, "table_kind": "r", "index_name": index.Name, "index_id": indexID, "is_unique": primary, "is_primary": primary, "nulls_not_distinct": "f", "tablespace_id": "0", "access_method_id": "9000"})
					case "RetrieveIndexColumns":
						for k, path := range index.Paths {
							for ci, c := range discoveryColumns(t) {
								if c.Path == path {
									add(map[string]string{"index_id": indexID, "col_idx": strconv.Itoa(k + 1), "in_key": "t", "column_position": strconv.Itoa(ci + 1), "column_options": "0", "can_order": "f"})
									break
								}
							}
						}
					case "RetrieveExistentConstraints":
						if ii == 0 {
							add(map[string]string{"oid": indexID})
						}
					case "RetrieveConstraints":
						if ii == 0 {
							pos := 0
							for ci, c := range discoveryColumns(t) {
								if c.Path == t.PrimaryKey {
									pos = ci + 1
								}
							}
							add(map[string]string{"table_id": id, "table_kind": "r", "con_id": indexID, "con_name": index.Name, "con_kind": "p", "con_columns": "{" + strconv.Itoa(pos) + "}", "index_id": indexID, "ref_table_id": "0", "is_deferrable": "f", "is_init_deferred": "f", "is_not_enforced": "f", "is_not_validated": "f", "is_defined_with_period": "f", "no_inherit": "t", "on_update": "a", "on_delete": "a", "match_type": "s"})
						}
					}
				}
			}
		}
		// All other recognized requests describe object classes VibeDB does not
		// have: routines, views, foreign keys, triggers, roles, sequences, etc.
		// Their exact column contract is retained, with zero fabricated objects.
	}
	if buildErr != nil {
		return nil, buildErr
	}
	return catalogResult(cols, rows), nil
}

func (s *Server) discoveryOID(name string) uint32 {
	s.catalogMu.Lock()
	defer s.catalogMu.Unlock()
	if oid := s.catalogIDs[name]; oid != 0 {
		return oid
	}
	if len(s.catalogIDs) >= 65536 || len(name) > (4<<20)-s.catalogNameBytes {
		return 0
	}
	if s.catalogIDs == nil {
		s.catalogIDs = make(map[string]uint32)
		s.catalogNext = firstCatalogTableOID
	}
	oid := s.catalogNext
	s.catalogNext++
	s.catalogIDs[strings.Clone(name)] = oid
	s.catalogNameBytes += len(name)
	return oid
}
