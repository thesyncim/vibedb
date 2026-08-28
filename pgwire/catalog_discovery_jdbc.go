package pgwire

import (
	"strconv"
	"strings"

	sqlast "github.com/thesyncim/vibedb/sql"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// JDBC sometimes processes these raw rows into its own DatabaseMetaData
// result. In particular getColumns expects pg_attribute fields, not the final
// JDBC COLUMN_NAME/DATA_TYPE layout. Keep the captured wire contract exact.
func (s *session) answerJDBCDiscovery(shape *discoveryShape, f discoveryFilter, a *catalogAnswer, args []any) (*fixedResult, error) {
	cols := discoveryResultColumns(shape)
	var rows [][]*string
	var buildErr error
	var bytes int64
	add := func(v ...*string) {
		if buildErr != nil {
			return
		}
		if buildErr = s.cancellationError(); buildErr != nil {
			return
		}
		charge := int64(24 + len(v)*24)
		for _, x := range v {
			if x != nil {
				charge += int64(len(*x))
			}
		}
		if (s.server.opts.MaxResultRows >= 0 && len(rows) >= s.server.opts.MaxResultRows) || (s.server.opts.MaxResultBytes >= 0 && charge > s.server.opts.MaxResultBytes-bytes) {
			buildErr = newError(sqlstateProgramLimitExceeded, "catalog discovery exceeds its result budget")
			return
		}
		bytes += charge
		rows = append(rows, v)
	}
	row := func(v ...string) {
		r := make([]*string, len(v))
		for i, x := range v {
			r[i] = strPtr(x)
		}
		add(r...)
	}
	switch shape.Name {
	case "JDBC catalogs":
		row(a.database)
	case "JDBC schemas", "JDBC public schemas":
		if !f.hasSchemaName || discoveryLike("public", f.schemaName) {
			add(strPtr("public"), nil)
		}
	case "JDBC all tables":
		for _, t := range a.tables {
			if !f.includes(t.Name) {
				continue
			}
			add(nil, strPtr("public"), strPtr(t.Name), strPtr("TABLE"), nil, strPtr(""), strPtr(""), strPtr(""), strPtr(""), strPtr(""))
		}
	case "JDBC columns documents":
		for _, t := range a.tables {
			if !f.includes(t.Name) {
				continue
			}
			for i, c := range discoveryColumns(&t) {
				name := pointerDisplayName(c.Path)
				if f.hasColumnPattern && !discoveryLike(name, f.columnPattern) {
					continue
				}
				notnull := "f"
				if c.Required && c.Types&sqlast.TypeNull == 0 {
					notnull = "t"
				}
				add(strPtr("public"), strPtr(t.Name), strPtr(name), strPtr("114"), strPtr(notnull), strPtr("-1"), strPtr("-1"), strPtr("-1"), strPtr(strconv.Itoa(i+1)), nil, nil, nil, nil, strPtr("0"), strPtr("b"))
			}
		}
	case "JDBC primary keys":
		for _, t := range a.tables {
			if f.includes(t.Name) {
				add(nil, strPtr("public"), strPtr(t.Name), strPtr(pointerDisplayName(t.PrimaryKey)), strPtr("1"), strPtr(t.Name+"_pkey"))
			}
		}
	case "JDBC indexes":
		uniqueOnly := strings.Contains(strings.Join(shape.tokens, " "), "and i . indisunique")
		for _, t := range a.tables {
			if !f.includes(t.Name) {
				continue
			}
			indexes := append([]sqldriver.IndexInfo{{Name: t.Name + "_pkey", Paths: []string{t.PrimaryKey}}}, t.Indexes...)
			for i, index := range indexes {
				if uniqueOnly && i > 0 {
					continue
				}
				nonunique := "t"
				if i == 0 {
					nonunique = "f"
				}
				for k, path := range index.Paths {
					add(nil, strPtr("public"), strPtr(t.Name), strPtr(nonunique), nil, strPtr(index.Name), strPtr("3"), strPtr(strconv.Itoa(k+1)), strPtr(pointerDisplayName(path)), nil, nil, nil, nil)
				}
			}
		}
	case "JDBC best row identifier":
		for _, t := range a.tables {
			if f.includes(t.Name) {
				row(pointerDisplayName(t.PrimaryKey), "114", "-1")
			}
		}
	case "JDBC imported keys":
		row("4") // supported exact-index key arity
	case "JDBC client info properties":
		row("-1") // no PostgreSQL name-width constraint
	case "JDBC type info":
		for _, t := range []struct{ name, oid string }{{"bool", "16"}, {"int8", "20"}, {"int4", "23"}, {"text", "25"}, {"json", "114"}, {"numeric", "1700"}} {
			row(t.name, t.oid)
		}
	case "JDBC type cache":
		for _, t := range []struct{ name, oid string }{{"bool", "16"}, {"int8", "20"}, {"int4", "23"}, {"text", "25"}, {"json", "114"}, {"numeric", "1700"}} {
			row("f", "b", t.name, t.oid)
		}
	case "JDBC version columns":
		// There is no xmin/MVCC system column. No such PostgreSQL type is
		// exported by this catalog; returning a synthetic xid would make JDBC
		// advertise a row-version column the query engine cannot read.
	default:
		// No role/column grants, user types, stored functions or procedures.
	}
	if buildErr != nil {
		return nil, buildErr
	}
	return catalogResult(cols, rows), nil
}

func jdbcDiscoveryColumnTypes(cols []column) {
	for i := range cols {
		switch cols[i].name {
		case "atttypid", "atttypmod", "attlen", "typtypmod", "attnum", "typbasetype", "key_seq", "ordinal_position", "cardinality", "pages", "data_type", "base_type", "function_type", "procedure_type", "typlen", "oid", "type":
			cols[i].typ = typeInt8
		case "attnotnull", "non_unique", "is_array":
			cols[i].typ = typeBool
		}
	}
}
