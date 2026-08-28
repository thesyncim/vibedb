package pgwire

import (
	"strings"
	"testing"
)

func TestJDBCPublicTablesUsesCatalogAndRejectsNearMisses(t *testing.T) {
	c := connectIntrospectionCatalog(t)
	oids := make([]int32, 10)
	for i := range oids {
		oids[i] = oidText
	}
	expectShimResult(t, c.query(jdbcPublicTables), []string{"table_cat", "table_schem", "table_name", "table_type", "remarks", "type_cat", "type_schem", "type_name", "self_referencing_col_name", "ref_generation"}, oids,
		[][]any{{nil, "public", "orders", "TABLE", nil, "", "", "", "", ""}, {nil, "public", "users", "TABLE", nil, "", "", "", "", ""}})
	for _, sql := range []string{jdbcPublicTables + " LIMIT 1", strings.Replace(jdbcPublicTables, "LIKE 'public'", "LIKE 'private'", 1), strings.Replace(jdbcPublicTables, "LIKE '%'", "LIKE 'orders'", 1)} {
		if _, _, ok := recognizeCatalogQuery(sql); ok {
			t.Fatal("recognized changed JDBC semantics")
		}
	}
}
