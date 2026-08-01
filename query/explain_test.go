package query

import (
	"strings"
	"testing"

	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibejson"
)

func TestExplainUsesCompiledPlanWithoutADataSource(t *testing.T) {
	tree, err := sqlast.Parse(`SELECT name FROM docs WHERE kind = 'active' ORDER BY name DESC LIMIT 10`)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := PrepareParsedStatement("test", tree)
	if err != nil {
		t.Fatal(err)
	}
	defer statement.Release()

	plan, err := statement.Explain()
	if err != nil {
		t.Fatal(err)
	}
	if !vibejson.Valid([]byte(plan)) {
		t.Fatalf("explain is not JSON: %s", plan)
	}
	for _, want := range []string{
		`"node":"scan"`,
		`"collection":"docs"`,
		`"access_path":"adaptive-posting-or-scan"`,
		`"order_by":["name DESC"]`,
		`"limit":10`,
	} {
		if !strings.Contains(plan, want) {
			t.Errorf("explain missing %s: %s", want, plan)
		}
	}
}
