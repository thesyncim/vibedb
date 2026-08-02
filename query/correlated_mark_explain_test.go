package query

import (
	"encoding/json"
	"slices"
	"testing"
)

func TestExplainGroupedCorrelatedMarksExposeAuthoredSemantics(t *testing.T) {
	outer := &plan{
		headers: []string{"id"},
		valuePaths: []compiledPath{
			{spec: "id"},
			{spec: "tenant"},
			{spec: "region"},
			{spec: "probe"},
		},
	}
	inner := &plan{valuePaths: []compiledPath{
		{spec: "tenant_id"},
		{spec: "region_id"},
		{spec: "projected_value"},
	}}
	for _, test := range []struct {
		name       string
		kind       correlatedMarkKind
		accessPath string
		probe      int
		value      int
		op         Op
		operator   string
	}{
		{"composite exists", correlatedMarkExists, "decorrelated-composite-exists-semi", -1, -1, Eq, ""},
		{"composite not exists", correlatedMarkNotExists, "decorrelated-composite-exists-anti", -1, -1, Eq, ""},
		{"correlated in", correlatedMarkIn, "decorrelated-correlated-in", 3, 2, Eq, ""},
		{"correlated not in", correlatedMarkNotIn, "decorrelated-correlated-not-in", 3, 2, Eq, ""},
		{"correlated scalar", correlatedMarkScalar, "decorrelated-correlated-scalar", 3, 2, Ge, ">="},
	} {
		t.Run(test.name, func(t *testing.T) {
			outer.marks = append(outer.marks[:0], planMark{
				collection: "inner_values",
				inner:      inner,
				outer:      []int{1, 2},
				innerKeys:  []int{0, 1},
				probe:      test.probe,
				value:      test.value,
				kind:       test.kind,
				op:         test.op,
			})
			raw, err := outer.explainJSON("outer_values", 1, ExplainOptions{})
			if err != nil {
				t.Fatal(err)
			}
			var document struct {
				Plan struct {
					Marks []struct {
						Collection string           `json:"collection"`
						Kind       string           `json:"kind"`
						AccessPath string           `json:"access_path"`
						Keys       []explainJoinKey `json:"keys"`
						KeyCount   int              `json:"key_count"`
						Probe      string           `json:"probe"`
						Value      string           `json:"value"`
						Operator   string           `json:"operator"`
					} `json:"marks"`
				} `json:"plan"`
			}
			if err := json.Unmarshal([]byte(raw), &document); err != nil {
				t.Fatalf("decode EXPLAIN %q: %v", raw, err)
			}
			if len(document.Plan.Marks) != 1 {
				t.Fatalf("marks = %+v, want one", document.Plan.Marks)
			}
			got := document.Plan.Marks[0]
			if got.Collection != "inner_values" || got.AccessPath != test.accessPath ||
				got.KeyCount != 2 || got.Operator != test.operator {
				t.Fatalf("mark = %+v, want collection=%q access=%q keys=2 operator=%q",
					got, "inner_values", test.accessPath, test.operator)
			}
			wantKeys := []explainJoinKey{
				{Left: "tenant", Right: "tenant_id"},
				{Left: "region", Right: "region_id"},
			}
			if !slices.Equal(got.Keys, wantKeys) {
				t.Fatalf("keys = %+v, want %+v", got.Keys, wantKeys)
			}
			if test.probe >= 0 {
				if got.Probe != "probe" || got.Value != "projected_value" {
					t.Fatalf("value mark = %+v, want probe/projected_value", got)
				}
			} else if got.Probe != "" || got.Value != "" {
				t.Fatalf("EXISTS mark leaked value operands: %+v", got)
			}
		})
	}
}

func TestExplainSingleKeyCorrelatedExistsKeepsLegacyJoinAccessPaths(t *testing.T) {
	inner := &plan{valuePaths: []compiledPath{{spec: "match_key"}}}
	outer := &plan{
		headers:    []string{"id"},
		valuePaths: []compiledPath{{spec: "id"}, {spec: "match_key"}},
		joins: []planJoin{{
			collection: "inner_values",
			inner:      inner,
			outerPath:  1,
			innerPath:  0,
			origin:     joinOriginDecorrelatedExists,
		}},
	}
	for _, test := range []struct {
		name string
		anti bool
		want string
	}{
		{"exists", false, "decorrelated-exists-semi"},
		{"not exists", true, "decorrelated-exists-anti"},
	} {
		t.Run(test.name, func(t *testing.T) {
			outer.joins[0].anti = test.anti
			raw, err := outer.explainJSON("outer_values", 1, ExplainOptions{})
			if err != nil {
				t.Fatal(err)
			}
			var document struct {
				Plan struct {
					Joins []struct {
						AccessPath string `json:"access_path"`
					} `json:"joins"`
					Marks []json.RawMessage `json:"marks"`
				} `json:"plan"`
			}
			if err := json.Unmarshal([]byte(raw), &document); err != nil {
				t.Fatal(err)
			}
			if len(document.Plan.Joins) != 1 ||
				document.Plan.Joins[0].AccessPath != test.want ||
				len(document.Plan.Marks) != 0 {
				t.Fatalf("EXPLAIN = %s, want one legacy %q join and no marks", raw, test.want)
			}
		})
	}
}

func TestSQLExplainSelectsGroupedMarkOnlyForNewCorrelatedShapes(t *testing.T) {
	for _, test := range []struct {
		name, source, access, kind string
	}{
		{
			"composite exists",
			`SELECT o.id FROM outer_values AS o WHERE EXISTS (` +
				`SELECT 1 FROM inner_values AS i WHERE i.tenant = o.tenant AND i.region = o.region)`,
			"decorrelated-composite-exists-semi", "exists",
		},
		{
			"direct not composite exists",
			`SELECT o.id FROM outer_values AS o WHERE NOT (EXISTS (` +
				`SELECT 1 FROM inner_values AS i WHERE i.tenant = o.tenant AND i.region = o.region))`,
			"decorrelated-composite-exists-anti", "not-exists",
		},
		{
			"correlated in",
			`SELECT o.id FROM outer_values AS o WHERE o.probe IN (` +
				`SELECT i.value FROM inner_values AS i WHERE i.tenant = o.tenant AND i.region = o.region)`,
			"decorrelated-correlated-in", "in",
		},
		{
			"correlated not in",
			`SELECT o.id FROM outer_values AS o WHERE o.probe NOT IN (` +
				`SELECT i.value FROM inner_values AS i WHERE i.tenant = o.tenant AND i.region = o.region)`,
			"decorrelated-correlated-not-in", "not-in",
		},
		{
			"correlated scalar",
			`SELECT o.id FROM outer_values AS o WHERE o.probe >= (` +
				`SELECT i.value FROM inner_values AS i WHERE i.tenant = o.tenant AND i.region = o.region)`,
			"decorrelated-correlated-scalar", "scalar",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			statement, err := PrepareStatement(test.source)
			if err != nil {
				t.Fatal(err)
			}
			defer statement.Release()
			raw, err := statement.Explain()
			if err != nil {
				t.Fatal(err)
			}
			var document struct {
				Plan struct {
					Joins []json.RawMessage `json:"joins"`
					Marks []struct {
						Kind       string `json:"kind"`
						AccessPath string `json:"access_path"`
						KeyCount   int    `json:"key_count"`
						Operator   string `json:"operator"`
					} `json:"marks"`
				} `json:"plan"`
			}
			if err := json.Unmarshal([]byte(raw), &document); err != nil {
				t.Fatal(err)
			}
			if len(document.Plan.Joins) != 0 || len(document.Plan.Marks) != 1 {
				t.Fatalf("EXPLAIN = %s, want one grouped mark and no join", raw)
			}
			mark := document.Plan.Marks[0]
			if mark.AccessPath != test.access || mark.Kind != test.kind || mark.KeyCount != 2 {
				t.Fatalf("mark = %+v, want access=%q kind=%q key_count=2",
					mark, test.access, test.kind)
			}
			if test.kind == "scalar" && mark.Operator != ">=" {
				t.Fatalf("scalar operator = %q, want >=", mark.Operator)
			}
		})
	}

	for _, anti := range []bool{false, true} {
		prefix, want := "", "decorrelated-exists-semi"
		if anti {
			prefix, want = "NOT ", "decorrelated-exists-anti"
		}
		statement, err := PrepareStatement(
			`SELECT o.id FROM outer_values AS o WHERE ` + prefix + `EXISTS (` +
				`SELECT 1 FROM inner_values AS i WHERE i.tenant = o.tenant)`,
		)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := statement.Explain()
		statement.Release()
		if err != nil {
			t.Fatal(err)
		}
		var document struct {
			Plan struct {
				Joins []struct {
					AccessPath string `json:"access_path"`
				} `json:"joins"`
				Marks []json.RawMessage `json:"marks"`
			} `json:"plan"`
		}
		if err := json.Unmarshal([]byte(raw), &document); err != nil {
			t.Fatal(err)
		}
		if len(document.Plan.Joins) != 1 ||
			document.Plan.Joins[0].AccessPath != want || len(document.Plan.Marks) != 0 {
			t.Fatalf("single-key EXPLAIN = %s, want legacy %q join only", raw, want)
		}
	}
}
