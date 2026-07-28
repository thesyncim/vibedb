package query

import (
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// These tests deliberately pin the permissive legacy JSON front end. They are
// characterization coverage for a future strict, versioned parser, not a
// recommendation that a new wire grammar copy these rules.

func TestLegacyJSONDuplicateMembersAreContextSensitive(t *testing.T) {
	t.Run("a duplicate query clause is last-wins", func(t *testing.T) {
		q, err := Parse([]byte(`{"select":"first","select":"second"}`))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}

		got := mustRun(t, q, mustSegment(t, `{"first":1,"second":2}`))
		if len(got.Columns) != 1 || got.Columns[0].Header != "second" {
			t.Fatalf("columns = %+v, want only the last select clause", got.Columns)
		}
		if json := string(got.Columns[0].Cells[0].JSON()); json != "2" {
			t.Fatalf("selected value = %s, want 2 from the last select clause", json)
		}
	})

	t.Run("duplicate filter paths conjoin", func(t *testing.T) {
		q, err := Parse([]byte(`{"select":"a","where":{"a":1,"a":2}}`))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}

		got := mustRun(t, q, mustSegment(t, `{"a":1}`, `{"a":2}`))
		if got.RowCount != 0 {
			t.Fatalf("RowCount = %d, want 0: both duplicate path predicates must apply", got.RowCount)
		}
	})
}

func TestLegacyJSONNullClausesMeanOmittedClauses(t *testing.T) {
	omitted, err := Parse([]byte(`{}`))
	if err != nil {
		t.Fatalf("Parse omitted clauses: %v", err)
	}
	explicitNull, err := Parse([]byte(`{
		"select":null,
		"where":null,
		"join":null,
		"groupBy":null,
		"orderBy":null,
		"limit":null
	}`))
	if err != nil {
		t.Fatalf("Parse null clauses: %v", err)
	}

	set := mustSegment(t, `{"a":1}`, `{"a":null}`, `{}`)
	got := mustRun(t, explicitNull, set)
	want := mustRun(t, omitted, set)
	if diff := diffResults(got, want); diff != "" {
		t.Fatalf("explicit null clauses differ from omitted clauses: %s", diff)
	}
}

func TestLegacyJSONParsePreservesExactFractionalLiterals(t *testing.T) {
	set := mustSegment(t,
		`{"v":1.0000000000000000}`,
		`{"v":1.0000000000000001}`,
	)
	q, err := Parse([]byte(`{
		"select":"v",
		"where":{"v":1.0000000000000001}
	}`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	got := mustRun(t, q, set)
	if got.RowCount != 1 {
		t.Fatalf("RowCount = %d, want 1: distinct exact decimals must not collapse through float64", got.RowCount)
	}
	column, ok := got.Column("v")
	if !ok {
		t.Fatal("result has no v column")
	}
	if json := string(column.Cells[0].JSON()); json != "1.0000000000000001" {
		t.Fatalf("matched value = %s, want its exact decimal spelling", json)
	}
}

func TestLegacyJSONDecodesEscapedMemberNamesAndValues(t *testing.T) {
	q, err := Parse([]byte(`{
		"sel\u0065ct":"/value",
		"where":{"/tag":"m\u0061tch"}
	}`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	got := mustRun(t, q, mustSegment(t,
		`{"value":1,"tag":"match"}`,
		`{"value":2,"tag":"other"}`,
	))
	if got.RowCount != 1 {
		t.Fatalf("RowCount = %d, want 1", got.RowCount)
	}
	column, ok := got.Column("/value")
	if !ok {
		t.Fatal("result has no /value column")
	}
	if json := string(column.Cells[0].JSON()); json != "1" {
		t.Fatalf("selected value = %s, want 1", json)
	}
}

func TestLegacyJSONDottedAndPointerPathsKeepTheirIdentity(t *testing.T) {
	q, err := Parse([]byte(`{"select":["a.b","/a.b","/a/b"]}`))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	got := mustRun(t, q, mustSegment(t,
		`{"a":{"b":"nested"},"a.b":"literal dotted key"}`,
	))
	want := map[string]string{
		"a.b":  "nested",
		"/a.b": "literal dotted key",
		"/a/b": "nested",
	}
	for header, text := range want {
		column, ok := got.Column(header)
		if !ok {
			t.Fatalf("result has no %q column", header)
		}
		value, ok := column.Cells[0].Text()
		if !ok || value != text {
			t.Fatalf("%s = (%q, %v), want %q", header, value, ok, text)
		}
	}
}

func TestLegacyJSONJoinHasNoImplicitAlias(t *testing.T) {
	db := &store.Database{}
	outer, err := db.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatalf("CreateCollection outer: %v", err)
	}
	if _, err := outer.Put("o0", []byte(
		`{"ref":"a","customers":{"seat":99}}`,
	)); err != nil {
		t.Fatalf("Put outer: %v", err)
	}
	inner, err := db.CreateCollection("customers", store.Options{})
	if err != nil {
		t.Fatalf("CreateCollection customers: %v", err)
	}
	if _, err := inner.Put("i0", []byte(`{"code":"a","seat":7}`)); err != nil {
		t.Fatalf("Put inner: %v", err)
	}
	catalog := db.Snapshot()

	cases := []struct {
		name string
		as   string
		want string
	}{
		{"absent", "", "99"},
		{"null", `,"as":null`, "99"},
		{"explicit", `,"as":"customers"`, "7"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := `{
				"select":"customers.seat",
				"join":{
					"from":"customers",
					"on":{"ref":"code"}` + tc.as + `
				}
			}`
			q, err := Parse([]byte(src))
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			got, err := q.Run(FromDatabase(catalog, "outer"))
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			column, ok := got.Column("customers.seat")
			if !ok {
				t.Fatal("result has no customers.seat column")
			}
			if json := string(column.Cells[0].JSON()); json != tc.want {
				t.Fatalf("customers.seat = %s, want %s", json, tc.want)
			}
		})
	}
}
