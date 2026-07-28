package query

import (
	"errors"
	"testing"
)

func TestNativeLiteralSubsetMissingNullSemantics(t *testing.T) {
	set := mustSegment(t,
		`{"id":1,"a":1}`,
		`{"id":2,"a":2}`,
		`{"id":3,"a":null}`,
		`{"id":4}`,
	)
	cases := []struct {
		name  string
		where string
		want  []string
	}{
		{"equal", `{"/a":{"$eq":1}}`, []string{"1"}},
		{"not equal", `{"/a":{"$ne":1}}`, []string{"2", "3", "4"}},
		{"bare null equality", `{"/a":null}`, []string{"3"}},
		{"explicit null equality", `{"/a":{"$eq":null}}`, []string{"3"}},
		{"not null", `{"/a":{"$ne":null}}`, []string{"1", "2", "4"}},
		{"membership with null", `{"/a":{"$in":[1,null]}}`, []string{"1", "3"}},
		{"negated membership", `{"/a":{"$nin":[1,null]}}`, []string{"2", "4"}},
		{"exists", `{"/a":{"$exists":true}}`, []string{"1", "2", "3"}},
		{"explicit null", `{"/a":{"$null":true}}`, []string{"3"}},
		{"missing", `{"/a":{"$missing":true}}`, []string{"4"}},
		{"nullish", `{"/a":{"$nullish":true}}`, []string{"3", "4"}},
		{"boolean not", `{"$not":{"/a":{"$eq":1}}}`, []string{"2", "3", "4"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := []byte(`{
				"dialect":"vibedb-query",
				"version":1,
				"from":"docs",
				"where":` + tc.where + `,
				"select":[{"name":"id","path":"/id"}]
			}`)
			prepared, err := prepareNativeQuery(src)
			if err != nil {
				t.Fatalf("prepareNativeQuery: %v", err)
			}
			if prepared.from != "docs" {
				t.Fatalf("from = %q, want docs", prepared.from)
			}
			result := mustRun(t, prepared.query, set)
			column, ok := result.Column("id")
			if !ok {
				t.Fatal("result has no id column")
			}
			got := make([]string, len(column.Cells))
			for i := range column.Cells {
				got[i] = string(column.Cells[i].JSON())
			}
			if len(got) != len(tc.want) {
				t.Fatalf("ids = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("ids = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestNativeLiteralSubsetPreservesExactNumbersAndLimit(t *testing.T) {
	prepared, err := prepareNativeQuery([]byte(`{
		"dialect":"vibedb-query",
		"version":1,
		"from":"docs",
		"where":{"/value":1.0000000000000001},
		"select":[{"name":"exact","path":"/value"}],
		"limit":1
	}`))
	if err != nil {
		t.Fatalf("prepareNativeQuery: %v", err)
	}
	result := mustRun(t, prepared.query, mustSegment(t,
		`{"value":1.0000000000000000}`,
		`{"value":1.0000000000000001}`,
		`{"value":1.0000000000000001}`,
	))
	if result.RowCount != 1 {
		t.Fatalf("RowCount = %d, want 1", result.RowCount)
	}
	column, ok := result.Column("exact")
	if !ok {
		t.Fatal("result has no exact column")
	}
	if got := string(column.Cells[0].JSON()); got != "1.0000000000000001" {
		t.Fatalf("exact value = %s", got)
	}
}

func TestNativeLoweringGatesPlanFeaturesThatAreNotReady(t *testing.T) {
	base := `{"dialect":"vibedb-query","version":1,"from":"docs"`
	cases := []struct {
		name    string
		suffix  string
		pointer string
	}{
		{
			"parameter", `,"where":{"/a":{"$eq":{"$param":"a"}}}}`,
			"/where",
		},
		{
			"ordered comparison", `,"where":{"/a":{"$gt":1}}}`,
			"/where",
		},
		{
			"ordering", `,"orderBy":[{"path":"/a","direction":"asc"}]}`,
			"/orderBy",
		},
		{
			"exists", `,"exists":[{"from":"other","on":{"outer":"/ref","inner":"$key"}}]}`,
			"/exists",
		},
		{
			"fan-out join",
			`,"join":{"from":"other","as":"o","on":{"outer":"/ref","inner":"$key"}}` +
				`,"select":[{"name":"value","path":"@o/value"}]}`,
			"/join",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := prepareNativeQuery([]byte(base + tc.suffix))
			var nativeErr *nativeSyntaxError
			if !errors.As(err, &nativeErr) {
				t.Fatalf("error = %v, want nativeSyntaxError", err)
			}
			if nativeErr.code != "feature_not_ready" || nativeErr.pointer != tc.pointer {
				t.Fatalf(
					"error = (%q,%q), want (feature_not_ready,%q)",
					nativeErr.code, nativeErr.pointer, tc.pointer,
				)
			}
		})
	}
}
