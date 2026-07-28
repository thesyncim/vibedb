package durable

import (
	"math"
	"testing"

	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

func TestFileMaterializationProjectionsEqual(t *testing.T) {
	tests := []struct {
		name        string
		indexes     []store.IndexDefinition
		float64     []string
		existing    string
		replacement string
		want        bool
	}{
		{
			name:        "no configured projections",
			existing:    `{"payload":"before"}`,
			replacement: `{"payload":"after"}`,
			want:        true,
		},
		{
			name: "equal exact scalar semantics",
			indexes: []store.IndexDefinition{
				{Name: "tuple", Paths: []string{"/status", "/rank", "/zero"}},
			},
			existing:    `{"status":"active","rank":1,"zero":-0,"payload":"before"}`,
			replacement: `{"status":"\u0061ctive","rank":1.0,"zero":0,"payload":"after"}`,
			want:        true,
		},
		{
			name: "both compound tuples absent after an earlier difference",
			indexes: []store.IndexDefinition{
				{Name: "tuple", Paths: []string{"/tenant", "/optional"}},
			},
			existing:    `{"tenant":"old"}`,
			replacement: `{"tenant":"new","optional":{"not":"scalar"}}`,
			want:        true,
		},
		{
			name: "absent exact tuple becomes present",
			indexes: []store.IndexDefinition{
				{Name: "tuple", Paths: []string{"/tenant", "/rank"}},
			},
			existing:    `{"tenant":"acme"}`,
			replacement: `{"tenant":"acme","rank":1}`,
			want:        false,
		},
		{
			name: "present exact tuple changes",
			indexes: []store.IndexDefinition{
				{Name: "status", Paths: []string{"/status"}},
			},
			existing:    `{"status":"active"}`,
			replacement: `{"status":"idle"}`,
			want:        false,
		},
		{
			name: "wide-number hash bucket is not equality",
			indexes: []store.IndexDefinition{
				{Name: "wide", Paths: []string{"/wide"}},
			},
			existing:    `{"wide":1e400}`,
			replacement: `{"wide":2e400}`,
			want:        false,
		},
		{
			name:        "equal float projection normalizes spelling",
			float64:     []string{"/score"},
			existing:    `{"score":1}`,
			replacement: `{"score":1.0}`,
			want:        true,
		},
		{
			name:        "both float projections absent",
			float64:     []string{"/score"},
			existing:    `{"payload":"missing"}`,
			replacement: `{"score":1e400}`,
			want:        true,
		},
		{
			name:        "float projection becomes present",
			float64:     []string{"/score"},
			existing:    `{"score":"1"}`,
			replacement: `{"score":1}`,
			want:        false,
		},
		{
			name:        "float signed zero remains distinct",
			float64:     []string{"/score"},
			existing:    `{"score":-0}`,
			replacement: `{"score":0}`,
			want:        false,
		},
		{
			name:        "float projection changes",
			float64:     []string{"/score"},
			existing:    `{"score":1}`,
			replacement: `{"score":2}`,
			want:        false,
		},
		{
			name: "all configured projections must match",
			indexes: []store.IndexDefinition{
				{Name: "status", Paths: []string{"/status"}},
			},
			float64:     []string{"/score", "/latency"},
			existing:    `{"status":"active","score":1,"latency":2}`,
			replacement: `{"status":"active","score":1,"latency":3}`,
			want:        false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			collection := projectionTestCollection(t, tc.indexes, tc.float64)
			existing := projectionTestIndex(t, tc.existing)
			replacement := projectionTestIndex(t, tc.replacement)
			got, err := collection.fileMaterializationProjectionsEqual(
				existing, replacement,
			)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("fileMaterializationProjectionsEqual() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFileFloat64ProjectionValuePreservesSignedZero(t *testing.T) {
	index := projectionTestIndex(t, `{"score":-0}`)
	pointer, err := vibejson.CompilePointer("/score")
	if err != nil {
		t.Fatal(err)
	}
	value, present, err := fileFloat64ProjectionValue(index, pointer)
	if err != nil {
		t.Fatal(err)
	}
	if !present || math.Float64bits(value) != math.Float64bits(math.Copysign(0, -1)) {
		t.Fatalf("projection = (%x,%v), want negative zero", math.Float64bits(value), present)
	}
}

func projectionTestCollection(
	t *testing.T,
	indexes []store.IndexDefinition,
	float64Columns []string,
) *Collection {
	t.Helper()
	options := testFileStoreOptions()
	options.Indexes = indexes
	options.Float64Columns = float64Columns
	normalized, err := options.normalized()
	if err != nil {
		t.Fatal(err)
	}
	return &Collection{options: normalized}
}

func projectionTestIndex(t *testing.T, src string) vibejson.Index {
	t.Helper()
	document := []byte(src)
	entries, err := vibejson.RequiredIndexEntries(document)
	if err != nil {
		t.Fatal(err)
	}
	index, err := vibejson.BuildIndex(
		document, make([]vibejson.IndexEntry, entries),
	)
	if err != nil {
		t.Fatal(err)
	}
	return index
}
