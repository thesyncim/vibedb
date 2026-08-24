package gateway

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	sqlast "github.com/thesyncim/vibedb/sql"
	vibejson "github.com/thesyncim/vibejson"
)

func mustGlobalIndexNumber(t testing.TB, spelling string) distribution.Scalar {
	t.Helper()
	number, err := distribution.NewNumber(spelling)
	if err != nil {
		t.Fatal(err)
	}
	return number
}

func TestGlobalIndexLocatorVibeJSONExactBytes(t *testing.T) {
	locator := []distribution.Scalar{
		distribution.NewString("quote\" slash\\ line\n tab\t nul\x00 <>& / \u2028 \u2029"),
		mustGlobalIndexNumber(t, "-0.0012300E+99"),
	}
	want := []byte("prefix:" + `["quote\" slash\\ line\n tab\t nul\u0000 <>& / \u2028 \u2029",-0.0012300E+99]`)
	var document globalIndexLocatorDocument
	storage := make([]byte, len("prefix:"), 256)
	copy(storage, "prefix:")
	got, err := appendGlobalIndexLocatorValue(storage, locator, &document)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("locator = %q, want %q", got, want)
	}
	if document.locator != nil {
		t.Fatalf("encoder retained locator after success: %+v", document.locator)
	}
}

func TestGlobalIndexLocatorVibeJSONErrorsRollBackAndReuse(t *testing.T) {
	tests := []struct {
		name    string
		locator []distribution.Scalar
	}{
		{
			name: "invalid kind after valid prefix",
			locator: []distribution.Scalar{
				distribution.NewString("valid-prefix"), {},
			},
		},
		{
			name: "invalid UTF-8",
			locator: []distribution.Scalar{
				distribution.NewString(string([]byte{'x', 0xff, 'y'})),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var document globalIndexLocatorDocument
			storage := make([]byte, len("prefix:"), 128)
			copy(storage, "prefix:")
			got, err := appendGlobalIndexLocatorValue(storage, test.locator, &document)
			if !errors.Is(err, ErrGlobalIndexDocument) {
				t.Fatalf("error = %v, want ErrGlobalIndexDocument", err)
			}
			if string(got) != "prefix:" {
				t.Fatalf("error output = %q, want original destination", got)
			}
			if document.locator != nil {
				t.Fatalf("encoder retained locator after error: %+v", document.locator)
			}

			got, err = appendGlobalIndexLocatorValue(
				storage, []distribution.Scalar{distribution.NewString("reused")}, &document,
			)
			if err != nil || string(got) != `prefix:["reused"]` {
				t.Fatalf("workspace reuse = %q, %v", got, err)
			}
		})
	}

	prefix := []byte("prefix:")
	got, err := appendGlobalIndexLocatorValue(
		prefix, []distribution.Scalar{distribution.NewString("x")}, nil,
	)
	if !errors.Is(err, ErrGlobalIndexDocument) || string(got) != "prefix:" {
		t.Fatalf("nil workspace = %q, %v", got, err)
	}
}

func TestGlobalIndexFlatScalarRouteWarmAllocationFree(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		t.Fatal(err)
	}
	program, err := snapshot.CompileGlobalIndex("messages", "by_email")
	if err != nil {
		t.Fatal(err)
	}
	key := []distribution.Scalar{distribution.NewString("a@example.com")}
	locator := []distribution.Scalar{
		distribution.NewString("tenant-7"), mustGlobalIndexNumber(t, "7.00e0"),
	}
	var workspace GlobalIndexWorkspace
	route, err := program.RouteScalars(key, locator, &workspace)
	if err != nil {
		t.Fatal(err)
	}
	if string(route.LocatorValue) != `["tenant-7",7.00e0]` {
		t.Fatalf("locator value = %s", route.LocatorValue)
	}
	if workspace.locatorJSON == nil || workspace.locatorJSON.locator != nil {
		t.Fatalf("locator encoder workspace = %+v", workspace.locatorJSON)
	}
	if allocations := testing.AllocsPerRun(1000, func() {
		if _, routeErr := program.RouteScalars(key, locator, &workspace); routeErr != nil {
			t.Fatal(routeErr)
		}
	}); allocations != 0 {
		t.Fatalf("warm flat scalar route allocations = %v, want 0", allocations)
	}
}

type gatewayBogusStringer string

func (s gatewayBogusStringer) String() string { return string(s) }

func TestGatewayBindBoundaryUsesExactPrimitives(t *testing.T) {
	scalar, err := writeScalarFromValue(vibejson.RawValue{Src: []byte("7.00e0")})
	if err != nil {
		t.Fatal(err)
	}
	if spelling, ok := scalar.NumberSpelling(); !ok || spelling != "7.00e0" {
		t.Fatalf("number scalar = %q, %v", spelling, ok)
	}
	for _, value := range []any{
		stdjson.Number("7.00e0"), gatewayBogusStringer("7.00e0"),
	} {
		if _, err := writeScalarFromValue(value); err == nil {
			t.Fatalf("write scalar accepted %T", value)
		}
	}

	snapshot := testSnapshot(t, 1)
	prepared, err := snapshot.Prepare(
		context.Background(),
		`SELECT n FROM messages WHERE tenant_id = ? ORDER BY n LIMIT ? OFFSET ?`,
	)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.Bind([]any{
		"tenant-7",
		vibejson.RawValue{Src: []byte("17")},
		uint64(3),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bound.hasLimit || bound.limit != 17 || bound.offset != 3 {
		t.Fatalf("window = limit %d (present %v), offset %d", bound.limit, bound.hasLimit, bound.offset)
	}

	integerBound, err := prepared.Bind([]any{"tenant-7", int64(5), int(2)})
	if err != nil || integerBound.limit != 5 || integerBound.offset != 2 {
		t.Fatalf("integer window = %+v, %v", integerBound, err)
	}

	tests := []struct {
		name string
		args []any
	}{
		{
			name: "stdjson shard key",
			args: []any{stdjson.Number("7"), vibejson.RawValue{Src: []byte("1")}, uint64(0)},
		},
		{
			name: "stdjson LIMIT",
			args: []any{"tenant-7", stdjson.Number("1"), uint64(0)},
		},
		{
			name: "stdjson OFFSET",
			args: []any{"tenant-7", vibejson.RawValue{Src: []byte("1")}, stdjson.Number("0")},
		},
		{
			name: "arbitrary Stringer",
			args: []any{"tenant-7", gatewayBogusStringer("1"), uint64(0)},
		},
		{
			name: "overflow LIMIT",
			args: []any{"tenant-7", vibejson.RawValue{Src: []byte("2147483648")}, uint64(0)},
		},
		{
			name: "negative OFFSET",
			args: []any{"tenant-7", uint64(1), int64(-1)},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := prepared.Bind(test.args); !errors.Is(err, ErrPlanParameters) {
				t.Fatalf("Bind error = %v, want ErrPlanParameters", err)
			}
		})
	}
}

func TestPreparedGlobalIndexInsertPreservesRawNumbersAndRejectsForeignScalars(t *testing.T) {
	config, endpoints := globalIndexCatalog(t)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := snapshot.Prepare(
		context.Background(),
		`INSERT INTO messages (tenant_id, id, email) VALUES (?, ?, ?)`,
	)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := prepared.BindWrite([]any{
		vibejson.RawValue{Src: []byte("7.00e0")},
		vibejson.RawValue{Src: []byte("-0E+9")},
		[]byte("a@example.com"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(bound.rowKeys) != 1 || len(bound.rowKeys[0]) != 1 {
		t.Fatalf("row keys = %+v", bound.rowKeys)
	}
	if spelling, ok := bound.rowKeys[0][0].NumberSpelling(); !ok || spelling != "7.00e0" {
		t.Fatalf("shard key = %q, %v", spelling, ok)
	}
	if len(bound.globalIndexes) != 1 {
		t.Fatalf("global mutations = %d, want 1", len(bound.globalIndexes))
	}
	mutation := bound.globalIndexes[0]
	value := bound.globalIndexArena[mutation.valueStart:mutation.valueEnd]
	if string(value) != `[7.00e0,-0E+9]` {
		t.Fatalf("global locator = %s", value)
	}

	for _, value := range []any{stdjson.Number("7"), gatewayBogusStringer("7")} {
		if _, err := prepared.BindWrite([]any{
			value,
			vibejson.RawValue{Src: []byte("9")},
			[]byte("a@example.com"),
		}); !errors.Is(err, ErrPlanParameters) {
			t.Fatalf("BindWrite(%T) error = %v, want ErrPlanParameters", value, err)
		}
	}
}

func TestPreparedFlatInsertRejectsDuplicateColumnsBeforeBind(t *testing.T) {
	tests := []struct {
		name     string
		snapshot *Snapshot
		sql      string
	}{
		{
			name:     "without global index",
			snapshot: testSnapshot(t, 1),
			sql:      `INSERT INTO messages (tenant_id, n, n) VALUES (?, ?, ?)`,
		},
		{
			name: "with global index",
			snapshot: func() *Snapshot {
				config, endpoints := globalIndexCatalog(t)
				snapshot, err := NewSnapshotWithIndexes(
					config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()},
				)
				if err != nil {
					t.Fatal(err)
				}
				return snapshot
			}(),
			sql: `INSERT INTO messages (tenant_id, id, email, email) VALUES (?, ?, ?, ?)`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prepared, err := test.snapshot.Prepare(context.Background(), test.sql)
			if err == nil || prepared != nil || !strings.Contains(err.Error(), "named twice") {
				t.Fatalf("Prepare = %+v, %v, want duplicate-column rejection", prepared, err)
			}
		})
	}
}

func parseFlatInsertColumns(t testing.TB, names ...string) []*sqlast.PathExpr {
	t.Helper()
	var sql strings.Builder
	sql.WriteString("INSERT INTO messages (")
	for i := range names {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString(names[i])
	}
	sql.WriteString(") VALUES (")
	for i := range names {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteByte('?')
	}
	sql.WriteByte(')')
	var parser sqlast.Parser
	var statement sqlast.Statement
	if err := parser.ParseStatement(&statement, sql.String()); err != nil {
		t.Fatal(err)
	}
	return statement.Insert.Columns
}

func TestFlatInsertColumnIndexResolvesHashCollisions(t *testing.T) {
	seed := maphash.MakeSeed()
	const mask = uint64(3) // Two columns build four open-addressed slots.
	firstBySlot := make(map[uint64]string, mask+1)
	var first, second string
	for i := 0; second == ""; i++ {
		candidate := fmt.Sprintf("collision_%d", i)
		slot := maphash.String(seed, "/"+candidate) & mask
		if prior, ok := firstBySlot[slot]; ok {
			first, second = prior, candidate
		} else {
			firstBySlot[slot] = candidate
		}
	}
	columns := parseFlatInsertColumns(t, first, second)
	index, duplicate := buildFlatInsertColumnIndexSeed(columns, seed)
	if duplicate {
		t.Fatal("distinct colliding columns were reported as duplicates")
	}
	if got, ok := index.find("/" + first); !ok || got != 0 {
		t.Fatalf("first colliding ordinal = %d, %v", got, ok)
	}
	if got, ok := index.find("/" + second); !ok || got != 1 {
		t.Fatalf("second colliding ordinal = %d, %v", got, ok)
	}
	if _, ok := index.find("/missing"); ok {
		t.Fatal("missing column resolved through a collision chain")
	}
	if _, duplicate := buildFlatInsertColumnIndexSeed(
		[]*sqlast.PathExpr{columns[0], columns[0]}, seed,
	); !duplicate {
		t.Fatal("repeated byte-identical column was not detected")
	}
}

var benchmarkGlobalIndexLocatorValue []byte
var benchmarkGlobalIndexRoute GlobalIndexRoute
var benchmarkGlobalIndexBoundWrite *BoundWritePlan
var benchmarkFlatInsertColumnIndex flatInsertColumnIndex
var benchmarkFlatInsertColumnMap map[string]int

func BenchmarkGlobalIndexLocatorEncoding(b *testing.B) {
	tests := []struct {
		name    string
		locator []distribution.Scalar
	}{
		{
			name: "strings",
			locator: []distribution.Scalar{
				distribution.NewString("tenant-7"), distribution.NewString("message-9"),
			},
		},
		{
			name: "escaped-and-exact-numbers",
			locator: []distribution.Scalar{
				distribution.NewString("tenant\n\"7"),
				mustGlobalIndexNumber(b, "7.00e0"),
				mustGlobalIndexNumber(b, "-0.0012300E+99"),
			},
		},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			var document globalIndexLocatorDocument
			storage := make([]byte, 0, 128)
			out, err := appendGlobalIndexLocatorValue(storage[:0], test.locator, &document)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(out)))
			b.ResetTimer()
			for range b.N {
				out, err = appendGlobalIndexLocatorValue(storage[:0], test.locator, &document)
				if err != nil {
					b.Fatal(err)
				}
			}
			benchmarkGlobalIndexLocatorValue = out
		})
	}
}

func BenchmarkGlobalIndexRouteScalarsColdWorkspace(b *testing.B) {
	config, endpoints := globalIndexCatalog(b)
	snapshot, err := NewSnapshotWithIndexes(
		config, endpoints, 1, []IndexDescriptor{testGlobalIndexDescriptor()},
	)
	if err != nil {
		b.Fatal(err)
	}
	program, err := snapshot.CompileGlobalIndex("messages", "by_email")
	if err != nil {
		b.Fatal(err)
	}
	key := []distribution.Scalar{distribution.NewString("a@example.com")}
	locator := []distribution.Scalar{
		distribution.NewString("tenant-7"), distribution.NewString("message-9"),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		var workspace GlobalIndexWorkspace
		benchmarkGlobalIndexRoute, err = program.RouteScalars(key, locator, &workspace)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkGlobalIndexSet(
	t testing.TB,
	count int,
) (distribution.ClusterConfig, map[distribution.EndpointID]string, []IndexDescriptor) {
	t.Helper()
	config := testConfig(t)
	endpoints := testEndpoints()
	endpoints["ep-index-a"] = "127.0.0.1:7101"
	endpoints["ep-index-b"] = "127.0.0.1:7102"
	descriptors := make([]IndexDescriptor, count)
	for i := range count {
		suffix := fmt.Sprintf("%d", i)
		distributionName := distribution.DistributionName("message_email_index_" + suffix)
		relation := "messages_email_index_" + suffix
		config.Distributions = append(config.Distributions, distribution.DistributionSpec{
			Name: distributionName, Arity: 1,
			MapperVersion: distribution.NativeMapperVersion,
			BucketBits:    distribution.DefaultVirtualBucketBits,
		})
		config.Placements = append(config.Placements, distribution.TablePlacement{
			Table: relation, Distribution: distributionName, Columns: []string{"/email"},
		})
		manifest, err := distribution.NewManifest(
			distributionName, distribution.RoutingVersion(5+i),
			[]distribution.Shard{
				{
					ID:                   distribution.ShardID("idx-" + suffix + "-a"),
					AllocationGeneration: 3,
					Range: distribution.KeyRange{
						Start: distribution.KeyspacePoint{},
						End:   distribution.KeyspaceEnd{Point: point(0x80)},
					},
					Leaders: []distribution.EndpointID{"ep-index-a"}, Epoch: 11,
				},
				{
					ID:                   distribution.ShardID("idx-" + suffix + "-b"),
					AllocationGeneration: 4,
					Range: distribution.KeyRange{
						Start: point(0x80), End: distribution.KeyspaceEnd{Max: true},
					},
					Leaders: []distribution.EndpointID{"ep-index-b"}, Epoch: 13,
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		config.Manifests = append(config.Manifests, manifest)
		descriptors[i] = IndexDescriptor{
			IndexID: uint64(51 + i), Incarnation: 2,
			Table: "messages", Name: "by_email_" + suffix, Relation: relation,
			Paths: []string{"/email"}, LocatorPaths: []string{"/tenant_id", "/id"},
			PrimaryPath: "/id",
			Flags:       IndexGlobal | IndexUnique | IndexOrdered, Lifecycle: IndexReady,
		}
	}
	return config, endpoints, descriptors
}

func benchmarkFlatInsert(rows int) (string, []any) {
	var sql strings.Builder
	sql.Grow(len("INSERT INTO messages (tenant_id, id, email) VALUES ") + rows*9)
	sql.WriteString("INSERT INTO messages (tenant_id, id, email) VALUES ")
	args := make([]any, 0, rows*3)
	for i := range rows {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteString("(?,?,?)")
		args = append(args,
			[]byte("tenant-7"), []byte("message-9"), []byte("a@example.com"),
		)
	}
	return sql.String(), args
}

func BenchmarkGlobalIndexPreparedWriteBind(b *testing.B) {
	for _, indexCount := range []int{1, 4} {
		config, endpoints, descriptors := benchmarkGlobalIndexSet(b, indexCount)
		snapshot, err := NewSnapshotWithIndexes(config, endpoints, 1, descriptors)
		if err != nil {
			b.Fatal(err)
		}
		for _, rows := range []int{1, 32, 256} {
			sql, args := benchmarkFlatInsert(rows)
			prepared, err := snapshot.Prepare(context.Background(), sql)
			if err != nil {
				b.Fatal(err)
			}
			name := fmt.Sprintf("rows=%d/indexes=%d", rows, indexCount)
			b.Run(name, func(b *testing.B) {
				bound, err := prepared.BindWrite(args)
				if err != nil {
					b.Fatal(err)
				}
				if got, want := len(bound.globalIndexes), rows*indexCount; got != want {
					b.Fatalf("global mutations = %d, want %d", got, want)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					bound, err = prepared.BindWrite(args)
					if err != nil {
						b.Fatal(err)
					}
				}
				benchmarkGlobalIndexBoundWrite = bound
			})
		}
	}
}

func benchmarkWideFlatInsertColumns(t testing.TB, count int) []*sqlast.PathExpr {
	t.Helper()
	var sql strings.Builder
	sql.WriteString("INSERT INTO messages (")
	for i := range count {
		if i != 0 {
			sql.WriteByte(',')
		}
		switch i {
		case 0:
			sql.WriteString("tenant_id")
		case 1:
			sql.WriteString("id")
		case 2:
			sql.WriteString("email")
		default:
			fmt.Fprintf(&sql, "column_%d", i)
		}
	}
	sql.WriteString(") VALUES (")
	for i := range count {
		if i != 0 {
			sql.WriteByte(',')
		}
		sql.WriteByte('?')
	}
	sql.WriteByte(')')
	var parser sqlast.Parser
	var statement sqlast.Statement
	if err := parser.ParseStatement(&statement, sql.String()); err != nil {
		t.Fatal(err)
	}
	return statement.Insert.Columns
}

func buildLegacyFlatInsertColumnMap(columns []*sqlast.PathExpr) map[string]int {
	index := make(map[string]int, len(columns))
	for i := range columns {
		index[string(columns[i].AppendPointer(nil))] = i
	}
	return index
}

func BenchmarkFlatInsertColumnOrdinalPreparation(b *testing.B) {
	for _, count := range []int{16, 256, 1024} {
		columns := benchmarkWideFlatInsertColumns(b, count)
		b.Run(fmt.Sprintf("columns=%d/byte-hash-ordinals", count), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				benchmarkFlatInsertColumnIndex, _ = buildFlatInsertColumnIndex(columns)
			}
		})
		b.Run(fmt.Sprintf("columns=%d/legacy-string-map", count), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				benchmarkFlatInsertColumnMap = buildLegacyFlatInsertColumnMap(columns)
			}
		})
	}
}
