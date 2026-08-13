package planner

import (
	"errors"
	"fmt"
	"testing"
	"unsafe"
)

func testStatistics(t testing.TB) *StatisticsCatalog {
	t.Helper()
	catalog, err := NewStatisticsCatalog(9, []TableStatistics{
		{
			Table: "events", Rows: Estimate{Value: 1_000_000, Lower: 900_000, Upper: 1_100_000, Confidence: .95},
			RowBytes: Estimate{Value: 128, Lower: 64, Upper: 256, Confidence: .9},
			Columns: []ColumnStatistics{
				{
					Path: "/tenant", Distinct: Estimate{Value: 1000, Upper: 1200, Confidence: .9},
					NullFraction: .1, AvgValueBytes: 12,
					MostCommon: []ValueFrequency{{Value: `"acme"`, Frequency: .2}},
					Histogram: []HistogramBucket{
						{Upper: `"m"`, Frequency: .45, Distinct: 500},
						{Upper: `"z"`, Frequency: .9, Distinct: 1000},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func TestStatisticsCatalogCompactLookupAndRoundTrip(t *testing.T) {
	catalog := testStatistics(t)
	if catalog.Generation() != 9 {
		t.Fatalf("generation = %d", catalog.Generation())
	}
	table, ok := catalog.Table("events")
	if !ok || table.Rows().Value != 1_000_000 || table.Rows().Lower != 900_000 ||
		table.Rows().Upper != 1_100_000 {
		t.Fatalf("table = %+v/%v", table, ok)
	}
	column, ok := table.Column("/tenant")
	if !ok {
		t.Fatal("column lookup missed")
	}
	if got := column.EqualitySelectivity(`"acme"`); got != .2 {
		t.Fatalf("heavy-hitter selectivity = %v, want .2", got)
	}
	if got := column.EqualitySelectivity(`"other"`); got <= 0 || got >= .001 {
		t.Fatalf("tail selectivity = %v, want a small positive estimate", got)
	}
	descriptors := catalog.Descriptors()
	roundTrip, err := NewStatisticsCatalog(9, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	if got := roundTrip.Descriptors(); len(got) != 1 || got[0].Rows.Lower != 900_000 ||
		len(got[0].Columns) != 1 ||
		len(got[0].Columns[0].MostCommon) != 1 || len(got[0].Columns[0].Histogram) != 2 {
		t.Fatalf("round trip = %+v", got)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		table, _ := catalog.Table("events")
		column, _ := table.Column("/tenant")
		_ = column.EqualitySelectivity(`"acme"`)
	}); allocs != 0 {
		t.Fatalf("statistics lookup allocations = %v, want 0", allocs)
	}
}

func TestStatisticsCatalogSpaceShape(t *testing.T) {
	if got := unsafe.Sizeof(compactTableStatistic{}); got != 64 {
		t.Fatalf("compact table statistic = %d bytes, want 64", got)
	}
	if got := unsafe.Sizeof(compactColumnStatistic{}); got != 64 {
		t.Fatalf("compact column statistic = %d bytes, want 64", got)
	}
	if got := unsafe.Sizeof(compactValueFrequency{}); got != 16 {
		t.Fatalf("compact heavy hitter = %d bytes, want 16", got)
	}
	if got := unsafe.Sizeof(compactHistogramBucket{}); got != 24 {
		t.Fatalf("compact histogram bucket = %d bytes, want 24", got)
	}
	catalog := testStatistics(t)
	if got := catalog.RetainedBytes(); got > 256 {
		t.Fatalf("retained bytes = %d, want <= 256 for one table/column/skew profile", got)
	}
}

func TestStatisticsValidation(t *testing.T) {
	valid := TableStatistics{
		Table: "t", Rows: ExactEstimate(10), RowBytes: ExactEstimate(8),
		Columns: []ColumnStatistics{{
			Path: "/x", Distinct: ExactEstimate(2), NullFraction: .1,
		}},
	}
	tests := []struct {
		name   string
		mutate func(*TableStatistics)
	}{
		{"missing table", func(s *TableStatistics) { s.Table = "" }},
		{"bad row interval", func(s *TableStatistics) { s.Rows.Upper = 1 }},
		{"bad null fraction", func(s *TableStatistics) { s.Columns[0].NullFraction = 2 }},
		{"container heavy hitter", func(s *TableStatistics) {
			s.Columns[0].MostCommon = []ValueFrequency{{Value: `{}`, Frequency: .1}}
		}},
		{"regressed histogram", func(s *TableStatistics) {
			s.Columns[0].Histogram = []HistogramBucket{
				{Upper: `1`, Frequency: .7, Distinct: 2},
				{Upper: `2`, Frequency: .6, Distinct: 3},
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := valid
			candidate.Columns = append([]ColumnStatistics(nil), valid.Columns...)
			test.mutate(&candidate)
			if _, err := NewStatisticsCatalog(1, []TableStatistics{candidate}); !errors.Is(err, ErrInvalidStatistics) {
				t.Fatalf("error = %v, want ErrInvalidStatistics", err)
			}
		})
	}
}

func BenchmarkStatisticsLookup(b *testing.B) {
	catalog := testStatistics(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		table, _ := catalog.Table("events")
		column, _ := table.Column("/tenant")
		_ = column.EqualitySelectivity(`"acme"`)
	}
}

func benchmarkStatisticsDescriptors(count int) []TableStatistics {
	descriptors := make([]TableStatistics, count)
	for i := range descriptors {
		descriptors[i] = TableStatistics{
			Table: fmt.Sprintf("table-%04d", i), Rows: ExactEstimate(float64(10_000 + i)),
			RowBytes: ExactEstimate(128),
			Columns: []ColumnStatistics{{
				Path: "/tenant", Distinct: ExactEstimate(1_000),
				MostCommon: []ValueFrequency{{Value: `"acme"`, Frequency: .01}},
			}},
		}
	}
	return descriptors
}

func BenchmarkStatisticsLookup1KTables(b *testing.B) {
	catalog, err := NewStatisticsCatalog(1, benchmarkStatisticsDescriptors(1024))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		table, _ := catalog.Table("table-0512")
		column, _ := table.Column("/tenant")
		_ = column.EqualitySelectivity(`"acme"`)
	}
	b.ReportMetric(float64(catalog.RetainedBytes())/1024, "retained-B/table")
}

func BenchmarkStatisticsCatalogBuild1KTables(b *testing.B) {
	descriptors := benchmarkStatisticsDescriptors(1024)
	b.ReportAllocs()
	for range b.N {
		catalog, err := NewStatisticsCatalog(1, descriptors)
		if err != nil {
			b.Fatal(err)
		}
		_ = catalog
	}
}
