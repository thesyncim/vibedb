package planner

import (
	"errors"
	"fmt"
	"math"
	"testing"
	"unsafe"
)

func testStatistics(t testing.TB) *StatisticsCatalog {
	t.Helper()
	catalog, err := NewStatisticsCatalog(9, []TableStatistics{
		{
			Table: "events", Rows: Estimate{Value: 1_000_000, Lower: 900_000, Upper: 1_100_000, Confidence: .95},
			RowBytes: Estimate{Value: 128, Lower: 64, Upper: 256, Confidence: .9},
			Partitions: []PartitionStatistics{
				{Partition: "s0", Rows: Estimate{Value: 400_000, Upper: 450_000, Confidence: .9}},
				{Partition: "s1", Rows: Estimate{Value: 600_000, Upper: 650_000, Confidence: .9}},
			},
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
	if partition, ok := table.PartitionRows("s1"); !ok || partition.Value != 600_000 || partition.Upper != 650_000 {
		t.Fatalf("partition s1 = %+v/%v", partition, ok)
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
	if estimate := column.EqualitySelectivityEstimate(`"other"`); estimate.Lower > estimate.Value || estimate.Value > estimate.Upper || estimate.Confidence <= 0 {
		t.Fatalf("tail selectivity interval = %+v", estimate)
	}
	if estimate := column.LessThanSelectivityEstimate(`"m"`, true); estimate.Value != .45 || estimate.Lower != 0 || estimate.Upper != .45 {
		t.Fatalf("inclusive histogram boundary = %+v", estimate)
	}
	if estimate := column.LessThanSelectivityEstimate(`"t"`, false); estimate.Lower != .45 || estimate.Upper != .9 || estimate.Value <= estimate.Lower || estimate.Value >= estimate.Upper {
		t.Fatalf("within-bucket histogram interval = %+v", estimate)
	}
	descriptors := catalog.Descriptors()
	roundTrip, err := NewStatisticsCatalog(9, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	if got := roundTrip.Descriptors(); len(got) != 1 || got[0].Rows.Lower != 900_000 ||
		len(got[0].Partitions) != 2 || got[0].Partitions[1].Partition != "s1" ||
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

func TestCanonicalStatisticScalars(t *testing.T) {
	numbers := []string{"5", "5.0", "50e-1", "0.5e1"}
	for _, number := range numbers {
		canonical, err := CanonicalScalarJSON(number)
		if err != nil {
			t.Fatalf("CanonicalScalarJSON(%q): %v", number, err)
		}
		if canonical != "5" {
			t.Fatalf("CanonicalScalarJSON(%q) = %q, want 5", number, canonical)
		}
	}
	canonical, err := CanonicalScalarJSON(`"\u003c"`)
	if err != nil || canonical != `"\u003c"` {
		t.Fatalf("canonical escaped string = %q, %v", canonical, err)
	}
	comparison, err := CompareCanonicalScalarJSON("1e1000000000", "9e999999999")
	if err != nil || comparison <= 0 {
		t.Fatalf("huge canonical number comparison = %d, %v", comparison, err)
	}
	if CanonicalNumberFitsDecimalBytes("1e1000000000", 1024) {
		t.Fatal("huge canonical number passed decimal materialization admission")
	}
	if !CanonicalNumberFitsDecimalBytes("1", 1) || CanonicalNumberFitsDecimalBytes("-1", 1) ||
		!CanonicalNumberFitsDecimalBytes("1e-2", 4) {
		t.Fatal("exact decimal byte admission rejected or accepted a boundary incorrectly")
	}

	catalog, err := NewStatisticsCatalog(1, []TableStatistics{{
		Table: "t", Rows: ExactEstimate(10), RowBytes: ExactEstimate(8),
		Columns: []ColumnStatistics{{
			Path: "/n", Distinct: ExactEstimate(2),
			MostCommon: []ValueFrequency{{Value: "5.0", Frequency: .4}},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	table, _ := catalog.Table("t")
	column, _ := table.Column("/n")
	if got := column.EqualitySelectivity("5"); got != .4 {
		t.Fatalf("canonical numeric heavy hitter selectivity = %v, want .4", got)
	}
}

func FuzzCanonicalScalarJSON(f *testing.F) {
	for _, seed := range []string{
		`0`, `-0`, `5.0`, `50e-1`, `1e1000000000`, `1e-1000000000`,
		`true`, `null`, `"text"`, `"\u003c"`, `{}`, `1 trailing`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, value string) {
		canonical, err := CanonicalScalarJSON(value)
		if err != nil {
			return
		}
		second, err := CanonicalScalarJSON(canonical)
		if err != nil {
			t.Fatalf("canonical output %q is invalid: %v", canonical, err)
		}
		if second != canonical {
			t.Fatalf("canonicalization is not idempotent: %q then %q", canonical, second)
		}
	})
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
	if got := unsafe.Sizeof(compactPartitionStatistic{}); got != 40 {
		t.Fatalf("compact partition statistic = %d bytes, want 40", got)
	}
	catalog := testStatistics(t)
	if got := catalog.RetainedBytes(); got > 384 {
		t.Fatalf("retained bytes = %d, want <= 384 for one table/column/skew/partition profile", got)
	}
}

func TestCompactEstimatePreservesHugeLowerBound(t *testing.T) {
	catalog, err := NewStatisticsCatalog(1, []TableStatistics{{
		Table: "huge",
		Rows: Estimate{
			Value: 1e100, Lower: 5e99, Upper: 2e100, Confidence: .75,
		},
		RowBytes: ExactEstimate(8),
	}})
	if err != nil {
		t.Fatal(err)
	}
	table, _ := catalog.Table("huge")
	rows := table.Rows()
	if rows.Lower <= 0 || rows.Lower > 5e99 || rows.Lower > rows.Value || rows.Upper != 2e100 ||
		math.Abs(rows.Lower-5e99)/5e99 > 1e-6 {
		t.Fatalf("huge compact estimate = %+v", rows)
	}
	awkward := newCompactEstimate(Estimate{
		Value: 2, Lower: 1.00000001, Upper: 2, Confidence: 1,
	}, 0).public()
	if awkward.Lower > 1.00000001 {
		t.Fatalf("compact lower bound rounded upward: %+v", awkward)
	}
}

func TestStatisticsHeavyHittersAreSortedForLookup(t *testing.T) {
	descriptors := []TableStatistics{{
		Table: "events", Rows: ExactEstimate(100), RowBytes: ExactEstimate(8),
		Columns: []ColumnStatistics{{
			Path: "/tenant", Distinct: ExactEstimate(10),
			MostCommon: []ValueFrequency{
				{Value: `"z"`, Frequency: .2},
				{Value: `"a"`, Frequency: .1},
				{Value: `"m"`, Frequency: .15},
			},
		}},
	}}
	catalog, err := NewStatisticsCatalog(1, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	table, _ := catalog.Table("events")
	column, _ := table.Column("/tenant")
	for value, want := range map[string]float64{`"a"`: .1, `"m"`: .15, `"z"`: .2} {
		if got := column.EqualitySelectivity(value); got != want {
			t.Fatalf("EqualitySelectivity(%s) = %v, want %v", value, got, want)
		}
	}
	descriptor := catalog.Descriptors()[0].Columns[0]
	if descriptor.MostCommon[0].Value != `"a"` || descriptor.MostCommon[2].Value != `"z"` {
		t.Fatalf("heavy hitters not stored in lookup order: %+v", descriptor.MostCommon)
	}
	if descriptors[0].Columns[0].MostCommon[0].Value != `"z"` {
		t.Fatalf("catalog construction mutated caller-owned skew input: %+v",
			descriptors[0].Columns[0].MostCommon)
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
		{"equivalent heavy hitters", func(s *TableStatistics) {
			s.Columns[0].MostCommon = []ValueFrequency{
				{Value: `5`, Frequency: .1}, {Value: `5.0`, Frequency: .1},
			}
		}},
		{"regressed histogram", func(s *TableStatistics) {
			s.Columns[0].Histogram = []HistogramBucket{
				{Upper: `1`, Frequency: .7, Distinct: 2},
				{Upper: `2`, Frequency: .6, Distinct: 3},
			}
		}},
		{"unordered histogram bounds", func(s *TableStatistics) {
			s.Columns[0].Histogram = []HistogramBucket{
				{Upper: `2`, Frequency: .4, Distinct: 1},
				{Upper: `1`, Frequency: .8, Distinct: 2},
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

func BenchmarkPartitionStatisticsLookup1KPartitions(b *testing.B) {
	partitions := make([]PartitionStatistics, 1024)
	for i := range partitions {
		partitions[i] = PartitionStatistics{
			Partition: fmt.Sprintf("shard-%04d", i), Rows: ExactEstimate(float64(1_000 + i)),
		}
	}
	catalog, err := NewStatisticsCatalog(1, []TableStatistics{{
		Table: "events", Rows: ExactEstimate(2_000_000), RowBytes: ExactEstimate(128),
		Partitions: partitions,
	}})
	if err != nil {
		b.Fatal(err)
	}
	table, _ := catalog.Table("events")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, _ = table.PartitionRows("shard-0512")
	}
	b.ReportMetric(float64(catalog.RetainedBytes())/1024, "retained-B/partition")
}

func BenchmarkStatisticsHeavyHitterLookup1KValues(b *testing.B) {
	values := make([]ValueFrequency, 1024)
	for i := range values {
		values[len(values)-1-i] = ValueFrequency{
			Value: fmt.Sprintf(`"tenant-%04d"`, i), Frequency: .0005,
		}
	}
	catalog, err := NewStatisticsCatalog(1, []TableStatistics{{
		Table: "events", Rows: ExactEstimate(1_000_000), RowBytes: ExactEstimate(128),
		Columns: []ColumnStatistics{{
			Path: "/tenant", Distinct: ExactEstimate(10_000), MostCommon: values,
		}},
	}})
	if err != nil {
		b.Fatal(err)
	}
	table, _ := catalog.Table("events")
	column, _ := table.Column("/tenant")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = column.EqualitySelectivity(`"tenant-0512"`)
	}
}
