package planner

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"testing"
)

func analysisRows(n int, offset int) iter.Seq2[StatisticsRow, error] {
	return func(yield func(StatisticsRow, error) bool) {
		for i := 0; i < n; i++ {
			v := fmt.Sprint((i + offset) % 100)
			if !yield(StatisticsRow{Values: []string{v, v}, Bytes: 32}, nil) {
				return
			}
		}
	}
}

func TestAnalyzeDistributedDistinctUnionAndCorrelation(t *testing.T) {
	var partitions []*PartitionAnalysis
	for i, n := range []int{900, 100} {
		a, err := AnalyzePartition(t.Context(), 7, "t", fmt.Sprint(i), []string{"/a", "/b"}, [][]string{{"/a", "/b"}}, analysisRows(n, 0), AnalyzeOptions{})
		if err != nil {
			t.Fatal(err)
		}
		partitions = append(partitions, a)
	}
	result, err := MergePartitionStatistics(t.Context(), partitions...)
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows.Value != 1000 || result.RowBytes.Value != 32 || result.Columns[0].Distinct.Value != 100 || result.Groups[2].Distinct.Value != 100 {
		t.Fatalf("bad merged statistics: %+v", result)
	}
	if result.Partitions[0].Groups[0].Distinct.Value != 100 || result.Partitions[1].Groups[0].Distinct.Value != 100 {
		t.Fatal("local counts lost")
	}
	catalog, err := NewStatisticsCatalog(7, []TableStatistics{result})
	if err != nil {
		t.Fatal(err)
	}
	table, _ := catalog.Table("t")
	// There are 100 correlated pairs, not 10,000 independent combinations.
	if e, ok := table.JointSelectivity([]EqualityConstraint{{"/a", []string{"0"}}, {"/b", []string{"0"}}}); !ok || e.Value != .01 {
		t.Fatalf("correlation estimate %+v/%v", e, ok)
	}
	reversed, err := MergePartitionStatistics(t.Context(), partitions[1], partitions[0])
	if err != nil || !reflect.DeepEqual(result, reversed) {
		t.Fatalf("merge depends on order: %v", err)
	}
	if _, err := MergePartitionStatistics(t.Context(), partitions[0], partitions[0]); !errors.Is(err, ErrInvalidStatistics) {
		t.Fatalf("duplicate partition: %v", err)
	}
}

func TestAnalyzeSampleWeightingAndBounds(t *testing.T) {
	options := AnalyzeOptions{SampleRows: 128, DistinctEntries: 64, MostCommon: 16}
	source := func(n int, value string) iter.Seq2[StatisticsRow, error] {
		return func(yield func(StatisticsRow, error) bool) {
			for i := 0; i < n; i++ {
				if !yield(StatisticsRow{Values: []string{value}, Bytes: 8}, nil) {
					return
				}
			}
		}
	}
	large, err := AnalyzePartition(t.Context(), 1, "t", "large", []string{"/v"}, nil, source(10000, `"hot"`), options)
	if err != nil {
		t.Fatal(err)
	}
	small, err := AnalyzePartition(t.Context(), 1, "t", "small", []string{"/v"}, nil, source(10, `"cold"`), options)
	if err != nil {
		t.Fatal(err)
	}
	result, err := MergePartitionStatistics(t.Context(), large, small)
	if err != nil {
		t.Fatal(err)
	}
	if len(large.sample) != 128 || len(large.distinct[0].hashes) > 64 {
		t.Fatal("unbounded retained synopsis")
	}
	hot := result.Groups[0].MostCommon[0]
	if hot.Values[0] != `"hot"` || hot.Frequency.Value < .9 || hot.Frequency.Lower >= hot.Frequency.Value {
		t.Fatalf("biased sample: %+v", hot)
	}
	if len(result.Columns[0].MostCommon) != 0 {
		t.Fatal("sampled frequency published as exact single-column MCV")
	}
	if _, err := NewStatisticsCatalog(1, []TableStatistics{result}); err != nil {
		t.Fatal(err)
	}
}

func TestAnalyzeErrorsAndCancellation(t *testing.T) {
	sentinel := errors.New("source failed")
	source := func(yield func(StatisticsRow, error) bool) { yield(StatisticsRow{}, sentinel) }
	if _, err := AnalyzePartition(t.Context(), 1, "t", "s", []string{"/a"}, nil, source, AnalyzeOptions{}); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := AnalyzePartition(ctx, 1, "t", "s", []string{"/a", "/b"}, nil, analysisRows(1, 0), AnalyzeOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := AnalyzePartition(t.Context(), 1, "t", "s", []string{"/a", "/b"}, nil, analysisRows(1, 0), AnalyzeOptions{MaxSampleBytes: 1}); !errors.Is(err, ErrInvalidStatistics) {
		t.Fatal(err)
	}
}

func TestDistinctSketchApproximateUnion(t *testing.T) {
	a, b, all := distinctSample{capacity: 512}, distinctSample{capacity: 512}, distinctSample{capacity: 512}
	for i := 0; i < 100000; i++ {
		h := uint64(i) * 0x9e3779b97f4a7c15
		all.add(h)
		if i%2 == 0 {
			a.add(h)
		} else {
			b.add(h)
		}
	}
	for _, h := range b.hashes {
		a.add(h)
	}
	a.full = a.full || b.full
	if !reflect.DeepEqual(a.hashes, all.hashes) {
		t.Fatal("KMV union does not equal serial sketch")
	}
	e := a.estimate(100000)
	if e.Lower > 100000 || e.Upper < 100000 || e.Value < 90000 {
		t.Fatalf("bad KMV estimate %+v", e)
	}
}

func BenchmarkAnalyzePartition(b *testing.B) {
	for b.Loop() {
		_, err := AnalyzePartition(b.Context(), 1, "t", "s", []string{"/a", "/b"}, [][]string{{"/a", "/b"}}, analysisRows(10000, 0), AnalyzeOptions{})
		if err != nil {
			b.Fatal(err)
		}
	}
}
