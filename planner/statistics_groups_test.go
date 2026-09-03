package planner

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestJointStatisticsCorrelationRoundTrip(t *testing.T) {
	input := []TableStatistics{{Table: "t", Rows: ExactEstimate(1000), RowBytes: ExactEstimate(20), Groups: []GroupStatistics{{Paths: []string{"/b", "/a"}, Distinct: ExactEstimate(2), MostCommon: []TupleFrequency{
		{Values: []string{`"x"`, `1.0`}, Frequency: ExactEstimate(.75)},
		{Values: []string{`"y"`, `2`}, Frequency: ExactEstimate(.25)},
	}}}, Partitions: []PartitionStatistics{{Partition: "s1", Rows: ExactEstimate(700), Groups: []GroupStatistics{{Paths: []string{"/a", "/b"}, Distinct: ExactEstimate(1)}}}}}}
	catalog, err := NewStatisticsCatalog(1, input)
	if err != nil {
		t.Fatal(err)
	}
	input[0].Groups[0].Paths[0] = "changed"
	input[0].Groups[0].MostCommon[0].Values[0] = "changed"
	table, _ := catalog.Table("t")
	for _, tc := range []struct {
		name string
		a, b []string
		want float64
	}{
		{"correlated", []string{"1"}, []string{`"x"`}, .75},
		{"incompatible", []string{"1"}, []string{`"y"`}, 0},
		{"membership", []string{"1", "2"}, []string{`"x"`, `"y"`}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, ok := table.JointSelectivity([]EqualityConstraint{{"/a", tc.a}, {"/b", tc.b}})
			if !ok || math.Abs(e.Value-tc.want) > 1e-9 || e.Lower > e.Value || e.Upper < e.Value {
				t.Fatalf("estimate %+v/%v, want %v", e, ok, tc.want)
			}
		})
	}
	if e, ok := table.GroupDistinct("s1", []string{"/b", "/a"}); !ok || e.Value != 1 {
		t.Fatalf("local NDV %+v/%v", e, ok)
	}
	if _, ok := table.GroupDistinct("s2", []string{"/a", "/b"}); ok {
		t.Fatal("invented missing local statistics")
	}
	if _, ok := table.GroupDistinct("", []string{"/a", "/a"}); ok {
		t.Fatal("accepted repeated paths")
	}
	raw, err := json.Marshal(catalog.Descriptors())
	if err != nil {
		t.Fatal(err)
	}
	var decoded []TableStatistics
	if err = json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	again, err := NewStatisticsCatalog(1, decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(catalog.Descriptors(), again.Descriptors()) {
		t.Fatal("round trip changed descriptors")
	}
	constraints := []EqualityConstraint{{"/a", []string{"1"}}, {"/b", []string{`"x"`}}}
	if allocations := testing.AllocsPerRun(100, func() { table.JointSelectivity(constraints); table.GroupDistinct("s1", []string{"/a", "/b"}) }); allocations != 0 {
		t.Fatalf("lookup allocated %v", allocations)
	}
}

func TestJointStatisticsRejectMalformed(t *testing.T) {
	for _, group := range []GroupStatistics{
		{Paths: []string{"/a", "/a"}, Distinct: ExactEstimate(2)},
		{Paths: []string{"/a", "/b"}, Distinct: ExactEstimate(2), MostCommon: []TupleFrequency{{Values: []string{"1"}, Frequency: ExactEstimate(.5)}}},
		{Paths: []string{"/a"}, Distinct: ExactEstimate(2), MostCommon: []TupleFrequency{{Values: []string{"1"}, Frequency: ExactEstimate(.5)}, {Values: []string{"1.0"}, Frequency: ExactEstimate(.5)}}},
		{Paths: []string{"/a"}, Distinct: ExactEstimate(1), MostCommon: []TupleFrequency{{Values: []string{"1"}, Frequency: ExactEstimate(.8)}, {Values: []string{"2"}, Frequency: ExactEstimate(.8)}}},
	} {
		_, err := NewStatisticsCatalog(1, []TableStatistics{{Table: "t", Groups: []GroupStatistics{group}}})
		if !errors.Is(err, ErrInvalidStatistics) {
			t.Fatalf("malformed group accepted: %+v, %v", group, err)
		}
	}
}

func TestConjunctionUsesSubsetWithoutDoubleCounting(t *testing.T) {
	catalog, err := NewStatisticsCatalog(1, []TableStatistics{{Table: "t", Columns: []ColumnStatistics{{Path: "/c", Distinct: ExactEstimate(100)}}, Groups: []GroupStatistics{
		{Paths: []string{"/a", "/b"}, Distinct: ExactEstimate(2), MostCommon: []TupleFrequency{{Values: []string{"1", "1"}, Frequency: ExactEstimate(.5)}}},
		{Paths: []string{"/b", "/c"}, Distinct: ExactEstimate(100)},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	table, _ := catalog.Table("t")
	e, ok := table.ConjunctionSelectivity([]EqualityConstraint{{"/a", []string{"1"}}, {"/b", []string{"1"}}, {"/c", []string{"1"}}})
	if !ok || math.Abs(e.Value-.01*math.Sqrt(.5)) > 1e-9 {
		t.Fatalf("conjunction=%+v/%v", e, ok)
	}
	paths := table.AppendColumnPaths(nil)
	if len(paths) != 3 {
		t.Fatalf("joint-only paths missing: %v", paths)
	}
}
