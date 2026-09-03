package planner

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"reflect"
	"strings"
	"testing"
)

func TestPartitionAnalysisWireUnion(t *testing.T) {
	opts := AnalyzeOptions{SampleRows: 64, DistinctEntries: 32, MostCommon: 16}
	paths := []string{"/a", "/b"}
	groups := [][]string{{"/b", "/a"}}
	var originals, decoded []*PartitionAnalysis
	for i, n := range []int{0, 17, 3000} {
		name := []string{"empty", "small", "large"}[i]
		a, err := AnalyzePartition(t.Context(), 7, "table", name, paths, groups, analysisRows(n, 0), opts)
		if err != nil {
			t.Fatal(err)
		}
		wire, err := a.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		b, err := UnmarshalPartitionAnalysis(wire)
		if err != nil {
			t.Fatal(err)
		}
		again, err := b.MarshalBinary()
		if err != nil || !bytes.Equal(wire, again) {
			t.Fatalf("noncanonical roundtrip: %v", err)
		}
		if !b.MatchesDefinition(7, "table", name, paths, groups, opts) || b.MatchesDefinition(8, "table", name, paths, groups, opts) || b.MatchesDefinition(7, "table", "other", paths, groups, opts) {
			t.Fatal("definition fencing")
		}
		clear(wire)
		originals = append(originals, a)
		decoded = append(decoded, b)
	}
	want, err := MergePartitionStatistics(t.Context(), originals...)
	if err != nil {
		t.Fatal(err)
	}
	got, err := MergePartitionStatistics(t.Context(), decoded...)
	if err != nil || !reflect.DeepEqual(want, got) {
		t.Fatalf("wire union differs: %v", err)
	}
}

func TestPartitionAnalysisWireRejectsDamage(t *testing.T) {
	a, err := AnalyzePartition(t.Context(), 1, "t", "s", []string{"/a", "/b"}, nil, analysisRows(20, 0), AnalyzeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := a.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < len(wire); n++ {
		if _, err := UnmarshalPartitionAnalysis(wire[:n]); err == nil {
			t.Fatalf("accepted truncation %d", n)
		}
	}
	for i := range wire {
		bad := bytes.Clone(wire)
		bad[i] ^= 0xff
		if _, err := UnmarshalPartitionAnalysis(bad); err == nil {
			t.Fatalf("accepted corruption %d", i)
		}
	}
	// Checksums are corruption detection, not authentication. Structural limits
	// must also hold when a sender computes a valid checksum on malformed data.
	for _, offset := range []int{16, 21, 26, 30, 34, 42, 62} {
		bad := bytes.Clone(wire)
		binary.LittleEndian.PutUint32(bad[offset:], 0xffffffff)
		binary.LittleEndian.PutUint32(bad[len(bad)-4:], crc32.Checksum(bad[:len(bad)-4], analysisCRC))
		if _, err := UnmarshalPartitionAnalysis(bad); err == nil {
			t.Fatalf("accepted invalid count/options at %d", offset)
		}
	}
	var empty PartitionAnalysis
	if _, err := empty.MarshalBinary(); err == nil {
		t.Fatal("accepted zero synopsis")
	}
}

func FuzzPartitionAnalysisWire(f *testing.F) {
	a, err := AnalyzePartition(f.Context(), 1, "t", "s", []string{"/a", "/b"}, nil, analysisRows(20, 0), AnalyzeOptions{})
	if err != nil {
		f.Fatal(err)
	}
	wire, err := a.MarshalBinary()
	if err != nil {
		f.Fatal(err)
	}
	f.Add(wire)
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > MaxAnalysisWireBytes {
			return
		}
		if len(data) >= len(analysisMagic)+4 {
			data = bytes.Clone(data)
			copy(data, analysisMagic)
			binary.LittleEndian.PutUint32(data[len(data)-4:], crc32.Checksum(data[:len(data)-4], analysisCRC))
		}
		a, err := UnmarshalPartitionAnalysis(data)
		if err != nil {
			return
		}
		wire, err := a.MarshalBinary()
		if err != nil || !bytes.Equal(wire, data) {
			t.Fatalf("accepted noncanonical synopsis: %v", err)
		}
		if _, err := MergePartitionStatistics(t.Context(), a); err != nil {
			t.Fatalf("accepted unmergeable synopsis: %v", err)
		}
	})
}

func TestAnalysisWireBoundsExpandedGroupPaths(t *testing.T) {
	// Ordinal references on the wire must not expand into unbounded cloned
	// path strings in the analysis constructor.
	opts, _ := (AnalyzeOptions{}).defaults()
	a := &PartitionAnalysis{generation: 1, table: "t", partition: "s", options: opts}
	a.paths = append(a.paths, "/"+strings.Repeat("a", 512<<10))
	for i := 1; i <= 128; i++ {
		a.paths = append(a.paths, fmt.Sprintf("/z%03d", i))
	}
	for i, p := range a.paths {
		a.groups = append(a.groups, []string{p})
		a.ordinals = append(a.ordinals, []int{i})
	}
	for i := 1; i < len(a.paths); i++ {
		a.groups = append(a.groups, []string{a.paths[0], a.paths[i]})
		a.ordinals = append(a.ordinals, []int{0, i})
	}
	a.distinct = make([]distinctSample, len(a.groups))
	a.nulls = make([]uint64, len(a.paths))
	a.valueBytes = make([]uint64, len(a.paths))
	wire, err := a.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := UnmarshalPartitionAnalysis(wire); err == nil {
		t.Fatal("accepted over-budget expanded schema")
	}
}
