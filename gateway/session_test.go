package gateway

import (
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/shardservice"
)

func sessionPosition(distributionName, shard string, log byte, index uint64) Position {
	return Position{
		Distribution: distribution.DistributionName(distributionName),
		Shard:        distribution.ShardID(shard),
		LogID:        [16]byte{log},
		Index:        index,
	}
}

func TestSessionVectorSortedMaxAndImmutable(t *testing.T) {
	in := []Position{
		sessionPosition("z", "b", 1, 2),
		sessionPosition("a", "c", 2, 4),
		sessionPosition("a", "a", 3, 1),
		sessionPosition("z", "b", 1, 9),
	}
	v, err := NewSessionVector(in...)
	if err != nil {
		t.Fatalf("NewSessionVector: %v", err)
	}
	if v.Len() != 3 {
		t.Fatalf("Len = %d, want 3", v.Len())
	}
	got := v.Positions()
	if got[0].Distribution != "a" || got[0].Shard != "a" ||
		got[1].Distribution != "a" || got[1].Shard != "c" ||
		got[2].Distribution != "z" || got[2].Shard != "b" {
		t.Fatalf("positions are not source-sorted: %+v", got)
	}
	if got[2].Index != 9 {
		t.Fatalf("same-log maximum index = %d, want 9", got[2].Index)
	}

	// Neither caller input nor a returned view aliases retained vector state.
	in[0].Index = 100
	got[0].Index = 200
	p, ok := v.PositionFor("z", "b")
	if !ok || p.Index != 9 {
		t.Fatalf("retained position = (%+v, %t), want index 9", p, ok)
	}
}

func TestSessionVectorMergeAndWith(t *testing.T) {
	left, err := NewSessionVector(
		sessionPosition("d", "a", 1, 3),
		sessionPosition("d", "b", 2, 7),
	)
	if err != nil {
		t.Fatal(err)
	}
	right, err := NewSessionVector(
		sessionPosition("d", "a", 1, 8),
		sessionPosition("e", "c", 3, 2),
	)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := left.Merge(right)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	if merged.Len() != 3 {
		t.Fatalf("merged Len = %d, want 3", merged.Len())
	}
	if p, _ := merged.PositionFor("d", "a"); p.Index != 8 {
		t.Fatalf("merged maximum = %d, want 8", p.Index)
	}
	if p, _ := left.PositionFor("d", "a"); p.Index != 3 {
		t.Fatalf("Merge mutated receiver: index = %d, want 3", p.Index)
	}

	with, err := left.With(sessionPosition("d", "a", 1, 11))
	if err != nil {
		t.Fatalf("With: %v", err)
	}
	if p, _ := with.PositionFor("d", "a"); p.Index != 11 {
		t.Fatalf("With maximum = %d, want 11", p.Index)
	}
	if p, _ := left.PositionFor("d", "a"); p.Index != 3 {
		t.Fatalf("With mutated receiver: index = %d, want 3", p.Index)
	}
}

func TestSessionVectorFailsClosed(t *testing.T) {
	invalid := sessionPosition("d", "a", 1, 1)
	invalid.LogID = [16]byte{}
	if _, err := NewSessionVector(invalid); !errors.Is(err, shardservice.ErrInvalidPosition) {
		t.Fatalf("invalid position = %v, want shardservice.ErrInvalidPosition", err)
	}

	left := sessionPosition("d", "a", 1, 4)
	right := sessionPosition("d", "a", 2, 9)
	if _, err := NewSessionVector(left, right); !errors.Is(err, ErrPositionLineageRequired) {
		t.Fatalf("constructor lineage conflict = %v, want ErrPositionLineageRequired", err)
	}
	a, _ := NewSessionVector(left)
	b, _ := NewSessionVector(right)
	if _, err := a.Merge(b); !errors.Is(err, ErrPositionLineageRequired) {
		t.Fatalf("merge lineage conflict = %v, want ErrPositionLineageRequired", err)
	}

	tooMany := make([]Position, MaxSessionVectorEntries+1)
	for i := range tooMany {
		tooMany[i] = sessionPosition("d", fmt.Sprintf("s-%02d", i), byte(i+1), 1)
	}
	if _, err := NewSessionVector(tooMany...); !errors.Is(err, ErrSessionVectorOverflow) {
		t.Fatalf("constructor overflow = %v, want ErrSessionVectorOverflow", err)
	}

	first, second := tooMany[:32], tooMany[32:]
	va, err := NewSessionVector(first...)
	if err != nil {
		t.Fatal(err)
	}
	vb, err := NewSessionVector(second...)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := va.Merge(vb); !errors.Is(err, ErrSessionVectorOverflow) {
		t.Fatalf("merge overflow = %v, want ErrSessionVectorOverflow", err)
	}
}

func TestSessionVectorZeroValue(t *testing.T) {
	var v SessionVector
	if v.Len() != 0 || v.Positions() != nil {
		t.Fatalf("zero vector = len %d positions %#v, want empty", v.Len(), v.Positions())
	}
	if _, ok := v.PositionFor("d", "s"); ok {
		t.Fatal("zero vector returned a position")
	}
}

func TestSessionVectorPositionForZeroAlloc(t *testing.T) {
	positions := make([]Position, MaxSessionVectorEntries)
	for i := range positions {
		positions[i] = sessionPosition("d", fmt.Sprintf("s-%02d", i), byte(i+1), uint64(i+1))
	}
	v, err := NewSessionVector(positions...)
	if err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		p, ok := v.PositionFor("d", "s-42")
		if !ok || p.Index != 43 {
			panic("position not found")
		}
	})
	if allocs != 0 {
		t.Fatalf("PositionFor allocations = %g, want 0", allocs)
	}
}

func BenchmarkSessionVectorPositionFor(b *testing.B) {
	positions := make([]Position, MaxSessionVectorEntries)
	for i := range positions {
		positions[i] = sessionPosition("d", fmt.Sprintf("s-%02d", i), byte(i+1), uint64(i+1))
	}
	v, err := NewSessionVector(positions...)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, ok := v.PositionFor("d", "s-42")
		if !ok || p.Index != 43 {
			b.Fatal("position not found")
		}
	}
}
