package storeio

import (
	"errors"
	"testing"
)

func TestGenerationMigrationDirtySetCoalescesAndBackpressures(t *testing.T) {
	set, err := NewGenerationMigrationDirtySet(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := set.Mark(7, 10); err != nil {
		t.Fatal(err)
	}
	if err := set.Mark(7, 11); err != nil {
		t.Fatal(err)
	}
	if err := set.Mark(9, 12); err != nil {
		t.Fatal(err)
	}
	if err := set.Mark(11, 13); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("full mark = %v, want ErrQueueFull", err)
	}
	ids, generation, topology := set.Drain(make([]uint64, 0, set.Capacity()))
	if len(ids) != 2 || generation != 12 || topology {
		t.Fatalf("drain = %v,%d,%v", ids, generation, topology)
	}
	if err := set.Mark(11, 13); err != nil {
		t.Fatal(err)
	}
}

func TestGenerationMigrationDirtySetObservesCanonicalPublications(t *testing.T) {
	set, err := NewGenerationMigrationDirtySet(8)
	if err != nil {
		t.Fatal(err)
	}
	route := func(key []byte) (uint64, bool) {
		if len(key) == 0 {
			return 0, false
		}
		return uint64(key[0]), true
	}
	observer := set.ObservePublication(route)
	descriptor, err := EncodePublicationDescriptor(
		make([]byte, 4096),
		[]PublicationMutation{
			{Key: []byte("apple"), Value: []byte(`{"v":1}`)},
			{Key: []byte("apricot"), Delete: true},
			{Key: []byte("banana"), Value: []byte(`{"v":2}`)},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := observer(20, descriptor); err != nil {
		t.Fatal(err)
	}
	ids, generation, topology := set.Drain(make([]uint64, 0, set.Capacity()))
	if len(ids) != 2 || generation != 20 || topology {
		t.Fatalf("mutation drain = %v,%d,%v", ids, generation, topology)
	}
	topologyDescriptor, err := EncodePublicationDescriptor(make([]byte, 4096), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := observer(21, topologyDescriptor); err != nil {
		t.Fatal(err)
	}
	ids, generation, topology = set.Drain(ids[:0])
	if len(ids) != 0 || generation != 21 || !topology {
		t.Fatalf("topology drain = %v,%d,%v", ids, generation, topology)
	}
}

func TestGenerationMigrationDirtySetWarmObserverIsZeroAllocation(t *testing.T) {
	set, err := NewGenerationMigrationDirtySet(8)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := EncodePublicationDescriptor(
		make([]byte, 4096),
		[]PublicationMutation{{Key: []byte("key"), Value: []byte(`{"v":1}`)}},
	)
	if err != nil {
		t.Fatal(err)
	}
	observer := set.ObservePublication(func([]byte) (uint64, bool) { return 7, true })
	if err := observer(1, descriptor); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if err := observer(2, descriptor); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("observer allocs/run = %.2f, want 0", allocs)
	}
}
