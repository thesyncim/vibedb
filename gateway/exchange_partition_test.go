package gateway

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/shardservice"
)

func TestExchangeRowPartitionerUsesExactGroupIdentity(t *testing.T) {
	partitioner, err := newExchangeRowPartitioner([]int{0, 1}, 127)
	if err != nil {
		t.Fatal(err)
	}
	pairs := [][2][]shardservice.Cell{
		{
			{{Bytes: []byte(`1`)}, {Bytes: []byte(`"same"`)}},
			{{Bytes: []byte(`1.0`)}, {Bytes: []byte(`"sa\u006de"`)}},
		},
		{
			{{Bytes: []byte(`-0`)}, {Null: true}},
			{{Bytes: []byte(`0e999`)}, {Bytes: []byte(`null`)}},
		},
		{
			{{Bytes: []byte(`100`)}, {Bytes: []byte(`true`)}},
			{{Bytes: []byte(`1e2`)}, {Bytes: []byte(`true`)}},
		},
	}
	for i := range pairs {
		left, err := partitioner.partition(pairs[i][0])
		if err != nil {
			t.Fatalf("pair %d left: %v", i, err)
		}
		right, err := partitioner.partition(pairs[i][1])
		if err != nil || left != right {
			t.Fatalf("pair %d partitions = %d/%d, err %v", i, left, right, err)
		}
	}
	if _, err := partitioner.partition([]shardservice.Cell{{Bytes: []byte(`{`)}}); !errors.Is(err, ErrExchangePartitionKey) {
		t.Fatalf("invalid key = %v, want ErrExchangePartitionKey", err)
	}
}

func TestExchangeRowPartitionerValidation(t *testing.T) {
	for _, test := range []struct {
		columns    []int
		partitions uint32
	}{
		{nil, 1}, {[]int{0}, 0}, {[]int{-1}, 1}, {[]int{1, 1}, 1},
	} {
		if _, err := newExchangeRowPartitioner(test.columns, test.partitions); !errors.Is(err, ErrExchangePartitionKey) {
			t.Fatalf("newExchangeRowPartitioner(%v,%d) = %v", test.columns, test.partitions, err)
		}
	}
	partitioner, err := newExchangeRowPartitioner([]int{1}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := partitioner.partition([]shardservice.Cell{{Bytes: []byte(`1`)}}); !errors.Is(err, ErrExchangePartitionKey) {
		t.Fatalf("short row = %v", err)
	}
}

func TestExchangeRowPartitionerWarmAllocations(t *testing.T) {
	partitioner, err := newExchangeRowPartitioner([]int{0, 2}, 64)
	if err != nil {
		t.Fatal(err)
	}
	row := []shardservice.Cell{
		{Bytes: []byte(`123456789.25`)},
		{Bytes: []byte(`{"payload":true}`)},
		{Bytes: []byte(`"tenant-00000001"`)},
	}
	if _, err := partitioner.partition(row); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := partitioner.partition(row); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("warmed exact exchange partitioning allocated %.2f times", allocs)
	}
}

func BenchmarkExchangeRowPartitioner(b *testing.B) {
	partitioner, err := newExchangeRowPartitioner([]int{0, 2}, 256)
	if err != nil {
		b.Fatal(err)
	}
	row := []shardservice.Cell{
		{Bytes: []byte(`123456789.25`)},
		{Bytes: []byte(`{"payload":true}`)},
		{Bytes: []byte(`"tenant-00000001"`)},
	}
	if _, err := partitioner.partition(row); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := partitioner.partition(row); err != nil {
			b.Fatal(err)
		}
	}
}
