package exchange

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func TestBlockRoundTripBorrowedDecodeAndReuse(t *testing.T) {
	storage := make([]byte, 0, 256)
	var builder BlockBuilder
	if err := builder.Reset(storage, 3, 4, 256); err != nil {
		t.Fatal(err)
	}
	rows := [][]Cell{
		{{Bytes: []byte(`"a"`)}, {Null: true}, {Bytes: []byte(`1`)}},
		{{Bytes: []byte(`"b"`)}, {Bytes: []byte(`true`)}, {Bytes: []byte(`1.0`)}},
	}
	for i := range rows {
		if err := builder.AppendRow(rows[i]); err != nil {
			t.Fatalf("AppendRow %d: %v", i, err)
		}
	}
	encoded, count := builder.Bytes()
	if count != 2 || len(encoded) > cap(storage) {
		t.Fatalf("block rows/size = %d/%d cap %d", count, len(encoded), cap(storage))
	}
	block, err := OpenBlock(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if block.Columns() != 3 || block.Rows() != 2 {
		t.Fatalf("block shape = %dx%d", block.Rows(), block.Columns())
	}
	decoded := make([]Cell, 3)
	for i := range rows {
		if !block.NextInto(decoded) {
			t.Fatalf("NextInto row %d = false", i)
		}
		for column := range decoded {
			if decoded[column].Null != rows[i][column].Null ||
				!bytes.Equal(decoded[column].Bytes, rows[i][column].Bytes) {
				t.Fatalf("row %d column %d = %+v, want %+v", i, column, decoded[column], rows[i][column])
			}
		}
	}
	if block.NextInto(decoded) {
		t.Fatal("NextInto returned a row after EOF")
	}

	// Decoded payloads borrow the block rather than allocating per cell.
	block, err = OpenBlock(encoded)
	if err != nil || !block.NextInto(decoded) {
		t.Fatalf("reopen = %v", err)
	}
	encoded[blockHeader+6] = 'z'
	if got := string(decoded[0].Bytes); got != `"z"` {
		t.Fatalf("decoded bytes = %q, want borrowed mutation", got)
	}

	previous := encoded
	if err := builder.Reset(previous, 1, 1, 256); err != nil {
		t.Fatal(err)
	}
	if next, _ := builder.Bytes(); cap(next) != cap(previous) {
		t.Fatalf("Reset capacity = %d, want reused %d", cap(next), cap(previous))
	}
}

func TestBlockBuilderLimitsAreAtomic(t *testing.T) {
	var builder BlockBuilder
	if err := builder.Reset(nil, 2, 1, 24); err != nil {
		t.Fatal(err)
	}
	before, rows := builder.Bytes()
	before = bytes.Clone(before)
	if err := builder.AppendRow([]Cell{{Bytes: []byte("1234")}, {Bytes: []byte("5678")}}); !errors.Is(err, ErrBlockLimit) {
		t.Fatalf("oversized AppendRow = %v, want ErrBlockLimit", err)
	}
	after, afterRows := builder.Bytes()
	if rows != afterRows || !bytes.Equal(before, after) {
		t.Fatalf("refused row changed block: rows %d -> %d, %x -> %x", rows, afterRows, before, after)
	}
	if err := builder.AppendRow([]Cell{{Null: true, Bytes: []byte("x")}, {Null: true}}); !errors.Is(err, ErrBlockShape) {
		t.Fatalf("noncanonical null = %v, want ErrBlockShape", err)
	}
	if err := builder.AppendRow([]Cell{{Null: true}, {Null: true}}); err != nil {
		t.Fatal(err)
	}
	if err := builder.AppendRow([]Cell{{Null: true}, {Null: true}}); !errors.Is(err, ErrBlockLimit) {
		t.Fatalf("row overflow = %v, want ErrBlockLimit", err)
	}
}

func TestPartitionedBlocksFlushAndReuse(t *testing.T) {
	blocks, err := NewPartitionedBlocks(4, 1, 2, 64, 4*64)
	if err != nil {
		t.Fatal(err)
	}
	if blocks.Partitions() != 4 || blocks.ReservedBytes() != 256 {
		t.Fatalf("partition reservation = %d/%d", blocks.Partitions(), blocks.ReservedBytes())
	}
	row := []Cell{{Bytes: []byte("payload")}}
	for i := 0; i < 2; i++ {
		flush, err := blocks.Append(2, row)
		if err != nil || flush {
			t.Fatalf("Append %d = flush %v, err %v", i, flush, err)
		}
	}
	if flush, err := blocks.Append(2, row); err != nil || !flush {
		t.Fatalf("full Append = flush %v, err %v", flush, err)
	}
	data, rows, err := blocks.Block(2)
	if err != nil || rows != 2 {
		t.Fatalf("Block = %d rows, %v", rows, err)
	}
	capacity := cap(data)
	if err := blocks.ResetPartition(2); err != nil {
		t.Fatal(err)
	}
	if flush, err := blocks.Append(2, row); err != nil || flush {
		t.Fatalf("post-reset Append = flush %v, err %v", flush, err)
	}
	data, rows, err = blocks.Block(2)
	if err != nil || rows != 1 || cap(data) != capacity {
		t.Fatalf("reused Block = rows %d cap %d, err %v; want cap %d", rows, cap(data), err, capacity)
	}
	if _, _, err := blocks.Block(4); !errors.Is(err, ErrPartitions) {
		t.Fatalf("bad partition Block = %v", err)
	}
	if _, err := NewPartitionedBlocks(4, 1, 2, 64, 255); !errors.Is(err, ErrBlockLimit) {
		t.Fatalf("under-reserved blocks = %v", err)
	}
}

func TestOpenBlockRejectsMalformedData(t *testing.T) {
	valid := func() []byte {
		var builder BlockBuilder
		if err := builder.Reset(nil, 1, 1, 64); err != nil {
			t.Fatal(err)
		}
		if err := builder.AppendRow([]Cell{{Bytes: []byte("x")}}); err != nil {
			t.Fatal(err)
		}
		data, _ := builder.Bytes()
		return bytes.Clone(data)
	}()
	tests := []struct {
		name string
		edit func([]byte) []byte
		want error
	}{
		{"short", func(data []byte) []byte { return data[:blockHeader-1] }, ErrBlockData},
		{"magic", func(data []byte) []byte { data[0] ^= 1; return data }, ErrBlockData},
		{"zero_columns", func(data []byte) []byte { binary.BigEndian.PutUint32(data[4:], 0); return data }, ErrBlockLimit},
		{"truncated_value", func(data []byte) []byte { return data[:len(data)-1] }, ErrBlockData},
		{"bad_kind", func(data []byte) []byte { data[blockHeader] = 2; return data }, ErrBlockData},
		{"trailing", func(data []byte) []byte { return append(data, 0) }, ErrBlockData},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := test.edit(bytes.Clone(valid))
			if _, err := OpenBlock(data); !errors.Is(err, test.want) {
				t.Fatalf("OpenBlock = %v, want %v", err, test.want)
			}
		})
	}
}

func TestBlockWarmPathAllocations(t *testing.T) {
	storage := make([]byte, 0, 256)
	row := []Cell{{Bytes: []byte(`"abc"`)}, {Bytes: []byte(`123`)}, {Null: true}}
	decoded := make([]Cell, len(row))
	var builder BlockBuilder
	allocs := testing.AllocsPerRun(1000, func() {
		if err := builder.Reset(storage, 3, 2, 256); err != nil {
			panic(err)
		}
		if err := builder.AppendRow(row); err != nil {
			panic(err)
		}
		data, _ := builder.Bytes()
		block, err := OpenBlock(data)
		if err != nil || !block.NextInto(decoded) {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("warm block build/open/decode allocated %.2f times", allocs)
	}
}

func FuzzOpenBlock(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("VBXB"))
	var builder BlockBuilder
	if err := builder.Reset(nil, 1, 1, 64); err != nil {
		f.Fatal(err)
	}
	if err := builder.AppendRow([]Cell{{Bytes: []byte(`{"x":1}`)}}); err != nil {
		f.Fatal(err)
	}
	valid, _ := builder.Bytes()
	f.Add(bytes.Clone(valid))
	f.Fuzz(func(t *testing.T, data []byte) {
		block, err := OpenBlock(data)
		if err != nil {
			return
		}
		row := make([]Cell, block.Columns())
		for block.NextInto(row) {
		}
	})
}

func BenchmarkBlockBuildOpenDecode(b *testing.B) {
	storage := make([]byte, 0, MaxBatchBytes)
	row := []Cell{
		{Bytes: []byte(`"tenant-00000001"`)},
		{Bytes: []byte(`123456789`)},
		{Bytes: []byte(`{"active":true,"class":"hot"}`)},
	}
	decoded := make([]Cell, len(row))
	var builder BlockBuilder
	b.ReportAllocs()
	b.SetBytes(int64(len(row[0].Bytes) + len(row[1].Bytes) + len(row[2].Bytes)))
	b.ResetTimer()
	for range b.N {
		if err := builder.Reset(storage, uint32(len(row)), 1, MaxBatchBytes); err != nil {
			b.Fatal(err)
		}
		if err := builder.AppendRow(row); err != nil {
			b.Fatal(err)
		}
		data, _ := builder.Bytes()
		block, err := OpenBlock(data)
		if err != nil || !block.NextInto(decoded) {
			b.Fatal(err)
		}
	}
}
