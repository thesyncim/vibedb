package routegate

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestStatusCanonicalFixedGrammar(t *testing.T) {
	statuses := []Status{
		{Epoch: 1},
		{Revision: 9, Epoch: 7, ActivePins: 2, ReleasedPins: 3, RetainedRecords: 5},
		{Revision: 10, Epoch: 8, Drain: DrainRecord{Identity: Identity{1}, Binding: Binding{2}, Epoch: 8, State: DrainActive}},
	}
	for _, status := range statuses {
		raw, err := AppendStatus(nil, status)
		if err != nil {
			t.Fatal(err)
		}
		got, err := OpenStatus(raw)
		if err != nil || got != status || len(raw) != StatusBytes {
			t.Fatalf("roundtrip %+v %v", got, err)
		}
		again, _ := AppendStatus(nil, got)
		if !bytes.Equal(raw, again) {
			t.Fatal("noncanonical")
		}
		for i := 0; i < len(raw); i++ {
			bad := append([]byte(nil), raw...)
			bad[i] ^= 1
			if _, err := OpenStatus(bad); err == nil {
				t.Fatalf("corruption %d accepted", i)
			}
		}
		for n := 0; n < len(raw); n++ {
			if _, err := OpenStatus(raw[:n]); err == nil {
				t.Fatalf("truncation %d", n)
			}
		}
		if _, err := OpenStatus(append(raw, 0)); err == nil {
			t.Fatal("trailing byte")
		}
	}
	raw, _ := AppendStatus(nil, statuses[1])
	for _, offset := range []int{4, 5, 7, 16, 24, 32, 40, 48, 56, 88} {
		bad := append([]byte(nil), raw...)
		switch offset {
		case 16:
			clear(bad[16:24])
		case 24:
			binary.LittleEndian.PutUint64(bad[24:32], 6)
		default:
			bad[offset] ^= 1
		}
		binary.LittleEndian.PutUint32(bad[statusBodyBytes:], crc32.Checksum(bad[:statusBodyBytes], castagnoli))
		if _, err := OpenStatus(bad); err == nil {
			t.Fatalf("canonical invalid field %d accepted", offset)
		}
	}
	var scratch [StatusBytes]byte
	if allocations := testing.AllocsPerRun(1000, func() {
		raw, err := AppendStatus(scratch[:0], statuses[1])
		if err != nil {
			panic(err)
		}
		if _, err = OpenStatus(raw); err != nil {
			panic(err)
		}
	}); allocations != 0 {
		t.Fatalf("allocs=%g", allocations)
	}
}
