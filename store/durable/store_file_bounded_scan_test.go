package durable

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestBoundedSequentialScanBinaryPrefixesOverflowAndReuse(t *testing.T) {
	type row struct {
		key   string
		value []byte
	}
	records := make([]row, 0, 512)
	for _, prefix := range []string{"a", "a\xff", "b", "\xff"} {
		for i := range 128 {
			value := fmt.Appendf(nil, `{"id":%d,"value":"%s"}`, i, strings.Repeat("x", 1+i%23))
			if i%16 == 15 {
				value = fmt.Appendf(nil, `{"id":%d,"value":"%s"}`, i, strings.Repeat("y", 70_000))
			}
			records = append(records, row{fmt.Sprintf("%s%03d", prefix, i), value})
		}
	}
	slices.SortFunc(records, func(a, b row) int { return strings.Compare(a.key, b.key) })
	keys, values := make([]string, len(records)), make([][]byte, len(records))
	for i, record := range records {
		keys[i], values[i] = record.key, record.value
	}
	initial := slices.Clone(values)
	for i := range initial {
		if len(initial[i]) > 512 {
			initial[i] = []byte(`{"id":0}`)
		}
	}
	collection := unifiedBenchStoreWith(t, keys, initial, unifiedBenchOptions(), unifiedBenchOptions())
	old, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	for i := range values {
		if len(values[i]) > 512 {
			if _, err := collection.Put([]byte(keys[i]), values[i]); err != nil {
				t.Fatal(err)
			}
		}
	}
	oldAt := 0
	if err := old.RangeBoundsRaw([]byte(keys[0]), []byte(keys[len(keys)-1]+"\x00"), false, func(key, value []byte) error {
		if oldAt >= len(keys) || string(key) != keys[oldAt] || !bytes.Equal(value, initial[oldAt]) {
			return fmt.Errorf("held generation row %d mismatch", oldAt)
		}
		oldAt++
		return nil
	}); err != nil || oldAt != len(keys) {
		t.Fatalf("held generation: rows=%d err=%v", oldAt, err)
	}
	snapshot, err := collection.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	var scratch []byte
	for _, prefix := range []string{"", "a", "a\xff", "a\xff0", "a\xff127", "\xff", "\xff\xff", strings.Repeat("z", 257)} {
		t.Run(fmt.Sprintf("prefix=%x", prefix), func(t *testing.T) {
			var want []row
			for _, record := range records {
				if strings.HasPrefix(record.key, prefix) {
					want = append(want, record)
				}
			}
			for range 2 {
				at := 0
				visit := func(key, value []byte) error {
					if at >= len(want) || string(key) != want[at].key || !bytes.Equal(value, want[at].value) {
						return fmt.Errorf("row %d mismatch: key %x value length %d", at, key, len(value))
					}
					at++
					// A callback may overwrite the borrowed render buffer. Sequential
					// decoding must retain its own key prefix and immutable scalar inputs.
					clear(key)
					clear(value)
					return nil
				}
				scratch, err = snapshot.RangePrefixRawBuffer([]byte(prefix), scratch, visit)
				if err != nil || at != len(want) {
					t.Fatalf("scan rows=%d want=%d: %v", at, len(want), err)
				}
			}
		})
	}
	for _, bounds := range [][2]int{{0, 512}, {0, 16}, {15, 17}, {16, 256}, {127, 129}, {255, 257}, {511, 512}, {16, 16}} {
		t.Run(fmt.Sprintf("range=%d-%d", bounds[0], bounds[1]), func(t *testing.T) {
			lower := []byte(keys[bounds[0]])
			upper := []byte(keys[len(keys)-1] + "\x00")
			if bounds[1] < len(keys) {
				upper = []byte(keys[bounds[1]])
			}
			at := bounds[0]
			scratch, err = snapshot.RangeBoundsRawBuffer(lower, upper, scratch, false, func(key, value []byte) error {
				if at >= bounds[1] || string(key) != keys[at] || !bytes.Equal(value, values[at]) {
					return fmt.Errorf("row %d mismatch", at)
				}
				at++
				return nil
			})
			if err != nil || at != bounds[1] {
				t.Fatalf("stopped at %d want %d: %v", at, bounds[1], err)
			}
		})
	}
	callbackErr := errors.New("stop bounded scan")
	_, err = snapshot.RangeBoundsRawBuffer([]byte(keys[0]), []byte(keys[100]), scratch, false, func(_, _ []byte) error { return callbackErr })
	if !errors.Is(err, callbackErr) {
		t.Fatalf("callback error = %v", err)
	}
	// Each worker owns its snapshot/scratch while sharing the reopened store.
	var workers sync.WaitGroup
	for part := range 4 {
		snap, err := collection.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		workers.Go(func() {
			defer snap.Close()
			lower := []byte(keys[part*128])
			upper := []byte(keys[len(keys)-1] + "\x00")
			if part < 3 {
				upper = []byte(keys[(part+1)*128])
			}
			for range 3 {
				at := part * 128
				err := snap.RangeBoundsRaw(lower, upper, false, func(key, value []byte) error {
					if at >= (part+1)*128 || string(key) != keys[at] || !bytes.Equal(value, values[at]) {
						return fmt.Errorf("partition %d row %d mismatch", part, at)
					}
					at++
					return nil
				})
				if err != nil || at != (part+1)*128 {
					t.Errorf("partition %d stopped at %d: %v", part, at, err)
					return
				}
			}
		})
	}
	workers.Wait()
}
