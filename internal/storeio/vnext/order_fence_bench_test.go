package vnext

import (
	"bytes"
	"fmt"
	"sort"
	"testing"
)

const (
	orderPrototypeRowsPerBlock = 64
	orderPrototypeRestartRows  = 8
	orderPrototypeKeyLimit     = 64
)

type orderPrototypeBlock struct {
	id       uint32
	count    uint8
	data     []byte
	restarts [orderPrototypeRowsPerBlock / orderPrototypeRestartRows]uint16
	first    string
	last     string
}

type orderPrototypeFence struct {
	separator string
	block     uint32
}

type orderPrototype struct {
	blocks []orderPrototypeBlock
	fences []orderPrototypeFence
	bytes  int
}

func buildOrderPrototype(keys []string) orderPrototype {
	var out orderPrototype
	for first := 0; first < len(keys); first += orderPrototypeRowsPerBlock {
		last := min(first+orderPrototypeRowsPerBlock, len(keys))
		block := orderPrototypeBlock{
			id: uint32(len(out.blocks)), count: uint8(last - first),
			first: keys[first], last: keys[last-1],
		}
		previous := ""
		for rank, key := range keys[first:last] {
			if rank%orderPrototypeRestartRows == 0 {
				block.restarts[rank/orderPrototypeRestartRows] = uint16(len(block.data))
				previous = ""
			}
			shared := commonOrderPrototypePrefix(previous, key)
			suffix := key[shared:]
			block.data = append(block.data, byte(shared), byte(len(suffix)), byte(rank))
			block.data = append(block.data, suffix...)
			previous = key
		}
		out.bytes += len(block.data) + 2*((int(block.count)+orderPrototypeRestartRows-1)/orderPrototypeRestartRows)
		out.blocks = append(out.blocks, block)
	}
	for block := 1; block < len(out.blocks); block++ {
		separator := shortestOrderPrototypeSeparator(
			out.blocks[block-1].last, out.blocks[block].first,
		)
		out.fences = append(out.fences, orderPrototypeFence{
			separator: separator, block: uint32(block),
		})
		out.bytes += len(separator) + 4
	}
	return out
}

func shortestOrderPrototypeSeparator(left, right string) string {
	if !(left < right) {
		return ""
	}
	for length := 1; length <= len(right); length++ {
		candidate := right[:length]
		if left < candidate {
			return candidate
		}
	}
	return right
}

func commonOrderPrototypePrefix(left, right string) int {
	limit := min(len(left), len(right), 255)
	at := 0
	for at < limit && left[at] == right[at] {
		at++
	}
	return at
}

func (p *orderPrototype) seek(target string, key *[orderPrototypeKeyLimit]byte) (block, rank int, ok bool) {
	block = sort.Search(len(p.fences), func(index int) bool {
		return p.fences[index].separator > target
	})
	rank, ok = p.blocks[block].seek(target, key)
	if !ok && block+1 < len(p.blocks) {
		block++
		rank, ok = p.blocks[block].seek(target, key)
	}
	return block, rank, ok
}

func (b *orderPrototypeBlock) seek(target string, key *[orderPrototypeKeyLimit]byte) (int, bool) {
	restartCount := (int(b.count) + orderPrototypeRestartRows - 1) / orderPrototypeRestartRows
	restart := sort.Search(restartCount, func(index int) bool {
		length, _ := b.decodeRestart(index, key)
		return string(key[:length]) > target
	}) - 1
	if restart < 0 {
		restart = 0
	}
	at := int(b.restarts[restart])
	length := 0
	for rank := restart * orderPrototypeRestartRows; rank < int(b.count); rank++ {
		var slot int
		length, at, slot = b.decode(at, length, key)
		if string(key[:length]) >= target {
			return slot, true
		}
		if rank+1 < int(b.count) && (rank+1)%orderPrototypeRestartRows == 0 {
			break
		}
	}
	return 0, false
}

func (b *orderPrototypeBlock) decodeRestart(
	restart int, key *[orderPrototypeKeyLimit]byte,
) (length, slot int) {
	at := int(b.restarts[restart])
	length, _, slot = b.decode(at, 0, key)
	return length, slot
}

func (b *orderPrototypeBlock) decode(
	at, previous int, key *[orderPrototypeKeyLimit]byte,
) (length, next, slot int) {
	shared, suffix := int(b.data[at]), int(b.data[at+1])
	slot = int(b.data[at+2])
	if shared > previous || shared+suffix > len(key) {
		panic("invalid order prototype")
	}
	copy(key[shared:shared+suffix], b.data[at+3:at+3+suffix])
	return shared + suffix, at + 3 + suffix, slot
}

func orderPrototypeKeys(count int) []string {
	keys := make([]string, count)
	for index := range keys {
		keys[index] = fmt.Sprintf("doc:%08d", index)
	}
	return keys
}

func TestOrderFencePrototypeShortestSeparatorsAndSeek(t *testing.T) {
	keys := []string{
		"a", "aa", "abz", "acaaa", "b", "b\x00", "b\xff", "c",
	}
	for left := range len(keys) - 1 {
		separator := shortestOrderPrototypeSeparator(keys[left], keys[left+1])
		if !(keys[left] < separator && separator <= keys[left+1]) {
			t.Fatalf("separator(%q,%q) = %q", keys[left], keys[left+1], separator)
		}
		for length := 1; length < len(separator); length++ {
			if keys[left] < separator[:length] {
				t.Fatalf("separator %q is not shortest; prefix %q also separates",
					separator, separator[:length])
			}
		}
	}

	corpus := orderPrototypeKeys(10_003)
	prototype := buildOrderPrototype(corpus)
	var key [orderPrototypeKeyLimit]byte
	for index, want := range corpus {
		block, slot, ok := prototype.seek(want, &key)
		if !ok || block != index/orderPrototypeRowsPerBlock ||
			slot != index%orderPrototypeRowsPerBlock {
			t.Fatalf("seek %q = (%d,%d,%v)", want, block, slot, ok)
		}
	}
	for index, fence := range prototype.fences {
		left, right := prototype.blocks[index], prototype.blocks[index+1]
		if !(left.last < fence.separator && fence.separator <= right.first) ||
			fence.block != uint32(index+1) {
			t.Fatalf("fence %d = %+v between %q and %q", index, fence, left.last, right.first)
		}
	}

	// Seeking exact keys is not enough to prove a range index: lower bounds
	// usually fall between stored keys, including exactly on a shortened fence.
	// Compare the prototype with the complete sorted corpus so a separator can
	// neither skip the first qualifying row nor route into its left sibling.
	gapped := buildOrderPrototype(keys)
	targets := []string{
		"", "a", "a\x00", "aaa", "ab", "abz", "abz\x00", "ac", "az",
		"b", "b\x00", "b\x01", "b\xfe", "b\xff", "b\xff\x00", "c", "c\x00", "\xff",
	}
	for _, target := range targets {
		want := sort.SearchStrings(keys, target)
		block, slot, ok := gapped.seek(target, &key)
		if want == len(keys) {
			if ok {
				t.Fatalf("seek %q = (%d,%d,true), want end", target, block, slot)
			}
			continue
		}
		if !ok {
			t.Fatalf("seek %q missed %q", target, keys[want])
		}
		got := block*orderPrototypeRowsPerBlock + slot
		if got != want {
			t.Fatalf("seek %q = row %d (%q), want row %d (%q)",
				target, got, keys[got], want, keys[want])
		}
	}
	for boundary := orderPrototypeRowsPerBlock; boundary < len(corpus); boundary += orderPrototypeRowsPerBlock {
		target := corpus[boundary-1] + "\x00"
		block, slot, ok := prototype.seek(target, &key)
		if !ok {
			t.Fatalf("seek between blocks at row %d missed", boundary)
		}
		got := block*orderPrototypeRowsPerBlock + slot
		if got != boundary {
			t.Fatalf("seek between blocks at row %d = row %d", boundary, got)
		}
	}
}

func BenchmarkOrderFencePrototype(b *testing.B) {
	const rows = 100_000
	keys := orderPrototypeKeys(rows)
	prototype := buildOrderPrototype(keys)
	reportLayout := func(b *testing.B) {
		b.ReportMetric(float64(prototype.bytes)/rows, "order-B/row")
		b.ReportMetric(float64(len(prototype.fences)), "fences")
	}

	b.Run("seek", func(b *testing.B) {
		var key [orderPrototypeKeyLimit]byte
		sink := 0
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			probe := keys[(i*8191)%len(keys)]
			block, slot, ok := prototype.seek(probe, &key)
			if !ok {
				b.Fatal("seek miss")
			}
			sink ^= block ^ slot
		}
		if sink == -1 {
			b.Fatal(sink)
		}
		reportLayout(b)
	})

	b.Run("scan-keys", func(b *testing.B) {
		var key [orderPrototypeKeyLimit]byte
		sink := byte(0)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			for block := range prototype.blocks {
				item := &prototype.blocks[block]
				at, length := 0, 0
				for rank := 0; rank < int(item.count); rank++ {
					if rank%orderPrototypeRestartRows == 0 {
						length = 0
					}
					length, at, _ = item.decode(at, length, &key)
					sink ^= key[length-1]
				}
			}
		}
		b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*rows), "ns/row")
		if sink == 0xff && bytes.Equal(key[:1], nil) {
			b.Fatal(sink)
		}
		reportLayout(b)
	})

	b.Run("range-32", func(b *testing.B) {
		var key [orderPrototypeKeyLimit]byte
		sink := 0
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			start := (iteration * 8191) % (len(keys) - 32)
			block, rank, ok := prototype.seek(keys[start], &key)
			if !ok {
				b.Fatal("range seek miss")
			}
			for row := 0; row < 32; row++ {
				sink ^= int(prototype.blocks[block].id) ^ rank
				rank++
				if rank == int(prototype.blocks[block].count) {
					block++
					rank = 0
				}
			}
		}
		if sink == -1 {
			b.Fatal(sink)
		}
		reportLayout(b)
	})
}
