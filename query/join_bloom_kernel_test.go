package query

import (
	"math/rand/v2"
	"testing"
)

// Compare the production dispatch with the original full signature, including
// overflowing uint32 products, both sign bits, empty/full blocks, and every lane.
func TestJoinBloomKernelDifferential(t *testing.T) {
	rng := rand.New(rand.NewPCG(19, 73))
	var bloom joinBloom
	for i := range 10000 {
		low := rng.Uint32()
		if i < 64 {
			low = uint32(1) << (i % 32)
			if i >= 32 {
				low = ^low
			}
		}
		if i == 64 {
			low = 0
		}
		if i == 65 {
			low = ^uint32(0)
		}
		_, signature := bloom.signature(uint64(low))
		for mode := range 4 {
			var block joinBloomBlock
			for lane := range block {
				switch mode {
				case 1:
					block[lane] = ^uint32(0)
				case 2:
					block[lane] = rng.Uint32()
				case 3:
					block[lane] = signature[lane]
				}
			}
			want := true
			for lane := range block {
				want = want && block[lane]&signature[lane] != 0
			}
			if got := joinBloomTestBlock(&block, low); got != want {
				t.Fatalf("test low=%08x mode=%d got=%v want=%v", low, mode, got, want)
			}
			if mode == 3 {
				for lane := range block {
					without := block
					without[lane] = 0
					if joinBloomTestBlock(&without, low) {
						t.Fatalf("missing lane %d admitted", lane)
					}
				}
			}
			expected := block
			for lane := range block {
				expected[lane] |= signature[lane]
			}
			joinBloomInsertBlock(&block, low)
			if block != expected {
				t.Fatalf("insert low=%08x mode=%d", low, mode)
			}
		}
	}
}

// The reference retains the pre-change signature materialization and counters.
func bloomAdmitsReference(b *joinBloom, hash uint64, pr *joinProbe) bool {
	index, word := b.signature(hash)
	block := &b.blocks[index]
	pr.tested++
	for i := range word {
		if block[i]&word[i] == 0 {
			return false
		}
	}
	pr.admitted++
	return true
}

func BenchmarkJoinBloomKernel(b *testing.B) {
	var filter joinBloom
	filter.reset(20000)
	rng := rand.New(rand.NewPCG(11, 31))
	hashes := make([]uint64, 32768)
	for i := range hashes {
		hashes[i] = rng.Uint64()
	}
	for _, hash := range hashes[:20000] {
		filter.insert(hash)
	}
	for _, hit := range []bool{false, true} {
		name, start := "miss", 20000
		if hit {
			name, start = "hit", 0
		}
		for _, reference := range []bool{true, false} {
			impl := "dispatch"
			if reference {
				impl = "original"
			}
			b.Run(name+"/"+impl, func(b *testing.B) {
				var probe joinProbe
				b.ReportAllocs()
				if reference {
					for i := range b.N {
						bloomAdmitsReference(&filter, hashes[start+(i&1023)], &probe)
					}
				} else {
					for i := range b.N {
						filter.admits(hashes[start+(i&1023)], &probe)
					}
				}
				if probe.tested != uint64(b.N) {
					b.Fatal("dispatch was not exercised")
				}
			})
		}
	}
	for _, reference := range []bool{true, false} {
		impl := "dispatch"
		if reference {
			impl = "original"
		}
		b.Run("insert/"+impl, func(b *testing.B) {
			b.ReportAllocs()
			if reference {
				for i := range b.N {
					index, word := filter.signature(hashes[i&1023])
					block := &filter.blocks[index]
					for lane := range word {
						block[lane] |= word[lane]
					}
					filter.inserted++
				}
			} else {
				for i := range b.N {
					filter.insert(hashes[i&1023])
				}
			}
		})
	}
}

// Original scalar signature, retained as a differential and benchmark oracle.
func (b *joinBloom) signature(hash uint64) (int, joinBloomBlock) {
	var word joinBloomBlock
	low := uint32(hash)
	for i := range word {
		word[i] = uint32(1) << ((low * joinBloomSalt[i]) >> 27)
	}
	return int(uint32(hash>>32) & b.mask), word
}
