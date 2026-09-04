package storeio

import (
	"fmt"
	"testing"
)

// Read individual bits so this oracle does not share word loads or byte-field
// extraction with either production path.
func compactReadBitsOracle(src []byte, bit, width int) (value uint64) {
	for i := 0; i < width; i++ {
		value |= uint64(src[(bit+i)/8]>>uint((bit+i)&7)&1) << uint(i)
	}
	return
}

func TestCompactReadBitsAllWidthsAlignmentsAndTails(t *testing.T) {
	for _, pattern := range []byte{0, 0xff, 0x53} {
		var data [96]byte
		for i := range data {
			data[i] = pattern
			if pattern == 0x53 {
				data[i] = byte(i*197 + 83)
			}
		}
		for size := 0; size <= len(data); size++ {
			src := data[:size:size]
			for width := 0; width <= 64; width++ {
				for bit := 0; bit+width <= size*8; bit++ {
					got, want := compactReadBits(src, bit, width), compactReadBitsOracle(src, bit, width)
					if got != want {
						t.Fatalf("pattern=%x size=%d bit=%d width=%d: got %x want %x", pattern, size, bit, width, got, want)
					}
				}
			}
		}
	}
	for _, bit := range []int{-1, 0, 1, 1024} {
		if got := compactReadBits(nil, bit, 0); got != 0 {
			t.Fatalf("zero width: bit=%d got=%d", bit, got)
		}
	}
}

func FuzzCompactReadBits(f *testing.F) {
	f.Add([]byte{0xff, 0x82, 0x21, 0x53, 0x94, 0xfe, 0x31, 0x77, 0x81}, uint16(3), uint8(64))
	f.Add([]byte{1}, uint16(7), uint8(1))
	f.Fuzz(func(t *testing.T, src []byte, start uint16, size uint8) {
		width := int(size % 65)
		if width > len(src)*8 {
			return
		}
		bit := int(start) % (len(src)*8 - width + 1)
		if got, want := compactReadBits(src, bit, width), compactReadBitsOracle(src, bit, width); got != want {
			t.Fatalf("bit=%d width=%d: got %x want %x", bit, width, got, want)
		}
	})
}

var compactReadBitsBenchSink uint64

func BenchmarkCompactReadBits(b *testing.B) {
	var data [4096]byte
	for i := range data {
		data[i] = byte(i*197 + 83)
	}
	for _, width := range []int{0, 1, 7, 10, 16, 32, 56, 57, 64} {
		b.Run(fmt.Sprintf("width=%d", width), func(b *testing.B) {
			var sum uint64
			b.ReportAllocs()
			for b.Loop() {
				for bit := 3; bit < 32000; bit += 67 {
					sum += compactReadBits(data[:], bit, width)
				}
			}
			compactReadBitsBenchSink = sum
		})
	}
}
