package orderedkey

import (
	"bytes"
	"testing"
)

// Encoding sits on the hot path of every point lookup and every index probe, so
// both directions must run without allocating once the caller's buffer has
// capacity. Each benchmark reuses one buffer, which is how callers are expected
// to drive the package.

var (
	benchInt     = []byte("1234567")
	benchDecimal = []byte("-12345.678e-3")
	benchShort   = []byte("abcdefgh")
	benchLong    = bytes.Repeat([]byte("x"), 256)
)

func benchKey(tb testing.TB, build func(dst []byte) ([]byte, bool)) []byte {
	tb.Helper()
	key, ok := build(nil)
	if !ok {
		tb.Fatal("build")
	}
	return key
}

func BenchmarkEncodeInteger(b *testing.B) {
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		var ok bool
		dst, ok = AppendNumber(dst[:0], benchInt, Ascending)
		if !ok {
			b.Fatal("encode")
		}
	}
}

func BenchmarkEncodeDecimal(b *testing.B) {
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		var ok bool
		dst, ok = AppendNumber(dst[:0], benchDecimal, Ascending)
		if !ok {
			b.Fatal("encode")
		}
	}
}

func BenchmarkEncodeStringShort(b *testing.B) {
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		var ok bool
		dst, ok = AppendString(dst[:0], benchShort, Ascending)
		if !ok {
			b.Fatal("encode")
		}
	}
}

func BenchmarkEncodeStringLong(b *testing.B) {
	dst := make([]byte, 0, 512)
	b.ReportAllocs()
	for b.Loop() {
		var ok bool
		dst, ok = AppendString(dst[:0], benchLong, Ascending)
		if !ok {
			b.Fatal("encode")
		}
	}
}

func BenchmarkEncodeComposite2(b *testing.B) {
	dst := make([]byte, 0, 128)
	b.ReportAllocs()
	for b.Loop() {
		var ok bool
		dst, ok = AppendString(dst[:0], benchShort, Ascending)
		if !ok {
			b.Fatal("encode")
		}
		dst, ok = AppendNumber(dst, benchInt, Ascending)
		if !ok {
			b.Fatal("encode")
		}
	}
}

func BenchmarkEncodeComposite3(b *testing.B) {
	dst := make([]byte, 0, 128)
	b.ReportAllocs()
	for b.Loop() {
		var ok bool
		dst, ok = AppendString(dst[:0], benchShort, Ascending)
		if !ok {
			b.Fatal("encode")
		}
		dst, ok = AppendNumber(dst, benchInt, Descending)
		if !ok {
			b.Fatal("encode")
		}
		dst, ok = AppendBool(dst, true, Ascending)
		if !ok {
			b.Fatal("encode")
		}
	}
}

// The seek-target path is what Leapfrog Triejoin calls per seek(x): encode one
// component standalone, then compare it against a level's span.
func BenchmarkEncodeSeekTarget(b *testing.B) {
	dst := make([]byte, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		var ok bool
		dst, ok = AppendNumber(dst[:0], benchInt, Ascending)
		if !ok {
			b.Fatal("encode")
		}
	}
}

func BenchmarkDecodeInteger(b *testing.B) {
	key := benchKey(b, func(dst []byte) ([]byte, bool) { return AppendNumber(dst, benchInt, Ascending) })
	buf := make([]byte, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		if _, _, _, err := DecodeComponent(buf[:0], key, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeStringShort(b *testing.B) {
	key := benchKey(b, func(dst []byte) ([]byte, bool) { return AppendString(dst, benchShort, Ascending) })
	buf := make([]byte, 0, 64)
	b.ReportAllocs()
	for b.Loop() {
		if _, _, _, err := DecodeComponent(buf[:0], key, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeStringLong(b *testing.B) {
	key := benchKey(b, func(dst []byte) ([]byte, bool) { return AppendString(dst, benchLong, Ascending) })
	buf := make([]byte, 0, 512)
	b.ReportAllocs()
	for b.Loop() {
		if _, _, _, err := DecodeComponent(buf[:0], key, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeComposite3(b *testing.B) {
	key := benchKey(b, func(dst []byte) ([]byte, bool) {
		dst, ok := AppendString(dst, benchShort, Ascending)
		if !ok {
			return dst, false
		}
		dst, ok = AppendNumber(dst, benchInt, Descending)
		if !ok {
			return dst, false
		}
		return AppendBool(dst, true, Ascending)
	})
	buf := make([]byte, 0, 128)
	b.ReportAllocs()
	for b.Loop() {
		out, off := buf[:0], 0
		for off < len(key) {
			var err error
			_, out, off, err = DecodeComponent(out, key, off)
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

// Decoding must not allocate either, or returning key columns would cost more
// than the copy it exists to avoid.
func TestDecodeZeroAllocation(t *testing.T) {
	key, ok := AppendString(nil, []byte("customer\x00name"), Ascending)
	if !ok {
		t.Fatal("string")
	}
	key, ok = AppendNumber(key, []byte("-123456.7500e-2"), Descending)
	if !ok {
		t.Fatal("number")
	}
	key, ok = AppendBool(key, true, Ascending)
	if !ok {
		t.Fatal("bool")
	}
	buf := make([]byte, 0, 128)
	if allocations := testing.AllocsPerRun(1000, func() {
		out, off := buf[:0], 0
		for off < len(key) {
			var err error
			_, out, off, err = DecodeComponent(out, key, off)
			if err != nil {
				panic(err)
			}
		}
	}); allocations != 0 {
		t.Fatalf("allocations = %v", allocations)
	}
}
