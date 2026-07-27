package store

import (
	"fmt"
	"testing"
)

// AppendIndexRawKeys validates each raw needle before it probes anything, so
// the validation is a fixed per-call cost that a caller pays on every lookup.
// This benchmark isolates it the only way the public surface allows: the
// corpus is one document and the index is on one path, so the probe below the
// needle preparation is as small as it can be and what moves between runs is
// the needle work.
func benchNeedleCollection(b *testing.B) *Collection {
	b.Helper()
	collection := &Collection{Options: Options{ChunkDocuments: 4}}
	if _, err := collection.Put("a", []byte(`{"v":"needle-value"}`)); err != nil {
		b.Fatal(err)
	}
	info, err := collection.CreateIndex(IndexDefinition{Name: "v", Paths: []string{"/v"}})
	if err != nil {
		b.Fatal(err)
	}
	for info.State != IndexReady {
		if info, err = collection.BackfillIndex("v", 2); err != nil {
			b.Fatal(err)
		}
	}
	return collection
}

// BenchmarkIndexRawKeysNeedle times a raw-needle lookup across needle widths.
// A wider needle costs more only in the validation, so the spread between the
// cases is what the sizing pass used to add.
func BenchmarkIndexRawKeysNeedle(b *testing.B) {
	collection := benchNeedleCollection(b)
	long := make([]byte, 0, 4096)
	long = append(long, '"')
	for range 4000 {
		long = append(long, 'x')
	}
	long = append(long, '"')
	for _, needle := range []struct {
		name string
		src  []byte
	}{
		{"short-number", []byte(`7`)},
		{"short-string", []byte(`"needle-value"`)},
		{"long-string", long},
	} {
		b.Run(needle.name, func(b *testing.B) {
			var dst []string
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				var err error
				dst, err = collection.AppendIndexRawKeys(dst[:0], "v", needle.src)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			_ = fmt.Sprint(len(dst))
		})
	}
}
