package durable

import (
	"fmt"
	"testing"
)

func BenchmarkSnapshotCollectionsInto(b *testing.B) {
	for _, count := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("collections=%d", count), func(b *testing.B) {
			names := make([]string, count)
			for i := range names {
				names[i] = fmt.Sprintf("c%03d", i)
			}
			db := newTestDatabase(b, names...)
			catalog := make([]NamedCollection, count)
			for i := range names {
				collection, _ := db.Collection(names[i])
				catalog[count-1-i] = NamedCollection{
					Name: names[i], Collection: collection,
				}
			}

			var dst DatabaseSnapshot
			if err := SnapshotCollectionsInto(&dst, catalog); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := SnapshotCollectionsInto(&dst, catalog); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := dst.Close(); err != nil {
				b.Fatal(err)
			}
		})
	}
}

func BenchmarkSnapshotCollectionsAllocating(b *testing.B) {
	db := newTestDatabase(b, "a", "b", "c")
	catalog := make([]NamedCollection, 0, 3)
	for _, name := range []string{"c", "a", "b"} {
		collection, _ := db.Collection(name)
		catalog = append(catalog, NamedCollection{Name: name, Collection: collection})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		snapshot, err := SnapshotCollections(catalog)
		if err != nil {
			b.Fatal(err)
		}
		if err := snapshot.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
