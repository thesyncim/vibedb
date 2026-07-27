package store

import (
	"fmt"
	"testing"
)

func BenchmarkStoreExactIndexAliasMutation(b *testing.B) {
	for _, test := range []struct {
		name     string
		physical int
		logical  int
	}{
		{name: "one_physical/one_logical", physical: 1, logical: 1},
		{name: "one_physical/eight_logical", physical: 1, logical: 8},
		{name: "eight_physical/eight_logical", physical: 8, logical: 8},
	} {
		b.Run(test.name, func(b *testing.B) {
			collection := &Collection{Options: Options{ChunkDocuments: 64}}
			doc0 := []byte(`{"v0":0,"v1":0,"v2":0,"v3":0,"v4":0,"v5":0,"v6":0,"v7":0}`)
			doc1 := []byte(`{"v0":1,"v1":1,"v2":1,"v3":1,"v4":1,"v5":1,"v6":1,"v7":1}`)
			for i := 0; i < 64; i++ {
				if _, err := collection.Put(fmt.Sprintf("k%02d", i), doc0); err != nil {
					b.Fatal(err)
				}
			}
			for i := 0; i < test.logical; i++ {
				path := i
				if test.physical == 1 {
					path = 0
				}
				name := fmt.Sprintf("i%d", i)
				if _, err := collection.CreateIndex(IndexDefinition{
					Name: name, Paths: []string{fmt.Sprintf("/v%d", path)},
				}); err != nil {
					b.Fatal(err)
				}
				if _, err := collection.BackfillIndex(name, 0); err != nil {
					b.Fatal(err)
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				doc := doc0
				if i&1 == 0 {
					doc = doc1
				}
				if created, err := collection.Put("k00", doc); err != nil || created {
					b.Fatalf("Put = (%v,%v)", created, err)
				}
			}
		})
	}
}
