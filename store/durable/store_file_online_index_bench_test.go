package durable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

// BenchmarkOnlineCreateIndex measures only the live leaf scan, canonical
// merge, and atomic publication. Corpus construction and restoration of the
// byte-identical unindexed base file stay outside the timer.
func BenchmarkOnlineCreateIndex(b *testing.B) {
	const documents = 8_000
	for _, cardinality := range []int{20, 1_000} {
		b.Run(fmt.Sprintf("docs=%d/values=%d", documents, cardinality),
			func(b *testing.B) {
				builder, err := store.NewBuilder(store.Options{})
				if err != nil {
					b.Fatal(err)
				}
				for row := range documents {
					if err := builder.Append(
						fmt.Sprintf("k%05d", row),
						fmt.Appendf(
							nil,
							`{"id":%d,"group":"g%04d","active":%t}`,
							row, row%cardinality, row&1 == 0,
						),
					); err != nil {
						b.Fatal(err)
					}
				}
				source, err := builder.Build()
				if err != nil {
					b.Fatal(err)
				}
				options := Options{
					Backend: BackendPortable, ResidentBytes: 64 << 20,
					Durability:         DurabilityBufferedVisible,
					CheckpointStrength: CheckpointFilesystem,
					MaxDocumentBytes:   1 << 10,
				}
				basePath := filepath.Join(b.TempDir(), "base.vibe")
				baseFile, err := os.OpenFile(
					basePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600,
				)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := CreateFromPrimary(
					source, baseFile, options,
				); err != nil {
					_ = baseFile.Close()
					b.Fatal(err)
				}
				if err := baseFile.Close(); err != nil {
					b.Fatal(err)
				}
				base, err := os.ReadFile(basePath)
				if err != nil {
					b.Fatal(err)
				}
				runPath := filepath.Join(b.TempDir(), "run.vibe")
				var indexBytes, growthBytes int64
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					b.StopTimer()
					file, err := os.OpenFile(
						runPath,
						os.O_CREATE|os.O_RDWR|os.O_TRUNC,
						0o600,
					)
					if err != nil {
						b.Fatal(err)
					}
					if _, err := file.WriteAt(base, 0); err != nil {
						_ = file.Close()
						b.Fatal(err)
					}
					collection, err := Open(file, options)
					if err != nil {
						_ = file.Close()
						b.Fatal(err)
					}
					b.StartTimer()
					_, buildErr := collection.CreateIndex(
						store.IndexDefinition{
							Name: "by_group", Paths: []string{"/group"},
						},
					)
					b.StopTimer()
					if buildErr != nil {
						_ = collection.Close()
						_ = file.Close()
						b.Fatal(buildErr)
					}
					indexBytes = int64(
						len(collection.primaryEpoch.exact[0].encoded),
					)
					if err := collection.Close(); err != nil {
						_ = file.Close()
						b.Fatal(err)
					}
					info, err := file.Stat()
					if err != nil {
						_ = file.Close()
						b.Fatal(err)
					}
					growthBytes = info.Size() - int64(len(base))
					if err := file.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(
					float64(documents)*float64(b.N)/b.Elapsed().Seconds(),
					"docs/s",
				)
				b.ReportMetric(
					float64(indexBytes)/documents, "index-B/doc",
				)
				b.ReportMetric(
					float64(growthBytes)/documents, "growth-B/doc",
				)
			})
	}
}
