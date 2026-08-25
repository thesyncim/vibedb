// Command speedprobe runs the read-side VibeDB lanes from the competitive
// harness without rebuilding the full testing package. It is used alongside
// ClickHouse's own client/server benchmark so both engines can be measured on
// the same machine and corpus.
package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
)

var (
	corpusSize = flag.Int("corpus", competitive.CorpusSize, "documents in the corpus")
	card       = flag.String("cardinality", "low", "corpus variant: low or high")
)

func main() {
	flag.Parse()
	cardinality, err := competitive.ParseCardinality(*card)
	if err != nil {
		fatalf("cardinality: %v", err)
	}
	docs := competitive.CorpusOf(*corpusSize, cardinality)
	probe := rand.New(rand.NewPCG(7, 11)).Perm(len(docs))

	factory, ok := competitive.FactoryNamed("vibedb")
	if !ok {
		fatalf("vibedb factory missing")
	}

	for _, indexed := range []bool{false, true} {
		exactIndexes := uint8(0)
		if indexed {
			exactIndexes = 1
		}
		dir, err := os.MkdirTemp("", "vibedb-speed-")
		if err != nil {
			fatalf("temp dir: %v", err)
		}
		e, err := factory.New(competitive.Config{
			Dir:          dir,
			ExactIndexes: exactIndexes,
			CacheBytes:   competitive.DefaultCacheBytes,
		})
		if err != nil {
			fatalf("create indexed=%v: %v", indexed, err)
		}
		if err := e.Load(docs); err != nil {
			_ = e.Close()
			_ = os.RemoveAll(dir)
			fatalf("load indexed=%v: %v", indexed, err)
		}

		fmt.Printf("vibedb cardinality=%s corpus=%d indexed=%v\n", cardinality, len(docs), indexed)
		printBench("point_get", testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			buf := make([]byte, 0, 512)
			for i := 0; i < b.N; i++ {
				out, err := e.Get(buf[:0], docs[probe[i%len(probe)]].Key)
				if err != nil || len(out) == 0 {
					b.Fatalf("get: %v", err)
				}
				buf = out
			}
		}))
		printBench("scan_all_bytes", testing.Benchmark(func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				n, err := e.ScanAllBytes()
				if err != nil || n != len(docs) {
					b.Fatalf("scan: n=%d err=%v", n, err)
				}
			}
		}))
		if indexed {
			printBench("indexed_count", testing.Benchmark(func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					n, err := e.IndexedCount(competitive.FilterValue)
					if err != nil || n == 0 {
						b.Fatalf("indexed count: n=%d err=%v", n, err)
					}
				}
			}))
		} else {
			printBench("filter_count", testing.Benchmark(func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					n, err := e.FilterCount(competitive.FilterValue)
					if err != nil || n == 0 {
						b.Fatalf("filter count: n=%d err=%v", n, err)
					}
				}
			}))
		}
		if err := e.Close(); err != nil {
			fatalf("close indexed=%v: %v", indexed, err)
		}
		if err := os.RemoveAll(dir); err != nil {
			fatalf("remove temp dir: %v", err)
		}
	}
}

func printBench(name string, result testing.BenchmarkResult) {
	fmt.Printf("%s ns/op=%d allocs/op=%d bytes/op=%d\n", name, result.NsPerOp(), result.AllocsPerOp(), result.AllocedBytesPerOp())
}

func fatalf(format string, args ...any) {
	fmt.Printf("speedprobe error: "+format+"\n", args...)

	os.Exit(1)
}
