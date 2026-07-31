// Command footprint loads the shared corpus into one engine and reports its
// bytes on disk and its steady-state memory.
//
// It is a separate process on purpose. Go-heap residency and process RSS are
// process-global, so measuring six engines inside one benchmark binary would
// report the sum of all of them plus whatever the benchmark harness retained.
// One engine per process is the only way these numbers mean anything. Run
// them from the shell loop in the README.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
)

func main() {
	engine := flag.String("engine", "", "engine name (see -list)")
	list := flag.Bool("list", false, "list engine names and exit")
	corpus := flag.Int("corpus", competitive.CorpusSize, "documents in the shared corpus")
	indexed := flag.Bool("indexed", false, "declare a secondary index over the filter field")
	putloop := flag.Bool("putloop", false, "store/durable only: build by replaying Put instead of the bulk path")
	durabilityName := flag.String(
		"durability", "buffered-visible",
		"buffered-visible, async-stable-in-flight, ordinary-sync, or power-safe",
	)
	storageProfileName := flag.String(
		"storage-profile", "intrinsic",
		"disk comparison profile: intrinsic (optional compression off) or production (recommended built-in compression)",
	)
	card := flag.String("cardinality", "low", "corpus variant: low (the shipped, ~92% redundant one) or high. "+
		"The two are shape- and length-identical and differ only in value entropy, isolating each format's "+
		"sensitivity to entropy at fixed shape and length.")
	header := flag.Bool("header", false, "print a column header first")
	corpusStats := flag.Bool("corpus-stats", false, "print the corpus's size and gzip -9 size, then exit")
	files := flag.Bool("files", false, "additionally list every file the engine left behind, with its apparent "+
		"size and its allocated blocks. This is how to see which file is sparse and by how much, which is the "+
		"difference between reporting Badger at 257 MiB and reporting it at 26.6 MiB.")
	flag.Parse()

	if *list {
		for _, f := range competitive.Factories() {
			fmt.Println(f.Name)
		}
		return
	}
	cardinality, err := competitive.ParseCardinality(*card)
	check(err)
	durability, err := competitive.ParseDurabilityMode(*durabilityName)
	check(err)
	storageProfile, err := competitive.ParseStorageProfile(*storageProfileName)
	check(err)

	if *corpusStats {
		// The number that has to sit beside every disk column: how much of this
		// corpus is redundancy that a dictionary-based writer can collapse and a
		// key/value store cannot.
		d := competitive.CorpusOf(*corpus, cardinality)
		total, gz, err := competitive.CorpusRedundancy(d)
		check(err)
		fmt.Printf("cardinality=%s docs=%d raw=%.2f MiB gzip-9=%.2f MiB (%.1f%% of raw)\n",
			cardinality, len(d), float64(total)/(1<<20), float64(gz)/(1<<20),
			100*float64(gz)/float64(total))
		return
	}

	if *header {
		printHeader(os.Stdout)
	}
	if *engine == "baseline" {
		// The harness's own cost with no engine at all: the corpus is built and
		// dropped exactly as it is for a real engine. Every row below has to be
		// read against this one.
		_ = competitive.CorpusOf(*corpus, cardinality)
		fp, err := competitive.Measure(nil, "")
		check(err)
		report(os.Stdout, "baseline", "-", cardinality, false, fp, competitive.StorageProfileResolution{
			Profile:     storageProfile,
			Compression: "n/a",
			Provenance:  "harness-baseline:no-engine",
		})
		return
	}
	if *engine == "" {
		fmt.Fprintln(os.Stderr, "footprint: -engine is required")
		os.Exit(2)
	}
	factory, ok := competitive.FactoryNamed(*engine)
	if !ok {
		fmt.Fprintf(os.Stderr, "footprint: unknown engine %q\n", *engine)
		os.Exit(2)
	}
	profile, err := competitive.ResolveStorageProfile(factory.Name, storageProfile)
	check(err)

	docs := competitive.CorpusOf(*corpus, cardinality)
	dir, err := os.MkdirTemp("", "vibebench-footprint-")
	check(err)
	defer os.RemoveAll(dir)

	e, err := factory.New(competitive.Config{
		Dir:            dir,
		Durability:     durability,
		Indexed:        *indexed,
		CacheBytes:     competitive.DefaultCacheBytes,
		PutLoop:        *putloop,
		StorageProfile: storageProfile,
	})
	check(err)

	check(e.Load(docs))

	// Drop the corpus before measuring. It is the harness's memory, not the
	// engine's, and holding ~25 MiB of it would be charged to whichever engine
	// happened to keep the least of its own.
	docs = nil

	fp, err := competitive.Measure(e, dir)
	check(err)

	name := factory.Name
	if *putloop {
		name += "/put"
	} else if factory.Name == "vibejson-durable" {
		name += "/bulk-unified"
	}
	report(os.Stdout, name, e.DurabilityMode().String(), cardinality, *indexed, fp, profile)

	if *files {
		entries, err := competitive.DirFileSizes(dir)
		check(err)
		for _, f := range entries {
			ratio := ""
			if f.AllocatedBytes > 0 && f.ApparentBytes > f.AllocatedBytes {
				ratio = fmt.Sprintf("  sparse %.1fx", float64(f.ApparentBytes)/float64(f.AllocatedBytes))
			}
			fmt.Printf("    %-40s %12s %12s%s\n", f.Path, mib(f.ApparentBytes), mib(f.AllocatedBytes), ratio)
		}
	}

	check(e.Close())
}

func printHeader(w io.Writer) {
	fmt.Fprintf(w, "%-40s %-12s %-24s %-24s %8s %8s %12s %12s %12s %12s %12s %12s %-12s %-20s %s\n",
		"git-commit", "vcs-modified", "engine", "durability", "corpus", "indexed", "disk", "diskalloc",
		"heapalloc", "heapsys", "resident", "maxrss", "storage-profile",
		"compression", "compression-provenance")
}

func report(
	w io.Writer,
	name, durability string,
	card competitive.Cardinality,
	indexed bool,
	fp competitive.Footprint,
	profile competitive.StorageProfileResolution,
) {
	commit, modified := vcsProvenance()
	fmt.Fprintf(w, "%-40s %-12s %-24s %-24s %8s %8v %12s %12s %12s %12s %12s %12s %-12s %-20s %s\n",
		commit, modified, name, durability, card, indexed,
		mib(fp.DiskBytes), mib(fp.DiskAllocatedBytes),
		mib(int64(fp.HeapAlloc)), mib(int64(fp.HeapSys)),
		mib(int64(fp.RuntimeResident)), mib(fp.MaxRSSBytes()),
		profile.Profile, profile.Compression, profile.Provenance)
}

func vcsProvenance() (revision, modified string) {
	revision, modified = "unknown", "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if setting.Value != "" {
					revision = setting.Value
				}
			case "vcs.modified":
				if setting.Value != "" {
					modified = setting.Value
				}
			}
		}
	}
	if revision == "unknown" {
		if out, err := exec.Command("git", "rev-parse", "HEAD").Output(); err == nil {
			revision = strings.TrimSpace(string(out))
		}
	}
	if modified == "unknown" {
		if out, err := exec.Command(
			"git", "status", "--porcelain", "--untracked-files=no",
		).Output(); err == nil {
			modified = fmt.Sprintf("%t", strings.TrimSpace(string(out)) != "")
		}
	}
	return revision, modified
}

func mib(n int64) string { return fmt.Sprintf("%.1f", float64(n)/(1<<20)) }

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "footprint:", err)
		os.Exit(1)
	}
}
