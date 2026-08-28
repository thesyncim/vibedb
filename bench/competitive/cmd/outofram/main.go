// Command outofram builds and scans a logical dataset larger than host physical
// memory without ever constructing a resident corpus. Loading is deliberately
// outside any speed claim; the row is evidence of a bounded-memory workload
// shape, with hard RSS, loader-byte, disk-space, and Linux physical-write
// guards.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"syscall"
	"time"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
	"github.com/thesyncim/vibedb/bench/competitive/cmd/internal/hostmetrics"
)

type config struct {
	engineName, durabilityName, cardinalityName, shapeName string
	corpus, exactIndexes, checkpointDocuments              int
	maxLoaderBytes, maxRSSBytes, maxPhysicalWriteBytes     int64
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "outofram:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}
	factory, ok := competitive.FactoryNamed(cfg.engineName)
	if !ok {
		return fmt.Errorf("unknown engine %q", cfg.engineName)
	}
	durability, err := competitive.ParseDurabilityMode(cfg.durabilityName)
	if err != nil {
		return err
	}
	durability, err = competitive.ResolveDurabilityMode(cfg.engineName, durability)
	if err != nil {
		return err
	}
	card, err := competitive.ParseCardinality(cfg.cardinalityName)
	if err != nil {
		return err
	}
	shape, err := competitive.ParseDocumentShape(cfg.shapeName)
	if err != nil {
		return err
	}
	if shape != competitive.OverflowHeavyDocuments {
		return errors.New("out-of-RAM evidence requires -document-shape=overflow-heavy")
	}
	physicalMemory, err := hostmetrics.PhysicalMemoryBytes()
	if err != nil {
		return err
	}
	minimumLogical := minimumOverflowHeavyLogicalBytes(cfg.corpus)
	if minimumLogical <= physicalMemory {
		return fmt.Errorf("dataset cannot exceed RAM: conservative logical lower bound %d <= physical memory %d", minimumLogical, physicalMemory)
	}
	maxRSS := cfg.maxRSSBytes
	if maxRSS == 0 {
		maxRSS = physicalMemory * 3 / 4
	}
	if maxRSS <= 0 || maxRSS >= physicalMemory {
		return fmt.Errorf("hard RSS bound %d must be positive and below physical memory %d", maxRSS, physicalMemory)
	}
	maxWrites := cfg.maxPhysicalWriteBytes
	if maxWrites == 0 {
		if minimumLogical > (1<<63-1)/8 {
			return errors.New("physical-write bound overflow")
		}
		maxWrites = minimumLogical * 8
	}

	dir, err := os.MkdirTemp("", "vibebench-outofram-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	if err := requireDiskSpace(dir, minimumLogical*2); err != nil {
		return err
	}
	engineCfg := competitive.Config{
		Dir: dir, Durability: durability, ExactIndexes: uint8(cfg.exactIndexes),
		MaxDocumentBytes: shape.MaxDocumentBytes(), CacheBytes: competitive.DefaultCacheBytes,
	}

	writeBefore, writeKnown, err := hostmetrics.LinuxPhysicalWriteBytes()
	if err != nil {
		return err
	}
	started := time.Now()
	var (
		engine                        competitive.Engine
		batch                         []competitive.Doc
		batchBytes, peakLoaderBytes   int64
		logicalBytes, loaded, sinceCP int64
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if engine == nil {
			var err error
			engine, err = factory.New(engineCfg)
			if err != nil {
				return err
			}
			if err := engine.Load(batch); err != nil {
				return err
			}
		} else {
			for i := range batch {
				if err := engine.Upsert(batch[i].Key, batch[i].JSON); err != nil {
					return err
				}
			}
		}
		loaded += int64(len(batch))
		sinceCP += int64(len(batch))
		if sinceCP >= int64(cfg.checkpointDocuments) {
			if err := engine.Checkpoint(); err != nil {
				return err
			}
			sinceCP = 0
		}
		for i := range batch {
			batch[i].JSON = nil
		}
		batch = batch[:0]
		batchBytes = 0
		rss, err := hostmetrics.MaxRSSBytes()
		if err != nil {
			return err
		}
		if rss > maxRSS {
			return fmt.Errorf("peak RSS %d exceeds hard bound %d after %d documents", rss, maxRSS, loaded)
		}
		return nil
	}
	err = competitive.GenerateCorpus(cfg.corpus, card, shape, func(doc competitive.Doc) error {
		docBytes := int64(len(doc.Key) + len(doc.JSON))
		if docBytes > cfg.maxLoaderBytes {
			return fmt.Errorf("one document requires %d loader bytes, bound is %d", docBytes, cfg.maxLoaderBytes)
		}
		if batchBytes != 0 && batchBytes+docBytes > cfg.maxLoaderBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, competitive.Doc{Key: doc.Key, JSON: append([]byte(nil), doc.JSON...)})
		batchBytes += docBytes
		logicalBytes += docBytes
		if batchBytes > peakLoaderBytes {
			peakLoaderBytes = batchBytes
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	if engine != nil {
		defer engine.Close()
	}
	if err != nil {
		return err
	}
	if logicalBytes <= physicalMemory {
		return fmt.Errorf("exact logical bytes %d did not exceed physical memory %d", logicalBytes, physicalMemory)
	}
	if err := engine.Checkpoint(); err != nil {
		return err
	}
	n, err := engine.ScanAllBytes()
	if err != nil {
		return err
	}
	if n != cfg.corpus {
		return fmt.Errorf("full-scan rows=%d want=%d", n, cfg.corpus)
	}
	if cfg.exactIndexes > 0 {
		probe, err := engine.ProbeExactIndex(0, competitive.FilterValue)
		if err != nil || !probe.IndexBounded {
			return fmt.Errorf("exact-index oracle: %+v: %w", probe, err)
		}
	}
	rss, err := hostmetrics.MaxRSSBytes()
	if err != nil {
		return err
	}
	if rss > maxRSS {
		return fmt.Errorf("peak RSS %d exceeds hard bound %d", rss, maxRSS)
	}
	writeAfter, afterKnown, err := hostmetrics.LinuxPhysicalWriteBytes()
	if err != nil {
		return err
	}
	writeKnown = writeKnown && afterKnown
	physicalWrites := int64(0)
	if writeKnown {
		physicalWrites = writeAfter - writeBefore
		if physicalWrites < 0 {
			return errors.New("physical write counter regressed")
		}
		if physicalWrites > maxWrites {
			return fmt.Errorf("physical writes %d exceed hard bound %d", physicalWrites, maxWrites)
		}
	}
	footprint, err := competitive.MeasureDiskFootprint(dir)
	if err != nil {
		return err
	}
	writeSource := "unavailable"
	if writeKnown {
		writeSource = "linux-proc-self-io-write_bytes"
	}
	fmt.Fprintln(out, "engine\tgoos\tdurability\texact-indexes\tcardinality\tdocument-shape\tdocs\tcheckpoint-documents\tcache-bytes\tstorage-profile\tlogical-bytes\tphysical-memory-bytes\tloader-peak-bytes\tmax-loader-bytes\tpeak-rss-bytes\tmax-rss-bytes\tphysical-write-known\tphysical-write-source\tphysical-write-bytes\tmax-physical-write-bytes\tapparent-bytes\tallocated-bytes\tload-and-scan-seconds")
	fmt.Fprintf(out, "%s\t%s\t%s\t%d\t%s\t%s\t%d\t%d\t%d\tintrinsic\t%d\t%d\t%d\t%d\t%d\t%d\t%t\t%s\t%d\t%d\t%d\t%d\t%.6f\n",
		cfg.engineName, runtime.GOOS, durability, cfg.exactIndexes, card, shape, cfg.corpus,
		cfg.checkpointDocuments, competitive.DefaultCacheBytes, logicalBytes,
		physicalMemory, peakLoaderBytes, cfg.maxLoaderBytes, rss, maxRSS, writeKnown, writeSource,
		physicalWrites, maxWrites, footprint.ApparentBytes, footprint.AllocatedBytes,
		time.Since(started).Seconds())
	return nil
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("outofram", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var cfg config
	fs.StringVar(&cfg.engineName, "engine", "", "engine name")
	fs.IntVar(&cfg.corpus, "corpus", 0, "document count; must yield logical bytes greater than host RAM")
	fs.StringVar(&cfg.durabilityName, "durability", "buffered-visible", "matched durability mode")
	fs.IntVar(&cfg.exactIndexes, "exact-indexes", 0, "matched exact-index count (0-3)")
	fs.StringVar(&cfg.cardinalityName, "cardinality", "low", "low or high corpus cardinality")
	fs.StringVar(&cfg.shapeName, "document-shape", "overflow-heavy", "must be overflow-heavy")
	fs.IntVar(&cfg.checkpointDocuments, "checkpoint-documents", 4096, "checkpoint after at most this many streamed documents")
	fs.Int64Var(&cfg.maxLoaderBytes, "max-loader-bytes", 8<<20, "hard retained corpus-batch byte bound")
	fs.Int64Var(&cfg.maxRSSBytes, "max-rss-bytes", 0, "hard peak-RSS bound; zero is 75% of host RAM")
	fs.Int64Var(&cfg.maxPhysicalWriteBytes, "max-physical-write-bytes", 0, "hard Linux write_bytes delta; zero is 8x conservative logical lower bound")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	if cfg.engineName == "" || cfg.corpus < 1 || cfg.exactIndexes < 0 ||
		cfg.exactIndexes > int(competitive.MaximumExactIndexes) || cfg.checkpointDocuments < 1 ||
		cfg.maxLoaderBytes < 1 || cfg.maxRSSBytes < 0 || cfg.maxPhysicalWriteBytes < 0 {
		return cfg, errors.New("engine, positive corpus/checkpoint/loader bounds, exact-indexes in [0,3], and nonnegative RSS/write bounds are required")
	}
	return cfg, nil
}

func minimumOverflowHeavyLogicalBytes(documents int) int64 {
	large := int64(documents/8*7 + max(0, documents%8-1))
	return large * (16 << 10)
}

func requireDiskSpace(path string, required int64) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return err
	}
	available := int64(stat.Bavail) * int64(stat.Bsize)
	if available < required {
		return fmt.Errorf("available disk bytes %d below conservative hard requirement %d", available, required)
	}
	return nil
}
