// Command snapshotpressure records matched control and pinned-snapshot
// mutation phases. Engines without an explicit lease hook fail closed.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
	"github.com/thesyncim/vibedb/bench/competitive/cmd/internal/hostmetrics"
)

type config struct {
	engine, durability                              string
	corpus, operations, checkpoint, exactIndexes    int
	maxRSS, maxAllocated, maxWrites, maxOperationNS int64
	requireWrites                                   bool
}

type phaseResult struct {
	name                      string
	p999, maximum             int64
	elapsed                   time.Duration
	allocated, physicalWrites int64
	physicalKnown             bool
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "snapshotpressure:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}
	factory, ok := competitive.FactoryNamed(cfg.engine)
	if !ok {
		return errors.New("unknown engine")
	}
	mode, err := competitive.ParseDurabilityMode(cfg.durability)
	if err != nil {
		return err
	}
	mode, err = competitive.ResolveDurabilityMode(cfg.engine, mode)
	if err != nil {
		return err
	}
	dir, err := os.MkdirTemp("", "vibedb-snapshotpressure-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	docs := competitive.CorpusOfShape(cfg.corpus, competitive.LowCardinality, competitive.InlineDocuments)
	engine, err := factory.New(competitive.Config{Dir: dir, Durability: mode, ExactIndexes: uint8(cfg.exactIndexes), MaxDocumentBytes: competitive.InlineDocuments.MaxDocumentBytes(), CacheBytes: competitive.DefaultCacheBytes})
	if err != nil {
		return err
	}
	defer engine.Close()
	if err := engine.Load(docs); err != nil {
		return err
	}
	if err := engine.Checkpoint(); err != nil {
		return err
	}
	pressure, ok := engine.(competitive.SnapshotPressureEngine)
	if !ok {
		return fmt.Errorf("%s exposes no truthful pinned snapshot hook", cfg.engine)
	}
	updated := make([]bool, len(docs))
	control, err := measurePhase(cfg, engine, dir, docs, updated, "control")
	if err != nil {
		return err
	}
	lease, err := pressure.PinSnapshot()
	if err != nil {
		return err
	}
	pinned, phaseErr := measurePhase(cfg, engine, dir, docs, updated, "pinned")
	closeErr := lease.Close()
	if phaseErr != nil {
		return phaseErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := engine.Checkpoint(); err != nil {
		return err
	}
	for i, doc := range docs {
		want := doc.JSON
		if updated[i] {
			want = competitive.SameSizeUpdatedJSON(docs, i)
		}
		want, err = competitive.AppendExpectedStoredJSON(nil, cfg.engine, want)
		if err != nil {
			return err
		}
		got, err := engine.Get(nil, doc.Key)
		if err != nil || !bytes.Equal(got, want) {
			return fmt.Errorf("final oracle %d: %v", i, err)
		}
	}
	rss, err := hostmetrics.MaxRSSBytes()
	if err != nil {
		return err
	}
	if cfg.maxRSS > 0 && rss > cfg.maxRSS {
		return fmt.Errorf("peak RSS %d exceeds %d", rss, cfg.maxRSS)
	}
	fmt.Fprintln(out, "engine\tphase\tdurability\texact-indexes\tdocument-shape\tdocs\toperations\tcheckpoint-mutations\tp99.9-us\tmax-us\ttotal-ops/s\tallocated-bytes\tmax-allocated-bytes\tphysical-write-known\tphysical-write-source\tphysical-write-bytes\tmax-physical-write-bytes\tpeak-rss-bytes\tmax-rss-bytes\tmax-operation-ns")
	for _, result := range []phaseResult{control, pinned} {
		source := "unavailable"
		if result.physicalKnown {
			source = "linux-proc-self-io-write_bytes"
		}
		fmt.Fprintf(out, "%s\t%s\t%s\t%d\tinline\t%d\t%d\t%d\t%.3f\t%.3f\t%.0f\t%d\t%d\t%t\t%s\t%d\t%d\t%d\t%d\t%d\n",
			cfg.engine, result.name, mode, cfg.exactIndexes, cfg.corpus, cfg.operations, cfg.checkpoint,
			float64(result.p999)/1000, float64(result.maximum)/1000, float64(cfg.operations)/result.elapsed.Seconds(),
			result.allocated, cfg.maxAllocated, result.physicalKnown, source, result.physicalWrites, cfg.maxWrites,
			rss, cfg.maxRSS, cfg.maxOperationNS)
	}
	return nil
}

func measurePhase(cfg config, engine competitive.Engine, dir string, docs []competitive.Doc, updated []bool, name string) (phaseResult, error) {
	before, known, err := hostmetrics.LinuxPhysicalWriteBytes()
	if err != nil {
		return phaseResult{}, err
	}
	if cfg.requireWrites && !known {
		return phaseResult{}, errors.New("Linux process write_bytes is required")
	}
	latencies := make([]int64, cfg.operations)
	started := time.Now()
	for operation := range cfg.operations {
		i := (operation*7919 + 17) % len(docs)
		value := docs[i].JSON
		if !updated[i] {
			value = competitive.SameSizeUpdatedJSON(docs, i)
		}
		begin := time.Now()
		if err := engine.Put(docs[i].Key, value); err != nil {
			return phaseResult{}, err
		}
		updated[i] = !updated[i]
		if (operation+1)%cfg.checkpoint == 0 {
			if err := engine.Checkpoint(); err != nil {
				return phaseResult{}, err
			}
		}
		latencies[operation] = time.Since(begin).Nanoseconds()
		if latencies[operation] > cfg.maxOperationNS {
			return phaseResult{}, fmt.Errorf("%s operation latency %d exceeds %d", name, latencies[operation], cfg.maxOperationNS)
		}
	}
	if cfg.operations%cfg.checkpoint != 0 {
		if err := engine.Checkpoint(); err != nil {
			return phaseResult{}, err
		}
	}
	elapsed := time.Since(started)
	after, afterKnown, err := hostmetrics.LinuxPhysicalWriteBytes()
	if err != nil {
		return phaseResult{}, err
	}
	known = known && afterKnown
	writes := int64(0)
	if known {
		writes = after - before
		if writes < 0 {
			return phaseResult{}, errors.New("process write counter regressed")
		}
		if cfg.requireWrites && writes == 0 {
			return phaseResult{}, errors.New("process write counter stayed zero")
		}
		if cfg.maxWrites > 0 && writes > cfg.maxWrites {
			return phaseResult{}, fmt.Errorf("writes %d exceed %d", writes, cfg.maxWrites)
		}
	} else if cfg.requireWrites {
		return phaseResult{}, errors.New("process write counter became unavailable")
	}
	footprint, err := competitive.MeasureDiskFootprint(dir)
	if err != nil {
		return phaseResult{}, err
	}
	if cfg.maxAllocated > 0 && footprint.AllocatedBytes > cfg.maxAllocated {
		return phaseResult{}, fmt.Errorf("allocated %d exceeds %d", footprint.AllocatedBytes, cfg.maxAllocated)
	}
	slices.Sort(latencies)
	return phaseResult{name: name, p999: percentile(latencies, 999, 1000), maximum: latencies[len(latencies)-1], elapsed: elapsed, allocated: footprint.AllocatedBytes, physicalKnown: known, physicalWrites: writes}, nil
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("snapshotpressure", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var cfg config
	fs.StringVar(&cfg.engine, "engine", "vibedb", "engine with explicit snapshot hook")
	fs.StringVar(&cfg.durability, "durability", "buffered-visible", "matched durability")
	fs.IntVar(&cfg.corpus, "corpus", 10000, "documents")
	fs.IntVar(&cfg.operations, "operations", 20000, "operations in each phase")
	fs.IntVar(&cfg.checkpoint, "checkpoint-mutations", 64, "checkpoint cadence")
	fs.IntVar(&cfg.exactIndexes, "exact-indexes", 0, "exact indexes")
	fs.Int64Var(&cfg.maxRSS, "max-rss-bytes", 2<<30, "hard RSS bound")
	fs.Int64Var(&cfg.maxAllocated, "max-allocated-bytes", 8<<30, "hard allocated bound")
	fs.Int64Var(&cfg.maxWrites, "max-physical-write-bytes", 16<<30, "hard Linux process-write bound")
	fs.Int64Var(&cfg.maxOperationNS, "max-operation-ns", 30_000_000_000, "hard per-operation latency bound")
	fs.BoolVar(&cfg.requireWrites, "require-physical-write", false, "require Linux process write counter")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 || cfg.corpus < 1 || cfg.operations < 1 || cfg.checkpoint < 1 || cfg.exactIndexes < 0 || cfg.exactIndexes > int(competitive.MaximumExactIndexes) || cfg.maxRSS < 0 || cfg.maxAllocated < 0 || cfg.maxWrites < 0 || cfg.maxOperationNS < 1 {
		return cfg, errors.New("invalid snapshot pressure configuration")
	}
	return cfg, nil
}

func percentile(sorted []int64, numerator, denominator int) int64 {
	index := (len(sorted)*numerator+denominator-1)/denominator - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}
