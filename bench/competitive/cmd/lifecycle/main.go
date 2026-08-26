// Command lifecycle records isolated open and crash-recovery wall time. The
// parent prepares one image; every measured open occurs in a fresh child
// process, with the timer wholly inside Factory.Open.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
	"github.com/thesyncim/vibedb/bench/competitive/cmd/internal/hostmetrics"
)

type config struct {
	engineName, mode, durabilityName, cardinalityName, shapeName string
	corpus, exactIndexes                                         int
	maxRSSBytes, maxPhysicalWriteBytes                           int64
	dir, child                                                   string
}

func main() {
	cfg, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "lifecycle:", err)
		os.Exit(2)
	}
	if cfg.child == "crash" {
		if err := crashWrite(cfg); err != nil {
			fmt.Fprintln(os.Stderr, "lifecycle crash child:", err)
			os.Exit(1)
		}
		// Deliberately bypass adapter Close: this is the crash-image producer.
		os.Exit(0)
	}
	if err := run(cfg, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "lifecycle:", err)
		os.Exit(1)
	}
}

func parseFlags(args []string) (config, error) {
	fs := flag.NewFlagSet("lifecycle", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var cfg config
	fs.StringVar(&cfg.engineName, "engine", "", "engine name")
	fs.StringVar(&cfg.mode, "mode", "hot", "open, hot, cold, or recovery")
	fs.IntVar(&cfg.corpus, "corpus", 10_000, "documents in the populated image")
	fs.StringVar(&cfg.durabilityName, "durability", "ordinary-sync", "matched durability mode")
	fs.IntVar(&cfg.exactIndexes, "exact-indexes", 0, "matched exact-index count (0-3)")
	fs.StringVar(&cfg.cardinalityName, "cardinality", "low", "low or high corpus cardinality")
	fs.StringVar(&cfg.shapeName, "document-shape", "inline", "inline, mixed, or overflow-heavy")
	fs.Int64Var(&cfg.maxRSSBytes, "max-rss-bytes", 0, "hard child peak-RSS bound; zero uses host physical memory")
	fs.Int64Var(&cfg.maxPhysicalWriteBytes, "max-physical-write-bytes", 1<<30, "hard Linux /proc/self/io write_bytes delta bound")
	fs.StringVar(&cfg.dir, "internal-dir", "", "internal prepared image directory")
	fs.StringVar(&cfg.child, "internal-child", "", "internal child operation")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	if fs.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	if cfg.engineName == "" || cfg.corpus < 1 || cfg.exactIndexes < 0 ||
		cfg.exactIndexes > int(competitive.MaximumExactIndexes) || cfg.maxRSSBytes < 0 ||
		cfg.maxPhysicalWriteBytes < 0 {
		return cfg, errors.New("-engine, -corpus>=1, -exact-indexes in [0,3], and nonnegative bounds are required")
	}
	switch cfg.mode {
	case "open", "hot", "cold", "recovery":
	default:
		return cfg, fmt.Errorf("unknown -mode %q", cfg.mode)
	}
	if cfg.child != "" && cfg.child != "timed" && cfg.child != "warm" && cfg.child != "crash" {
		return cfg, fmt.Errorf("invalid internal child %q", cfg.child)
	}
	if cfg.child != "" && cfg.dir == "" {
		return cfg, errors.New("internal child requires -internal-dir")
	}
	return cfg, nil
}

func resolved(cfg config) (competitive.Factory, competitive.Config, competitive.Cardinality, competitive.DocumentShape, error) {
	factory, ok := competitive.FactoryNamed(cfg.engineName)
	if !ok {
		return competitive.Factory{}, competitive.Config{}, 0, 0, fmt.Errorf("unknown engine %q", cfg.engineName)
	}
	durability, err := competitive.ParseDurabilityMode(cfg.durabilityName)
	if err != nil {
		return competitive.Factory{}, competitive.Config{}, 0, 0, err
	}
	durability, err = competitive.ResolveDurabilityMode(cfg.engineName, durability)
	if err != nil {
		return competitive.Factory{}, competitive.Config{}, 0, 0, err
	}
	card, err := competitive.ParseCardinality(cfg.cardinalityName)
	if err != nil {
		return competitive.Factory{}, competitive.Config{}, 0, 0, err
	}
	shape, err := competitive.ParseDocumentShape(cfg.shapeName)
	if err != nil {
		return competitive.Factory{}, competitive.Config{}, 0, 0, err
	}
	engineCfg := competitive.Config{
		Dir: cfg.dir, Durability: durability, ExactIndexes: uint8(cfg.exactIndexes),
		MaxDocumentBytes: shape.MaxDocumentBytes(), CacheBytes: competitive.DefaultCacheBytes,
	}
	return factory, engineCfg, card, shape, nil
}

func run(cfg config, out io.Writer) error {
	if cfg.child == "timed" {
		return timedOpen(cfg, out)
	}
	if cfg.child == "warm" {
		return warmImage(cfg)
	}
	return parentRun(cfg, out)
}

func parentRun(cfg config, out io.Writer) error {
	dir, err := os.MkdirTemp("", "vibebench-lifecycle-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	cfg.dir = dir
	factory, engineCfg, card, shape, err := resolved(cfg)
	if err != nil {
		return err
	}
	docs := competitive.CorpusOfShape(cfg.corpus, card, shape)
	engine, err := factory.New(engineCfg)
	if err != nil {
		return err
	}
	if err := engine.Load(docs); err != nil {
		_ = engine.Close()
		return err
	}
	if err := engine.Checkpoint(); err != nil {
		_ = engine.Close()
		return err
	}
	if err := engine.Close(); err != nil {
		return err
	}
	docs = nil

	cacheControl := "uncontrolled"
	switch cfg.mode {
	case "hot":
		if _, err := child(cfg, "warm"); err != nil {
			return fmt.Errorf("hot-cache conditioning: %w", err)
		}
		cacheControl = "full-scan-close"
	case "cold":
		cacheControl, err = hostmetrics.DropFileCaches()
		if err != nil {
			return err
		}
	case "recovery":
		if cfg.durabilityName == competitive.DurabilityBufferedVisible.String() {
			return errors.New("recovery mode rejects buffered-visible: the crash mutation must be acknowledged stable")
		}
		if _, err := child(cfg, "crash"); err != nil {
			return fmt.Errorf("crash-image producer: %w", err)
		}
		cacheControl = "crash-child-exit"
	}

	row, err := child(cfg, "timed")
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "engine\tphase\tcache-control\tgoos\tdurability\texact-indexes\tcardinality\tdocument-shape\tdocs\tcache-bytes\tstorage-profile\topen-ns\tpeak-rss-bytes\tmax-rss-bytes\tphysical-write-known\tphysical-write-source\tphysical-write-bytes\tmax-physical-write-bytes")
	fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%d\t%d\tintrinsic\t%s\n",
		cfg.engineName, cfg.mode, cacheControl, runtime.GOOS, cfg.durabilityName, cfg.exactIndexes,
		card, shape, cfg.corpus, competitive.DefaultCacheBytes, strings.TrimSpace(string(row)))
	return nil
}

func child(cfg config, operation string) ([]byte, error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	args := []string{
		"-engine=" + cfg.engineName, "-mode=" + cfg.mode, "-corpus=" + strconv.Itoa(cfg.corpus),
		"-durability=" + cfg.durabilityName, "-exact-indexes=" + strconv.Itoa(cfg.exactIndexes),
		"-cardinality=" + cfg.cardinalityName, "-document-shape=" + cfg.shapeName,
		"-max-rss-bytes=" + strconv.FormatInt(cfg.maxRSSBytes, 10),
		"-max-physical-write-bytes=" + strconv.FormatInt(cfg.maxPhysicalWriteBytes, 10),
		"-internal-dir=" + cfg.dir, "-internal-child=" + operation,
	}
	command := exec.Command(executable, args...)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	result, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("%s: %w: %s", operation, err, strings.TrimSpace(stderr.String()))
	}
	return result, nil
}

func warmImage(cfg config) error {
	factory, engineCfg, _, _, err := resolved(cfg)
	if err != nil {
		return err
	}
	engine, err := factory.Open(engineCfg)
	if err != nil {
		return err
	}
	n, scanErr := engine.ScanAllBytes()
	closeErr := engine.Close()
	if scanErr != nil {
		return scanErr
	}
	if n != cfg.corpus {
		return fmt.Errorf("warm scan rows=%d want=%d", n, cfg.corpus)
	}
	return closeErr
}

func timedOpen(cfg config, out io.Writer) error {
	factory, engineCfg, card, shape, err := resolved(cfg)
	if err != nil {
		return err
	}
	before, writeKnown, err := hostmetrics.LinuxPhysicalWriteBytes()
	if err != nil {
		return err
	}
	start := time.Now()
	engine, err := factory.Open(engineCfg)
	elapsed := time.Since(start)
	if err != nil {
		return err
	}
	defer engine.Close()
	after, afterKnown, err := hostmetrics.LinuxPhysicalWriteBytes()
	if err != nil {
		return err
	}
	writeKnown = writeKnown && afterKnown
	physicalWrites := int64(0)
	if writeKnown {
		physicalWrites = after - before
		if physicalWrites < 0 {
			return errors.New("physical write counter regressed")
		}
		if physicalWrites > cfg.maxPhysicalWriteBytes {
			return fmt.Errorf("open physical writes %d exceed hard bound %d", physicalWrites, cfg.maxPhysicalWriteBytes)
		}
	}
	rss, err := hostmetrics.MaxRSSBytes()
	if err != nil {
		return err
	}
	maxRSS := cfg.maxRSSBytes
	if maxRSS == 0 {
		maxRSS, err = hostmetrics.PhysicalMemoryBytes()
		if err != nil {
			return err
		}
	}
	if rss > maxRSS {
		return fmt.Errorf("peak RSS %d exceeds hard bound %d", rss, maxRSS)
	}
	n, err := engine.ScanAllBytes()
	if err != nil {
		return err
	}
	if n != cfg.corpus {
		return fmt.Errorf("post-open scan rows=%d want=%d", n, cfg.corpus)
	}
	if cfg.exactIndexes > 0 {
		probe, err := engine.ProbeExactIndex(0, competitive.FilterValue)
		if err != nil || !probe.IndexBounded {
			return fmt.Errorf("post-open exact-index oracle: %+v: %w", probe, err)
		}
	}
	if cfg.mode == "recovery" {
		doc := competitive.CorpusOfShape(1, card, shape)
		want, err := competitive.AppendExpectedStoredJSON(nil, cfg.engineName, competitive.SameSizeUpdatedJSON(doc, 0))
		if err != nil {
			return err
		}
		got, err := engine.Get(nil, doc[0].Key)
		if err != nil || !bytes.Equal(got, want) {
			return fmt.Errorf("recovery oracle mismatch: bytes=%d want=%d err=%v", len(got), len(want), err)
		}
	}
	rss, err = hostmetrics.MaxRSSBytes()
	if err != nil {
		return err
	}
	if rss > maxRSS {
		return fmt.Errorf("post-oracle peak RSS %d exceeds hard bound %d", rss, maxRSS)
	}
	writeSource := "unavailable"
	if writeKnown {
		writeSource = "linux-proc-self-io-write_bytes"
	}
	fmt.Fprintf(out, "%d\t%d\t%d\t%t\t%s\t%d\t%d", elapsed.Nanoseconds(), rss, maxRSS, writeKnown, writeSource, physicalWrites, cfg.maxPhysicalWriteBytes)
	return nil
}

func crashWrite(cfg config) error {
	factory, engineCfg, card, shape, err := resolved(cfg)
	if err != nil {
		return err
	}
	engine, err := factory.Open(engineCfg)
	if err != nil {
		return err
	}
	doc := competitive.CorpusOfShape(1, card, shape)
	return engine.Put(doc[0].Key, competitive.SameSizeUpdatedJSON(doc, 0))
}
