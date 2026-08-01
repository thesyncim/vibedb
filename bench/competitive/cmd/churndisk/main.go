// Command churndisk measures live disk bytes during sustained fixed-live-set
// mutation churn. It runs exactly one engine per process.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"runtime/debug"
	"strings"
	"time"

	competitive "github.com/thesyncim/vibedb/bench/competitive"
)

const defaultSeed int64 = 0xC11D15C

type config struct {
	engineName          string
	corpusSize          int
	mutationBudget      int
	replacePercent      int
	sampleMutations     int
	checkpointMutations int
	cardinalityName     string
	storageProfileName  string
	seed                int64
	allowDiagnostic     bool
}

type sample struct {
	phase          string
	mutationIndex  int
	apparentBytes  int64
	allocatedBytes int64
	elapsed        time.Duration
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "churndisk: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("churndisk", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	cfg := config{}
	fs.StringVar(&cfg.engineName, "engine", "", "disk-backed engine name")
	fs.IntVar(&cfg.corpusSize, "corpus", competitive.CorpusSize, "documents in the shared corpus")
	fs.IntVar(&cfg.mutationBudget, "mutations", 200_000, "acknowledged state-change budget")
	fs.IntVar(&cfg.replacePercent, "replace-percent", 80, "percentage of churn choices that replace a uniformly random key")
	fs.IntVar(&cfg.sampleMutations, "sample-mutations", 5_000, "sample disk bytes after this many additional mutations")
	fs.IntVar(&cfg.checkpointMutations, "checkpoint-mutations", 64, "checkpoint cadence in acknowledged mutations; zero means final only")
	fs.StringVar(&cfg.cardinalityName, "cardinality", "low", "low or high corpus cardinality")
	fs.StringVar(
		&cfg.storageProfileName, "storage-profile", "intrinsic",
		"disk comparison profile: intrinsic (optional compression off) or production (recommended built-in compression)",
	)
	fs.Int64Var(&cfg.seed, "seed", defaultSeed, "deterministic churn seed")
	fs.BoolVar(
		&cfg.allowDiagnostic, "allow-diagnostic", false,
		"allow a nonstandard or forced-checkpoint run and mark every row non-publishable",
	)
	list := fs.Bool("list", false, "list eligible engines")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " "))
	}
	if *list {
		fmt.Fprintln(out, "vibedb bbolt badger pebble sqlite")
		return nil
	}
	if cfg.engineName == "" || cfg.corpusSize < 1 || cfg.mutationBudget < 1 ||
		cfg.sampleMutations < 1 || cfg.checkpointMutations < 0 ||
		cfg.replacePercent < 0 || cfg.replacePercent > 100 {
		return fmt.Errorf("-engine, -corpus>=1, -mutations>=1, -sample-mutations>=1, -checkpoint-mutations>=0, and -replace-percent in [0,100] are required")
	}
	if !publicationShape(cfg) && !cfg.allowDiagnostic {
		return fmt.Errorf(
			"nonstandard churn shape is diagnostic; use -allow-diagnostic "+
				"(publishable requires corpus=%d mutations=200000 replace-percent=80 "+
				"sample-mutations=5000 checkpoint-mutations=64 seed=%d)",
			competitive.CorpusSize, defaultSeed,
		)
	}
	factory, ok := competitive.FactoryNamed(cfg.engineName)
	if !ok {
		return fmt.Errorf("unknown engine %q", cfg.engineName)
	}
	cardinality, err := competitive.ParseCardinality(cfg.cardinalityName)
	if err != nil {
		return err
	}
	storageProfile, err := competitive.ParseStorageProfile(cfg.storageProfileName)
	if err != nil {
		return err
	}
	profile, err := competitive.ResolveStorageProfile(cfg.engineName, storageProfile)
	if err != nil {
		return err
	}
	durability, err := competitive.ResolveDurabilityMode(
		cfg.engineName, competitive.DurabilityBufferedVisible,
	)
	if err != nil {
		return err
	}

	docs := competitive.CorpusOf(cfg.corpusSize, cardinality)
	dir, err := os.MkdirTemp("", "vibebench-churndisk-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	engine, err := factory.New(competitive.Config{
		Dir:            dir,
		Durability:     durability,
		CacheBytes:     competitive.DefaultCacheBytes,
		StorageProfile: storageProfile,
	})
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
	floor, ok := engine.(competitive.MaintenanceFloorer)
	if !ok {
		return fmt.Errorf("%s does not implement the maintenance-floor hook", cfg.engineName)
	}

	forcedStart := automaticCheckpointCount(engine)
	rng := rand.New(rand.NewSource(cfg.seed))
	updated := make([]bool, len(docs))
	var replacement []byte
	checkpoints := checkpointSchedule{every: cfg.checkpointMutations}
	samples := make([]sample, 0, cfg.mutationBudget/cfg.sampleMutations+2)
	start := time.Now()
	nextSample := cfg.sampleMutations
	mutations := 0

	takeSample := func(phase string) error {
		fp, err := competitive.MeasureDiskFootprint(dir)
		if err != nil {
			return err
		}
		samples = append(samples, sample{
			phase:          phase,
			mutationIndex:  mutations,
			apparentBytes:  fp.ApparentBytes,
			allocatedBytes: fp.AllocatedBytes,
			elapsed:        time.Since(start),
		})
		return nil
	}
	currentJSON := func(i int) []byte {
		if !updated[i] {
			return docs[i].JSON
		}
		replacement = competitive.AppendSameSizeUpdatedJSON(replacement[:0], docs, i)
		return replacement
	}

	for mutations < cfg.mutationBudget {
		i := rng.Intn(len(docs))
		stateChanges := 1
		replace := rng.Intn(100) < cfg.replacePercent
		// A delete+reinsert is indivisible for live-set sampling. Finish an odd
		// budget with a replacement rather than overshooting it.
		if !replace && cfg.mutationBudget-mutations >= 2 {
			value := currentJSON(i)
			if err := engine.Delete(docs[i].Key); err != nil {
				return err
			}
			if err := engine.Upsert(docs[i].Key, value); err != nil {
				return err
			}
			stateChanges = 2
		} else {
			if updated[i] {
				replacement = append(replacement[:0], docs[i].JSON...)
			} else {
				replacement = competitive.AppendSameSizeUpdatedJSON(replacement[:0], docs, i)
			}
			if err := engine.Put(docs[i].Key, replacement); err != nil {
				return err
			}
			updated[i] = !updated[i]
		}
		mutations += stateChanges
		if checkpoints.Add(stateChanges) {
			if err := engine.Checkpoint(); err != nil {
				return err
			}
			checkpoints.Mark()
		}
		if mutations >= nextSample && mutations < cfg.mutationBudget {
			if err := takeSample("sample"); err != nil {
				return err
			}
			nextSample = (mutations/cfg.sampleMutations + 1) * cfg.sampleMutations
		}
	}
	if checkpoints.Pending() != 0 {
		if err := engine.Checkpoint(); err != nil {
			return err
		}
	}
	if err := takeSample("pre-floor"); err != nil {
		return err
	}
	// Verify every final key and value outside the timed mutation interval. The
	// maintenance row remains comparable with the pre-maintenance row by
	// excluding the oracle's wall time from its cumulative elapsed value.
	verifyStart := time.Now()
	if err := verifyChurnCorpus(engine, cfg.engineName, docs, updated); err != nil {
		return err
	}
	start = start.Add(time.Since(verifyStart))
	if err := floor.MaintenanceFloor(); err != nil {
		return err
	}
	if err := takeSample("post-floor"); err != nil {
		return err
	}

	forced := automaticCheckpointCount(engine) - forcedStart
	if forced != 0 && !cfg.allowDiagnostic {
		return fmt.Errorf(
			"%s forced %d checkpoint(s); rerun with -allow-diagnostic "+
				"to retain explicitly non-publishable output",
			cfg.engineName, forced,
		)
	}
	publishable := publicationShape(cfg) &&
		forced == 0 && !cfg.allowDiagnostic
	printHeader(out)
	commit, modified := vcsProvenance()
	for _, s := range samples {
		fmt.Fprintf(out, "%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%d\t%t\t%s\t%s\t%d\t%d\t%d\t%.6f\t%s\t%s\t%s\n",
			commit, modified, cfg.engineName, cardinality, cfg.corpusSize,
			cfg.mutationBudget, cfg.replacePercent, cfg.sampleMutations,
			engine.DurabilityMode(), cfg.checkpointMutations,
			competitive.DefaultCacheBytes, cfg.seed,
			forced, publishable, floor.MaintenanceFloorDescription(), s.phase,
			s.mutationIndex, s.apparentBytes, s.allocatedBytes, s.elapsed.Seconds(),
			profile.Profile, profile.Compression, profile.Provenance,
		)
	}
	return nil
}

func verifyChurnCorpus(
	engine competitive.Engine,
	engineName string,
	docs []competitive.Doc,
	updated []bool,
) error {
	if len(updated) != len(docs) {
		return fmt.Errorf(
			"churn oracle state length %d does not match corpus length %d",
			len(updated), len(docs),
		)
	}
	keyIndexes := make(map[string]int, len(docs))
	for i := range docs {
		keyIndexes[docs[i].Key] = i
	}
	seen := make([]bool, len(docs))
	seenCount := 0
	var submitted, expected []byte
	if err := engine.Visit(func(key string, value []byte) error {
		i, ok := keyIndexes[key]
		if !ok {
			return fmt.Errorf("churn oracle found unexpected key %q", key)
		}
		if seen[i] {
			return fmt.Errorf("churn oracle found duplicate key %q", key)
		}
		seen[i] = true
		seenCount++
		submitted = submitted[:0]
		if updated[i] {
			submitted = competitive.AppendSameSizeUpdatedJSON(submitted, docs, i)
		} else {
			submitted = append(submitted, docs[i].JSON...)
		}
		var err error
		expected, err = competitive.AppendExpectedStoredJSON(
			expected[:0], engineName, submitted,
		)
		if err != nil {
			return fmt.Errorf("churn oracle canonicalize %q: %w", key, err)
		}
		if !bytes.Equal(value, expected) {
			return fmt.Errorf(
				"churn oracle value mismatch for %q: got %d bytes, want %d",
				key, len(value), len(expected),
			)
		}
		return nil
	}); err != nil {
		return err
	}
	if seenCount != len(docs) {
		for i := range docs {
			if !seen[i] {
				return fmt.Errorf("churn oracle is missing key %q", docs[i].Key)
			}
		}
		return fmt.Errorf(
			"churn oracle visited %d keys, want %d", seenCount, len(docs),
		)
	}
	return nil
}

func publicationShape(cfg config) bool {
	return cfg.corpusSize == competitive.CorpusSize &&
		cfg.mutationBudget == 200_000 &&
		cfg.replacePercent == 80 &&
		cfg.sampleMutations == 5_000 &&
		cfg.checkpointMutations == 64 &&
		cfg.seed == defaultSeed
}

func printHeader(w io.Writer) {
	fmt.Fprintln(w, strings.Join([]string{
		"git-commit", "vcs-modified", "engine", "cardinality", "corpus", "mutation-budget",
		"replace-percent", "sample-mutations", "durability",
		"checkpoint-mutations", "cache-bytes", "seed", "forced-cp", "publishable",
		"maintenance-floor", "phase", "mutation-index", "apparent-bytes",
		"allocated-bytes", "elapsed-seconds", "storage-profile", "compression",
		"compression-provenance",
	}, "\t"))
}

type automaticCheckpointReporter interface {
	AutomaticCheckpoints() uint64
}

func automaticCheckpointCount(engine competitive.Engine) uint64 {
	reporter, ok := engine.(automaticCheckpointReporter)
	if !ok {
		return 0
	}
	return reporter.AutomaticCheckpoints()
}

type checkpointSchedule struct {
	every   int
	pending int
}

func (s *checkpointSchedule) Add(mutations int) bool {
	s.pending += mutations
	return s.every > 0 && s.pending >= s.every
}

func (s *checkpointSchedule) Mark()        { s.pending = 0 }
func (s *checkpointSchedule) Pending() int { return s.pending }

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
