// Command rf3chaos repeatedly executes the shipped three-process RF3 fault
// harness and writes canonical raw evidence. It publishes no performance claim.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"hash"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/rf3bench"
)

const (
	faultTestName    = "TestServeRF3ShippedFaultHarness"
	maximumLogPrefix = 2 << 20
)

type options struct {
	output  string
	runs    uint
	timeout time.Duration
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "rf3chaos:", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	flags := flag.NewFlagSet("rf3chaos", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var config options
	flags.StringVar(&config.output, "output", "", "new canonical TSV evidence path")
	flags.UintVar(&config.runs, "runs", 1, "isolated fault-harness repetitions")
	flags.DurationVar(&config.timeout, "timeout", 3*time.Minute, "timeout for each repetition")
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 {
		return errors.New("usage: rf3chaos -output PATH [-runs N] [-timeout DURATION]")
	}
	if config.output == "" || !filepath.IsAbs(config.output) || config.runs == 0 ||
		config.runs > rf3bench.MaximumChaosRuns || config.timeout <= 0 {
		return errors.New("output must be absolute, runs must be within the evidence bound, and timeout must be positive")
	}
	if _, err := os.Stat(config.output); err == nil || !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return errors.New("output path already exists")
		}
		return fmt.Errorf("inspect output: %w", err)
	}
	root, err := repositoryRoot()
	if err != nil {
		return err
	}
	temporary, err := os.MkdirTemp("", "vibedb-rf3chaos-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temporary)
	binary := filepath.Join(temporary, "vibedb-shard.test")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.Command("go", "test", "-c", "-o", binary, "./cmd/vibedb-shard")
	build.Dir = root
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		return fmt.Errorf("build shipped fault harness: %w: %s", buildErr, boundedText(output))
	}
	binaryDigest, err := digestFile(binary)
	if err != nil {
		return err
	}
	metadata, err := environmentMetadata(root, binaryDigest)
	if err != nil {
		return err
	}
	report := rf3bench.ChaosReport{TimeoutNS: uint64(config.timeout), Metadata: metadata,
		Runs: make([]rf3bench.ChaosRun, 0, config.runs)}
	allPassed := true
	for ordinal := uint(1); ordinal <= config.runs; ordinal++ {
		outcome := executeFaultRun(binary, uint32(ordinal), config.timeout)
		report.Runs = append(report.Runs, outcome.run)
		if !outcome.run.Passed {
			allPassed = false
			fmt.Fprintf(os.Stderr, "rf3chaos: run %d failed (exit=%d timeout=%t exact=%t): %s\n",
				ordinal, outcome.run.ExitCode, outcome.run.TimedOut, outcome.run.ExactRun,
				boundedText(outcome.prefix))
		}
	}
	var encoded bytes.Buffer
	if err = rf3bench.WriteChaosTSV(&encoded, report); err != nil {
		return err
	}
	file, err := os.OpenFile(config.output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	writeErr := writeAndSync(file, encoded.Bytes())
	closeErr := file.Close()
	if writeErr != nil {
		return writeErr
	}
	if closeErr != nil {
		return closeErr
	}
	if !allPassed {
		return errors.New("one or more fault-harness runs failed; raw evidence was preserved")
	}
	return nil
}

type faultOutcome struct {
	run    rf3bench.ChaosRun
	prefix []byte
}

func executeFaultRun(binary string, ordinal uint32, timeout time.Duration) faultOutcome {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "-test.run", "^"+faultTestName+"$", "-test.count=1", "-test.v")
	output := newDigestPrefixWriter(maximumLogPrefix)
	command.Stdout, command.Stderr = output, output
	started := time.Now()
	err := command.Run()
	elapsed := uint64(max(time.Since(started), time.Nanosecond))
	prefix, outputBytes, digest := output.result()
	exitCode := int32(0)
	if err != nil {
		exitCode = -1
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			exitCode = int32(exitError.ExitCode())
		}
	}
	exactRun := bytes.Contains(prefix, []byte("=== RUN   "+faultTestName))
	exactPass := bytes.Contains(prefix, []byte("--- PASS: "+faultTestName))
	timedOut := errors.Is(ctx.Err(), context.DeadlineExceeded)
	return faultOutcome{prefix: prefix, run: rf3bench.ChaosRun{
		Ordinal: ordinal, ElapsedNS: elapsed, OutputBytes: outputBytes,
		OutputSHA256: digest, ExitCode: exitCode, TimedOut: timedOut,
		ExactRun: exactRun, Passed: err == nil && !timedOut && exactRun && exactPass,
	}}
}

type digestPrefixWriter struct {
	mu     sync.Mutex
	digest hash.Hash
	prefix []byte
	limit  int
	bytes  uint64
}

func newDigestPrefixWriter(limit int) *digestPrefixWriter {
	return &digestPrefixWriter{digest: sha256.New(), prefix: make([]byte, 0, min(limit, 4096)), limit: limit}
}

func (writer *digestPrefixWriter) Write(value []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	written, err := writer.digest.Write(value)
	writer.bytes += uint64(written)
	if remaining := writer.limit - len(writer.prefix); remaining > 0 {
		writer.prefix = append(writer.prefix, value[:min(len(value), remaining)]...)
	}
	return written, err
}

func (writer *digestPrefixWriter) result() ([]byte, uint64, [32]byte) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	var digest [32]byte
	copy(digest[:], writer.digest.Sum(nil))
	return bytes.Clone(writer.prefix), writer.bytes, digest
}

func repositoryRoot() (string, error) {
	command := exec.Command("git", "rev-parse", "--show-toplevel")
	output, err := command.Output()
	if err != nil {
		return "", fmt.Errorf("locate repository: %w", err)
	}
	return strings.TrimSpace(string(output)), nil
}

func environmentMetadata(root string, binaryDigest [32]byte) ([]rf3bench.Metadata, error) {
	git := func(arguments ...string) ([]byte, error) {
		command := exec.Command("git", arguments...)
		command.Dir = root
		return command.Output()
	}
	revision, err := git("rev-parse", "HEAD")
	if err != nil {
		return nil, fmt.Errorf("read revision: %w", err)
	}
	dirty, err := git("status", "--porcelain=v1", "--untracked-files=normal")
	if err != nil {
		return nil, fmt.Errorf("read dirty state: %w", err)
	}
	goVersion, err := exec.Command("go", "version").Output()
	if err != nil {
		return nil, fmt.Errorf("read go version: %w", err)
	}
	return []rf3bench.Metadata{
		{Key: []byte("binary_sha256"), Value: []byte(fmt.Sprintf("%x", binaryDigest))},
		{Key: []byte("go_version"), Value: bytes.TrimSpace(goVersion)},
		{Key: []byte("goarch"), Value: []byte(runtime.GOARCH)},
		{Key: []byte("goos"), Value: []byte(runtime.GOOS)},
		{Key: []byte("scenario"), Value: []byte("isolate-elect-kill-lose-response-retry-restart-ack-waiter-wal")},
		{Key: []byte("test_name"), Value: []byte(faultTestName)},
		{Key: []byte("vcs_modified"), Value: []byte(strconv.FormatBool(len(dirty) != 0))},
		{Key: []byte("vcs_revision"), Value: bytes.TrimSpace(revision)},
	}, nil
}

func digestFile(path string) ([32]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return [32]byte{}, err
	}
	digest := sha256.New()
	_, copyErr := io.Copy(digest, file)
	closeErr := file.Close()
	if copyErr != nil {
		return [32]byte{}, copyErr
	}
	if closeErr != nil {
		return [32]byte{}, closeErr
	}
	var result [32]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

func writeAndSync(destination *os.File, value []byte) error {
	for len(value) != 0 {
		written, err := destination.Write(value)
		if written > 0 {
			value = value[written:]
		}
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
	}
	return destination.Sync()
}

func boundedText(value []byte) string {
	const limit = 4096
	value = bytes.TrimSpace(value)
	if len(value) > limit {
		value = value[:limit]
	}
	return string(value)
}
