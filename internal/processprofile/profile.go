// Package processprofile provides opt-in local diagnostics for performance work.
// No listener is opened. Normal serving does not start a profiler.
package processprofile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/pprof"
	"runtime/trace"
	"sync"
	"time"
)

// StartFromEnv writes CPU and execution-trace profiles when
// VIBEDB_PROFILE_DIRECTORY names an existing directory. The recording ends after
// VIBEDB_PROFILE_DURATION (default 60s, maximum 5m), or when stop is called.
// The flight recorder has a 16 MiB retention target, not a hard allocation bound.
// Instrumented runs are diagnostics and must not be used as benchmark baselines.
func StartFromEnv(label string) (stop func(), err error) {
	dir := os.Getenv("VIBEDB_PROFILE_DIRECTORY")
	if dir == "" {
		return func() {}, nil
	}
	duration := 60 * time.Second
	if value := os.Getenv("VIBEDB_PROFILE_DURATION"); value != "" {
		duration, err = time.ParseDuration(value)
		if err != nil || duration < time.Second || duration > 5*time.Minute {
			return nil, fmt.Errorf("invalid profile duration")
		}
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil, errors.Join(err, fmt.Errorf("profile directory must exist"))
	}
	prefix := filepath.Join(dir, fmt.Sprintf("%s-%d-%d", label, os.Getpid(), time.Now().UnixNano()))
	cpu, err := os.OpenFile(prefix+".cpu.pprof", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	recorder := trace.NewFlightRecorder(trace.FlightRecorderConfig{MinAge: duration, MaxBytes: 16 << 20})
	if err = recorder.Start(); err != nil {
		_ = cpu.Close()
		return nil, err
	}
	if err = pprof.StartCPUProfile(cpu); err != nil {
		recorder.Stop()
		_ = cpu.Close()
		return nil, err
	}
	var once sync.Once
	finish := func() {
		once.Do(func() {
			pprof.StopCPUProfile()
			closeErr := cpu.Close()
			output, openErr := os.OpenFile(prefix+".trace", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if openErr == nil {
				_, openErr = recorder.WriteTo(output)
				openErr = errors.Join(openErr, output.Close())
			}
			recorder.Stop()
			if err := errors.Join(closeErr, openErr); err != nil {
				fmt.Fprintf(os.Stderr, "profile %s: %v\n", label, err)
			}
		})
	}
	timer := time.AfterFunc(duration, finish)
	return func() { timer.Stop(); finish() }, nil
}
