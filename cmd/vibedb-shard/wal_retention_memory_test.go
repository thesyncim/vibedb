//go:build darwin || linux

package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"syscall"
	"testing"
	"time"
)

const walRetentionMemoryEnvironment = "VIBEDB_WAL_RETENTION_MEMORY"
const walRetentionProfileBound = 8 << 20
const walRetentionStatusBound = 16 << 10

func prepareWALRetentionMemoryDiagnostics(t *testing.T) string {
	t.Helper()
	root := os.Getenv(walRetentionEvidenceEnvironment)
	if root == "" {
		root = t.TempDir()
	}
	directory := filepath.Join(root, "memory")
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(walRetentionMemoryEnvironment, directory)
	return directory
}

// This hook exists only in the test binary and is armed before inherited
// listeners can publish readiness. No signal is sent without its ready file.
func startWALRetentionMemoryDiagnostics(t testing.TB) func() {
	t.Helper()
	directory := os.Getenv(walRetentionMemoryEnvironment)
	if os.Getenv(walRetentionQualificationEnvironment) != "1" ||
		os.Getenv(rf3CommandHelperEnvironment) != "1" || directory == "" {
		return func() {}
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		t.Fatalf("memory diagnostic directory must already exist and be private: %v", err)
	}
	signals := make(chan os.Signal, 1)
	stop, done := make(chan struct{}), make(chan struct{})
	signal.Notify(signals, syscall.SIGUSR1)
	prefix := filepath.Join(directory, fmt.Sprintf("pid-%d", os.Getpid()))
	go func() {
		defer close(done)
		select {
		case <-stop:
			return
		case <-signals:
			writeWALRetentionMemoryProfiles(prefix)
		}
		// At most one bounded dump per process, even if a signal is repeated.
		<-stop
	}()
	if err := writeWALRetentionDiagnosticFile(prefix+".ready", []byte(strconv.Itoa(os.Getpid()))); err != nil {
		signal.Stop(signals)
		close(stop)
		<-done
		t.Fatal(err)
	}
	return func() {
		// Stop advertising the handler before restoring signal disposition.
		// The test parent owns the child lifetime and never captures during close.
		_ = os.Remove(prefix + ".ready")
		signal.Stop(signals)
		close(stop)
		<-done
	}
}

type walRetentionBoundedWriter struct {
	writer io.Writer
	left   int
}

func (writer *walRetentionBoundedWriter) Write(data []byte) (int, error) {
	if len(data) > writer.left {
		n, err := writer.writer.Write(data[:writer.left])
		writer.left -= n
		return n, errors.Join(err, errors.New("memory diagnostic profile exceeds bound"))
	}
	n, err := writer.writer.Write(data)
	writer.left -= n
	return n, err
}

func writeWALRetentionDiagnosticFile(path string, data []byte) error {
	if len(data) > walRetentionStatusBound {
		return errors.New("memory diagnostic metadata exceeds bound")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	return errors.Join(writeErr, file.Close())
}

func writeWALRetentionMemoryProfiles(prefix string) {
	var memory runtime.MemStats
	// ReadMemStats and profile.WriteTo do not request a GC. Heap profiles can
	// reflect the last completed collection; retain both live and total samples.
	runtime.ReadMemStats(&memory)
	metadata := fmt.Appendf(nil, "pid\t%d\nheap_alloc\t%d\nheap_sys\t%d\nheap_inuse\t%d\nheap_idle\t%d\nheap_released\t%d\nstack_inuse\t%d\nsys\t%d\ntotal_alloc\t%d\nnum_gc\t%d\nnext_gc\t%d\n",
		os.Getpid(), memory.HeapAlloc, memory.HeapSys, memory.HeapInuse, memory.HeapIdle,
		memory.HeapReleased, memory.StackInuse, memory.Sys, memory.TotalAlloc, memory.NumGC, memory.NextGC)
	for _, name := range []string{"heap", "allocs"} {
		file, err := os.OpenFile(prefix+"."+name+".pprof", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err == nil {
			err = errors.Join(pprof.Lookup(name).WriteTo(&walRetentionBoundedWriter{writer: file, left: walRetentionProfileBound}, 0), file.Close())
		}
		metadata = fmt.Appendf(metadata, "%s_profile_error\t%v\n", name, err)
	}
	// Publish completion only after the immutable metadata is fully closed;
	// the polling parent must never mistake a partial write for a complete dump.
	if err := writeWALRetentionDiagnosticFile(prefix+".metadata", metadata); err == nil {
		_ = os.Link(prefix+".metadata", prefix+".done")
	}
}

func readWALRetentionDiagnosticFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, walRetentionStatusBound+1))
	err = errors.Join(readErr, file.Close())
	if len(data) > walRetentionStatusBound {
		return nil, errors.New("memory diagnostic input exceeds bound")
	}
	return data, err
}

func logWALRetentionMemory(t testing.TB, children [rf3CommandMembers]*rf3CommandChild, phase string) {
	t.Helper()
	for member, child := range children {
		pid := child.command.Process.Pid
		data, err := readWALRetentionDiagnosticFile(fmt.Sprintf("/proc/%d/status", pid))
		if err != nil {
			t.Logf("memory phase=%s member=%d pid=%d status_error=%v", phase, member+1, pid, err)
			continue
		}
		for _, line := range bytes.Split(data, []byte{'\n'}) {
			fields := bytes.Fields(line)
			if len(fields) != 3 {
				continue
			}
			switch string(fields[0]) {
			case "VmRSS:", "VmHWM:", "RssAnon:", "RssFile:", "RssShmem:", "VmData:":
				t.Logf("memory phase=%s member=%d pid=%d %s", phase, member+1, pid, line)
			}
		}
	}
}

func captureWALRetentionMemory(t testing.TB, children [rf3CommandMembers]*rf3CommandChild, directory string) {
	t.Helper()
	var pending [rf3CommandMembers]string
	for member, child := range children {
		pid := child.command.Process.Pid
		prefix := filepath.Join(directory, fmt.Sprintf("pid-%d", pid))
		ready, err := readWALRetentionDiagnosticFile(prefix + ".ready")
		if err != nil || string(ready) != strconv.Itoa(pid) {
			t.Logf("memory member=%d handler not ready: %v", member+1, err)
			continue
		}
		if err := child.command.Process.Signal(syscall.SIGUSR1); err != nil {
			t.Logf("memory member=%d signal failed: %v", member+1, err)
			continue
		}
		pending[member] = prefix + ".done"
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		remaining := false
		for member, path := range pending {
			if path == "" {
				continue
			}
			data, err := readWALRetentionDiagnosticFile(path)
			if err != nil {
				remaining = true
				continue
			}
			t.Logf("memory member=%d profile_prefix=%s\n%s", member+1, path[:len(path)-len(".done")], data)
			pending[member] = ""
		}
		if !remaining {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for member, path := range pending {
		if path != "" {
			t.Logf("memory member=%d profile capture timed out: %s", member+1, path)
		}
	}
}

func TestWALRetentionMemoryDiagnosticBounds(t *testing.T) {
	var output bytes.Buffer
	writer := walRetentionBoundedWriter{writer: &output, left: 5}
	if n, err := writer.Write([]byte("123")); n != 3 || err != nil {
		t.Fatalf("bounded write=%d %v", n, err)
	}
	if n, err := writer.Write([]byte("456")); n != 2 || err == nil || output.String() != "12345" {
		t.Fatalf("overflow write=%d %v output=%q", n, err, output.String())
	}
	if n, err := writer.Write([]byte("7")); n != 0 || err == nil || output.Len() != 5 {
		t.Fatal("profile grew after bound")
	}
	path := filepath.Join(t.TempDir(), "metadata")
	if err := writeWALRetentionDiagnosticFile(path, []byte("original")); err != nil {
		t.Fatal(err)
	}
	if err := writeWALRetentionDiagnosticFile(path, []byte("overwrite")); err == nil {
		t.Fatal("diagnostic overwrote existing evidence")
	}
	if data, err := readWALRetentionDiagnosticFile(path); err != nil || string(data) != "original" {
		t.Fatalf("metadata=%q err=%v", data, err)
	}
}

func TestWALRetentionMemoryDiagnosticHandlerReadinessLifetime(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(walRetentionMemoryEnvironment, directory)
	t.Setenv(walRetentionQualificationEnvironment, "1")
	t.Setenv(rf3CommandHelperEnvironment, "1")
	stop := startWALRetentionMemoryDiagnostics(t)
	path := filepath.Join(directory, fmt.Sprintf("pid-%d.ready", os.Getpid()))
	ready, err := readWALRetentionDiagnosticFile(path)
	stop()
	if err != nil || string(ready) != strconv.Itoa(os.Getpid()) {
		t.Fatalf("handler readiness=%q err=%v", ready, err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stopped handler still advertises readiness: %v", err)
	}
}

func TestWALRetentionMemoryDiagnosticProfilesPublishCompleteMetadata(t *testing.T) {
	prefix := filepath.Join(t.TempDir(), "sample")
	writeWALRetentionMemoryProfiles(prefix)
	metadata, err := readWALRetentionDiagnosticFile(prefix + ".done")
	if err != nil || !bytes.Contains(metadata, []byte("heap_alloc\t")) ||
		!bytes.Contains(metadata, []byte("heap_profile_error\t<nil>")) ||
		!bytes.Contains(metadata, []byte("allocs_profile_error\t<nil>")) {
		t.Fatalf("incomplete memory metadata=%q err=%v", metadata, err)
	}
	for _, name := range []string{"heap", "allocs"} {
		info, err := os.Stat(prefix + "." + name + ".pprof")
		if err != nil || info.Size() == 0 || info.Size() > walRetentionProfileBound {
			t.Fatalf("invalid %s profile: %v %v", name, info, err)
		}
	}
	writeWALRetentionMemoryProfiles(prefix)
	again, err := readWALRetentionDiagnosticFile(prefix + ".done")
	if err != nil || !bytes.Equal(again, metadata) {
		t.Fatal("repeated dump replaced original evidence")
	}
}
