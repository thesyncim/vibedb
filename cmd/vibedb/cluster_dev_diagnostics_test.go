package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestDevDiagnosticsKeepsBoundedTailWithoutWarmAllocations(t *testing.T) {
	diagnostics := &devDiagnostics{maximum: 31}
	var all []byte
	for _, length := range []int{0, 7, 15, 1, 31, 5, 80, 2} {
		chunk := bytes.Repeat([]byte{byte('a' + length%26)}, length)
		all = append(all, chunk...)
		if n, err := diagnostics.Write(chunk); n != length || err != nil {
			t.Fatalf("write=%d err=%v", n, err)
		}
		if got, want := diagnostics.String(), string(all[max(0, len(all)-31):]); got != want {
			t.Fatalf("tail=%q want=%q", got, want)
		}
		if len(diagnostics.data) > 31 || cap(diagnostics.data) > 31 {
			t.Fatal("diagnostic storage exceeds fixed bound")
		}
	}
	chunk := []byte("more output\n")
	if allocations := testing.AllocsPerRun(1000, func() { _, _ = diagnostics.Write(chunk) }); allocations != 0 {
		t.Fatalf("warm write allocations=%g", allocations)
	}
}

type devChunkReader struct{ chunks [][]byte }

func (r *devChunkReader) Read(dst []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	n := copy(dst, r.chunks[0])
	r.chunks[0] = r.chunks[0][n:]
	if len(r.chunks[0]) == 0 {
		r.chunks = r.chunks[1:]
	}
	return n, nil
}

func TestDevChildOutputFindsSplitMarkerAndRetainsLongFatalLine(t *testing.T) {
	diagnostics := &devDiagnostics{maximum: devChildDiagnosticBytes}
	ready := make(chan error, 1)
	reader := &devChunkReader{chunks: [][]byte{[]byte("boot RE"), []byte("AD"), []byte("Y\n"), bytes.Repeat([]byte("x"), 128<<10), []byte("fatal-final-without-newline")}}
	captureDevChildOutput(reader, diagnostics, "READY", ready)
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(diagnostics.String(), "fatal-final-without-newline") || diagnostics.count != devChildDiagnosticBytes {
		t.Fatal("final output was not retained within fixed bound")
	}
}

func TestDevDiagnosticChildHelper(t *testing.T) {
	if len(os.Args) < 2 {
		return
	}
	mode := os.Args[len(os.Args)-1]
	if mode != "dev-diagnostic-fatal" && mode != "dev-diagnostic-held-pipe" {
		return
	}
	if mode == "dev-diagnostic-held-pipe" {
		child := exec.Command("/bin/sleep", "30")
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if err := child.Start(); err != nil {
			os.Exit(18)
		}
		fmt.Fprintf(os.Stderr, "DESCENDANT_PID=%d\n", child.Process.Pid)
	}
	fmt.Fprintln(os.Stdout, "READY")
	if mode == "dev-diagnostic-fatal" {
		_, _ = os.Stderr.Write(bytes.Repeat([]byte("x"), 128<<10))
	}
	fmt.Fprint(os.Stderr, "fatal-after-readiness-without-newline")
	os.Exit(17)
}

func TestDevSupervisorExitIncludesDrainedFinalTail(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child, err := startDevChild(binary, []string{"-test.run=^TestDevDiagnosticChildHelper$", "--", "dev-diagnostic-fatal"}, "READY")
	if err != nil {
		t.Fatal(err)
	}
	defer stopDevChildren([]*devChild{child})
	exits := make(chan devChildExit, 1)
	watchDevChildExit(exits, "request-ledger shard member 2", child)
	select {
	case exit := <-exits:
		if exit.name != "request-ledger shard member 2" || exit.err == nil || !strings.Contains(exit.err.Error(), "fatal-after-readiness-without-newline") {
			t.Fatalf("fatal tail missing: %+v", exit)
		}
		if len(exit.err.Error()) > devChildDiagnosticBytes+256 {
			t.Fatal("exit diagnostics exceed fixed envelope")
		}
		var status *exec.ExitError
		if !errors.As(exit.err, &status) || status.ExitCode() != 17 {
			t.Fatalf("original child status lost: %v", exit.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("child exit was not reported")
	}
}

func TestDevSupervisorBoundsInheritedPipeDrain(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child, err := startDevChild(binary, []string{"-test.run=^TestDevDiagnosticChildHelper$", "--", "dev-diagnostic-held-pipe"}, "READY")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		// Keep the failure path bounded too: reap the deliberately inherited
		// pipe holder even if this regression reintroduces a hung Wait.
		for _, line := range strings.Split(child.diagnostics.String(), "\n") {
			if text, ok := strings.CutPrefix(line, "DESCENDANT_PID="); ok {
				if pid, err := strconv.Atoi(text); err == nil {
					if process, err := os.FindProcess(pid); err == nil {
						_ = process.Kill()
					}
				}
			}
		}
		stopDevChildren([]*devChild{child})
	}()
	started := time.Now()
	select {
	case <-child.done:
		if time.Since(started) > 3*time.Second {
			t.Fatal("descendant pipe delayed child exit")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("inherited pipe hung process wait")
	}
	diagnostic := child.diagnostics.String()
	for _, line := range strings.Split(diagnostic, "\n") {
		if text, ok := strings.CutPrefix(line, "DESCENDANT_PID="); ok {
			pid, err := strconv.Atoi(text)
			if err != nil || pid <= 0 {
				t.Fatal(err)
			}
			if !strings.HasSuffix(diagnostic, "fatal-after-readiness-without-newline") {
				t.Fatal("bounded drain dropped final fatal output")
			}
			return
		}
	}
	t.Fatal("missing bounded descendant diagnostic")
}

func TestDevSupervisorStopsChildrenAppendedAfterDeferredCleanup(t *testing.T) {
	root := t.TempDir()
	reaped := [3]bool{}
	// Register cleanup before starting any child and inspect every recorded
	// PID independently, even if the first assertion exposes a regression.
	t.Cleanup(func() {
		for i := 0; i < 3; i++ {
			if reaped[i] {
				continue
			}
			raw, err := os.ReadFile(filepath.Join(root, strconv.Itoa(i)+".pid"))
			if err != nil {
				continue
			}
			pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
			if err != nil || pid <= 0 {
				continue
			}
			if process, err := os.FindProcess(pid); err == nil {
				_ = process.Kill()
			}
		}
	})
	binary := filepath.Join(root, "fake-shard")
	script := []byte("#!/bin/sh\necho $$ > \"$3.pid\"\necho 'vibedb-shard RF1-development-only-no-HA ready' >&2\ntrap 'exit 0' TERM\nwhile :; do sleep 0.05; done\n")
	if err := os.WriteFile(binary, script, 0o700); err != nil {
		t.Fatal(err)
	}
	m := devClusterManifest{Nodes: devClusterRF1}
	for i, target := range []*[]devClusterMember{&m.Members, &m.LedgerMembers, &m.DataMembers} {
		*target = []devClusterMember{{Member: 1, ServeManifest: filepath.Join(root, strconv.Itoa(i))}}
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		result <- serveDevCluster(ctx, m, binary, "unused")
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Error("supervisor did not stop after cancellation")
		}
	})
	// Wait for actual child startup, not a fixed sleep that can expire before
	// readiness is consumed on a busy test host. Cancellation may reach either
	// the readiness wait or the serving wait; both must reap every child.
	deadline := time.Now().Add(10 * time.Second)
	for {
		started := 0
		for i := 0; i < 3; i++ {
			if raw, err := os.ReadFile(filepath.Join(root, strconv.Itoa(i)+".pid")); err == nil {
				if pid, err := strconv.Atoi(strings.TrimSpace(string(raw))); err == nil && pid > 0 {
					started++
				}
			}
		}
		if started == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("supervisor did not start all three children")
		}
		select {
		case err := <-result:
			t.Fatalf("supervisor stopped before child startup: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	var stopErr error
	select {
	case stopErr = <-result:
	case <-time.After(5 * time.Second):
		t.Fatal("supervisor did not reap children")
	}
	if err := stopErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		raw, err := os.ReadFile(filepath.Join(root, strconv.Itoa(i)+".pid"))
		if err != nil {
			t.Fatal(err)
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			t.Fatal(err)
		}
		process, err := os.FindProcess(pid)
		if err != nil {
			t.Fatal(err)
		}
		if err := process.Signal(syscall.Signal(0)); err == nil {
			t.Fatalf("supervisor left child %d running", pid)
		}
		reaped[i] = true
	}
}
