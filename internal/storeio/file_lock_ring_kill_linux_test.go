//go:build linux && (amd64 || arm64 || riscv64 || loong64)

package storeio

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const registeredWriterChildMode = "VIBEDB_REGISTERED_WRITER_KILL_CHILD"

// TestRegisteredWriterSIGKILLRelease measures the boundary between waitpid and
// final release of a registered open file description. Delayed kernel cleanup
// is an observation, not permission for production to ignore a writer lock.
// A live writer must always exclude another owner, and a persistent lock after
// child death fails this diagnostic's fixed deadline.
func TestRegisteredWriterSIGKILLRelease(t *testing.T) {
	if mode := os.Getenv(registeredWriterChildMode); mode != "" {
		registeredWriterKillChild(t, mode)
		return
	}
	if kernel, err := os.ReadFile("/proc/sys/kernel/osrelease"); err == nil {
		t.Logf("kernel=%s", bytes.TrimSpace(kernel))
	}
	for _, mode := range []string{"plain", "registered"} {
		t.Run(mode, func(t *testing.T) {
			blocked := 0
			var maximum time.Duration
			for cycle := range 8 {
				delayed, elapsed := registeredWriterKillCycle(t, mode, cycle)
				if delayed {
					blocked++
				}
				maximum = max(maximum, elapsed)
			}
			t.Logf("mode=%s kills=8 immediately_blocked=%d maximum_post_wait_release=%s bound=2s",
				mode, blocked, maximum)
		})
	}
}

func registeredWriterKillChild(t *testing.T, mode string) {
	t.Helper()
	if mode != "plain" && mode != "registered" {
		t.Fatalf("invalid writer child mode %q", mode)
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	ready := os.NewFile(3, "writer-ready")
	if ready == nil {
		t.Fatal("missing readiness descriptor")
	}
	file, err := os.OpenFile(os.Getenv("VIBEDB_REGISTERED_WRITER_KILL_PATH"), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := LockWriter(file); err != nil {
		t.Fatal(err)
	}
	var ring *Ring
	if mode == "registered" {
		ring, err = Open(Config{Entries: 8, SingleIssuer: true})
		if errors.Is(err, ErrUnavailable) || errors.Is(err, ErrUnsupported) {
			t.Logf("io_uring unavailable: %v", err)
			_, _ = ready.Write([]byte{'U'})
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := ring.RegisterFiles([]int{int(file.Fd())}); err != nil {
			if errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.ENOMEM) {
				t.Logf("io_uring file registration unavailable: %v", err)
				_, _ = ready.Write([]byte{'U'})
				return
			}
			t.Fatal(err)
		}
	}
	if _, err := ready.Write([]byte{'R'}); err != nil {
		t.Fatal(err)
	}
	// The parent holds this pipe open until SIGKILL. Do not close the ring or
	// unlock the file: process death is the release mechanism under test.
	var stop [1]byte
	_, err = os.Stdin.Read(stop[:])
	runtime.KeepAlive(ring)
	runtime.KeepAlive(file)
	t.Fatalf("child control pipe returned before SIGKILL: %v", err)
}

func registeredWriterKillCycle(t *testing.T, mode string, cycle int) (bool, time.Duration) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "writer.vdb")
	contender, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer contender.Close()
	var identity unix.Stat_t
	if err := unix.Fstat(int(contender.Fd()), &identity); err != nil {
		t.Fatal(err)
	}
	readyRead, readyWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyRead.Close()
	defer readyWrite.Close()
	controlRead, controlWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer controlRead.Close()
	defer controlWrite.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, executable, "-test.run=^TestRegisteredWriterSIGKILLRelease$", "-test.count=1")
	command.Env = append(os.Environ(), registeredWriterChildMode+"="+mode,
		"VIBEDB_REGISTERED_WRITER_KILL_PATH="+path)
	command.ExtraFiles = []*os.File{readyWrite}
	command.Stdin = controlRead
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	_ = readyWrite.Close()
	_ = controlRead.Close()
	if err := readyRead.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	var ready [1]byte
	if _, err := io.ReadFull(readyRead, ready[:]); err != nil {
		_ = command.Process.Kill()
		waitErr := command.Wait()
		waited = true
		t.Fatalf("child readiness: %v wait=%v output=%s", err, waitErr, output.Bytes())
	}
	if ready[0] == 'U' {
		waitErr := command.Wait()
		waited = true
		if waitErr != nil {
			t.Fatalf("unsupported child exit=%v output=%s", waitErr, output.Bytes())
		}
		t.Skipf("registered OFD SIGKILL diagnostic unavailable: %s", output.Bytes())
	}
	if ready[0] != 'R' {
		t.Fatalf("invalid readiness byte %d", ready[0])
	}
	if err := LockWriter(contender); !errors.Is(err, ErrWriterLocked) {
		if err == nil {
			_ = UnlockWriter(contender)
		}
		t.Fatalf("live child did not exclude writer: %v", err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	waitErr := command.Wait()
	waited = true
	status, ok := command.ProcessState.Sys().(syscall.WaitStatus)
	if waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child was not reaped after SIGKILL: %v status=%v output=%s", waitErr, status, output.Bytes())
	}
	started := time.Now()
	err = LockWriter(contender)
	immediatelyBlocked := errors.Is(err, ErrWriterLocked)
	deadline := started.Add(2 * time.Second)
	if immediatelyBlocked {
		// Exercise the shipped startup waiter against an actual registered OFD
		// left by SIGKILL; retain the same measured two-second release gate.
		err = LockWriterUntil(contender, context.Background(), deadline)
	}
	elapsed := time.Since(started)
	if err != nil || elapsed > 2*time.Second {
		if err == nil {
			_ = UnlockWriter(contender)
		}
		t.Fatalf("writer lease not released within bound after waitpid: mode=%s cycle=%d device=%d inode=%d elapsed=%s err=%v",
			mode, cycle, identity.Dev, identity.Ino, elapsed, err)
	}
	if err := UnlockWriter(contender); err != nil {
		t.Fatal(err)
	}
	t.Logf("mode=%s cycle=%d device=%d inode=%d immediately_blocked=%t post_wait_release=%s",
		mode, cycle, identity.Dev, identity.Ino, immediatelyBlocked, elapsed)
	return immediatelyBlocked, elapsed
}
