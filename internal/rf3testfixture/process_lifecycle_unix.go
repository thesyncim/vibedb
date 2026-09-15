//go:build darwin || linux

package rf3testfixture

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

const MaxProcessDiagnosticBytes = 1 << 20

type ProcessDiagnostic struct {
	mu        sync.Mutex
	buffer    bytes.Buffer
	dropped   int64
	tail      [32 << 10]byte
	tailBytes int
	tailNext  int
}

func (diagnostic *ProcessDiagnostic) Write(value []byte) (int, error) {
	diagnostic.mu.Lock()
	defer diagnostic.mu.Unlock()
	accepted := len(value)
	remaining := MaxProcessDiagnosticBytes - len(diagnostic.tail) - diagnostic.buffer.Len()
	if remaining > 0 {
		if remaining > len(value) {
			remaining = len(value)
		}
		_, _ = diagnostic.buffer.Write(value[:remaining])
	}
	value = value[max(remaining, 0):]
	// Keep the startup prefix for readiness and the final output for failure
	// diagnosis. Both remain within the existing fixed diagnostic budget.
	if len(value) > len(diagnostic.tail) {
		diagnostic.dropped += int64(diagnostic.tailBytes + len(value) - len(diagnostic.tail))
		copy(diagnostic.tail[:], value[len(value)-len(diagnostic.tail):])
		diagnostic.tailBytes, diagnostic.tailNext = len(diagnostic.tail), 0
	} else if len(value) != 0 {
		overflow := max(0, diagnostic.tailBytes+len(value)-len(diagnostic.tail))
		diagnostic.dropped += int64(overflow)
		first := copy(diagnostic.tail[diagnostic.tailNext:], value)
		copy(diagnostic.tail[:], value[first:])
		diagnostic.tailNext = (diagnostic.tailNext + len(value)) % len(diagnostic.tail)
		diagnostic.tailBytes = min(len(diagnostic.tail), diagnostic.tailBytes+len(value))
	}
	return accepted, nil
}

func (diagnostic *ProcessDiagnostic) String() string {
	diagnostic.mu.Lock()
	defer diagnostic.mu.Unlock()
	if diagnostic.tailBytes == 0 {
		return diagnostic.buffer.String()
	}
	var output strings.Builder
	output.Grow(diagnostic.buffer.Len() + diagnostic.tailBytes + 32)
	output.Write(diagnostic.buffer.Bytes())
	if diagnostic.dropped != 0 {
		output.WriteString("\n[diagnostic output truncated]\n")
	}
	if diagnostic.tailBytes < len(diagnostic.tail) {
		output.Write(diagnostic.tail[:diagnostic.tailBytes])
	} else {
		output.Write(diagnostic.tail[diagnostic.tailNext:])
		output.Write(diagnostic.tail[:diagnostic.tailNext])
	}
	return output.String()
}

type ExternalProcess struct {
	Binary string
	Args   []string
	Env    []string

	mu         sync.Mutex
	command    *exec.Cmd
	diagnostic *ProcessDiagnostic
	exited     chan struct{}
	waitErr    error
}

func (process *ExternalProcess) Start() error {
	if process == nil || process.Binary == "" {
		return errors.New("rf3 process fixture: invalid external process")
	}
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.command != nil && process.exited != nil {
		select {
		case <-process.exited:
		default:
			return errors.New("rf3 process fixture: process already running")
		}
	}
	diagnostic := new(ProcessDiagnostic)
	command := exec.Command(process.Binary, process.Args...)
	if process.Env != nil {
		command.Env = append([]string(nil), process.Env...)
	}
	command.Stdout, command.Stderr = diagnostic, diagnostic
	if err := command.Start(); err != nil {
		return err
	}
	process.command, process.diagnostic = command, diagnostic
	process.exited, process.waitErr = make(chan struct{}), nil
	exited := process.exited
	go func() {
		err := command.Wait()
		process.mu.Lock()
		process.waitErr = err
		process.mu.Unlock()
		close(exited)
	}()
	return nil
}

func (process *ExternalProcess) WaitReady(ctx context.Context, marker string) error {
	if process == nil || ctx == nil || marker == "" {
		return errors.New("rf3 process fixture: invalid readiness wait")
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if strings.Contains(process.Diagnostics(), marker) {
			return nil
		}
		process.mu.Lock()
		exited := process.exited
		process.mu.Unlock()
		if exited == nil {
			return errors.New("rf3 process fixture: process not started")
		}
		select {
		case <-ctx.Done():
			return errors.Join(context.Cause(ctx), errors.New("rf3 process fixture: readiness timeout"))
		case <-exited:
			return errors.New("rf3 process fixture: process exited before readiness")
		case <-ticker.C:
		}
	}
}

func (process *ExternalProcess) Stop(ctx context.Context) error {
	if process == nil || ctx == nil {
		return errors.New("rf3 process fixture: invalid process stop")
	}
	process.mu.Lock()
	command, exited := process.command, process.exited
	process.mu.Unlock()
	if command == nil || exited == nil {
		return nil
	}
	select {
	case <-exited:
		return process.WaitError()
	default:
	}
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}
	select {
	case <-exited:
		return process.WaitError()
	case <-ctx.Done():
		_ = command.Process.Kill()
		<-exited
		return errors.Join(context.Cause(ctx), errors.New("rf3 process fixture: forced process cleanup"))
	}
}

func (process *ExternalProcess) Kill(ctx context.Context) error {
	if process == nil || ctx == nil {
		return errors.New("rf3 process fixture: invalid process kill")
	}
	process.mu.Lock()
	command, exited := process.command, process.exited
	process.mu.Unlock()
	if command == nil || exited == nil {
		return nil
	}
	select {
	case <-exited:
		return nil
	default:
	}
	if err := command.Process.Kill(); err != nil {
		return err
	}
	select {
	case <-exited:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (process *ExternalProcess) WaitError() error {
	process.mu.Lock()
	defer process.mu.Unlock()
	return process.waitErr
}

func (process *ExternalProcess) Diagnostics() string {
	process.mu.Lock()
	diagnostic := process.diagnostic
	process.mu.Unlock()
	if diagnostic == nil {
		return ""
	}
	return diagnostic.String()
}

// PID returns the current child identity for bounded Linux qualification
// sampling. Zero means no child is running. Callers must still tolerate the
// process exiting immediately after the sample.
func (process *ExternalProcess) PID() int {
	if process == nil {
		return 0
	}
	process.mu.Lock()
	defer process.mu.Unlock()
	if process.command == nil || process.command.Process == nil || process.exited == nil {
		return 0
	}
	select {
	case <-process.exited:
		return 0
	default:
		return process.command.Process.Pid
	}
}

type ReservedAddresses struct {
	Listeners []*net.TCPListener
	Addresses []string
}

func ReserveLoopbackAddresses(count int) (*ReservedAddresses, error) {
	if count <= 0 || count > 64 {
		return nil, errors.New("rf3 process fixture: invalid listener count")
	}
	reservation := &ReservedAddresses{Listeners: make([]*net.TCPListener, 0, count),
		Addresses: make([]string, 0, count)}
	for range count {
		listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.ParseIP("127.0.0.1")})
		if err != nil {
			_ = reservation.Close()
			return nil, err
		}
		reservation.Listeners = append(reservation.Listeners, listener)
		reservation.Addresses = append(reservation.Addresses, listener.Addr().String())
	}
	return reservation, nil
}

func (reservation *ReservedAddresses) Close() error {
	if reservation == nil {
		return nil
	}
	var result error
	for _, listener := range reservation.Listeners {
		if listener != nil {
			result = errors.Join(result, listener.Close())
		}
	}
	reservation.Listeners = nil
	return result
}
