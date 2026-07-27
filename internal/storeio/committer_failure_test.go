package storeio

import (
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

type commitFailurePhase uint8

const (
	failDataWrite commitFailurePhase = iota
	failDataBarrier
	failRootWrite
	failRootBarrier
)

type phaseFailureDevice struct {
	arena      []byte
	bufferSize int
	phase      commitFailurePhase
	err        error
	once       sync.Once
	commits    int
}

func newPhaseFailureDevice(buffers, bufferSize int, phase commitFailurePhase, err error) *phaseFailureDevice {
	return &phaseFailureDevice{
		arena: make([]byte, buffers*bufferSize), bufferSize: bufferSize,
		phase: phase, err: err,
	}
}

func (*phaseFailureDevice) Backend() Backend { return BackendPortable }

func (d *phaseFailureDevice) Buffer(index int) ([]byte, error) {
	start := index * d.bufferSize
	return d.arena[start : start+d.bufferSize], nil
}

func (d *phaseFailureDevice) Commit(_ []Write, _ Write) error {
	d.commits++
	var result error
	d.once.Do(func() {
		result = d.err
		if d.phase >= failRootWrite {
			result = commitOutcomeUnknown(result)
		}
	})
	return result
}

func (*phaseFailureDevice) Close() error { return nil }

func TestCommitterPersistenceFailurePhasesPoisonWriterAndReportOutcome(t *testing.T) {
	persistErr := errors.New("injected persistence I/O failure")
	tests := []struct {
		name    string
		phase   commitFailurePhase
		unknown bool
	}{
		{name: "data-write", phase: failDataWrite},
		{name: "data-barrier", phase: failDataBarrier},
		{name: "root-write", phase: failRootWrite, unknown: true},
		{name: "root-barrier", phase: failRootBarrier, unknown: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "commit-failure")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			pageSize := os.Getpagesize()
			device := newPhaseFailureDevice(3, pageSize, test.phase, persistErr)
			committer, err := newCommitter(
				file,
				DeviceOptions{
					Backend: BackendPortable, BufferCount: 3,
					BufferSize: pageSize,
				},
				CommitterOptions{
					QueueSlots: 2, MaxPagesPerBatch: 1, GroupLimit: 1,
				},
				func(*os.File, DeviceOptions) (Device, error) {
					return device, nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			failed := make(chan error, 1)
			if err := committer.SetCallbacks(nil, func(err error) {
				failed <- err
			}); err != nil {
				t.Fatal(err)
			}
			if err := committer.InitializeGeneration(1); err != nil {
				t.Fatal(err)
			}

			batch, err := committer.Begin(1)
			if err != nil {
				t.Fatal(err)
			}
			page, _ := batch.PageBuffer(0)
			page[0] = 1
			if err := batch.SetPage(0, int64(2*pageSize), 1); err != nil {
				t.Fatal(err)
			}
			root, _ := batch.RootBuffer()
			root[0] = 2
			if err := batch.SetRoot(0, 1); err != nil {
				t.Fatal(err)
			}
			if err := batch.Publish(2); err != nil {
				t.Fatal(err)
			}
			waitErr := committer.Wait(2)
			if !errors.Is(waitErr, persistErr) {
				t.Fatalf("Wait = %v, want injected error", waitErr)
			}
			if got := errors.Is(waitErr, ErrCommitOutcomeUnknown); got != test.unknown {
				t.Fatalf("Wait outcome unknown = %v, want %v: %v", got, test.unknown, waitErr)
			}
			if callbackErr := <-failed; !errors.Is(callbackErr, waitErr) {
				t.Fatalf("failure callback = %v, want %v", callbackErr, waitErr)
			}
			if got := committer.DurableGeneration(); got != 1 {
				t.Fatalf("durable generation = %d, want 1", got)
			}
			if begin, beginErr := committer.Begin(0); begin != nil ||
				!errors.Is(beginErr, persistErr) {
				t.Fatalf("Begin after failure = (%v, %v), want sticky error", begin, beginErr)
			}
			if failure := committer.Failure(); !errors.Is(failure, waitErr) {
				t.Fatalf("Failure = %v, want %v", failure, waitErr)
			}
			// A wait for an older, already-durable generation must not hide
			// that this committer is poisoned by a later failed publication.
			if oldWaitErr := committer.Wait(1); !errors.Is(oldWaitErr, waitErr) {
				t.Fatalf("Wait(durable) after failure = %v, want %v", oldWaitErr, waitErr)
			}
			if flushErr := committer.Flush(); !errors.Is(flushErr, waitErr) {
				t.Fatalf("Flush after failure = %v, want %v", flushErr, waitErr)
			}
			if closeErr := committer.Close(); !errors.Is(closeErr, waitErr) {
				t.Fatalf("Close = %v, want %v", closeErr, waitErr)
			}
			if device.commits != 1 {
				t.Fatalf(
					"device commits after persistence failure = %d, want 1",
					device.commits,
				)
			}
		})
	}
}

func TestCommitterWaitAcknowledgesOnlyAfterDurableCallback(t *testing.T) {
	committer, _, _ := newPortableCommitter(t, 2, 0)
	entered := make(chan struct{})
	release := make(chan struct{})
	if err := committer.SetCallbacks(func(generation uint64) {
		if generation != 2 {
			t.Errorf("durable callback generation = %d, want 2", generation)
		}
		close(entered)
		<-release
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := committer.InitializeGeneration(1); err != nil {
		t.Fatal(err)
	}
	publishTestGeneration(
		t, committer, 2, nil, 0, []byte("generation-two-root"),
	)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("durable callback did not start")
	}
	if got := committer.DurableGeneration(); got != 2 {
		t.Fatalf(
			"physical durable generation = %d, want 2",
			got,
		)
	}
	if got := committer.settled.Load(); got != 1 {
		t.Fatalf("callback-settled generation = %d, want 1", got)
	}
	waited := make(chan error, 1)
	go func() { waited <- committer.Wait(2) }()
	select {
	case err := <-waited:
		t.Fatalf("Wait returned before durable callback completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after durable callback")
	}
	if got := committer.DurableGeneration(); got != 2 {
		t.Fatalf("durable generation = %d, want 2", got)
	}
	if err := committer.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCommitterWaitSettlesFailureCallbackWithoutBlockingAdmission(t *testing.T) {
	persistErr := errors.New("injected persistence failure")
	file, err := os.CreateTemp(t.TempDir(), "commit-failure-callback")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	pageSize := os.Getpagesize()
	device := newPhaseFailureDevice(2, pageSize, failDataWrite, persistErr)
	committer, err := newCommitter(
		file,
		DeviceOptions{
			Backend: BackendPortable, BufferCount: 2, BufferSize: pageSize,
		},
		CommitterOptions{
			QueueSlots: 2, MaxPagesPerBatch: 0, GroupLimit: 1,
		},
		func(*os.File, DeviceOptions) (Device, error) {
			return device, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	if err := committer.SetCallbacks(nil, func(err error) {
		if !errors.Is(err, persistErr) {
			t.Errorf("failure callback = %v, want %v", err, persistErr)
		}
		close(entered)
		<-release
	}); err != nil {
		t.Fatal(err)
	}
	if err := committer.InitializeGeneration(1); err != nil {
		t.Fatal(err)
	}
	publishTestGeneration(
		t, committer, 2, nil, 0, []byte("generation-two-root"),
	)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("failure callback did not start")
	}

	if failure := committer.Failure(); !errors.Is(failure, persistErr) {
		t.Fatalf("Failure during callback = %v, want %v", failure, persistErr)
	}
	if batch, beginErr := committer.Begin(0); batch != nil ||
		!errors.Is(beginErr, persistErr) {
		t.Fatalf(
			"Begin during failure callback = (%v, %v), want sticky failure",
			batch, beginErr,
		)
	}
	observed := make(chan error, 1)
	go func() { observed <- committer.Wait(2) }()
	select {
	case err := <-observed:
		t.Fatalf("Wait returned before failure callback settled: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-observed:
		if !errors.Is(err, persistErr) {
			t.Fatalf("settled failure = %v, want %v", err, persistErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after failure callback")
	}
	if err := committer.Close(); !errors.Is(err, persistErr) {
		t.Fatalf("Close = %v, want %v", err, persistErr)
	}
}
