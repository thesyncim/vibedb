package gatewayruntime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

type emptyGatewaySplitDirectory struct {
	once   sync.Once
	called chan struct{}
}

func (*emptyGatewaySplitDirectory) Read(context.Context) (*gateway.Snapshot, error) {
	return nil, errors.New("unexpected catalog read for an empty directory")
}

func (directory *emptyGatewaySplitDirectory) ReadOperationIDs(context.Context) ([][32]byte, error) {
	directory.once.Do(func() { close(directory.called) })
	return nil, nil
}

func (*emptyGatewaySplitDirectory) ReadOperation(
	context.Context, [32]byte,
) (gateway.ReplicatedOperationRecord, error) {
	return gateway.ReplicatedOperationRecord{}, errors.New("unexpected operation read")
}

func TestRunServingSplitControllerUsesDirectEmptyDirectoryPass(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	directory := &emptyGatewaySplitDirectory{called: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runServingSplitController(
			ctx, directory, new(splitcontroller.ControllerService), time.Hour,
			func(string, ...any) {},
		)
	}()
	select {
	case <-directory.called:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("direct controller pass did not read the replicated operation directory")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("serving split controller did not stop after cancellation")
	}
}

func TestServingSplitControllerWaitsWithoutDurableProgress(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	passes := make(chan int, 2)
	waiting := make(chan struct{}, 1)
	tick := make(chan struct{})
	done := make(chan struct{})
	passCount := 0
	go func() {
		defer close(done)
		runServingSplitControllerLoop(ctx, func(context.Context) (splitcontroller.ControllerPass, error) {
			passCount++
			passes <- passCount
			if passCount == 1 {
				return splitcontroller.ControllerPass{Discovered: 1, Triggered: 1}, nil
			}
			cancel()
			return splitcontroller.ControllerPass{}, nil
		}, func(ctx context.Context) bool {
			waiting <- struct{}{}
			select {
			case <-ctx.Done():
				return false
			case <-tick:
				return true
			}
		}, func(string, ...any) {})
	}()

	select {
	case got := <-passes:
		if got != 1 {
			t.Fatalf("first pass index=%d", got)
		}
	case <-time.After(time.Second):
		t.Fatal("controller did not start its first pass")
	}
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("no-progress pass did not wait for its cadence signal")
	}
	select {
	case got := <-passes:
		t.Fatalf("no-progress pass ran again before the cadence signal: pass=%d", got)
	default:
	}
	tick <- struct{}{}
	select {
	case got := <-passes:
		if got != 2 {
			t.Fatalf("second pass index=%d", got)
		}
	case <-time.After(time.Second):
		t.Fatal("controller did not resume after the cadence signal")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("controller loop did not stop after cancellation")
	}
}

func TestServingSplitControllerContinuesImmediatelyOnlyAfterProgress(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	passes := make(chan int, 2)
	waiting := make(chan struct{}, 1)
	done := make(chan struct{})
	passCount := 0
	go func() {
		defer close(done)
		runServingSplitControllerLoop(ctx, func(context.Context) (splitcontroller.ControllerPass, error) {
			passCount++
			passes <- passCount
			if passCount == 1 {
				return splitcontroller.ControllerPass{Discovered: 1, Triggered: 1, Progressed: 1}, nil
			}
			cancel()
			return splitcontroller.ControllerPass{Discovered: 1, Triggered: 1, Progressed: 1}, nil
		}, func(ctx context.Context) bool {
			waiting <- struct{}{}
			<-ctx.Done()
			return false
		}, func(string, ...any) {})
	}()

	select {
	case got := <-passes:
		if got != 1 {
			t.Fatalf("first pass index=%d", got)
		}
	case <-time.After(time.Second):
		t.Fatal("controller did not start its first pass")
	}
	select {
	case got := <-passes:
		if got != 2 {
			t.Fatalf("second pass index=%d", got)
		}
	case <-time.After(time.Second):
		t.Fatal("positive progress did not continue immediately")
	}
	select {
	case <-waiting:
		t.Fatal("controller waited despite durable progress")
	default:
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("controller loop did not stop after cancellation")
	}
}

func TestNewGatewayServingSplitRuntimeRequiresAuthenticatedManifestComposition(t *testing.T) {
	if runtime, err := newGatewayServingSplitRuntime(gatewayServingSplitOptions{}); runtime != nil ||
		!errors.Is(err, splitcontroller.ErrControllerTrigger) {
		t.Fatalf("runtime=%#v err=%v", runtime, err)
	}
}
