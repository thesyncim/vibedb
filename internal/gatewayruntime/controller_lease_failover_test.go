package gatewayruntime

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/controllerlease"
	"github.com/thesyncim/vibedb/internal/splitcontroller"
)

func TestServingSplitControllerGatedByControllerLeaseFailover(t *testing.T) {
	store, err := controllerlease.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	e1, err := controllerlease.NewElector(store, controllerlease.Config{
		Key:           "vibedb/controller/split",
		Holder:        "frontend-1",
		LeaseTTL:      120 * time.Millisecond,
		RenewInterval: 30 * time.Millisecond,
		RetryInterval: 15 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	e2, err := controllerlease.NewElector(store, controllerlease.Config{
		Key:           "vibedb/controller/split",
		Holder:        "frontend-2",
		LeaseTTL:      120 * time.Millisecond,
		RenewInterval: 30 * time.Millisecond,
		RetryInterval: 15 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Pre-elect e1 so e2 starts strictly as standby.
	if ok, epoch, err := e1.Step(ctx); err != nil || !ok || epoch != 1 {
		t.Fatalf("e1 initial Step = (%v, %d, %v)", ok, epoch, err)
	}

	go func() { _ = e1.Run(ctx) }()
	go func() { _ = e2.Run(ctx) }()

	var e1Passes, e2Passes atomic.Int64
	runGated := func(el *controllerlease.Elector, counter *atomic.Int64) {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		runServingSplitControllerLoop(ctx, func(context.Context) (splitcontroller.ControllerPass, error) {
			if _, ok := el.Lease(); !ok {
				return splitcontroller.ControllerPass{}, nil
			}
			counter.Add(1)
			return splitcontroller.ControllerPass{}, nil
		}, func(c context.Context) bool {
			select {
			case <-c.Done():
				return false
			case <-ticker.C:
				return true
			}
		}, func(string, ...any) {})
	}
	go runGated(e1, &e1Passes)
	go runGated(e2, &e2Passes)

	// Wait until e1 has executed at least 3 passes; e2 must still have 0.
	deadline := time.Now().Add(2 * time.Second)
	for e1Passes.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if e1Passes.Load() < 3 {
		t.Fatalf("leader e1 passes = %d, want >= 3", e1Passes.Load())
	}
	if got := e2Passes.Load(); got != 0 {
		t.Fatalf("standby e2 executed %d passes while e1 held lease, want 0", got)
	}

	// Release e1 gracefully (simulating frontend drain); e2 must acquire epoch >= 2
	// and start executing passes promptly.
	if err := e1.Release(ctx); err != nil {
		t.Fatalf("e1.Release: %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for e2Passes.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if e2Passes.Load() < 2 {
		t.Fatalf("after failover, e2 passes = %d, want >= 2", e2Passes.Load())
	}
	cancel()
}
