//go:build darwin || linux

package main

import (
	"context"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/frontenddrain"
)

type blockingRF3ServiceCutReader struct {
	entered chan struct{}
}

func (reader *blockingRF3ServiceCutReader) ReadLatestServiceCut(
	ctx context.Context, _ frontenddrain.ServiceCutReadLatestRequest,
) (frontenddrain.ServiceCut, error) {
	select {
	case reader.entered <- struct{}{}:
	case <-ctx.Done():
		return frontenddrain.ServiceCut{}, context.Cause(ctx)
	}
	<-ctx.Done()
	return frontenddrain.ServiceCut{}, context.Cause(ctx)
}

func TestRF3FrontendDrainServiceCutRefreshCancellationKeepsReadinessClosed(t *testing.T) {
	owner := &rf3FaultWaveTestOwner{}
	fixture, server, _, _ := newRF3FaultWaveTestServer(t, 1, 1, owner)
	reader := &blockingRF3ServiceCutReader{entered: make(chan struct{}, 1)}
	ready := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runRF3FrontendDrainServiceCutRefresh(ctx, reader, fixture.profiles[0], 1, server, time.Hour, ready)
	}()
	select {
	case <-reader.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not issue its canonical source read")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || err != context.Canceled {
			t.Fatalf("refresh cancellation error=%v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not stop after cancellation")
	}
	select {
	case <-ready:
		t.Fatal("readiness opened before a canonical service cut was installed")
	default:
	}
}
