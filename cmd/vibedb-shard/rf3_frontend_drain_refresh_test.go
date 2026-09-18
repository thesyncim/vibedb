//go:build darwin || linux

package main

import (
	"context"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/frontenddrain"
	"github.com/thesyncim/vibedb/internal/rafttransport"
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

func TestRF3PreferLocalServiceCutReaderFallsBackToRemote(t *testing.T) {
	query := frontenddrain.ServiceCutReadLatestRequest{
		Operation: frontenddrain.ReadLatest, Nonce: [16]byte{1},
		ReceiverNode: rafttransport.NodeID{1}, ReceiverIncarnation: 1,
		ReceiverServiceKeyDigest: [32]byte{2},
	}
	if !query.Valid() {
		t.Fatal("read-latest query must be valid")
	}
	want := frontenddrain.ServiceCut{DirectoryRevision: 7}
	local := &countingRF3ServiceCutReader{err: errRF3FrontendDrainCutRefreshUnavailable}
	remote := &countingRF3ServiceCutReader{cut: want}
	got, err := (rf3PreferLocalServiceCutReader{local: local, remote: remote}).ReadLatestServiceCut(context.Background(), query)
	if err != nil || got.DirectoryRevision != want.DirectoryRevision || local.calls != 1 || remote.calls != 1 {
		t.Fatalf("fallback cut=%+v err=%v local=%d remote=%d", got, err, local.calls, remote.calls)
	}
	localSuccess := &countingRF3ServiceCutReader{cut: frontenddrain.ServiceCut{DirectoryRevision: 9}}
	remoteUnused := &countingRF3ServiceCutReader{cut: want}
	got, err = (rf3PreferLocalServiceCutReader{local: localSuccess, remote: remoteUnused}).ReadLatestServiceCut(context.Background(), query)
	if err != nil || got.DirectoryRevision != 9 || localSuccess.calls != 1 || remoteUnused.calls != 0 {
		t.Fatalf("local cut=%+v err=%v local=%d remote=%d", got, err, localSuccess.calls, remoteUnused.calls)
	}
}

func TestWaitRF3FrontendDrainServiceCutReadyHonorsCallerContext(t *testing.T) {
	ready := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := waitRF3FrontendDrainServiceCutReady(ctx, ready)
	elapsed := time.Since(start)
	if err == nil || elapsed > time.Second {
		t.Fatalf("wait error=%v elapsed=%s, want caller context to expire quickly", err, elapsed)
	}
	close(ready)
	if err := waitRF3FrontendDrainServiceCutReady(context.Background(), ready); err != nil {
		t.Fatalf("ready channel wait: %v", err)
	}
}

type countingRF3ServiceCutReader struct {
	cut   frontenddrain.ServiceCut
	err   error
	calls int
}

func (reader *countingRF3ServiceCutReader) ReadLatestServiceCut(
	context.Context, frontenddrain.ServiceCutReadLatestRequest,
) (frontenddrain.ServiceCut, error) {
	reader.calls++
	return reader.cut, reader.err
}
