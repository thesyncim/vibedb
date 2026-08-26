package main

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

func TestNewGatewayServingSplitRuntimeRequiresAuthenticatedManifestComposition(t *testing.T) {
	if runtime, err := newGatewayServingSplitRuntime(gatewayServingSplitOptions{}); runtime != nil ||
		!errors.Is(err, splitcontroller.ErrControllerTrigger) {
		t.Fatalf("runtime=%#v err=%v", runtime, err)
	}
}
