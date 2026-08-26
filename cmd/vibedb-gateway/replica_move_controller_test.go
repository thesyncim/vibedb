package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rebalanceexec"
)

type oneReplicaMovePass struct {
	cancel context.CancelFunc
	calls  int
}

func (runner *oneReplicaMovePass) RunPass(context.Context) (rebalanceexec.ControllerPass, error) {
	runner.calls++
	runner.cancel()
	return rebalanceexec.ControllerPass{Discovered: 3, Moves: 2, Advanced: 1}, nil
}

func TestRunReplicaMoveControllerUsesReplicatedPass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runner := &oneReplicaMovePass{cancel: cancel}
	var logs int
	runReplicaMoveController(ctx, runner, time.Hour, func(string, ...any) { logs++ })
	if runner.calls != 1 || logs != 1 {
		t.Fatalf("calls=%d logs=%d", runner.calls, logs)
	}
}

func TestNewGatewayReplicaMoveControllerRejectsMissingAuthorities(t *testing.T) {
	controller, err := newGatewayReplicaMoveController(nil, nil, nil, gatewayReplicaMoveControls{})
	if controller != nil || !errors.Is(err, rebalanceexec.ErrControllerConfig) {
		t.Fatalf("controller=%v err=%v", controller, err)
	}
}
