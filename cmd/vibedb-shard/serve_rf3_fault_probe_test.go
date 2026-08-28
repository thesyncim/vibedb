//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/shardservice"
)

// Only the post-pressure observation waits for asynchronously returned socket
// capacity. Every attempt reauthenticates; actual authentication/policy errors
// and response refusals are not retried. No proposal or waiter is retried here.
func rf3FaultProbeAfterWave(ctx context.Context, probe func() (shardservice.ReplicatedMemberState, error)) (shardservice.ReplicatedMemberState, error) {
	if _, bounded := ctx.Deadline(); !bounded {
		return shardservice.ReplicatedMemberState{}, errors.New("post-wave probe requires a deadline")
	}
	for {
		if err := context.Cause(ctx); err != nil {
			return shardservice.ReplicatedMemberState{}, err
		}
		state, err := probe()
		if err == nil || !rf3FaultProbeCapacityClosed(err) {
			return state, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return shardservice.ReplicatedMemberState{}, errors.Join(context.Cause(ctx), err)
		case <-timer.C:
		}
	}
}

func rf3FaultProbeCapacityClosed(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET)
}

func TestRF3FaultProbeAfterWaveKeepsAuthenticationAndDeadline(t *testing.T) {
	t.Run("truncated-handshake", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		calls := 0
		state, err := rf3FaultProbeAfterWave(ctx, func() (shardservice.ReplicatedMemberState, error) {
			calls++
			if calls == 1 {
				return shardservice.ReplicatedMemberState{}, errors.Join(rafttransport.ErrPeerAuthentication, io.EOF)
			}
			return shardservice.ReplicatedMemberState{Applied: 9}, nil
		})
		if err != nil || state.Applied != 9 || calls != 2 {
			t.Fatalf("post-wave observation calls=%d applied=%d: %v", calls, state.Applied, err)
		}
	})
	t.Run("authentication-failure", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		calls := 0
		_, err := rf3FaultProbeAfterWave(ctx, func() (shardservice.ReplicatedMemberState, error) {
			calls++
			return shardservice.ReplicatedMemberState{}, rafttransport.ErrPeerAuthentication
		})
		if !errors.Is(err, rafttransport.ErrPeerAuthentication) || calls != 1 {
			t.Fatalf("authentication rejection retried: calls=%d err=%v", calls, err)
		}
	})
	t.Run("bounded-capacity-wait", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		defer cancel()
		_, err := rf3FaultProbeAfterWave(ctx, func() (shardservice.ReplicatedMemberState, error) {
			return shardservice.ReplicatedMemberState{}, io.EOF
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("lost wait bound: %v", err)
		}
	})
}

func TestRF3FaultProbeAfterWaveWaitsForAuthenticatedCapacity(t *testing.T) {
	fixture, server, _, _ := newRF3FaultWaveTestServer(t, 1, 1, &rf3FaultWaveTestOwner{})
	fixture.group = rf3CommandGroup()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	held, err := fixture.dialNative(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	waitRF3FaultWaveStats(t, server, 1)
	deadline, _ := ctx.Deadline()
	rejections := 0
	state, err := rf3FaultProbeAfterWave(ctx, func() (shardservice.ReplicatedMemberState, error) {
		state, err := fixture.tryProbe(0, time.Until(deadline))
		if rf3FaultProbeCapacityClosed(err) {
			rejections++
			_ = held.Close()
		}
		return state, err
	})
	if err != nil || rejections == 0 || server.Stats().Rejected == 0 || state.Applied != 9 || state.Fence.MemberID != 1 {
		t.Fatalf("capacity was not reclaimed through an authenticated probe: rejections=%d state=%+v err=%v", rejections, state, err)
	}
	waitRF3FaultWaveStats(t, server, 0)
}
