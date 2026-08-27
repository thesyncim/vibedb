package raftservice

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	pb "go.etcd.io/raft/v3/raftpb"
)

// Model only the bounded Host ownership seam; the corresponding multiraft
// test exercises the real queue and exact counters. All mutation is serialized
// by Owner.Run, with a mutex solely for test observations.
type tickPressureHost struct {
	ownerHost
	mu                  sync.Mutex
	outbound            *pb.Message
	message             *pb.Message
	ticks, appliedTicks int
	appliedMessage      *pb.Message
	tickErr             error
	busyGroup           raftmember.GroupKey
	attempts            chan error
	drained             chan struct{}
}

func (host *tickPressureHost) RequestTick(group raftmember.GroupKey) error {
	host.mu.Lock()
	err := host.tickErr
	if group == host.busyGroup {
		err = multiraft.ErrGroupBusy
	}
	if err == nil && host.ticks == 1 {
		err = multiraft.ErrQueueFull
	}
	if err == nil {
		host.ticks++
	}
	host.mu.Unlock()
	host.attempts <- err
	return err
}

func (host *tickPressureHost) AdoptMessage(_ raftmember.GroupKey, message *pb.Message) error {
	host.mu.Lock()
	defer host.mu.Unlock()
	if host.message != nil {
		return multiraft.ErrQueueFull
	}
	host.message = message
	return nil
}

func (host *tickPressureHost) PopOutbound() (raftmember.OutboundMessage, bool) {
	host.mu.Lock()
	defer host.mu.Unlock()
	message := host.outbound
	host.outbound = nil
	return raftmember.OutboundMessage{Message: message}, message != nil
}

func (host *tickPressureHost) RunOne() (multiraft.Progress, bool, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	if host.ticks == 0 && host.message == nil {
		return multiraft.Progress{}, false, nil
	}
	host.appliedTicks += host.ticks
	if host.message != nil {
		host.appliedMessage = host.message
	}
	host.ticks, host.message = 0, nil
	host.drained <- struct{}{}
	return multiraft.Progress{}, true, nil
}

func (*tickPressureHost) Close() error { return nil }

type tickPressureSink struct {
	blocked  atomic.Bool
	started  chan struct{}
	once     sync.Once
	expected *pb.Message
	accepted chan *pb.Message
}

func (sink *tickPressureSink) Send(message raftmember.OutboundMessage) error {
	if message.Message != sink.expected {
		return errors.New("outbound ownership changed")
	}
	sink.once.Do(func() { close(sink.started) })
	if sink.blocked.Load() {
		return rafttransport.ErrBackpressure
	}
	sink.accepted <- message.Message
	return nil
}

func newTickPressureOwner(host *tickPressureHost, pulse <-chan struct{}, sink OutboundSink) *Owner {
	return &Owner{host: host, groups: []raftmember.GroupKey{peerServerTestGroup()},
		pulse: pulse, outbound: sink, ingress: make(chan ownerRequest, 2),
		ready: make(chan struct{}), done: make(chan struct{}),
		limits: Limits{MaxIngressItems: 2, MaxIngressBytes: 4096, MaxPendingOutboundBytes: 4096}}
}

func tickPressureMessage() *pb.Message {
	from, to, term := uint64(1), uint64(2), uint64(3)
	return &pb.Message{Type: pb.MsgHeartbeat.Enum(), From: &from, To: &to, Term: &term}
}

func TestOwnerRejectedTimerOffersPreserveBoundedMessagesAndResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	outbound := tickPressureMessage()
	host := &tickPressureHost{outbound: outbound, attempts: make(chan error, 1), drained: make(chan struct{}, 2)}
	sink := &tickPressureSink{expected: outbound, started: make(chan struct{}), accepted: make(chan *pb.Message, 1)}
	sink.blocked.Store(true)
	pulse := make(chan struct{})
	owner := newTickPressureOwner(host, pulse, sink)
	done := make(chan error, 1)
	go func() { done <- owner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("owner did not stop")
		}
	})
	select {
	case <-sink.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	inbound := tickPressureMessage()
	if err := owner.HandleInbound(ctx, rafttransport.Inbound{Group: peerServerTestGroup(), Message: inbound}); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 64; index++ {
		select {
		case pulse <- struct{}{}:
		case err := <-done:
			t.Fatalf("owner stopped on timer pressure: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		select {
		case err := <-host.attempts:
			if index == 0 && err != nil || index != 0 && err != multiraft.ErrQueueFull {
				t.Fatalf("tick %d: %v", index, err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	rejected := tickPressureMessage()
	if err := owner.HandleInbound(ctx, rafttransport.Inbound{Group: peerServerTestGroup(), Message: rejected}); err != multiraft.ErrQueueFull {
		t.Fatalf("full inbound was acknowledged: %v", err)
	}
	host.mu.Lock()
	preserved := host.ticks == 1 && host.appliedTicks == 0 && host.message == inbound && host.appliedMessage == nil
	host.mu.Unlock()
	if !preserved {
		t.Fatal("rejected pulse changed already-admitted input ownership")
	}
	sink.blocked.Store(false)
	select {
	case pulse <- struct{}{}:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-host.attempts:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case got := <-sink.accepted:
		if got != outbound {
			t.Fatal("wrong outbound delivered")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-host.drained:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	host.mu.Lock()
	resumed := host.appliedTicks == 1 && host.appliedMessage == inbound && host.ticks == 0 && host.message == nil
	host.mu.Unlock()
	if !resumed {
		t.Fatal("resume lost input or manufactured a tick catch-up burst")
	}
}

func TestOwnerOnlyOmitsExactRejectedTimerSentinels(t *testing.T) {
	for _, refusal := range []error{multiraft.ErrGroupBusy, multiraft.ErrQueueFull,
		multiraft.ErrGroupNotFound, multiraft.ErrHostClosed,
		errors.Join(multiraft.ErrGroupFaulted, multiraft.ErrQueueFull),
		errors.Join(multiraft.ErrGroupFaulted, multiraft.ErrGroupBusy)} {
		t.Run(refusal.Error(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			host := &tickPressureHost{tickErr: refusal, attempts: make(chan error, 1), drained: make(chan struct{}, 1)}
			pulse := make(chan struct{}, 1)
			owner := newTickPressureOwner(host, pulse, nil)
			done := make(chan error, 1)
			go func() { done <- owner.Run(ctx) }()
			pulse <- struct{}{}
			select {
			case <-host.attempts:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			omitted := refusal == multiraft.ErrGroupBusy || refusal == multiraft.ErrQueueFull
			if omitted {
				// A second offer proves the loop continued; no sleeps or false
				// assertion based on a not-yet-scheduled fatal return.
				pulse <- struct{}{}
				select {
				case <-host.attempts:
				case err := <-done:
					t.Fatalf("rejected timer killed owner: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				cancel()
			}
			select {
			case err := <-done:
				if omitted && !errors.Is(err, context.Canceled) || !omitted && !errors.Is(err, refusal) {
					t.Fatalf("owner result: %v", err)
				}
			case <-ctx.Done():
				if !omitted {
					t.Fatal("fatal timer error was hidden")
				}
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Fatal("owner cancellation leaked")
				}
			}
		})
	}
}

func TestOwnerRejectedTimerDoesNotSuppressOtherGroups(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	busy := peerServerTestGroup()
	healthy := busy
	healthy.GroupID[0]++
	host := &tickPressureHost{busyGroup: busy, attempts: make(chan error, 2), drained: make(chan struct{}, 1)}
	pulse := make(chan struct{}, 1)
	owner := newTickPressureOwner(host, pulse, nil)
	owner.groups = []raftmember.GroupKey{busy, healthy}
	done := make(chan error, 1)
	go func() { done <- owner.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("owner did not stop")
		}
	})
	pulse <- struct{}{}
	for _, want := range []error{multiraft.ErrGroupBusy, nil} {
		select {
		case err := <-host.attempts:
			if err != want {
				t.Fatalf("group timer offer: %v, want %v", err, want)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	select {
	case <-host.drained:
	case <-ctx.Done():
		t.Fatal("healthy group timer did not progress")
	}
}
