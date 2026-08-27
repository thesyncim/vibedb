package raftservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	raft "go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type isolationInput struct {
	data []byte
	read bool
}

type isolationEvent struct {
	index uint64
	data  []byte
	read  bool
}

// Real Raft cores behind the Owner's bounded Host port. MemoryStorage makes
// this a deterministic scheduling/quorum regression, not a durable RF3 proof.
type isolationRaftHost struct {
	ownerHost
	mu       sync.Mutex
	raw      *raft.RawNode
	storage  *raft.MemoryStorage
	input    *isolationInput
	outbox   [32]*pb.Message
	count    int
	maxCount int
	events   chan isolationEvent
	runs     atomic.Uint64
}

func newIsolationRaftHost(t *testing.T, id uint64) *isolationRaftHost {
	t.Helper()
	storage := raft.NewMemoryStorage()
	one := uint64(1)
	if err := storage.ApplySnapshot(&pb.Snapshot{Metadata: &pb.SnapshotMetadata{
		Index: &one, Term: &one, ConfState: &pb.ConfState{Voters: []uint64{1, 2, 3}},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := storage.SetHardState(&pb.HardState{Term: &one, Commit: &one}); err != nil {
		t.Fatal(err)
	}
	config := raftmodel.NewConfig(id, storage, 1)
	raw, err := raft.NewRawNode(&config)
	if err != nil {
		t.Fatal(err)
	}
	return &isolationRaftHost{raw: raw, storage: storage, events: make(chan isolationEvent, 128)}
}

func (host *isolationRaftHost) RequestCampaign(raftmember.GroupKey) error {
	host.mu.Lock()
	defer host.mu.Unlock()
	return host.raw.Campaign()
}

func (host *isolationRaftHost) RequestTick(raftmember.GroupKey) error {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.raw.Tick()
	return nil
}

func (host *isolationRaftHost) AdoptMessage(_ raftmember.GroupKey, message *pb.Message) error {
	host.mu.Lock()
	defer host.mu.Unlock()
	return host.raw.Step(message)
}

func (*isolationRaftHost) Close() error { return nil }

func (host *isolationRaftHost) PopOutbound() (raftmember.OutboundMessage, bool) {
	host.mu.Lock()
	defer host.mu.Unlock()
	if host.count == 0 {
		return raftmember.OutboundMessage{}, false
	}
	message := host.outbox[0]
	copy(host.outbox[:], host.outbox[1:host.count])
	host.count--
	host.outbox[host.count] = nil
	return raftmember.OutboundMessage{Group: peerServerTestGroup(), Message: message}, true
}

func (host *isolationRaftHost) RunOne() (multiraft.Progress, bool, error) {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.runs.Add(1)
	if input := host.input; input != nil {
		host.input = nil
		if input.read {
			host.raw.ReadIndex(input.data)
		} else if err := host.raw.Propose(input.data); err != nil {
			return multiraft.Progress{}, false, err
		}
	}
	if !host.raw.HasReady() {
		return multiraft.Progress{}, false, nil
	}
	ready := host.raw.Ready()
	if host.count+len(ready.Messages) > len(host.outbox) {
		return multiraft.Progress{}, false, errors.New("test fixed outbox exhausted")
	}
	if !raft.IsEmptyHardState(ready.HardState) {
		if err := host.storage.SetHardState(ready.HardState); err != nil {
			return multiraft.Progress{}, false, err
		}
	}
	if err := host.storage.Append(ready.Entries); err != nil {
		return multiraft.Progress{}, false, err
	}
	for _, entry := range ready.CommittedEntries {
		host.events <- isolationEvent{index: entry.GetIndex(), data: bytes.Clone(entry.Data)}
	}
	for _, read := range ready.ReadStates {
		host.events <- isolationEvent{index: read.Index, data: bytes.Clone(read.RequestCtx), read: true}
	}
	for _, message := range ready.Messages {
		host.outbox[host.count] = proto.Clone(message).(*pb.Message)
		host.count++
	}
	host.maxCount = max(host.maxCount, host.count)
	host.raw.Advance(ready)
	return multiraft.Progress{}, true, nil
}

type isolationRaftSink struct {
	owner    *Owner
	follower *isolationRaftHost
	rejected atomic.Uint64
	accepted atomic.Uint64
}

func (sink *isolationRaftSink) Send(outbound raftmember.OutboundMessage) error {
	if outbound.Message.GetTo() == 3 {
		sink.rejected.Add(1)
		return rafttransport.ErrBackpressure
	}
	if outbound.Message.GetTo() != 2 {
		return errors.New("unexpected Raft destination")
	}
	sink.accepted.Add(1)
	if err := sink.follower.AdoptMessage(outbound.Group, proto.Clone(outbound.Message).(*pb.Message)); err != nil {
		return err
	}
	if _, _, err := sink.follower.RunOne(); err != nil {
		return err
	}
	for {
		response, ok := sink.follower.PopOutbound()
		if !ok {
			return nil
		}
		if response.Message.GetTo() != 1 {
			return errors.New("unexpected follower destination")
		}
		if err := sink.owner.TryInbound(rafttransport.Inbound{Group: response.Group, Message: response.Message}); err != nil {
			return err
		}
	}
}

func TestOwnerRejectedDestinationDoesNotBlockHealthyRaftQuorum(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	leader, follower := newIsolationRaftHost(t, 1), newIsolationRaftHost(t, 2)
	pulse := make(chan struct{}, 1)
	sink := &isolationRaftSink{follower: follower}
	owner := &Owner{host: leader, groups: []raftmember.GroupKey{peerServerTestGroup()},
		pulse: pulse, outbound: sink, ingress: make(chan ownerRequest, 32),
		ready: make(chan struct{}), done: make(chan struct{}),
		limits: Limits{MaxIngressItems: 32, MaxIngressBytes: 64 << 10, MaxPendingOutboundBytes: 64 << 10}}
	sink.owner = owner
	var runErr error
	stopped := make(chan struct{})
	go func() { runErr = owner.Run(ctx); close(stopped) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
			if !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
				t.Errorf("owner stopped: %v", runErr)
			}
		case <-time.After(time.Second):
			t.Error("owner cancellation leaked")
		}
	})
	select {
	case <-owner.ready:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := owner.Campaign(ctx, peerServerTestGroup()); err != nil {
		t.Fatal(err)
	}
	wait := func(data []byte, read bool) uint64 {
		t.Helper()
		for {
			select {
			case event := <-leader.events:
				if event.read == read && bytes.Equal(event.data, data) && event.index > 1 {
					return event.index
				}
			case <-stopped:
				t.Fatalf("owner stopped before settlement: %v", runErr)
			case <-ctx.Done():
				t.Fatalf("healthy quorum stalled: %v", ctx.Err())
			}
		}
	}
	wait(nil, false) // The actual newly elected leader's committed no-op.
	for ordinal := byte(1); ordinal <= 32; ordinal++ {
		command := []byte{ordinal}
		leader.mu.Lock()
		leader.input = &isolationInput{data: command}
		leader.mu.Unlock()
		pulse <- struct{}{}
		committed := wait(command, false)
		request := []byte{0xff, ordinal}
		leader.mu.Lock()
		leader.input = &isolationInput{data: request, read: true}
		leader.mu.Unlock()
		pulse <- struct{}{}
		if index := wait(request, true); index < committed {
			t.Fatalf("ReadIndex %d precedes committed proposal %d", index, committed)
		}
	}
	if sink.rejected.Load() < 32 || sink.accepted.Load() < 32 {
		t.Fatal("fixture did not sustain isolated-peer pressure alongside healthy traffic")
	}
	leader.mu.Lock()
	maxCount := leader.maxCount
	leader.mu.Unlock()
	if maxCount > 32 || leader.runs.Load() > 4096 {
		t.Fatalf("unbounded scheduling: outbox=%d runs=%d", maxCount, leader.runs.Load())
	}
	before := leader.runs.Load()
	time.Sleep(20 * time.Millisecond)
	if delta := leader.runs.Load() - before; delta > 32 {
		t.Fatalf("owner busy-looped without a pulse/input: %d iterations", delta)
	}
}

type outboundErrorSink struct {
	err   error
	calls int
}

func (sink *outboundErrorSink) Send(raftmember.OutboundMessage) error {
	sink.calls++
	return sink.err
}

func TestOwnerOutboundWrappedBackpressureRemainsFatal(t *testing.T) {
	for _, failure := range []error{fmt.Errorf("transport fault: %w", rafttransport.ErrBackpressure),
		errors.Join(rafttransport.ErrBackpressure, errors.New("transport failed")), errors.New("closed transport")} {
		t.Run(failure.Error(), func(t *testing.T) {
			sink := &outboundErrorSink{err: failure}
			host := &tickPressureHost{outbound: tickPressureMessage()}
			owner := newTickPressureOwner(host, nil, sink)
			if err := owner.Run(t.Context()); !errors.Is(err, failure) || sink.calls != 1 {
				t.Fatalf("fatal send result=%v calls=%d", err, sink.calls)
			}
		})
	}
}

func TestOwnerOutboundSnapshotNeverEntersOrdinaryLossPath(t *testing.T) {
	for _, snapshotType := range []bool{false, true} {
		t.Run(fmt.Sprint(snapshotType), func(t *testing.T) {
			message := tickPressureMessage()
			if snapshotType {
				message.Type = pb.MsgSnap.Enum()
			} else {
				message.Snapshot = &pb.Snapshot{}
			}
			sink := &outboundErrorSink{err: rafttransport.ErrBackpressure}
			owner := newTickPressureOwner(&tickPressureHost{outbound: message}, nil, sink)
			if err := owner.Run(t.Context()); !errors.Is(err, ErrInvalidOwner) || sink.calls != 0 {
				t.Fatalf("snapshot result=%v sends=%d", err, sink.calls)
			}
		})
	}
}
