package raftservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
)

// This Host stays runnable even after the selected group's work finishes,
// like a lane with an unrelated hot group. It must never need to become idle
// for another group's SQL read or owner cancellation to make progress.
type alwaysRunnableReadHost struct {
	*deferredReadOwnerHost
	turns      int
	admittedAt int
}

func (h *alwaysRunnableReadHost) RunOne() (multiraft.Progress, bool, error) {
	h.turns++
	if h.turns == 1 {
		close(h.firstRunEntered)
		<-h.releaseFirstRun
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.readyPending {
		// The first admission attempt is refused. A later durability completion
		// clears Ready, while unrelated protocol work keeps the lane runnable.
		if h.readCalls != 0 {
			h.readyPending = false
		}
	}
	p := multiraft.Progress{Kind: multiraft.ProgressReady, ReadyKind: raftmember.DrivePersisted}
	if len(h.readContext) != 0 {
		p.ReadOutcomes = []raftmodel.ReadOutcome{{Barrier: raftmodel.ReadBarrier{
			Context: append([]byte(nil), h.readContext...), Index: 7, Term: 2, Incarnation: 1,
		}}}
		h.readContext = nil
	}
	return p, true, nil
}
func (h *alwaysRunnableReadHost) ReadIndex(g raftmember.GroupKey, value []byte) error {
	h.admittedAt = h.turns
	return h.deferredReadOwnerHost.ReadIndex(g, value)
}

func TestOwnerReadAndCancellationProgressWhileHostNeverIdle(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		t.Run(map[bool]string{false: "queued", true: "deferred"}[deferred], func(t *testing.T) {
			group, command, base, owner, source := newDeferredReadOwnerFixture()
			base.readyPending = deferred
			host := &alwaysRunnableReadHost{deferredReadOwnerHost: base}
			owner.host = host
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- owner.Run(ctx) }()
			<-host.firstRunEntered
			result := make(chan error, 1)
			var cut LinearizablePointReadCut
			defer cut.Close()
			go func() {
				result <- owner.ReadLinearizablePointInto(ctx, deferredLinearizablePointRequest(group, command), &cut)
			}()
			// Ensure admission precedes the Host's continuous progress window.
			for {
				owner.mu.Lock()
				queued := owner.ingressItems
				owner.mu.Unlock()
				if queued != 0 {
					break
				}
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
				time.Sleep(time.Millisecond)
			}
			close(host.releaseFirstRun)
			err := <-result
			cancel()
			if stop := <-done; !errors.Is(stop, context.Canceled) && !errors.Is(stop, context.DeadlineExceeded) {
				t.Fatal(stop)
			}
			if err != nil {
				t.Fatalf("read starved behind continuous Ready: %v", err)
			}
			if cut.Source() != source || cut.minimumApplied != 7 {
				t.Fatal("read lost its cut")
			}
			maxTurns := ownerProgressQuantum
			if deferred {
				maxTurns *= 2
			}
			if host.admittedAt > maxTurns {
				t.Fatalf("read admitted at turn %d, bound %d", host.admittedAt, maxTurns)
			}
			cut.Close()
			if owner.ingressItems != 0 || owner.pendingReadItems != 0 {
				t.Fatal("admission leaked")
			}
		})
	}
}

// Both channels are ready before the first progress quantum. Async wakeups and
// clock offers each receive one turn; they do not compete in a random select.
type fairnessSourcesHost struct {
	*alwaysRunnableReadHost
	wakeAt, tickAt int
	cancel         context.CancelFunc
}

func (h *fairnessSourcesHost) WakePipelined() { h.wakeAt = h.turns }
func (h *fairnessSourcesHost) RequestTick(raftmember.GroupKey) error {
	h.tickAt = h.turns
	h.cancel()
	return nil
}
func TestOwnerServicesAsyncAndTickInSameBusyQuantum(t *testing.T) {
	_, _, base, owner, _ := newDeferredReadOwnerFixture()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	host := &fairnessSourcesHost{alwaysRunnableReadHost: &alwaysRunnableReadHost{deferredReadOwnerHost: base}, cancel: cancel}
	owner.host = host
	base.async = make(chan struct{}, 1)
	base.async <- struct{}{}
	pulse := make(chan struct{}, 1)
	pulse <- struct{}{}
	owner.pulse = pulse
	close(base.releaseFirstRun)
	if err := owner.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	if host.wakeAt != ownerProgressQuantum || host.tickAt != ownerProgressQuantum {
		t.Fatalf("wake at %d, tick at %d; both must run at %d", host.wakeAt, host.tickAt, ownerProgressQuantum)
	}
}

// New timer work must not replenish the Ready drain needed by a queued control.
// Otherwise the control blocks peer ingress while timers continue elections.
type controlDrainTickHost struct {
	*alwaysRunnableReadHost
	work, controlAt, ticksAfterControl int
	asyncAt                            int
	pulse                              chan struct{}
	cancel                             context.CancelFunc
}

func (h *controlDrainTickHost) RunOne() (multiraft.Progress, bool, error) {
	h.turns++
	if h.turns == 1 {
		close(h.firstRunEntered)
		<-h.releaseFirstRun
	}
	if h.turns >= 1024 {
		h.cancel()
	}
	if h.work == 0 {
		return multiraft.Progress{}, false, nil
	}
	h.work--
	return multiraft.Progress{Kind: multiraft.ProgressReady, ReadyKind: raftmember.DrivePersisted}, true, nil
}
func (h *controlDrainTickHost) WakePipelined() { h.asyncAt = h.turns }

func (h *controlDrainTickHost) RequestTick(raftmember.GroupKey) error {
	if h.controlAt != 0 {
		h.ticksAfterControl++
		h.cancel()
		return nil
	}
	h.work += 2 * ownerProgressQuantum
	select {
	case h.pulse <- struct{}{}:
	default:
	}
	return nil
}
func (h *controlDrainTickHost) RequestCampaign(raftmember.GroupKey) error {
	if h.work != 0 {
		return errors.New("campaign crossed pending Ready")
	}
	h.controlAt = h.turns
	return nil
}
func TestOwnerControlDrainStopsNewTicksThenResumesThem(t *testing.T) {
	group, _, base, owner, _ := newDeferredReadOwnerFixture()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	pulse := make(chan struct{}, 1)
	pulse <- struct{}{}
	host := &controlDrainTickHost{alwaysRunnableReadHost: &alwaysRunnableReadHost{deferredReadOwnerHost: base}, work: 4 * ownerProgressQuantum, pulse: pulse, cancel: cancel}
	base.async = make(chan struct{}, 1)
	base.async <- struct{}{}
	owner.host, owner.pulse = host, pulse
	done := make(chan error, 1)
	go func() { done <- owner.Run(ctx) }()
	<-host.firstRunEntered
	reply := make(chan ownerReply, 1)
	if err := owner.publish(ownerRequest{kind: requestCampaign, group: group, reply: reply}); err != nil {
		t.Fatal(err)
	}
	close(host.releaseFirstRun)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run: %v", err)
	}
	if host.controlAt == 0 || host.controlAt > 4*ownerProgressQuantum+1 {
		t.Fatalf("control starved: admitted at %d after %d turns", host.controlAt, host.turns)
	}
	if host.asyncAt != ownerProgressQuantum {
		t.Fatalf("async completion starved at control barrier: %d", host.asyncAt)
	}
	if host.ticksAfterControl == 0 {
		t.Fatal("ticks did not resume after control")
	}
	select {
	case r := <-reply:
		if r.err != nil {
			t.Fatal(r.err)
		}
	default:
		t.Fatal("control completion lost")
	}
	if owner.ingressItems != 0 {
		t.Fatal("control ingress charge leaked")
	}
}
