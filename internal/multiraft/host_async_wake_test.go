package multiraft

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

type completionRuntime struct {
	*fakeRuntime
	notify func()
}

func (*completionRuntime) Pipelined() bool             { return true }
func (r *completionRuntime) SetPipelinedWake(f func()) { r.notify = f }

func completionFixture(t testing.TB, count int) (*Host, []*completionRuntime) {
	t.Helper()
	limits := testHostLimits()
	limits.MaxGroups = count
	host, err := NewHost(limits)
	if err != nil {
		t.Fatal(err)
	}
	runtimes := make([]*completionRuntime, count)
	for i := range runtimes {
		r := &completionRuntime{fakeRuntime: newFakeRuntime(byte(i))}
		if err := host.addRuntime(r); err != nil {
			t.Fatal(err)
		}
		runtimes[i] = r
	}
	if _, done, err := host.RunOne(); err != nil || done {
		t.Fatalf("initial idle probe: %t %v", done, err)
	}
	return host, runtimes
}

func TestHostAppendCompletionDoesNotProbeIdleGroups(t *testing.T) {
	host, runtimes := completionFixture(t, 64)
	for range 10 {
		runtimes[17].notify()
	}
	<-host.AsyncNotify()
	host.WakePipelined()
	if _, done, err := host.RunOne(); err != nil || done {
		t.Fatalf("completion idle probe: %t %v", done, err)
	}
	for i, r := range runtimes {
		want := 1
		if i == 17 {
			want++
		}
		if r.driveCalls != want {
			t.Fatalf("group %d probed %d times, want %d", i, r.driveCalls, want)
		}
	}
	// The same group must be eligible for a new edge after the first drain.
	runtimes[17].notify()
	<-host.AsyncNotify()
	host.WakePipelined()
	host.RunOne()
	if runtimes[17].driveCalls != 3 {
		t.Fatal("lost repeated completion")
	}
}

func TestHostAppendCompletionDoesNotReviveRemovedGroup(t *testing.T) {
	host, runtimes := completionFixture(t, 2)
	old := runtimes[0]
	old.notify()
	if err := host.Remove(old.identity.Group); err != nil {
		t.Fatal(err)
	}
	replacement := &completionRuntime{fakeRuntime: newFakeRuntime(0)}
	if err := host.addRuntime(replacement); err != nil {
		t.Fatal(err)
	}
	host.RunOne()
	host.WakePipelined()
	host.RunOne()
	if replacement.driveCalls != 1 || old.driveCalls != 1 {
		t.Fatal("stale edge woke removed or replacement allocation")
	}
	replacement.notify()
	host.WakePipelined()
	host.RunOne()
	if replacement.driveCalls != 2 {
		t.Fatal("replacement completion lost")
	}
}

func TestHostConcurrentAppendCompletionDrain(t *testing.T) {
	host, runtimes := completionFixture(t, 32)
	var completed atomic.Int32
	var workers sync.WaitGroup
	for _, r := range runtimes {
		workers.Go(func() {
			for range 2000 {
				r.notify()
				runtime.Gosched()
			}
			completed.Add(1)
		})
	}
	for completed.Load() != int32(len(runtimes)) {
		host.WakePipelined()
		host.RunOne()
		runtime.Gosched()
	}
	workers.Wait()
	host.WakePipelined()
	host.RunOne()
	// A final distinct edge for every group proves each producer can enqueue
	// again after racing with detach/reset, rather than leaving a stuck flag.
	before := make([]int, len(runtimes))
	for i, r := range runtimes {
		before[i] = r.driveCalls
		r.notify()
	}
	host.WakePipelined()
	host.RunOne()
	for i, r := range runtimes {
		if r.driveCalls != before[i]+1 {
			t.Fatalf("group %d lost final wake", i)
		}
	}
	if host.asyncReady.Load() != nil {
		t.Fatal("completion list retained after drain")
	}
}

func BenchmarkHostSparseAppendCompletion(b *testing.B) {
	for _, count := range []int{1, 16, 64, 256} {
		b.Run(fmt.Sprintf("groups=%d", count), func(b *testing.B) {
			host, runtimes := completionFixture(b, count)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				runtimes[0].notify()
				<-host.AsyncNotify()
				host.WakePipelined()
				if _, done, err := host.RunOne(); err != nil || done {
					b.Fatalf("idle probe: %t %v", done, err)
				}
			}
		})
	}
}
