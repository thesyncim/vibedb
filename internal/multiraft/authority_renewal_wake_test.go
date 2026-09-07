package multiraft

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

var errEnsureAuthorityRound = errors.New("multiraft test: ensure authority round")

type ensureOnlyRuntime struct {
	*fakeRuntime
	ensureCalls int
	ensureErr   error
}

func (runtime *ensureOnlyRuntime) EnsureReadAuthorityRound() error {
	runtime.ensureCalls++
	return runtime.ensureErr
}

type ensurePendingRuntime struct {
	*ensureOnlyRuntime
	pending      bool
	pendingCalls int
	emitPending  bool
}

func (runtime *ensurePendingRuntime) EnsureReadAuthorityRound() error {
	runtime.ensureCalls++
	if runtime.ensureErr != nil {
		return runtime.ensureErr
	}
	if runtime.emitPending {
		runtime.pending = true
	}
	return nil
}

func (runtime *ensurePendingRuntime) AuthorityOutboundPending() bool {
	runtime.pendingCalls++
	return runtime.pending
}

func newEnsureWakeHost[T memberRuntime](t *testing.T, runtime T) (*Host, raftmember.GroupKey) {
	t.Helper()
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	key := runtime.Identity().Group
	if _, done, err := host.RunOne(); err != nil || done || host.runnableLen() != 0 {
		t.Fatalf("initial idle turn = done %v err %v runnable %d", done, err, host.runnableLen())
	}
	t.Cleanup(func() {
		if err := host.Close(); err != nil {
			t.Errorf("close host: %v", err)
		}
	})
	return host, key
}

func TestHostEnsureReadAuthorityRoundNoopDoesNotWakeIdleGroup(t *testing.T) {
	runtime := &ensurePendingRuntime{
		ensureOnlyRuntime: &ensureOnlyRuntime{fakeRuntime: newFakeRuntime(111)},
	}
	host, key := newEnsureWakeHost(t, runtime)
	if err := host.EnsureReadAuthorityRound(key); err != nil {
		t.Fatal(err)
	}
	if runtime.ensureCalls != 1 || runtime.pendingCalls != 1 || host.runnableLen() != 0 {
		t.Fatalf("no-op renewal calls=%d pending=%d runnable=%d; want 1/1/0",
			runtime.ensureCalls, runtime.pendingCalls, host.runnableLen())
	}
}

func TestHostEnsureReadAuthorityRoundWakesPendingAuthorityOutbound(t *testing.T) {
	runtime := &ensurePendingRuntime{
		ensureOnlyRuntime: &ensureOnlyRuntime{fakeRuntime: newFakeRuntime(112)},
		emitPending:       true,
	}
	host, key := newEnsureWakeHost(t, runtime)
	if err := host.EnsureReadAuthorityRound(key); err != nil {
		t.Fatal(err)
	}
	if runtime.ensureCalls != 1 || runtime.pendingCalls != 1 || host.runnableLen() != 1 {
		t.Fatalf("pending renewal calls=%d pending=%d runnable=%d; want 1/1/1",
			runtime.ensureCalls, runtime.pendingCalls, host.runnableLen())
	}
}

func TestHostEnsureReadAuthorityRoundPreservesScheduledWork(t *testing.T) {
	runtime := &ensurePendingRuntime{
		ensureOnlyRuntime: &ensureOnlyRuntime{fakeRuntime: newFakeRuntime(113)},
	}
	host, key := newEnsureWakeHost(t, runtime)
	if err := host.RequestTick(key); err != nil {
		t.Fatal(err)
	}
	queued := host.groups[key].ticks
	if err := host.EnsureReadAuthorityRound(key); err != nil {
		t.Fatal(err)
	}
	if runtime.ensureCalls != 1 || runtime.pendingCalls != 1 || host.runnableLen() != 1 ||
		host.groups[key].ticks != queued {
		t.Fatalf("scheduled renewal calls=%d pending=%d runnable=%d ticks=%d/%d",
			runtime.ensureCalls, runtime.pendingCalls, host.runnableLen(),
			host.groups[key].ticks, queued)
	}
}

func TestHostEnsureReadAuthorityRoundMissingPendingCapabilityKeepsWakeBehavior(t *testing.T) {
	runtime := &ensureOnlyRuntime{fakeRuntime: newFakeRuntime(114)}
	host, key := newEnsureWakeHost(t, runtime)
	if err := host.EnsureReadAuthorityRound(key); err != nil {
		t.Fatal(err)
	}
	if runtime.ensureCalls != 1 || host.runnableLen() != 1 {
		t.Fatalf("legacy renewal calls=%d runnable=%d; want 1/1",
			runtime.ensureCalls, host.runnableLen())
	}
}

func TestHostEnsureReadAuthorityRoundReturnsRuntimeErrorUnchanged(t *testing.T) {
	runtime := &ensurePendingRuntime{
		ensureOnlyRuntime: &ensureOnlyRuntime{
			fakeRuntime: newFakeRuntime(115), ensureErr: errEnsureAuthorityRound,
		},
		pending: true,
	}
	host, key := newEnsureWakeHost(t, runtime)
	if err := host.EnsureReadAuthorityRound(key); !errors.Is(err, errEnsureAuthorityRound) {
		t.Fatalf("ensure error=%v, want %v", err, errEnsureAuthorityRound)
	}
	if runtime.ensureCalls != 1 || runtime.pendingCalls != 0 || host.runnableLen() != 0 {
		t.Fatalf("failed renewal calls=%d pending=%d runnable=%d",
			runtime.ensureCalls, runtime.pendingCalls, host.runnableLen())
	}
}
