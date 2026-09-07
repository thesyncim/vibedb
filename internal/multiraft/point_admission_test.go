package multiraft

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type pointAdmissionRuntime struct {
	*fakeRuntime
	token          raftauthority.AuthorityToken
	tokenErr       error
	ensureErr      error
	emitPending    bool
	pending        bool
	ensureCalls    int
	pendingCalls   int
	tokenCalls     int
	lockProbe      func() bool
	statusUnlocked int
	tokenUnlocked  int
	ensureUnlocked int
}

func (runtime *pointAdmissionRuntime) Status() (raftmember.RuntimeStatus, error) {
	if runtime.lockProbe != nil && !runtime.lockProbe() {
		runtime.statusUnlocked++
	}
	return runtime.fakeRuntime.Status()
}

func (runtime *pointAdmissionRuntime) ReadAuthorityToken() (raftauthority.AuthorityToken, error) {
	runtime.tokenCalls++
	if runtime.lockProbe != nil && !runtime.lockProbe() {
		runtime.tokenUnlocked++
	}
	return runtime.token, runtime.tokenErr
}

func (runtime *pointAdmissionRuntime) EnsureReadAuthorityRound() error {
	runtime.ensureCalls++
	if runtime.lockProbe != nil && !runtime.lockProbe() {
		runtime.ensureUnlocked++
	}
	if runtime.ensureErr != nil {
		return runtime.ensureErr
	}
	if runtime.emitPending {
		runtime.pending = true
	}
	return nil
}

func (runtime *pointAdmissionRuntime) AuthorityOutboundPending() bool {
	runtime.pendingCalls++
	return runtime.pending
}

type pointAdmissionNoPendingRuntime struct {
	*fakeRuntime
	token       raftauthority.AuthorityToken
	ensureCalls int
}

func (runtime *pointAdmissionNoPendingRuntime) ReadAuthorityToken() (raftauthority.AuthorityToken, error) {
	return runtime.token, nil
}

func (runtime *pointAdmissionNoPendingRuntime) EnsureReadAuthorityRound() error {
	runtime.ensureCalls++
	return nil
}

type pointAdmissionSchemaRuntime struct {
	*pointAdmissionRuntime
	nextDigest   [32]byte
	installCalls int
}

func (runtime *pointAdmissionSchemaRuntime) InstallSQLGeneration(
	*sqldriver.Database,
	*sqldriver.ReplicatedApply,
	sqldriver.ReplicatedShardStoreIdentity,
	sqldriver.ReplicatedApplyIdentity,
) error {
	runtime.installCalls++
	runtime.identity.RelationManifestDigest = runtime.nextDigest
	return nil
}

func newPointAdmissionLane(
	t *testing.T, runtime *pointAdmissionRuntime,
) (*ExecutionLanes, *ExecutionLane) {
	t.Helper()
	set, err := NewExecutionLanes(1, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := set.addRuntime(runtime); err != nil {
		_ = set.Close()
		t.Fatal(err)
	}
	lane, err := set.OwnerLane(0)
	if err != nil {
		_ = set.Close()
		t.Fatal(err)
	}
	if _, done, err := lane.RunOne(); err != nil || done {
		_ = set.Close()
		t.Fatalf("initial lane turn done=%v err=%v", done, err)
	}
	t.Cleanup(func() {
		if err := set.Close(); err != nil {
			t.Errorf("close execution lanes: %v", err)
		}
	})
	return set, lane
}

func TestExecutionLaneTryReadPointAdmissionUsesFreshCutAndRenewal(t *testing.T) {
	runtime := &pointAdmissionRuntime{fakeRuntime: newFakeRuntime(121)}
	runtime.token = concurrentAuthorityToken(runtime.fakeRuntime, 1)
	runtime.emitPending = true
	set, lane := newPointAdmissionLane(t, runtime)
	// The Host stores only the immutable identity at Add. Status and token
	// observations must come from the live Runtime turn, not from values
	// captured while the lane was constructed.
	runtime.status.Commit = 37
	runtime.status.Applied = 37
	runtime.token.Nonce = 2
	runtime.lockProbe = func() bool { return executionLaneLockHeld(lane) }
	key := runtime.identity.Group
	callbackCalls := 0
	attempted, admitted, authorized, result, err := lane.TryReadPointAdmission(
		key, runtime.identity, runtime.status.Term,
		func(identity raftmember.RuntimeIdentity, status raftmember.RuntimeStatus, token raftauthority.AuthorityToken) bool {
			callbackCalls++
			if !executionLaneLockHeld(lane) || identity != runtime.identity ||
				status.Commit != runtime.status.Commit || token != runtime.token {
				t.Errorf("callback cut not held/coherent: identity=%+v status=%+v token=%+v", identity, status, token)
				return false
			}
			return true
		},
	)
	if err != nil || !attempted || !admitted || !authorized {
		t.Fatalf("admission attempted=%v admitted=%v authorized=%v err=%v", attempted, admitted, authorized, err)
	}
	if callbackCalls != 1 || result.Identity != runtime.identity || result.Status.Commit != runtime.status.Commit || result.Token != runtime.token {
		t.Fatalf("result=%+v callbackCalls=%d", result, callbackCalls)
	}
	if runtime.statusUnlocked != 0 || runtime.tokenUnlocked != 0 || runtime.ensureUnlocked != 0 {
		t.Fatalf("runtime methods escaped lane lock status=%d token=%d ensure=%d", runtime.statusUnlocked, runtime.tokenUnlocked, runtime.ensureUnlocked)
	}
	if runtime.ensureCalls != 1 || runtime.pendingCalls != 1 || set.lanes[0].host.runnableLen() != 1 {
		t.Fatalf("renewal ensure=%d pending=%d runnable=%d", runtime.ensureCalls, runtime.pendingCalls, set.lanes[0].host.runnableLen())
	}
	select {
	case <-lane.AsyncNotify():
	default:
		t.Fatal("pending renewal did not signal coalesced async wake")
	}
}

func TestExecutionLaneTryReadPointAdmissionBusyDoesNotObserveRuntime(t *testing.T) {
	runtime := &pointAdmissionRuntime{fakeRuntime: newFakeRuntime(122)}
	runtime.token = concurrentAuthorityToken(runtime.fakeRuntime, 1)
	_, lane := newPointAdmissionLane(t, runtime)
	entry := &lane.set.lanes[lane.index]
	entry.mu.Lock()
	var callbackCalled atomic.Bool
	attempted, admitted, authorized, _, err := lane.TryReadPointAdmission(
		runtime.identity.Group, runtime.identity, runtime.status.Term,
		func(raftmember.RuntimeIdentity, raftmember.RuntimeStatus, raftauthority.AuthorityToken) bool {
			callbackCalled.Store(true)
			return false
		},
	)
	entry.mu.Unlock()
	if callbackCalled.Load() {
		t.Fatal("busy lane invoked authorization callback")
	}
	if err != nil || attempted || admitted || authorized {
		t.Fatalf("busy admission attempted=%v admitted=%v authorized=%v err=%v", attempted, admitted, authorized, err)
	}
	if runtime.statusCalls != 0 || runtime.tokenCalls != 0 || runtime.ensureCalls != 0 || runtime.pendingCalls != 0 {
		t.Fatalf("busy lane observed runtime status=%d token=%d ensure=%d pending=%d",
			runtime.statusCalls, runtime.tokenCalls, runtime.ensureCalls, runtime.pendingCalls)
	}
}

func TestExecutionLaneTryReadPointAdmissionTokenMissDefersRenewal(t *testing.T) {
	runtime := &pointAdmissionRuntime{
		fakeRuntime: newFakeRuntime(123), tokenErr: raftauthority.ErrNotQuorum,
	}
	set, lane := newPointAdmissionLane(t, runtime)
	attempted, admitted, authorized, _, err := lane.TryReadPointAdmission(
		runtime.identity.Group, runtime.identity, runtime.status.Term,
		func(raftmember.RuntimeIdentity, raftmember.RuntimeStatus, raftauthority.AuthorityToken) bool {
			t.Fatal("token miss invoked authorization callback")
			return false
		},
	)
	if err != nil || !attempted || admitted || authorized {
		t.Fatalf("token miss attempted=%v admitted=%v authorized=%v err=%v", attempted, admitted, authorized, err)
	}
	if runtime.ensureCalls != 0 || runtime.pendingCalls != 0 || set.lanes[0].host.runnableLen() != 0 {
		t.Fatalf("token miss scheduled renewal ensure=%d pending=%d runnable=%d",
			runtime.ensureCalls, runtime.pendingCalls, set.lanes[0].host.runnableLen())
	}
	select {
	case <-lane.AsyncNotify():
		t.Fatal("token miss signaled async wake")
	default:
	}
}

func TestExecutionLaneTryReadPointAdmissionDeniedSkipsRenewal(t *testing.T) {
	runtime := &pointAdmissionRuntime{fakeRuntime: newFakeRuntime(124)}
	runtime.token = concurrentAuthorityToken(runtime.fakeRuntime, 1)
	set, lane := newPointAdmissionLane(t, runtime)
	attempted, admitted, authorized, _, err := lane.TryReadPointAdmission(
		runtime.identity.Group, runtime.identity, runtime.status.Term,
		func(raftmember.RuntimeIdentity, raftmember.RuntimeStatus, raftauthority.AuthorityToken) bool {
			return false
		},
	)
	if err != nil || !attempted || !admitted || authorized {
		t.Fatalf("denied admission attempted=%v admitted=%v authorized=%v err=%v", attempted, admitted, authorized, err)
	}
	if runtime.ensureCalls != 0 || runtime.pendingCalls != 0 || set.lanes[0].host.runnableLen() != 0 {
		t.Fatalf("denied admission scheduled renewal ensure=%d pending=%d runnable=%d",
			runtime.ensureCalls, runtime.pendingCalls, set.lanes[0].host.runnableLen())
	}
}

func TestExecutionLaneTryReadPointAdmissionAuthorizedNoopSkipsWake(t *testing.T) {
	runtime := &pointAdmissionRuntime{fakeRuntime: newFakeRuntime(127)}
	runtime.token = concurrentAuthorityToken(runtime.fakeRuntime, 1)
	set, lane := newPointAdmissionLane(t, runtime)
	attempted, admitted, authorized, _, err := lane.TryReadPointAdmission(
		runtime.identity.Group, runtime.identity, runtime.status.Term,
		func(raftmember.RuntimeIdentity, raftmember.RuntimeStatus, raftauthority.AuthorityToken) bool {
			return true
		},
	)
	if err != nil || !attempted || !admitted || !authorized {
		t.Fatalf("authorized no-op attempted=%v admitted=%v authorized=%v err=%v", attempted, admitted, authorized, err)
	}
	if runtime.ensureCalls != 1 || runtime.pendingCalls != 1 || set.lanes[0].host.runnableLen() != 0 {
		t.Fatalf("authorized no-op ensure=%d pending=%d runnable=%d",
			runtime.ensureCalls, runtime.pendingCalls, set.lanes[0].host.runnableLen())
	}
	select {
	case <-lane.AsyncNotify():
		t.Fatal("authorized no-op signaled async wake")
	default:
	}
}

func TestExecutionLaneTryReadPointAdmissionEnsureErrorSkipsWake(t *testing.T) {
	runtime := &pointAdmissionRuntime{
		fakeRuntime: newFakeRuntime(128),
		ensureErr:   errors.New("point admission test: renewal unavailable"),
	}
	runtime.token = concurrentAuthorityToken(runtime.fakeRuntime, 1)
	set, lane := newPointAdmissionLane(t, runtime)
	attempted, admitted, authorized, _, err := lane.TryReadPointAdmission(
		runtime.identity.Group, runtime.identity, runtime.status.Term,
		func(raftmember.RuntimeIdentity, raftmember.RuntimeStatus, raftauthority.AuthorityToken) bool {
			return true
		},
	)
	if err != nil || !attempted || !admitted || !authorized {
		t.Fatalf("Ensure error attempted=%v admitted=%v authorized=%v err=%v", attempted, admitted, authorized, err)
	}
	if runtime.ensureCalls != 1 || runtime.pendingCalls != 0 || set.lanes[0].host.runnableLen() != 0 {
		t.Fatalf("Ensure error ensure=%d pending=%d runnable=%d",
			runtime.ensureCalls, runtime.pendingCalls, set.lanes[0].host.runnableLen())
	}
	select {
	case <-lane.AsyncNotify():
		t.Fatal("Ensure error signaled async wake")
	default:
	}
}

func TestHostPointAdmissionWithoutPendingCapabilityConservativelyWakes(t *testing.T) {
	runtime := &pointAdmissionNoPendingRuntime{
		fakeRuntime: newFakeRuntime(125),
	}
	runtime.token = concurrentAuthorityToken(runtime.fakeRuntime, 1)
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	key := runtime.identity.Group
	if _, done, err := host.RunOne(); err != nil || done {
		t.Fatalf("initial host turn done=%v err=%v", done, err)
	}
	admitted, authorized, _, err := host.tryReadPointAdmission(
		key, runtime.identity, runtime.status.Term, nil,
	)
	if err != nil || !admitted || !authorized {
		t.Fatalf("admission admitted=%v authorized=%v err=%v", admitted, authorized, err)
	}
	if runtime.ensureCalls != 1 || host.runnableLen() != 1 {
		t.Fatalf("missing pending capability ensure=%d runnable=%d", runtime.ensureCalls, host.runnableLen())
	}
	select {
	case <-host.AsyncNotify():
	default:
		t.Fatal("successful Ensure without pending capability did not signal async wake")
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestHostPointAdmissionRefreshesIdentityAfterSQLGenerationInstall(t *testing.T) {
	runtime := &pointAdmissionSchemaRuntime{
		pointAdmissionRuntime: &pointAdmissionRuntime{fakeRuntime: newFakeRuntime(126)},
		nextDigest:            [32]byte{0x7a},
	}
	runtime.token = concurrentAuthorityToken(runtime.fakeRuntime, 1)
	host, err := NewHost(testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	if err := host.addRuntime(runtime); err != nil {
		t.Fatal(err)
	}
	key := runtime.identity.Group
	group := host.groups[key]
	group.schemaQuiesced = true
	oldIdentity := runtime.identity
	if err := host.InstallSQLGeneration(
		key, &sqldriver.Database{}, &sqldriver.ReplicatedApply{},
		sqldriver.ReplicatedShardStoreIdentity{}, sqldriver.ReplicatedApplyIdentity{},
	); err != nil {
		t.Fatalf("install SQL generation: %v", err)
	}
	if runtime.installCalls != 1 || group.identity != runtime.identity ||
		group.identity.RelationManifestDigest != runtime.nextDigest {
		t.Fatalf("identity after install=%+v runtime=%+v installs=%d", group.identity, runtime.identity, runtime.installCalls)
	}
	if _, _, _, err := host.tryReadPointAdmission(key, oldIdentity, runtime.status.Term, nil); !errors.Is(err, raftmodel.ErrNotLeader) {
		t.Fatalf("old identity admission=%v, want ErrNotLeader", err)
	}
	admitted, authorized, result, err := host.tryReadPointAdmission(
		key, runtime.identity, runtime.status.Term, nil,
	)
	if err != nil || !admitted || !authorized || result.Identity != runtime.identity {
		t.Fatalf("new identity admission admitted=%v authorized=%v result=%+v err=%v",
			admitted, authorized, result, err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
}
