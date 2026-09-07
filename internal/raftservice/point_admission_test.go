package raftservice

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// pointAdmissionFixture uses the existing serialized read fixture but supplies
// only the narrow concurrent Host capability. It keeps source bytes and final
// token validation real while making the admission result deterministic.
type pointAdmissionFixture struct {
	readAuthorityOwnerFixture
	host   *pointAdmissionTestHost
	slot   *pointReadViewSlot
	owners *ExecutionOwners
}

type pointAdmissionTestHost struct {
	*authorityTestHost
	mu                    sync.Mutex
	busy                  bool
	afterAuthorize        func()
	tryCalls              int
	validateCalls         int
	serializedStatusCalls int
	validationErr         error
}

func (host *pointAdmissionTestHost) Status(key raftmember.GroupKey) (raftmember.RuntimeStatus, error) {
	host.mu.Lock()
	host.serializedStatusCalls++
	host.mu.Unlock()
	return host.authorityTestHost.Status(key)
}

func (host *pointAdmissionTestHost) TryReadPointAdmission(
	key raftmember.GroupKey,
	expected raftmember.RuntimeIdentity,
	term uint64,
	authorize func(raftmember.RuntimeIdentity, raftmember.RuntimeStatus, raftauthority.AuthorityToken) bool,
) (bool, bool, bool, multiraft.PointReadAdmission, error) {
	host.mu.Lock()
	host.tryCalls++
	busy := host.busy
	host.mu.Unlock()
	if busy {
		return false, false, false, multiraft.PointReadAdmission{}, nil
	}
	status := host.status
	token := host.token
	result := multiraft.PointReadAdmission{Identity: expected, Status: status, Token: token}
	if key != expected.Group || status.Term != term {
		return true, false, false, result, raftmodel.ErrNotLeader
	}
	if authorize != nil && !authorize(expected, status, token) {
		return true, true, false, result, nil
	}
	if host.afterAuthorize != nil {
		host.afterAuthorize()
	}
	return true, true, true, result, nil
}

func (host *pointAdmissionTestHost) TryValidateReadAuthorityToken(
	_ raftmember.GroupKey, _ uint64, _ uint64, _ uint64,
	_ raftauthority.AuthorityToken,
) (bool, error) {
	host.mu.Lock()
	host.validateCalls++
	err := host.validationErr
	host.validationErr = nil
	host.mu.Unlock()
	return true, err
}

func newPointAdmissionFixture(t testing.TB) pointAdmissionFixture {
	t.Helper()
	base := newReadAuthorityOwnerFixture(t)
	host := &pointAdmissionTestHost{authorityTestHost: base.host}
	base.owner.host = host
	slot := &pointReadViewSlot{}
	member := base.owner.members[base.group]
	member.pointReadSlot = slot
	base.owner.members[base.group] = member
	serving := ServingState{Identity: member.identity, Command: member.command, Status: base.host.status}
	if permit := base.owner.ensureServingFencePermit(serving); permit == nil {
		t.Fatal("point admission permit was not published")
	}
	owners := &ExecutionOwners{}
	owners.byGroup.Store(&executionOwnerGroups{values: map[raftmember.GroupKey]executionOwnerRoute{
		base.group: {owner: base.owner, point: slot},
	}})
	return pointAdmissionFixture{readAuthorityOwnerFixture: base, host: host, slot: slot, owners: owners}
}

func TestExecutionOwnersWarmPointAdmissionSkipsOwnerQueue(t *testing.T) {
	fixture := newPointAdmissionFixture(t)
	defer fixture.close()
	var cut LinearizablePointReadCut
	if err := fixture.owners.ReadLinearizablePointInto(t.Context(), LinearizablePointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
	}, &cut); err != nil {
		t.Fatalf("warm point admission: %v", err)
	}
	if got := len(fixture.owner.ingress); got != 0 {
		t.Fatalf("warm admission queued %d owner requests", got)
	}
	value, err := cut.PointReadInto(t.Context(), 1, []byte("key"), 64, nil)
	if err != nil || !value.Found || string(value.Value) != "value" {
		t.Fatalf("point result=%+v err=%v", value, err)
	}
	if err := cut.Close(); err != nil {
		t.Fatalf("close cut: %v", err)
	}
	fixture.host.mu.Lock()
	tryCalls, validateCalls := fixture.host.tryCalls, fixture.host.validateCalls
	fixture.host.mu.Unlock()
	serializedStatusCalls := fixture.host.serializedStatusCalls
	if tryCalls != 1 || validateCalls != 1 || serializedStatusCalls != 0 {
		t.Fatalf("direct calls=%d validation=%d serialized-status=%d", tryCalls, validateCalls, serializedStatusCalls)
	}
	fixture.host.authorityTestHost.mu.Lock()
	readIndexes := fixture.host.authorityTestHost.readIndexes
	fixture.host.authorityTestHost.mu.Unlock()
	if readIndexes != 0 {
		t.Fatalf("warm admission issued ReadIndex=%d", readIndexes)
	}
}

func TestExecutionOwnersPointAdmissionDenialIsTerminal(t *testing.T) {
	fixture := newPointAdmissionFixture(t)
	defer fixture.close()
	var cut LinearizablePointReadCut
	err := fixture.owners.ReadLinearizablePointInto(t.Context(), LinearizablePointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
		ConcurrentAuthorize: func(ServingState) bool { return false },
	}, &cut)
	if !errors.Is(err, ErrServingAuthorization) {
		t.Fatalf("denied admission=%v, want ErrServingAuthorization", err)
	}
	if cut.owner != nil || fixture.owner.pendingReadItems != 0 || fixture.generation.pins.Load() != 0 {
		t.Fatalf("denied admission retained cut/resources: owner=%p reads=%d pins=%d",
			cut.owner, fixture.owner.pendingReadItems, fixture.generation.pins.Load())
	}
	if got := len(fixture.owner.ingress); got != 0 {
		t.Fatalf("denied admission queued %d owner requests", got)
	}
}

func TestExecutionOwnersBusyPointAdmissionNormalizesConcurrentAuthorization(t *testing.T) {
	fixture := newPointAdmissionFixture(t)
	defer fixture.close()
	fixture.host.mu.Lock()
	fixture.host.busy = true
	fixture.host.mu.Unlock()
	expectedIdentity := fixture.owner.members[fixture.group].identity
	var calls atomic.Int32
	var cut LinearizablePointReadCut
	err := fixture.owners.ReadLinearizablePointInto(t.Context(), LinearizablePointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
		ConcurrentAuthorize: func(state ServingState) bool {
			calls.Add(1)
			return state.Identity == expectedIdentity
		},
	}, &cut)
	if err != nil {
		t.Fatalf("busy fallback admission: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("serialized fallback authorization calls=%d, want 1", calls.Load())
	}
	value, err := cut.PointReadInto(t.Context(), 1, []byte("key"), 64, nil)
	if err != nil || !value.Found {
		t.Fatalf("busy fallback point result=%+v err=%v", value, err)
	}
	cut.Close()
	if got := len(fixture.owner.ingress); got != 0 {
		t.Fatalf("fallback left %d owner requests", got)
	}
}

func TestExecutionOwnersRejectsAmbiguousPointAuthorization(t *testing.T) {
	fixture := newPointAdmissionFixture(t)
	defer fixture.close()
	var cut LinearizablePointReadCut
	err := fixture.owners.ReadLinearizablePointInto(t.Context(), LinearizablePointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
		Authorize:           func(ServingState) bool { return true },
		ConcurrentAuthorize: func(ServingState) bool { return true },
	}, &cut)
	if !errors.Is(err, ErrInvalidOwner) {
		t.Fatalf("dual authorization=%v, want ErrInvalidOwner", err)
	}
	if got := len(fixture.owner.ingress); got != 0 {
		t.Fatalf("dual authorization queued %d owner requests", got)
	}
}

func TestExecutionOwnersCanceledPointAdmissionReleasesBudget(t *testing.T) {
	fixture := newPointAdmissionFixture(t)
	defer fixture.close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var cut LinearizablePointReadCut
	err := fixture.owners.ReadLinearizablePointInto(ctx, LinearizablePointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
	}, &cut)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled admission=%v, want context.Canceled", err)
	}
	if fixture.owner.pendingReadItems != 0 || fixture.owner.pendingReadBytes != 0 || fixture.generation.pins.Load() != 0 {
		t.Fatalf("canceled admission retained resources reads=%d/%d pins=%d",
			fixture.owner.pendingReadItems, fixture.owner.pendingReadBytes, fixture.generation.pins.Load())
	}
}

func TestExecutionOwnersPointAdmissionRevocationDuringCaptureFailsClosed(t *testing.T) {
	fixture := newPointAdmissionFixture(t)
	defer fixture.close()
	fixture.host.afterAuthorize = func() {
		fixture.owner.revokeServingFencePermit(fixture.group)
	}
	var cut LinearizablePointReadCut
	err := fixture.owners.ReadLinearizablePointInto(t.Context(), LinearizablePointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
		ConcurrentAuthorize: func(ServingState) bool { return true },
	}, &cut)
	if !errors.Is(err, ErrServingFence) {
		t.Fatalf("revoked capture=%v, want ErrServingFence", err)
	}
	if cut.owner != nil || fixture.owner.pendingReadItems != 0 || fixture.generation.pins.Load() != 0 {
		t.Fatalf("revoked capture retained resources owner=%p reads=%d pins=%d",
			cut.owner, fixture.owner.pendingReadItems, fixture.generation.pins.Load())
	}
	if fixture.slot.value.Load() != nil {
		t.Fatal("revoked capture left point view published")
	}
}

func TestExecutionOwnersForcedRetryReauthorizesConcurrentOnlyRequest(t *testing.T) {
	fixture := newPointAdmissionFixture(t)
	defer fixture.close()
	fixture.host.mu.Lock()
	fixture.host.busy = true
	fixture.host.validationErr = errors.New("point admission test: force retry")
	fixture.host.mu.Unlock()
	var calls atomic.Int32
	var cut LinearizablePointReadCut
	if err := fixture.owners.ReadLinearizablePointInto(t.Context(), LinearizablePointReadRequest{
		Fence: fixture.fence, Capability: serviceauthz.CapabilityDataRead,
		ConcurrentAuthorize: func(state ServingState) bool {
			calls.Add(1)
			return state.Identity.MemberID == fixture.fence.MemberID
		},
	}, &cut); err != nil {
		t.Fatalf("point admission: %v", err)
	}
	value, err := cut.PointReadInto(t.Context(), 1, []byte("key"), 64, nil)
	if err != nil || !value.Found {
		t.Fatalf("forced retry result=%+v err=%v", value, err)
	}
	cut.Close()
	if calls.Load() != 2 {
		t.Fatalf("authorization calls=%d, want initial serialized cut plus forced retry", calls.Load())
	}
	fixture.host.authorityTestHost.mu.Lock()
	readIndexes := fixture.host.authorityTestHost.readIndexes
	fixture.host.authorityTestHost.mu.Unlock()
	if readIndexes != 1 {
		t.Fatalf("forced retry read indexes=%d, want 1", readIndexes)
	}
}

func TestExecutionOwnersPointSlotSurvivesMemberReplacement(t *testing.T) {
	fixture := newPointAdmissionFixture(t)
	defer fixture.close()
	member := fixture.owner.members[fixture.group]
	next := member
	next.command.RouteGeneration++
	next.generation = &ownerGeneration{}
	next.pointReadSlot = &pointReadViewSlot{}
	fixture.owner.storeOwnerMember(fixture.group, next)
	if got := fixture.owner.members[fixture.group].pointReadSlot; got != fixture.slot {
		t.Fatalf("member replacement changed stable point slot: got=%p want=%p", got, fixture.slot)
	}
	if fixture.slot.value.Load() != nil {
		t.Fatal("member replacement left revoked point view published")
	}
	serving := ServingState{Identity: next.identity, Command: next.command, Status: fixture.host.status}
	fresh := fixture.owner.ensureServingFencePermit(serving)
	if fresh == nil || fixture.slot.value.Load() == nil {
		t.Fatal("replacement did not republish a fresh point view")
	}
	fixture.owner.revokeAllServingFencePermits()
	if fresh.valid(serving.Fence(), next.generation) || fixture.slot.value.Load() != nil {
		t.Fatal("shutdown revoke left point view published")
	}
}
