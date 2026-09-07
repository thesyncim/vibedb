package multiraft

import (
	"errors"
	"testing"
	"time"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
)

var errConcurrentValidationUnexpectedToken = errors.New(
	"multiraft test: unexpected authority token",
)

type concurrentValidationRuntime struct {
	*fakeRuntime
	acceptedTokens      []raftauthority.AuthorityToken
	validationTokens    []raftauthority.AuthorityToken
	validationCalls     int
	validationLockProbe func() bool
	lockChecks          int
	lockNotHeld         int
}

func (runtime *concurrentValidationRuntime) ValidateReadAuthorityToken(
	token raftauthority.AuthorityToken,
) error {
	runtime.validationCalls++
	runtime.validationTokens = append(runtime.validationTokens, token)
	if runtime.validationLockProbe != nil {
		runtime.lockChecks++
		if !runtime.validationLockProbe() {
			runtime.lockNotHeld++
		}
	}
	if token.ExpiresAt <= 0 {
		return raftauthority.ErrRoundExpired
	}
	for _, accepted := range runtime.acceptedTokens {
		if token == accepted {
			return nil
		}
	}
	return errConcurrentValidationUnexpectedToken
}

func newConcurrentValidationLane(
	t *testing.T,
) (*ExecutionLanes, *ExecutionLane, *concurrentValidationRuntime) {
	t.Helper()
	set, err := NewExecutionLanes(1, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	runtime := &concurrentValidationRuntime{fakeRuntime: newFakeRuntime(17)}
	if err := set.addRuntime(runtime); err != nil {
		_ = set.Close()
		t.Fatal(err)
	}
	lane, err := set.OwnerLane(0)
	if err != nil {
		_ = set.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := set.Close(); err != nil {
			t.Errorf("close execution lanes: %v", err)
		}
	})
	return set, lane, runtime
}

func concurrentAuthorityToken(runtime *fakeRuntime, nonce uint64) raftauthority.AuthorityToken {
	identity := runtime.identity
	return raftauthority.AuthorityToken{
		Group: raftauthorityGroup(identity.Group),
		Config: raftauthority.ConfigIdentity{
			AppliedVersion: 1,
			Digest:         [32]byte{0x31},
		},
		Term:              runtime.status.Term,
		Holder:            identity.MemberID,
		HolderIncarnation: identity.NodeIncarnation,
		PolicyVersion:     1,
		PolicyDigest:      [32]byte{0x32},
		Nonce:             nonce,
		StartedAt:         time.Second,
		ExpiresAt:         time.Hour,
	}
}

func executionLaneLockHeld(lane *ExecutionLane) bool {
	entry := &lane.set.lanes[lane.index]
	if entry.mu.TryLock() {
		entry.mu.Unlock()
		return false
	}
	return true
}

func TestExecutionLaneTryValidateReadAuthorityTokenBusyDoesNotObserveRuntime(t *testing.T) {
	set, lane, runtime := newConcurrentValidationLane(t)
	key := runtime.identity.Group
	token := concurrentAuthorityToken(runtime.fakeRuntime, 1)
	entry := &set.lanes[lane.index]
	entry.mu.Lock()
	attempted, err := lane.TryValidateReadAuthorityToken(
		key, runtime.identity.MemberID, runtime.identity.NodeIncarnation,
		runtime.status.Term, token,
	)
	entry.mu.Unlock()

	if attempted || err != nil {
		t.Fatalf("busy validation = attempted %t, err %v; want false, nil", attempted, err)
	}
	if runtime.statusCalls != 0 || runtime.validationCalls != 0 {
		t.Fatalf("busy validation observed runtime: status=%d token=%d",
			runtime.statusCalls, runtime.validationCalls)
	}
}

func TestExecutionLaneTryValidateReadAuthorityTokenRejectsWrongLaneAndClosedLane(t *testing.T) {
	set, err := NewExecutionLanes(2, testHostLimits())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := set.Close(); err != nil {
			t.Errorf("close execution lanes: %v", err)
		}
	}()
	first := &concurrentValidationRuntime{
		fakeRuntime: runtimeForLane(t, set, 0, 1),
	}
	second := &concurrentValidationRuntime{
		fakeRuntime: runtimeForLane(t, set, 1, 81),
	}
	if err := set.addRuntime(first); err != nil {
		t.Fatal(err)
	}
	if err := set.addRuntime(second); err != nil {
		t.Fatal(err)
	}
	lane, err := set.OwnerLane(0)
	if err != nil {
		t.Fatal(err)
	}
	token := concurrentAuthorityToken(second.fakeRuntime, 1)
	attempted, err := lane.TryValidateReadAuthorityToken(
		second.identity.Group, second.identity.MemberID, second.identity.NodeIncarnation,
		second.status.Term, token,
	)
	if attempted || !errors.Is(err, ErrExecutionLane) {
		t.Fatalf("wrong-lane validation = attempted %t, err %v; want false, ErrExecutionLane",
			attempted, err)
	}
	if first.statusCalls != 0 || first.validationCalls != 0 ||
		second.statusCalls != 0 || second.validationCalls != 0 {
		t.Fatalf("wrong-lane validation observed runtime: first status/token=%d/%d second=%d/%d",
			first.statusCalls, first.validationCalls, second.statusCalls, second.validationCalls)
	}

	if err := lane.Close(); err != nil {
		t.Fatalf("close owning lane: %v", err)
	}
	attempted, err = lane.TryValidateReadAuthorityToken(
		first.identity.Group, first.identity.MemberID, first.identity.NodeIncarnation,
		first.status.Term, concurrentAuthorityToken(first.fakeRuntime, 2),
	)
	if !attempted || !errors.Is(err, ErrHostClosed) {
		t.Fatalf("closed-lane validation = attempted %t, err %v; want true, ErrHostClosed",
			attempted, err)
	}
	if first.statusCalls != 0 || first.validationCalls != 0 {
		t.Fatalf("closed-lane validation observed runtime: status=%d token=%d",
			first.statusCalls, first.validationCalls)
	}
}

func TestExecutionLaneTryValidateReadAuthorityTokenUsesExclusiveLaneAndExactTokens(t *testing.T) {
	_, lane, runtime := newConcurrentValidationLane(t)
	older := concurrentAuthorityToken(runtime.fakeRuntime, 1)
	newer := older
	newer.Nonce = 2
	newer.StartedAt = 2 * time.Second
	newer.ExpiresAt = 2 * time.Hour
	runtime.acceptedTokens = []raftauthority.AuthorityToken{newer, older}
	runtime.validationLockProbe = func() bool { return executionLaneLockHeld(lane) }

	for _, token := range []raftauthority.AuthorityToken{newer, older} {
		attempted, err := lane.TryValidateReadAuthorityToken(
			runtime.identity.Group, runtime.identity.MemberID,
			runtime.identity.NodeIncarnation, runtime.status.Term, token,
		)
		if !attempted || err != nil {
			t.Fatalf("validation token nonce=%d = attempted %t, err %v; want true, nil",
				token.Nonce, attempted, err)
		}
	}
	if runtime.statusCalls != 2 || runtime.validationCalls != 2 {
		t.Fatalf("successful validation calls: status=%d token=%d; want 2/2",
			runtime.statusCalls, runtime.validationCalls)
	}
	if runtime.lockChecks != 2 || runtime.lockNotHeld != 0 {
		t.Fatalf("validator lane-lock observations: checks=%d not-held=%d; want 2/0",
			runtime.lockChecks, runtime.lockNotHeld)
	}
	if len(runtime.validationTokens) != 2 ||
		runtime.validationTokens[0] != newer || runtime.validationTokens[1] != older {
		t.Fatalf("validator tokens=%v; want exact newer then older tokens",
			runtime.validationTokens)
	}
}

type concurrentValidationRequest struct {
	key             raftmember.GroupKey
	memberID        uint64
	nodeIncarnation uint64
	term            uint64
	token           raftauthority.AuthorityToken
}

func TestExecutionLaneTryValidateReadAuthorityTokenBindsObservationAndExpiry(t *testing.T) {
	tests := []struct {
		name                string
		mutate              func(*concurrentValidationRequest, *concurrentValidationRuntime)
		wantErr             error
		wantStatusCalls     int
		wantValidationCalls int
		acceptMutatedToken  bool
	}{
		{
			name: "unknown group key",
			mutate: func(request *concurrentValidationRequest, _ *concurrentValidationRuntime) {
				request.key.GroupID[0] ^= 0xff
			},
			wantErr:         ErrGroupNotFound,
			wantStatusCalls: 0,
		},
		{
			name: "foreign holder argument",
			mutate: func(request *concurrentValidationRequest, _ *concurrentValidationRuntime) {
				request.memberID++
			},
			wantErr:         raftmodel.ErrNotLeader,
			wantStatusCalls: 0,
		},
		{
			name: "foreign incarnation argument",
			mutate: func(request *concurrentValidationRequest, _ *concurrentValidationRuntime) {
				request.nodeIncarnation++
			},
			wantErr:         raftmodel.ErrNotLeader,
			wantStatusCalls: 0,
		},
		{
			name: "foreign group token",
			mutate: func(request *concurrentValidationRequest, _ *concurrentValidationRuntime) {
				request.token.Group.GroupID[0] ^= 0xff
			},
			wantErr:         raftauthority.ErrObservationStale,
			wantStatusCalls: 1,
		},
		{
			name: "foreign holder token",
			mutate: func(request *concurrentValidationRequest, _ *concurrentValidationRuntime) {
				request.token.Holder++
			},
			wantErr:         raftauthority.ErrObservationStale,
			wantStatusCalls: 1,
		},
		{
			name: "foreign incarnation token",
			mutate: func(request *concurrentValidationRequest, _ *concurrentValidationRuntime) {
				request.token.HolderIncarnation++
			},
			wantErr:         raftauthority.ErrObservationStale,
			wantStatusCalls: 1,
		},
		{
			name: "foreign term argument",
			mutate: func(request *concurrentValidationRequest, _ *concurrentValidationRuntime) {
				request.term++
			},
			wantErr:         raftmodel.ErrNotLeader,
			wantStatusCalls: 1,
		},
		{
			name: "foreign term token",
			mutate: func(request *concurrentValidationRequest, _ *concurrentValidationRuntime) {
				request.token.Term++
			},
			wantErr:         raftauthority.ErrObservationStale,
			wantStatusCalls: 1,
		},
		{
			name: "refused leader status",
			mutate: func(_ *concurrentValidationRequest, runtime *concurrentValidationRuntime) {
				runtime.status.LeaderID++
			},
			wantErr:         raftmodel.ErrNotLeader,
			wantStatusCalls: 1,
		},
		{
			name: "expired token",
			mutate: func(request *concurrentValidationRequest, _ *concurrentValidationRuntime) {
				request.token.ExpiresAt = 0
			},
			wantErr:             raftauthority.ErrRoundExpired,
			wantStatusCalls:     1,
			wantValidationCalls: 1,
			acceptMutatedToken:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, lane, runtime := newConcurrentValidationLane(t)
			request := concurrentValidationRequest{
				key:             runtime.identity.Group,
				memberID:        runtime.identity.MemberID,
				nodeIncarnation: runtime.identity.NodeIncarnation,
				term:            runtime.status.Term,
				token:           concurrentAuthorityToken(runtime.fakeRuntime, 1),
			}
			test.mutate(&request, runtime)
			accepted := concurrentAuthorityToken(runtime.fakeRuntime, 1)
			if test.acceptMutatedToken {
				accepted = request.token
			}
			runtime.acceptedTokens = []raftauthority.AuthorityToken{accepted}

			attempted, err := lane.TryValidateReadAuthorityToken(
				request.key, request.memberID, request.nodeIncarnation,
				request.term, request.token,
			)
			if !attempted || !errors.Is(err, test.wantErr) {
				t.Fatalf("validation = attempted %t, err %v; want true, %v",
					attempted, err, test.wantErr)
			}
			if runtime.statusCalls != test.wantStatusCalls ||
				runtime.validationCalls != test.wantValidationCalls {
				t.Fatalf("runtime observations: status=%d token=%d; want %d/%d",
					runtime.statusCalls, runtime.validationCalls,
					test.wantStatusCalls, test.wantValidationCalls)
			}
			if test.wantValidationCalls == 0 && len(runtime.validationTokens) != 0 {
				t.Fatalf("unexpected provider tokens=%v", runtime.validationTokens)
			}
			if test.wantValidationCalls == 1 &&
				(runtime.validationTokens[0] != request.token) {
				t.Fatalf("provider token=%v; want exact request token=%v",
					runtime.validationTokens[0], request.token)
			}
		})
	}
}
