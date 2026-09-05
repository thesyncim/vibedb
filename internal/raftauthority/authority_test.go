package raftauthority

import (
	"errors"
	"testing"
	"time"
)

type manualClock struct {
	now time.Duration
	err error
}

func (clock *manualClock) Now() (time.Duration, error) {
	if clock.err != nil {
		return 0, clock.err
	}
	return clock.now, nil
}

func authorityTestGroup() GroupIdentity {
	return GroupIdentity{
		ClusterID:             [16]byte{1},
		ClusterIncarnation:    [16]byte{2},
		TopologyRecoveryEpoch: 3,
		ShardIncarnation:      [16]byte{4},
		GroupID:               [16]byte{5},
	}
}

func authorityTestPolicy(enabled bool) ReadAuthorityPolicy {
	return ReadAuthorityPolicy{
		Enabled:        enabled,
		PolicyVersion:  7,
		MaxGrant:       time.Second,
		ClockRatePPM:   100_000,
		RoundingMargin: 10 * time.Millisecond,
		Voters:         []uint64{1, 2, 3},
		Capabilities: []VoterCapability{
			{MemberID: 1, PolicyVersion: 7, Enabled: enabled},
			{MemberID: 2, PolicyVersion: 7, Enabled: enabled},
			{MemberID: 3, PolicyVersion: 7, Enabled: enabled},
		},
	}
}

func authorityTestObservation(term, leader uint64, config ConfigIdentity) AuthorityObservation {
	return AuthorityObservation{
		Group: authorityTestGroup(), Term: term, Leader: leader, LeaderIncarnation: 22, Config: config,
		CurrentTermCommitted: true, Stable: true,
	}
}

func authorityTestConfig() ConfigIdentity {
	return ConfigIdentity{AppliedVersion: 11, Digest: [32]byte{9}}
}

func TestReadAuthorityPolicyRequiresEveryVoterCapability(t *testing.T) {
	policy := authorityTestPolicy(true)
	if err := policy.Validate(); err != nil {
		t.Fatalf("valid policy: %v", err)
	}
	if got := policy.Quorum(); got != 2 {
		t.Fatalf("quorum = %d, want 2", got)
	}
	policy.Capabilities[1].Enabled = false
	if err := policy.Validate(); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("partially enabled policy error = %v", err)
	}
	policy = authorityTestPolicy(true)
	policy.Capabilities[1].PolicyVersion++
	if err := policy.Validate(); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("mixed-version policy error = %v", err)
	}
	if err := authorityTestPolicy(false).Validate(); err != nil {
		t.Fatalf("disabled policy: %v", err)
	}
}

func TestReadAuthorityRoundPairsExactGrantsAndExpiresConservatively(t *testing.T) {
	policy := authorityTestPolicy(true)
	clockSource := &manualClock{}
	clock := NewCheckedClock(clockSource)
	config := authorityTestConfig()
	group := authorityTestGroup()
	observation := authorityTestObservation(5, 1, config)
	round, request, err := NewAuthorityRound(clock, group, 1, 22, 5, config, observation, policy, 41)
	if err != nil {
		t.Fatalf("NewAuthorityRound: %v", err)
	}
	books := make([]*PromiseBook, 3)
	for index, member := range policy.Voters {
		memberClock := NewCheckedClock(&manualClock{})
		book, bookErr := NewPromiseBook(memberClock, group, member, policy)
		if bookErr != nil {
			t.Fatalf("NewPromiseBook(%d): %v", member, bookErr)
		}
		books[index] = book
		grant, grantErr := book.Grant(request, observation)
		if grantErr != nil {
			t.Fatalf("Grant(%d): %v", member, grantErr)
		}
		if index == 0 {
			if grantErr := round.AddGrant(grant); grantErr != nil {
				t.Fatalf("AddGrant(self): %v", grantErr)
			}
		}
		if index == 1 {
			if grantErr := round.AddGrant(grant); grantErr != nil {
				t.Fatalf("AddGrant(peer): %v", grantErr)
			}
		}
	}
	if !round.HasQuorum() {
		t.Fatal("round did not reach exact majority")
	}
	token, err := round.Token(observation)
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if token.ExpiresAt <= token.StartedAt || token.ExpiresAt-token.StartedAt >= policy.MaxGrant {
		t.Fatalf("token window = %s, want positive conservative window below %s", token.ExpiresAt-token.StartedAt, policy.MaxGrant)
	}
	if err := round.AddGrant(AuthorityGrant{Request: request, Voter: 3, GrantedAt: 0, PromiseUntil: policy.MaxGrant}); !errors.Is(err, ErrRoundComplete) {
		t.Fatalf("grant after quorum error = %v", err)
	}
	clockSource.now = token.ExpiresAt
	if _, err := round.Token(observation); !errors.Is(err, ErrRoundExpired) {
		t.Fatalf("expired Token error = %v", err)
	}
	_ = books
}

func TestReadAuthorityPromiseRejectsHigherTermUntilLocalExpiry(t *testing.T) {
	policy := authorityTestPolicy(true)
	clockSource := &manualClock{}
	book, err := NewPromiseBook(NewCheckedClock(clockSource), authorityTestGroup(), 2, policy)
	if err != nil {
		t.Fatalf("NewPromiseBook: %v", err)
	}
	config := authorityTestConfig()
	firstObservation := authorityTestObservation(5, 1, config)
	firstRequest := AuthorityRequest{
		Group: authorityTestGroup(), Term: 5, Holder: 1, HolderIncarnation: 22,
		Config: config, PolicyVersion: policy.PolicyVersion, PolicyDigest: policy.PolicyDigest(),
		Nonce: 1,
	}
	firstRequest.StartAt = 0
	first, err := book.Grant(firstRequest, firstObservation)
	if err != nil {
		t.Fatalf("first Grant: %v", err)
	}
	duplicate, err := book.Grant(firstRequest, firstObservation)
	if err != nil || duplicate != first {
		t.Fatalf("duplicate Grant = %+v err=%v", duplicate, err)
	}
	secondObservation := authorityTestObservation(6, 3, config)
	secondRequest := firstRequest
	secondRequest.Term = 6
	secondRequest.Holder = 3
	secondRequest.Nonce = 2
	if _, err := book.Grant(secondRequest, secondObservation); !errors.Is(err, ErrPromiseHeld) {
		t.Fatalf("higher-term Grant error = %v", err)
	}
	clockSource.now = policy.MaxGrant
	second, err := book.Grant(secondRequest, secondObservation)
	if err != nil {
		t.Fatalf("post-expiry Grant: %v", err)
	}
	if second.Request != secondRequest || second.Voter != 2 {
		t.Fatalf("post-expiry grant = %+v", second)
	}
}

func TestReadAuthorityClockRollbackPermanentlyFailsClosed(t *testing.T) {
	clockSource := &manualClock{now: 10}
	clock := NewCheckedClock(clockSource)
	if _, err := clock.Now(); err != nil {
		t.Fatalf("initial Now: %v", err)
	}
	clockSource.now = 9
	if _, err := clock.Now(); !errors.Is(err, ErrClockRollback) || !errors.Is(err, ErrClockFault) {
		t.Fatalf("rollback error = %v", err)
	}
	clockSource.now = 11
	if _, err := clock.Now(); !errors.Is(err, ErrClockFault) {
		t.Fatalf("post-rollback error = %v", err)
	}
}

func TestReadAuthorityPromiseAllowsOnlyMonotoneSameHolderRenewal(t *testing.T) {
	policy := authorityTestPolicy(true)
	clockSource := &manualClock{}
	book, err := NewPromiseBook(NewCheckedClock(clockSource), authorityTestGroup(), 2, policy)
	if err != nil {
		t.Fatalf("NewPromiseBook: %v", err)
	}
	config := authorityTestConfig()
	observation := authorityTestObservation(5, 1, config)
	first := AuthorityRequest{
		Group: authorityTestGroup(), Term: 5, Holder: 1, HolderIncarnation: 22,
		Config: config, PolicyVersion: policy.PolicyVersion, PolicyDigest: policy.PolicyDigest(),
		Nonce: 1, StartAt: 10,
	}
	firstGrant, err := book.Grant(first, observation)
	if err != nil {
		t.Fatalf("first Grant: %v", err)
	}
	clockSource.now = 100 * time.Millisecond
	renewed := first
	renewed.Nonce = 2
	renewed.StartAt = 20
	secondGrant, err := book.Grant(renewed, observation)
	if err != nil {
		t.Fatalf("renewal Grant: %v", err)
	}
	if secondGrant.PromiseUntil <= firstGrant.PromiseUntil {
		t.Fatalf("renewal promise until %s, first %s", secondGrant.PromiseUntil, firstGrant.PromiseUntil)
	}
	if _, err := book.Grant(first, observation); !errors.Is(err, ErrStaleRequest) {
		t.Fatalf("reordered old renewal error = %v", err)
	}
	changedHolder := renewed
	changedHolder.Holder = 3
	changedHolder.Nonce = 3
	changedHolder.StartAt = 30
	if _, err := book.Grant(changedHolder, authorityTestObservation(5, 3, config)); !errors.Is(err, ErrPromiseHeld) {
		t.Fatalf("identity-changing renewal error = %v", err)
	}
	clockSource.now = secondGrant.PromiseUntil
	if _, err := book.Grant(first, observation); !errors.Is(err, ErrStaleRequest) {
		t.Fatalf("expired reordered old renewal error = %v", err)
	}
}

func TestReadAuthorityRenewalRequiresFreshObservation(t *testing.T) {
	policy := authorityTestPolicy(true)
	clockSource := &manualClock{now: 100 * time.Millisecond}
	book, err := NewPromiseBook(NewCheckedClock(clockSource), authorityTestGroup(), 2, policy)
	if err != nil {
		t.Fatalf("NewPromiseBook: %v", err)
	}
	config := authorityTestConfig()
	request := AuthorityRequest{
		Group: authorityTestGroup(), Term: 5, Holder: 1, HolderIncarnation: 22,
		Config: config, PolicyVersion: policy.PolicyVersion, PolicyDigest: policy.PolicyDigest(),
		Nonce: 1, StartAt: 10,
	}
	if _, err := book.Grant(request, authorityTestObservation(5, 1, config)); err != nil {
		t.Fatalf("first Grant: %v", err)
	}
	renewed := request
	renewed.Nonce = 2
	renewed.StartAt = 20
	staleCases := []AuthorityObservation{
		authorityTestObservation(6, 1, config),
		authorityTestObservation(5, 3, config),
		func() AuthorityObservation {
			stale := authorityTestObservation(5, 1, config)
			stale.LeaderIncarnation++
			return stale
		}(),
		func() AuthorityObservation {
			stale := authorityTestObservation(5, 1, config)
			stale.Config.Pending = true
			stale.Stable = false
			return stale
		}(),
	}
	for index, stale := range staleCases {
		if _, err := book.Grant(renewed, stale); !errors.Is(err, ErrObservationStale) {
			t.Fatalf("stale renewal %d error = %v", index, err)
		}
		until, found, untilErr := book.PromiseUntil()
		if untilErr != nil || !found || until != policy.MaxGrant+100*time.Millisecond {
			t.Fatalf("stale renewal %d changed promise until=%s found=%v err=%v", index, until, found, untilErr)
		}
	}
}

func TestQualifiedElapsedClockIsExplicitAndMonotonic(t *testing.T) {
	clock, err := NewQualifiedElapsedClock()
	if errors.Is(err, ErrClockUnavailable) {
		return // Unsupported platforms intentionally keep the ReadIndex fallback.
	}
	if err != nil {
		t.Fatalf("NewQualifiedElapsedClock: %v", err)
	}
	first, err := clock.Now()
	if err != nil {
		t.Fatalf("first qualified Now: %v", err)
	}
	second, err := clock.Now()
	if err != nil {
		t.Fatalf("second qualified Now: %v", err)
	}
	if second < first {
		t.Fatalf("qualified clock regressed from %s to %s", first, second)
	}
}

func TestReadAuthorityRejectsUnrepresentableDriftAndDeadlines(t *testing.T) {
	extreme := authorityTestPolicy(true)
	extreme.MaxGrant = MaxGrant
	extreme.ClockRatePPM = 999_999
	if err := extreme.Validate(); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("extreme drift policy error = %v", err)
	}
	maxRange := authorityTestPolicy(true)
	maxRange.MaxGrant = MaxGrant
	maxRange.ClockRatePPM = 999_990
	if err := maxRange.Validate(); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("signed-range quarantine policy error = %v", err)
	}
	policy := authorityTestPolicy(true)
	nearEnd := &manualClock{now: time.Duration(1<<63 - 1)}
	clock := NewCheckedClock(nearEnd)
	config := authorityTestConfig()
	obs := authorityTestObservation(5, 1, config)
	if _, _, err := NewAuthorityRound(clock, authorityTestGroup(), 1, 22, 5, config, obs, policy, 1); !errors.Is(err, ErrDeadlineOverflow) {
		t.Fatalf("holder deadline overflow = %v", err)
	}
	book, err := NewPromiseBook(clock, authorityTestGroup(), 2, policy)
	if err != nil {
		t.Fatalf("NewPromiseBook near end: %v", err)
	}
	if err := book.EnterRestartQuarantine(); !errors.Is(err, ErrDeadlineOverflow) {
		t.Fatalf("quarantine deadline overflow = %v", err)
	}
	if quarantined, err := book.ElectionQuarantined(); !quarantined || !errors.Is(err, ErrDeadlineOverflow) {
		t.Fatalf("failed quarantine state = %v, err=%v", quarantined, err)
	}
}

func TestReadAuthorityDetachesPolicySlices(t *testing.T) {
	policy := authorityTestPolicy(true)
	clock := NewCheckedClock(&manualClock{})
	book, err := NewPromiseBook(clock, authorityTestGroup(), 2, policy)
	if err != nil {
		t.Fatalf("NewPromiseBook: %v", err)
	}
	config := authorityTestConfig()
	request := AuthorityRequest{
		Group: authorityTestGroup(), Term: 5, Holder: 1, HolderIncarnation: 22,
		Config: config, PolicyVersion: policy.PolicyVersion, PolicyDigest: policy.PolicyDigest(),
		Nonce: 1,
	}
	policy.Voters[1] = 99
	policy.Capabilities[1].MemberID = 99
	if _, err := book.Grant(request, authorityTestObservation(5, 1, config)); err != nil {
		t.Fatalf("book changed after caller mutation: %v", err)
	}
}

func TestReadAuthorityRestartQuarantineIsExplicitAndBounded(t *testing.T) {
	policy := authorityTestPolicy(true)
	clockSource := &manualClock{}
	book, err := NewPromiseBook(NewCheckedClock(clockSource), authorityTestGroup(), 1, policy)
	if err != nil {
		t.Fatalf("NewPromiseBook: %v", err)
	}
	quarantine, err := policy.QuarantineDuration()
	if err != nil {
		t.Fatalf("QuarantineDuration: %v", err)
	}
	if quarantine <= policy.MaxGrant {
		t.Fatalf("quarantine = %s, want longer than grant %s", quarantine, policy.MaxGrant)
	}
	if err := book.EnterRestartQuarantine(); err != nil {
		t.Fatalf("EnterRestartQuarantine: %v", err)
	}
	request := AuthorityRequest{
		Group: authorityTestGroup(), Term: 5, Holder: 2, HolderIncarnation: 22,
		Config: authorityTestConfig(), PolicyVersion: policy.PolicyVersion, PolicyDigest: policy.PolicyDigest(),
		Nonce: 1,
	}
	if _, err := book.Grant(request, authorityTestObservation(5, 2, authorityTestConfig())); !errors.Is(err, ErrPromiseHeld) {
		t.Fatalf("quarantine Grant error = %v", err)
	}
	clockSource.now = quarantine
	if _, err := book.Grant(request, authorityTestObservation(5, 2, authorityTestConfig())); err != nil {
		t.Fatalf("post-quarantine Grant: %v", err)
	}
}

func TestReadAuthorityFinalValidationAndInvalidation(t *testing.T) {
	policy := authorityTestPolicy(true)
	holderClock := &manualClock{}
	clock := NewCheckedClock(holderClock)
	config := authorityTestConfig()
	observation := authorityTestObservation(5, 1, config)
	round, request, err := NewAuthorityRound(clock, authorityTestGroup(), 1, 22, 5, config, observation, policy, 8)
	if err != nil {
		t.Fatalf("NewAuthorityRound: %v", err)
	}
	for _, member := range policy.Voters[:2] {
		memberClock := NewCheckedClock(&manualClock{})
		book, bookErr := NewPromiseBook(memberClock, authorityTestGroup(), member, policy)
		if bookErr != nil {
			t.Fatalf("NewPromiseBook(%d): %v", member, bookErr)
		}
		grant, grantErr := book.Grant(request, observation)
		if grantErr != nil {
			t.Fatalf("Grant(%d): %v", member, grantErr)
		}
		if grantErr := round.AddGrant(grant); grantErr != nil {
			t.Fatalf("AddGrant(%d): %v", member, grantErr)
		}
	}
	token, err := round.Token(observation)
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	stale := observation
	stale.Term++
	if err := round.ValidateToken(token, stale); !errors.Is(err, ErrObservationStale) {
		t.Fatalf("stale token validation = %v", err)
	}
	round.Invalidate()
	if err := round.ValidateToken(token, observation); !errors.Is(err, ErrRoundInvalidated) {
		t.Fatalf("invalidated token validation = %v", err)
	}
}
