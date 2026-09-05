package raftauthority

import (
	"errors"
	"math"
	"math/big"
	"testing"
	"time"
)

const driftTestScale int64 = 1_000_000

type driftTestClock struct {
	now time.Duration
}

func (clock *driftTestClock) Now() (time.Duration, error) {
	return clock.now, nil
}

func driftTestGroup() GroupIdentity {
	return GroupIdentity{
		ClusterID:             [16]byte{0x11},
		ClusterIncarnation:    [16]byte{0x22},
		TopologyRecoveryEpoch: 3,
		ShardIncarnation:      [16]byte{0x33},
		GroupID:               [16]byte{0x44},
	}
}

func driftTestConfig() ConfigIdentity {
	return ConfigIdentity{AppliedVersion: 9, Digest: [32]byte{0x55}}
}

func driftTestObservation(group GroupIdentity, term, leader uint64, config ConfigIdentity) AuthorityObservation {
	return AuthorityObservation{
		Group: group, Term: term, Leader: leader, LeaderIncarnation: 5,
		Config: config, CurrentTermCommitted: true, Stable: true,
	}
}

func driftTestPolicy() ReadAuthorityPolicy {
	return ReadAuthorityPolicy{
		Enabled:        true,
		PolicyVersion:  13,
		MaxGrant:       3*time.Second + 123*time.Nanosecond,
		ClockRatePPM:   333_333,
		RoundingMargin: 17 * time.Nanosecond,
		Voters:         []uint64{1, 2, 3},
		Capabilities: []VoterCapability{
			{MemberID: 1, PolicyVersion: 13, Enabled: true},
			{MemberID: 2, PolicyVersion: 13, Enabled: true},
			{MemberID: 3, PolicyVersion: 13, Enabled: true},
		},
	}
}

func driftTestDurationValue(value time.Duration) *big.Int {
	return big.NewInt(int64(value))
}

func driftTestFloor(value *big.Int, numerator, denominator int64) *big.Int {
	product := new(big.Int).Mul(new(big.Int).Set(value), big.NewInt(numerator))
	return product.Quo(product, big.NewInt(denominator))
}

func driftTestCeil(value *big.Int, numerator, denominator int64) *big.Int {
	product := new(big.Int).Mul(new(big.Int).Set(value), big.NewInt(numerator))
	divisor := big.NewInt(denominator)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(product, divisor, remainder)
	if remainder.Sign() != 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	return quotient
}

func driftTestUsable(policy ReadAuthorityPolicy) *big.Int {
	scaled := driftTestFloor(
		driftTestDurationValue(policy.MaxGrant),
		driftTestScale-int64(policy.ClockRatePPM),
		driftTestScale+int64(policy.ClockRatePPM),
	)
	return scaled.Sub(scaled, driftTestDurationValue(policy.RoundingMargin))
}

func driftTestQuarantine(policy ReadAuthorityPolicy) *big.Int {
	scaled := driftTestCeil(
		driftTestDurationValue(policy.MaxGrant),
		driftTestScale+int64(policy.ClockRatePPM),
		driftTestScale-int64(policy.ClockRatePPM),
	)
	return scaled.Add(scaled, driftTestDurationValue(policy.RoundingMargin))
}

// The reference clock maps an integer physical nanosecond to the largest
// elapsed integer tick observed at that instant. Release is the first integer
// physical tick at which a local deadline is reached.
func driftTestLocalAt(physical time.Duration, ppm int64) time.Duration {
	local := driftTestFloor(
		driftTestDurationValue(physical),
		driftTestScale+ppm,
		driftTestScale,
	)
	return time.Duration(local.Int64())
}

func driftTestPhysicalAtLocal(local time.Duration, ppm int64) time.Duration {
	physical := driftTestCeil(
		driftTestDurationValue(local),
		driftTestScale,
		driftTestScale+ppm,
	)
	return time.Duration(physical.Int64())
}

func driftTestPhysicalMin(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}
	return right
}

func TestAuthorityDeadlinePrecedesQuorumPromiseReleaseAtBothDriftExtremes(t *testing.T) {
	policy := driftTestPolicy()
	rho := int64(policy.ClockRatePPM)
	cases := []struct {
		name         string
		holderPPM    int64
		voterPPM     int64
		requestDelay time.Duration
		grantDelay   time.Duration
	}{
		{
			name:         "slow-holder-fast-voter",
			holderPPM:    -rho,
			voterPPM:     rho,
			requestDelay: 257 * time.Millisecond,
			grantDelay:   113 * time.Millisecond,
		},
		{
			name:         "fast-holder-slow-voter",
			holderPPM:    rho,
			voterPPM:     -rho,
			requestDelay: 41 * time.Millisecond,
			grantDelay:   319 * time.Millisecond,
		},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			group, config := driftTestGroup(), driftTestConfig()
			observation := driftTestObservation(group, 7, 1, config)
			holderSource := &driftTestClock{}
			round, request, err := NewAuthorityRound(
				NewCheckedClock(holderSource), group, 1, 5, 7, config,
				observation, policy, 1,
			)
			if err != nil {
				t.Fatalf("NewAuthorityRound: %v", err)
			}

			holderPromiseSource := &driftTestClock{}
			holderBook, err := NewPromiseBook(
				NewCheckedClock(holderPromiseSource), group, 1, policy,
			)
			if err != nil {
				t.Fatalf("NewPromiseBook(holder): %v", err)
			}
			holderGrant, err := holderBook.Grant(request, observation)
			if err != nil {
				t.Fatalf("holder Grant: %v", err)
			}

			peerSource := &driftTestClock{
				now: driftTestLocalAt(testCase.requestDelay, testCase.voterPPM),
			}
			peerBook, err := NewPromiseBook(
				NewCheckedClock(peerSource), group, 2, policy,
			)
			if err != nil {
				t.Fatalf("NewPromiseBook(peer): %v", err)
			}
			peerGrant, err := peerBook.Grant(request, observation)
			if err != nil {
				t.Fatalf("peer Grant: %v", err)
			}

			grantArrival := testCase.requestDelay + testCase.grantDelay
			holderSource.now = driftTestLocalAt(grantArrival, testCase.holderPPM)
			if err := round.AddGrant(holderGrant); err != nil {
				t.Fatalf("AddGrant(holder): %v", err)
			}
			if err := round.AddGrant(peerGrant); err != nil {
				t.Fatalf("AddGrant(peer): %v", err)
			}
			token, err := round.Token(observation)
			if err != nil {
				t.Fatalf("Token: %v", err)
			}

			wantUsable := time.Duration(driftTestUsable(policy).Int64())
			if token.ExpiresAt != wantUsable {
				t.Fatalf("usable deadline=%s, reference=%s", token.ExpiresAt, wantUsable)
			}
			holderEnd := driftTestPhysicalAtLocal(token.ExpiresAt, testCase.holderPPM)
			earliestPromiseEnd := driftTestPhysicalMin(
				driftTestPhysicalAtLocal(holderGrant.PromiseUntil, testCase.holderPPM),
				driftTestPhysicalAtLocal(peerGrant.PromiseUntil, testCase.voterPPM),
			)
			if holderEnd > earliestPromiseEnd {
				t.Fatalf("holder end=%s exceeds earliest quorum promise release=%s", holderEnd, earliestPromiseEnd)
			}

			holderSource.now = driftTestLocalAt(holderEnd-1, testCase.holderPPM)
			if _, err := round.Token(observation); err != nil {
				t.Fatalf("Token one physical tick before deadline: %v", err)
			}
			holderSource.now = driftTestLocalAt(holderEnd, testCase.holderPPM)
			if _, err := round.Token(observation); !errors.Is(err, ErrRoundExpired) {
				t.Fatalf("Token at physical deadline=%v, want ErrRoundExpired", err)
			}
		})
	}
}

func TestAuthorityRenewalReorderCannotOutliveRecomputedPromise(t *testing.T) {
	policy := driftTestPolicy()
	rho := int64(policy.ClockRatePPM)
	holderPPM, voterPPM := -rho, rho
	group, config := driftTestGroup(), driftTestConfig()
	observation := driftTestObservation(group, 7, 1, config)
	holderSource := &driftTestClock{}
	holderClock := NewCheckedClock(holderSource)
	round1, request1, err := NewAuthorityRound(
		holderClock, group, 1, 5, 7, config, observation, policy, 1,
	)
	if err != nil {
		t.Fatalf("NewAuthorityRound(first): %v", err)
	}
	_ = round1

	voterSource := &driftTestClock{now: driftTestLocalAt(191*time.Millisecond, voterPPM)}
	book, err := NewPromiseBook(NewCheckedClock(voterSource), group, 2, policy)
	if err != nil {
		t.Fatalf("NewPromiseBook: %v", err)
	}
	firstGrant, err := book.Grant(request1, observation)
	if err != nil {
		t.Fatalf("first Grant: %v", err)
	}

	const renewalAt = 700 * time.Millisecond
	holderSource.now = driftTestLocalAt(renewalAt, holderPPM)
	round2, request2, err := NewAuthorityRound(
		holderClock, group, 1, 5, 7, config, observation, policy, 2,
	)
	if err != nil {
		t.Fatalf("NewAuthorityRound(renewal): %v", err)
	}
	request2Arrival := renewalAt + 37*time.Millisecond
	voterSource.now = driftTestLocalAt(request2Arrival, voterPPM)
	secondGrant, err := book.Grant(request2, observation)
	if err != nil {
		t.Fatalf("renewal Grant: %v", err)
	}
	if secondGrant.PromiseUntil <= firstGrant.PromiseUntil {
		t.Fatalf("renewal promise until=%s, first=%s", secondGrant.PromiseUntil, firstGrant.PromiseUntil)
	}
	if want := driftTestLocalAt(request2Arrival, voterPPM) + policy.MaxGrant; secondGrant.PromiseUntil != want {
		t.Fatalf("renewal promise until=%s, reference=%s", secondGrant.PromiseUntil, want)
	}

	selfSource := &driftTestClock{now: driftTestLocalAt(renewalAt, holderPPM)}
	selfBook, err := NewPromiseBook(NewCheckedClock(selfSource), group, 1, policy)
	if err != nil {
		t.Fatalf("NewPromiseBook(self): %v", err)
	}
	selfGrant, err := selfBook.Grant(request2, observation)
	if err != nil {
		t.Fatalf("self renewal Grant: %v", err)
	}
	grantArrival := request2Arrival + 211*time.Millisecond
	holderSource.now = driftTestLocalAt(grantArrival, holderPPM)
	if err := round2.AddGrant(selfGrant); err != nil {
		t.Fatalf("AddGrant(self): %v", err)
	}
	if err := round2.AddGrant(secondGrant); err != nil {
		t.Fatalf("AddGrant(peer): %v", err)
	}
	token, err := round2.Token(observation)
	if err != nil {
		t.Fatalf("renewal Token: %v", err)
	}
	wantExpires := time.Duration(driftTestUsable(policy).Int64()) + request2.StartAt
	if token.ExpiresAt != wantExpires {
		t.Fatalf("renewal expiry=%s, reference=%s", token.ExpiresAt, wantExpires)
	}
	holderEnd := driftTestPhysicalAtLocal(token.ExpiresAt, holderPPM)
	earliestPromiseEnd := driftTestPhysicalMin(
		driftTestPhysicalAtLocal(selfGrant.PromiseUntil, holderPPM),
		driftTestPhysicalAtLocal(secondGrant.PromiseUntil, voterPPM),
	)
	if holderEnd > earliestPromiseEnd {
		t.Fatalf("renewal holder end=%s exceeds promise release=%s", holderEnd, earliestPromiseEnd)
	}

	voterSource.now = driftTestLocalAt(grantArrival+time.Millisecond, voterPPM)
	if _, err := book.Grant(request1, observation); !errors.Is(err, ErrStaleRequest) {
		t.Fatalf("reordered original request error=%v, want ErrStaleRequest", err)
	}
	changedHolder := request2
	changedHolder.Holder = 3
	changedHolder.Nonce = 3
	changedHolder.StartAt++
	if _, err := book.Grant(changedHolder, driftTestObservation(group, 7, 3, config)); !errors.Is(err, ErrPromiseHeld) {
		t.Fatalf("identity-changing request error=%v, want ErrPromiseHeld", err)
	}
}

func TestRestartQuarantineCoversWorstCasePromiseAcrossDriftSigns(t *testing.T) {
	policy := driftTestPolicy()
	quarantine, err := policy.QuarantineDuration()
	if err != nil {
		t.Fatalf("QuarantineDuration: %v", err)
	}
	wantQuarantine := time.Duration(driftTestQuarantine(policy).Int64())
	if quarantine != wantQuarantine {
		t.Fatalf("quarantine=%s, reference=%s", quarantine, wantQuarantine)
	}
	rho := int64(policy.ClockRatePPM)
	for _, testCase := range []struct {
		name        string
		oldPromise  int64
		restartRate int64
	}{
		{name: "slow-old-fast-restart", oldPromise: -rho, restartRate: rho},
		{name: "fast-old-slow-restart", oldPromise: rho, restartRate: -rho},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			oldPhysical := driftTestPhysicalAtLocal(policy.MaxGrant, testCase.oldPromise)
			restartPhysical := driftTestPhysicalAtLocal(quarantine, testCase.restartRate)
			if restartPhysical < oldPhysical {
				t.Fatalf("restart quarantine=%s shorter than old promise=%s", restartPhysical, oldPhysical)
			}
		})
	}

	group, config := driftTestGroup(), driftTestConfig()
	source := &driftTestClock{now: 123 * time.Millisecond}
	book, err := NewPromiseBook(NewCheckedClock(source), group, 2, policy)
	if err != nil {
		t.Fatalf("NewPromiseBook: %v", err)
	}
	if err := book.EnterRestartQuarantine(); err != nil {
		t.Fatalf("EnterRestartQuarantine: %v", err)
	}
	quarantineTill := 123*time.Millisecond + quarantine
	source.now = quarantineTill - time.Nanosecond
	if quarantined, err := book.ElectionQuarantined(); err != nil || !quarantined {
		t.Fatalf("before quarantine boundary: quarantined=%v err=%v", quarantined, err)
	}
	source.now = quarantineTill
	if quarantined, err := book.ElectionQuarantined(); err != nil || quarantined {
		t.Fatalf("at quarantine boundary: quarantined=%v err=%v", quarantined, err)
	}
	request := AuthorityRequest{
		Group: group, Term: 8, Holder: 1, HolderIncarnation: 5,
		Config: config, PolicyVersion: policy.PolicyVersion,
		PolicyDigest: policy.PolicyDigest(), Nonce: 1, StartAt: time.Nanosecond,
	}
	if _, err := book.Grant(request, driftTestObservation(group, 8, 1, config)); err != nil {
		t.Fatalf("Grant after quarantine boundary: %v", err)
	}
}

func TestAuthorityMaxGrantDriftBoundaryMatchesBigIntReference(t *testing.T) {
	policy := driftTestPolicy()
	policy.MaxGrant = MaxGrant
	policy.RoundingMargin = time.Nanosecond
	for _, ppm := range []uint32{999_981, 999_982} {
		candidate := policy
		candidate.ClockRatePPM = ppm
		wantValid := driftTestQuarantine(candidate).Cmp(
			new(big.Int).Sub(big.NewInt(math.MaxInt64), driftTestDurationValue(candidate.RoundingMargin)),
		) <= 0
		err := candidate.Validate()
		if (err == nil) != wantValid {
			t.Fatalf("ppm=%d Validate=%v, reference representable=%v", ppm, err, wantValid)
		}
		if !wantValid {
			continue
		}
		usable, err := candidate.UsableDuration()
		if err != nil {
			t.Fatalf("ppm=%d UsableDuration: %v", ppm, err)
		}
		if usable != time.Duration(driftTestUsable(candidate).Int64()) {
			t.Fatalf("ppm=%d usable=%s, reference=%s", ppm, usable,
				time.Duration(driftTestUsable(candidate).Int64()))
		}
		gotQuarantine, err := candidate.QuarantineDuration()
		if err != nil || gotQuarantine != time.Duration(driftTestQuarantine(candidate).Int64()) {
			t.Fatalf("ppm=%d quarantine=%s err=%v reference=%s", ppm, gotQuarantine, err,
				time.Duration(driftTestQuarantine(candidate).Int64()))
		}
	}

	policy.ClockRatePPM = 999_981
	usable, err := policy.UsableDuration()
	if err != nil {
		t.Fatalf("boundary UsableDuration: %v", err)
	}
	group, config := driftTestGroup(), driftTestConfig()
	observation := driftTestObservation(group, 7, 1, config)
	maxStart := time.Duration(math.MaxInt64) - usable
	exactSource := &driftTestClock{now: maxStart}
	if _, _, err := NewAuthorityRound(
		NewCheckedClock(exactSource), group, 1, 5, 7, config,
		observation, policy, 1,
	); err != nil {
		t.Fatalf("exact max deadline: %v", err)
	}
	overflowSource := &driftTestClock{now: maxStart + 1}
	if _, _, err := NewAuthorityRound(
		NewCheckedClock(overflowSource), group, 1, 5, 7, config,
		observation, policy, 1,
	); !errors.Is(err, ErrDeadlineOverflow) {
		t.Fatalf("one tick beyond max deadline: %v", err)
	}
}
