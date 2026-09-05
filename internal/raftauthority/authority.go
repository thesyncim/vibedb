// Package raftauthority contains the bounded, explicit protocol state used by
// the optional leader read authority.  It deliberately has no transport or
// Raft dependency: transport authentication, Raft admission, and SQL serving
// are layered on top of these exact records.
//
// The feature is opt-in.  A zero ReadAuthorityPolicy is disabled and callers
// must configure the same enabled policy, including every voter capability, on
// every member before a holder can start a round.  Keeping this package small
// and dependency-free also makes the failure and restart tests deterministic.
package raftauthority

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"
)

const (
	// MaxVoters is the fixed upper bound retained by one authority round or
	// promise book.  The cluster membership layer may impose a smaller bound.
	MaxVoters = 64

	// MaxGrant is an implementation ceiling for the configured grant window.
	// A deployment may choose a lower value, but an unbounded lease is never
	// accepted by this package.
	MaxGrant = 24 * time.Hour

	// FullClockRatePPM is one million parts per million, or a 100% rate error.
	FullClockRatePPM = uint32(1_000_000)
)

var (
	ErrClockUnavailable = errors.New("raftauthority: elapsed clock unavailable")
	ErrClockFault       = errors.New("raftauthority: elapsed clock fault")
	ErrClockRollback    = errors.New("raftauthority: elapsed clock moved backwards")
	ErrPolicyDisabled   = errors.New("raftauthority: read authority policy disabled")
	ErrInvalidPolicy    = errors.New("raftauthority: invalid read authority policy")
	ErrInvalidRequest   = errors.New("raftauthority: invalid authority request")
	ErrInvalidGrant     = errors.New("raftauthority: invalid authority grant")
	ErrPromiseHeld      = errors.New("raftauthority: voter promise is still held")
	ErrRoundComplete    = errors.New("raftauthority: authority round is already complete")
	ErrRoundExpired     = errors.New("raftauthority: authority round expired")
	ErrRoundInvalidated = errors.New("raftauthority: authority round invalidated")
	ErrNotQuorum        = errors.New("raftauthority: authority round lacks a quorum")
	ErrNotVoter         = errors.New("raftauthority: member is not a configured voter")
	ErrConfigUnstable   = errors.New("raftauthority: configuration is joint or pending")
	ErrObservationStale = errors.New("raftauthority: holder observation is stale")
	ErrDeadlineOverflow = errors.New("raftauthority: authority deadline overflows elapsed clock")
	ErrStaleRequest     = errors.New("raftauthority: stale authority request")
)

// ElapsedClock is the only time source accepted by the authority protocol.
// Implementations must return a process-local elapsed duration that never
// goes backwards.  A qualified Linux implementation should use
// CLOCK_BOOTTIME so suspend time advances; callers on an unqualified platform
// must leave the policy disabled and continue with ReadIndex.
type ElapsedClock interface {
	Now() (time.Duration, error)
}

// CheckedClock wraps an injected clock and permanently fails closed after an
// error or rollback.  It is intentionally stateful and must be owned by the
// serialized Raft member that owns the corresponding promise or holder.
type CheckedClock struct {
	source      ElapsedClock
	last        time.Duration
	initialized bool
	fault       error
}

// NewCheckedClock returns a rollback-detecting elapsed clock wrapper.  It does
// not infer qualification from the source; the deployment policy decides
// whether the source is suitable for authority use.
func NewCheckedClock(source ElapsedClock) *CheckedClock {
	return &CheckedClock{source: source}
}

// Now returns the next elapsed reading, or a stable fault after the source
// fails.  A rollback is a protocol fault even when the source later recovers.
func (clock *CheckedClock) Now() (time.Duration, error) {
	if clock == nil || clock.source == nil {
		return 0, ErrClockUnavailable
	}
	if clock.fault != nil {
		return 0, clock.fault
	}
	now, err := clock.source.Now()
	if err != nil {
		clock.fault = errors.Join(ErrClockFault, err)
		return 0, clock.fault
	}
	if now < 0 {
		clock.fault = fmt.Errorf("%w: negative reading %s", ErrClockFault, now)
		return 0, clock.fault
	}
	if clock.initialized && now < clock.last {
		clock.fault = errors.Join(ErrClockRollback, ErrClockFault)
		return 0, clock.fault
	}
	clock.last = now
	clock.initialized = true
	return now, nil
}

// Faulted reports whether the clock has permanently disabled this owner.
func (clock *CheckedClock) Faulted() bool { return clock != nil && clock.fault != nil }

// GroupIdentity is the complete group/incarnation identity carried by every
// request and grant.  It mirrors raftmember.GroupKey without importing that
// package and creating an import cycle.
type GroupIdentity struct {
	ClusterID             [16]byte
	ClusterIncarnation    [16]byte
	TopologyRecoveryEpoch uint64
	ShardIncarnation      [16]byte
	GroupID               [16]byte
}

// ConfigIdentity identifies the exact applied membership configuration used
// by a round.  Digest is supplied by the membership layer and is never
// reconstructed from a mutable or borrowed protobuf.
type ConfigIdentity struct {
	AppliedVersion uint64
	Digest         [32]byte
	Joint          bool
	Pending        bool
}

func (config ConfigIdentity) stable() bool {
	return config.AppliedVersion != 0 && config.Digest != ([32]byte{}) && !config.Joint && !config.Pending
}

// VoterCapability is the explicit feature/policy marker for one configured
// voter.  An enabled policy is valid only when every voter has an enabled
// capability at the same policy version.  This prevents mixed-version auto
// enablement during rolling upgrades and requires an explicit downgrade.
type VoterCapability struct {
	MemberID      uint64
	PolicyVersion uint32
	Enabled       bool
}

// ReadAuthorityPolicy is the fixed deployment contract for one group.  The
// zero value is disabled.  Voters and Capabilities must list exactly the same
// sorted member IDs; callers must distribute this complete policy to every
// voter before enabling the feature.
type ReadAuthorityPolicy struct {
	Enabled        bool
	PolicyVersion  uint32
	MaxGrant       time.Duration
	ClockRatePPM   uint32
	RoundingMargin time.Duration
	Voters         []uint64
	Capabilities   []VoterCapability
}

// Validate checks the finite, explicit policy shape.  It does not inspect the
// operating system clock; that qualification belongs to the owner setup.
func (policy ReadAuthorityPolicy) Validate() error {
	if !policy.Enabled {
		return nil
	}
	if policy.PolicyVersion == 0 || policy.MaxGrant <= 0 || policy.MaxGrant > MaxGrant {
		return fmt.Errorf("%w: version or max grant", ErrInvalidPolicy)
	}
	if policy.ClockRatePPM >= FullClockRatePPM {
		return fmt.Errorf("%w: clock-rate bound %d", ErrInvalidPolicy, policy.ClockRatePPM)
	}
	if policy.RoundingMargin < 0 || policy.RoundingMargin >= policy.MaxGrant {
		return fmt.Errorf("%w: rounding margin", ErrInvalidPolicy)
	}
	if len(policy.Voters) == 0 || len(policy.Voters) > MaxVoters || len(policy.Capabilities) != len(policy.Voters) {
		return fmt.Errorf("%w: voter capability count", ErrInvalidPolicy)
	}
	for index, member := range policy.Voters {
		if member == 0 || (index != 0 && policy.Voters[index-1] >= member) {
			return fmt.Errorf("%w: voters must be non-zero and sorted", ErrInvalidPolicy)
		}
		capability := policy.Capabilities[index]
		if capability.MemberID != member || capability.PolicyVersion != policy.PolicyVersion || !capability.Enabled {
			return fmt.Errorf("%w: voter %d capability is not explicitly enabled", ErrInvalidPolicy, member)
		}
	}
	quarantine, ok := policy.scaledQuarantine()
	if !ok || quarantine > uint64(math.MaxInt64)-uint64(policy.RoundingMargin) {
		return fmt.Errorf("%w: quarantine duration overflows", ErrInvalidPolicy)
	}
	return nil
}

// Quorum is the exact majority required by the configured voter set.
func (policy ReadAuthorityPolicy) Quorum() int { return len(policy.Voters)/2 + 1 }

func (policy ReadAuthorityPolicy) containsVoter(member uint64) bool {
	index, found := slices.BinarySearch(policy.Voters, member)
	return found && index < len(policy.Capabilities) && policy.Capabilities[index].Enabled
}

// PolicyDigest returns a deterministic digest of all policy fields, including
// every voter capability.  It is carried in the protocol records so an
// otherwise valid version cannot be reused with a different voter set.
func (policy ReadAuthorityPolicy) PolicyDigest() [32]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("vibedb/raft-read-authority/v1\x00"))
	var fixed [32]byte
	if policy.Enabled {
		fixed[0] = 1
	}
	binary.BigEndian.PutUint32(fixed[4:8], policy.PolicyVersion)
	binary.BigEndian.PutUint64(fixed[8:16], uint64(policy.MaxGrant))
	binary.BigEndian.PutUint32(fixed[16:20], policy.ClockRatePPM)
	binary.BigEndian.PutUint64(fixed[20:28], uint64(policy.RoundingMargin))
	binary.BigEndian.PutUint32(fixed[28:32], uint32(len(policy.Voters)))
	_, _ = hash.Write(fixed[:])
	for index, member := range policy.Voters {
		var voter [16]byte
		binary.BigEndian.PutUint64(voter[:8], member)
		if index < len(policy.Capabilities) {
			capability := policy.Capabilities[index]
			binary.BigEndian.PutUint32(voter[8:12], capability.PolicyVersion)
			if capability.Enabled {
				voter[12] = 1
			}
		}
		_, _ = hash.Write(voter[:])
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest
}

// UsableDuration applies the conservative drift formula from the policy:
// D*(1-rho)/(1+rho)-margin.  All arithmetic is integer and rounded down.
func (policy ReadAuthorityPolicy) UsableDuration() (time.Duration, error) {
	if err := policy.Validate(); err != nil {
		return 0, err
	}
	if !policy.Enabled {
		return 0, ErrPolicyDisabled
	}
	value := uint64(policy.MaxGrant)
	numerator := uint64(FullClockRatePPM - policy.ClockRatePPM)
	denominator := uint64(FullClockRatePPM + policy.ClockRatePPM)
	scaled, ok := multiplyDivideFloor(value, numerator, denominator)
	if !ok {
		return 0, fmt.Errorf("%w: usable duration overflows", ErrInvalidPolicy)
	}
	margin := uint64(policy.RoundingMargin)
	if scaled <= margin {
		return 0, fmt.Errorf("%w: drift and margin consume grant", ErrInvalidPolicy)
	}
	return time.Duration(scaled - margin), nil
}

// QuarantineDuration bounds how long a restarted voter must stay out of the
// election protocol before a prior promise can no longer overlap it.
func (policy ReadAuthorityPolicy) QuarantineDuration() (time.Duration, error) {
	if err := policy.Validate(); err != nil {
		return 0, err
	}
	if !policy.Enabled {
		return 0, ErrPolicyDisabled
	}
	scaled, ok := policy.scaledQuarantine()
	if !ok {
		return 0, fmt.Errorf("%w: quarantine duration overflows", ErrInvalidPolicy)
	}
	margin := uint64(policy.RoundingMargin)
	if scaled > math.MaxInt64-margin {
		return 0, fmt.Errorf("%w: quarantine overflow", ErrInvalidPolicy)
	}
	return time.Duration(scaled + margin), nil
}

func (policy ReadAuthorityPolicy) scaledQuarantine() (uint64, bool) {
	if !policy.Enabled || policy.MaxGrant <= 0 || policy.ClockRatePPM >= FullClockRatePPM {
		return 0, false
	}
	value := uint64(policy.MaxGrant)
	numerator := uint64(FullClockRatePPM + policy.ClockRatePPM)
	denominator := uint64(FullClockRatePPM - policy.ClockRatePPM)
	return multiplyDivideCeil(value, numerator, denominator)
}

// AuthorityObservation is the serialized local view used to gate grants and
// holder use.  It must be refreshed from the current Raft owner each time a
// protocol boundary is crossed.
type AuthorityObservation struct {
	Group                GroupIdentity
	Term                 uint64
	Leader               uint64
	LeaderIncarnation    uint64
	Config               ConfigIdentity
	CurrentTermCommitted bool
	Stable               bool
}

func (observation AuthorityObservation) validFor(group GroupIdentity, holder, holderIncarnation, term uint64, config ConfigIdentity) bool {
	return observation.Group == group && observation.Term == term && observation.Leader == holder &&
		observation.LeaderIncarnation == holderIncarnation &&
		observation.Config == config && observation.CurrentTermCommitted &&
		observation.Stable && config.stable()
}

// AuthorityRequest starts one holder round. StartAt is the holder's elapsed
// reading at the outbound SEND boundary; callers must construct/send it at
// that boundary rather than at request admission.
type AuthorityRequest struct {
	Group             GroupIdentity
	Term              uint64
	Holder            uint64
	HolderIncarnation uint64
	Config            ConfigIdentity
	PolicyVersion     uint32
	PolicyDigest      [32]byte
	Nonce             uint64
	StartAt           time.Duration
}

func (request AuthorityRequest) valid(policy ReadAuthorityPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	if !policy.Enabled {
		return ErrPolicyDisabled
	}
	if request.Term == 0 || request.Holder == 0 || request.HolderIncarnation == 0 || request.Nonce == 0 || request.StartAt < 0 || !request.Config.stable() {
		return ErrInvalidRequest
	}
	if request.PolicyVersion != policy.PolicyVersion || request.PolicyDigest != policy.PolicyDigest() {
		return fmt.Errorf("%w: policy identity", ErrInvalidRequest)
	}
	return nil
}

// AuthorityGrant is a detached voter promise for exactly one request. The
// PromiseUntil value is local to the granting voter's elapsed clock and is
// diagnostic; the holder uses its own conservative StartAt deadline.
type AuthorityGrant struct {
	Request      AuthorityRequest
	Voter        uint64
	GrantedAt    time.Duration
	PromiseUntil time.Duration
}

func (grant AuthorityGrant) valid(policy ReadAuthorityPolicy) error {
	if err := grant.Request.valid(policy); err != nil {
		return err
	}
	if !policy.containsVoter(grant.Voter) || grant.GrantedAt < 0 || grant.PromiseUntil <= grant.GrantedAt {
		return ErrInvalidGrant
	}
	expectedUntil, ok := checkedAdd(grant.GrantedAt, policy.MaxGrant)
	if !ok || grant.PromiseUntil != expectedUntil {
		return fmt.Errorf("%w: promise duration is outside policy", ErrInvalidGrant)
	}
	return nil
}

// AuthorityToken is the exact bounded capability used by the read owner after
// a round reaches a quorum. It is immutable data; the caller must revalidate
// it against a fresh observation before exposing a pinned read.
type AuthorityToken struct {
	Group             GroupIdentity
	Config            ConfigIdentity
	Term              uint64
	Holder            uint64
	HolderIncarnation uint64
	PolicyVersion     uint32
	PolicyDigest      [32]byte
	Nonce             uint64
	StartedAt         time.Duration
	ExpiresAt         time.Duration
}

// AuthorityRound retains at most one bounded quorum acquisition. The round
// and all grants are single-owner state; no goroutine may call its methods
// concurrently with the Raft owner.
type AuthorityRound struct {
	clock       *CheckedClock
	policy      ReadAuthorityPolicy
	request     AuthorityRequest
	usableUntil time.Duration
	grants      []AuthorityGrant
	complete    bool
	invalidated bool
}

// NewAuthorityRound starts a round at the current holder elapsed time. Call it
// at the exact outbound-send boundary. It requires a current-term commit and a
// stable, applied configuration; callers should continue ReadIndex otherwise.
func NewAuthorityRound(
	clock *CheckedClock,
	group GroupIdentity,
	localMember, holderIncarnation, term uint64,
	config ConfigIdentity,
	observation AuthorityObservation,
	policy ReadAuthorityPolicy,
	nonce uint64,
) (*AuthorityRound, AuthorityRequest, error) {
	if clock == nil {
		return nil, AuthorityRequest{}, ErrClockUnavailable
	}
	if err := policy.Validate(); err != nil {
		return nil, AuthorityRequest{}, err
	}
	if !policy.Enabled {
		return nil, AuthorityRequest{}, ErrPolicyDisabled
	}
	if !policy.containsVoter(localMember) {
		return nil, AuthorityRequest{}, ErrNotVoter
	}
	if localMember != observation.Leader || !observation.validFor(group, localMember, holderIncarnation, term, config) {
		return nil, AuthorityRequest{}, ErrObservationStale
	}
	if nonce == 0 {
		return nil, AuthorityRequest{}, ErrInvalidRequest
	}
	usable, err := policy.UsableDuration()
	if err != nil {
		return nil, AuthorityRequest{}, err
	}
	start, err := clock.Now()
	if err != nil {
		return nil, AuthorityRequest{}, err
	}
	request := AuthorityRequest{
		Group: group, Term: term, Holder: localMember,
		HolderIncarnation: holderIncarnation, Config: config,
		PolicyVersion: policy.PolicyVersion, PolicyDigest: policy.PolicyDigest(),
		Nonce: nonce, StartAt: start,
	}
	if err := request.valid(policy); err != nil {
		return nil, AuthorityRequest{}, err
	}
	usableUntil, ok := checkedAdd(start, usable)
	if !ok {
		return nil, AuthorityRequest{}, ErrDeadlineOverflow
	}
	return &AuthorityRound{
		clock: clock, policy: clonePolicy(policy), request: request,
		usableUntil: usableUntil,
		grants:      make([]AuthorityGrant, 0, len(policy.Voters)),
	}, request, nil
}

// Request returns a detached copy of the exact round request.
func (round *AuthorityRound) Request() AuthorityRequest {
	if round == nil {
		return AuthorityRequest{}
	}
	return round.request
}

// AddGrant accepts one exact, non-duplicate grant. Late, stale, or replayed
// grants cannot complete a different round.
func (round *AuthorityRound) AddGrant(grant AuthorityGrant) error {
	if round == nil {
		return ErrRoundInvalidated
	}
	if round.invalidated {
		return ErrRoundInvalidated
	}
	if round.complete {
		return ErrRoundComplete
	}
	if err := grant.valid(round.policy); err != nil {
		return err
	}
	if grant.Request != round.request {
		return fmt.Errorf("%w: request identity", ErrInvalidGrant)
	}
	now, err := round.clock.Now()
	if err != nil {
		return err
	}
	if now >= round.usableUntil {
		return ErrRoundExpired
	}
	for _, prior := range round.grants {
		if prior.Voter == grant.Voter {
			return fmt.Errorf("%w: voter %d", ErrInvalidGrant, grant.Voter)
		}
	}
	round.grants = append(round.grants, grant)
	if len(round.grants) >= round.policy.Quorum() {
		round.complete = true
	}
	return nil
}

// HasQuorum reports whether exact distinct configured voters have granted this
// round. It does not bypass current-term or expiry checks; use Token for a
// serving capability.
func (round *AuthorityRound) HasQuorum() bool {
	return round != nil && round.complete && !round.invalidated
}

// Token returns a serving authority only while the round is complete, current,
// and within the holder's conservative deadline.
func (round *AuthorityRound) Token(observation AuthorityObservation) (AuthorityToken, error) {
	if round == nil || round.invalidated {
		return AuthorityToken{}, ErrRoundInvalidated
	}
	if !round.complete {
		return AuthorityToken{}, ErrNotQuorum
	}
	now, err := round.clock.Now()
	if err != nil {
		return AuthorityToken{}, err
	}
	if now >= round.usableUntil {
		return AuthorityToken{}, ErrRoundExpired
	}
	request := round.request
	if !observation.validFor(request.Group, request.Holder, request.HolderIncarnation, request.Term, request.Config) {
		return AuthorityToken{}, ErrObservationStale
	}
	return AuthorityToken{
		Group: request.Group, Config: request.Config, Term: request.Term,
		Holder: request.Holder, HolderIncarnation: request.HolderIncarnation,
		PolicyVersion: request.PolicyVersion, PolicyDigest: request.PolicyDigest,
		Nonce: request.Nonce, StartedAt: request.StartAt, ExpiresAt: round.usableUntil,
	}, nil
}

// ValidateToken performs the final serialized owner check. A caller may keep a
// validated immutable data cut after expiry, but it must never use this token
// to authorize a fresh read after expiry or a leadership/configuration change.
func (round *AuthorityRound) ValidateToken(token AuthorityToken, observation AuthorityObservation) error {
	if round == nil || round.invalidated {
		return ErrRoundInvalidated
	}
	if !round.complete || token != mustToken(round) {
		return ErrObservationStale
	}
	now, err := round.clock.Now()
	if err != nil {
		return err
	}
	if now >= token.ExpiresAt {
		return ErrRoundExpired
	}
	request := round.request
	if !observation.validFor(request.Group, request.Holder, request.HolderIncarnation, request.Term, request.Config) {
		return ErrObservationStale
	}
	return nil
}

// Invalidate drops holder authority and late grants. It intentionally does not
// revoke voter promises already issued by another member; those promises
// remain until their local expiry, which prevents an election overlap.
func (round *AuthorityRound) Invalidate() {
	if round != nil {
		round.invalidated = true
		round.grants = round.grants[:0]
	}
}

func mustToken(round *AuthorityRound) AuthorityToken {
	request := round.request
	return AuthorityToken{
		Group: request.Group, Config: request.Config, Term: request.Term,
		Holder: request.Holder, HolderIncarnation: request.HolderIncarnation,
		PolicyVersion: request.PolicyVersion, PolicyDigest: request.PolicyDigest,
		Nonce: request.Nonce, StartedAt: request.StartAt, ExpiresAt: round.usableUntil,
	}
}

type promiseRecord struct {
	request      AuthorityRequest
	promiseUntil time.Duration
	grant        AuthorityGrant
}

// PromiseBook is the one-record voter side of the protocol. It never clears a
// live promise merely because a higher term or a leadership message arrives;
// only local elapsed expiry permits a different request. Restart quarantine is
// an explicit separate state and must be persisted by the embedding owner.
type PromiseBook struct {
	clock          *CheckedClock
	policy         ReadAuthorityPolicy
	group          GroupIdentity
	localMember    uint64
	record         *promiseRecord
	quarantineTill time.Duration
	quarantineErr  error
}

// NewPromiseBook constructs a voter promise owner. A disabled policy is
// accepted so callers can uniformly retain the ReadIndex fallback path.
func NewPromiseBook(clock *CheckedClock, group GroupIdentity, localMember uint64, policy ReadAuthorityPolicy) (*PromiseBook, error) {
	if clock == nil {
		return nil, ErrClockUnavailable
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if policy.Enabled && !policy.containsVoter(localMember) {
		return nil, ErrNotVoter
	}
	return &PromiseBook{clock: clock, policy: clonePolicy(policy), group: group, localMember: localMember}, nil
}

// EnterRestartQuarantine records the minimum local elapsed time before this
// voter may participate in election authority. The caller must persist the
// feature/policy marker alongside this fact; this in-memory method alone does
// not claim restart durability.
func (book *PromiseBook) EnterRestartQuarantine() error {
	if book == nil {
		return ErrClockUnavailable
	}
	if !book.policy.Enabled {
		return ErrPolicyDisabled
	}
	duration, err := book.policy.QuarantineDuration()
	if err != nil {
		book.quarantineErr = err
		return err
	}
	now, err := book.clock.Now()
	if err != nil {
		book.quarantineErr = err
		return err
	}
	quarantineTill, ok := checkedAdd(now, duration)
	if !ok {
		book.quarantineErr = ErrDeadlineOverflow
		return ErrDeadlineOverflow
	}
	book.quarantineTill = quarantineTill
	book.quarantineErr = nil
	return nil
}

func (book *PromiseBook) quarantined(now time.Duration) bool {
	return book.quarantineTill != 0 && now < book.quarantineTill
}

// ElectionQuarantined reports whether the voter must still refuse election
// traffic. Clock faults conservatively keep this true.
func (book *PromiseBook) ElectionQuarantined() (bool, error) {
	if book == nil || !book.policy.Enabled {
		return false, nil
	}
	now, err := book.clock.Now()
	if err != nil {
		return true, err
	}
	if book.quarantineErr != nil {
		return true, book.quarantineErr
	}
	return book.quarantined(now), nil
}

// Grant validates the complete current observation and records one promise.
// A duplicate exact request is idempotent while its promise remains live; a
// different request, including a higher-term request, is rejected until local
// expiry.
func (book *PromiseBook) Grant(request AuthorityRequest, observation AuthorityObservation) (AuthorityGrant, error) {
	if book == nil {
		return AuthorityGrant{}, ErrClockUnavailable
	}
	if err := request.valid(book.policy); err != nil {
		return AuthorityGrant{}, err
	}
	if request.Group != book.group || !book.policy.containsVoter(book.localMember) {
		return AuthorityGrant{}, ErrNotVoter
	}
	now, err := book.clock.Now()
	if err != nil {
		return AuthorityGrant{}, err
	}
	if book.quarantineErr != nil {
		return AuthorityGrant{}, book.quarantineErr
	}
	if book.quarantined(now) {
		return AuthorityGrant{}, ErrPromiseHeld
	}
	if book.record != nil && now < book.record.promiseUntil {
		if book.record.request == request {
			return book.record.grant, nil
		}
		if sameAuthorityIdentity(book.record.request, request) &&
			request.Nonce > book.record.request.Nonce && request.StartAt > book.record.request.StartAt {
			if !observation.validFor(request.Group, request.Holder, request.HolderIncarnation, request.Term, request.Config) {
				return AuthorityGrant{}, ErrObservationStale
			}
			promiseUntil, ok := checkedAdd(now, book.policy.MaxGrant)
			if !ok || promiseUntil < book.record.promiseUntil {
				return AuthorityGrant{}, ErrDeadlineOverflow
			}
			grant := AuthorityGrant{Request: request, Voter: book.localMember,
				GrantedAt: now, PromiseUntil: promiseUntil}
			book.record = &promiseRecord{request: request, promiseUntil: promiseUntil, grant: grant}
			return grant, nil
		}
		if sameAuthorityIdentity(book.record.request, request) && request.Nonce <= book.record.request.Nonce {
			return AuthorityGrant{}, ErrStaleRequest
		}
		return AuthorityGrant{}, ErrPromiseHeld
	}
	if book.record != nil && sameAuthorityIdentity(book.record.request, request) &&
		request.Nonce <= book.record.request.Nonce {
		return AuthorityGrant{}, ErrStaleRequest
	}
	if !observation.validFor(request.Group, request.Holder, request.HolderIncarnation, request.Term, request.Config) {
		return AuthorityGrant{}, ErrObservationStale
	}
	// The local promise uses the full configured window. The holder's
	// conservative deadline is shorter under drift, so a valid holder expires
	// before this record may support another election request.
	promiseUntil, ok := checkedAdd(now, book.policy.MaxGrant)
	if !ok {
		return AuthorityGrant{}, ErrDeadlineOverflow
	}
	grant := AuthorityGrant{
		Request: request, Voter: book.localMember,
		GrantedAt: now, PromiseUntil: promiseUntil,
	}
	book.record = &promiseRecord{request: request, promiseUntil: promiseUntil, grant: grant}
	return grant, nil
}

// PromiseUntil returns the current local promise expiry for diagnostics. The
// duration is local to this voter and must never be compared across nodes.
func (book *PromiseBook) PromiseUntil() (time.Duration, bool, error) {
	if book == nil || book.record == nil {
		return 0, false, nil
	}
	if _, err := book.clock.Now(); err != nil {
		return 0, false, err
	}
	return book.record.promiseUntil, true, nil
}

func multiplyDivideFloor(value, numerator, denominator uint64) (uint64, bool) {
	if denominator == 0 {
		return 0, false
	}
	if value == 0 || numerator == 0 {
		return 0, true
	}
	quotient, remainder := value/denominator, value%denominator
	if quotient > math.MaxUint64/numerator {
		return 0, false
	}
	whole := quotient * numerator
	part := (remainder * numerator) / denominator
	if whole > math.MaxUint64-part {
		return 0, false
	}
	return whole + part, true
}

func multiplyDivideCeil(value, numerator, denominator uint64) (uint64, bool) {
	floor, ok := multiplyDivideFloor(value, numerator, denominator)
	if !ok {
		return 0, false
	}
	remainder := value % denominator
	if remainder != 0 && (remainder*numerator)%denominator != 0 && floor == math.MaxUint64 {
		return 0, false
	}
	if remainder != 0 && (remainder*numerator)%denominator != 0 {
		floor++
	}
	return floor, true
}

func checkedAdd(left, right time.Duration) (time.Duration, bool) {
	if left < 0 || right < 0 || left > time.Duration(math.MaxInt64)-right {
		return 0, false
	}
	return left + right, true
}

func sameAuthorityIdentity(left, right AuthorityRequest) bool {
	return left.Group == right.Group && left.Term == right.Term && left.Holder == right.Holder &&
		left.HolderIncarnation == right.HolderIncarnation && left.Config == right.Config &&
		left.PolicyVersion == right.PolicyVersion && left.PolicyDigest == right.PolicyDigest
}

func clonePolicy(policy ReadAuthorityPolicy) ReadAuthorityPolicy {
	policy.Voters = append([]uint64(nil), policy.Voters...)
	policy.Capabilities = append([]VoterCapability(nil), policy.Capabilities...)
	return policy
}
