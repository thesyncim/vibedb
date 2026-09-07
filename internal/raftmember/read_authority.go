package raftmember

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/thesyncim/vibedb/internal/raftauthority"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"go.etcd.io/raft/v3"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrAuthorityElectionBlocked is returned while an enabled voter promise
	// or restart quarantine prevents an election edge. Callers should keep the
	// ordinary Raft tick/read path alive and retry after the local promise ends.
	ErrAuthorityElectionBlocked = errors.New("raftmember: read authority blocks election")
	// ErrAuthorityRoundActive prevents two holder rounds from overlapping. The
	// existing round remains the only source of serving authority until it is
	// invalidated or expires.
	ErrAuthorityRoundActive = errors.New("raftmember: read authority round is active")
	// ErrAuthorityReconfiguration keeps a live promise from being discarded by
	// an in-process policy change. A deployment must drain the old policy and
	// explicitly re-enable a complete voter set.
	ErrAuthorityReconfiguration = errors.New("raftmember: read authority reconfiguration refused")
	// ErrAuthorityLeaderIncarnationUnavailable refuses a follower grant until
	// the topology/transport owner supplies an independently authenticated
	// current incarnation for the remote leader.
	ErrAuthorityLeaderIncarnationUnavailable = errors.New("raftmember: leader incarnation is unavailable")
	// ErrAuthorityConfigurationMismatch keeps a policy tied to the exact
	// published stable voter set. A stale/shrunk policy can never form a
	// self-only quorum after membership changes.
	ErrAuthorityConfigurationMismatch = errors.New("raftmember: authority voter set differs from applied configuration")
	// ErrAuthorityTransferPending reports that an explicit leader-transfer
	// preparation currently owns the local authority optimization. ReadIndex
	// remains available while the finite transfer guard drains readiness and
	// election promises.
	ErrAuthorityTransferPending = errors.New("raftmember: leader transfer is pending")
)

// ReadAuthorityOptions is the explicit per-member feature contract. An
// enabled policy must be installed on every voter with the same version,
// digest inputs, and capabilities; Runtime never auto-enables it from the
// host platform. ConfigureReadAuthority enters restart quarantine before the
// member can participate in the protocol.
type ReadAuthorityOptions struct {
	Policy raftauthority.ReadAuthorityPolicy
	Clock  *raftauthority.CheckedClock
	// LeaderIncarnation resolves the current boot incarnation of a remote
	// leader from an authenticated topology/source record. Runtime never uses
	// the request's claim or the local voter's counter as this value.
	LeaderIncarnation func(memberID uint64) (uint64, bool, error)
}

// ReadAuthorityRoundMetrics is a detached protocol counter cut. Started is
// incremented only after a new holder request has been admitted, so it measures
// actual quorum rounds rather than per-read Ensure calls. RequestsCreated is
// incremented when each remote request is appended to the bounded outbound
// queue; it does not claim that transport sent the request. GrantsAccepted
// includes the local self-grant and each distinct accepted remote grant; replayed
// or duplicate grants are excluded.
type ReadAuthorityRoundMetrics struct {
	RoundsStarted   uint64
	RequestsCreated uint64
	GrantsAccepted  uint64
}

// ReadAuthorityGateInputCount and the input indexes below are the fixed
// serialized election edges instrumented by the authority gate. Keeping the
// indexes explicit makes the diagnostic shape stable across process boots and
// avoids maps or labels on the election hot path.
const (
	ReadAuthorityGateMessage = iota
	ReadAuthorityGateTick
	ReadAuthorityGateCampaign
	ReadAuthorityGateTransfer
)

const ReadAuthorityGateInputCount = ReadAuthorityGateTransfer + 1

// ReadAuthorityGateReason indexes ReadAuthorityGateMetrics.BlockedByReason.
const (
	ReadAuthorityGateQuarantine = iota
	ReadAuthorityGateLivePromise
	ReadAuthorityGateClockFault
)

const ReadAuthorityGateReasonCount = ReadAuthorityGateClockFault + 1

// ReadAuthorityGateBlockTime retains timestamps only for blocked edges whose
// gate invocation obtained a current, accepted elapsed-clock sample. A clock
// fault is still counted, but it cannot borrow the last good sample as its
// event time.
type ReadAuthorityGateBlockTime struct {
	First     time.Duration
	Last      time.Duration
	Available bool
}

// ReadAuthorityGateMetrics is a bounded detached cut of actual election-gate
// edges. BlockedByReason is indexed by fixed input and reason; BlockedTime has
// matching indexes and is populated only when that edge obtained a fresh
// accepted sample. Allowed is kept separate because a leader tick may be
// allowed while a live promise still protects the group. All local timestamps
// use the checked elapsed clock of this Runtime's boot incarnation.
type ReadAuthorityGateMetrics struct {
	BlockedByReason            [ReadAuthorityGateInputCount][ReadAuthorityGateReasonCount]uint64
	BlockedTime                [ReadAuthorityGateInputCount][ReadAuthorityGateReasonCount]ReadAuthorityGateBlockTime
	Allowed                    [ReadAuthorityGateInputCount]uint64
	AllowedWhileLivePromise    [ReadAuthorityGateInputCount]uint64
	FirstQuarantineExpiredAt   time.Duration
	QuarantineExpiredAvailable bool
}

// ReadAuthorityEvidenceStatus identifies whether a per-group evidence cut is
// usable. Disabled and unavailable are explicit states so a missing record is
// never interpreted as proof that no authority existed.
type ReadAuthorityEvidenceStatus uint8

const (
	ReadAuthorityEvidenceUnavailable ReadAuthorityEvidenceStatus = iota
	ReadAuthorityEvidenceDisabled
	ReadAuthorityEvidenceConfigured
)

// ReadAuthorityEvidenceError is a bounded reason for an unavailable current
// observation. It deliberately avoids retaining arbitrary resolver or clock
// error text in the diagnostic record.
type ReadAuthorityEvidenceError uint8

const (
	ReadAuthorityEvidenceErrorNone ReadAuthorityEvidenceError = iota
	ReadAuthorityEvidenceErrorUnavailable
	ReadAuthorityEvidenceErrorLeaderIncarnation
	ReadAuthorityEvidenceErrorConfiguration
	ReadAuthorityEvidenceErrorClockFault
)

// ReadAuthorityHolderEvidence describes the one valid serving token, if the
// current checked-clock sample and exact observation validate one. Request and
// accepted voters are copied from the bounded round, so this record remains
// detached from owner state.
type ReadAuthorityHolderEvidence struct {
	Available      bool
	Request        raftauthority.AuthorityRequest
	ExpiresAt      time.Duration
	AcceptedVoters []uint64
}

// ReadAuthorityEvidence is one detached per-group authority proof. It is read
// only through Runtime owner serialization and then copied by Host and
// ExecutionLanes. No field is populated by starting, renewing, invalidating,
// or otherwise changing an authority round or PromiseBook.
type ReadAuthorityEvidence struct {
	Identity             RuntimeIdentity
	Status               ReadAuthorityEvidenceStatus
	PolicyVersion        uint32
	PolicyDigest         [32]byte
	Clock                raftauthority.CheckedClockEvidence
	Observation          raftauthority.AuthorityObservation
	ObservationAvailable bool
	ObservationError     ReadAuthorityEvidenceError
	Promise              raftauthority.PromiseBookEvidence
	PromiseKnown         bool
	PromiseActive        bool
	QuarantineKnown      bool
	QuarantineActive     bool
	Holder               ReadAuthorityHolderEvidence
	Gate                 ReadAuthorityGateMetrics
}

type runtimeAuthority struct {
	policy  raftauthority.ReadAuthorityPolicy
	clock   *raftauthority.CheckedClock
	promise *raftauthority.PromiseBook
	// round is the currently serving or initial acquisition round. renewal is
	// one newer bounded acquisition; keeping both lets existing reads finish
	// under the old token while the next quorum is collected.
	round   *raftauthority.AuthorityRound
	renewal *raftauthority.AuthorityRound
	nonce   uint64
	// disabled retains the promise book and election gate while a local
	// downgrade drains. Clearing a live promise here would let an election
	// overlap a grant made under the previous feature policy.
	disabled          bool
	leaderIncarnation func(uint64) (uint64, bool, error)
	outbound          []OutboundMessage
	// These counters are read by diagnostics outside the serialized owner. The
	// protocol state itself remains single-owner; only the detached counters are
	// atomic.
	roundsStarted   atomic.Uint64
	requestsCreated atomic.Uint64
	grantsAccepted  atomic.Uint64
	gate            ReadAuthorityGateMetrics
}

func authorityGroupKey(group GroupKey) raftauthority.GroupIdentity {
	return raftauthority.GroupIdentity{
		ClusterID: group.ClusterID, ClusterIncarnation: group.ClusterIncarnation,
		TopologyRecoveryEpoch: group.TopologyRecoveryEpoch,
		ShardIncarnation:      group.ShardIncarnation, GroupID: group.GroupID,
	}
}

// ConfigureReadAuthority explicitly installs or disables this member's
// authority state. Enabled installation is fail-closed on unsupported clocks,
// incomplete voter capability policy, unstable persisted configuration, or a
// live prior authority. The caller must persist the feature marker that makes
// this configuration operational across restart; this method conservatively
// applies the required local quarantine on every enabled installation.
func (runtime *Runtime) ConfigureReadAuthority(options ReadAuthorityOptions) error {
	if err := runtime.checkUsable(); err != nil {
		return err
	}
	if !options.Policy.Enabled {
		if runtime.authority == nil {
			return nil
		}
		runtime.authority.disabled = true
		return nil
	}
	if options.Clock == nil {
		return raftauthority.ErrClockUnavailable
	}
	if err := options.Policy.Validate(); err != nil {
		return err
	}
	if !runtime.authorityVotersMatch(options.Policy.Voters) {
		return ErrAuthorityConfigurationMismatch
	}
	if runtime.authority != nil {
		if runtime.authority.policy.PolicyVersion != options.Policy.PolicyVersion ||
			runtime.authority.policy.PolicyDigest() != options.Policy.PolicyDigest() {
			return ErrAuthorityReconfiguration
		}
		if runtime.authority.clock != options.Clock {
			return ErrAuthorityReconfiguration
		}
		runtime.authority.disabled = false
		return nil
	}
	if err := runtime.node.CheckElectionGateActivation(); err != nil {
		return err
	}
	book, err := raftauthority.NewPromiseBook(
		options.Clock, authorityGroupKey(runtime.identity.Group), runtime.identity.MemberID,
		options.Policy,
	)
	if err != nil {
		return err
	}
	if err := book.EnterRestartQuarantine(); err != nil {
		return err
	}
	state := &runtimeAuthority{
		policy: cloneAuthorityPolicy(options.Policy), clock: options.Clock, promise: book,
		leaderIncarnation: options.LeaderIncarnation,
		outbound:          make([]OutboundMessage, 0, len(options.Policy.Voters)),
	}
	runtime.authority = state
	return nil
}

// ReadAuthorityEnabled reports whether this Runtime currently accepts
// authority requests. It is a diagnostic owner-lane query; a true result does
// not itself authorize a read.
func (runtime *Runtime) ReadAuthorityEnabled() bool {
	return runtime != nil && runtime.authority != nil && !runtime.authority.disabled
}

// ReadAuthorityRoundMetrics returns the actual protocol-round counters for
// this Runtime. It has no side effects and never enters the Owner or transport.
func (runtime *Runtime) ReadAuthorityRoundMetrics() ReadAuthorityRoundMetrics {
	if runtime == nil || runtime.authority == nil {
		return ReadAuthorityRoundMetrics{}
	}
	state := runtime.authority
	return ReadAuthorityRoundMetrics{
		RoundsStarted:   state.roundsStarted.Load(),
		RequestsCreated: state.requestsCreated.Load(),
		GrantsAccepted:  state.grantsAccepted.Load(),
	}
}

func authorityGateInputIndex(input raftmodel.ElectionInput) (int, bool) {
	index := int(input) - 1
	return index, index >= 0 && index < ReadAuthorityGateInputCount
}

func authorityGateErrorIsClockFault(err error) bool {
	return errors.Is(err, raftauthority.ErrClockUnavailable) ||
		errors.Is(err, raftauthority.ErrClockFault) ||
		errors.Is(err, raftauthority.ErrClockRollback)
}

func (state *runtimeAuthority) recordGateBlocked(
	input raftmodel.ElectionInput,
	reason int,
	sample time.Duration,
	timestampAvailable bool,
) {
	if state == nil || reason < 0 || reason >= ReadAuthorityGateReasonCount {
		return
	}
	index, ok := authorityGateInputIndex(input)
	if !ok {
		return
	}
	state.gate.BlockedByReason[index][reason]++
	if !timestampAvailable || sample < 0 {
		return
	}
	blocked := &state.gate.BlockedTime[index][reason]
	if !blocked.Available {
		blocked.First = sample
		blocked.Available = true
	}
	blocked.Last = sample
}

func (state *runtimeAuthority) recordGateAllowed(input raftmodel.ElectionInput, livePromise bool) {
	if state == nil {
		return
	}
	index, ok := authorityGateInputIndex(input)
	if !ok {
		return
	}
	state.gate.Allowed[index]++
	if livePromise {
		state.gate.AllowedWhileLivePromise[index]++
	}
}

func (state *runtimeAuthority) observeQuarantineExpired() {
	if state == nil || state.promise == nil || state.gate.QuarantineExpiredAvailable {
		return
	}
	if !state.promise.Evidence().QuarantineConfigured {
		return
	}
	clock := state.clock.Evidence()
	if !clock.Initialized || clock.Faulted {
		return
	}
	state.gate.FirstQuarantineExpiredAt = clock.Sample
	state.gate.QuarantineExpiredAvailable = true
}

// ReadAuthorityEvidence returns the detached per-group authority proof used by
// diagnostics. It takes one fresh checked-clock sample to classify the current
// holder and promise state, but never enters a round, clears an expired round,
// renews a promise, or invalidates any state. Callers outside Runtime's
// serialized owner must use Host/ExecutionLanes, which take the lane lock
// before invoking it.
func (runtime *Runtime) ReadAuthorityEvidence() ReadAuthorityEvidence {
	result := ReadAuthorityEvidence{}
	if runtime == nil {
		return result
	}
	result.Identity = runtime.Identity()
	state := runtime.authority
	if state == nil {
		if runtime.closed || runtime.stopping || runtime.node == nil || runtime.stableStore() == nil ||
			runtime.apply == nil || runtime.database == nil || runtime.failure != nil ||
			runtime.node.Phase() == raftmodel.PhaseFailed {
			result.Status = ReadAuthorityEvidenceUnavailable
			return result
		}
		result.Status = ReadAuthorityEvidenceDisabled
		return result
	}
	if state.disabled {
		result.Status = ReadAuthorityEvidenceDisabled
	} else {
		result.Status = ReadAuthorityEvidenceConfigured
	}
	result.PolicyVersion = state.policy.PolicyVersion
	result.PolicyDigest = state.policy.PolicyDigest()
	if state.promise != nil {
		result.Promise = state.promise.Evidence()
	}
	var now time.Duration
	var clockErr error
	if state.clock != nil {
		now, clockErr = state.clock.Now()
		result.Clock = state.clock.Evidence()
	} else {
		clockErr = raftauthority.ErrClockUnavailable
		result.Clock.Faulted = true
	}
	clockUsable := clockErr == nil && now >= 0
	if clockUsable {
		result.PromiseKnown = result.Promise.HasRecord
		if result.PromiseKnown {
			result.PromiseActive = now < result.Promise.PromiseUntil
		}
		result.QuarantineKnown = result.Promise.QuarantineConfigured
		if result.QuarantineKnown {
			result.QuarantineActive = result.Promise.QuarantineError ||
				(result.Promise.QuarantineUntil != 0 && now < result.Promise.QuarantineUntil)
		}
	}
	servingUsable := !runtime.closed && !runtime.stopping && runtime.node != nil &&
		runtime.stableStore() != nil && runtime.apply != nil && runtime.database != nil &&
		runtime.failure == nil && runtime.node.Phase() != raftmodel.PhaseFailed
	var observation raftauthority.AuthorityObservation
	if servingUsable {
		var observationErr error
		observation, observationErr = runtime.readAuthorityObservation()
		result.Observation = observation
		if observationErr == nil {
			result.ObservationAvailable = true
		} else {
			result.ObservationError = readAuthorityEvidenceError(observationErr)
		}
	} else {
		result.ObservationError = ReadAuthorityEvidenceErrorUnavailable
	}
	result.Gate = state.gate
	// A disabled state may retain a live promise for fencing, but it must never
	// expose a serving holder. A failed or faulted clock likewise cannot support
	// a current capability classification.
	if state.disabled || !servingUsable || !result.ObservationAvailable || !clockUsable {
		return result
	}
	for _, round := range [2]*raftauthority.AuthorityRound{state.renewal, state.round} {
		if round == nil {
			continue
		}
		roundEvidence := round.Evidence()
		if !roundEvidence.ValidAt(now, observation) {
			continue
		}
		result.Holder = ReadAuthorityHolderEvidence{
			Available:      true,
			Request:        roundEvidence.Request,
			ExpiresAt:      roundEvidence.ExpiresAt,
			AcceptedVoters: roundEvidence.AcceptedVoters,
		}
		break
	}
	return result
}

func readAuthorityEvidenceError(err error) ReadAuthorityEvidenceError {
	if err == nil {
		return ReadAuthorityEvidenceErrorNone
	}
	if errors.Is(err, ErrAuthorityLeaderIncarnationUnavailable) {
		return ReadAuthorityEvidenceErrorLeaderIncarnation
	}
	if errors.Is(err, ErrAuthorityConfigurationMismatch) {
		return ReadAuthorityEvidenceErrorConfiguration
	}
	if authorityGateErrorIsClockFault(err) {
		return ReadAuthorityEvidenceErrorClockFault
	}
	return ReadAuthorityEvidenceErrorUnavailable
}

func (runtime *Runtime) authorityElectionGate(input raftmodel.ElectionInput) error {
	state := runtime.authority
	if state == nil || state.promise == nil {
		return nil
	}
	quarantined, err := state.promise.ElectionQuarantined()
	if err != nil {
		clock := state.clock.Evidence()
		if authorityGateErrorIsClockFault(err) {
			state.recordGateBlocked(input, ReadAuthorityGateClockFault, 0, false)
		} else {
			state.recordGateBlocked(input, ReadAuthorityGateQuarantine, clock.Sample,
				clock.Initialized && !clock.Faulted)
		}
		return err
	}
	if quarantined {
		clock := state.clock.Evidence()
		state.recordGateBlocked(input, ReadAuthorityGateQuarantine, clock.Sample,
			clock.Initialized && !clock.Faulted)
		return ErrAuthorityElectionBlocked
	}
	state.observeQuarantineExpired()
	until, held, err := state.promise.PromiseUntil()
	if err != nil {
		state.recordGateBlocked(input, ReadAuthorityGateClockFault, 0, false)
		return err
	}
	if !held {
		state.recordGateAllowed(input, false)
		return nil
	}
	now, err := state.clock.Now()
	if err != nil {
		state.recordGateBlocked(input, ReadAuthorityGateClockFault, 0, false)
		return err
	}
	if now >= until {
		state.recordGateAllowed(input, false)
		return nil
	}
	// A current leader must continue logical ticks to send ordinary
	// heartbeats and renew the authority. Ticks on followers remain election
	// edges and are refused for the duration of the voter promise.
	if input == raftmodel.ElectionTickInput && runtime.node.Status().RaftState == raft.StateLeader {
		state.recordGateAllowed(input, true)
		return nil
	}
	state.recordGateBlocked(input, ReadAuthorityGateLivePromise, now, true)
	return ErrAuthorityElectionBlocked
}

// readAuthorityObservation builds the current local observation and returns a
// partially populated value when a resolver or configuration check prevents a
// complete authority proof. The public protocol method keeps its historical
// all-or-nothing contract; diagnostics use the partial value so an unknown
// state remains visible alongside the current term/leader/configuration.
func (runtime *Runtime) readAuthorityObservation() (raftauthority.AuthorityObservation, error) {
	if err := runtime.checkUsable(); err != nil {
		return raftauthority.AuthorityObservation{}, err
	}
	status := runtime.node.Status()
	publication := runtime.node.Published()
	observation := raftauthority.AuthorityObservation{
		Group: authorityGroupKey(runtime.identity.Group), Term: status.GetTerm(),
		Leader: status.Lead, CurrentTermCommitted: runtime.node.CurrentTermCommitted(),
	}
	if publication.ConfState == nil {
		return observation, errors.New("raftmember: authority observation has nil ConfState")
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(publication.ConfState)
	if err != nil {
		return observation, fmt.Errorf("raftmember: encode authority ConfState: %w", err)
	}
	config := raftauthority.ConfigIdentity{
		AppliedVersion: publication.ReplicaSetVersion,
		Digest:         sha256.Sum256(encoded),
		Joint: len(publication.ConfState.GetVotersOutgoing()) != 0 ||
			len(publication.ConfState.GetLearnersNext()) != 0 || publication.ConfState.GetAutoLeave(),
		Pending: runtime.node.PendingConfiguration(),
	}
	observation.Config = config
	observation.Stable = !config.Joint && !config.Pending
	if runtime.authority != nil && !runtime.authorityVotersMatch(runtime.authority.policy.Voters) {
		return observation, ErrAuthorityConfigurationMismatch
	}
	if status.Lead == runtime.identity.MemberID {
		observation.LeaderIncarnation = runtime.identity.NodeIncarnation
	} else if runtime.authority != nil && runtime.authority.leaderIncarnation != nil && status.Lead != 0 {
		var found bool
		observation.LeaderIncarnation, found, err = runtime.authority.leaderIncarnation(status.Lead)
		if err != nil {
			return observation, err
		}
		if !found || observation.LeaderIncarnation == 0 {
			return observation, ErrAuthorityLeaderIncarnationUnavailable
		}
	} else if runtime.authority != nil && !runtime.authority.disabled && status.Lead != 0 {
		return observation, ErrAuthorityLeaderIncarnationUnavailable
	}
	return observation, nil
}

// ReadAuthorityObservation returns a fresh serialized observation for holder
// admission, grant validation, and final token revalidation. Config digest is
// deterministic over the published ConfState and the pending/applied flags
// are deliberately conservative.
func (runtime *Runtime) ReadAuthorityObservation() (raftauthority.AuthorityObservation, error) {
	observation, err := runtime.readAuthorityObservation()
	if err != nil {
		return raftauthority.AuthorityObservation{}, err
	}
	return observation, nil
}

func (runtime *Runtime) authorityVotersMatch(expected []uint64) bool {
	publication := runtime.node.Published()
	if publication.ConfState == nil || len(publication.ConfState.GetVotersOutgoing()) != 0 ||
		len(publication.ConfState.GetLearnersNext()) != 0 || publication.ConfState.GetAutoLeave() {
		return false
	}
	actual := publication.ConfState.GetVoters()
	if len(actual) != len(expected) {
		return false
	}
	for index, member := range expected {
		if actual[index] != member {
			return false
		}
	}
	return true
}

// StartReadAuthorityRound begins one explicit holder round. It is called by
// the serialized owner at the authority-request send boundary; each remote
// request is retained in a bounded Runtime outbound list until Host transfers
// ownership to the authenticated transport.
func (runtime *Runtime) StartReadAuthorityRound() error {
	if runtime == nil {
		return ErrRuntimeClosed
	}
	state := runtime.authority
	if state == nil || state.disabled {
		// Preserve the disabled-policy ordering: ordinary callers still observe
		// the existing Ready/settlement error before the opt-in feature reports
		// that it is disabled.
		if err := runtime.requireEmptyInputWindow(); err != nil {
			return err
		}
		return raftauthority.ErrPolicyDisabled
	}
	if err := runtime.checkUsable(); err != nil {
		return err
	}
	if runtime.transferPending() {
		return ErrAuthorityTransferPending
	}
	if err := runtime.requireEmptyInputWindow(); err != nil {
		return err
	}
	status := runtime.node.Status()
	if status.RaftState != raft.StateLeader || status.Lead != runtime.identity.MemberID {
		return raftmodel.ErrNotLeader
	}
	observation, err := runtime.ReadAuthorityObservation()
	if err != nil {
		return err
	}
	if observation.Leader != runtime.identity.MemberID || !observation.CurrentTermCommitted || !observation.Stable {
		return raftauthority.ErrObservationStale
	}
	if state.round != nil {
		expired, expiredErr := state.round.Expired()
		if expiredErr != nil {
			return expiredErr
		}
		if expired {
			state.round = nil
			if state.renewal == nil {
				state.outbound = state.outbound[:0]
			}
		}
	}
	if state.renewal != nil {
		expired, expiredErr := state.renewal.Expired()
		if expiredErr != nil {
			return expiredErr
		}
		if expired {
			state.renewal = nil
			state.outbound = state.outbound[:0]
		}
	}
	if state.renewal != nil && state.renewal.HasQuorum() {
		// Promote a completed candidate before deciding whether another round
		// may start. The old token remains valid until its own deadline, and the
		// next call can acquire a further renewal without ever retaining more
		// than two rounds.
		state.round = state.renewal
		state.renewal = nil
		state.outbound = state.outbound[:0]
		return nil
	}
	if state.renewal != nil {
		// An unfinished renewal is the one bounded acquisition for this holder.
		// Do not replace it with a stream of never-completing rounds after the
		// old token expires.
		return ErrAuthorityRoundActive
	}
	if state.round != nil && !state.round.HasQuorum() {
		// Keep one unfinished acquisition intact until its deadline. A grant
		// may still arrive through the ordinary bounded input queue.
		return ErrAuthorityRoundActive
	}
	if state.nonce == math.MaxUint64 {
		return raftauthority.ErrInvalidRequest
	}
	state.nonce++
	round, request, err := raftauthority.NewAuthorityRound(
		state.clock, authorityGroupKey(runtime.identity.Group), runtime.identity.MemberID,
		runtime.identity.NodeIncarnation, status.GetTerm(), observation.Config,
		observation, state.policy, state.nonce,
	)
	if err != nil {
		return err
	}
	selfGrant, err := state.promise.Grant(request, observation)
	if err != nil {
		return err
	}
	if err := round.AddGrant(selfGrant); err != nil {
		return err
	}
	state.grantsAccepted.Add(1)
	if state.round == nil {
		state.round = round
	} else {
		state.renewal = round
	}
	state.outbound = state.outbound[:0]
	for _, voter := range state.policy.Voters {
		if voter == runtime.identity.MemberID {
			continue
		}
		requestCopy := request
		state.outbound = append(state.outbound, OutboundMessage{
			Group: runtime.identity.Group, From: runtime.identity.MemberID, To: voter,
			Authority: &raftauthority.Message{Kind: raftauthority.MessageRequest, Request: requestCopy},
		})
	}
	state.roundsStarted.Add(1)
	state.requestsCreated.Add(uint64(len(state.outbound)))
	return nil
}

// EnsureReadAuthorityRound offers one bounded renewal/acquisition opportunity
// for a read owner. A usable token is reused until its conservative renewal
// lead window; repeated reads outside that window do not emit quorum traffic.
// Once due, StartReadAuthorityRound retains the existing token and permits at
// most one newer candidate, so a slow or partitioned quorum cannot create an
// unbounded stream of rounds.
func (runtime *Runtime) EnsureReadAuthorityRound() error {
	if runtime == nil {
		return ErrRuntimeClosed
	}
	state := runtime.authority
	if state == nil || state.disabled {
		// Keep the existing disabled-policy path unchanged while enabled
		// transfers can be checked before any Ready-window retry.
		if err := runtime.requireEmptyInputWindow(); err != nil {
			return err
		}
		return raftauthority.ErrPolicyDisabled
	}
	if err := runtime.checkUsable(); err != nil {
		return err
	}
	if runtime.transferPending() {
		return ErrAuthorityTransferPending
	}
	if err := runtime.requireEmptyInputWindow(); err != nil {
		return err
	}
	token, err := runtime.ReadAuthorityToken()
	if err == nil {
		now, nowErr := state.clock.Now()
		if nowErr != nil {
			return nowErr
		}
		usable, usableErr := state.policy.UsableDuration()
		if usableErr != nil {
			return usableErr
		}
		// A quarter of the conservative serving window gives the quorum a
		// bounded renewal lead without turning every read into a round. Keep
		// at least the configured rounding margin so a sub-millisecond policy
		// cannot schedule a renewal after its usable deadline.
		lead := usable / 4
		if lead < state.policy.RoundingMargin {
			lead = state.policy.RoundingMargin
		}
		if token.ExpiresAt > now && token.ExpiresAt-now > lead {
			return nil
		}
	} else if errors.Is(err, raftauthority.ErrClockFault) ||
		errors.Is(err, raftauthority.ErrClockUnavailable) ||
		errors.Is(err, raftauthority.ErrClockRollback) ||
		errors.Is(err, raftauthority.ErrDeadlineOverflow) {
		// A clock fault is permanent for this owner. Do not turn it into an
		// apparently successful fallback or keep retrying rounds.
		return err
	}
	return runtime.StartReadAuthorityRound()
}

// DrainAuthorityOutbound transfers one detached request to the Host. The
// caller owns the returned value only until it passes it to the transport or
// makes its own value copy.
func (runtime *Runtime) DrainAuthorityOutbound() (OutboundMessage, bool) {
	if runtime == nil || runtime.authority == nil || len(runtime.authority.outbound) == 0 {
		return OutboundMessage{}, false
	}
	message := runtime.authority.outbound[0]
	runtime.authority.outbound[0] = OutboundMessage{}
	runtime.authority.outbound = runtime.authority.outbound[1:]
	return message, true
}

// AuthorityOutboundPending is a non-destructive scheduler preflight. Host uses
// it before applying outbox capacity backpressure so a configured runtime with
// no authority traffic never stalls ordinary Raft work.
func (runtime *Runtime) AuthorityOutboundPending() bool {
	return runtime != nil && runtime.authority != nil && len(runtime.authority.outbound) != 0
}

// ReadAuthorityToken returns the current holder capability after a fresh
// observation. The caller must perform ValidateReadAuthorityToken in the
// serialized owner immediately before exposing a new data cut.
func (runtime *Runtime) ReadAuthorityToken() (raftauthority.AuthorityToken, error) {
	if err := runtime.checkUsable(); err != nil {
		return raftauthority.AuthorityToken{}, err
	}
	state := runtime.authority
	if state == nil || state.disabled {
		return raftauthority.AuthorityToken{}, raftauthority.ErrPolicyDisabled
	}
	if runtime.transferPending() {
		return raftauthority.AuthorityToken{}, ErrAuthorityTransferPending
	}
	observation, err := runtime.ReadAuthorityObservation()
	if err != nil {
		return raftauthority.AuthorityToken{}, err
	}
	// A completed renewal is the freshest bounded authority. Keep the old
	// round usable while it remains valid so a renewal cannot introduce a read
	// gap, but prefer the newer token whenever its observation is still exact.
	if state.renewal != nil {
		expired, expiredErr := state.renewal.Expired()
		if expiredErr != nil {
			return raftauthority.AuthorityToken{}, expiredErr
		}
		if expired {
			state.renewal = nil
		} else if state.renewal.HasQuorum() {
			if token, tokenErr := state.renewal.Token(observation); tokenErr == nil {
				return token, nil
			}
		}
	}
	if state.round == nil {
		return raftauthority.AuthorityToken{}, raftauthority.ErrNotQuorum
	}
	expired, expiredErr := state.round.Expired()
	if expiredErr != nil {
		return raftauthority.AuthorityToken{}, expiredErr
	}
	if expired {
		state.round = nil
		return raftauthority.AuthorityToken{}, raftauthority.ErrRoundExpired
	}
	return state.round.Token(observation)
}

// ValidateReadAuthorityToken performs the final owner-side check against the
// exact current term/incarnation/configuration before a fresh read cut is
// exposed. An already pinned immutable cut may outlive this token.
func (runtime *Runtime) ValidateReadAuthorityToken(token raftauthority.AuthorityToken) error {
	if err := runtime.checkUsable(); err != nil {
		return err
	}
	state := runtime.authority
	if state == nil || state.disabled {
		return raftauthority.ErrPolicyDisabled
	}
	if runtime.transferPending() {
		return ErrAuthorityTransferPending
	}
	observation, err := runtime.ReadAuthorityObservation()
	if err != nil {
		return err
	}
	// Select by the immutable nonce before validating. This keeps one old
	// token and one pending/new token bounded without accepting a token from an
	// unrelated round that happens to share the current term and config.
	if state.renewal != nil && token.Nonce == state.renewal.Request().Nonce {
		return state.renewal.ValidateToken(token, observation)
	}
	if state.round != nil && token.Nonce == state.round.Request().Nonce {
		return state.round.ValidateToken(token, observation)
	}
	return raftauthority.ErrObservationStale
}

// StepAuthorityMessage handles one authenticated control record in the same
// uncaptured-Ready window as ordinary Raft input. Requests are granted only
// from the exact current observation; grants are accepted only by the exact
// live round. Invalid/stale/replayed records are benign drops, while clock or
// ownership faults remain visible to the owner.
func (runtime *Runtime) StepAuthorityMessage(
	message *raftauthority.Message,
) (OutboundMessage, bool, error) {
	return runtime.StepAuthorityMessageFrom(0, message)
}

// StepAuthorityMessageFrom is the authenticated transport entry point. The
// source member is checked against the protocol union before any promise
// state can change.
func (runtime *Runtime) StepAuthorityMessageFrom(
	source uint64,
	message *raftauthority.Message,
) (OutboundMessage, bool, error) {
	if err := runtime.requireEmptyInputWindow(); err != nil {
		return OutboundMessage{}, false, err
	}
	state := runtime.authority
	if state == nil || state.disabled {
		return OutboundMessage{}, false, raftauthority.ErrPolicyDisabled
	}
	if message == nil {
		return OutboundMessage{}, false, raftauthority.ErrInvalidWire
	}
	if message.Request.Group != authorityGroupKey(runtime.identity.Group) {
		return OutboundMessage{}, false, raftauthority.ErrInvalidRequest
	}
	switch message.Kind {
	case raftauthority.MessageRequest:
		if source == 0 || source != message.Request.Holder {
			return OutboundMessage{}, false, raftauthority.ErrInvalidRequest
		}
	case raftauthority.MessageGrant:
		if source == 0 || source != message.Grant.Voter {
			return OutboundMessage{}, false, raftauthority.ErrInvalidGrant
		}
	default:
		return OutboundMessage{}, false, raftauthority.ErrInvalidWire
	}
	observation, err := runtime.ReadAuthorityObservation()
	if err != nil {
		// A topology cache miss or a just-published voter-set change only
		// invalidates this authority record.  Treat it as a protocol drop so a
		// stale authority request cannot stall the Host's ordinary Raft lane;
		// the caller will keep using the quorum-backed ReadIndex path.  Clock
		// faults and ownership failures remain visible and fail closed.
		if errors.Is(err, ErrAuthorityLeaderIncarnationUnavailable) ||
			errors.Is(err, ErrAuthorityConfigurationMismatch) {
			return OutboundMessage{}, false, nil
		}
		return OutboundMessage{}, false, err
	}
	switch message.Kind {
	case raftauthority.MessageRequest:
		if message.Request.Holder == runtime.identity.MemberID {
			return OutboundMessage{}, false, nil
		}
		if observation.Leader != message.Request.Holder ||
			observation.LeaderIncarnation != message.Request.HolderIncarnation {
			return OutboundMessage{}, false, nil
		}
		grant, grantErr := state.promise.Grant(message.Request, observation)
		if grantErr != nil {
			if authorityMessageDrop(grantErr) {
				return OutboundMessage{}, false, nil
			}
			return OutboundMessage{}, false, grantErr
		}
		return OutboundMessage{
			Group: runtime.identity.Group, From: runtime.identity.MemberID, To: grant.Request.Holder,
			Authority: &raftauthority.Message{Kind: raftauthority.MessageGrant, Request: grant.Request, Grant: grant},
		}, true, nil
	case raftauthority.MessageGrant:
		if message.Request.Holder != runtime.identity.MemberID {
			return OutboundMessage{}, false, nil
		}
		if message.Grant.Request != message.Request {
			return OutboundMessage{}, false, nil
		}
		var round *raftauthority.AuthorityRound
		if state.round != nil && state.round.Request() == message.Request {
			round = state.round
		} else if state.renewal != nil && state.renewal.Request() == message.Request {
			round = state.renewal
		}
		if round == nil {
			return OutboundMessage{}, false, nil
		}
		if err := round.AddGrant(message.Grant); err != nil {
			if authorityMessageDrop(err) {
				return OutboundMessage{}, false, nil
			}
			return OutboundMessage{}, false, err
		}
		state.grantsAccepted.Add(1)
		return OutboundMessage{}, false, nil
	default:
		return OutboundMessage{}, false, nil
	}
}

func authorityMessageDrop(err error) bool {
	return errors.Is(err, raftauthority.ErrInvalidRequest) ||
		errors.Is(err, raftauthority.ErrInvalidGrant) ||
		errors.Is(err, raftauthority.ErrPromiseHeld) ||
		errors.Is(err, raftauthority.ErrStaleRequest) ||
		errors.Is(err, raftauthority.ErrObservationStale) ||
		errors.Is(err, raftauthority.ErrNotVoter) ||
		errors.Is(err, raftauthority.ErrRoundExpired) ||
		errors.Is(err, raftauthority.ErrRoundComplete) ||
		errors.Is(err, raftauthority.ErrRoundInvalidated)
}

func cloneAuthorityPolicy(policy raftauthority.ReadAuthorityPolicy) raftauthority.ReadAuthorityPolicy {
	policy.Voters = append([]uint64(nil), policy.Voters...)
	policy.Capabilities = append([]raftauthority.VoterCapability(nil), policy.Capabilities...)
	return policy
}

func (runtime *Runtime) refreshAuthority() {
	runtime.refreshLeaderTransfer()
	state := runtime.authority
	if state == nil || (state.round == nil && state.renewal == nil) {
		return
	}
	observation, err := runtime.ReadAuthorityObservation()
	valid := func(round *raftauthority.AuthorityRound) bool {
		if round == nil {
			return true
		}
		request := round.Request()
		return err == nil && observation.Group == request.Group &&
			observation.Term == request.Term && observation.Leader == request.Holder &&
			observation.LeaderIncarnation == request.HolderIncarnation &&
			observation.Config == request.Config && observation.CurrentTermCommitted &&
			observation.Stable
	}
	if !valid(state.round) || !valid(state.renewal) {
		if state.round != nil {
			state.round.Invalidate()
		}
		if state.renewal != nil {
			state.renewal.Invalidate()
		}
		state.outbound = state.outbound[:0]
	}
}

func (runtime *Runtime) transferPending() bool {
	if runtime == nil {
		return false
	}
	runtime.refreshLeaderTransfer()
	return runtime.leaderTransfer != nil
}

// invalidateAuthority is called before topology-changing operations and after
// every ordinary protocol edge that can change term, leader, or publication.
func (runtime *Runtime) invalidateAuthority() {
	if runtime != nil && runtime.authority != nil {
		if runtime.authority.round != nil {
			runtime.authority.round.Invalidate()
			runtime.authority.round = nil
		}
		if runtime.authority.renewal != nil {
			runtime.authority.renewal.Invalidate()
			runtime.authority.renewal = nil
		}
		runtime.authority.outbound = runtime.authority.outbound[:0]
	}
}
