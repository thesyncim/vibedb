package gateway

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/distributedtxn"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibejson/x/byteview"
)

var (
	ErrReplicatedTransaction          = errors.New("gateway: invalid replicated transaction")
	ErrReplicatedTransactionBound     = errors.New("gateway: replicated transaction exceeds its bound")
	ErrReplicatedTransactionConflict  = errors.New("gateway: replicated transaction mutation conflict")
	ErrReplicatedTransactionUnknown   = errors.New("gateway: replicated transaction outcome is unknown")
	ErrReplicatedTransactionCommitted = errors.New("gateway: replicated transaction committed but cleanup is incomplete")
)

const (
	maxReplicatedTransactionOrdinal = uint64(math.MaxUint32) + 1
	// AbsoluteMaxReplicatedTransactionConcurrency is a scheduler/resource
	// ceiling, not a participant limit. Wider transactions advance in waves.
	AbsoluteMaxReplicatedTransactionConcurrency     = 256
	AbsoluteMaxReplicatedTransactionRecoveryTimeout = 30 * 24 * time.Hour
	// AbsoluteMaxReplicatedTransactionInFlightBytes is an admission-memory
	// ceiling, not a transaction-size or participant-count limit.
	AbsoluteMaxReplicatedTransactionInFlightBytes = uint64(1 << 30)
	maxReplicatedTransactionIDAttempts            = 4
	// replicatedTransactionRecoveryPulseLimit is the number of distinct,
	// quorum-committed recovery observations required before a staging
	// coordinator may be aborted. Wall time can schedule observations but can
	// never satisfy this authority.
	replicatedTransactionRecoveryPulseLimit = distributedtxn.MaxRecoveryPulses
	maxReplicatedTransactionMutations       = uint64(replication.MaxMutations) *
		maxReplicatedTransactionOrdinal
	maxReplicatedTransactionMutationBytes = uint64(replication.MaxCommandBytes) *
		maxReplicatedTransactionOrdinal
	replicatedTransactionPendingLogicalBytes = uint64(unsafe.Sizeof(
		ReplicatedTransactionPendingCommand{},
	)) + uint64(unsafe.Sizeof(replicatedTransactionPendingReservation{})) +
		uint64(unsafe.Sizeof((*replicatedTransactionPendingReservation)(nil)))
	replicatedTransactionReservationLogicalBytes = uint64(unsafe.Sizeof(
		replicatedTransactionPendingReservation{},
	))
	replicatedTransactionOwnershipLogicalBytes = uint64(unsafe.Sizeof(
		replicatedTransactionRecoveryOwnership{},
	))
	replicatedTransactionRecoveryValidationBytes = uint64(distributedtxn.ManifestSegmentBytes) +
		uint64(unsafe.Sizeof(distributedtxn.ManifestBuilder{})) +
		uint64(unsafe.Sizeof([distributedtxn.MaxInlineParticipants]distributedtxn.ParticipantRef{})) +
		uint64(distributedtxn.MaxManifestPageParticipants)*
			uint64(unsafe.Sizeof(distributedtxn.ParticipantRef{})) +
		2*uint64(distributedtxn.MaxShardIdentityBytes)*
			uint64(distributedtxn.MaxManifestPageParticipants) +
		uint64(unsafe.Sizeof(distributedtxn.ManifestReader{}))
)

// ReplicatedTransactionParticipant is one already grouped, byte-native shard
// mutation. Route and every borrowed batch byte must remain immutable until
// Execute returns. Relation IDs are authenticated by Route.Command's schema
// generation; neither SQL text nor relation names enter this boundary.
type ReplicatedTransactionParticipant struct {
	Route        ReplicatedRoute
	Batches      []replication.RelationMutationBatch
	BucketBits   uint8
	IntentScopes []distributedtxn.IntentScope
}

// ReplicatedTransactionOrchestratorOptions bounds all retained and concurrent
// transaction work. Participant count has no policy cap: canonical manifest
// bytes, mutation bytes, and the uint32 wire ordinal are the representation
// bounds.
type ReplicatedTransactionOrchestratorOptions struct {
	Executor         *ReplicatedExecutor
	Tenant           []byte
	RetryHome        replication.RetryHome
	MaxConcurrency   int
	MaxInFlightBytes uint64
	MaxMutations     uint64
	MaxMutationBytes uint64
	RecoveryTimeout  time.Duration
	// RecoveryAuthority is the gateway service identity used only while
	// recovering an already admitted transaction. It prevents a client retry
	// from forwarding the client's narrower data authority to hidden
	// transaction-control reads. A zero value is accepted only for an
	// unauthenticated in-process embedding; Recover fails closed if its caller
	// carries an authority and no distinct recovery identity is configured.
	RecoveryAuthority serviceauthz.Authority
	IDSource          io.Reader
}

type ReplicatedTransactionOrchestrator struct {
	executor         *ReplicatedExecutor
	tenant           []byte
	retryHome        replication.RetryHome
	maxConcurrency   int
	maxInFlightBytes uint64
	// maxWorkerRetainedBytes is zero for production orchestrators: propose grows
	// reusable worker scratch by exact command size opportunistically. Tests and
	// microbenchmarks may seed a fixed reservation to isolate runWave behavior.
	maxWorkerRetainedBytes int
	maxMutations           uint64
	maxMutationBytes       uint64
	recoveryTimeout        time.Duration
	recoveryAuthority      serviceauthz.Authority
	idSource               io.Reader
	byteBudget             replicatedTransactionByteBudget
	activeByteBudget       replicatedTransactionByteBudget
}

// ReplicatedTransactionPhase is the durable protocol cut represented by a
// recovery handle.
type ReplicatedTransactionPhase uint8

const (
	ReplicatedTransactionPhaseInvalid ReplicatedTransactionPhase = iota
	ReplicatedTransactionPhaseBeginning
	ReplicatedTransactionPhasePreparing
	ReplicatedTransactionPhaseDeciding
	ReplicatedTransactionPhaseCommitted
	ReplicatedTransactionPhaseAborted
	ReplicatedTransactionPhaseFinishing
	ReplicatedTransactionPhaseTerminal
)

// ReplicatedTransactionRouteWitness owns one exact allocation and its latest
// validated group-local applied cut. Digest and ordinal bind it byte-for-byte
// to the coordinator participant manifest.
type ReplicatedTransactionRouteWitness struct {
	Route            ReplicatedRoute
	Ordinal          uint32
	AuthorityWitness distributedtxn.AuthorityWitness
	MutationDigest   distributedtxn.Digest
	MinimumApplied   uint64
	Prepared         bool
	Terminal         bool
}

// ReplicatedTransactionPendingCommand retains the only bytes that may settle
// an outcome-unknown proposal. Recovery must call RetryUnknown with this exact
// route and byte slice; rebuilding the command is forbidden.
type ReplicatedTransactionPendingCommand struct {
	Route   ReplicatedRoute
	Ordinal uint32
	Command []byte

	// reservation is shared by shallow recovery-handle copies. Its atomic
	// release makes clearing the same pending ownership twice harmless while
	// keeping the exact backing-buffer capacity charged after Execute returns.
	reservation *replicatedTransactionPendingReservation
}

// ReplicatedTransactionRecoveryHandle is detached, exact retry and routing
// material. Replicated coordinator/participant records remain the decision
// authority; this handle never substitutes a process-local journal for them.
type ReplicatedTransactionRecoveryHandle struct {
	ID distributedtxn.ID
	// RetryHome is part of the persisted transaction identity. Recovery and
	// byte-identical retries must never substitute the current process default.
	RetryHome                 replication.RetryHome
	CatalogGeneration         uint64
	Phase                     ReplicatedTransactionPhase
	CoordinatorOrdinal        uint32
	DecisionRevision          uint64
	CoordinatorMinimumApplied uint64
	// RecoveryDeadline is the persisted logical pulse limit, not wall time.
	RecoveryDeadline          int64
	Participants              []ReplicatedTransactionRouteWitness
	Pending                   []ReplicatedTransactionPendingCommand

	ownership *replicatedTransactionRecoveryOwnership
	journal   ReplicatedTransactionJournal
}

// ReplicatedTransactionResult reports a proved outcome. Recovery is non-nil
// only when the outcome is committed but terminal cleanup could not complete.
type ReplicatedTransactionResult struct {
	ID           distributedtxn.ID
	Committed    bool
	AffectedRows int64
	Recovery     *ReplicatedTransactionRecoveryHandle
}

// ReplicatedTransactionError carries exact recovery material whenever a
// proposal may have crossed Raft admission. Committed distinguishes outcome
// recovery from terminal cleanup recovery.
type ReplicatedTransactionError struct {
	ID        distributedtxn.ID
	Committed bool
	Recovery  *ReplicatedTransactionRecoveryHandle
	Cause     error
}

func (err *ReplicatedTransactionError) Error() string {
	if err == nil {
		return ErrReplicatedTransaction.Error()
	}
	if err.Cause == nil {
		if err.Committed {
			return ErrReplicatedTransactionCommitted.Error()
		}
		return ErrReplicatedTransactionUnknown.Error()
	}
	if err.Committed {
		return ErrReplicatedTransactionCommitted.Error() + ": " + err.Cause.Error()
	}
	return ErrReplicatedTransactionUnknown.Error() + ": " + err.Cause.Error()
}

func (err *ReplicatedTransactionError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

func NewReplicatedTransactionOrchestrator(
	options ReplicatedTransactionOrchestratorOptions,
) (*ReplicatedTransactionOrchestrator, error) {
	if options.Executor == nil || len(options.Tenant) == 0 ||
		len(options.Tenant) > replication.MaxIdentityBytes ||
		options.MaxConcurrency <= 0 || options.MaxMutations == 0 ||
		options.MaxConcurrency > AbsoluteMaxReplicatedTransactionConcurrency ||
		options.MaxInFlightBytes < uint64(replication.MaxCommandBytes)+
			uint64(distributedtxn.MaxReplicatedCommandBytes)+
			replicatedTransactionPendingLogicalBytes+
			replicatedTransactionRecoveryValidationBytes ||
		options.MaxInFlightBytes > AbsoluteMaxReplicatedTransactionInFlightBytes ||
		options.MaxMutations > maxReplicatedTransactionMutations ||
		options.MaxMutationBytes == 0 ||
		options.MaxMutationBytes > maxReplicatedTransactionMutationBytes ||
		options.RecoveryTimeout <= 0 ||
		options.RecoveryTimeout > AbsoluteMaxReplicatedTransactionRecoveryTimeout ||
		(options.RecoveryAuthority != (serviceauthz.Authority{}) &&
			!options.RecoveryAuthority.Valid()) {
		return nil, ErrReplicatedTransaction
	}
	source := options.IDSource
	if source == nil {
		source = cryptorand.Reader
	}
	orchestrator := &ReplicatedTransactionOrchestrator{
		executor: options.Executor, tenant: bytes.Clone(options.Tenant),
		retryHome: options.RetryHome, maxConcurrency: options.MaxConcurrency,
		maxInFlightBytes: options.MaxInFlightBytes,
		maxMutations:     options.MaxMutations, maxMutationBytes: options.MaxMutationBytes,
		recoveryTimeout:   options.RecoveryTimeout,
		recoveryAuthority: options.RecoveryAuthority, idSource: source,
	}
	activeBytes := uint64(replication.MaxCommandBytes) +
		uint64(distributedtxn.MaxReplicatedCommandBytes) +
		replicatedTransactionPendingLogicalBytes +
		replicatedTransactionRecoveryValidationBytes
	retainedBytes := options.MaxInFlightBytes - activeBytes
	orchestrator.byteBudget.reset(retainedBytes)
	orchestrator.activeByteBudget.reset(activeBytes)
	orchestrator.activeByteBudget.reserve = replicatedTransactionRecoveryValidationBytes
	return orchestrator, nil
}

type replicatedTransactionByteBudget struct {
	mu      sync.Mutex
	limit   uint64
	reserve uint64
	used    uint64
	peak    uint64
	notify  chan struct{}
}

func (budget *replicatedTransactionByteBudget) reset(limit uint64) {
	budget.limit = limit
	budget.reserve = 0
	budget.used = 0
	budget.peak = 0
	budget.notify = make(chan struct{})
}

func (budget *replicatedTransactionByteBudget) acquire(ctx context.Context, size uint64) error {
	if size == 0 || budget.reserve > budget.limit || size > budget.limit-budget.reserve {
		return ErrReplicatedTransactionBound
	}
	for {
		budget.mu.Lock()
		if budget.used <= budget.limit-budget.reserve &&
			size <= budget.limit-budget.reserve-budget.used {
			budget.used += size
			budget.peak = max(budget.peak, budget.used)
			budget.mu.Unlock()
			return nil
		}
		notify := budget.notify
		budget.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

// acquireReserved admits the recovery validator into the partition that
// normal proposals cannot consume. Thus outcome-unknown active commands never
// block the validation required to release their own reservations.
func (budget *replicatedTransactionByteBudget) acquireReserved(
	ctx context.Context,
	size uint64,
) error {
	if size == 0 || size > budget.reserve {
		return ErrReplicatedTransactionBound
	}
	for {
		budget.mu.Lock()
		if size <= budget.limit-budget.used {
			budget.used += size
			budget.peak = max(budget.peak, budget.used)
			budget.mu.Unlock()
			return nil
		}
		notify := budget.notify
		budget.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

func (budget *replicatedTransactionByteBudget) tryAcquire(size uint64) bool {
	if size == 0 {
		return true
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.reserve > budget.limit || budget.used > budget.limit-budget.reserve ||
		size > budget.limit-budget.reserve-budget.used {
		return false
	}
	budget.used += size
	budget.peak = max(budget.peak, budget.used)
	return true
}

func (budget *replicatedTransactionByteBudget) release(size uint64) {
	if size == 0 {
		return
	}
	budget.mu.Lock()
	if size > budget.used {
		budget.mu.Unlock()
		panic("gateway: replicated transaction byte budget underflow")
	}
	budget.used -= size
	close(budget.notify)
	budget.notify = make(chan struct{})
	budget.mu.Unlock()
}

type replicatedTransactionPendingReservation struct {
	budget      *replicatedTransactionByteBudget
	budgetBytes uint64
	spillBudget *replicatedTransactionByteBudget
	spillBytes  uint64
	bytes       uint64
	released    atomic.Bool
}

// replicatedTransactionRecoveryOwnership is the private shared lease registry
// for one detached handle. It is independent of the exported mutable Pending
// slice, so dropping or duplicating a public entry cannot orphan quota.
type replicatedTransactionRecoveryOwnership struct {
	handle  *replicatedTransactionPendingReservation
	pending []*replicatedTransactionPendingReservation
}

func (ownership *replicatedTransactionRecoveryOwnership) addPending(
	reservation *replicatedTransactionPendingReservation,
) {
	if ownership == nil || reservation == nil {
		panic("gateway: invalid replicated transaction pending ownership")
	}
	ownership.pending = append(ownership.pending, reservation)
}

func (ownership *replicatedTransactionRecoveryOwnership) releasePending(
	reservation *replicatedTransactionPendingReservation,
) bool {
	if ownership == nil || reservation == nil {
		return false
	}
	for index := range ownership.pending {
		if ownership.pending[index] != reservation {
			continue
		}
		reservation.release()
		last := len(ownership.pending) - 1
		ownership.pending[index] = ownership.pending[last]
		ownership.pending[last] = nil
		ownership.pending = ownership.pending[:last]
		return true
	}
	return false
}

func (ownership *replicatedTransactionRecoveryOwnership) releaseAll() {
	if ownership == nil {
		return
	}
	for index := range ownership.pending {
		ownership.pending[index].release()
		ownership.pending[index] = nil
	}
	ownership.pending = nil
	ownership.handle.release()
	ownership.handle = nil
}

// adoptExternalRecoveryHandle atomically admits public recovery material that
// crossed a gateway/process boundary without private lease pointers. It also
// reserves the fixed validation workspace, avoiding a hold-and-wait cycle
// between the retained and active pools.
func (orchestrator *ReplicatedTransactionOrchestrator) adoptExternalRecoveryHandle(
	handle *ReplicatedTransactionRecoveryHandle,
) (bool, error) {
	if handle == nil || handle.ownership != nil {
		return false, nil
	}
	// Pending width is bounded by the producing scheduler, not by this recovery
	// process's worker count. A C=1 replacement must be able to drain a handle
	// emitted by the maximum supported producer concurrency.
	if len(handle.Pending) > 2*AbsoluteMaxReplicatedTransactionConcurrency+1 {
		return false, ErrReplicatedTransactionBound
	}
	handleBytes, err := replicatedTransactionRecoveryHandleLogicalBytes(handle)
	if err != nil {
		return false, err
	}
	var pendingTotal uint64
	for index := range handle.Pending {
		if handle.Pending[index].reservation != nil ||
			int(handle.Pending[index].Ordinal) >= len(handle.Participants) ||
			len(handle.Pending[index].Command) == 0 ||
			len(handle.Pending[index].Command) > replication.MaxCommandBytes {
			return false, ErrReplicatedTransaction
		}
		if !equalReplicatedTransactionRoute(
			handle.Pending[index].Route,
			handle.Participants[handle.Pending[index].Ordinal].Route,
		) {
			return false, ErrReplicatedTransaction
		}
		pendingBytes := uint64(len(handle.Pending[index].Command)) +
			replicatedTransactionPendingLogicalBytes
		pendingTotal, err = checkedReplicatedTransactionLogicalSum(
			pendingTotal, pendingBytes,
		)
		if err != nil {
			return false, err
		}
	}

	orchestrator.byteBudget.mu.Lock()
	orchestrator.activeByteBudget.mu.Lock()
	if handleBytes > orchestrator.byteBudget.limit-orchestrator.byteBudget.used ||
		replicatedTransactionRecoveryValidationBytes >
			orchestrator.activeByteBudget.limit-orchestrator.activeByteBudget.used {
		orchestrator.activeByteBudget.mu.Unlock()
		orchestrator.byteBudget.mu.Unlock()
		return false, ErrReplicatedTransactionBound
	}
	retainedAvailable := orchestrator.byteBudget.limit -
		orchestrator.byteBudget.used - handleBytes
	activeAvailable := orchestrator.activeByteBudget.limit -
		orchestrator.activeByteBudget.used -
		replicatedTransactionRecoveryValidationBytes
	retainedPending := min(pendingTotal, retainedAvailable)
	activePending := pendingTotal - retainedPending
	if activePending > activeAvailable {
		orchestrator.activeByteBudget.mu.Unlock()
		orchestrator.byteBudget.mu.Unlock()
		return false, ErrReplicatedTransactionBound
	}
	retainedCharge := handleBytes + retainedPending
	activeCharge := replicatedTransactionRecoveryValidationBytes + activePending
	orchestrator.byteBudget.used += retainedCharge
	orchestrator.byteBudget.peak = max(
		orchestrator.byteBudget.peak, orchestrator.byteBudget.used,
	)
	orchestrator.activeByteBudget.used += activeCharge
	orchestrator.activeByteBudget.peak = max(
		orchestrator.activeByteBudget.peak, orchestrator.activeByteBudget.used,
	)
	orchestrator.activeByteBudget.mu.Unlock()
	orchestrator.byteBudget.mu.Unlock()

	ownership := &replicatedTransactionRecoveryOwnership{
		handle: newReplicatedTransactionPendingReservation(
			&orchestrator.byteBudget, handleBytes,
		),
		pending: make([]*replicatedTransactionPendingReservation, 0, len(handle.Pending)),
	}
	canonicalParticipants := make(
		[]ReplicatedTransactionRouteWitness, len(handle.Participants),
	)
	for index := range handle.Participants {
		canonicalParticipants[index] = handle.Participants[index]
		canonicalParticipants[index].Route = cloneReplicatedTransactionRoute(
			handle.Participants[index].Route,
		)
	}
	canonicalPending := make(
		[]ReplicatedTransactionPendingCommand, len(handle.Pending),
	)
	retainedRemaining := retainedPending
	for index := range handle.Pending {
		pendingBytes := uint64(len(handle.Pending[index].Command)) +
			replicatedTransactionPendingLogicalBytes
		retainedBytes := min(pendingBytes, retainedRemaining)
		activeBytes := pendingBytes - retainedBytes
		var reservation *replicatedTransactionPendingReservation
		switch {
		case activeBytes == 0:
			reservation = newReplicatedTransactionPendingReservation(
				&orchestrator.byteBudget, retainedBytes,
			)
		case retainedBytes == 0:
			reservation = newReplicatedTransactionPendingReservation(
				&orchestrator.activeByteBudget, activeBytes,
			)
		default:
			reservation = newSplitReplicatedTransactionPendingReservation(
				&orchestrator.byteBudget, retainedBytes,
				&orchestrator.activeByteBudget, activeBytes,
			)
		}
		retainedRemaining -= retainedBytes
		command := make([]byte, len(handle.Pending[index].Command))
		copy(command, handle.Pending[index].Command)
		canonicalPending[index] = ReplicatedTransactionPendingCommand{
			Route:   canonicalParticipants[handle.Pending[index].Ordinal].Route,
			Ordinal: handle.Pending[index].Ordinal,
			Command: command, reservation: reservation,
		}
		ownership.addPending(reservation)
	}
	handle.Participants = canonicalParticipants
	handle.Pending = canonicalPending
	handle.ownership = ownership
	return true, nil
}

func rollbackExternalRecoveryAdoption(handle *ReplicatedTransactionRecoveryHandle) {
	if handle == nil || handle.ownership == nil {
		return
	}
	handle.ownership.releaseAll()
	for index := range handle.Pending {
		clear(handle.Pending[index].Command)
		handle.Pending[index] = ReplicatedTransactionPendingCommand{}
	}
	clear(handle.Pending)
	handle.Pending = nil
	for index := range handle.Participants {
		clear(handle.Participants[index].Route.Replicas)
		handle.Participants[index] = ReplicatedTransactionRouteWitness{}
	}
	clear(handle.Participants)
	handle.Participants = nil
	handle.ownership = nil
	handle.Phase = ReplicatedTransactionPhaseInvalid
}

func newReplicatedTransactionPendingReservation(
	budget *replicatedTransactionByteBudget,
	bytes uint64,
) *replicatedTransactionPendingReservation {
	if budget == nil || bytes == 0 {
		panic("gateway: invalid replicated transaction pending reservation")
	}
	return &replicatedTransactionPendingReservation{
		budget: budget, budgetBytes: bytes, bytes: bytes,
	}
}

func newSplitReplicatedTransactionPendingReservation(
	primary *replicatedTransactionByteBudget,
	primaryBytes uint64,
	spill *replicatedTransactionByteBudget,
	spillBytes uint64,
) *replicatedTransactionPendingReservation {
	if primary == nil || spill == nil || primary == spill ||
		primaryBytes == 0 || spillBytes == 0 || spillBytes > math.MaxUint64-primaryBytes {
		panic("gateway: invalid split replicated transaction pending reservation")
	}
	return &replicatedTransactionPendingReservation{
		budget: primary, budgetBytes: primaryBytes,
		spillBudget: spill, spillBytes: spillBytes,
		bytes: primaryBytes + spillBytes,
	}
}

func (reservation *replicatedTransactionPendingReservation) release() {
	if reservation == nil || !reservation.released.CompareAndSwap(false, true) {
		return
	}
	reservation.budget.release(reservation.budgetBytes)
	if reservation.spillBudget != nil {
		reservation.spillBudget.release(reservation.spillBytes)
	}
}

// replicatedTransactionOwnedLogicalBytes defines the admission contract for
// transaction-owned metadata. It charges every logical byte in plan/handle
// backing arrays, cloned route strings, replica records, cloned endpoint
// strings, group-deduplication keys, and the largest live manifest-construction
// workspace. Go allocator headers, size-class rounding, and unused slice/map
// capacity are deliberately excluded; outcome-unknown command backing is
// charged separately by its actual capacity.
func replicatedTransactionOwnedLogicalBytes(
	participants []ReplicatedTransactionParticipant,
) (planBytes, handleBytes uint64, err error) {
	count := uint64(len(participants))
	if count == 0 || count > maxReplicatedTransactionOrdinal {
		return 0, 0, ErrReplicatedTransaction
	}
	planBytes, err = checkedReplicatedTransactionLogicalProduct(
		count, uint64(unsafe.Sizeof(replicatedTransactionPlanParticipant{})),
	)
	if err != nil {
		return 0, 0, err
	}
	groupEntries, err := checkedReplicatedTransactionLogicalProduct(
		count, uint64(unsafe.Sizeof(raftmember.GroupKey{}))+1,
	)
	if err != nil {
		return 0, 0, err
	}
	planBytes, err = checkedReplicatedTransactionLogicalSum(
		planBytes, uint64(unsafe.Sizeof(map[raftmember.GroupKey]struct{}{})), groupEntries,
		replicatedTransactionReservationLogicalBytes,
	)
	if err != nil {
		return 0, 0, err
	}
	if len(participants) <= distributedtxn.MaxInlineParticipants {
		inlineRefs, sizeErr := checkedReplicatedTransactionLogicalProduct(
			count, uint64(unsafe.Sizeof(distributedtxn.ParticipantRef{})),
		)
		if sizeErr != nil {
			return 0, 0, sizeErr
		}
		planBytes, err = checkedReplicatedTransactionLogicalSum(
			planBytes, inlineRefs, uint64(distributedtxn.MaxCoordinatorRecordBytes),
		)
	} else {
		planBytes, err = checkedReplicatedTransactionLogicalSum(
			planBytes,
			uint64(distributedtxn.ManifestSegmentBytes),
			2*uint64(distributedtxn.MaxManifestSegmentSequenceBytes),
			2*uint64(distributedtxn.ReplicatedManifestCoordinatorRecordBytes),
		)
	}
	if err != nil {
		return 0, 0, err
	}

	handleBytes = uint64(unsafe.Sizeof(ReplicatedTransactionRecoveryHandle{})) +
		replicatedTransactionReservationLogicalBytes +
		replicatedTransactionOwnershipLogicalBytes
	witnessBytes, err := checkedReplicatedTransactionLogicalProduct(
		count, uint64(unsafe.Sizeof(ReplicatedTransactionRouteWitness{})),
	)
	if err != nil {
		return 0, 0, err
	}
	handleBytes, err = checkedReplicatedTransactionLogicalSum(handleBytes, witnessBytes)
	if err != nil {
		return 0, 0, err
	}
	for index := range participants {
		route := &participants[index].Route
		replicaBytes, sizeErr := checkedReplicatedTransactionLogicalProduct(
			uint64(len(route.Replicas)), uint64(unsafe.Sizeof(ReplicatedEndpoint{})),
		)
		if sizeErr != nil {
			return 0, 0, sizeErr
		}
		handleBytes, err = checkedReplicatedTransactionLogicalSum(
			handleBytes, uint64(len(route.Distribution)), uint64(len(route.Shard)), replicaBytes,
		)
		if err != nil {
			return 0, 0, err
		}
		for replicaIndex := range route.Replicas {
			replica := &route.Replicas[replicaIndex]
			handleBytes, err = checkedReplicatedTransactionLogicalSum(
				handleBytes,
				uint64(len(replica.NativeEndpoint)), uint64(len(replica.Address)),
				uint64(len(replica.ControlEndpoint)), uint64(len(replica.ControlAddress)),
			)
			if err != nil {
				return 0, 0, err
			}
		}
	}
	return planBytes, handleBytes, nil
}

func replicatedTransactionRecoveryHandleLogicalBytes(
	handle *ReplicatedTransactionRecoveryHandle,
) (uint64, error) {
	if handle == nil {
		return 0, ErrReplicatedTransaction
	}
	count := uint64(len(handle.Participants))
	witnessBytes, err := checkedReplicatedTransactionLogicalProduct(
		count, uint64(unsafe.Sizeof(ReplicatedTransactionRouteWitness{})),
	)
	if err != nil {
		return 0, err
	}
	total, err := checkedReplicatedTransactionLogicalSum(
		uint64(unsafe.Sizeof(ReplicatedTransactionRecoveryHandle{})), witnessBytes,
		replicatedTransactionReservationLogicalBytes,
		replicatedTransactionOwnershipLogicalBytes,
	)
	if err != nil {
		return 0, err
	}
	for index := range handle.Participants {
		route := &handle.Participants[index].Route
		replicaBytes, sizeErr := checkedReplicatedTransactionLogicalProduct(
			uint64(len(route.Replicas)), uint64(unsafe.Sizeof(ReplicatedEndpoint{})),
		)
		if sizeErr != nil {
			return 0, sizeErr
		}
		total, err = checkedReplicatedTransactionLogicalSum(
			total, uint64(len(route.Distribution)), uint64(len(route.Shard)), replicaBytes,
		)
		if err != nil {
			return 0, err
		}
		for replicaIndex := range route.Replicas {
			replica := &route.Replicas[replicaIndex]
			total, err = checkedReplicatedTransactionLogicalSum(
				total,
				uint64(len(replica.NativeEndpoint)), uint64(len(replica.Address)),
				uint64(len(replica.ControlEndpoint)), uint64(len(replica.ControlAddress)),
			)
			if err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

func checkedReplicatedTransactionLogicalProduct(left, right uint64) (uint64, error) {
	if left != 0 && right > math.MaxUint64/left {
		return 0, ErrReplicatedTransactionBound
	}
	return left * right, nil
}

func checkedReplicatedTransactionLogicalSum(values ...uint64) (uint64, error) {
	var total uint64
	for _, value := range values {
		if value > math.MaxUint64-total {
			return 0, ErrReplicatedTransactionBound
		}
		total += value
	}
	return total, nil
}

type replicatedTransactionPlanParticipant struct {
	route        *ReplicatedRoute
	batches      []replication.RelationMutationBatch
	bucketBits   uint8
	intentScopes []distributedtxn.IntentScope
	ref          distributedtxn.ParticipantRef
	digest       distributedtxn.Digest
}

type replicatedTransactionProposal struct {
	ordinal            uint32
	result             ReplicatedResult
	code               uint32
	value              replicatedstate.TransactionCompletionResult
	err                error
	stop               bool
	pending            []byte
	pendingReservation *replicatedTransactionPendingReservation
	// scratch is worker-owned reusable command capacity. On outcome unknown its
	// command backing and exact reservation transfer together into pending.
	scratch replicatedTransactionWorkerScratch
}

type replicatedTransactionWorkerScratch struct {
	command  []byte
	control  []byte
	reserved uint64
	reusable bool
}

// Execute commits one native transaction through one or more RF3 groups. A
// singleton group preserves atomic multi-relation/multi-statement semantics;
// larger plans add cross-group atomicity. Every proposal uses
// ReplicatedExecutor.Propose; the method has no legacy transport or journal
// fallback.
func (orchestrator *ReplicatedTransactionOrchestrator) Execute(
	ctx context.Context,
	catalogGeneration uint64,
	participants []ReplicatedTransactionParticipant,
) (result ReplicatedTransactionResult, resultErr error) {
	if orchestrator == nil || orchestrator.executor == nil || ctx == nil ||
		catalogGeneration == 0 || len(participants) == 0 ||
		uint64(len(participants)) > maxReplicatedTransactionOrdinal {
		return ReplicatedTransactionResult{}, ErrReplicatedTransaction
	}
	id, err := newReplicatedTransactionID(orchestrator.idSource)
	if err != nil {
		return ReplicatedTransactionResult{}, err
	}
	planBytes, handleBytes, err := replicatedTransactionOwnedLogicalBytes(participants)
	if err != nil {
		return ReplicatedTransactionResult{}, err
	}
	ownedBytes, err := checkedReplicatedTransactionLogicalSum(planBytes, handleBytes)
	if err != nil {
		return ReplicatedTransactionResult{}, err
	}
	if ownedBytes > orchestrator.byteBudget.limit {
		return ReplicatedTransactionResult{}, ErrReplicatedTransactionBound
	}
	if err = orchestrator.byteBudget.acquire(ctx, ownedBytes); err != nil {
		return ReplicatedTransactionResult{}, err
	}
	planReservation := newReplicatedTransactionPendingReservation(
		&orchestrator.byteBudget, planBytes,
	)
	handleReservation := newReplicatedTransactionPendingReservation(
		&orchestrator.byteBudget, handleBytes,
	)
	defer planReservation.release()
	var handle *ReplicatedTransactionRecoveryHandle
	defer func() {
		if handle == nil {
			handleReservation.release()
			return
		}
		if !replicatedTransactionHandleEscapes(result, resultErr, handle) {
			releaseReplicatedTransactionTerminalOwnership(handle)
		}
	}()
	plan, err := orchestrator.plan(participants)
	if err != nil {
		return ReplicatedTransactionResult{}, err
	}
	coordinatorOrdinal := replicatedTransactionCoordinatorOrdinal(id, len(plan))
	deadline := int64(replicatedTransactionRecoveryPulseLimit)
	handle = newReplicatedTransactionRecoveryHandle(
		id, catalogGeneration, uint32(coordinatorOrdinal), deadline, plan,
		handleReservation,
	)
	handle.RetryHome = orchestrator.retryHome
	decisionRevision, begin, err := orchestrator.begin(
		ctx, handle, plan, coordinatorOrdinal, catalogGeneration, deadline,
	)
	handle.DecisionRevision = decisionRevision
	if err != nil {
		if begin.pending == nil && handle.CoordinatorMinimumApplied == 0 {
			return ReplicatedTransactionResult{}, err
		}
		return ReplicatedTransactionResult{}, orchestrator.executionError(handle, false, err)
	}
	if begin.code == replicatedstate.ResultIndexConflict {
		handle.Phase = ReplicatedTransactionPhasePreparing
		return orchestrator.abortAfterConflict(ctx, handle, plan, ErrReplicatedTransactionConflict)
	}
	if begin.code != replicatedstate.ResultApplied {
		return ReplicatedTransactionResult{}, orchestrator.executionError(
			handle, false, ErrReplicatedTransaction,
		)
	}
	handle.Participants[coordinatorOrdinal].Prepared = true
	handle.Participants[coordinatorOrdinal].MinimumApplied = begin.result.Outcome.AppliedIndex
	handle.CoordinatorMinimumApplied = begin.result.Outcome.AppliedIndex
	handle.Phase = ReplicatedTransactionPhasePreparing

	var prepareErr error
	prepareCount := orchestrator.runWave(ctx, len(plan), coordinatorOrdinal, true, func(
		work context.Context, ordinal int, scratch replicatedTransactionWorkerScratch,
	) replicatedTransactionProposal {
		control := distributedtxn.ReplicatedCommand{
			Role:      distributedtxn.ReplicatedRoleParticipant,
			Operation: distributedtxn.ReplicatedStagePrepareParticipant,
			ID:        id, PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
			Participant: orchestrator.participantStage(
				plan[ordinal], *plan[coordinatorOrdinal].route, uint32(ordinal),
			),
		}
		proposal := orchestrator.propose(work, *plan[ordinal].route, control,
			plan[ordinal].batches, uint32(ordinal), scratch)
		// A deterministic prepare conflict or malformed/unexpected completion
		// cannot become a successful commit. Stop admitting untouched shards while
		// runWave still drains every proposal which was already in flight.
		proposal.stop = proposal.err != nil || proposal.code != replicatedstate.ResultApplied
		return proposal
	}, func(proposal replicatedTransactionProposal) {
		if proposal.result.Outcome.AppliedIndex != 0 {
			handle.Participants[proposal.ordinal].MinimumApplied = proposal.result.Outcome.AppliedIndex
		}
		orchestrator.capturePending(handle, proposal)
		switch {
		case proposal.err != nil:
			prepareErr = errors.Join(prepareErr, proposal.err)
		case proposal.code == replicatedstate.ResultApplied:
			handle.Participants[proposal.ordinal].Prepared = true
		case proposal.code == replicatedstate.ResultIndexConflict:
			prepareErr = errors.Join(prepareErr, ErrReplicatedTransactionConflict)
		default:
			prepareErr = errors.Join(prepareErr, ErrReplicatedTransaction)
		}
	})
	if prepareCount != len(plan)-1 && prepareErr == nil {
		// A definite completion conflict deliberately cancels untouched work;
		// abort fencing below makes those shards terminal without making the
		// outcome unknown. A short wave with no observed completion/error can only
		// come from cancellation outside that deliberate stop.
		prepareErr = errors.Join(context.Cause(ctx), ErrReplicatedTransactionUnknown)
	}
	if prepareErr != nil {
		return orchestrator.abortAfterConflict(ctx, handle, plan, prepareErr)
	}

	handle.Phase = ReplicatedTransactionPhaseDeciding
	decision := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedCommitCoordinator,
		ID:        id, ExpectedRevision: decisionRevision,
		PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}
	commit := orchestrator.propose(ctx, *plan[coordinatorOrdinal].route,
		decision, nil, uint32(coordinatorOrdinal), replicatedTransactionWorkerScratch{})
	if commit.result.Outcome.AppliedIndex != 0 {
		handle.CoordinatorMinimumApplied = commit.result.Outcome.AppliedIndex
	}
	orchestrator.capturePending(handle, commit)
	if commit.err != nil || commit.code != replicatedstate.ResultApplied {
		if commit.pending == nil {
			// A definite pre-admission decision failure leaves the durable
			// coordinator staging. Recovery may choose abort; only a retained exact
			// unknown decision forbids that inference.
			handle.Phase = ReplicatedTransactionPhasePreparing
		}
		return ReplicatedTransactionResult{}, orchestrator.executionError(
			handle, false, errors.Join(commit.err, ErrReplicatedTransactionUnknown),
		)
	}
	handle.Phase = ReplicatedTransactionPhaseCommitted
	handle.CoordinatorMinimumApplied = commit.result.Outcome.AppliedIndex

	affected, finishErr := orchestrator.finish(ctx, handle, true)
	if finishErr != nil {
		return ReplicatedTransactionResult{
			ID: id, Committed: true, Recovery: handle,
		}, orchestrator.executionError(handle, true, finishErr)
	}
	// Every participant returned a route-fenced, applied RF3 completion and
	// finish marked the corresponding witness terminal. Those P durable results
	// are already the terminal proof; an additional P-wide ReadIndex wave would
	// add a quorum round and network barrier without strengthening the cut.
	// Recovery and partial/ambiguous paths still re-prove terminal state by
	// ReadIndex before retirement.
	if err := orchestrator.retire(
		ctx, handle, *plan[coordinatorOrdinal].route,
		distributedtxn.ReplicatedRetirementSummary{
			AffectedRows: affected, AffectedRowsValid: true,
		},
	); err != nil {
		return ReplicatedTransactionResult{
			ID: id, Committed: true, AffectedRows: affected, Recovery: handle,
		}, orchestrator.executionError(handle, true, err)
	}
	return ReplicatedTransactionResult{ID: id, Committed: true, AffectedRows: affected}, nil
}

func (orchestrator *ReplicatedTransactionOrchestrator) plan(
	participants []ReplicatedTransactionParticipant,
) ([]replicatedTransactionPlanParticipant, error) {
	plan := make([]replicatedTransactionPlanParticipant, len(participants))
	var mutations, mutationBytes uint64
	for index := range participants {
		participant := &participants[index]
		if !validReplicatedRoute(participant.Route) {
			return nil, ErrReplicatedTransaction
		}
		digest, err := replication.TransactionMutationDigest(participant.Batches)
		if err != nil || !distributedtxn.ValidateIntentScopes(
			participant.IntentScopes, participant.BucketBits,
		) {
			return nil, errors.Join(ErrReplicatedTransaction, err)
		}
		for batchIndex := range participant.Batches {
			batch := &participant.Batches[batchIndex]
			for mutationIndex := range batch.Mutations {
				mutation := &batch.Mutations[mutationIndex]
				itemBytes := uint64(len(mutation.Key)) + uint64(len(mutation.Value))
				if mutations == math.MaxUint64 ||
					itemBytes > math.MaxUint64-mutationBytes {
					return nil, ErrReplicatedTransactionBound
				}
				mutations++
				mutationBytes += itemBytes
				if mutations > orchestrator.maxMutations || mutationBytes > orchestrator.maxMutationBytes {
					return nil, ErrReplicatedTransactionBound
				}
			}
		}
		plan[index] = replicatedTransactionPlanParticipant{
			route: &participant.Route, batches: participant.Batches,
			bucketBits: participant.BucketBits, intentScopes: participant.IntentScopes,
			digest: digest,
		}
	}
	slices.SortFunc(plan, func(left, right replicatedTransactionPlanParticipant) int {
		if order := strings.Compare(string(left.route.Distribution), string(right.route.Distribution)); order != 0 {
			return order
		}
		return strings.Compare(string(left.route.Shard), string(right.route.Shard))
	})
	cluster := plan[0].route.Group
	groups := make(map[raftmember.GroupKey]struct{}, len(plan))
	for index := range plan {
		route := *plan[index].route
		if route.Group.ClusterID != cluster.ClusterID ||
			route.Group.ClusterIncarnation != cluster.ClusterIncarnation ||
			route.Group.TopologyRecoveryEpoch != cluster.TopologyRecoveryEpoch {
			return nil, ErrReplicatedTransaction
		}
		if _, duplicate := groups[route.Group]; duplicate {
			return nil, ErrReplicatedTransaction
		}
		groups[route.Group] = struct{}{}
		if index > 0 {
			prior := *plan[index-1].route
			if route.Distribution == prior.Distribution && route.Shard == prior.Shard {
				return nil, ErrReplicatedTransaction
			}
		}
		plan[index].ref = distributedtxn.ParticipantRef{
			Distribution:         byteview.Bytes(string(route.Distribution)),
			Shard:                byteview.Bytes(string(route.Shard)),
			RoutingVersion:       route.Command.RoutingVersion,
			AllocationGeneration: route.AllocationGeneration,
			OwnershipEpoch:       route.Command.OwnershipEpoch,
			AuthorityWitness:     replicatedRouteAuthorityWitness(route),
			MutationDigest:       plan[index].digest,
			State:                distributedtxn.ParticipantStaged,
		}
	}
	return plan, nil
}

func replicatedRouteAuthorityDigest(route ReplicatedRoute) replication.Digest {
	return replication.RouteAuthorityDigest(replication.RouteAuthority{
		ClusterID:              route.Group.ClusterID,
		ClusterIncarnation:     route.Group.ClusterIncarnation,
		TopologyRecoveryEpoch:  route.Group.TopologyRecoveryEpoch,
		ShardIncarnation:       route.Group.ShardIncarnation,
		GroupID:                route.Group.GroupID,
		AllocationGeneration:   route.AllocationGeneration,
		ReplicaSetVersion:      route.Command.ReplicaSetVersion,
		ActivePolicyGeneration: route.Command.ActivePolicyGeneration,
		ProtectionEpoch:        route.Command.ProtectionEpoch,
		OwnershipEpoch:         route.Command.OwnershipEpoch,
		SchemaGeneration:       route.Command.SchemaGeneration,
		RelationManifestDigest: replication.Digest(route.Command.RelationManifestDigest),
		RoutingVersion:         route.Command.RoutingVersion,
		RouteGeneration:        route.Command.RouteGeneration,
	})
}

func replicatedRouteAuthorityWitness(route ReplicatedRoute) distributedtxn.AuthorityWitness {
	digest := replicatedRouteAuthorityDigest(route)
	var witness distributedtxn.AuthorityWitness
	copy(witness[:], digest[:])
	return witness
}

func newReplicatedTransactionID(reader io.Reader) (distributedtxn.ID, error) {
	for range maxReplicatedTransactionIDAttempts {
		var id distributedtxn.ID
		if _, err := io.ReadFull(reader, id[:]); err != nil {
			return distributedtxn.ID{}, err
		}
		if !id.IsZero() {
			return id, nil
		}
	}
	return distributedtxn.ID{}, ErrReplicatedTransaction
}

func replicatedTransactionCoordinatorOrdinal(id distributedtxn.ID, participants int) int {
	digest := sha256.Sum256(id[:])
	return int(binary.LittleEndian.Uint64(digest[:8]) % uint64(participants))
}

func (orchestrator *ReplicatedTransactionOrchestrator) participantStage(
	participant replicatedTransactionPlanParticipant,
	coordinator ReplicatedRoute,
	ordinal uint32,
) distributedtxn.ParticipantStage {
	return distributedtxn.ParticipantStage{
		CoordinatorGroup:            distributedtxn.ID(coordinator.Group.GroupID),
		CoordinatorShardIncarnation: distributedtxn.ID(coordinator.Group.ShardIncarnation),
		CoordinatorAllocation:       coordinator.AllocationGeneration,
		BucketBits:                  participant.bucketBits,
		IntentScopes:                participant.intentScopes,
		MutationDigest:              participant.digest,
		ParticipantOrdinal:          ordinal,
	}
}

func (orchestrator *ReplicatedTransactionOrchestrator) begin(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	plan []replicatedTransactionPlanParticipant,
	coordinatorOrdinal int,
	catalogGeneration uint64,
	recoveryDeadline int64,
) (uint64, replicatedTransactionProposal, error) {
	coordinator := plan[coordinatorOrdinal]
	var inlineErr error
	if len(plan) <= distributedtxn.MaxInlineParticipants {
		participants := make([]distributedtxn.ParticipantRef, len(plan))
		for index := range plan {
			participants[index] = plan[index].ref
		}
		inline, err := distributedtxn.AppendCoordinator(nil, distributedtxn.CoordinatorRecord{
			ID: handle.ID, State: distributedtxn.CoordinatorStaging, Revision: 1,
			CatalogGeneration: catalogGeneration, RecoveryDeadline: recoveryDeadline,
			Participants: participants,
		})
		inlineErr = err
		if err == nil {
			control := distributedtxn.ReplicatedCommand{
				Role:      distributedtxn.ReplicatedRoleCoordinator,
				Operation: distributedtxn.ReplicatedBeginPrepareCoordinator,
				ID:        handle.ID, PayloadKind: distributedtxn.ReplicatedPayloadCoordinator,
				Payload: inline,
				Participant: orchestrator.participantStage(
					coordinator, *coordinator.route, uint32(coordinatorOrdinal),
				),
			}
			handle.Phase = ReplicatedTransactionPhaseBeginning
			proposal := orchestrator.propose(ctx, *coordinator.route, control,
				coordinator.batches, uint32(coordinatorOrdinal), replicatedTransactionWorkerScratch{})
			orchestrator.capturePending(handle, proposal)
			return 1, proposal, proposal.err
		}
	}

	descriptor, initial, err := replicatedTransactionManifest(plan)
	if err != nil {
		return 0, replicatedTransactionProposal{}, errors.Join(inlineErr, err)
	}
	manifest, err := distributedtxn.AppendManifestCoordinator(nil,
		distributedtxn.ManifestCoordinatorRecord{
			ID: handle.ID, State: distributedtxn.CoordinatorStaging, Revision: 1,
			CatalogGeneration: catalogGeneration, RecoveryDeadline: recoveryDeadline,
			Manifest: descriptor,
		})
	if err != nil {
		return 0, replicatedTransactionProposal{}, err
	}
	payload := append(manifest, initial...)
	control := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedBeginPrepareManifestCoordinator,
		ID:        handle.ID, PayloadKind: distributedtxn.ReplicatedPayloadManifestCoordinator,
		Payload: payload,
		Participant: orchestrator.participantStage(
			coordinator, *coordinator.route, uint32(coordinatorOrdinal),
		),
	}
	handle.Phase = ReplicatedTransactionPhaseBeginning
	proposal := orchestrator.propose(ctx, *coordinator.route, control,
		coordinator.batches, uint32(coordinatorOrdinal), replicatedTransactionWorkerScratch{})
	orchestrator.capturePending(handle, proposal)
	if proposal.err != nil ||
		proposal.code != replicatedstate.ResultApplied &&
			proposal.code != replicatedstate.ResultIndexConflict {
		return uint64(descriptor.SegmentCount), proposal, proposal.err
	}
	handle.CoordinatorMinimumApplied = proposal.result.Outcome.AppliedIndex
	if descriptor.SegmentCount > uint32(distributedtxn.MaxManifestSegmentsPerCommand) {
		err = orchestrator.appendManifestRemainder(
			ctx, handle, *coordinator.route, plan, descriptor,
		)
	}
	return uint64(descriptor.SegmentCount), proposal, err
}

func replicatedTransactionManifest(
	plan []replicatedTransactionPlanParticipant,
) (distributedtxn.ManifestDescriptor, []byte, error) {
	scratch := make([]byte, distributedtxn.ManifestSegmentBytes)
	// Grow with the pages actually emitted. Reserving the fifteen-page wire
	// maximum here made a 65-participant, one-page transaction retain almost a
	// megabyte before its first proposal.
	var initial []byte
	builder, err := distributedtxn.NewManifestBuilder(scratch,
		func(segment distributedtxn.ManifestSegment) error {
			if segment.Index < distributedtxn.MaxManifestSegmentsPerCommand {
				initial = append(initial, segment.Raw...)
			}
			return nil
		})
	if err != nil {
		return distributedtxn.ManifestDescriptor{}, nil, err
	}
	for index := range plan {
		if err = builder.Append(plan[index].ref); err != nil {
			return distributedtxn.ManifestDescriptor{}, nil, err
		}
	}
	descriptor, err := builder.Seal()
	return descriptor, initial, err
}

func (orchestrator *ReplicatedTransactionOrchestrator) appendManifestRemainder(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	route ReplicatedRoute,
	plan []replicatedTransactionPlanParticipant,
	want distributedtxn.ManifestDescriptor,
) error {
	scratch := make([]byte, distributedtxn.ManifestSegmentBytes)
	// The final pack is commonly one short page. Let append track actual bytes
	// instead of reserving the fifteen-page command ceiling on every pass.
	var pack []byte
	packCount := 0
	flush := func() error {
		if packCount == 0 {
			return nil
		}
		sequence, err := distributedtxn.OpenManifestSegmentSequence(pack)
		if err != nil {
			return err
		}
		control := distributedtxn.ReplicatedCommand{
			Role:      distributedtxn.ReplicatedRoleCoordinator,
			Operation: distributedtxn.ReplicatedAppendManifestSegments,
			ID:        handle.ID, ExpectedRevision: uint64(sequence.FirstIndex()),
			PayloadKind: distributedtxn.ReplicatedPayloadManifestSegments,
			Payload:     pack,
		}
		proposal := orchestrator.propose(ctx, route, control, nil,
			handle.CoordinatorOrdinal, replicatedTransactionWorkerScratch{})
		orchestrator.capturePending(handle, proposal)
		if proposal.err != nil || proposal.code != replicatedstate.ResultApplied {
			return errors.Join(proposal.err, ErrReplicatedTransaction)
		}
		handle.CoordinatorMinimumApplied = proposal.result.Outcome.AppliedIndex
		pack = pack[:0]
		packCount = 0
		return nil
	}
	builder, err := distributedtxn.NewManifestBuilder(scratch,
		func(segment distributedtxn.ManifestSegment) error {
			if segment.Index < distributedtxn.MaxManifestSegmentsPerCommand {
				return nil
			}
			pack = append(pack, segment.Raw...)
			packCount++
			if packCount == distributedtxn.MaxManifestSegmentsPerCommand {
				return flush()
			}
			return nil
		})
	if err != nil {
		return err
	}
	for index := range plan {
		if err = builder.Append(plan[index].ref); err != nil {
			return err
		}
	}
	got, err := builder.Seal()
	if err != nil {
		return err
	}
	if err = flush(); err != nil {
		return err
	}
	if got != want {
		return ErrReplicatedTransaction
	}
	return nil
}

func (orchestrator *ReplicatedTransactionOrchestrator) propose(
	ctx context.Context,
	route ReplicatedRoute,
	control distributedtxn.ReplicatedCommand,
	batches []replication.RelationMutationBatch,
	ordinal uint32,
	scratch replicatedTransactionWorkerScratch,
) (proposal replicatedTransactionProposal) {
	return orchestrator.proposeWithCapability(
		ctx, orchestrator.retryHome, route, control, batches, ordinal, scratch,
		serviceauthz.CapabilityDataWrite, nil,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) proposeRecovery(
	ctx context.Context,
	route ReplicatedRoute,
	control distributedtxn.ReplicatedCommand,
	batches []replication.RelationMutationBatch,
	ordinal uint32,
	scratch replicatedTransactionWorkerScratch,
) replicatedTransactionProposal {
	return orchestrator.proposeWithCapability(
		ctx, orchestrator.retryHome, route, control, batches, ordinal, scratch,
		serviceauthz.CapabilityTransactionRecovery, nil,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) proposeExactForHandle(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	route ReplicatedRoute,
	control distributedtxn.ReplicatedCommand,
	batches []replication.RelationMutationBatch,
	ordinal uint32,
	scratch replicatedTransactionWorkerScratch,
	exact []byte,
	capability serviceauthz.Capability,
) replicatedTransactionProposal {
	if handle == nil || handle.RetryHome == (replication.RetryHome{}) || len(exact) == 0 {
		return replicatedTransactionProposal{ordinal: ordinal, err: ErrReplicatedTransaction}
	}
	return orchestrator.proposeWithCapability(
		ctx, handle.RetryHome, route, control, batches, ordinal, scratch, capability, exact,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) proposeWithCapability(
	ctx context.Context,
	retryHome replication.RetryHome,
	route ReplicatedRoute,
	control distributedtxn.ReplicatedCommand,
	batches []replication.RelationMutationBatch,
	ordinal uint32,
	scratch replicatedTransactionWorkerScratch,
	capability serviceauthz.Capability,
	expectedExact []byte,
) (proposal replicatedTransactionProposal) {
	proposal = replicatedTransactionProposal{ordinal: ordinal}
	proposal.scratch = scratch
	if capability != serviceauthz.CapabilityDataWrite &&
		capability != serviceauthz.CapabilityTransactionRecovery {
		proposal.err = ErrReplicatedTransaction
		return proposal
	}
	controlSize, err := distributedtxn.ReplicatedCommandSize(control)
	if err != nil {
		proposal.err = err
		return proposal
	}
	command := replicatedTransactionCommandHeader(
		route, orchestrator.tenant, retryHome,
		replication.ID128(control.ID), uint64(control.Role), 1,
	)
	command.Kind = replication.CommandTransaction
	command.Batches = batches
	commandSize, err := replication.TransactionCommandSize(command, controlSize)
	if err != nil {
		proposal.err = err
		return proposal
	}
	totalBytes := uint64(controlSize) + uint64(commandSize)
	proposalBytes, err := checkedReplicatedTransactionLogicalSum(
		totalBytes, replicatedTransactionPendingLogicalBytes,
	)
	if err != nil {
		proposal.err = err
		return proposal
	}
	useRetained := false
	if proposal.scratch.reusable {
		if proposalBytes <= proposal.scratch.reserved {
			useRetained = true
		} else {
			delta := proposalBytes - proposal.scratch.reserved
			if orchestrator.byteBudget.tryAcquire(delta) {
				proposal.scratch.reserved += delta
				useRetained = true
			}
		}
	}
	var activeRelease uint64
	var controlDst, commandDst []byte
	if useRetained {
		controlCap := max(cap(proposal.scratch.control), controlSize)
		commandCap := max(cap(proposal.scratch.command), commandSize)
		if uint64(controlCap)+uint64(commandCap)+
			replicatedTransactionPendingLogicalBytes > proposal.scratch.reserved {
			proposal.scratch.control = nil
			proposal.scratch.command = nil
		}
		if cap(proposal.scratch.control) < controlSize {
			proposal.scratch.control = make([]byte, 0, controlSize)
		}
		if cap(proposal.scratch.command) < commandSize {
			proposal.scratch.command = make([]byte, 0, commandSize)
		}
		controlDst, commandDst = proposal.scratch.control, proposal.scratch.command
	} else {
		if err = orchestrator.activeByteBudget.acquire(ctx, proposalBytes); err != nil {
			proposal.err = err
			return proposal
		}
		activeRelease = proposalBytes
		defer func() { orchestrator.activeByteBudget.release(activeRelease) }()
		controlDst = make([]byte, 0, controlSize)
		commandDst = make([]byte, 0, commandSize)
	}
	controlBytes, err := distributedtxn.AppendReplicatedCommand(controlDst[:0], control)
	if err != nil {
		proposal.err = err
		return proposal
	}
	sequence, err := replication.TransactionClientSequence(controlBytes)
	if err != nil {
		proposal.err = err
		return proposal
	}
	command = replicatedTransactionCommandHeader(
		route, orchestrator.tenant, retryHome,
		replication.ID128(control.ID), uint64(control.Role), sequence,
	)
	command.Kind = replication.CommandTransaction
	command.Transaction = controlBytes
	command.Batches = batches
	command.Fingerprint = nativeCommandFingerprint(command)
	size, err := replication.CommandSize(command)
	if err != nil || size != commandSize {
		proposal.err = errors.Join(err, ErrReplicatedTransaction)
		return proposal
	}
	dst := commandDst
	dst, err = replication.AppendCommand(dst[:0], command)
	if err != nil {
		proposal.err = err
		return proposal
	}
	if len(expectedExact) != 0 && !bytes.Equal(dst, expectedExact) {
		proposal.err = ErrReplicatedTransaction
		return proposal
	}
	if useRetained {
		proposal.scratch.command = dst[:0]
		proposal.scratch.control = controlBytes[:0]
	}
	proposal.result, proposal.err = orchestrator.executor.proposeOwnedWithCapability(
		ctx, route, dst, capability,
	)
	if proposal.err != nil {
		var unknown *raftservice.UnknownOutcomeError
		if errors.As(proposal.err, &unknown) {
			if len(unknown.Command) != len(dst) || len(dst) == 0 ||
				&unknown.Command[0] != &dst[0] {
				proposal.err = errors.Join(proposal.err, ErrReplicatedTransaction)
				return proposal
			}
			// The executor transfers the original backing allocation. Move the
			// matching reservation into pending before the worker/active remainder
			// is released; no command clone exists at this boundary.
			retainedBytes := uint64(cap(commandDst)) +
				replicatedTransactionPendingLogicalBytes
			if useRetained {
				if retainedBytes > proposal.scratch.reserved {
					panic("gateway: retained command exceeds worker reservation")
				}
				proposal.scratch.command = nil
				proposal.scratch.reserved -= retainedBytes
				proposal.pendingReservation = newReplicatedTransactionPendingReservation(
					&orchestrator.byteBudget, retainedBytes,
				)
			} else {
				if retainedBytes > activeRelease {
					panic("gateway: retained command exceeds active reservation")
				}
				activeRelease -= retainedBytes
				proposal.pendingReservation = newReplicatedTransactionPendingReservation(
					&orchestrator.activeByteBudget, retainedBytes,
				)
			}
			proposal.pending = dst[:len(dst):cap(commandDst)]
			unknown.Command = nil
		}
		return proposal
	}
	commandView, commandErr := replication.OpenCommand(dst)
	completion, err := replication.OpenCompletion(proposal.result.Completion)
	if err != nil {
		proposal.err = err
		return proposal
	}
	if commandErr != nil || !nativeCompletionMatches(commandView, completion) {
		proposal.err = errors.Join(commandErr, ErrReplicatedTransaction)
		return proposal
	}
	proposal.code = completion.ResultCode
	proposal.value, err = replicatedstate.OpenTransactionCompletionResult(
		completion.ResultCode, completion.InlineResult,
	)
	if err != nil || proposal.value.Role != control.Role ||
		proposal.value.Operation != control.Operation {
		proposal.err = errors.Join(err, ErrReplicatedTransaction)
	}
	return proposal
}

func replicatedTransactionCommandHeader(
	route ReplicatedRoute,
	tenant []byte,
	retryHome replication.RetryHome,
	clientID replication.ID128,
	epoch, sequence uint64,
) replication.Command {
	fence := route.Command
	return replication.Command{
		AuthorityClass:        replication.CommandAuthorityData,
		ClusterID:             route.Group.ClusterID,
		ClusterIncarnation:    route.Group.ClusterIncarnation,
		TopologyRecoveryEpoch: route.Group.TopologyRecoveryEpoch,
		Distribution:          string(route.Distribution), Shard: string(route.Shard),
		AllocationGeneration: route.AllocationGeneration,
		ShardIncarnation:     route.Group.ShardIncarnation, GroupID: route.Group.GroupID,
		ReplicaSetVersion:      fence.ReplicaSetVersion,
		ActivePolicyGeneration: fence.ActivePolicyGeneration,
		ProtectionEpoch:        fence.ProtectionEpoch,
		OwnershipEpoch:         fence.OwnershipEpoch,
		SchemaGeneration:       fence.SchemaGeneration,
		RoutingVersion:         fence.RoutingVersion,
		RouteGeneration:        fence.RouteGeneration,
		Tenant:                 tenant, ClientID: clientID, ClientEpoch: epoch,
		ClientSequence: sequence, RetryHome: retryHome,
	}
}

func (orchestrator *ReplicatedTransactionOrchestrator) runWave(
	ctx context.Context,
	count int,
	skip int,
	stopOnError bool,
	visit func(context.Context, int, replicatedTransactionWorkerScratch) replicatedTransactionProposal,
	consume func(replicatedTransactionProposal),
) int {
	want := count
	if skip >= 0 && skip < count {
		want--
	}
	if want == 0 {
		return 0
	}
	workers := min(orchestrator.maxConcurrency, want)
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan replicatedTransactionProposal, workers)
	var next atomic.Uint64
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			reserved := uint64(orchestrator.maxWorkerRetainedBytes)
			if reserved != 0 && !orchestrator.byteBudget.tryAcquire(reserved) {
				reserved = 0
			}
			scratch := replicatedTransactionWorkerScratch{
				reserved: reserved, reusable: true,
			}
			defer func() { orchestrator.byteBudget.release(scratch.reserved) }()
			for workCtx.Err() == nil {
				index := int(next.Add(1) - 1)
				if index >= count {
					return
				}
				if index == skip {
					continue
				}
				proposal := visit(workCtx, index, scratch)
				scratch = proposal.scratch
				if stopOnError && (proposal.err != nil || proposal.stop) {
					// Preserve every already-started outcome-unknown command. The
					// worker-sized channel bounds these exact owned buffers.
					results <- proposal
					cancel()
					return
				}
				select {
				case results <- proposal:
				case <-workCtx.Done():
					return
				}
			}
		}()
	}
	go func() {
		wait.Wait()
		close(results)
	}()
	collected := 0
	for result := range results {
		consume(result)
		collected++
	}
	return collected
}

func (orchestrator *ReplicatedTransactionOrchestrator) finish(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	commit bool,
) (int64, error) {
	return orchestrator.finishWithCapability(
		ctx, handle, commit, serviceauthz.CapabilityDataWrite,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) finishRecovery(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	commit bool,
) (int64, error) {
	return orchestrator.finishWithCapability(
		ctx, handle, commit, serviceauthz.CapabilityTransactionRecovery,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) finishWithCapability(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	commit bool,
	capability serviceauthz.Capability,
) (int64, error) {
	handle.Phase = ReplicatedTransactionPhaseFinishing
	operation := distributedtxn.ReplicatedAbortReleaseParticipant
	if commit {
		operation = distributedtxn.ReplicatedApplyReleaseParticipant
	}
	var affected int64
	var joined error
	resultCount := orchestrator.runWave(ctx, len(handle.Participants), -1, true, func(
		work context.Context, ordinal int, scratch replicatedTransactionWorkerScratch,
	) replicatedTransactionProposal {
		control := distributedtxn.ReplicatedCommand{
			Role:      distributedtxn.ReplicatedRoleParticipant,
			Operation: operation, ID: handle.ID, ExpectedRevision: 2,
			PayloadKind: distributedtxn.ReplicatedPayloadNone,
		}
		return orchestrator.proposeWithCapability(
			work, handle.RetryHome, handle.Participants[ordinal].Route, control,
			nil, uint32(ordinal), scratch, capability, nil,
		)
	}, func(proposal replicatedTransactionProposal) {
		if proposal.result.Outcome.AppliedIndex != 0 {
			handle.Participants[proposal.ordinal].MinimumApplied = proposal.result.Outcome.AppliedIndex
		}
		orchestrator.capturePending(handle, proposal)
		accepted := proposal.code == replicatedstate.ResultApplied ||
			!commit && proposal.code == replicatedstate.ResultTransactionConflict
		if proposal.err != nil || !accepted {
			joined = errors.Join(joined, proposal.err, ErrReplicatedTransactionUnknown)
			return
		}
		// ResultTransactionConflict on abort-release is only a candidate absence;
		// a leader ReadIndex lookup below must prove it before retirement.
		handle.Participants[proposal.ordinal].Terminal =
			proposal.code == replicatedstate.ResultApplied
		if commit {
			if !proposal.value.AffectedRowsValid || proposal.value.AffectedRows > math.MaxInt64-affected {
				joined = errors.Join(joined, ErrReplicatedTransaction)
				return
			}
			affected += proposal.value.AffectedRows
		}
	})
	if resultCount != len(handle.Participants) {
		joined = errors.Join(joined, ErrReplicatedTransactionUnknown)
	}
	for ordinal := range handle.Participants {
		if commit && !handle.Participants[ordinal].Terminal {
			joined = errors.Join(joined, ErrReplicatedTransactionUnknown)
		}
	}
	return affected, joined
}

// abortFenceAndRelease installs a durable cancellation witness on a missing
// participant before attempting the prepared/rev2 release transition. The two
// commands race safely: a StagePrepare that wins first makes the rev0 fence
// conflict and is then released at rev2; a rev0 fence that wins makes every
// delayed StagePrepare conflict forever.
func (orchestrator *ReplicatedTransactionOrchestrator) abortFenceAndRelease(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
) error {
	return orchestrator.abortFenceAndReleaseWithCapability(
		ctx, handle, serviceauthz.CapabilityDataWrite,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) abortFenceAndReleaseRecovery(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
) error {
	return orchestrator.abortFenceAndReleaseWithCapability(
		ctx, handle, serviceauthz.CapabilityTransactionRecovery,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) abortFenceAndReleaseWithCapability(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	capability serviceauthz.Capability,
) error {
	handle.Phase = ReplicatedTransactionPhaseFinishing
	coordinator := handle.Participants[handle.CoordinatorOrdinal].Route
	var joined error
	resultCount := orchestrator.runWave(ctx, len(handle.Participants), -1, true, func(
		work context.Context, ordinal int, scratch replicatedTransactionWorkerScratch,
	) replicatedTransactionProposal {
		fence := distributedtxn.ReplicatedCommand{
			Role:      distributedtxn.ReplicatedRoleParticipant,
			Operation: distributedtxn.ReplicatedAbortReleaseParticipant,
			ID:        handle.ID, ExpectedRevision: 0,
			PayloadKind: distributedtxn.ReplicatedPayloadParticipantStage,
			Participant: distributedtxn.ParticipantStage{
				CoordinatorGroup:            distributedtxn.ID(coordinator.Group.GroupID),
				CoordinatorShardIncarnation: distributedtxn.ID(coordinator.Group.ShardIncarnation),
				CoordinatorAllocation:       coordinator.AllocationGeneration,
				MutationDigest:              handle.Participants[ordinal].MutationDigest,
				ParticipantOrdinal:          uint32(ordinal),
			},
		}
		proposal := orchestrator.proposeWithCapability(
			work, handle.RetryHome, handle.Participants[ordinal].Route,
			fence, nil, uint32(ordinal), scratch, capability, nil,
		)
		if proposal.err != nil || proposal.code == replicatedstate.ResultApplied {
			return proposal
		}
		if proposal.code != replicatedstate.ResultTransactionConflict {
			return proposal
		}
		release := distributedtxn.ReplicatedCommand{
			Role:      distributedtxn.ReplicatedRoleParticipant,
			Operation: distributedtxn.ReplicatedAbortReleaseParticipant,
			ID:        handle.ID, ExpectedRevision: 2,
			PayloadKind: distributedtxn.ReplicatedPayloadNone,
		}
		return orchestrator.proposeWithCapability(
			work, handle.RetryHome, handle.Participants[ordinal].Route,
			release, nil, uint32(ordinal), proposal.scratch, capability, nil,
		)
	}, func(proposal replicatedTransactionProposal) {
		if proposal.result.Outcome.AppliedIndex != 0 {
			handle.Participants[proposal.ordinal].MinimumApplied = proposal.result.Outcome.AppliedIndex
		}
		orchestrator.capturePending(handle, proposal)
		if proposal.err != nil {
			joined = errors.Join(joined, proposal.err)
			return
		}
		if proposal.code == replicatedstate.ResultApplied {
			handle.Participants[proposal.ordinal].Terminal = true
			return
		}
		// A double conflict can mean an already terminal exact participant. It
		// remains unproved until the subsequent leader ReadIndex lookup.
		if proposal.code != replicatedstate.ResultTransactionConflict {
			joined = errors.Join(joined, ErrReplicatedTransactionUnknown)
		}
	})
	if resultCount != len(handle.Participants) {
		joined = errors.Join(joined, ErrReplicatedTransactionUnknown)
	}
	return joined
}

func (orchestrator *ReplicatedTransactionOrchestrator) abortAfterConflict(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	plan []replicatedTransactionPlanParticipant,
	cause error,
) (ReplicatedTransactionResult, error) {
	handle.Phase = ReplicatedTransactionPhaseDeciding
	coordinator := int(handle.CoordinatorOrdinal)
	abort := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedAbortCoordinator,
		ID:        handle.ID, ExpectedRevision: handle.DecisionRevision,
		PayloadKind: distributedtxn.ReplicatedPayloadNone,
	}
	proposal := orchestrator.propose(ctx, *plan[coordinator].route,
		abort, nil, handle.CoordinatorOrdinal, replicatedTransactionWorkerScratch{})
	orchestrator.capturePending(handle, proposal)
	if proposal.err != nil || proposal.code != replicatedstate.ResultApplied {
		return ReplicatedTransactionResult{}, orchestrator.executionError(
			handle, false, errors.Join(cause, proposal.err),
		)
	}
	handle.Phase = ReplicatedTransactionPhaseAborted
	handle.CoordinatorMinimumApplied = proposal.result.Outcome.AppliedIndex
	// Fence first. StagePrepare and the rev0 cancellation witness intentionally
	// share retry sequence one, so whichever command crossed admission prevents
	// the other from later acquiring a different meaning.
	finishErr := orchestrator.abortFenceAndRelease(ctx, handle)
	if finishErr != nil {
		return ReplicatedTransactionResult{}, orchestrator.executionError(
			handle, false, errors.Join(cause, finishErr),
		)
	}
	if len(handle.Pending) != 0 {
		if err := orchestrator.retryPending(ctx, handle); err != nil {
			return ReplicatedTransactionResult{}, orchestrator.executionError(
				handle, false, errors.Join(cause, err),
			)
		}
		// A prepare that won the first race is now known and must be released;
		// a cancellation witness that won is retried byte-identically here.
		if err := orchestrator.abortFenceAndRelease(ctx, handle); err != nil {
			return ReplicatedTransactionResult{}, orchestrator.executionError(
				handle, false, errors.Join(cause, err),
			)
		}
	}
	if _, proofErr := orchestrator.proveTerminalParticipants(ctx, handle, false); proofErr != nil {
		return ReplicatedTransactionResult{}, orchestrator.executionError(
			handle, false, errors.Join(cause, proofErr),
		)
	}
	if err := orchestrator.retire(
		ctx, handle, *plan[coordinator].route,
		distributedtxn.ReplicatedRetirementSummary{},
	); err != nil {
		return ReplicatedTransactionResult{}, orchestrator.executionError(
			handle, false, errors.Join(cause, err),
		)
	}
	return ReplicatedTransactionResult{ID: handle.ID}, cause
}

func (orchestrator *ReplicatedTransactionOrchestrator) retire(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	route ReplicatedRoute,
	summary distributedtxn.ReplicatedRetirementSummary,
) error {
	return orchestrator.retireWithCapability(
		ctx, handle, route, summary, serviceauthz.CapabilityDataWrite,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) retireRecovery(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	route ReplicatedRoute,
	summary distributedtxn.ReplicatedRetirementSummary,
) error {
	return orchestrator.retireWithCapability(
		ctx, handle, route, summary, serviceauthz.CapabilityTransactionRecovery,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) retireWithCapability(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	route ReplicatedRoute,
	summary distributedtxn.ReplicatedRetirementSummary,
	capability serviceauthz.Capability,
) error {
	for index := range handle.Participants {
		witness := handle.Participants[index]
		if !witness.Terminal {
			return ErrReplicatedTransactionUnknown
		}
	}
	var retirementBytes [distributedtxn.ReplicatedRetirementSummaryBytes]byte
	payload, err := distributedtxn.AppendReplicatedRetirementSummary(
		retirementBytes[:0], summary,
	)
	if err != nil {
		return errors.Join(err, ErrReplicatedTransaction)
	}
	control := distributedtxn.ReplicatedCommand{
		Role:      distributedtxn.ReplicatedRoleCoordinator,
		Operation: distributedtxn.ReplicatedRetireCoordinator,
		ID:        handle.ID, ExpectedRevision: handle.DecisionRevision + 1,
		PayloadKind: distributedtxn.ReplicatedPayloadRetirement, Payload: payload,
	}
	proposal := orchestrator.proposeWithCapability(
		ctx, handle.RetryHome, route, control, nil, handle.CoordinatorOrdinal,
		replicatedTransactionWorkerScratch{}, capability, nil,
	)
	orchestrator.capturePending(handle, proposal)
	if proposal.err != nil || proposal.code != replicatedstate.ResultApplied ||
		proposal.value.AffectedRowsValid != summary.AffectedRowsValid ||
		proposal.value.AffectedRows != summary.AffectedRows {
		return errors.Join(proposal.err, ErrReplicatedTransactionUnknown)
	}
	handle.CoordinatorMinimumApplied = proposal.result.Outcome.AppliedIndex
	handle.Phase = ReplicatedTransactionPhaseTerminal
	return nil
}

func (orchestrator *ReplicatedTransactionOrchestrator) retryPending(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
) error {
	return orchestrator.retryPendingMatchingWithCapability(
		ctx, handle, func(distributedtxn.ReplicatedCommand) bool { return true },
		serviceauthz.CapabilityDataWrite,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) retryPendingMatching(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	allow func(distributedtxn.ReplicatedCommand) bool,
) error {
	return orchestrator.retryPendingMatchingWithCapability(
		ctx, handle, allow, serviceauthz.CapabilityDataWrite,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) retryRecoveryPendingMatching(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	allow func(distributedtxn.ReplicatedCommand) bool,
) error {
	return orchestrator.retryPendingMatchingWithCapability(
		ctx, handle, allow, serviceauthz.CapabilityTransactionRecovery,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) retryPendingMatchingWithCapability(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	allow func(distributedtxn.ReplicatedCommand) bool,
	capability serviceauthz.Capability,
) error {
	return orchestrator.retryPendingMatchingObservedWithCapability(
		ctx, handle, allow, nil, capability,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) retryPendingMatchingObserved(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	allow func(distributedtxn.ReplicatedCommand) bool,
	applied func(distributedtxn.ReplicatedCommand),
) error {
	return orchestrator.retryPendingMatchingObservedWithCapability(
		ctx, handle, allow, applied, serviceauthz.CapabilityDataWrite,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) retryRecoveryPendingMatchingObserved(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	allow func(distributedtxn.ReplicatedCommand) bool,
	applied func(distributedtxn.ReplicatedCommand),
) error {
	return orchestrator.retryPendingMatchingObservedWithCapability(
		ctx, handle, allow, applied, serviceauthz.CapabilityTransactionRecovery,
	)
}

func (orchestrator *ReplicatedTransactionOrchestrator) retryPendingMatchingObservedWithCapability(
	ctx context.Context,
	handle *ReplicatedTransactionRecoveryHandle,
	allow func(distributedtxn.ReplicatedCommand) bool,
	applied func(distributedtxn.ReplicatedCommand),
	capability serviceauthz.Capability,
) error {
	pendingCommands := handle.Pending
	remaining := pendingCommands[:0]
	var joined error
	for index := range handle.Pending {
		pending := handle.Pending[index]
		commandView, commandErr := replication.OpenCommand(pending.Command)
		if commandErr != nil {
			remaining = append(remaining, pending)
			joined = errors.Join(joined, commandErr, ErrReplicatedTransaction)
			continue
		}
		controlView, controlErr := distributedtxn.OpenReplicatedCommand(commandView.TransactionBytes())
		if controlErr != nil {
			remaining = append(remaining, pending)
			joined = errors.Join(joined, controlErr, ErrReplicatedTransaction)
			continue
		}
		if !allow(controlView.Command()) {
			remaining = append(remaining, pending)
			continue
		}
		result, err := orchestrator.executor.retryUnknownBorrowedWithCapability(
			ctx, pending.Route, pending.Command, capability,
		)
		if err != nil {
			remaining = append(remaining, pending)
			joined = errors.Join(joined, err)
			continue
		}
		command, commandErr := replication.OpenCommand(pending.Command)
		completion, openErr := replication.OpenCompletion(result.Completion)
		if commandErr != nil || openErr != nil || !nativeCompletionMatches(command, completion) {
			remaining = append(remaining, pending)
			joined = errors.Join(joined, commandErr, openErr, ErrReplicatedTransaction)
			continue
		}
		value, openErr := replicatedstate.OpenTransactionCompletionResult(
			completion.ResultCode, completion.InlineResult,
		)
		if openErr != nil || value.Role != controlView.Role ||
			value.Operation != controlView.Operation {
			remaining = append(remaining, pending)
			joined = errors.Join(joined, openErr, ErrReplicatedTransaction)
			continue
		}
		if value.Operation == distributedtxn.ReplicatedRetireCoordinator &&
			completion.ResultCode == replicatedstate.ResultApplied {
			summary, summaryErr := distributedtxn.OpenReplicatedRetirementSummary(
				controlView.Payload,
			)
			if summaryErr != nil || value.AffectedRowsValid != summary.AffectedRowsValid ||
				value.AffectedRows != summary.AffectedRows {
				remaining = append(remaining, pending)
				joined = errors.Join(joined, summaryErr, ErrReplicatedTransaction)
				continue
			}
		}
		if completion.ResultCode != replicatedstate.ResultApplied &&
			value.AffectedRowsValid {
			remaining = append(remaining, pending)
			joined = errors.Join(joined, ErrReplicatedTransaction)
			continue
		}
		if completion.ResultCode == replicatedstate.ResultApplied && applied != nil {
			applied(controlView.Command())
		}
		ordinal := int(pending.Ordinal)
		if ordinal >= len(handle.Participants) {
			return ErrReplicatedTransaction
		}
		handle.Participants[ordinal].MinimumApplied = result.Outcome.AppliedIndex
		switch value.Operation {
		case distributedtxn.ReplicatedStagePrepareParticipant,
			distributedtxn.ReplicatedBeginPrepareCoordinator,
			distributedtxn.ReplicatedBeginPrepareManifestCoordinator:
			handle.Participants[ordinal].Prepared = completion.ResultCode == replicatedstate.ResultApplied
			if value.Role == distributedtxn.ReplicatedRoleCoordinator {
				handle.Phase = ReplicatedTransactionPhasePreparing
			}
		case distributedtxn.ReplicatedApplyReleaseParticipant,
			distributedtxn.ReplicatedAbortReleaseParticipant:
			handle.Participants[ordinal].Terminal = completion.ResultCode == replicatedstate.ResultApplied
		case distributedtxn.ReplicatedCommitCoordinator:
			if completion.ResultCode == replicatedstate.ResultApplied {
				handle.Phase = ReplicatedTransactionPhaseCommitted
			}
		case distributedtxn.ReplicatedAbortCoordinator:
			if completion.ResultCode == replicatedstate.ResultApplied {
				handle.Phase = ReplicatedTransactionPhaseAborted
			}
		case distributedtxn.ReplicatedRetireCoordinator:
			if completion.ResultCode == replicatedstate.ResultApplied {
				handle.Phase = ReplicatedTransactionPhaseTerminal
			}
		}
		if value.Role == distributedtxn.ReplicatedRoleCoordinator {
			handle.CoordinatorMinimumApplied = result.Outcome.AppliedIndex
		}
		if !handle.ownership.releasePending(pending.reservation) {
			panic("gateway: missing replicated transaction pending ownership")
		}
	}
	clear(pendingCommands[len(remaining):])
	handle.Pending = remaining
	return joined
}

func (orchestrator *ReplicatedTransactionOrchestrator) capturePending(
	handle *ReplicatedTransactionRecoveryHandle,
	proposal replicatedTransactionProposal,
) {
	if proposal.pending == nil {
		return
	}
	if proposal.pendingReservation == nil ||
		int(proposal.ordinal) >= len(handle.Participants) || handle.ownership == nil {
		panic("gateway: unreserved replicated transaction pending command")
	}
	handle.ownership.addPending(proposal.pendingReservation)
	handle.Pending = append(handle.Pending, ReplicatedTransactionPendingCommand{
		Route: handle.Participants[proposal.ordinal].Route, Ordinal: proposal.ordinal,
		Command: proposal.pending, reservation: proposal.pendingReservation,
	})
}

func (orchestrator *ReplicatedTransactionOrchestrator) executionError(
	handle *ReplicatedTransactionRecoveryHandle,
	committed bool,
	cause error,
) error {
	return &ReplicatedTransactionError{
		ID: handle.ID, Committed: committed, Recovery: handle, Cause: cause,
	}
}

// DiscardRecovery deliberately abandons this process's ability to settle an
// outcome-unknown transaction and releases every charged byte it owns. The
// transaction may still commit or require cleanup in Raft; callers should use
// Recover unless an operator has accepted that consequence. Calls must be
// serialized with Recover, but repeated calls and shallow handle copies are
// byte-budget safe.
func (orchestrator *ReplicatedTransactionOrchestrator) DiscardRecovery(
	handle *ReplicatedTransactionRecoveryHandle,
) error {
	if orchestrator == nil || handle == nil {
		return ErrReplicatedTransaction
	}
	ownership := handle.ownership
	if ownership == nil || ownership.handle == nil {
		if handle.Phase == ReplicatedTransactionPhaseInvalid ||
			handle.Phase == ReplicatedTransactionPhaseTerminal {
			return nil
		}
		// An external, never-admitted handle owns no orchestrator quota.
		clear(handle.Pending)
		handle.Pending = nil
		clear(handle.Participants)
		handle.Participants = nil
		handle.Phase = ReplicatedTransactionPhaseInvalid
		return nil
	}
	if ownership.handle.budget != &orchestrator.byteBudget {
		return ErrReplicatedTransaction
	}
	for index := range ownership.pending {
		pendingReservation := ownership.pending[index]
		if pendingReservation != nil &&
			pendingReservation.budget != &orchestrator.byteBudget &&
			pendingReservation.budget != &orchestrator.activeByteBudget {
			return ErrReplicatedTransaction
		}
		if pendingReservation != nil && pendingReservation.spillBudget != nil &&
			pendingReservation.spillBudget != &orchestrator.byteBudget &&
			pendingReservation.spillBudget != &orchestrator.activeByteBudget {
			return ErrReplicatedTransaction
		}
	}
	releaseReplicatedTransactionTerminalOwnership(handle)
	handle.Phase = ReplicatedTransactionPhaseInvalid
	return nil
}

func replicatedTransactionHandleEscapes(
	result ReplicatedTransactionResult,
	err error,
	handle *ReplicatedTransactionRecoveryHandle,
) bool {
	if result.Recovery == handle {
		return true
	}
	var transactionErr *ReplicatedTransactionError
	return errors.As(err, &transactionErr) && transactionErr.Recovery == handle
}

func releaseReplicatedTransactionTerminalOwnership(
	handle *ReplicatedTransactionRecoveryHandle,
) {
	if handle == nil {
		return
	}
	handle.ownership.releaseAll()
	clear(handle.Pending)
	handle.Pending = nil
	for index := range handle.Participants {
		witness := &handle.Participants[index]
		clear(witness.Route.Replicas)
		witness.Route = ReplicatedRoute{}
	}
	clear(handle.Participants)
	handle.Participants = nil
	handle.ownership = nil
}

func newReplicatedTransactionRecoveryHandle(
	id distributedtxn.ID,
	catalogGeneration uint64,
	coordinator uint32,
	recoveryDeadline int64,
	plan []replicatedTransactionPlanParticipant,
	retainedReservation *replicatedTransactionPendingReservation,
) *ReplicatedTransactionRecoveryHandle {
	handle := &ReplicatedTransactionRecoveryHandle{
		ID: id, CatalogGeneration: catalogGeneration,
		CoordinatorOrdinal: coordinator, RecoveryDeadline: recoveryDeadline,
		Participants: make([]ReplicatedTransactionRouteWitness, len(plan)),
		ownership: &replicatedTransactionRecoveryOwnership{
			handle: retainedReservation,
		},
	}
	for ordinal := range plan {
		handle.Participants[ordinal] = ReplicatedTransactionRouteWitness{
			Route:            cloneReplicatedTransactionRoute(*plan[ordinal].route),
			Ordinal:          uint32(ordinal),
			AuthorityWitness: replicatedRouteAuthorityWitness(*plan[ordinal].route),
			MutationDigest:   plan[ordinal].digest,
		}
	}
	return handle
}

func cloneReplicatedTransactionRoute(route ReplicatedRoute) ReplicatedRoute {
	clone := route
	clone.Distribution = distribution.DistributionName(strings.Clone(string(route.Distribution)))
	clone.Shard = distribution.ShardID(strings.Clone(string(route.Shard)))
	clone.Replicas = make([]ReplicatedEndpoint, len(route.Replicas))
	copy(clone.Replicas, route.Replicas)
	for index := range clone.Replicas {
		clone.Replicas[index].NativeEndpoint = strings.Clone(clone.Replicas[index].NativeEndpoint)
		clone.Replicas[index].Address = strings.Clone(clone.Replicas[index].Address)
		clone.Replicas[index].ControlEndpoint = strings.Clone(clone.Replicas[index].ControlEndpoint)
		clone.Replicas[index].ControlAddress = strings.Clone(clone.Replicas[index].ControlAddress)
	}
	return clone
}
