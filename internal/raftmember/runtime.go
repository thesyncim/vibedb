package raftmember

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrRuntimeClosed reports use after Runtime ownership has begun closing.
	ErrRuntimeClosed = errors.New("raftmember: runtime is closed")
	// ErrRuntimeFailed marks a deterministic terminal Runtime invariant failure.
	ErrRuntimeFailed = errors.New("raftmember: runtime failed")
	// ErrRuntimeOwnership reports a WAL, database, or apply claim that cannot be
	// transferred into one exclusive Runtime owner.
	ErrRuntimeOwnership = errors.New("raftmember: runtime ownership mismatch")
	// ErrSchemaGenerationSwap reports an invalid or non-quiescent live SQL
	// generation replacement attempt.
	ErrSchemaGenerationSwap = errors.New("raftmember: schema generation swap refused")
	// ErrResultSettlementRequired reports a result-bearing apply that cannot
	// begin because no synchronous settlement sink was provided.
	ErrResultSettlementRequired = errors.New("raftmember: applied result settlement sink is required")
	// ErrResultSettlementPending reports a published applied range whose result
	// sink has not yet acknowledged every entry.
	ErrResultSettlementPending = errors.New("raftmember: applied result settlement is pending")
	// ErrResultSettlementRejected marks a retryable settlement sink failure. The
	// Runtime retains the exact applied range and performs no later Node work.
	ErrResultSettlementRejected = errors.New("raftmember: applied result settlement rejected")
	// ErrRetryableResultSettlement identifies the sole sink error class that may
	// retry the identical applied range. Every unwrapped sink error is terminal.
	ErrRetryableResultSettlement = errors.New("raftmember: retryable applied result settlement")
	// ErrReadyWorkspaceRequired reports a result-bearing apply that cannot begin
	// without the caller-owned bounded batch workspace.
	ErrReadyWorkspaceRequired = errors.New("raftmember: Ready workspace is required")
	// errOutboundRejected marks an error returned by the caller's Ready message
	// sink. Only this class is retryable at the same message position.
	errOutboundRejected = errors.New("raftmember: outbound sink rejected message")
)

// GroupKey is the portable logical identity used to select one local member.
// It deliberately excludes member-local StoreID and SQL LogID coordinates.
type GroupKey struct {
	ClusterID             [16]byte
	ClusterIncarnation    [16]byte
	TopologyRecoveryEpoch uint64
	ShardIncarnation      [16]byte
	GroupID               [16]byte
}

// RuntimeIdentity is a detached description of one adopted local member.
type RuntimeIdentity struct {
	Group                GroupKey
	Distribution         string
	Shard                string
	AllocationGeneration uint64
	MemberID             uint64
	StoreID              [16]byte
	NodeIncarnation      uint64
	// RelationManifestDigest is the exact portable relation contract opened by
	// the adopted apply machine. Unlike the bound SQL catalog digest, it omits
	// replica-local storage identities. Serving registration must use this
	// digest so every replica advertises the same command grammar.
	RelationManifestDigest [32]byte
}

// DurablePromotionProof identifies one canonical unapplied promotion entry in
// the retained durable log. It grants no Raft role by itself; authenticated
// transport consumes it only as a narrow election-liveness witness.
type DurablePromotionProof struct {
	Version             uint64
	TargetMember        uint64
	AuthorizationDigest [MembershipTransitionDigestBytes]byte
}

// AppliedSourceOwner is the exact fixed-width identity of one Runtime whose
// published apply batches may settle local serving attempts. Distribution and
// shard labels are deliberately excluded: the sealed allocation generation,
// member store, and node incarnation are the ABA-resistant ownership boundary.
type AppliedSourceOwner struct {
	Group                GroupKey
	AllocationGeneration uint64
	MemberID             uint64
	StoreID              [16]byte
	NodeIncarnation      uint64
}

// AppliedSourceToken is a registry-minted, fixed-width capability for one
// AppliedSourceOwner claim. RegistryID fences different registries while
// OwnerEpoch prevents same-registry release/reclaim ABA.
type AppliedSourceToken struct {
	RegistryID uint64
	OwnerEpoch uint64
}

// AppliedSourceOwner returns the exact serving ownership identity.
func (identity RuntimeIdentity) AppliedSourceOwner() AppliedSourceOwner {
	return AppliedSourceOwner{
		Group: identity.Group, AllocationGeneration: identity.AllocationGeneration,
		MemberID: identity.MemberID, StoreID: identity.StoreID,
		NodeIncarnation: identity.NodeIncarnation,
	}
}

// OutboundMessage is borrowed only for the duration of the DriveReady callback.
// A caller retaining it must clone Message before returning.
type OutboundMessage struct {
	Group   GroupKey
	From    uint64
	To      uint64
	Message *pb.Message
}

// ReadyWorkspace is zero-value scratch for one serialized DriveReady lane. A
// scheduler should reuse one workspace across all of its Runtime groups.
type ReadyWorkspace struct {
	normal raftmodel.NormalApplyBatchWorkspace
}

// AppliedBatch is the exact atomically published normal-entry range offered to
// a settlement sink. Entry data and the final ConfState are borrowed until the
// next DriveReady call. A sink must not mutate or retain borrowed values.
type AppliedBatch struct {
	normal raftmodel.AppliedNormalBatch
	apply  *sqldriver.ReplicatedApply
	source AppliedBatchSource
}

// AppliedBatchCompletionWorkspace owns one reusable exact completion cut for a
// serialized AppliedBatch settlement. The zero value is ready for use. It is
// single-consumer, must not be copied, and retains only bounded snapshot
// scratch between batches.
type AppliedBatchCompletionWorkspace struct {
	apply  sqldriver.CompletionLookupWorkspace
	owner  *sqldriver.ReplicatedApply
	source AppliedBatchSource
}

// AppliedBatchSource is the fixed identity of one applied Ready interval.
// ReadyID is scoped by the exact local member store and Node incarnation.
type AppliedBatchSource struct {
	// Group is the exact GroupKey of the Runtime that published the interval.
	Group                GroupKey
	AllocationGeneration uint64
	MemberID             uint64
	StoreID              [16]byte
	NodeIncarnation      uint64
	ReadyID              uint64
	FirstIndex           uint64
	LastIndex            uint64
	FinalDataChainDigest [32]byte
}

// Owner returns the exact Runtime identity that published this interval.
func (source AppliedBatchSource) Owner() AppliedSourceOwner {
	return AppliedSourceOwner{
		Group: source.Group, AllocationGeneration: source.AllocationGeneration,
		MemberID: source.MemberID, StoreID: source.StoreID,
		NodeIncarnation: source.NodeIncarnation,
	}
}

// Len returns the number of applied normal entries awaiting settlement.
func (batch AppliedBatch) Len() int { return batch.normal.Len() }

// Group returns the exact logical group whose entries were published.
func (batch AppliedBatch) Group() GroupKey { return batch.source.Group }

// Source returns the exact local member and applied Ready interval.
func (batch AppliedBatch) Source() AppliedBatchSource { return batch.source }

// ReadyID identifies the Ready containing the applied range.
func (batch AppliedBatch) ReadyID() uint64 { return batch.normal.ReadyID() }

// FirstIndex returns the first applied index, or zero for an empty value.
func (batch AppliedBatch) FirstIndex() uint64 { return batch.normal.FirstIndex() }

// LastIndex returns the final applied index, or zero for an empty value.
func (batch AppliedBatch) LastIndex() uint64 { return batch.normal.LastIndex() }

// Entry returns one borrowed normal-entry input.
func (batch AppliedBatch) Entry(index int) (raftmodel.NormalApply, bool) {
	return batch.normal.Entry(index)
}

// FinalPublication returns the sole reader-visible publication for the range.
// Its ConfState is borrowed and immutable.
func (batch AppliedBatch) FinalPublication() raftmodel.Publication {
	return batch.normal.FinalPublication()
}

// LookupCompletion resolves the owned deterministic completion for one
// nonempty command. hasCommand is false for an empty Raft no-op. The lookup is
// intentionally performed only when a serving settlement sink needs it.
func (batch AppliedBatch) LookupCompletion(
	index int,
) (lookup replicatedstate.CompletionLookup, hasCommand bool, err error) {
	entry, ok := batch.normal.Entry(index)
	if !ok {
		return replicatedstate.CompletionLookup{}, false, errors.New("raftmember: applied result index out of range")
	}
	if len(entry.Data) == 0 {
		return replicatedstate.CompletionLookup{}, false, nil
	}
	if batch.apply == nil {
		return replicatedstate.CompletionLookup{}, true, ErrRuntimeClosed
	}
	lookup, err = batch.apply.LookupCompletion(entry.Data)
	return lookup, true, err
}

// LookupCompletionInto is LookupCompletion with caller-owned result storage.
func (batch AppliedBatch) LookupCompletionInto(
	index int,
	dst []byte,
) (lookup replicatedstate.CompletionLookup, hasCommand bool, err error) {
	entry, ok := batch.normal.Entry(index)
	if !ok {
		return replicatedstate.CompletionLookup{}, false, errors.New("raftmember: applied result index out of range")
	}
	if len(entry.Data) == 0 {
		return replicatedstate.CompletionLookup{}, false, nil
	}
	if batch.apply == nil {
		return replicatedstate.CompletionLookup{}, true, ErrRuntimeClosed
	}
	lookup, err = batch.apply.LookupCompletionInto(entry.Data, dst)
	return lookup, true, err
}

// BeginCompletionLookup captures one exact durable cut for every completion
// lookup subsequently made through workspace for this AppliedBatch.
func (batch AppliedBatch) BeginCompletionLookup(
	workspace *AppliedBatchCompletionWorkspace,
) error {
	if batch.apply == nil {
		return ErrRuntimeClosed
	}
	if workspace == nil || workspace.owner != nil {
		return replicatedstate.ErrCompletionWorkspaceBusy
	}
	if err := batch.apply.BeginCompletionLookupBatch(
		&workspace.apply, batch.FinalPublication(),
	); err != nil {
		return err
	}
	workspace.owner = batch.apply
	workspace.source = batch.source
	return nil
}

// LookupCompletionIntoWorkspace is LookupCompletionInto through the exact cut
// captured by BeginCompletionLookup.
func (batch AppliedBatch) LookupCompletionIntoWorkspace(
	workspace *AppliedBatchCompletionWorkspace,
	index int,
	dst []byte,
) (lookup replicatedstate.CompletionLookup, hasCommand bool, err error) {
	if workspace == nil || workspace.owner != batch.apply || workspace.source != batch.source {
		return replicatedstate.CompletionLookup{}, false, replicatedstate.ErrCompletionWorkspaceBusy
	}
	entry, ok := batch.normal.Entry(index)
	if !ok {
		return replicatedstate.CompletionLookup{}, false, errors.New("raftmember: applied result index out of range")
	}
	if len(entry.Data) == 0 {
		return replicatedstate.CompletionLookup{}, false, nil
	}
	lookup, err = batch.apply.LookupCompletionIntoWorkspace(&workspace.apply, entry.Data, dst)
	return lookup, true, err
}

// EndCompletionLookup releases the exact durable cut. The workspace remains
// warm for a later AppliedBatch.
func (batch AppliedBatch) EndCompletionLookup(
	workspace *AppliedBatchCompletionWorkspace,
) error {
	if workspace == nil || workspace.owner != batch.apply || workspace.source != batch.source {
		return replicatedstate.ErrCompletionWorkspaceBusy
	}
	workspace.owner = nil
	workspace.source = AppliedBatchSource{}
	return batch.apply.EndCompletionLookupBatch(&workspace.apply)
}

// Release drops every inactive reusable snapshot buffer retained by workspace.
func (workspace *AppliedBatchCompletionWorkspace) Release() error {
	if workspace == nil {
		return nil
	}
	if workspace.owner != nil {
		return replicatedstate.ErrCompletionWorkspaceBusy
	}
	workspace.source = AppliedBatchSource{}
	return workspace.apply.Release()
}

// ResultSettlementSink runs synchronously after apply publication and before
// read-state release or Ready advancement. A sink must not retain borrowed
// values or re-enter the Runtime. Returning an error is terminal unless it was
// produced by RetryResultSettlement. A retry receives the identical source and
// range, so every sink must be idempotent.
type ResultSettlementSink func(AppliedBatch) error

type retryableResultSettlement struct {
	cause error
}

func (failure retryableResultSettlement) Error() string {
	return failure.cause.Error()
}

func (failure retryableResultSettlement) Unwrap() []error {
	return []error{ErrRetryableResultSettlement, failure.cause}
}

// RetryResultSettlement marks a sink failure whose side effects may be
// outcome-unknown and whose identical source and range must be retried.
func RetryResultSettlement(cause error) error {
	if cause == nil {
		cause = errors.New("unspecified retryable settlement failure")
	}
	return retryableResultSettlement{cause: cause}
}

// DriveKind identifies the single Ready lifecycle operation performed by one
// DriveReady call. DriveIdle means no Ready was available and no work occurred.
type DriveKind uint8

const (
	DriveIdle DriveKind = iota
	DriveCaptured
	DrivePersisted
	DriveMessage
	DriveMessagesFinished
	DriveSnapshotFinished
	DriveEntry
	DriveNormalBatch
	DriveEntriesFinished
	DriveReadState
	DriveReadStatesFinished
	DriveAdvanced
)

// DriveResult reports one bounded synchronous Ready micro-step. ReadOutcomes
// is present only for DriveReadStatesFinished; ownership of the slice and its
// detached contexts transfers to the caller.
type DriveResult struct {
	Kind         DriveKind
	ReadyID      uint64
	ReadOutcomes []raftmodel.ReadOutcome
	Applied      AppliedBatch
}

// RuntimeStatus is a detached allocation-free control-plane view. It is
// evidence only; serving authority still requires topology ownership fences.
type RuntimeStatus struct {
	MemberID          uint64
	LeaderID          uint64
	Term              uint64
	Commit            uint64
	Applied           uint64
	CheckpointApplied uint64
	LeadTransferee    uint64
	RaftState         raft.StateType
}

// Progressed reports whether DriveReady performed one lifecycle operation.
func (result DriveResult) Progressed() bool { return result.Kind != DriveIdle }

// Runtime exclusively owns one exact WAL, bound SQL database, opaque apply
// claim, and synchronous Raft Node. It is deliberately not safe for concurrent
// use; a single scheduler must serialize every method.
//
// Runtime is a non-serving kernel. Proposal success means only local core
// admission. It does not certify leadership, commit, apply, or a client result.
type Runtime struct {
	wal             *raftstore.Store
	database        *sqldriver.Database
	apply           *sqldriver.ReplicatedApply
	node            *raftmodel.Node
	identity        RuntimeIdentity
	walGeneration   *walGenerationDriver
	schemaWALResume *WALGenerationDriverOptions

	proposalBatchEntries     int
	proposalBatchBytes       int64
	promotionScan            durablePromotionScan
	failure                  error
	stopping                 bool
	closed                   bool
	schemaGenerationQuiesced bool
}

type durablePromotionScan struct {
	applied uint64
	last    uint64
	commit  uint64
	target  uint64
	proof   DurablePromotionProof
	found   bool
	valid   bool
}

// AdoptRuntime constructs the sole synchronous owner of wal, database, and
// apply. Read-only qualification failures leave ownership with the caller. Once
// BeginIncarnation is attempted, ownership transfers to the returned Runtime;
// if setup then fails, AdoptRuntime closes all three handles. A nonnil Runtime
// is returned with the joined error only when that cleanup needs a retry.
//
// The caller must already exclusively own the three handles and must not use
// them concurrently with this call or after a successful ownership transfer.
func AdoptRuntime(
	wal *raftstore.Store,
	database *sqldriver.Database,
	apply *sqldriver.ReplicatedApply,
) (*Runtime, error) {
	return adoptRuntime(wal, database, apply, 0)
}

// AdoptStagedRuntime adopts one preplanned learner incarnation. The first
// attempt mints expected; an exact retry may reclaim it only through the WAL's
// pristine-incarnation proof. It never silently advances to expected+1.
func AdoptStagedRuntime(
	wal *raftstore.Store,
	database *sqldriver.Database,
	apply *sqldriver.ReplicatedApply,
	expected uint64,
) (*Runtime, error) {
	if expected == 0 {
		return nil, ErrRuntimeOwnership
	}
	return adoptRuntime(wal, database, apply, expected)
}

func adoptRuntime(
	wal *raftstore.Store,
	database *sqldriver.Database,
	apply *sqldriver.ReplicatedApply,
	expected uint64,
) (*Runtime, error) {
	if wal == nil || database == nil || apply == nil {
		return nil, ErrRuntimeOwnership
	}
	if err := ValidateImmutableBaseApplyCapacity(wal, apply); err != nil {
		return nil, err
	}
	profile, err := apply.CapacityQualificationProfile()
	if err != nil {
		return nil, err
	}
	liveBinding, err := BindingFromWAL(wal, profile.Binding.Authority)
	if err != nil {
		return nil, err
	}
	if liveBinding != profile.Binding {
		return nil, ErrBindingMismatch
	}
	current := wal.CurrentIncarnation()
	resume := expected != 0 && current == expected
	if expected != 0 && !resume && (current == ^uint64(0) || current+1 != expected) {
		return nil, ErrRuntimeOwnership
	}
	// This is the ownership transfer point. It atomically retires the public SQL
	// connector so no session can race incarnation minting or Node construction.
	if err := apply.ClaimRuntimeOwnership(database); err != nil {
		return nil, fmt.Errorf("%w: claim SQL apply ownership: %w", ErrRuntimeOwnership, err)
	}

	sealed := wal.Identity()
	runtime := &Runtime{
		wal: wal, database: database, apply: apply,
		identity: RuntimeIdentity{
			Group: GroupKey{
				ClusterID: sealed.ClusterID, ClusterIncarnation: sealed.ClusterIncarnation,
				TopologyRecoveryEpoch: wal.TopologyRecoveryEpoch(),
				ShardIncarnation:      sealed.ShardIncarnation,
				GroupID:               sealed.GroupID,
			},
			Distribution: strings.Clone(sealed.Distribution),
			Shard:        strings.Clone(sealed.Shard), AllocationGeneration: sealed.AllocationGeneration,
			MemberID: sealed.MemberID, StoreID: sealed.StoreID,
			RelationManifestDigest: profile.RelationManifestDigest,
		},
	}
	var incarnation uint64
	if resume {
		err = wal.ResumePristineIncarnation(expected)
		incarnation = expected
	} else {
		incarnation, err = wal.BeginIncarnation()
		if err == nil && expected != 0 && incarnation != expected {
			err = ErrRuntimeOwnership
		}
	}
	if err != nil {
		return runtime.abortAdoption(fmt.Errorf("raftmember: begin incarnation: %w", err))
	}
	runtime.identity.NodeIncarnation = incarnation
	runtime.node, err = raftmodel.NewNode(sealed.MemberID, incarnation, wal, apply)
	if err != nil {
		return runtime.abortAdoption(fmt.Errorf("raftmember: construct node: %w", err))
	}
	if err = runtime.node.BindMembershipTransitionContext(); err != nil {
		return runtime.abortAdoption(fmt.Errorf("raftmember: bind membership transition context: %w", err))
	}
	return runtime, nil
}

func (runtime *Runtime) abortAdoption(setupErr error) (*Runtime, error) {
	closeErr := runtime.Close()
	if closeErr == nil {
		return nil, setupErr
	}
	return runtime, errors.Join(setupErr, closeErr)
}

// Identity returns a detached immutable Runtime identity.
func (runtime *Runtime) Identity() RuntimeIdentity {
	if runtime == nil {
		return RuntimeIdentity{}
	}
	result := runtime.identity
	result.Distribution = strings.Clone(result.Distribution)
	result.Shard = strings.Clone(result.Shard)
	return result
}

// Failure returns the stable terminal invariant failure, if any.
func (runtime *Runtime) Failure() error {
	if runtime == nil || runtime.closed || runtime.stopping || runtime.node == nil {
		return ErrRuntimeClosed
	}
	if runtime.failure != nil {
		return runtime.failure
	}
	if runtime.node != nil && runtime.node.Phase() == raftmodel.PhaseFailed {
		return runtime.node.Failure()
	}
	return nil
}

func (runtime *Runtime) checkUsable() error {
	if runtime == nil || runtime.closed || runtime.stopping || runtime.node == nil ||
		runtime.wal == nil || runtime.apply == nil || runtime.database == nil {
		return ErrRuntimeClosed
	}
	if runtime.failure != nil {
		return runtime.failure
	}
	if runtime.node.Phase() == raftmodel.PhaseFailed {
		return runtime.fail(runtime.node.Failure())
	}
	return nil
}

// QuiesceSQLGeneration fences every later Runtime operation, proves the Raft
// node has no pending Ready/read/settlement work, and releases only the SQL
// generation. WAL, RawNode, member incarnation, and replication progress stay
// owned by Runtime for an exact target-generation install.
func (runtime *Runtime) QuiesceSQLGeneration() error {
	if runtime == nil || runtime.closed || runtime.stopping || runtime.failure != nil ||
		runtime.node == nil || runtime.wal == nil || runtime.apply == nil ||
		runtime.database == nil || runtime.schemaGenerationQuiesced {
		return ErrSchemaGenerationSwap
	}
	if err := runtime.node.ReplaceStateMachine(runtime.apply); err != nil {
		return errors.Join(ErrSchemaGenerationSwap, err)
	}
	if driver := runtime.walGeneration; driver != nil {
		driver.stopAndWait()
		resume := &WALGenerationDriverOptions{
			IntervalTicks: driver.interval, Key: driver.key, OnError: driver.onError,
		}
		resume.Key.Wrapped = append([]byte(nil), driver.key.Wrapped...)
		runtime.schemaWALResume = resume
		clear(driver.key.Material[:])
		clear(driver.key.Wrapped)
		runtime.walGeneration = nil
	}
	if err := runtime.apply.Close(); err != nil {
		return errors.Join(ErrSchemaGenerationSwap, err)
	}
	runtime.apply = nil
	if err := runtime.database.Close(); err != nil {
		return errors.Join(ErrSchemaGenerationSwap, err)
	}
	runtime.database = nil
	runtime.schemaGenerationQuiesced = true
	return nil
}

func (runtime *Runtime) ObserveSchemaTransition(command []byte) (uint64, bool, error) {
	if err := runtime.checkUsable(); err != nil {
		return 0, false, err
	}
	return runtime.apply.ObserveReplicatedSchemaTransition(command)
}

// InstallSQLGeneration atomically replaces the quiesced local state-machine
// handle at the identical durable Raft publication. The exact catalog and
// apply identities are checked before Node publication; failure leaves Runtime
// quiesced and serving-fenced for a retry or process recovery.
func (runtime *Runtime) InstallSQLGeneration(
	database *sqldriver.Database,
	apply *sqldriver.ReplicatedApply,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	expectedApply sqldriver.ReplicatedApplyIdentity,
) error {
	if runtime == nil || runtime.closed || runtime.stopping || runtime.failure != nil ||
		!runtime.schemaGenerationQuiesced || runtime.node == nil || runtime.wal == nil ||
		runtime.apply != nil || runtime.database != nil || database == nil || apply == nil {
		return ErrSchemaGenerationSwap
	}
	if _, err := database.RequireReplicatedShardStore(expectedSQL); err != nil {
		return errors.Join(ErrSchemaGenerationSwap, err)
	}
	actualApply, err := apply.Identity()
	if err != nil || actualApply != expectedApply {
		return errors.Join(ErrSchemaGenerationSwap, err)
	}
	manifest, err := apply.RangeSplitRelationManifestDigest()
	if err != nil || manifest == ([32]byte{}) {
		return errors.Join(ErrSchemaGenerationSwap, err)
	}
	if err = runtime.node.ReplaceStateMachine(apply); err != nil {
		return errors.Join(ErrSchemaGenerationSwap, err)
	}
	runtime.database = database
	runtime.apply = apply
	runtime.identity.RelationManifestDigest = manifest
	runtime.schemaGenerationQuiesced = false
	if runtime.schemaWALResume != nil {
		runtime.walGeneration = newWALGenerationDriver(*runtime.schemaWALResume)
		clear(runtime.schemaWALResume.Key.Material[:])
		clear(runtime.schemaWALResume.Key.Wrapped)
		runtime.schemaWALResume = nil
	}
	return nil
}

func (runtime *Runtime) fail(cause error) error {
	if cause == nil {
		cause = errors.New("unknown terminal failure")
	}
	runtime.proposalBatchEntries = 0
	runtime.proposalBatchBytes = 0
	if runtime.failure == nil {
		runtime.failure = errors.Join(ErrRuntimeFailed, cause)
	}
	return runtime.failure
}

func (runtime *Runtime) checkNoPendingSettlement() error {
	if err := runtime.checkUsable(); err != nil {
		return err
	}
	if _, pending := runtime.pendingAppliedResults(); pending {
		return ErrResultSettlementPending
	}
	return nil
}

func (runtime *Runtime) requireEmptyInputWindow() error {
	if err := runtime.checkNoPendingSettlement(); err != nil {
		return err
	}
	if runtime.node.Phase() != raftmodel.PhaseIdle {
		return fmt.Errorf("raftmember: drain captured Ready before protocol input: %w", raftmodel.ErrReadyPending)
	}
	hasReady, err := runtime.node.HasReady()
	if err != nil {
		return err
	}
	if hasReady {
		return fmt.Errorf("raftmember: capture and drain Ready before protocol input: %w", raftmodel.ErrReadyPending)
	}
	return nil
}

// Propose admits one already-encoded replicated command. Consecutive normal
// proposals may share one bounded uncaptured Ready window. The first proposal
// reserves worst-case WAL headroom; later proposals are admitted only while
// the entry and coalescing-byte limits still fit that same reservation. A first
// proposal above the coalescing target remains valid, but closes the batch.
// Every other protocol input remains an empty-window barrier. Nil means only
// that the local core accepted the proposal; it grants no serving or result
// claim.
func (runtime *Runtime) Propose(data []byte) error {
	if err := runtime.checkUsable(); err != nil {
		return err
	}
	dataBytes := int64(len(data))
	continuing := runtime.proposalBatchEntries != 0
	if continuing {
		if runtime.node.Phase() != raftmodel.PhaseIdle ||
			runtime.proposalBatchEntries >= raftmodel.MaxProposalBatchEntries ||
			runtime.proposalBatchBytes >= raftmodel.MaxProposalBatchBytes ||
			dataBytes > raftmodel.MaxProposalBatchBytes-runtime.proposalBatchBytes {
			return errors.Join(raftmodel.ErrReadyPending, raftmodel.ErrAdmissionBound)
		}
	} else if err := runtime.requireEmptyInputWindow(); err != nil {
		return err
	}
	if err := runtime.apply.AdmitCommand(data); err != nil {
		if errors.Is(err, replicatedstate.ErrApplyPoisoned) ||
			errors.Is(err, sqldriver.ErrReplicatedApplyClosed) {
			return runtime.fail(err)
		}
		return err
	}
	if !continuing {
		if err := runtime.wal.ReserveReady(); err != nil {
			if deterministicPersistFailure(err) {
				return runtime.fail(err)
			}
			return err
		}
	}
	if err := runtime.node.Propose(data); err != nil {
		return err
	}
	runtime.proposalBatchEntries++
	runtime.proposalBatchBytes += dataBytes
	return nil
}

// ProposeConfChange admits one topology-authorized configuration change into
// the existing model-checked Raft path. Runtime does not itself authorize
// membership or grant serving authority. Nil means only local core admission.
func (runtime *Runtime) ProposeConfChange(change pb.ConfChangeI) error {
	if err := runtime.requireEmptyInputWindow(); err != nil {
		return err
	}
	if err := runtime.wal.ReserveReady(); err != nil {
		if deterministicPersistFailure(err) {
			return runtime.fail(err)
		}
		return err
	}
	return runtime.node.ProposeConfChange(change)
}

// ReadIndex starts one quorum-confirmed linearizable-read barrier. Context is
// copied by the Node before return. Completion is surfaced exactly once in a
// later DriveResult.ReadOutcomes value; it does not itself expose SQL data.
func (runtime *Runtime) ReadIndex(context []byte) error {
	if err := runtime.requireEmptyInputWindow(); err != nil {
		return err
	}
	// ReadIndex cannot append an entry or advance HardState. Its Ready contains
	// only messages/read states, so pessimistic worst-case WAL reservation would
	// incorrectly make linearizable reads unavailable at a sealed log limit.
	// PersistReady still performs the empty-batch namespace proof before send.
	return runtime.node.ReadIndex(context)
}

// Publication returns a detached view of the atomically published apply cut.
// It is diagnostic/control evidence, not a leader or range-ownership proof.
func (runtime *Runtime) Publication() (raftmodel.Publication, error) {
	if err := runtime.checkUsable(); err != nil {
		return raftmodel.Publication{}, err
	}
	return runtime.node.Published(), nil
}

// DurablePromotion finds the newest exact canonical AddNode entry for target
// in the bounded durable-but-unapplied suffix. This reconstructs the election
// witness after restart without trusting transient transport observations.
func (runtime *Runtime) DurablePromotion(
	target uint64,
) (DurablePromotionProof, bool, error) {
	if err := runtime.checkUsable(); err != nil {
		return DurablePromotionProof{}, false, err
	}
	if target == 0 {
		return DurablePromotionProof{}, false, errors.New("raftmember: invalid promotion target")
	}
	applied := runtime.node.PublishedApplied()
	last, err := runtime.wal.LastIndex()
	if err != nil {
		return DurablePromotionProof{}, false, err
	}
	commit, err := runtime.wal.DurableCommit()
	if err != nil {
		return DurablePromotionProof{}, false, err
	}
	if commit <= applied {
		runtime.promotionScan = durablePromotionScan{applied: applied,
			last: last, commit: commit, target: target, valid: true}
		return DurablePromotionProof{}, false, nil
	}
	if cached := runtime.promotionScan; cached.valid && cached.applied == applied &&
		cached.last == last && cached.commit == commit && cached.target == target {
		return cached.proof, cached.found, nil
	}
	first := applied + 1
	lastCommitted := min(last, commit)
	if span := lastCommitted - first + 1; span > raftmodel.MaxMessageEntries {
		first = lastCommitted - raftmodel.MaxMessageEntries + 1
	}
	for index := lastCommitted; index >= first; index-- {
		entries, readErr := runtime.wal.Entries(index, index+1, raftmodel.MaxInboundMessageBytes)
		if readErr != nil {
			return DurablePromotionProof{}, false, readErr
		}
		if len(entries) == 1 {
			promotionTarget, digest := durablePromotion(entries[0])
			if promotionTarget != target {
				if index == first {
					break
				}
				continue
			}
			proof := DurablePromotionProof{Version: index, TargetMember: target,
				AuthorizationDigest: digest}
			runtime.promotionScan = durablePromotionScan{applied: applied,
				last: last, commit: commit, target: target, proof: proof, found: true, valid: true}
			return proof, true, nil
		}
		if index == first {
			break
		}
	}
	runtime.promotionScan = durablePromotionScan{applied: applied,
		last: last, commit: commit, target: target, valid: true}
	return DurablePromotionProof{}, false, nil
}

func durablePromotion(entry *pb.Entry) (uint64, [MembershipTransitionDigestBytes]byte) {
	var digest [MembershipTransitionDigestBytes]byte
	if entry == nil {
		return 0, digest
	}
	if entry.GetType() == pb.EntryConfChange {
		var change pb.ConfChange
		if err := proto.Unmarshal(entry.GetData(), &change); err != nil ||
			change.GetType() != pb.ConfChangeAddNode ||
			len(change.GetContext()) != MembershipTransitionDigestBytes {
			return 0, digest
		}
		canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(&change)
		if err != nil || !bytes.Equal(canonical, entry.GetData()) {
			return 0, digest
		}
		copy(digest[:], change.GetContext())
		return change.GetNodeId(), digest
	}
	if entry.GetType() != pb.EntryConfChangeV2 {
		return 0, digest
	}
	var change pb.ConfChangeV2
	if err := proto.Unmarshal(entry.GetData(), &change); err != nil ||
		len(change.GetContext()) != MembershipTransitionDigestBytes || len(change.GetChanges()) != 1 ||
		change.GetTransition() != pb.ConfChangeTransition_ConfChangeTransitionAuto ||
		change.GetChanges()[0].GetType() != pb.ConfChangeAddNode {
		return 0, digest
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(&change)
	if err != nil || !bytes.Equal(canonical, entry.GetData()) {
		return 0, digest
	}
	copy(digest[:], change.GetContext())
	return change.GetChanges()[0].GetNodeId(), digest
}

// SnapshotState returns the complete coherent durable state paired with a
// short-lived read snapshot. It is control-plane evidence for certified
// learner installation; callers do not receive collection handles or serving
// authority.
func (runtime *Runtime) SnapshotState() (replicatedstate.State, error) {
	if err := runtime.checkNoPendingSettlement(); err != nil {
		return replicatedstate.State{}, err
	}
	cut, err := runtime.apply.SnapshotArtifactCut()
	if err != nil {
		return replicatedstate.State{}, err
	}
	state := cut.State()
	if closeErr := cut.Close(); closeErr != nil {
		return replicatedstate.State{}, closeErr
	}
	return state, nil
}

// SnapshotBaseCertificate returns the exact immutable snapshot-base
// certificate sealed into the current WAL generation. It never synthesizes a
// certificate from live state: callers use the returned digest together with
// SnapshotState to reject a stale generation or unrelated base.
func (runtime *Runtime) SnapshotBaseCertificate() (replicatedstate.SnapshotBaseCertificate, error) {
	if err := runtime.checkNoPendingSettlement(); err != nil {
		return replicatedstate.SnapshotBaseCertificate{}, err
	}
	snapshot, err := runtime.wal.Snapshot()
	if err != nil {
		return replicatedstate.SnapshotBaseCertificate{}, err
	}
	return replicatedstate.OpenSnapshotBase(snapshot)
}

// Status returns detached local Raft status without allocating the leader's
// complete progress map.
func (runtime *Runtime) Status() (RuntimeStatus, error) {
	if err := runtime.checkUsable(); err != nil {
		return RuntimeStatus{}, err
	}
	status := runtime.node.Status()
	publishedApplied := runtime.node.PublishedApplied()
	checkpointApplied := runtime.apply.CheckpointAppliedIndex()
	if status.Applied > publishedApplied || publishedApplied > status.GetCommit() ||
		checkpointApplied > publishedApplied {
		return RuntimeStatus{}, runtime.fail(fmt.Errorf(
			"runtime status cuts are inconsistent: raw applied %d, published %d, checkpoint %d, commit %d",
			status.Applied, publishedApplied, checkpointApplied, status.GetCommit(),
		))
	}
	return RuntimeStatus{
		MemberID: status.ID, LeaderID: status.Lead, Term: status.GetTerm(),
		Commit: status.GetCommit(), Applied: publishedApplied,
		CheckpointApplied: checkpointApplied,
		LeadTransferee:    status.LeadTransferee, RaftState: status.RaftState,
	}, nil
}

// WALRetentionInput returns the certificate-backed contiguous apply cut. The
// current append-only store uses it only as qualification evidence; a future
// compactor must additionally prove the exact term, configuration, member
// lineage, certificate witness, and retained suffix before deleting anything.
func (runtime *Runtime) WALRetentionInput() (uint64, error) {
	if err := runtime.checkNoPendingSettlement(); err != nil {
		return 0, err
	}
	checkpoint := runtime.apply.CheckpointAppliedIndex()
	applied := runtime.apply.Applied()
	if checkpoint > applied {
		return 0, runtime.fail(fmt.Errorf(
			"%w: checkpoint applied %d exceeds visible applied %d",
			ErrRuntimeFailed, checkpoint, applied,
		))
	}
	return checkpoint, nil
}

// Progress returns one allocation-free detached follower progress record from
// a local leader.
func (runtime *Runtime) Progress(memberID uint64) (raftmodel.MemberProgress, bool, error) {
	if err := runtime.checkUsable(); err != nil {
		return raftmodel.MemberProgress{}, false, err
	}
	progress, found := runtime.node.Progress(memberID)
	return progress, found, nil
}

// TransferLeader starts an explicit handoff to one configured voter. The
// caller must drain Ready and observe Status before treating the handoff as
// complete.
func (runtime *Runtime) TransferLeader(transferee uint64) error {
	if err := runtime.requireEmptyInputWindow(); err != nil {
		return err
	}
	return runtime.node.TransferLeader(transferee)
}

// StepMessage admits one ordinary, non-snapshot peer message. The message is
// detached by raftmodel.Node before this method returns.
func (runtime *Runtime) StepMessage(message *pb.Message) error {
	if err := runtime.requireEmptyInputWindow(); err != nil {
		return err
	}
	if _, err := validateOrdinaryMessage(message); err != nil {
		return err
	}
	if message.GetTo() != runtime.identity.MemberID {
		return errors.New("raftmember: ordinary message targets another member")
	}
	if err := runtime.wal.ReserveReady(); err != nil {
		if deterministicPersistFailure(err) {
			return runtime.fail(err)
		}
		return err
	}
	return runtime.node.Step(message)
}

// Tick advances exactly one logical Raft tick. Runtime supplies no wall-clock
// cadence and owns no timer.
func (runtime *Runtime) Tick() error {
	if err := runtime.requireEmptyInputWindow(); err != nil {
		return err
	}
	if err := runtime.wal.ReserveReady(); err != nil {
		if deterministicPersistFailure(err) {
			return runtime.fail(err)
		}
		return err
	}
	runtime.tickWALGeneration()
	return runtime.node.Tick()
}

// Campaign starts one local election attempt after Ready and WAL admission.
func (runtime *Runtime) Campaign() error {
	if err := runtime.requireEmptyInputWindow(); err != nil {
		return err
	}
	if err := runtime.wal.ReserveReady(); err != nil {
		if deterministicPersistFailure(err) {
			return runtime.fail(err)
		}
		return err
	}
	return runtime.node.Campaign()
}

// DriveReady performs at most one explicit Ready lifecycle operation. Message
// callbacks run only after the Ready's stable-storage boundary. A callback
// error leaves the exact message position unchanged for explicit retry.
// Result settlement runs synchronously after a normal apply publishes and
// before this Ready can release read states or advance. A failure marked by
// RetryResultSettlement retries the identical ReadyID and range on the next
// call. Any other settlement failure is terminal.
func (runtime *Runtime) DriveReady(
	workspace *ReadyWorkspace,
	send func(OutboundMessage) error,
	settle ResultSettlementSink,
) (DriveResult, error) {
	if err := runtime.checkUsable(); err != nil {
		return DriveResult{}, err
	}
	if _, pending := runtime.pendingAppliedResults(); pending {
		return runtime.settleAppliedResults(settle)
	}
	switch runtime.node.Phase() {
	case raftmodel.PhaseIdle:
		captured, err := runtime.node.CaptureReady()
		if err != nil || !captured {
			if err == nil && runtime.proposalBatchEntries != 0 {
				return DriveResult{}, runtime.fail(raftmodel.ErrReadyPending)
			}
			return DriveResult{}, err
		}
		runtime.proposalBatchEntries = 0
		runtime.proposalBatchBytes = 0
		progress, ok := runtime.node.CurrentReady()
		if !ok {
			return DriveResult{}, runtime.fail(errors.New("captured Ready has no progress record"))
		}
		if progress.HasSnapshot {
			return DriveResult{}, runtime.fail(&raftmodel.UnsupportedError{
				Feature: "in-band Ready snapshots in the immutable-base WAL kernel",
			})
		}
		return DriveResult{Kind: DriveCaptured, ReadyID: progress.ReadyID}, nil

	case raftmodel.PhaseCaptured:
		readyID := runtime.node.ReadyID()
		if err := runtime.node.PersistReady(); err != nil {
			if deterministicPersistFailure(err) {
				return DriveResult{}, runtime.fail(err)
			}
			return DriveResult{}, err
		}
		return DriveResult{Kind: DrivePersisted, ReadyID: readyID}, nil

	case raftmodel.PhasePersisted:
		progress, ok := runtime.node.CurrentReady()
		if !ok {
			return DriveResult{}, runtime.fail(errors.New("persisted Ready has no progress record"))
		}
		if progress.MessagesSent == progress.MessageCount {
			if err := runtime.node.FinishMessages(); err != nil {
				return DriveResult{}, runtime.fail(err)
			}
			return DriveResult{Kind: DriveMessagesFinished, ReadyID: progress.ReadyID}, nil
		}
		var sinkErr error
		sent, err := runtime.node.SendNextMessage(func(message *pb.Message) error {
			if _, validationErr := validateOrdinaryMessage(message); validationErr != nil {
				return validationErr
			}
			if message.GetFrom() != runtime.identity.MemberID || message.GetTo() == raft.None ||
				message.GetTo() == runtime.identity.MemberID {
				return errors.New("raftmember: invalid outbound message identity")
			}
			if send == nil {
				sinkErr = errors.New("raftmember: nil outbound sink")
				return errors.Join(errOutboundRejected, sinkErr)
			}
			sinkErr = send(OutboundMessage{
				Group: runtime.identity.Group, From: message.GetFrom(), To: message.GetTo(), Message: message,
			})
			if sinkErr != nil {
				return errors.Join(errOutboundRejected, sinkErr)
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, errOutboundRejected) {
				return DriveResult{}, sinkErr
			}
			return DriveResult{}, runtime.fail(err)
		}
		if !sent {
			return DriveResult{}, runtime.fail(errors.New("Ready message position made no progress"))
		}
		return DriveResult{Kind: DriveMessage, ReadyID: progress.ReadyID}, nil

	case raftmodel.PhaseMessagesDrained:
		readyID := runtime.node.ReadyID()
		if err := runtime.node.InstallSnapshot(); err != nil {
			return DriveResult{}, runtime.fail(err)
		}
		return DriveResult{Kind: DriveSnapshotFinished, ReadyID: readyID}, nil

	case raftmodel.PhaseSnapshotInstalled:
		readyID := runtime.node.ReadyID()
		requiresSettlement, err := runtime.node.NextApplyRequiresResultSettlement()
		if err != nil {
			return DriveResult{}, runtime.fail(err)
		}
		if requiresSettlement && settle == nil {
			return DriveResult{}, ErrResultSettlementRequired
		}
		if requiresSettlement && workspace == nil {
			return DriveResult{}, ErrReadyWorkspaceRequired
		}
		var normalWorkspace *raftmodel.NormalApplyBatchWorkspace
		if workspace != nil {
			normalWorkspace = &workspace.normal
		}
		appliedResult, err := runtime.node.ApplyNextBatch(normalWorkspace)
		if err != nil {
			return DriveResult{}, runtime.fail(err)
		}
		kind := DriveEntriesFinished
		if appliedResult.Applied != 0 {
			kind = DriveEntry
		}
		if appliedResult.Normal.Len() != 0 {
			return runtime.settleAppliedResults(settle)
		}
		return DriveResult{Kind: kind, ReadyID: readyID}, nil

	case raftmodel.PhaseEntriesApplied:
		progress, ok := runtime.node.CurrentReady()
		if !ok {
			return DriveResult{}, runtime.fail(errors.New("applied Ready has no progress record"))
		}
		if progress.ReadStatesRecorded < progress.ReadStateCount {
			recorded, err := runtime.node.RecordNextReadState()
			if err != nil {
				return DriveResult{}, runtime.fail(err)
			}
			if !recorded {
				return DriveResult{}, runtime.fail(errors.New("Ready read-state position made no progress"))
			}
			return DriveResult{Kind: DriveReadState, ReadyID: progress.ReadyID}, nil
		}
		outcomes, err := runtime.node.FinishReadStates()
		if err != nil {
			return DriveResult{}, runtime.fail(err)
		}
		return DriveResult{
			Kind: DriveReadStatesFinished, ReadyID: progress.ReadyID,
			ReadOutcomes: outcomes,
		}, nil

	case raftmodel.PhaseReadStatesRecorded:
		readyID := runtime.node.ReadyID()
		if err := runtime.node.AdvanceReady(); err != nil {
			return DriveResult{}, runtime.fail(err)
		}
		return DriveResult{Kind: DriveAdvanced, ReadyID: readyID}, nil

	case raftmodel.PhaseFailed:
		return DriveResult{}, runtime.fail(runtime.node.Failure())
	default:
		return DriveResult{}, runtime.fail(fmt.Errorf("unknown Ready phase %d", runtime.node.Phase()))
	}
}

func (runtime *Runtime) settleAppliedResults(
	settle ResultSettlementSink,
) (DriveResult, error) {
	batch, pending := runtime.pendingAppliedResults()
	if !pending {
		return DriveResult{}, runtime.fail(errors.New("settlement called without a pending applied range"))
	}
	if settle == nil {
		return DriveResult{}, ErrResultSettlementRequired
	}
	if err := settle(batch); err != nil {
		if errors.Is(err, ErrRetryableResultSettlement) {
			return DriveResult{}, errors.Join(ErrResultSettlementRejected, err)
		}
		return DriveResult{}, runtime.fail(err)
	}
	if err := runtime.node.SettleAppliedNormalBatch(batch.normal); err != nil {
		return DriveResult{}, runtime.fail(err)
	}
	return DriveResult{
		Kind: DriveNormalBatch, ReadyID: batch.ReadyID(), Applied: batch,
	}, nil
}

func (runtime *Runtime) pendingAppliedResults() (AppliedBatch, bool) {
	if runtime == nil || runtime.node == nil {
		return AppliedBatch{}, false
	}
	normal, pending := runtime.node.PendingAppliedNormalBatch()
	if !pending {
		return AppliedBatch{}, false
	}
	return AppliedBatch{
		normal: normal,
		apply:  runtime.apply,
		source: AppliedBatchSource{
			Group:                runtime.identity.Group,
			AllocationGeneration: runtime.identity.AllocationGeneration,
			MemberID:             runtime.identity.MemberID, StoreID: runtime.identity.StoreID,
			NodeIncarnation: runtime.identity.NodeIncarnation,
			ReadyID:         normal.ReadyID(), FirstIndex: normal.FirstIndex(), LastIndex: normal.LastIndex(),
			FinalDataChainDigest: normal.FinalPublication().DataChainDigest,
		},
	}, true
}

// HasPendingResultSettlement reports whether Close and all later protocol work
// are blocked on one live, retryable, already-published applied range. A
// terminal Runtime failure may still retain an in-memory pending range, but it
// cannot be retried and does not prevent resource teardown.
func (runtime *Runtime) HasPendingResultSettlement() bool {
	if runtime == nil || runtime.failure != nil {
		return false
	}
	_, pending := runtime.pendingAppliedResults()
	return pending
}

func deterministicPersistFailure(err error) bool {
	return errors.Is(err, raftstore.ErrUnsupportedSnapshot) ||
		errors.Is(err, raftstore.ErrRetryConflict) ||
		errors.Is(err, raftstore.ErrInvalid) ||
		errors.Is(err, raftstore.ErrBounds) ||
		errors.Is(err, raftstore.ErrFull) ||
		errors.Is(err, raftstore.ErrClosed) ||
		errors.Is(err, raftstore.ErrCorrupt) ||
		errors.Is(err, raftstore.ErrIdentityMismatch) ||
		errors.Is(err, raftstore.ErrKeyMismatch) ||
		errors.Is(err, raftstore.ErrNamespaceChanged) ||
		errors.Is(err, raftstore.ErrPlatformUnsupported)
}

// CloneOrdinaryMessage validates a bounded flat ordinary-message graph and
// returns an owned clone plus its encoded byte size. Structural rejection runs
// before proto.Size or proto.Clone, so recursive Responses graphs and aliased
// oversized entry payloads cannot amplify work at this boundary.
func CloneOrdinaryMessage(message *pb.Message) (*pb.Message, int, error) {
	size, err := MeasureOrdinaryMessage(message)
	if err != nil {
		return nil, 0, err
	}
	return proto.Clone(message).(*pb.Message), size, nil
}

// MeasureOrdinaryMessage validates the same bounded flat graph accepted by
// CloneOrdinaryMessage without retaining or cloning it. A caller may use the
// result for admission before allocating an owned copy.
func MeasureOrdinaryMessage(message *pb.Message) (int, error) {
	return validateOrdinaryMessage(message)
}

func validateOrdinaryMessage(message *pb.Message) (int, error) {
	if message == nil {
		return 0, errors.New("raftmember: nil Raft message")
	}
	if message.GetType() == pb.MsgSnap || message.Snapshot != nil {
		return 0, &raftmodel.UnsupportedError{Feature: "snapshot message in the immutable-base WAL runtime"}
	}
	if len(message.GetResponses()) != 0 || message.Vote != nil {
		return 0, errors.New("raftmember: ordinary message carries recursive or local-storage fields")
	}
	if len(message.ProtoReflect().GetUnknown()) != 0 {
		return 0, errors.New("raftmember: ordinary message has unknown protobuf fields")
	}
	if message.GetFrom() == raft.None || raft.IsLocalMsgTarget(message.GetFrom()) ||
		message.GetTo() == raft.None || raft.IsLocalMsgTarget(message.GetTo()) ||
		message.GetFrom() == message.GetTo() {
		return 0, errors.New("raftmember: ordinary message has invalid peer identity")
	}
	switch message.GetType() {
	case pb.MsgApp, pb.MsgAppResp, pb.MsgVote, pb.MsgVoteResp,
		pb.MsgHeartbeat, pb.MsgHeartbeatResp, pb.MsgPreVote, pb.MsgPreVoteResp,
		pb.MsgTimeoutNow:
	default:
		return 0, &raftmodel.UnsupportedError{Feature: "ordinary message type " + message.GetType().String()}
	}
	if message.GetTerm() == 0 || message.GetTerm() == math.MaxUint64 ||
		message.GetIndex() == math.MaxUint64 || message.GetLogTerm() == math.MaxUint64 ||
		message.GetCommit() == math.MaxUint64 {
		return 0, errors.New("raftmember: ordinary message has invalid Raft term or terminal index")
	}
	if message.GetType() == pb.MsgTimeoutNow &&
		(message.Type == nil || message.From == nil || message.To == nil || message.Term == nil ||
			message.LogTerm != nil || message.Index != nil || len(message.Entries) != 0 ||
			message.Commit != nil || message.Snapshot != nil || message.Reject != nil ||
			message.RejectHint != nil || len(message.Context) != 0 || message.Vote != nil ||
			len(message.Responses) != 0) {
		return 0, errors.New("raftmember: malformed leader-transfer message")
	}
	if len(message.GetEntries()) > raftmodel.MaxMessageEntries {
		return 0, fmt.Errorf("%w: too many Raft entries", raftmodel.ErrAdmissionBound)
	}
	if len(message.GetEntries()) != 0 && message.GetType() != pb.MsgApp {
		return 0, errors.New("raftmember: non-append ordinary message carries entries")
	}
	contextBytes := len(message.GetContext())
	switch message.GetType() {
	case pb.MsgHeartbeat, pb.MsgHeartbeatResp, pb.MsgVote, pb.MsgPreVote:
		if contextBytes > raftmodel.MaxReadContextBytes {
			return 0, fmt.Errorf("%w: Raft context exceeds bound", raftmodel.ErrAdmissionBound)
		}
	default:
		if contextBytes != 0 {
			return 0, errors.New("raftmember: ordinary message has unexpected context")
		}
	}
	payloadBytes := int64(0)
	previousIndex := message.GetIndex()
	previousTerm := message.GetLogTerm()
	for _, entry := range message.GetEntries() {
		if entry == nil || len(entry.ProtoReflect().GetUnknown()) != 0 {
			return 0, errors.New("raftmember: ordinary message has nil or unknown-field entry")
		}
		if entry.GetType() < pb.EntryNormal || entry.GetType() > pb.EntryConfChangeV2 ||
			entry.GetIndex() == 0 || entry.GetIndex() == math.MaxUint64 ||
			entry.GetTerm() == 0 || entry.GetTerm() == math.MaxUint64 ||
			previousIndex == math.MaxUint64 || entry.GetIndex() != previousIndex+1 ||
			entry.GetTerm() < previousTerm || entry.GetTerm() > message.GetTerm() ||
			len(entry.GetData()) > raftmodel.MaxProposalBytes {
			return 0, errors.New("raftmember: ordinary message has malformed entry")
		}
		dataBytes := int64(len(entry.GetData()))
		if dataBytes > raftmodel.MaxPendingInputBytes-payloadBytes {
			return 0, fmt.Errorf("%w: aggregate Raft payload exceeds bound", raftmodel.ErrAdmissionBound)
		}
		payloadBytes += dataBytes
		previousIndex = entry.GetIndex()
		previousTerm = entry.GetTerm()
	}
	size := proto.Size(message)
	if size < 0 || size > raftmodel.MaxInboundMessageBytes || size == math.MaxInt {
		return 0, fmt.Errorf("%w: encoded Raft message exceeds bound", raftmodel.ErrAdmissionBound)
	}
	return size, nil
}

// Close monotonically retires the Runtime in apply, database, WAL order. A
// failed stage remains owned and is retried by a later Close call.
func (runtime *Runtime) Close() error {
	if runtime == nil || runtime.closed {
		return nil
	}
	if _, pending := runtime.pendingAppliedResults(); pending && runtime.failure == nil {
		return ErrResultSettlementPending
	}
	runtime.stopping = true
	if runtime.walGeneration != nil {
		runtime.walGeneration.stopAndWait()
		clear(runtime.walGeneration.key.Material[:])
		clear(runtime.walGeneration.key.Wrapped)
		runtime.walGeneration = nil
	}
	if runtime.schemaWALResume != nil {
		clear(runtime.schemaWALResume.Key.Material[:])
		clear(runtime.schemaWALResume.Key.Wrapped)
		runtime.schemaWALResume = nil
	}
	runtime.node = nil
	if runtime.apply != nil {
		if err := runtime.apply.Close(); err != nil {
			return err
		}
		runtime.apply = nil
	}
	if runtime.database != nil {
		if err := runtime.database.Close(); err != nil {
			return err
		}
		runtime.database = nil
	}
	if runtime.wal != nil {
		if err := runtime.wal.Close(); err != nil {
			return err
		}
		runtime.wal = nil
	}
	runtime.closed = true
	return nil
}
