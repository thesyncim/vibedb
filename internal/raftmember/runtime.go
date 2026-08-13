package raftmember

import (
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
}

// OutboundMessage is borrowed only for the duration of the DriveReady callback.
// A caller retaining it must clone Message before returning.
type OutboundMessage struct {
	Group   GroupKey
	From    uint64
	To      uint64
	Message *pb.Message
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
	DriveEntriesFinished
	DriveReadState
	DriveReadStatesFinished
	DriveAdvanced
)

// DriveResult reports one bounded synchronous Ready micro-step.
type DriveResult struct {
	Kind    DriveKind
	ReadyID uint64
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
	wal      *raftstore.Store
	database *sqldriver.Database
	apply    *sqldriver.ReplicatedApply
	node     *raftmodel.Node
	identity RuntimeIdentity
	failure  error
	stopping bool
	closed   bool
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
	if wal == nil || database == nil || apply == nil {
		return nil, ErrRuntimeOwnership
	}
	if err := ValidateStaticNoGCCompletionCapacity(wal, apply); err != nil {
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
		},
	}
	incarnation, err := wal.BeginIncarnation()
	if err != nil {
		return runtime.abortAdoption(fmt.Errorf("raftmember: begin incarnation: %w", err))
	}
	runtime.identity.NodeIncarnation = incarnation
	runtime.node, err = raftmodel.NewNode(sealed.MemberID, incarnation, wal, apply)
	if err != nil {
		return runtime.abortAdoption(fmt.Errorf("raftmember: construct node: %w", err))
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

func (runtime *Runtime) fail(cause error) error {
	if cause == nil {
		cause = errors.New("unknown terminal failure")
	}
	if runtime.failure == nil {
		runtime.failure = errors.Join(ErrRuntimeFailed, cause)
	}
	return runtime.failure
}

func (runtime *Runtime) requireEmptyInputWindow() error {
	if err := runtime.checkUsable(); err != nil {
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

// Propose admits one already-encoded replicated command. Nil means only that
// the local core accepted the proposal; it grants no serving or result claim.
func (runtime *Runtime) Propose(data []byte) error {
	if err := runtime.requireEmptyInputWindow(); err != nil {
		return err
	}
	if err := runtime.apply.AdmitCommand(data); err != nil {
		if errors.Is(err, replicatedstate.ErrApplyPoisoned) ||
			errors.Is(err, sqldriver.ErrReplicatedApplyClosed) {
			return runtime.fail(err)
		}
		return err
	}
	if err := runtime.wal.ReserveReady(); err != nil {
		if deterministicPersistFailure(err) {
			return runtime.fail(err)
		}
		return err
	}
	return runtime.node.Propose(data)
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
func (runtime *Runtime) DriveReady(
	send func(OutboundMessage) error,
) (DriveResult, error) {
	if err := runtime.checkUsable(); err != nil {
		return DriveResult{}, err
	}
	switch runtime.node.Phase() {
	case raftmodel.PhaseIdle:
		captured, err := runtime.node.CaptureReady()
		if err != nil || !captured {
			return DriveResult{}, err
		}
		progress, ok := runtime.node.CurrentReady()
		if !ok {
			return DriveResult{}, runtime.fail(errors.New("captured Ready has no progress record"))
		}
		if progress.HasSnapshot {
			return DriveResult{}, runtime.fail(&raftmodel.UnsupportedError{
				Feature: "runtime snapshots in the static WAL kernel",
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
		_, applied, err := runtime.node.ApplyNext()
		if err != nil {
			return DriveResult{}, runtime.fail(err)
		}
		kind := DriveEntriesFinished
		if applied {
			kind = DriveEntry
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
		if len(outcomes) != 0 {
			return DriveResult{}, runtime.fail(&raftmodel.UnsupportedError{
				Feature: "read outcomes in the non-serving runtime",
			})
		}
		return DriveResult{Kind: DriveReadStatesFinished, ReadyID: progress.ReadyID}, nil

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
		return 0, &raftmodel.UnsupportedError{Feature: "snapshot message in the static WAL runtime"}
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
		pb.MsgHeartbeat, pb.MsgHeartbeatResp, pb.MsgPreVote, pb.MsgPreVoteResp:
	default:
		return 0, &raftmodel.UnsupportedError{Feature: "ordinary message type " + message.GetType().String()}
	}
	if message.GetTerm() == 0 || message.GetTerm() == math.MaxUint64 ||
		message.GetIndex() == math.MaxUint64 || message.GetLogTerm() == math.MaxUint64 ||
		message.GetCommit() == math.MaxUint64 {
		return 0, errors.New("raftmember: ordinary message has invalid Raft term or terminal index")
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
	runtime.stopping = true
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
