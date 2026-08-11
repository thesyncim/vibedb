package raftmodel

import (
	"errors"
	"fmt"
	"math"
	"slices"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/confchange"
	pb "go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"
)

// Node owns one synchronous RawNode integration. Node and every value borrowed
// from it are deliberately not safe for concurrent use: one scheduler owner
// must perform all protocol input and Ready lifecycle calls in program order.
type Node struct {
	id          uint64
	incarnation uint64
	raw         *raft.RawNode
	stable      StableStore
	machine     StateMachine

	phase      Phase
	ready      raft.Ready
	readyID    uint64
	readySeq   uint64
	messagePos int
	entryPos   int
	readPos    int
	failure    error
	published  Publication

	issuedReads  map[string]readIssue
	pendingReads []ReadBarrier
	readBytes    int
	readSeq      uint64

	readyFromInput    bool
	pendingInputCalls int
	pendingInputUnits int
	pendingInputBytes int64
}

// ReadyProgress is an allocation-free observation of one captured Ready. It
// exposes only bounded counts needed to schedule explicit crash cuts; no core
// protobuf storage is borrowed.
type ReadyProgress struct {
	ReadyID            uint64
	MessageCount       int
	MessagesSent       int
	CommittedCount     int
	CommittedApplied   int
	ReadStateCount     int
	ReadStatesRecorded int
	HasSnapshot        bool
}

// NewNode recovers a RawNode at the state machine's atomically published cut.
// If StableStore has already advanced its snapshot past that cut, NewNode first
// asks StateMachine to install and atomically publish the exact durable
// snapshot. That closes the crash interval between Ready persistence and
// InstallSnapshot without permitting RawNode to start from mismatched durable
// and applied bases. Incarnation is the non-zero, durable, strictly increasing
// member boot counter allocated by StableStore; it fences ReadIndex results and
// Ready retries across restarts.
func NewNode(id, incarnation uint64, stable StableStore, machine StateMachine) (*Node, error) {
	if id == raft.None {
		return nil, errors.New("raftmodel: member ID must be non-zero")
	}
	if incarnation == 0 {
		return nil, errors.New("raftmodel: incarnation must be non-zero")
	}
	if stable == nil {
		return nil, errors.New("raftmodel: stable store is nil")
	}
	if machine == nil {
		return nil, errors.New("raftmodel: state machine is nil")
	}

	pub := clonePublication(machine.Published())
	if applied := machine.Applied(); applied != pub.Applied {
		return nil, fmt.Errorf("raftmodel: state machine applied %d differs from published %d", applied, pub.Applied)
	}
	if pub.ConfState == nil {
		return nil, errors.New("raftmodel: published ConfState is nil")
	}
	if pub.ConfState.GetAutoLeave() {
		return nil, &UnsupportedError{Feature: "recovery from automatic joint consensus"}
	}
	if pub.ReplicaSetVersion == 0 && confStateHasMembers(pub.ConfState) {
		return nil, errors.New("raftmodel: nonempty published ConfState has zero ReplicaSetVersion")
	}
	if pub.ReplicaSetVersion > pub.Applied {
		return nil, fmt.Errorf("raftmodel: ReplicaSetVersion %d exceeds published index %d", pub.ReplicaSetVersion, pub.Applied)
	}

	hs, _, err := stable.InitialState()
	if err != nil {
		return nil, fmt.Errorf("raftmodel: read initial state: %w", err)
	}
	first, err := stable.FirstIndex()
	if err != nil {
		return nil, fmt.Errorf("raftmodel: read first index: %w", err)
	}
	if first == 0 {
		return nil, errors.New("raftmodel: stable store returned zero first index")
	}
	base := first - 1
	last, err := stable.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("raftmodel: read last index: %w", err)
	}
	if last < base || last == math.MaxUint64 {
		return nil, fmt.Errorf("raftmodel: durable log range [%d,%d] is invalid", base, last)
	}
	durableSnapshot, err := stable.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("raftmodel: read durable snapshot: %w", err)
	}
	if err := validateSnapshotEnvelope(durableSnapshot); err != nil {
		return nil, fmt.Errorf("raftmodel: durable snapshot: %w", err)
	}
	metadata := durableSnapshot.GetMetadata()
	if metadata.GetIndex() != base {
		return nil, fmt.Errorf(
			"raftmodel: durable snapshot index %d differs from log base %d",
			metadata.GetIndex(), base,
		)
	}
	baseTerm, err := stable.Term(base)
	if err != nil || (base != 0 && baseTerm == 0) {
		return nil, fmt.Errorf("raftmodel: read durable base term at %d: %v", base, err)
	}
	if metadata.GetTerm() != baseTerm {
		return nil, fmt.Errorf(
			"raftmodel: durable snapshot term %d differs from log base term %d",
			metadata.GetTerm(), baseTerm,
		)
	}
	lastTerm, err := stable.Term(last)
	if err != nil {
		return nil, fmt.Errorf("raftmodel: read durable last term at %d: %w", last, err)
	}
	committed := base
	if !raft.IsEmptyHardState(hs) {
		if hs.GetCommit() < base || hs.GetCommit() > last || hs.GetTerm() < lastTerm ||
			(hs.GetTerm() == 0 && hs.GetVote() != 0) {
			return nil, fmt.Errorf("raftmodel: durable commit %d outside log range [%d,%d]", hs.GetCommit(), base, last)
		}
		committed = hs.GetCommit()
	}
	if pub.Applied <= base {
		reconciled := &Node{machine: machine, published: pub}
		installed, installErr := machine.InstallSnapshot(durableSnapshot)
		if installErr != nil {
			return nil, fmt.Errorf(
				"raftmodel: reconcile durable snapshot at %d: %w",
				base, installErr,
			)
		}
		if acceptErr := reconciled.acceptSnapshotPublication(
			base, metadata.GetConfState(), installed,
		); acceptErr != nil {
			return nil, fmt.Errorf(
				"raftmodel: reconcile durable snapshot at %d: %w",
				base, acceptErr,
			)
		}
		pub = reconciled.published
	}
	if err := ValidateConfState(pub.ConfState, last); err != nil {
		return nil, fmt.Errorf("raftmodel: published ConfState: %w", err)
	}
	if pub.Applied < base || pub.Applied > committed {
		return nil, fmt.Errorf("raftmodel: published index %d outside durable committed range [%d,%d]", pub.Applied, base, committed)
	}
	if pub.Applied == base {
		if equivalentErr := pub.ConfState.Equivalent(metadata.GetConfState()); equivalentErr != nil {
			return nil, fmt.Errorf(
				"raftmodel: publication at durable snapshot cut differs from snapshot ConfState: %w",
				equivalentErr,
			)
		}
	}

	// ConfState becomes active at ordered application, not at Ready persistence.
	// It therefore belongs to the durable state-machine publication. Overlay it
	// on the log store's HardState for RawNode recovery; an older snapshot's
	// InitialState ConfState is not authoritative after later config entries.
	recovery := recoveryStorage{StableStore: stable, confState: cloneConfState(pub.ConfState)}
	cfg := NewConfig(id, recovery, pub.Applied)
	raw, err := newRawNodeChecked(&cfg)
	if err != nil {
		return nil, fmt.Errorf("raftmodel: construct RawNode: %w", err)
	}
	n := &Node{
		id:          id,
		incarnation: incarnation,
		raw:         raw,
		stable:      stable,
		machine:     machine,
		phase:       PhaseIdle,
		published:   pub,
		issuedReads: make(map[string]readIssue),
	}
	return n, nil
}

func newRawNodeChecked(config *raft.Config) (raw *raft.RawNode, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			raw = nil
			err = fmt.Errorf("raftmodel: construct RawNode panic: %v", recovered)
		}
	}()
	return raft.NewRawNode(config)
}

// Phase returns the current synchronous Ready lifecycle phase.
func (n *Node) Phase() Phase { return n.phase }

// Failure returns the terminal apply failure, if any.
func (n *Node) Failure() error { return n.failure }

// ReadyID identifies the currently captured Ready, or zero while idle.
func (n *Node) ReadyID() uint64 { return n.readyID }

// CurrentReady reports progress through the captured Ready. The boolean is
// false while idle and after a terminal failure has invalidated the lifecycle.
func (n *Node) CurrentReady() (ReadyProgress, bool) {
	if n == nil || n.readyID == 0 || n.phase == PhaseIdle || n.phase == PhaseFailed {
		return ReadyProgress{}, false
	}
	return ReadyProgress{
		ReadyID:      n.readyID,
		MessageCount: len(n.ready.Messages), MessagesSent: n.messagePos,
		CommittedCount: len(n.ready.CommittedEntries), CommittedApplied: n.entryPos,
		ReadStateCount: len(n.ready.ReadStates), ReadStatesRecorded: n.readPos,
		HasSnapshot: !raft.IsEmptySnap(n.ready.Snapshot),
	}, true
}

// Published returns a defensive copy of the last publication accepted by the
// driver.
func (n *Node) Published() Publication { return clonePublication(n.published) }

// Status returns the allocation-free core status. It remains subject to the
// Node's single-owner contract.
func (n *Node) Status() raft.BasicStatus { return n.raw.BasicStatus() }

// HasReady reports whether CaptureReady would capture work.
func (n *Node) HasReady() (bool, error) {
	if err := n.requirePhase("HasReady", PhaseIdle); err != nil {
		return false, err
	}
	return n.raw.HasReady(), nil
}

// CaptureReady accepts exactly one Ready from RawNode. All protocol input is
// blocked until that Ready is advanced.
func (n *Node) CaptureReady() (bool, error) {
	if err := n.requirePhase("CaptureReady", PhaseIdle); err != nil {
		return false, err
	}
	if !n.raw.HasReady() {
		return false, nil
	}
	if n.readySeq == math.MaxUint64 {
		return false, errors.New("raftmodel: Ready ID exhausted")
	}
	n.readySeq++
	n.readyID = n.readySeq
	n.ready = n.raw.Ready()
	n.messagePos = 0
	n.entryPos = 0
	n.readPos = 0
	n.readyFromInput = false
	n.pendingInputCalls = 0
	n.pendingInputUnits = 0
	n.pendingInputBytes = 0
	n.phase = PhaseCaptured
	return true, nil
}

// PersistReady crosses the stable-storage boundary. On error the Node remains
// captured and a caller may retry the same ReadyID.
func (n *Node) PersistReady() error {
	if err := n.requirePhase("PersistReady", PhaseCaptured); err != nil {
		return err
	}
	err := n.stable.Persist(PersistBatch{
		NodeIncarnation: n.incarnation,
		ReadyID:         n.readyID,
		HardState:       n.ready.HardState,
		Entries:         n.ready.Entries,
		Snapshot:        n.ready.Snapshot,
		MustSync:        n.ready.MustSync,
	})
	if err != nil {
		return &PersistError{ReadyID: n.readyID, Err: err}
	}
	n.phase = PhasePersisted
	return nil
}

// SendNextMessage synchronously hands at most one outbound message to send
// after the persistence boundary. The message is borrowed: send must consume
// or copy it before returning. A callback error does not advance messagePos, so
// retrying is safe at the Raft protocol layer but may duplicate a delivery if
// the callback sent externally before reporting its error.
func (n *Node) SendNextMessage(send func(*pb.Message) error) (bool, error) {
	if err := n.requirePhase("SendNextMessage", PhasePersisted); err != nil {
		return false, err
	}
	if err := n.validateOutboundMessages(); err != nil {
		return false, err
	}
	if n.messagePos == len(n.ready.Messages) {
		return false, nil
	}
	if send == nil {
		return false, errors.New("raftmodel: nil message sink")
	}
	message := n.ready.Messages[n.messagePos]
	if err := send(message); err != nil {
		return false, fmt.Errorf("raftmodel: send Ready %d message %d: %w", n.readyID, n.messagePos, err)
	}
	n.messagePos++
	return true, nil
}

// FinishMessages closes the outbound-message stage after every message has
// been consumed. It is a separate operation so an empty batch and the cut after
// the final send remain explicit in deterministic traces.
func (n *Node) FinishMessages() error {
	if err := n.requirePhase("FinishMessages", PhasePersisted); err != nil {
		return err
	}
	if err := n.validateOutboundMessages(); err != nil {
		return err
	}
	if n.messagePos != len(n.ready.Messages) {
		return fmt.Errorf("raftmodel: %d Ready messages remain unsent", len(n.ready.Messages)-n.messagePos)
	}
	n.phase = PhaseMessagesDrained
	return nil
}

// DrainMessages is a convenience wrapper over SendNextMessage and
// FinishMessages. Simulators should call the micro-step methods directly.
func (n *Node) DrainMessages(send func(*pb.Message) error) error {
	for {
		sent, err := n.SendNextMessage(send)
		if err != nil {
			return err
		}
		if !sent {
			return n.FinishMessages()
		}
	}
}

func (n *Node) validateOutboundMessages() error {
	for _, message := range n.ready.Messages {
		if message.GetType() == pb.MsgSnap {
			return &UnsupportedError{Feature: "snapshot transfer and ReportSnapshot lifecycle"}
		}
	}
	return nil
}

// InstallSnapshot installs the Ready snapshot, if present, before any suffix
// entries. Snapshot support is intentionally only an integration port in this
// phase; creation, verification, and crash-atomic storage arrive with the WAL.
func (n *Node) InstallSnapshot() error {
	if err := n.requirePhase("InstallSnapshot", PhaseMessagesDrained); err != nil {
		return err
	}
	if raft.IsEmptySnap(n.ready.Snapshot) {
		n.phase = PhaseSnapshotInstalled
		return nil
	}
	snapshot := n.ready.Snapshot
	index := snapshot.GetMetadata().GetIndex()
	if err := n.validateSnapshot(snapshot); err != nil {
		return n.fail(PhaseSnapshotInstalled, index, err)
	}
	pub, err := n.machine.InstallSnapshot(snapshot)
	if err != nil {
		return n.fail(PhaseSnapshotInstalled, index, err)
	}
	if err := n.acceptSnapshotPublication(index, snapshot.GetMetadata().GetConfState(), pub); err != nil {
		return n.fail(PhaseSnapshotInstalled, index, err)
	}
	n.phase = PhaseSnapshotInstalled
	return nil
}

// ApplyNext applies at most one committed entry in exact index order, exposing
// a deterministic crash cut between entries. The boolean reports whether an
// entry was applied. Normal no-op entries still advance and publish Applied.
func (n *Node) ApplyNext() (Publication, bool, error) {
	if err := n.requirePhase("ApplyNext", PhaseSnapshotInstalled); err != nil {
		return Publication{}, false, err
	}
	if n.entryPos == len(n.ready.CommittedEntries) {
		n.phase = PhaseEntriesApplied
		return n.Published(), false, nil
	}
	entry := n.ready.CommittedEntries[n.entryPos]
	if err := n.applyEntry(entry); err != nil {
		return Publication{}, false, err
	}
	n.entryPos++
	if n.entryPos == len(n.ready.CommittedEntries) {
		n.phase = PhaseEntriesApplied
	}
	return n.Published(), true, nil
}

// ApplyCommitted is a convenience wrapper over ApplyNext. Simulators should
// call ApplyNext directly when they need every entry-level crash boundary.
func (n *Node) ApplyCommitted() error {
	for n.phase == PhaseSnapshotInstalled {
		if _, _, err := n.ApplyNext(); err != nil {
			return err
		}
	}
	if n.phase != PhaseEntriesApplied {
		return n.requirePhase("ApplyCommitted", PhaseSnapshotInstalled)
	}
	return nil
}

// RecordNextReadState records at most one quorum-confirmed ReadIndex barrier,
// exposing a deterministic crash cut between returned read states.
func (n *Node) RecordNextReadState() (bool, error) {
	if err := n.requirePhase("RecordReadStates", PhaseEntriesApplied); err != nil {
		return false, err
	}
	if n.readPos == len(n.ready.ReadStates) {
		return false, nil
	}
	state := n.ready.ReadStates[n.readPos]
	if state.Index == 0 {
		return false, n.fail(PhaseReadStatesRecorded, 0, errors.New("core returned zero ReadIndex barrier"))
	}
	key := string(state.RequestCtx)
	issue, ok := n.issuedReads[key]
	if !ok {
		return false, n.fail(PhaseReadStatesRecorded, state.Index, errors.New("unknown or duplicate ReadIndex context returned by core"))
	}
	delete(n.issuedReads, key)
	n.pendingReads = append(n.pendingReads, ReadBarrier{
		Context:     append([]byte(nil), state.RequestCtx...),
		Index:       state.Index,
		Term:        issue.term,
		Incarnation: issue.incarnation,
	})
	n.readPos++
	return true, nil
}

// FinishReadStates releases eligible barriers and cancels stale-leadership
// requests after every Ready ReadState has been recorded.
func (n *Node) FinishReadStates() ([]ReadOutcome, error) {
	if err := n.requirePhase("FinishReadStates", PhaseEntriesApplied); err != nil {
		return nil, err
	}
	if n.readPos != len(n.ready.ReadStates) {
		return nil, fmt.Errorf("raftmodel: %d Ready ReadStates remain unrecorded", len(n.ready.ReadStates)-n.readPos)
	}
	outcomes := n.cancelStaleIssuedReads()
	outcomes = append(outcomes, n.releaseReads()...)
	n.phase = PhaseReadStatesRecorded
	return outcomes, nil
}

// RecordReadStates is a convenience wrapper over RecordNextReadState and
// FinishReadStates.
func (n *Node) RecordReadStates() ([]ReadOutcome, error) {
	for {
		recorded, err := n.RecordNextReadState()
		if err != nil {
			return nil, err
		}
		if !recorded {
			return n.FinishReadStates()
		}
	}
}

// AdvanceReady acknowledges the fully persisted, sent, applied, and published
// Ready to the core and reopens protocol input.
func (n *Node) AdvanceReady() error {
	if err := n.requirePhase("AdvanceReady", PhaseReadStatesRecorded); err != nil {
		return err
	}
	n.raw.Advance(n.ready)
	n.ready = raft.Ready{}
	n.readyID = 0
	n.messagePos = 0
	n.entryPos = 0
	n.readPos = 0
	n.readyFromInput = false
	n.pendingInputCalls = 0
	n.pendingInputUnits = 0
	n.pendingInputBytes = 0
	n.phase = PhaseIdle
	return nil
}

// Step applies one received Raft message within the bounded uncaptured-Ready
// input window.
func (n *Node) Step(message *pb.Message) error {
	units := 1
	if message != nil && len(message.GetEntries()) > units {
		units = len(message.GetEntries())
	}
	inputBytes := inboundReadyBytes(message)
	if err := n.admitProtocolInput("Step", units, inputBytes); err != nil {
		return err
	}
	if message == nil {
		return errors.New("raftmodel: nil Raft message")
	}
	if err := n.validateInboundMessage(message); err != nil {
		return err
	}
	// RawNode may retain entry, snapshot, or ReadIndex protobuf backing beyond
	// Step. The transport owns its message and is free to recycle it as soon as
	// this call returns, so the integration boundary must detach the full graph.
	err := n.raw.Step(proto.Clone(message).(*pb.Message))
	if err == nil {
		n.recordProtocolInput(units, inputBytes)
	}
	return err
}

func (n *Node) validateInboundMessage(message *pb.Message) error {
	if message.GetFrom() == raft.None || raft.IsLocalMsgTarget(message.GetFrom()) ||
		message.GetFrom() == n.id || message.GetTo() != n.id {
		return errors.New("raftmodel: invalid remote message identity")
	}
	if len(message.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("raftmodel: remote message has unknown protobuf fields")
	}
	switch message.GetType() {
	case pb.MsgApp, pb.MsgAppResp, pb.MsgVote, pb.MsgVoteResp, pb.MsgSnap,
		pb.MsgHeartbeat, pb.MsgHeartbeatResp, pb.MsgPreVote, pb.MsgPreVoteResp:
	default:
		return &UnsupportedError{Feature: "remote message type " + message.GetType().String()}
	}
	if message.GetTerm() == 0 || message.GetTerm() == math.MaxUint64 || message.GetIndex() == math.MaxUint64 ||
		message.GetLogTerm() == math.MaxUint64 || message.GetCommit() == math.MaxUint64 {
		return errors.New("raftmodel: remote message has invalid Raft term or terminal index")
	}
	if len(message.GetResponses()) != 0 || message.Vote != nil {
		return errors.New("raftmodel: remote message carries local-storage fields")
	}
	contextBytes := len(message.GetContext())
	switch message.GetType() {
	case pb.MsgHeartbeat, pb.MsgHeartbeatResp, pb.MsgVote, pb.MsgPreVote:
		if contextBytes > MaxReadContextBytes {
			return fmt.Errorf("%w: inbound Raft context bytes %d exceed %d", ErrAdmissionBound, contextBytes, MaxReadContextBytes)
		}
	default:
		if contextBytes != 0 {
			return errors.New("raftmodel: remote message has unexpected context")
		}
	}
	if len(message.GetEntries()) != 0 && message.GetType() != pb.MsgApp {
		return errors.New("raftmodel: remote non-append message carries entries")
	}
	if message.GetType() == pb.MsgSnap {
		if err := validateSnapshotEnvelope(message.GetSnapshot()); err != nil {
			return fmt.Errorf("raftmodel: invalid inbound snapshot: %w", err)
		}
	}
	if len(message.GetEntries()) > MaxMessageEntries || proto.Size(message) > MaxInboundMessageBytes {
		return fmt.Errorf("%w: inbound Raft message exceeds bound", ErrAdmissionBound)
	}
	previous := message.GetIndex()
	previousTerm := message.GetLogTerm()
	for _, entry := range message.GetEntries() {
		if entry == nil || entry.GetIndex() == 0 || entry.GetIndex() == math.MaxUint64 ||
			entry.GetTerm() == 0 || entry.GetTerm() == math.MaxUint64 ||
			len(entry.ProtoReflect().GetUnknown()) != 0 ||
			entry.GetType() < pb.EntryNormal || entry.GetType() > pb.EntryConfChangeV2 ||
			len(entry.GetData()) > MaxProposalBytes ||
			previous == math.MaxUint64 || entry.GetIndex() != previous+1 ||
			entry.GetTerm() < previousTerm || entry.GetTerm() > message.GetTerm() {
			return errors.New("raftmodel: malformed inbound Raft entries")
		}
		previous = entry.GetIndex()
		previousTerm = entry.GetTerm()
	}
	return nil
}

// Tick advances the logical election clock within the bounded uncaptured-Ready
// input window.
func (n *Node) Tick() error {
	if err := n.admitProtocolInput("Tick", 1, 0); err != nil {
		return err
	}
	n.raw.Tick()
	n.recordProtocolInput(1, 0)
	return nil
}

// Campaign starts an election within the bounded uncaptured-Ready input window.
func (n *Node) Campaign() error {
	if err := n.admitProtocolInput("Campaign", 1, 0); err != nil {
		return err
	}
	err := n.raw.Campaign()
	if err == nil {
		n.recordProtocolInput(1, 0)
	}
	return err
}

// Propose submits a normal entry. The payload is copied before the core can
// retain it.
func (n *Node) Propose(data []byte) error {
	if err := n.admitProtocolInput("Propose", 1, int64(len(data))); err != nil {
		return err
	}
	if err := admitProposalBytes(len(data)); err != nil {
		return err
	}
	err := n.raw.Propose(append([]byte(nil), data...))
	if err == nil {
		n.recordProtocolInput(1, int64(len(data)))
	}
	return err
}

// ProposeConfChange submits one of the two supported Raft configuration entry
// forms. Automatic/implicit joint exit is rejected so every joint transition
// remains an explicit, model-visible command. This protocol adapter is not a
// membership authority: the topology layer must durably reject member-ID reuse
// and authorize every proposed identity before calling this method.
func (n *Node) ProposeConfChange(change pb.ConfChangeI) error {
	if err := n.requirePhase("ProposeConfChange", PhaseIdle); err != nil {
		return err
	}
	if err := n.validateConfChange(change); err != nil {
		return err
	}
	// Upstream silently rewrites a second/premature configuration proposal to
	// EntryNormal. Refuse unless the local Ready stream and durable log are
	// exactly caught up to the published predecessor, so nil always means that
	// the proposal was admitted as a configuration entry.
	if n.raw.HasReady() {
		return errors.Join(ErrReadyPending, ErrConfChangePending)
	}
	lastIndex, err := n.stable.LastIndex()
	if err != nil {
		return fmt.Errorf("raftmodel: read last index before configuration proposal: %w", err)
	}
	if lastIndex != n.published.Applied || n.raw.BasicStatus().Applied != n.published.Applied {
		return ErrConfChangePending
	}
	if _, err := n.preflightConfChange(change, n.published.Applied); err != nil {
		return err
	}
	_, encoded, err := pb.MarshalConfChange(change)
	if err != nil {
		return fmt.Errorf("raftmodel: encode configuration proposal: %w", err)
	}
	if err := admitProposalBytes(len(encoded)); err != nil {
		return fmt.Errorf("raftmodel: configuration proposal: %w", err)
	}
	if err := n.admitProtocolInput("ProposeConfChange", 1, int64(len(encoded))); err != nil {
		return err
	}
	err = n.raw.ProposeConfChange(change)
	if err == nil {
		n.recordProtocolInput(1, int64(len(encoded)))
	}
	return err
}

func admitProposalBytes(size int) error {
	if size > MaxProposalBytes {
		return fmt.Errorf("%w: proposal bytes %d exceed %d", ErrAdmissionBound, size, MaxProposalBytes)
	}
	return nil
}

func (n *Node) applyEntry(entry *pb.Entry) error {
	if entry == nil {
		return n.fail(PhaseEntriesApplied, 0, errors.New("nil committed entry"))
	}
	if n.published.Applied == math.MaxUint64 {
		return n.fail(PhaseEntriesApplied, entry.GetIndex(), errors.New("applied index exhausted"))
	}
	expected := n.published.Applied + 1
	if entry.GetIndex() != expected {
		return n.fail(PhaseEntriesApplied, entry.GetIndex(), fmt.Errorf("non-contiguous committed entry: want %d", expected))
	}
	if entry.GetTerm() == 0 {
		return n.fail(PhaseEntriesApplied, entry.GetIndex(), errors.New("committed entry has zero term"))
	}
	meta := ApplyMeta{Index: entry.GetIndex(), Term: entry.GetTerm(), Type: entry.GetType()}
	switch entry.GetType() {
	case pb.EntryType_EntryNormal:
		pub, err := n.machine.ApplyNormal(meta, entry.GetData())
		if err != nil {
			return n.fail(PhaseEntriesApplied, meta.Index, err)
		}
		if err := n.acceptNormalPublication(meta, len(entry.GetData()) == 0, pub); err != nil {
			return n.fail(PhaseEntriesApplied, meta.Index, err)
		}
	case pb.EntryType_EntryConfChange, pb.EntryType_EntryConfChangeV2:
		change, err := decodeConfChange(entry)
		if err != nil {
			return n.fail(PhaseEntriesApplied, meta.Index, err)
		}
		if err := n.validateConfChange(change); err != nil {
			return n.fail(PhaseEntriesApplied, meta.Index, err)
		}
		predicted, err := n.preflightConfChange(change, meta.Index)
		if err != nil {
			return n.fail(PhaseEntriesApplied, meta.Index, err)
		}
		confState, err := applyConfChangeChecked(n.raw, change)
		if err != nil {
			return n.fail(PhaseEntriesApplied, meta.Index, err)
		}
		if confState == nil {
			return n.fail(PhaseEntriesApplied, meta.Index, errors.New("core returned nil ConfState"))
		}
		if err := predicted.Equivalent(confState); err != nil {
			return n.fail(PhaseEntriesApplied, meta.Index, fmt.Errorf("core ConfState differs from preflight: %w", err))
		}
		if err := ValidateConfState(confState, meta.Index); err != nil {
			return n.fail(PhaseEntriesApplied, meta.Index, fmt.Errorf("core ConfState is invalid: %w", err))
		}
		pub, err := n.machine.ApplyConfiguration(meta, confState)
		if err != nil {
			return n.fail(PhaseEntriesApplied, meta.Index, err)
		}
		if err := n.acceptConfigurationPublication(meta, confState, pub); err != nil {
			return n.fail(PhaseEntriesApplied, meta.Index, err)
		}
	default:
		return n.fail(PhaseEntriesApplied, meta.Index, fmt.Errorf("unknown committed entry type %d", entry.GetType()))
	}
	return nil
}

func decodeConfChange(entry *pb.Entry) (pb.ConfChangeI, error) {
	switch entry.GetType() {
	case pb.EntryType_EntryConfChange:
		change := new(pb.ConfChange)
		if err := proto.Unmarshal(entry.GetData(), change); err != nil {
			return nil, fmt.Errorf("decode ConfChange: %w", err)
		}
		return change, nil
	case pb.EntryType_EntryConfChangeV2:
		change := new(pb.ConfChangeV2)
		if err := proto.Unmarshal(entry.GetData(), change); err != nil {
			return nil, fmt.Errorf("decode ConfChangeV2: %w", err)
		}
		return change, nil
	default:
		return nil, fmt.Errorf("entry type %d is not a configuration change", entry.GetType())
	}
}

func (n *Node) validateConfChange(change pb.ConfChangeI) error {
	var v2 *pb.ConfChangeV2
	switch typed := change.(type) {
	case *pb.ConfChange:
		if typed == nil {
			return errors.New("raftmodel: nil ConfChange")
		}
		if len(typed.GetContext()) != 0 {
			return &UnsupportedError{Feature: "configuration-change context before topology apply binding"}
		}
		v2 = typed.AsV2()
	case *pb.ConfChangeV2:
		if typed == nil {
			return errors.New("raftmodel: nil ConfChangeV2")
		}
		if len(typed.GetContext()) != 0 {
			return &UnsupportedError{Feature: "configuration-change context before topology apply binding"}
		}
		v2 = typed
	default:
		return &UnsupportedError{Feature: "unknown configuration change representation"}
	}

	switch v2.GetTransition() {
	case pb.ConfChangeTransition_ConfChangeTransitionAuto:
		if len(v2.GetChanges()) > 1 {
			return &UnsupportedError{Feature: "automatic joint consensus exit"}
		}
	case pb.ConfChangeTransition_ConfChangeTransitionJointImplicit:
		return &UnsupportedError{Feature: "implicit joint consensus exit"}
	case pb.ConfChangeTransition_ConfChangeTransitionJointExplicit:
		if len(v2.GetChanges()) == 0 {
			return &UnsupportedError{Feature: "empty explicit joint transition"}
		}
	default:
		return &UnsupportedError{Feature: "unknown joint-consensus transition"}
	}
	if len(v2.GetChanges()) > MaxConfStateMembers {
		return fmt.Errorf("%w: configuration change count %d exceeds %d", ErrAdmissionBound, len(v2.GetChanges()), MaxConfStateMembers)
	}

	for _, single := range v2.GetChanges() {
		if single == nil || single.GetNodeId() == raft.None || raft.IsLocalMsgTarget(single.GetNodeId()) {
			return errors.New("raftmodel: configuration change has invalid member ID")
		}
		switch single.GetType() {
		case pb.ConfChangeType_ConfChangeAddNode,
			pb.ConfChangeType_ConfChangeAddLearnerNode,
			pb.ConfChangeType_ConfChangeRemoveNode:
		case pb.ConfChangeType_ConfChangeUpdateNode:
			return &UnsupportedError{Feature: "configuration metadata update before context-aware apply"}
		default:
			return &UnsupportedError{Feature: "unknown configuration change type"}
		}
	}
	return nil
}

// preflightConfChange runs the same upstream Changer transition against the
// last atomically published membership before a proposal enters the log and
// again before a committed entry mutates RawNode. RawNode.ApplyConfChange
// panics when Changer rejects a transition, so this is both an admission fence
// and a deterministic fail-stop boundary for malformed committed input.
func (n *Node) preflightConfChange(change pb.ConfChangeI, lastIndex uint64) (*pb.ConfState, error) {
	progress := tracker.MakeProgressTracker(MaxInflightMsgs, MaxInflightBytes)
	config, members, err := confchange.Restore(confchange.Changer{
		Tracker:   progress,
		LastIndex: lastIndex,
	}, n.published.ConfState)
	if err != nil {
		return nil, fmt.Errorf("raftmodel: restore published configuration: %w", err)
	}
	progress.Config = config
	progress.Progress = members
	changer := confchange.Changer{Tracker: progress, LastIndex: lastIndex}
	v2 := change.AsV2()
	var nextConfig tracker.Config
	var nextMembers tracker.ProgressMap
	if v2.LeaveJoint() {
		nextConfig, nextMembers, err = changer.LeaveJoint()
	} else if autoLeave, joint := v2.EnterJoint(); joint {
		nextConfig, nextMembers, err = changer.EnterJoint(autoLeave, v2.GetChanges()...)
	} else {
		nextConfig, nextMembers, err = changer.Simple(v2.GetChanges()...)
	}
	if err != nil {
		return nil, fmt.Errorf("raftmodel: configuration change invalid for published state: %w", err)
	}
	progress.Config = nextConfig
	progress.Progress = nextMembers
	predicted := progress.ConfState()
	if err := ValidateConfState(predicted, lastIndex); err != nil {
		return nil, fmt.Errorf("raftmodel: resulting configuration is invalid: %w", err)
	}
	return predicted, nil
}

func applyConfChangeChecked(raw *raft.RawNode, change pb.ConfChangeI) (state *pb.ConfState, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			state = nil
			err = fmt.Errorf("raftmodel: ApplyConfChange panic: %v", recovered)
		}
	}()
	return raw.ApplyConfChange(change), nil
}

func (n *Node) validateSnapshot(snapshot *pb.Snapshot) error {
	if err := validateSnapshotEnvelope(snapshot); err != nil {
		return err
	}
	metadata := snapshot.GetMetadata()
	if metadata.GetIndex() <= n.published.Applied {
		return fmt.Errorf("snapshot index %d does not advance published index %d", metadata.GetIndex(), n.published.Applied)
	}
	return nil
}

func validateSnapshotEnvelope(snapshot *pb.Snapshot) error {
	if snapshot == nil || snapshot.GetMetadata() == nil {
		return errors.New("snapshot or metadata is nil")
	}
	metadata := snapshot.GetMetadata()
	if len(snapshot.ProtoReflect().GetUnknown()) != 0 || len(metadata.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("snapshot has unknown protobuf fields")
	}
	if metadata.GetIndex() == 0 || metadata.GetIndex() == math.MaxUint64 {
		return fmt.Errorf("snapshot index %d is invalid", metadata.GetIndex())
	}
	if metadata.GetTerm() == 0 || metadata.GetTerm() == math.MaxUint64 {
		return fmt.Errorf("snapshot term %d is invalid", metadata.GetTerm())
	}
	if len(snapshot.GetData()) > MaxSnapshotBytes {
		return fmt.Errorf("%w: snapshot bytes %d exceed %d", ErrAdmissionBound, len(snapshot.GetData()), MaxSnapshotBytes)
	}
	if err := ValidateConfState(metadata.GetConfState(), metadata.GetIndex()); err != nil {
		return fmt.Errorf("snapshot ConfState: %w", err)
	}
	return nil
}

// ValidateConfState proves that state is one bounded, canonical configuration
// that the synchronous RawNode integration can restore at lastIndex. State is
// borrowed and is never mutated or retained. State-machine codecs call this
// before persistence so a durable publication can never become unrecoverable
// only when a later Node is constructed.
func ValidateConfState(state *pb.ConfState, lastIndex uint64) error {
	if state == nil {
		return errors.New("ConfState is nil")
	}
	if len(state.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("ConfState has unknown protobuf fields")
	}
	if state.GetAutoLeave() {
		return &UnsupportedError{Feature: "automatic joint consensus ConfState"}
	}
	if len(state.GetVoters()) == 0 {
		return errors.New("ConfState has no incoming voter")
	}
	memberCount := 0
	memberSets := [][]uint64{
		state.GetVoters(), state.GetVotersOutgoing(), state.GetLearners(), state.GetLearnersNext(),
	}
	for _, members := range memberSets {
		if !slices.IsSorted(members) {
			return errors.New("ConfState member lists are not canonically sorted")
		}
		if len(members) > MaxConfStateMembers-memberCount {
			return fmt.Errorf("%w: ConfState members exceed %d", ErrAdmissionBound, MaxConfStateMembers)
		}
		memberCount += len(members)
		for _, id := range members {
			if id == raft.None || raft.IsLocalMsgTarget(id) {
				return errors.New("ConfState has invalid member ID")
			}
		}
	}
	progress := tracker.MakeProgressTracker(MaxInflightMsgs, MaxInflightBytes)
	config, members, err := confchange.Restore(confchange.Changer{
		Tracker: progress, LastIndex: lastIndex,
	}, state)
	if err != nil {
		return fmt.Errorf("restore ConfState: %w", err)
	}
	progress.Config = config
	progress.Progress = members
	if err := state.Equivalent(progress.ConfState()); err != nil {
		return fmt.Errorf("noncanonical ConfState: %w", err)
	}
	return nil
}

func (n *Node) requirePhase(operation string, want Phase) error {
	if n.phase != want {
		return &PhaseError{Operation: operation, Have: n.phase, Want: want}
	}
	return nil
}

func (n *Node) admitProtocolInput(operation string, units int, inputBytes int64) error {
	if err := n.requirePhase(operation, PhaseIdle); err != nil {
		return err
	}
	if units <= 0 || units > MaxPendingInputUnits {
		return fmt.Errorf("%w: protocol input units %d exceed %d", ErrAdmissionBound, units, MaxPendingInputUnits)
	}
	if inputBytes < 0 || inputBytes > MaxPendingInputBytes {
		return fmt.Errorf("%w: protocol input bytes %d exceed %d", ErrAdmissionBound, inputBytes, MaxPendingInputBytes)
	}
	if n.raw.HasReady() && (!n.readyFromInput || n.pendingInputCalls >= MaxPendingInputCalls ||
		n.pendingInputUnits > MaxPendingInputUnits-units || n.pendingInputBytes > MaxPendingInputBytes-inputBytes) {
		return errors.Join(ErrReadyPending, ErrAdmissionBound)
	}
	return nil
}

func (n *Node) recordProtocolInput(units int, inputBytes int64) {
	if !n.raw.HasReady() {
		n.readyFromInput = false
		n.pendingInputCalls = 0
		n.pendingInputUnits = 0
		n.pendingInputBytes = 0
		return
	}
	if n.readyFromInput {
		n.pendingInputCalls++
		n.pendingInputUnits += units
		n.pendingInputBytes += inputBytes
		return
	}
	n.readyFromInput = true
	n.pendingInputCalls = 1
	n.pendingInputUnits = units
	n.pendingInputBytes = inputBytes
}

func inboundReadyBytes(message *pb.Message) int64 {
	if message == nil {
		return 0
	}
	entries := message.GetEntries()
	snapshotBytes := len(message.GetSnapshot().GetData())
	if len(entries) > MaxMessageEntries || snapshotBytes > MaxSnapshotBytes {
		return MaxPendingInputBytes + 1
	}
	total := int64(snapshotBytes)
	for _, entry := range entries {
		size := int64(len(entry.GetData()))
		if size > MaxProposalBytes || total > MaxPendingInputBytes-size {
			return MaxPendingInputBytes + 1
		}
		total += size
	}
	return total
}

func (n *Node) fail(stage Phase, index uint64, err error) error {
	applyErr := &ApplyError{Stage: stage, Index: index, Err: err}
	n.failure = applyErr
	n.phase = PhaseFailed
	return applyErr
}

type recoveryStorage struct {
	StableStore
	confState *pb.ConfState
}

func (s recoveryStorage) InitialState() (*pb.HardState, *pb.ConfState, error) {
	hardState, _, err := s.StableStore.InitialState()
	return hardState, cloneConfState(s.confState), err
}
