package raftmodel

import (
	"errors"
	"fmt"
	"math"

	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
)

// PipelinedReady is the bounded work emitted by one accepted asynchronous
// Ready. Messages are owned by upstream Raft's local-storage protocol and may
// be retained until their target processes them. ReadOutcomes are detached.
// Entries, HardState, and committed entries are deliberately exposed only
// through MsgStorageAppend and MsgStorageApply.
type PipelinedReady struct {
	Messages     []*pb.Message
	ReadOutcomes []ReadOutcome
}

// CapturePipelinedReady accepts one upstream asynchronous Ready without
// waiting for either local storage target. Messages for the same local target
// must subsequently be processed reliably and in order. Completion responses
// replace RawNode.Advance.
func (n *Node) CapturePipelinedReady() (PipelinedReady, bool, error) {
	if n == nil || !n.async {
		return PipelinedReady{}, false, errors.New("raftmodel: pipelined Ready requires an asynchronous Node")
	}
	if err := n.requirePhase("CapturePipelinedReady", PhaseIdle); err != nil {
		return PipelinedReady{}, false, err
	}
	if n.settlementCount != 0 {
		return PipelinedReady{}, false, ErrAppliedSettlementPending
	}
	if !n.raw.HasReady() {
		return PipelinedReady{}, false, nil
	}
	ready := n.raw.Ready()
	if len(ready.Messages) > MaxPipelinedReadyMessages {
		return PipelinedReady{}, false, n.fail(
			PhaseFailed, 0, fmt.Errorf("raftmodel: pipelined Ready messages %d exceed %d",
				len(ready.Messages), MaxPipelinedReadyMessages),
		)
	}
	n.readyFromInput = false
	n.pendingInputCalls = 0
	n.pendingInputUnits = 0
	n.pendingInputBytes = 0
	if !raft.IsEmptySnap(ready.Snapshot) {
		return PipelinedReady{}, false, n.fail(
			PhaseFailed, ready.Snapshot.GetMetadata().GetIndex(),
			&UnsupportedError{Feature: "in-band Ready snapshots in the immutable-base WAL kernel"},
		)
	}
	for _, message := range ready.Messages {
		if err := n.validatePipelinedMessage(message); err != nil {
			return PipelinedReady{}, false, n.fail(PhaseFailed, 0, err)
		}
	}
	outcomes, err := n.acceptPipelinedReadStates(ready.ReadStates)
	if err != nil {
		return PipelinedReady{}, false, err
	}
	return PipelinedReady{Messages: ready.Messages, ReadOutcomes: outcomes}, true, nil
}

func (n *Node) validatePipelinedMessage(message *pb.Message) error {
	if message == nil || message.Type == nil || message.To == nil || message.From == nil ||
		len(message.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("raftmodel: malformed pipelined Ready message")
	}
	switch message.GetType() {
	case pb.MsgStorageAppend:
		if message.GetTo() != raft.LocalAppendThread || message.GetFrom() != n.id {
			return errors.New("raftmodel: invalid local append target")
		}
		if message.Term == nil != (message.Vote == nil) || message.Term == nil != (message.Commit == nil) {
			return errors.New("raftmodel: partial local append HardState")
		}
		if message.GetTerm() == math.MaxUint64 || message.GetCommit() == math.MaxUint64 ||
			len(message.GetEntries()) > MaxMessageEntries ||
			len(message.GetResponses()) > MaxPipelinedReadyMessages {
			return errors.New("raftmodel: invalid local append bounds")
		}
	case pb.MsgStorageApply:
		if message.GetTo() != raft.LocalApplyThread || message.GetFrom() != n.id ||
			len(message.GetEntries()) == 0 || len(message.GetEntries()) > MaxMessageEntries ||
			len(message.GetResponses()) > MaxPipelinedReadyMessages {
			return errors.New("raftmodel: invalid local apply target")
		}
	case pb.MsgStorageAppendResp, pb.MsgStorageApplyResp:
		return errors.New("raftmodel: Ready emitted a storage response as work")
	default:
		if raft.IsLocalMsgTarget(message.GetTo()) || message.GetFrom() != n.id || message.GetTo() == raft.None ||
			message.GetTo() == n.id {
			return errors.New("raftmodel: invalid pipelined network message identity")
		}
	}
	return nil
}

func (n *Node) acceptPipelinedReadStates(states []raft.ReadState) ([]ReadOutcome, error) {
	for _, state := range states {
		if state.Index == 0 {
			return nil, n.fail(PhaseFailed, 0, errors.New("core returned zero ReadIndex barrier"))
		}
		key, keyOK := makeReadContextKey(state.RequestCtx)
		if !keyOK {
			return nil, n.fail(PhaseFailed, state.Index, errors.New("invalid ReadIndex context returned by core"))
		}
		issue, ok := n.issuedReads[key]
		if !ok {
			return nil, n.fail(PhaseFailed, state.Index, errors.New("unknown or duplicate ReadIndex context returned by core"))
		}
		delete(n.issuedReads, key)
		n.pendingReads = append(n.pendingReads, ReadBarrier{
			Context: append([]byte(nil), state.RequestCtx...), Index: state.Index,
			Term: issue.term, Incarnation: issue.incarnation,
		})
	}
	outcomes := n.cancelStaleIssuedReads()
	outcomes = append(outcomes, n.releaseReads()...)
	return outcomes, nil
}

// StepPipelinedResponse returns one completed local-storage response (or one
// response nested behind that storage fence) to RawNode. It is the only
// replacement for Advance in asynchronous mode.
func (n *Node) StepPipelinedResponse(message *pb.Message) error {
	if n == nil || !n.async {
		return errors.New("raftmodel: storage response requires an asynchronous Node")
	}
	if err := n.requirePhase("StepPipelinedResponse", PhaseIdle); err != nil {
		return err
	}
	if message == nil || message.Type == nil || message.To == nil || message.From == nil ||
		message.GetTo() != n.id || len(message.GetResponses()) != 0 ||
		len(message.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("raftmodel: malformed local storage response")
	}
	switch message.GetType() {
	case pb.MsgStorageAppendResp:
		if message.GetFrom() != raft.LocalAppendThread {
			return errors.New("raftmodel: invalid local append response source")
		}
	case pb.MsgStorageApplyResp:
		if message.GetFrom() != raft.LocalApplyThread || len(message.GetEntries()) == 0 {
			return errors.New("raftmodel: invalid local apply response source")
		}
	default:
		if message.GetFrom() != n.id || raft.IsLocalMsgTarget(message.GetFrom()) {
			return fmt.Errorf("raftmodel: unexpected nested local response %s", message.GetType())
		}
	}
	// Async-storage responses are created by RawNode and ownership is returned
	// to that same RawNode after the corresponding local work completes. Raft
	// consumes these messages synchronously: storage-append responses read the
	// stable entry/snapshot identity, storage-apply responses read the applied
	// entry extent, and delayed protocol responses contain no caller-owned
	// transport graph. Cloning here defeated that ownership transfer and made
	// every local durability completion allocate through protobuf reflection.
	if err := n.raw.Step(message); err != nil {
		return fmt.Errorf("raftmodel: step local storage response: %w", err)
	}
	n.observeCommitAdvancement()
	return nil
}

// BeginPipelinedApply installs one ordered MsgStorageApply payload into the
// existing model-checked apply machinery. RawNode remains owner-thread only;
// configuration entries are therefore applied in exact log order here.
func (n *Node) BeginPipelinedApply(taskID uint64, entries []*pb.Entry) error {
	if n == nil || !n.async || taskID == 0 || len(entries) == 0 || len(entries) > MaxMessageEntries {
		return errors.New("raftmodel: invalid pipelined apply task")
	}
	if err := n.requirePhase("BeginPipelinedApply", PhaseIdle); err != nil {
		return err
	}
	if n.settlementCount != 0 {
		return ErrAppliedSettlementPending
	}
	n.ready = raft.Ready{CommittedEntries: entries}
	n.readyID = taskID
	n.entryPos = 0
	if hasConfigurationEntry(entries) {
		n.pendingConfChange = true
	}
	n.phase = PhaseSnapshotInstalled
	return nil
}

// FinishPipelinedApply releases a completely applied and settled local task.
// The task's nested responses may be delivered only after this succeeds.
func (n *Node) FinishPipelinedApply() error {
	if n == nil || !n.async {
		return errors.New("raftmodel: pipelined apply requires an asynchronous Node")
	}
	if err := n.requirePhase("FinishPipelinedApply", PhaseEntriesApplied); err != nil {
		return err
	}
	if n.settlementCount != 0 || n.entryPos != len(n.ready.CommittedEntries) {
		return ErrAppliedSettlementPending
	}
	n.ready = raft.Ready{}
	n.readyID = 0
	n.entryPos = 0
	n.phase = PhaseIdle
	return nil
}

// SendPipelinedSnapshotBaseProbe abandons an in-band snapshot send and probes
// whether the peer already retains the immutable base. A fresh leader can lose
// its progress knowledge even when the peer has that base after a restart.
// Only an ordinary AppendEntries response can advance Match; this never reports
// snapshot installation or sends the snapshot's data over ordinary transport.
// A peer genuinely behind the base still requires certified out-of-band repair.
func (n *Node) SendPipelinedSnapshotBaseProbe(message *pb.Message, send func(*pb.Message) error) error {
	if n == nil || !n.async || send == nil {
		return errors.New("raftmodel: invalid snapshot base probe")
	}
	if err := n.requirePhase("SendPipelinedSnapshotBaseProbe", PhaseIdle); err != nil {
		return err
	}
	if message == nil || message.GetType() != pb.MsgSnap || message.GetFrom() != n.id || message.GetTo() == raft.None || message.GetTo() == n.id || raft.IsLocalMsgTarget(message.GetTo()) || message.GetTerm() == 0 || message.GetTerm() == math.MaxUint64 {
		return errors.New("raftmodel: invalid snapshot base probe identity")
	}
	if err := validateSnapshotEnvelope(message.GetSnapshot()); err != nil {
		return err
	}
	metadata := message.GetSnapshot().GetMetadata()
	if metadata.GetTerm() > message.GetTerm() {
		return errors.New("raftmodel: snapshot base term exceeds leader term")
	}
	index, logTerm := metadata.GetIndex(), metadata.GetTerm()
	from, to, term := message.GetFrom(), message.GetTo(), message.GetTerm()
	probe := &pb.Message{Type: pb.MsgApp.Enum(), From: &from, To: &to, Term: &term, Index: &index, LogTerm: &logTerm, Commit: &index}
	if err := send(probe); err != nil {
		return err
	}
	status := n.raw.BasicStatus()
	if status.RaftState == raft.StateLeader && status.GetTerm() == term {
		// No snapshot was transferred. Clear its pause without asserting that
		// the peer installed anything; its append response is the sole proof.
		n.raw.ReportSnapshot(to, raft.SnapshotFailure)
	}
	return nil
}
