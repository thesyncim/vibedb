package raftmodel

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"

	"go.etcd.io/raft/v3"
)

const (
	// MaxReadContextBytes bounds each opaque correlation value retained by the
	// driver. The bytes are never transformed or interpreted.
	MaxReadContextBytes = 256
	// MaxPendingReads bounds issued plus quorum-confirmed barriers per member.
	MaxPendingReads = 1024
	// MaxPendingReadBytes independently bounds aggregate retained contexts.
	MaxPendingReadBytes = MaxPendingReads * MaxReadContextBytes
	// MaxProposalBytes matches the independently versioned command-envelope
	// ceiling. MaxSizePerMsg is a batching target; Raft still sends one entry
	// larger than that target as a single message.
	MaxProposalBytes = 16 << 20
	// MaxInboundMessageBytes includes one maximum command plus bounded Raft
	// protobuf framing. Transport admission must enforce the same or a smaller
	// complete-frame limit before allocating a decoded message.
	MaxInboundMessageBytes = MaxProposalBytes + 1<<20
	// MaxSnapshotBytes bounds one logical snapshot carried by the Phase-0
	// integration port. Production snapshot streaming uses a separate format.
	MaxSnapshotBytes = 16 << 20
	// MaxMessageEntries prevents pointer-heavy empty-entry batches from
	// bypassing the encoded-byte bound.
	MaxMessageEntries = 4096
	// MaxPendingInputUnits bounds weighted protocol input accumulated into one
	// uncaptured Ready. A call costs at least one unit and one unit per Entry, so
	// the same bound also caps accumulated unstable entries.
	MaxPendingInputUnits = MaxMessageEntries
	// MaxPendingInputCalls independently bounds message/read/control work that
	// carries no unstable entries but can still grow an uncaptured Ready.
	MaxPendingInputCalls = MaxInflightMsgs
	// MaxPendingInputBytes bounds the Entry/Snapshot payload bytes accumulated
	// into one uncaptured Ready. Followers do not apply the leader proposal
	// watermark, so the integration boundary enforces it independently.
	MaxPendingInputBytes int64 = MaxUncommittedEntriesSize
	// MaxConfStateMembers bounds the total incoming/outgoing voter and learner
	// references reconstructed by upstream Changer. Joint configurations count
	// both voter sets because both are retained and safety-critical.
	MaxConfStateMembers = 64
)

type readIssue struct {
	term        uint64
	incarnation uint64
	context     []byte
	sequence    uint64
}

// ReadBarrier is a quorum-confirmed read cut bound to one leadership term and
// process incarnation. Context is an exact defensive copy of the opaque value
// supplied to ReadIndex.
type ReadBarrier struct {
	Context     []byte
	Index       uint64
	Term        uint64
	Incarnation uint64
}

// ReadOutcome completes one ReadIndex request. A nil Err certifies that the
// local reader publication reached Barrier.Index while the issuing leadership
// was still current. ErrReadLeadershipLost requires the caller to retry.
type ReadOutcome struct {
	Barrier ReadBarrier
	Err     error
}

// ReadIndex starts a quorum-confirmed linearizable read. Empty and duplicate
// contexts are rejected because the core returns contexts as exact opaque
// correlation keys.
func (n *Node) ReadIndex(context []byte) error {
	if err := n.admitProtocolInput("ReadIndex", 1, 0); err != nil {
		return err
	}
	if len(context) == 0 {
		return errors.New("raftmodel: empty ReadIndex context")
	}
	if len(context) > MaxReadContextBytes {
		return fmt.Errorf("%w: ReadIndex context bytes %d exceed %d", ErrAdmissionBound, len(context), MaxReadContextBytes)
	}
	if n.PendingReads() >= MaxPendingReads || n.readBytes+len(context) > MaxPendingReadBytes {
		return fmt.Errorf("%w: pending ReadIndex limit reached", ErrAdmissionBound)
	}
	if n.readSeq == math.MaxUint64 {
		return fmt.Errorf("%w: ReadIndex sequence exhausted", ErrAdmissionBound)
	}
	status := n.raw.BasicStatus()
	if status.RaftState != raft.StateLeader || status.Lead != n.id {
		return ErrNotLeader
	}
	key := string(context)
	if _, exists := n.issuedReads[key]; exists {
		return ErrDuplicateReadContext
	}
	for _, barrier := range n.pendingReads {
		if string(barrier.Context) == key {
			return ErrDuplicateReadContext
		}
	}
	copyOfContext := append([]byte(nil), context...)
	n.readSeq++
	n.issuedReads[string(copyOfContext)] = readIssue{
		term:        status.GetTerm(),
		incarnation: n.incarnation,
		context:     copyOfContext,
		sequence:    n.readSeq,
	}
	n.readBytes += len(copyOfContext)
	n.raw.ReadIndex(copyOfContext)
	n.recordProtocolInput(1, 0)
	return nil
}

// PendingReads is the number of issued or quorum-confirmed barriers awaiting
// a terminal outcome.
func (n *Node) PendingReads() int { return len(n.issuedReads) + len(n.pendingReads) }

// PendingReadBytes is the aggregate number of opaque context bytes retained.
func (n *Node) PendingReadBytes() int { return n.readBytes }

func (n *Node) cancelStaleIssuedReads() []ReadOutcome {
	if len(n.issuedReads) == 0 {
		return nil
	}
	status := n.raw.BasicStatus()
	type staleRead struct {
		key   string
		issue readIssue
	}
	var stale []staleRead
	for key, issue := range n.issuedReads {
		currentLeadership := issue.incarnation == n.incarnation &&
			issue.term == status.GetTerm() &&
			status.RaftState == raft.StateLeader &&
			status.Lead == n.id
		if currentLeadership {
			continue
		}
		stale = append(stale, staleRead{key: key, issue: issue})
	}
	slices.SortFunc(stale, func(left, right staleRead) int {
		return cmp.Compare(left.issue.sequence, right.issue.sequence)
	})
	outcomes := make([]ReadOutcome, 0, len(stale))
	for _, item := range stale {
		issue := item.issue
		barrier := ReadBarrier{
			Context:     append([]byte(nil), issue.context...),
			Term:        issue.term,
			Incarnation: issue.incarnation,
		}
		outcomes = append(outcomes, ReadOutcome{Barrier: barrier, Err: ErrReadLeadershipLost})
		n.readBytes -= len(issue.context)
		delete(n.issuedReads, item.key)
	}
	return outcomes
}

func (n *Node) releaseReads() []ReadOutcome {
	if len(n.pendingReads) == 0 {
		return nil
	}
	status := n.raw.BasicStatus()
	outcomes := make([]ReadOutcome, 0, len(n.pendingReads))
	retained := n.pendingReads[:0]
	for _, barrier := range n.pendingReads {
		currentLeadership := barrier.Incarnation == n.incarnation &&
			barrier.Term == status.GetTerm() &&
			status.RaftState == raft.StateLeader &&
			status.Lead == n.id
		if !currentLeadership {
			outcomes = append(outcomes, ReadOutcome{Barrier: cloneReadBarrier(barrier), Err: ErrReadLeadershipLost})
			n.readBytes -= len(barrier.Context)
			continue
		}
		if n.published.Applied >= barrier.Index {
			outcomes = append(outcomes, ReadOutcome{Barrier: cloneReadBarrier(barrier)})
			n.readBytes -= len(barrier.Context)
			continue
		}
		retained = append(retained, barrier)
	}
	for i := len(retained); i < len(n.pendingReads); i++ {
		n.pendingReads[i] = ReadBarrier{}
	}
	n.pendingReads = retained
	return outcomes
}

func cloneReadBarrier(barrier ReadBarrier) ReadBarrier {
	barrier.Context = append([]byte(nil), barrier.Context...)
	return barrier
}
