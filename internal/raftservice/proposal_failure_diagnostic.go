package raftservice

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"go.etcd.io/raft/v3"
)

// ProposalFailureReason is a bounded diagnostic, not a retry or admission
// decision. It deliberately omits raw error text, which can contain input.
type ProposalFailureReason uint8

var proposalFailureCauses = [...]struct {
	cause error
	name  string
}{
	{nil, "unclassified"},
	{raftmodel.ErrReadyPending, "ready-pending"},
	{raftmodel.ErrAdmissionBound, "raft-admission-bound"},
	{raftstore.ErrFull, "wal-headroom"},
	{sqldriver.ErrReplicatedApplyBusy, "apply-busy"},
	{sqldriver.ErrReplicatedApplyBasePending, "apply-base-pending"},
	{replicatedstate.ErrWrongBinding, "command-binding"},
	{replicatedstate.ErrTransitionCapture, "transition-capture"},
	{replicatedstate.ErrStateCorrupt, "state-corrupt"},
	{replicatedstate.ErrSessionCorrupt, "session-corrupt"},
	{replicatedstate.ErrApplyPoisoned, "apply-poisoned"},
	{raftmodel.ErrWrongPhase, "ready-phase"},
	{raftmember.ErrResultSettlementPending, "result-settlement-pending"},
	{raft.ErrProposalDropped, "raft-proposal-dropped"},
	{raftstore.ErrPersistenceUnknown, "wal-persistence-unknown"},
	{raftstore.ErrPersistenceDefinite, "wal-persistence-refused"},
	{replicatedstate.ErrAdmissionBound, "state-admission-bound"},
	{replicatedstate.ErrRetryRetired, "retry-retired"},
}

func (reason ProposalFailureReason) String() string {
	if int(reason) >= len(proposalFailureCauses) {
		return proposalFailureCauses[0].name
	}
	return proposalFailureCauses[reason].name
}

func (metrics *ProgressMetrics) observeProposalFailure(group raftmember.GroupKey, err error) {
	if errors.Is(err, raftmodel.ErrNotLeader) {
		return
	}
	reason := ProposalFailureReason(0)
	for i := 1; i < len(proposalFailureCauses); i++ {
		if errors.Is(err, proposalFailureCauses[i].cause) {
			reason = ProposalFailureReason(i)
			break
		}
	}
	bit := uint64(1) << reason
	if metrics.proposalFailureSeen.Or(bit)&bit == 0 {
		metrics.ProposalFailure(group, reason)
	}
}
