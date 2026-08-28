package raftservice

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
)

func TestProposalFailureDiagnosticBoundedAndPayloadFree(t *testing.T) {
	if len(proposalFailureCauses) > 64 {
		t.Fatal("reason directory exceeds fixed first-seen mask")
	}
	group := raftmember.GroupKey{GroupID: [16]byte{7}}
	var seen [64]atomic.Uint32
	metrics := &ProgressMetrics{ProposalFailure: func(got raftmember.GroupKey, reason ProposalFailureReason) {
		if got != group || int(reason) >= len(proposalFailureCauses) || strings.Contains(reason.String(), "private-input") {
			t.Errorf("unsafe diagnostic: group=%+v reason=%s", got, reason)
			return
		}
		seen[reason].Add(1)
	}}
	progress := multiraft.Progress{Group: group, Kind: multiraft.ProgressProposal}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			for i, cause := range proposalFailureCauses {
				observed := progress
				if i%2 != 0 {
					// Host changes a terminal proposal's kind to Fault, retaining
					// its proposal count and original admission error.
					observed.Kind = multiraft.ProgressFault
					observed.ProposalCount = 1
				}
				err := errors.New("private-input")
				if i != 0 {
					err = fmt.Errorf("private-input: %w", cause.cause)
				}
				metrics.observeProgress(observed, true, err)
			}
		})
	}
	workers.Wait()
	for i := range proposalFailureCauses {
		if got := seen[i].Load(); got != 1 {
			t.Fatalf("reason %d callbacks=%d, want exactly one", i, got)
		}
	}
	if ProposalFailureReason(255).String() != "unclassified" {
		t.Fatal("unknown reason exposed arbitrary text")
	}
}

func TestProposalFailureDiagnosticIgnoresSuccessAndLeadership(t *testing.T) {
	metrics := &ProgressMetrics{ProposalFailure: func(raftmember.GroupKey, ProposalFailureReason) {
		t.Fatal("unexpected failure diagnostic")
	}}
	proposal := multiraft.Progress{Kind: multiraft.ProgressProposal}
	metrics.observeProgress(proposal, true, raftmodel.ErrNotLeader)
	metrics.observeProgress(multiraft.Progress{Kind: multiraft.ProgressReady}, true, errors.New("persistence"))
	metrics.observeProgress(multiraft.Progress{Kind: multiraft.ProgressFault}, true, errors.New("non-proposal fault"))
	if allocations := testing.AllocsPerRun(100, func() {
		metrics.observeProgress(proposal, true, nil)
	}); allocations != 0 {
		t.Fatalf("successful proposal diagnostic allocated %v", allocations)
	}
}
