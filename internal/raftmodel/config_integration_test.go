package raftmodel_test

import (
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/replication"
)

func TestProposalAdmissionMatchesCommandEnvelopeAndHardGroupBound(t *testing.T) {
	if raftmodel.MaxProposalBytes != replication.MaxCommandBytes {
		t.Fatalf("proposal bytes %d differ from command envelope %d", raftmodel.MaxProposalBytes, replication.MaxCommandBytes)
	}
	if raftmodel.MaxProposalBytes > raftmodel.MaxUncommittedEntriesSize {
		t.Fatalf("one proposal %d exceeds hard uncommitted bound %d", raftmodel.MaxProposalBytes, raftmodel.MaxUncommittedEntriesSize)
	}
	if raftmodel.MaxProposalBytes <= raftmodel.MaxSizePerMsg ||
		raftmodel.MaxProposalBytes <= raftmodel.MaxCommittedSizePerReady ||
		raftmodel.MaxProposalBytes <= raftmodel.MaxInflightBytes {
		t.Fatal("test no longer exercises documented soft-target overshoot")
	}
}
