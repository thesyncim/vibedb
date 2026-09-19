package main

import (
	"context"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replicaaction"
)

func (factory *rf3DynamicLearnerFactory) sourceRetired(ctx context.Context, intent gateway.GroupEnrollmentIntent) (bool, error) {
	if factory == nil || factory.runtime == nil || factory.runtime.actionJournal == nil {
		return false, nodecontrol.ErrControl
	}
	records, err := factory.runtime.actionJournal.SourceRetirements(ctx)
	if err != nil {
		return false, err
	}
	return rf3ReplicaSourceRetired(records, intent.Group, intent.Target.Member,
		intent.Target.StoreID, uint64(intent.AllocationGeneration)), nil
}

// A source retirement permanently names its storage identity. Runtime
// incarnations and command generations advance across restarts; neither can
// revive storage which already crossed the durable removal-proof boundary.
// A subsequent enrollment must own a different replica storage identity.
func rf3ReplicaSourceRetired(
	records []replicaaction.Record,
	group raftmember.GroupKey,
	member uint64,
	store [16]byte,
	allocation uint64,
) bool {
	for _, record := range records {
		request := record.Request
		if request.Kind == replicaaction.SourceRetirement &&
			(record.State == replicaaction.RetirementAuthorized || record.State == replicaaction.Complete) &&
			request.Fence.Group == group && request.SourceMember == member &&
			request.Fence.MemberID == member && request.Fence.StoreID == store &&
			request.Fence.AllocationGeneration == allocation {
			return true
		}
	}
	return false
}
