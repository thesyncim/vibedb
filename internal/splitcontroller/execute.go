package splitcontroller

import (
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// AppendSourceSeal appends the exact binary ownership transition required by
// ActionSealSource. The state must equal the exact validated unsealed tail cut.
// The current source binding and replica-set version remain in the command, so
// replicated apply rechecks them at proposal execution.
// A caller can reuse a buffer with capacity MaxOwnershipTransitionBytes to keep
// the control loop allocation-free.
func (p *Plan) AppendSourceSeal(
	dst []byte,
	state replicatedstate.State,
	tail rangesplit.TailCursor,
	sourceMember uint64,
	targetMember uint64,
) ([]byte, error) {
	if p == nil || !p.initialSourceState(state) || state.ReplicaSetVersion == 0 ||
		p.partitioner.ValidateTailCursor(tail) != nil || tail.Sealed() ||
		!p.sourceStateMatchesCut(state, tail) {
		return dst, ErrTopologyConflict
	}
	if !sourceSessionsEmpty(state) {
		return dst, ErrSessionTransferRequired
	}
	retained := p.children[p.retained]
	encoded, err := replicatedstate.AppendOwnershipTransition(
		dst,
		replicatedstate.OwnershipTransition{
			From: state.Binding, ExpectedReplicaSetVersion: state.ReplicaSetVersion,
			SourceMember: sourceMember, TargetMember: targetMember,
			ToOwnershipEpoch:  uint64(retained.OwnershipEpoch),
			ToRoutingVersion:  uint64(p.targetManifest.Version()),
			ToRouteGeneration: p.next,
			ToOwnedRange:      retained.Range,
		},
	)
	if err != nil {
		return dst, errors.Join(ErrTopologyConflict, err)
	}
	return encoded, nil
}

// BuildCatalogTransition constructs the exact unpublished successor required
// by ActionPublishCatalog from the caller's coherent sealed source
// state. The caller must keep that source quiescent through the catalog CAS;
// this validation does not reserve either authority. SaveSnapshotAfter and
// CatalogHolder.PublishAfter remain the only durable and in-memory authority
// changes.
func (p *Plan) BuildCatalogTransition(
	current *gateway.Snapshot,
	sourceState replicatedstate.State,
	certificate rangesplit.CutoverCertificate,
) (*gateway.Snapshot, error) {
	if p == nil || current == nil {
		return nil, ErrInvalidPlan
	}
	stage, err := p.catalogStage(current)
	if err != nil || stage != catalogSource {
		return nil, errors.Join(ErrTopologyConflict, err)
	}
	if !p.sourceStateAfterCutover(sourceState, certificate) {
		return nil, ErrTopologyConflict
	}
	if !sourceSessionsEmpty(sourceState) {
		return nil, ErrSessionTransferRequired
	}
	next, err := BuildCertifiedRangeSplitTransition(
		current, p.targetManifest, p.next, p.partitioner, certificate,
	)
	if err != nil {
		return nil, errors.Join(ErrTopologyConflict, err)
	}
	return next, nil
}

func (p *Plan) initialSourceState(state replicatedstate.State) bool {
	if p == nil {
		return false
	}
	binding := state.Binding
	return state.Applied != 0 && state.LastTerm != 0 &&
		state.DataChainDigest != ([32]byte{}) && state.LastEntryDigest != ([32]byte{}) &&
		state.SnapshotBaseDigest != ([32]byte{}) &&
		binding.Distribution == string(p.source.Distribution) &&
		binding.Shard == string(p.source.Shard) &&
		binding.AllocationGeneration == uint64(p.source.AllocationGeneration) &&
		p.sourceBindingInitial(binding)
}
