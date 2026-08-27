package replicatedstate

import (
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmodel"
)

// ErrSchemaSourceNotCommitted is returned only after authenticating the exact
// prepared source cut. It is not a catch-all recovery fallback.
var ErrSchemaSourceNotCommitted = errors.New("replicatedstate: prepared schema source is not committed")

func (m *Machine) validateOpenedSchemaSource(state State, prepared openInputs) error {
	p := prepared.options.SchemaSourceRecovery
	transition, err := OpenSchemaTransition(p.Command)
	if err != nil || m.checkpointGroup == nil || p.SourceApplied == ^uint64(0) ||
		len(prepared.options.SchemaTransition) != 0 ||
		transition.FromManifest != prepared.manifestDigest ||
		transition.FromApplyContract != prepared.applyContract ||
		transition.ExpectedReplicaSetVersion != state.ReplicaSetVersion ||
		transition.FromPlacementDigest != relationPlacementStateDigest(
			transition.From.SchemaGeneration, prepared.manifestDigest, m.relations) ||
		transition.MembershipSequence != p.Membership.Sequence ||
		transition.MembershipSource != p.Membership.Source ||
		transition.MembershipTarget != p.Membership.Target ||
		transition.AuthorizationDigest != p.AuthorizationDigest ||
		transition.CatalogCASDigest != p.CatalogCASDigest {
		return errors.Join(ErrSchemaTransition, err)
	}
	sourceState := state
	sourceState.Binding = transition.From
	if !bindingMatchesFenceOrigin(prepared.binding, sourceState) {
		return ErrSchemaTransition
	}
	if state.Applied == p.SourceApplied && state.Binding == transition.From &&
		state.ApplyContractDigest == transition.FromApplyContract &&
		state.RelationPlacementDigest == transition.FromPlacementDigest &&
		state.BootstrapDigest == prepared.bootstrapDigest {
		if err := m.checkpointGroup.ObserveMembershipTransition(p.Membership, transition.RequestDigest); err != nil {
			return errors.Join(ErrSchemaTransition, err)
		}
		return ErrSchemaSourceNotCommitted
	}
	meta := raftmodel.ApplyMeta{Index: state.Applied, Term: state.LastTerm, Type: state.LastEntryType}
	if state.Applied != p.SourceApplied+1 || state.LastKind != RecordSchema ||
		state.Binding != schemaTransitionBinding(transition) ||
		state.ApplyContractDigest != transition.ToApplyContract ||
		state.RelationPlacementDigest != transition.ToPlacementDigest ||
		state.BootstrapDigest != prepared.bootstrapDigest ||
		normalEntryDigest(meta, p.Command) != state.LastEntryDigest {
		return ErrSchemaTransition
	}
	return m.checkpointGroup.ObserveCommittedSourceMembershipTransition(
		p.Membership, transition.RequestDigest, state.Applied, sha256.Sum256(p.Command),
	)
}
