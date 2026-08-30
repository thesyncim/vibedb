package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftmodel"
)

// ErrSchemaSourceNotCommitted is returned only after authenticating the exact
// prepared source cut. It is not a catch-all recovery fallback.
var ErrSchemaSourceNotCommitted = errors.New("replicatedstate: prepared schema source is not committed")

func (m *Machine) validateOpenedSchemaSource(state State, prepared openInputs) error {
	p := prepared.options.SchemaSourceRecovery
	transition, err := OpenSchemaTransition(p.Command)
	if err != nil {
		return errors.Join(ErrSchemaTransition, err)
	}
	checks := []struct {
		ok   bool
		name string
	}{
		{m.checkpointGroup != nil, "checkpoint group"},
		{p.SourceApplied != ^uint64(0), "source applied"},
		{len(prepared.options.SchemaTransition) == 0, "source mode"},
		{transition.FromManifest == prepared.manifestDigest, "source manifest"},
		{transition.FromApplyContract == prepared.applyContract, "source apply contract"},
		{transition.ExpectedReplicaSetVersion == state.ReplicaSetVersion, "replica set"},
		{transition.FromPlacementDigest == relationPlacementStateDigest(transition.From.SchemaGeneration, prepared.manifestDigest, m.relations), "source placement"},
		{p.Membership.Sequence != 0, "local membership sequence"},
		{p.Membership.Source != ([sha256.Size]byte{}), "local membership source"},
		{p.Membership.Target != ([sha256.Size]byte{}), "local membership target"},
		{transition.AuthorizationDigest == p.AuthorizationDigest, "authorization"},
		{transition.CatalogCASDigest == p.CatalogCASDigest, "catalog CAS"},
	}
	for _, check := range checks {
		if !check.ok {
			return fmt.Errorf("%w: source %s", ErrSchemaTransition, check.name)
		}
	}
	preCommandApplied := p.SourceApplied
	if p.PreCommandApplied != 0 {
		if p.PreCommandApplied <= p.SourceApplied {
			return fmt.Errorf("%w: empty suffix bound", ErrSchemaTransition)
		}
		preCommandApplied = p.PreCommandApplied
	}
	sourceState := state
	sourceState.Binding = transition.From
	if !bindingMatchesFenceOrigin(prepared.binding, sourceState) {
		return fmt.Errorf("%w: source binding", ErrSchemaTransition)
	}
	if state.Applied == preCommandApplied && state.Binding == transition.From &&
		state.ApplyContractDigest == transition.FromApplyContract &&
		state.RelationPlacementDigest == transition.FromPlacementDigest &&
		state.BootstrapDigest == prepared.bootstrapDigest {
		if err := m.checkpointGroup.ObserveMembershipTransition(p.Membership, transition.RequestDigest); err != nil {
			return errors.Join(ErrSchemaTransition, err)
		}
		return ErrSchemaSourceNotCommitted
	}
	meta := raftmodel.ApplyMeta{Index: state.Applied, Term: state.LastTerm, Type: state.LastEntryType}
	committedCommand := p.Command
	if len(p.CommittedCommand) != 0 {
		committedTransition, committedErr := OpenSchemaTransition(p.CommittedCommand)
		if committedErr != nil || !schemaTransitionEqualExceptCatalogCAS(transition, committedTransition) {
			return errors.Join(ErrSchemaTransition, committedErr)
		}
		committedCommand = p.CommittedCommand
	}
	legacyReplicaLocalCommand := transition.MembershipSequence == p.Membership.Sequence &&
		transition.MembershipSource == p.Membership.Source && transition.MembershipTarget == p.Membership.Target
	exactCommand := normalEntryDigest(meta, committedCommand) == state.LastEntryDigest
	if state.Applied != preCommandApplied+1 || state.LastKind != RecordSchema ||
		state.Binding != schemaTransitionBinding(transition) ||
		state.ApplyContractDigest != transition.ToApplyContract ||
		state.RelationPlacementDigest != transition.ToPlacementDigest ||
		state.BootstrapDigest != prepared.bootstrapDigest ||
		(!exactCommand && !legacyReplicaLocalCommand) {
		return fmt.Errorf("%w: committed source state applied=%d source=%d pre-command=%d expected=%d kind=%d binding=%t contract=%t placement=%t bootstrap=%t digest=%t local-command=%t membership-sequence=%d/%d source=%t target=%t",
			ErrSchemaTransition, state.Applied, p.SourceApplied, p.PreCommandApplied, preCommandApplied+1, state.LastKind,
			state.Binding == schemaTransitionBinding(transition),
			state.ApplyContractDigest == transition.ToApplyContract,
			state.RelationPlacementDigest == transition.ToPlacementDigest,
			state.BootstrapDigest == prepared.bootstrapDigest,
			exactCommand, legacyReplicaLocalCommand,
			transition.MembershipSequence, p.Membership.Sequence,
			transition.MembershipSource == p.Membership.Source,
			transition.MembershipTarget == p.Membership.Target)
	}
	if !exactCommand || len(p.CommittedCommand) != 0 && !bytes.Equal(p.CommittedCommand, p.Command) {
		m.legacySchemaSourceCommand = sha256.Sum256(p.Command)
	}
	if p.PreCommandApplied != 0 {
		return m.checkpointGroup.ObserveCommittedSourceMembershipTransitionAfterEmptySuffix(
			p.Membership, transition.RequestDigest, p.SourceApplied, state.Applied, sha256.Sum256(p.Command),
		)
	}
	return m.checkpointGroup.ObserveCommittedSourceMembershipTransition(
		p.Membership, transition.RequestDigest, state.Applied, sha256.Sum256(p.Command),
	)
}

func schemaTransitionEqualExceptCatalogCAS(a, b SchemaTransitionView) bool {
	left, right := a.SchemaTransition, b.SchemaTransition
	left.CatalogCASDigest, right.CatalogCASDigest = [sha256.Size]byte{}, [sha256.Size]byte{}
	return left == right
}
