package gateway

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/requestledger"
)

// validateDurableRequestPreparedCut checks a leader-read cut against the exact
// immutable recipe, then reuses the kernel's prepared transition validation.
// The reconstructed prior head is only validation input, never published.
func validateDurableRequestPreparedCut(execution DurableRequestTypedExecutionContext, cut durableRequestTerminalReadCut) error {
	head, prepared := cut.Head, cut.Prepared
	contract := execution.Recipe.Contract
	if cut.Applied == 0 || head.Phase != requestledger.PhasePrepared || cut.Terminal.Revision != 0 || prepared.Revision <= 1 ||
		head.Revision < prepared.Revision || head.Key != execution.Key.RequestKey ||
		head.KeyDigest != requestledger.Digest(execution.Recipe.KeyDigest) ||
		head.RequestDigest != requestledger.Digest(execution.Recipe.RequestDigest) ||
		head.TerminalContractDigest != requestledger.Digest(contract.TerminalContractDigest) ||
		head.CatalogGeneration != contract.CatalogGeneration ||
		head.PinID != requestledger.PinID(contract.PinID) ||
		head.PinDigest != requestledger.Digest(contract.PinDigest) ||
		head.RouteSchemaCertificateDigest != requestledger.Digest(contract.RouteSchemaCertificateDigest) ||
		head.PreparedTerminalDigest != prepared.PreparedDigest ||
		prepared.RetirementWitnessDigest != requestledger.Digest(contract.RetirementWitnessDigest) {
		return ErrDurableRequestConflict
	}
	prior := head
	prior.Phase, prior.Revision = requestledger.PhaseSealed, prepared.Revision-1
	prior.PreparedTerminalDigest, prior.SchemaPinReleaseCertificateDigest = requestledger.Digest{}, requestledger.Digest{}
	if _, err := requestledger.MarkTerminalPrepared(prior, cut.Continuation, prepared); err != nil {
		return errors.Join(err, ErrDurableRequestConflict)
	}
	if cut.SchemaPin.Revision == 0 && (head.Revision != prepared.Revision ||
		head.SchemaPinReleaseCertificateDigest != (requestledger.Digest{})) {
		return ErrDurableRequestConflict
	}
	return nil
}

// durableRequestTerminalReleaseCommand exposes only a release already owned
// by the authenticated ledger cut. It never substitutes today's controller or
// lease into those bytes, and grants no authority to execute another wave.
func durableRequestTerminalReleaseCommand(execution DurableRequestTypedExecutionContext, cut durableRequestTerminalReadCut) (executionpin.Command, error) {
	fail := func(err error) (executionpin.Command, error) {
		return executionpin.Command{}, errors.Join(err, ErrDurableRequestConflict)
	}
	if err := validateDurableRequestPreparedCut(execution, cut); err != nil {
		return fail(err)
	}
	head, prepared, release := cut.Head, cut.Prepared, cut.SchemaPin
	if release.Revision != head.Revision || release.PreparedTerminalDigest != prepared.PreparedDigest ||
		release.KeyDigest != head.KeyDigest || release.RequestDigest != head.RequestDigest ||
		release.PlanRoot != head.PlanRoot || release.CatalogGeneration != head.CatalogGeneration ||
		release.PinID != head.PinID || release.PinDigest != head.PinDigest ||
		release.RouteSchemaCertificateDigest != head.RouteSchemaCertificateDigest {
		return fail(nil)
	}
	if release.Phase != requestledger.SchemaPinReleased || release.Revision != prepared.Revision+1 ||
		release.CertificateDigest != head.SchemaPinReleaseCertificateDigest {
		return fail(nil)
	}
	if _, err := requestledger.NewTerminal(head, prepared, release, head.Revision+1); err != nil {
		return fail(err)
	}

	command, err := executionpin.OpenCommand(release.Command)
	binding, bindingErr := BuildDurableRequestExecutionPinBinding(execution)
	if err != nil || bindingErr != nil || command.Operation != executionpin.OperationRelease ||
		command.Binding != binding || command.PrepareTerminalDigest != executionpin.Digest(prepared.PreparedDigest) {
		return fail(errors.Join(err, bindingErr))
	}
	proof, err := executionpin.OpenCompletion(release.Completion)
	authority, authorityErr := executionpin.ReleaseAuthorityDigest(release.Command)
	if err != nil || authorityErr != nil || executionpin.ValidateReleasePair(command, proof, authority) != nil {
		return fail(err)
	}

	return command, nil
}
