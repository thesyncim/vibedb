package raftserve

import (
	"github.com/thesyncim/vibedb/internal/executionpin"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/replication"
)

// The outer completion has already been bound to the exact admitted command
// and applied session slot. Validate the fixed proof body as its own grammar,
// including the command authority, rather than treating it as a row count.
func validateExecutionPinSettlement(identity commandIdentity, outer replication.CompletionView) error {
	if outer.ResultFormat != replicatedstate.ResultFormatExecutionPin ||
		outer.ResultLength != executionpin.CompletionBytes || len(outer.InlineResult) != executionpin.CompletionBytes {
		return ErrSettlementResult
	}
	command, err := executionpin.OpenCommand(identity.executionPin)
	proof, proofErr := executionpin.OpenCompletion(outer.InlineResult)
	if err != nil || proofErr != nil || proof.Operation != command.Operation {
		return ErrSettlementResult
	}
	if outer.ResultCode != replicatedstate.ResultApplied {
		switch outer.ResultCode {
		case replicatedstate.ResultIndexConflict, replicatedstate.ResultIntentBusy,
			replicatedstate.ResultTargetBound, replicatedstate.ResultStaleFence:
			if !proof.Found {
				return nil
			}
		}
		return ErrSettlementResult
	}
	authority := executionpin.Digest(identity.executionPinAuthority)
	if !proof.Found || authority == (executionpin.Digest{}) {
		return ErrSettlementResult
	}
	if proof.Acquire != (executionpin.AcquireCertificate{}) &&
		(proof.Acquire.PinID != command.PinID || proof.Acquire.Binding != command.Binding) {
		return ErrSettlementResult
	}
	switch command.Operation {
	case executionpin.OperationAcquire:
		if proof.Acquire.Applied != outer.AppliedSequence || executionpin.ValidateAcquirePair(command, proof, authority) != nil {
			return ErrSettlementResult
		}
	case executionpin.OperationRenew, executionpin.OperationRecover:
		if proof.Status != executionpin.StatusActive || proof.Lease.AuthorityDigest != authority ||
			proof.Lease.AcquireCertificateDigest != command.AcquireCertificateDigest ||
			proof.Lease.Controller != command.NextController || proof.Lease.ControllerEpoch != command.NextControllerEpoch ||
			proof.Lease.Applied != outer.AppliedSequence || command.ExpectedLeaseRevision == ^uint64(0) ||
			proof.Lease.Revision != command.ExpectedLeaseRevision+1 ||
			command.NextLeaseSpan > ^uint64(0)-outer.AppliedSequence ||
			proof.Lease.LeaseAppliedThrough != outer.AppliedSequence+command.NextLeaseSpan {
			return ErrSettlementResult
		}
	case executionpin.OperationRelease:
		if proof.Terminal.Applied != outer.AppliedSequence || executionpin.ValidateReleasePair(command, proof, authority) != nil {
			return ErrSettlementResult
		}
	case executionpin.OperationExpire:
		terminal := proof.Terminal
		if proof.Status != executionpin.StatusExpired || terminal.PinID != command.PinID ||
			terminal.RequestKeyDigest != command.Binding.RequestKeyDigest || terminal.AuthorityDigest != authority ||
			terminal.Applied != outer.AppliedSequence || terminal.Controller != command.ExpectedController ||
			terminal.ControllerEpoch != command.ExpectedControllerEpoch ||
			terminal.ExpectedLeaseAppliedThrough != command.ExpectedLeaseAppliedThrough ||
			(proof.Lease != (executionpin.LeaseCertificate{}) &&
				(proof.Lease.Controller != command.ExpectedController || proof.Lease.ControllerEpoch != command.ExpectedControllerEpoch ||
					proof.Lease.LeaseAppliedThrough != command.ExpectedLeaseAppliedThrough || proof.Lease.Revision != command.ExpectedLeaseRevision)) {
			return ErrSettlementResult
		}
	default:
		return ErrSettlementResult
	}
	return nil
}
