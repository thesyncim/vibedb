package executionpin

// ValidateAcquirePair semantically authenticates an acquired logical pin and
// its exact acquisition authority. Callers derive expectedAuthority from the
// already authenticated outer replicated command.
func ValidateAcquirePair(
	command Command,
	completion Completion,
	expectedAuthority Digest,
) error {
	if !command.Valid() || command.Operation != OperationAcquire ||
		expectedAuthority == (Digest{}) || !completion.Valid() ||
		completion.Operation != OperationAcquire || !completion.Found ||
		completion.Status != StatusActive || completion.Acquire.PinID != command.PinID ||
		completion.Acquire.Binding != command.Binding ||
		completion.Acquire.AuthorityDigest != expectedAuthority ||
		completion.Lease.AuthorityDigest != expectedAuthority ||
		completion.Acquire.Controller != command.NextController ||
		completion.Acquire.ControllerEpoch != command.NextControllerEpoch ||
		completion.Acquire.LeaseAppliedThrough != completion.Acquire.Applied+command.NextLeaseSpan ||
		completion.Lease.Controller != command.NextController ||
		completion.Lease.ControllerEpoch != command.NextControllerEpoch ||
		completion.Lease.LeaseAppliedThrough != completion.Lease.Applied+command.NextLeaseSpan ||
		completion.Lease.Revision != 1 ||
		completion.Lease.Applied != completion.Acquire.Applied {
		return ErrCorrupt
	}
	acquireDigest, err := AcquireCertificateDigest(completion.Acquire)
	if err != nil || completion.Lease.AcquireCertificateDigest != acquireDigest {
		return ErrCorrupt
	}
	return nil
}

// ValidateReleasePair semantically authenticates the exact inner command and
// transferable proof for a successful PrepareTerminal-bound release. Callers
// separately authenticate the outer replicated command/completion envelope,
// derive expectedAuthority from that outer command, then compare Binding and
// PrepareTerminalDigest with their own durable request state.
func ValidateReleasePair(
	command Command,
	completion Completion,
	expectedAuthority Digest,
) error {
	if !command.Valid() || command.Operation != OperationRelease ||
		expectedAuthority == (Digest{}) ||
		!completion.Valid() || completion.Operation != OperationRelease ||
		!completion.Found || completion.Status != StatusReleased ||
		completion.Acquire.PinID != command.PinID ||
		completion.Acquire.Binding != command.Binding ||
		completion.Lease.PinID != command.PinID ||
		completion.Terminal.PinID != command.PinID ||
		completion.Lease.Controller != command.ExpectedController ||
		completion.Lease.ControllerEpoch != command.ExpectedControllerEpoch ||
		completion.Lease.LeaseAppliedThrough != command.ExpectedLeaseAppliedThrough ||
		completion.Lease.Revision != command.ExpectedLeaseRevision ||
		completion.Terminal.Controller != command.ExpectedController ||
		completion.Terminal.ControllerEpoch != command.ExpectedControllerEpoch ||
		completion.Terminal.ExpectedLeaseAppliedThrough != command.ExpectedLeaseAppliedThrough ||
		completion.Terminal.AuthorityDigest != expectedAuthority ||
		completion.Terminal.PrepareTerminalDigest != command.PrepareTerminalDigest ||
		completion.Terminal.RequestKeyDigest != command.Binding.RequestKeyDigest {
		return ErrCorrupt
	}
	acquireDigest, err := AcquireCertificateDigest(completion.Acquire)
	if err != nil || acquireDigest != command.AcquireCertificateDigest ||
		completion.Lease.AcquireCertificateDigest != acquireDigest ||
		completion.Terminal.AcquireCertificateDigest != acquireDigest {
		return ErrCorrupt
	}
	leaseDigest, err := LeaseCertificateDigest(completion.Lease)
	if err != nil || completion.Terminal.LeaseCertificateDigest != leaseDigest {
		return ErrCorrupt
	}
	return nil
}
