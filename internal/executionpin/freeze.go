package executionpin

// FreezeRelease validates an exact release intent and makes its lease
// irreversible. The caller must publish this replacement and the durable
// release intent atomically. No lease, acquisition, or accounting field moves;
// a frozen record cannot admit more side effects or be recovered/expired.
func FreezeRelease(current Record, release Command) (Record, error) {
	if !current.Valid() || !release.Valid() || release.Operation != OperationRelease ||
		current.Status != StatusActive || current.PinID != release.PinID ||
		current.Binding != release.Binding || !matchesExpectedLease(current, release) ||
		!matchesAcquireCertificate(current, release.AcquireCertificateDigest) ||
		current.PrepareTerminalDigest != (Digest{}) && current.PrepareTerminalDigest != release.PrepareTerminalDigest {
		return Record{}, ErrCorrupt
	}
	current.PrepareTerminalDigest = release.PrepareTerminalDigest
	return current, nil
}
