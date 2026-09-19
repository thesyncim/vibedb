package executionpin

import "crypto/sha256"

// ReleaseAuthorityDigest binds a co-located ledger release to its complete
// immutable pin binding, authenticated gateway, lease, and prepared result.
// The ledger adapter authenticates the gateway before applying this command.
func ReleaseAuthorityDigest(command []byte) (Digest, error) {
	const domain = "vibedb/execution-pin/ledger-release\x00"
	var framed [len(domain) + CommandBytes]byte
	copy(framed[:], domain)
	view, err := OpenCommand(command)
	if err != nil || view.Operation != OperationRelease {
		return Digest{}, ErrCorrupt
	}
	copy(framed[len(domain):], command)
	return Digest(sha256.Sum256(framed[:])), nil
}
