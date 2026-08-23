package replicatedstate

import (
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/replication"
)

// sessionOpenFor returns the canonical empty-session request for the same
// stable identity and binding as prototype. Its fingerprint is deliberately
// independent from the first user request.
func sessionOpenFor(prototype replication.Command) replication.Command {
	prototype.Kind = replication.CommandSessionOpen
	prototype.ClientEpoch = 0
	prototype.ClientSequence = 1
	prototype.AckThrough = 0
	prototype.Mutations = nil
	seed := make([]byte, 0, len("replicatedstate/test-session-open/")+len(prototype.Tenant)+len(prototype.ClientID))
	seed = append(seed, "replicatedstate/test-session-open/"...)
	seed = append(seed, prototype.Tenant...)
	seed = append(seed, prototype.ClientID[:]...)
	prototype.Fingerprint = sha256.Sum256(seed)
	return prototype
}

// applySessionOpen applies one open at index and proves that its retained
// completion returns the apply-index token.
func applySessionOpen(
	t testing.TB,
	machine *Machine,
	index uint64,
	prototype replication.Command,
) (replication.Command, []byte, uint64) {
	t.Helper()
	command := sessionOpenFor(prototype)
	encoded := encodeCommand(t, command)
	if err := machine.AdmitCommand(encoded); err != nil {
		t.Fatalf("admit session open at %d: %v", index, err)
	}
	publication, err := machine.ApplyNormal(normalMeta(index), encoded)
	if err != nil || publication.Applied != index {
		t.Fatalf("apply session open at %d = %+v, %v", index, publication, err)
	}
	lookup, err := machine.LookupCompletion(encoded)
	if err != nil {
		t.Fatalf("lookup session open at %d: %v", index, err)
	}
	completion, err := replication.OpenCompletion(lookup.Bytes)
	if err != nil || completion.ResultCode != ResultSessionOpened ||
		completion.ClientEpoch != index || completion.ClientSequence != 1 ||
		completion.AppliedSequence != index {
		t.Fatalf("session open completion at %d = %+v, %v", index, completion, err)
	}
	return command, encoded, index
}
