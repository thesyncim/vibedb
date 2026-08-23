package replicatedstate

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/replication"
)

var logicalCommandDigestDomain = []byte(
	"vibedb/replicated-state/logical-command\x00",
)

// LogicalCommandDigest binds the stable client operation while deliberately
// excluding AckThrough and physical routing fences. A duplicate can therefore
// advance its cumulative acknowledgement and survive a mutable topology fence
// refresh without changing request identity. The exact ordered mutation bytes
// remain bound independently of the caller-minted fingerprint.
func LogicalCommandDigest(command replication.CommandView) [32]byte {
	h := sha256.New()
	_, _ = h.Write(logicalCommandDigestDomain)
	var marker [1]byte
	marker[0] = byte(command.Kind())
	_, _ = h.Write(marker[:])
	_, _ = h.Write(command.ClusterID[:])
	_, _ = h.Write(command.ClusterIncarnation[:])
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(command.Tenant)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(command.Tenant)
	_, _ = h.Write(command.ClientID[:])
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], command.ClientEpoch)
	_, _ = h.Write(scalar[:])
	binary.LittleEndian.PutUint64(scalar[:], command.ClientSequence)
	_, _ = h.Write(scalar[:])
	_, _ = h.Write(command.Fingerprint[:])
	_, _ = h.Write(command.RetryHome[:])
	binary.LittleEndian.PutUint64(length[:], uint64(len(command.Collection)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(command.Collection)
	binary.LittleEndian.PutUint64(scalar[:], uint64(command.MutationCount()))
	_, _ = h.Write(scalar[:])
	mutations := command.Mutations()
	for mutations.Next() {
		mutation := mutations.Mutation()
		marker[0] = byte(mutation.Kind)
		_, _ = h.Write(marker[:])
		binary.LittleEndian.PutUint64(length[:], uint64(len(mutation.Key)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(mutation.Key)
		binary.LittleEndian.PutUint64(length[:], uint64(len(mutation.Value)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(mutation.Value)
	}
	var result [32]byte
	_ = h.Sum(result[:0])
	return result
}

func writeHashFrame(h interface{ Write([]byte) (int, error) }, value []byte) {
	var length [8]byte
	binary.LittleEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write(value)
}
