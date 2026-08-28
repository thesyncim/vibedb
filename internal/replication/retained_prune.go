package replication

import (
	"encoding/binary"

	"github.com/thesyncim/vibedb/distribution"
)

const (
	RetainedPruneProofBytes = 256
	retainedPruneProofBytes = RetainedPruneProofBytes
)

// RetainedPruneProof binds one bounded physical cleanup batch to the exact
// split operation, certified terminal cut, and already-narrowed source range.
// The outer command binds the live shard/group/generation fence; BatchDigest
// must also be the command fingerprint.
type RetainedPruneProof struct {
	OperationDigest   Digest
	CertificateDigest Digest
	BatchDigest       Digest
	DataChainDigest   Digest
	EntryDigest       Digest
	BaseDigest        Digest
	CutApplied        uint64
	CutTerm           uint64
	OwnershipEpoch    uint64
	RoutingVersion    uint64
	RouteGeneration   uint64
	RetainedRange     distribution.KeyRange
}

func (proof RetainedPruneProof) Valid() bool {
	return proof.OperationDigest != (Digest{}) && proof.CertificateDigest != (Digest{}) &&
		proof.BatchDigest != (Digest{}) && proof.DataChainDigest != (Digest{}) &&
		proof.EntryDigest != (Digest{}) && proof.BaseDigest != (Digest{}) &&
		proof.CutApplied != 0 && proof.CutTerm != 0 && proof.OwnershipEpoch != 0 &&
		proof.RoutingVersion != 0 && proof.RouteGeneration != 0 && proof.RetainedRange.Valid()
}

func putRetainedPruneProof(frame []byte, proof RetainedPruneProof) {
	copy(frame[0:32], proof.OperationDigest[:])
	copy(frame[32:64], proof.CertificateDigest[:])
	copy(frame[64:96], proof.BatchDigest[:])
	copy(frame[96:128], proof.DataChainDigest[:])
	copy(frame[128:160], proof.EntryDigest[:])
	copy(frame[160:192], proof.BaseDigest[:])
	binary.LittleEndian.PutUint64(frame[192:200], proof.CutApplied)
	binary.LittleEndian.PutUint64(frame[200:208], proof.CutTerm)
	binary.LittleEndian.PutUint64(frame[208:216], proof.OwnershipEpoch)
	binary.LittleEndian.PutUint64(frame[216:224], proof.RoutingVersion)
	binary.LittleEndian.PutUint64(frame[224:232], proof.RouteGeneration)
	copy(frame[232:240], proof.RetainedRange.Start[:])
	copy(frame[240:248], proof.RetainedRange.End.Point[:])
	if proof.RetainedRange.End.Max {
		frame[248] = 1
	}
}

func openRetainedPruneProof(raw []byte) (RetainedPruneProof, bool) {
	if len(raw) != retainedPruneProofBytes || raw[248] > 1 || !allZero(raw[249:]) {
		return RetainedPruneProof{}, false
	}
	var proof RetainedPruneProof
	copy(proof.OperationDigest[:], raw[0:32])
	copy(proof.CertificateDigest[:], raw[32:64])
	copy(proof.BatchDigest[:], raw[64:96])
	copy(proof.DataChainDigest[:], raw[96:128])
	copy(proof.EntryDigest[:], raw[128:160])
	copy(proof.BaseDigest[:], raw[160:192])
	proof.CutApplied = binary.LittleEndian.Uint64(raw[192:200])
	proof.CutTerm = binary.LittleEndian.Uint64(raw[200:208])
	proof.OwnershipEpoch = binary.LittleEndian.Uint64(raw[208:216])
	proof.RoutingVersion = binary.LittleEndian.Uint64(raw[216:224])
	proof.RouteGeneration = binary.LittleEndian.Uint64(raw[224:232])
	copy(proof.RetainedRange.Start[:], raw[232:240])
	copy(proof.RetainedRange.End.Point[:], raw[240:248])
	proof.RetainedRange.End.Max = raw[248] == 1
	return proof, proof.Valid()
}
