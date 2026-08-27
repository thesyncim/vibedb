package clusterrestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
)

const catalogActivationBytes = 8 + 232 + sha256.Size

var catalogActivationMagic = [8]byte{'V', 'B', 'R', 'S', 'C', 'A', 'T', 0}

// CatalogProposer synchronously returns the deterministic 32-byte witness
// digest settled by the replicated target catalog. Its caller must exercise
// CapabilityRestoreActivate; backup/topology/membership authority is invalid.
type CatalogProposer interface {
	ProposeRestoreActivation(context.Context, []byte) ([]byte, error)
}

// ReplicatedCatalogPublisher is the controller's target-catalog boundary.
// Publication succeeds only after the proposal result settles the exact
// activation digest.
type ReplicatedCatalogPublisher struct{ Proposer CatalogProposer }

func (publisher ReplicatedCatalogPublisher) Publish(ctx context.Context, witness CatalogWitness) error {
	if ctx == nil || publisher.Proposer == nil {
		return ErrActivation
	}
	command, err := AppendCatalogActivation(nil, witness)
	if err != nil {
		return err
	}
	result, err := publisher.Proposer.ProposeRestoreActivation(ctx, command)
	if err != nil || len(result) != sha256.Size || !bytes.Equal(result, witness.CatalogDigest[:]) {
		return errors.Join(ErrActivation, err)
	}
	return nil
}

func AppendCatalogActivation(dst []byte, witness CatalogWitness) ([]byte, error) {
	if witness.Operation == ([32]byte{}) || witness.CatalogGroup == ([72]byte{}) ||
		witness.GroupsDigest == ([32]byte{}) || witness.TargetPolicyDigest == ([32]byte{}) ||
		witness.TargetCatalogDigest == ([32]byte{}) || witness.CatalogDigest == ([32]byte{}) {
		return dst, ErrActivation
	}
	start := len(dst)
	dst = append(dst, make([]byte, catalogActivationBytes)...)
	raw := dst[start:]
	copy(raw[:8], catalogActivationMagic[:])
	offset := 8
	for _, field := range [][]byte{witness.Operation[:], witness.CatalogGroup[:], witness.GroupsDigest[:],
		witness.TargetPolicyDigest[:], witness.TargetCatalogDigest[:], witness.CatalogDigest[:]} {
		copy(raw[offset:], field)
		offset += len(field)
	}
	digest := sha256.Sum256(raw[:offset])
	copy(raw[offset:], digest[:])
	return dst, nil
}

func OpenCatalogActivation(raw []byte) (CatalogWitness, error) {
	if len(raw) != catalogActivationBytes || [8]byte(raw[:8]) != catalogActivationMagic ||
		sha256.Sum256(raw[:len(raw)-sha256.Size]) != [sha256.Size]byte(raw[len(raw)-sha256.Size:]) {
		return CatalogWitness{}, ErrActivation
	}
	witness := CatalogWitness{}
	offset := 8
	copy(witness.Operation[:], raw[offset:offset+32])
	offset += 32
	copy(witness.CatalogGroup[:], raw[offset:offset+72])
	offset += 72
	copy(witness.GroupsDigest[:], raw[offset:offset+32])
	offset += 32
	copy(witness.TargetPolicyDigest[:], raw[offset:offset+32])
	offset += 32
	copy(witness.TargetCatalogDigest[:], raw[offset:offset+32])
	offset += 32
	copy(witness.CatalogDigest[:], raw[offset:offset+32])
	canonical, err := AppendCatalogActivation(nil, witness)
	if err != nil || !bytes.Equal(canonical, raw) {
		return CatalogWitness{}, errors.Join(ErrActivation, err)
	}
	return witness, nil
}
