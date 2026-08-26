// Package executionpin defines the string-free logical catalog/schema pin
// contract used by durable requests. It owns no transport, clock, or storage;
// replicas apply its fixed commands in their existing catalog Raft group.
//
// This format is unreleased. The clockless grammar intentionally has distinct
// domains and magic from the superseded wall-clock prototype; there is no
// legacy decoder that could reinterpret a timestamp as replicated authority.
package executionpin

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const MaxRetainedRecords = uint64(1 << 20)

var (
	ErrCorrupt = errors.New("executionpin: corrupt canonical record")
	ErrBound   = errors.New("executionpin: bounded state exhausted")

	pinIdentityDomain = []byte("vibedb/logical-execution-pin/identity\x00")
	bindingDomain     = []byte("vibedb/logical-execution-pin/binding\x00")
)

type ID [16]byte
type Digest [32]byte
type PinID [32]byte

// Binding is the immutable logical program authority. It intentionally omits
// physical placement: short-lived route gates bind those attempts separately.
type Binding struct {
	RequestKeyDigest          Digest
	RequestDigest             Digest
	CatalogGeneration         uint64
	SchemaManifestDigest      Digest
	TransactionManifestDigest Digest
	ParticipantAuthorityRoot  Digest
	ParticipantCount          uint64
	ExecutionContractDigest   Digest
	LedgerHomeGroup           ID
}

func (binding Binding) Valid() bool {
	return binding.RequestKeyDigest != (Digest{}) && binding.RequestDigest != (Digest{}) &&
		binding.CatalogGeneration != 0 &&
		binding.SchemaManifestDigest != (Digest{}) &&
		binding.TransactionManifestDigest != (Digest{}) &&
		binding.ParticipantAuthorityRoot != (Digest{}) && binding.ParticipantCount != 0 &&
		binding.ExecutionContractDigest != (Digest{}) && binding.LedgerHomeGroup != (ID{})
}

// DerivePinID returns the sole deterministic PinID grammar. Controller and
// lease fields are excluded so a recovery controller can rederive the same
// identity from the durable logical pin intent.
func DerivePinID(binding Binding) (PinID, error) {
	if !binding.Valid() {
		return PinID{}, ErrCorrupt
	}
	var material [len("vibedb/logical-execution-pin/identity\x00") + bindingBytes]byte
	cursor := copy(material[:], pinIdentityDomain)
	appendBinding(material[cursor:cursor], binding)
	return PinID(sha256.Sum256(material[:])), nil
}

// BindingDigest is the compact contract digest fed into short-lived physical
// route-gate bindings and the durable request recipe.
func BindingDigest(binding Binding) (Digest, error) {
	if !binding.Valid() {
		return Digest{}, ErrCorrupt
	}
	var material [len("vibedb/logical-execution-pin/binding\x00") + bindingBytes]byte
	cursor := copy(material[:], bindingDomain)
	appendBinding(material[cursor:cursor], binding)
	return Digest(sha256.Sum256(material[:])), nil
}

const bindingBytes = 224

func appendBinding(dst []byte, binding Binding) []byte {
	dst = append(dst, binding.RequestKeyDigest[:]...)
	dst = append(dst, binding.RequestDigest[:]...)
	dst = binary.LittleEndian.AppendUint64(dst, binding.CatalogGeneration)
	dst = append(dst, binding.SchemaManifestDigest[:]...)
	dst = append(dst, binding.TransactionManifestDigest[:]...)
	dst = append(dst, binding.ParticipantAuthorityRoot[:]...)
	dst = binary.LittleEndian.AppendUint64(dst, binding.ParticipantCount)
	dst = append(dst, binding.ExecutionContractDigest[:]...)
	return append(dst, binding.LedgerHomeGroup[:]...)
}

func openBinding(raw []byte) (Binding, bool) {
	if len(raw) != bindingBytes {
		return Binding{}, false
	}
	var binding Binding
	copy(binding.RequestKeyDigest[:], raw[0:32])
	copy(binding.RequestDigest[:], raw[32:64])
	binding.CatalogGeneration = binary.LittleEndian.Uint64(raw[64:72])
	copy(binding.SchemaManifestDigest[:], raw[72:104])
	copy(binding.TransactionManifestDigest[:], raw[104:136])
	copy(binding.ParticipantAuthorityRoot[:], raw[136:168])
	binding.ParticipantCount = binary.LittleEndian.Uint64(raw[168:176])
	copy(binding.ExecutionContractDigest[:], raw[176:208])
	copy(binding.LedgerHomeGroup[:], raw[208:224])
	return binding, binding.Valid()
}

func allZero(raw []byte) bool {
	for _, value := range raw {
		if value != 0 {
			return false
		}
	}
	return true
}
