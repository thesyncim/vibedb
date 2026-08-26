// Package executionpin defines the string-free logical catalog/schema pin
// contract used by durable requests. It owns no transport, clock, or storage;
// replicas apply its fixed commands in their existing catalog Raft group.
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

	pinIdentityDomain = []byte("vibedb/execution-pin/identity\x00")
	bindingDomain     = []byte("vibedb/execution-pin/binding\x00")
)

type ID [16]byte
type Digest [32]byte
type PinID [32]byte

// Binding is the immutable logical program authority. It intentionally omits
// physical placement: short-lived route gates bind those attempts separately.
type Binding struct {
	RequestKeyDigest        Digest
	RequestDigest           Digest
	CatalogGeneration       uint64
	SchemaGeneration        uint64
	SchemaManifestDigest    Digest
	SchemaCertificateDigest Digest
	LogicalGroup            ID
	LogicalRange            ID
	MutationDigest          Digest
}

func (binding Binding) Valid() bool {
	return binding.RequestKeyDigest != (Digest{}) && binding.RequestDigest != (Digest{}) &&
		binding.CatalogGeneration != 0 && binding.SchemaGeneration != 0 &&
		binding.SchemaManifestDigest != (Digest{}) &&
		binding.SchemaCertificateDigest != (Digest{}) &&
		binding.LogicalGroup != (ID{}) && binding.LogicalRange != (ID{}) &&
		binding.MutationDigest != (Digest{})
}

// DerivePinID returns the sole deterministic PinID grammar. Controller and
// lease fields are excluded so a recovery controller can rederive the same
// identity from the durable logical pin intent.
func DerivePinID(binding Binding) (PinID, error) {
	if !binding.Valid() {
		return PinID{}, ErrCorrupt
	}
	var material [len("vibedb/execution-pin/identity\x00") + bindingBytes]byte
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
	var material [len("vibedb/execution-pin/binding\x00") + bindingBytes]byte
	cursor := copy(material[:], bindingDomain)
	appendBinding(material[cursor:cursor], binding)
	return Digest(sha256.Sum256(material[:])), nil
}

const bindingBytes = 208

func appendBinding(dst []byte, binding Binding) []byte {
	dst = append(dst, binding.RequestKeyDigest[:]...)
	dst = append(dst, binding.RequestDigest[:]...)
	dst = binary.LittleEndian.AppendUint64(dst, binding.CatalogGeneration)
	dst = binary.LittleEndian.AppendUint64(dst, binding.SchemaGeneration)
	dst = append(dst, binding.SchemaManifestDigest[:]...)
	dst = append(dst, binding.SchemaCertificateDigest[:]...)
	dst = append(dst, binding.LogicalGroup[:]...)
	dst = append(dst, binding.LogicalRange[:]...)
	return append(dst, binding.MutationDigest[:]...)
}

func openBinding(raw []byte) (Binding, bool) {
	if len(raw) != bindingBytes {
		return Binding{}, false
	}
	var binding Binding
	copy(binding.RequestKeyDigest[:], raw[0:32])
	copy(binding.RequestDigest[:], raw[32:64])
	binding.CatalogGeneration = binary.LittleEndian.Uint64(raw[64:72])
	binding.SchemaGeneration = binary.LittleEndian.Uint64(raw[72:80])
	copy(binding.SchemaManifestDigest[:], raw[80:112])
	copy(binding.SchemaCertificateDigest[:], raw[112:144])
	copy(binding.LogicalGroup[:], raw[144:160])
	copy(binding.LogicalRange[:], raw[160:176])
	copy(binding.MutationDigest[:], raw[176:208])
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
