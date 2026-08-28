// Package schemainstall provides the shard-local durable half of a replicated
// schema rollout. It prepares an immutable relation bundle away from the
// serving state machine, returns an exact receipt, and refuses activation until
// the catalog authority supplies a byte-exact authorization certificate.
package schemainstall

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
)

var (
	ErrInvalid        = errors.New("schemainstall: invalid request")
	ErrConflict       = errors.New("schemainstall: request conflicts with durable state")
	ErrMissing        = errors.New("schemainstall: operation is missing")
	ErrBound          = errors.New("schemainstall: configured bound reached")
	ErrOutcomeUnknown = errors.New("schemainstall: outcome is unknown")
	ErrClosed         = errors.New("schemainstall: installer is closed")
)

const (
	AbsoluteMaxRecords     = 4096
	AbsoluteMaxBundleBytes = 64 << 20
)

var contractDomain = [...]byte{
	0x56, 0x44, 0x42, 0x2d, 0x53, 0x43, 0x48, 0x45,
	0x4d, 0x41, 0x2d, 0x52, 0x4f, 0x4c, 0x4c, 0x4f,
	0x55, 0x54, 0x2d, 0x43, 0x4f, 0x4e, 0x54, 0x52,
	0x41, 0x43, 0x54, 0x2d, 0x52, 0x46, 0x33, 0x01,
}

// ContractDigest is shared with the replicated catalog rollout grammar. A
// rolling fleet with a different installer contract fails prepare rather than
// interpreting the same relation artifact differently.
func ContractDigest() [32]byte { return sha256.Sum256(contractDomain[:]) }

type State uint8

const (
	StatePrepared State = iota + 1
	StateAuthorized
	StateActive
	StateDrained
)

func (state State) valid() bool { return state >= StatePrepared && state <= StateDrained }

// Request binds one opaque bundle to the complete old/new generation cut. The
// bundle itself is interpreted only by Backend.Prepare; its digest and the
// separate ApplyContractDigest make substitution impossible at activation.
type Request struct {
	Operation                  [32]byte
	Group                      raftmember.GroupKey
	AllocationGeneration       distribution.ShardAllocationGeneration
	FromSchemaGeneration       uint64
	FromRelationManifestDigest replication.Digest
	ToSchemaGeneration         uint64
	ToRelationManifestDigest   replication.Digest
	ApplyContractDigest        [32]byte
	BundleDigest               [32]byte
	BundleBytes                uint64
}

// Authorization is the replicated catalog's exact permission to make a
// prepared generation visible. TargetCatalogDigest binds the complete catalog
// document; PreparedGroupRoot binds every shard receipt in that rollout.
type Authorization struct {
	Operation               [32]byte
	TargetCatalogGeneration uint64
	TargetCatalogDigest     [32]byte
	PreparedGroupCount      uint64
	PreparedGroupRoot       [32]byte
	ContractDigest          [32]byte
}

// DrainProof is issued only after the target catalog is durably active and all
// old-generation execution pins covered by ReleasedExecutionPinRoot are gone.
// Keeping this separate from activation authorization prevents an installer
// from reclaiming the old generation in the publish-before-response window.
type DrainProof struct {
	Operation                     [32]byte
	TargetCatalogGeneration       uint64
	TargetCatalogDigest           [32]byte
	ActivationAuthorizationDigest [32]byte
	CompletedOperationDigest      [32]byte
	ReleasedExecutionPinRoot      [32]byte
}

// Receipt is safe to translate directly into gateway.SchemaRolloutPreparedGroup.
// InstallationDigest is produced from the durable prepared artifact, not from
// the request bytes alone.
type Receipt struct {
	Group                      raftmember.GroupKey
	AllocationGeneration       distribution.ShardAllocationGeneration
	FromSchemaGeneration       uint64
	FromRelationManifestDigest replication.Digest
	ToSchemaGeneration         uint64
	ToRelationManifestDigest   replication.Digest
	InstallationDigest         [32]byte
	ContractDigest             [32]byte
}

type Record struct {
	Request       Request
	Revision      uint64
	State         State
	Installation  [32]byte
	Authorization Authorization
	DrainProof    DrainProof
}

func validRequest(request Request) bool {
	return request.Operation != ([32]byte{}) && request.Group != (raftmember.GroupKey{}) &&
		request.AllocationGeneration != 0 && request.FromSchemaGeneration != 0 &&
		request.ToSchemaGeneration > request.FromSchemaGeneration &&
		request.FromRelationManifestDigest != (replication.Digest{}) &&
		request.ToRelationManifestDigest != (replication.Digest{}) &&
		request.FromRelationManifestDigest != request.ToRelationManifestDigest &&
		request.ApplyContractDigest != ([32]byte{}) && request.BundleDigest != ([32]byte{}) &&
		request.BundleBytes != 0 && request.BundleBytes <= AbsoluteMaxBundleBytes
}

func validAuthorization(authorization Authorization, operation [32]byte) bool {
	return authorization.Operation == operation && operation != ([32]byte{}) &&
		authorization.TargetCatalogGeneration != 0 &&
		authorization.TargetCatalogDigest != ([32]byte{}) &&
		authorization.PreparedGroupCount != 0 &&
		authorization.PreparedGroupRoot != ([32]byte{}) &&
		authorization.ContractDigest == ContractDigest()
}

func validDrainProof(proof DrainProof, authorization Authorization) bool {
	return validAuthorization(authorization, proof.Operation) && proof.Operation == authorization.Operation &&
		proof.TargetCatalogGeneration == authorization.TargetCatalogGeneration &&
		proof.TargetCatalogDigest == authorization.TargetCatalogDigest &&
		proof.ActivationAuthorizationDigest == AuthorizationDigest(authorization) &&
		proof.CompletedOperationDigest != ([32]byte{}) &&
		proof.ReleasedExecutionPinRoot != ([32]byte{})
}

// AuthorizationDigest binds every field of the activation certificate for a
// later drain proof without retaining another copy in the catalog operation.
func AuthorizationDigest(authorization Authorization) [32]byte {
	if !validAuthorization(authorization, authorization.Operation) {
		return [32]byte{}
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("vibedb/schema-rollout/activation-authorization\x00"))
	_, _ = hasher.Write(authorization.Operation[:])
	var scalar [8]byte
	binary.BigEndian.PutUint64(scalar[:], authorization.TargetCatalogGeneration)
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(authorization.TargetCatalogDigest[:])
	binary.BigEndian.PutUint64(scalar[:], authorization.PreparedGroupCount)
	_, _ = hasher.Write(scalar[:])
	_, _ = hasher.Write(authorization.PreparedGroupRoot[:])
	_, _ = hasher.Write(authorization.ContractDigest[:])
	var result [32]byte
	hasher.Sum(result[:0])
	return result
}

func validRecord(record Record) bool {
	if !validRequest(record.Request) || record.Revision == 0 || !record.State.valid() ||
		record.Installation == ([32]byte{}) {
		return false
	}
	if record.State == StatePrepared {
		return record.Revision == 1 && record.Authorization == (Authorization{}) && record.DrainProof == (DrainProof{})
	}
	if record.Revision < 2 || !validAuthorization(record.Authorization, record.Request.Operation) {
		return false
	}
	if record.State == StateDrained {
		return validDrainProof(record.DrainProof, record.Authorization)
	}
	return record.DrainProof == (DrainProof{})
}

func receiptFor(record Record) Receipt {
	request := record.Request
	return Receipt{
		Group: request.Group, AllocationGeneration: request.AllocationGeneration,
		FromSchemaGeneration:       request.FromSchemaGeneration,
		FromRelationManifestDigest: request.FromRelationManifestDigest,
		ToSchemaGeneration:         request.ToSchemaGeneration,
		ToRelationManifestDigest:   request.ToRelationManifestDigest,
		InstallationDigest:         record.Installation, ContractDigest: ContractDigest(),
	}
}
