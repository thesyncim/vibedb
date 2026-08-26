package schemainstall

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"

	"github.com/thesyncim/vibedb/distribution"
)

const recordBytes = 636

var (
	recordMagic = [8]byte{'V', 'B', 'S', 'C', 'H', 'I', 'N', 0}
	recordCRC   = crc32.MakeTable(crc32.Castagnoli)
)

func appendRecord(dst []byte, record Record) ([]byte, error) {
	if !validRecord(record) {
		return dst, ErrInvalid
	}
	start := len(dst)
	dst = append(dst, make([]byte, recordBytes)...)
	raw := dst[start:]
	copy(raw[:8], recordMagic[:])
	raw[8] = byte(record.State)
	binary.LittleEndian.PutUint64(raw[16:24], record.Revision)
	at := 24
	at = putRequest(raw, at, record.Request)
	copy(raw[at:at+32], record.Installation[:])
	at += 32
	at = putAuthorization(raw, at, record.Authorization)
	at = putDrainProof(raw, at, record.DrainProof)
	if at != recordBytes-4 {
		return dst[:start], ErrInvalid
	}
	binary.LittleEndian.PutUint32(raw[recordBytes-4:], crc32.Checksum(raw[:recordBytes-4], recordCRC))
	return dst, nil
}

func openRecord(raw []byte) (Record, error) {
	if len(raw) != recordBytes || !bytes.Equal(raw[:8], recordMagic[:]) ||
		crc32.Checksum(raw[:recordBytes-4], recordCRC) != binary.LittleEndian.Uint32(raw[recordBytes-4:]) {
		return Record{}, ErrInvalid
	}
	record := Record{State: State(raw[8]), Revision: binary.LittleEndian.Uint64(raw[16:24])}
	if raw[9] != 0 || binary.LittleEndian.Uint16(raw[10:12]) != 0 || binary.LittleEndian.Uint32(raw[12:16]) != 0 {
		return Record{}, ErrInvalid
	}
	at := 24
	record.Request, at = getRequest(raw, at)
	copy(record.Installation[:], raw[at:at+32])
	at += 32
	record.Authorization, at = getAuthorization(raw, at)
	record.DrainProof, at = getDrainProof(raw, at)
	if at != recordBytes-4 || !validRecord(record) {
		return Record{}, ErrInvalid
	}
	canonical, err := appendRecord(nil, record)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Record{}, ErrInvalid
	}
	return record, nil
}

func putRequest(raw []byte, at int, request Request) int {
	copy(raw[at:at+32], request.Operation[:])
	at += 32
	copy(raw[at:at+16], request.Group.ClusterID[:])
	at += 16
	copy(raw[at:at+16], request.Group.ClusterIncarnation[:])
	at += 16
	binary.LittleEndian.PutUint64(raw[at:at+8], request.Group.TopologyRecoveryEpoch)
	at += 8
	copy(raw[at:at+16], request.Group.ShardIncarnation[:])
	at += 16
	copy(raw[at:at+16], request.Group.GroupID[:])
	at += 16
	values := [...]uint64{uint64(request.AllocationGeneration), request.FromSchemaGeneration, request.ToSchemaGeneration, request.BundleBytes}
	for _, value := range values {
		binary.LittleEndian.PutUint64(raw[at:at+8], value)
		at += 8
	}
	copy(raw[at:at+32], request.FromRelationManifestDigest[:])
	at += 32
	copy(raw[at:at+32], request.ToRelationManifestDigest[:])
	at += 32
	copy(raw[at:at+32], request.ApplyContractDigest[:])
	at += 32
	copy(raw[at:at+32], request.BundleDigest[:])
	at += 32
	return at
}

func getRequest(raw []byte, at int) (Request, int) {
	var request Request
	copy(request.Operation[:], raw[at:at+32])
	at += 32
	copy(request.Group.ClusterID[:], raw[at:at+16])
	at += 16
	copy(request.Group.ClusterIncarnation[:], raw[at:at+16])
	at += 16
	request.Group.TopologyRecoveryEpoch = binary.LittleEndian.Uint64(raw[at : at+8])
	at += 8
	copy(request.Group.ShardIncarnation[:], raw[at:at+16])
	at += 16
	copy(request.Group.GroupID[:], raw[at:at+16])
	at += 16
	request.AllocationGeneration = distribution.ShardAllocationGeneration(binary.LittleEndian.Uint64(raw[at : at+8]))
	at += 8
	request.FromSchemaGeneration = binary.LittleEndian.Uint64(raw[at : at+8])
	at += 8
	request.ToSchemaGeneration = binary.LittleEndian.Uint64(raw[at : at+8])
	at += 8
	request.BundleBytes = binary.LittleEndian.Uint64(raw[at : at+8])
	at += 8
	copy(request.FromRelationManifestDigest[:], raw[at:at+32])
	at += 32
	copy(request.ToRelationManifestDigest[:], raw[at:at+32])
	at += 32
	copy(request.ApplyContractDigest[:], raw[at:at+32])
	at += 32
	copy(request.BundleDigest[:], raw[at:at+32])
	at += 32
	return request, at
}

func putAuthorization(raw []byte, at int, authorization Authorization) int {
	copy(raw[at:at+32], authorization.Operation[:])
	at += 32
	binary.LittleEndian.PutUint64(raw[at:at+8], authorization.TargetCatalogGeneration)
	at += 8
	copy(raw[at:at+32], authorization.TargetCatalogDigest[:])
	at += 32
	binary.LittleEndian.PutUint64(raw[at:at+8], authorization.PreparedGroupCount)
	at += 8
	copy(raw[at:at+32], authorization.PreparedGroupRoot[:])
	at += 32
	copy(raw[at:at+32], authorization.ContractDigest[:])
	at += 32
	return at
}

func getAuthorization(raw []byte, at int) (Authorization, int) {
	var authorization Authorization
	copy(authorization.Operation[:], raw[at:at+32])
	at += 32
	authorization.TargetCatalogGeneration = binary.LittleEndian.Uint64(raw[at : at+8])
	at += 8
	copy(authorization.TargetCatalogDigest[:], raw[at:at+32])
	at += 32
	authorization.PreparedGroupCount = binary.LittleEndian.Uint64(raw[at : at+8])
	at += 8
	copy(authorization.PreparedGroupRoot[:], raw[at:at+32])
	at += 32
	copy(authorization.ContractDigest[:], raw[at:at+32])
	at += 32
	return authorization, at
}

func putDrainProof(raw []byte, at int, proof DrainProof) int {
	copy(raw[at:at+32], proof.Operation[:])
	at += 32
	binary.LittleEndian.PutUint64(raw[at:at+8], proof.TargetCatalogGeneration)
	at += 8
	copy(raw[at:at+32], proof.TargetCatalogDigest[:])
	at += 32
	copy(raw[at:at+32], proof.ActivationAuthorizationDigest[:])
	at += 32
	copy(raw[at:at+32], proof.CompletedOperationDigest[:])
	at += 32
	copy(raw[at:at+32], proof.ReleasedExecutionPinRoot[:])
	at += 32
	return at
}

func getDrainProof(raw []byte, at int) (DrainProof, int) {
	var proof DrainProof
	copy(proof.Operation[:], raw[at:at+32])
	at += 32
	proof.TargetCatalogGeneration = binary.LittleEndian.Uint64(raw[at : at+8])
	at += 8
	copy(proof.TargetCatalogDigest[:], raw[at:at+32])
	at += 32
	copy(proof.ActivationAuthorizationDigest[:], raw[at:at+32])
	at += 32
	copy(proof.CompletedOperationDigest[:], raw[at:at+32])
	at += 32
	copy(proof.ReleasedExecutionPinRoot[:], raw[at:at+32])
	at += 32
	return proof, at
}
