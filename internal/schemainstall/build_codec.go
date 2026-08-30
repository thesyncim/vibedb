package schemainstall

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
	"github.com/thesyncim/vibejson"
)

const (
	buildRequestBytes          = 208
	buildResponseBytes         = 56
	maxBuildReceiptBytes       = 32 << 20
	maxBuildResumeReceiptBytes = maxBuildReceiptBytes + sqldriver.ReplicatedChildSchemaMaxBytes + buildRequestBytes + 24
)

var buildRequestMagic = [8]byte{'V', 'B', 'S', 'B', 'R', 'E', 'Q', 1}
var buildResumeRequestMagic = [8]byte{'V', 'B', 'S', 'B', 'R', 'S', 'M', 1}
var buildResponseMagic = [8]byte{'V', 'B', 'S', 'B', 'R', 'E', 'S', 1}

func BuildRequestDiscriminator() [8]byte       { return buildRequestMagic }
func BuildResumeRequestDiscriminator() [8]byte { return buildResumeRequestMagic }

// BuildRequest binds the source cut before target storage identities exist.
// SQL follows the fixed header and is read only after peer authorization.
// This is not an activation request; callers must hold the exclusive route
// gate and retain the exact receipt before preparing a schema rollout.
type BuildRequest struct {
	Resume                     bool
	Operation                  [32]byte
	Group                      raftmember.GroupKey
	AllocationGeneration       distribution.ShardAllocationGeneration
	FromSchemaGeneration       uint64
	FromRelationManifestDigest replication.Digest
	SourceApplied              uint64
	SQLBytes                   uint64
	SQLDigest                  [32]byte
}

func validBuildRequest(r BuildRequest) bool {
	if r.Resume {
		return r.Operation != ([32]byte{}) && r.Group.ClusterID != ([16]byte{}) &&
			r.Group.ClusterIncarnation != ([16]byte{}) && r.Group.TopologyRecoveryEpoch != 0 &&
			r.Group.ShardIncarnation != ([16]byte{}) && r.Group.GroupID != ([16]byte{}) &&
			r.AllocationGeneration == 0 && r.FromSchemaGeneration == 0 &&
			r.FromRelationManifestDigest == (replication.Digest{}) && r.SourceApplied == 0 &&
			r.SQLBytes == 0 && r.SQLDigest == ([32]byte{})
	}
	return r.Operation != ([32]byte{}) && r.Group.ClusterID != ([16]byte{}) &&
		r.Group.ClusterIncarnation != ([16]byte{}) && r.Group.TopologyRecoveryEpoch != 0 &&
		r.Group.ShardIncarnation != ([16]byte{}) && r.Group.GroupID != ([16]byte{}) &&
		r.AllocationGeneration != 0 && r.FromSchemaGeneration != 0 && r.FromSchemaGeneration != ^uint64(0) &&
		r.FromRelationManifestDigest != (replication.Digest{}) && r.SourceApplied != 0 &&
		r.SQLBytes > 0 && r.SQLBytes <= sqldriver.ReplicatedChildSchemaMaxBytes && r.SQLDigest != ([32]byte{})
}

func AppendBuildRequest(dst []byte, r BuildRequest) ([]byte, error) {
	if !validBuildRequest(r) {
		return dst, ErrInvalid
	}
	start := len(dst)
	dst = append(dst, make([]byte, buildRequestBytes)...)
	raw := dst[start:]
	magic := buildRequestMagic
	if r.Resume {
		magic = buildResumeRequestMagic
	}
	copy(raw, magic[:])
	copy(raw[8:40], r.Operation[:])
	copy(raw[40:56], r.Group.ClusterID[:])
	copy(raw[56:72], r.Group.ClusterIncarnation[:])
	binary.LittleEndian.PutUint64(raw[72:80], r.Group.TopologyRecoveryEpoch)
	copy(raw[80:96], r.Group.ShardIncarnation[:])
	copy(raw[96:112], r.Group.GroupID[:])
	binary.LittleEndian.PutUint64(raw[112:120], uint64(r.AllocationGeneration))
	binary.LittleEndian.PutUint64(raw[120:128], r.FromSchemaGeneration)
	copy(raw[128:160], r.FromRelationManifestDigest[:])
	binary.LittleEndian.PutUint64(raw[160:168], r.SourceApplied)
	binary.LittleEndian.PutUint64(raw[168:176], r.SQLBytes)
	copy(raw[176:208], r.SQLDigest[:])
	return dst, nil
}

// BuildRequestDigest is the shard-local build identity. It binds the source
// authority and exact SQL as well as the coordinator's retained operation ID.
func BuildRequestDigest(r BuildRequest) ([32]byte, error) {
	var buffer [buildRequestBytes]byte
	raw, err := AppendBuildRequest(buffer[:0], r)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(raw), nil
}

func ReadBuildRequest(reader io.Reader) (BuildRequest, error) {
	var raw [buildRequestBytes]byte
	var r BuildRequest
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return r, err
	}
	if !bytes.Equal(raw[:8], buildRequestMagic[:]) && !bytes.Equal(raw[:8], buildResumeRequestMagic[:]) {
		return r, ErrInvalid
	}
	r.Resume = bytes.Equal(raw[:8], buildResumeRequestMagic[:])
	copy(r.Operation[:], raw[8:40])
	copy(r.Group.ClusterID[:], raw[40:56])
	copy(r.Group.ClusterIncarnation[:], raw[56:72])
	r.Group.TopologyRecoveryEpoch = binary.LittleEndian.Uint64(raw[72:80])
	copy(r.Group.ShardIncarnation[:], raw[80:96])
	copy(r.Group.GroupID[:], raw[96:112])
	r.AllocationGeneration = distribution.ShardAllocationGeneration(binary.LittleEndian.Uint64(raw[112:120]))
	r.FromSchemaGeneration = binary.LittleEndian.Uint64(raw[120:128])
	copy(r.FromRelationManifestDigest[:], raw[128:160])
	r.SourceApplied = binary.LittleEndian.Uint64(raw[160:168])
	r.SQLBytes = binary.LittleEndian.Uint64(raw[168:176])
	copy(r.SQLDigest[:], raw[176:208])
	if !validBuildRequest(r) {
		return BuildRequest{}, ErrInvalid
	}
	return r, nil
}

func appendBuildReceipt(target sqldriver.ReplicatedSchemaDDLTarget, request BuildRequest) ([]byte, error) {
	if err := sqldriver.ValidateReplicatedSchemaDDLTarget(target, request.SourceApplied, request.FromSchemaGeneration); err != nil {
		return nil, errors.Join(ErrInvalid, err)
	}
	raw, err := vibejson.Marshal(&target)
	if len(raw) > maxBuildReceiptBytes {
		return nil, ErrBound
	}
	return raw, err
}

func readBuildResponseHeader(reader io.Reader, request BuildRequest) (uint64, error) {
	var raw [buildResponseBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return 0, err
	}
	digest, err := BuildRequestDigest(request)
	if err != nil || !bytes.Equal(raw[:8], buildResponseMagic[:]) || !bytes.Equal(raw[16:48], digest[:]) ||
		binary.LittleEndian.Uint64(raw[8:16]) != uint64(raw[8]) {
		return 0, ErrInvalid
	}
	n := binary.LittleEndian.Uint64(raw[48:56])
	code := ResponseCode(raw[8])
	if code != ResponseOK {
		if n != 0 {
			return 0, ErrInvalid
		}
		return 0, responseError(code)
	}
	return n, nil
}

func readBuildResponse(reader io.Reader, request BuildRequest) (sqldriver.ReplicatedSchemaDDLTarget, error) {
	var target sqldriver.ReplicatedSchemaDDLTarget
	n, err := readBuildResponseHeader(reader, request)
	if err != nil {
		return target, err
	}
	if n == 0 || n > maxBuildReceiptBytes {
		return target, ErrBound
	}
	body := make([]byte, int(n))
	if _, err := io.ReadFull(reader, body); err != nil {
		return target, err
	}
	if err := vibejson.Unmarshal(body, &target); err != nil {
		return sqldriver.ReplicatedSchemaDDLTarget{}, errors.Join(ErrInvalid, err)
	}
	canonical, err := appendBuildReceipt(target, request)
	if err != nil || !bytes.Equal(body, canonical) {
		return sqldriver.ReplicatedSchemaDDLTarget{}, errors.Join(ErrInvalid, err)
	}
	return target, nil
}

func appendBuildResumeReceipt(dst []byte, request BuildRequest, sql string,
	target sqldriver.ReplicatedSchemaDDLTarget, active bool,
) ([]byte, error) {
	if request.Resume || !validBuildRequest(request) || uint64(len(sql)) != request.SQLBytes ||
		sha256.Sum256([]byte(sql)) != request.SQLDigest {
		return dst, ErrInvalid
	}
	targetRaw, err := appendBuildReceipt(target, request)
	if err != nil || len(targetRaw) > maxBuildReceiptBytes {
		return dst, errors.Join(err, ErrInvalid)
	}
	dst, err = AppendBuildRequest(dst, request)
	if err != nil {
		return dst, err
	}
	var lengths [24]byte
	binary.LittleEndian.PutUint64(lengths[:8], uint64(len(sql)))
	binary.LittleEndian.PutUint64(lengths[8:16], uint64(len(targetRaw)))
	if active {
		binary.LittleEndian.PutUint64(lengths[16:24], 1)
	}
	dst = append(dst, lengths[:]...)
	dst = append(dst, sql...)
	dst = append(dst, targetRaw...)
	return dst, nil
}

func readBuildResumeResponse(reader io.Reader, lookup BuildRequest) (BuildRequest, string,
	sqldriver.ReplicatedSchemaDDLTarget, bool, error,
) {
	var request BuildRequest
	var target sqldriver.ReplicatedSchemaDDLTarget
	n, err := readBuildResponseHeader(reader, lookup)
	if err != nil || n <= buildRequestBytes+24 || n > maxBuildResumeReceiptBytes {
		return request, "", target, false, errors.Join(err, ErrInvalid)
	}
	body := make([]byte, int(n))
	if _, err = io.ReadFull(reader, body); err != nil {
		return request, "", target, false, err
	}
	request, err = ReadBuildRequest(bytes.NewReader(body[:buildRequestBytes]))
	if err != nil || request.Resume || request.Operation != lookup.Operation || request.Group != lookup.Group {
		return BuildRequest{}, "", target, false, errors.Join(err, ErrConflict)
	}
	sqlBytes := binary.LittleEndian.Uint64(body[buildRequestBytes : buildRequestBytes+8])
	targetBytes := binary.LittleEndian.Uint64(body[buildRequestBytes+8 : buildRequestBytes+16])
	activeRaw := binary.LittleEndian.Uint64(body[buildRequestBytes+16 : buildRequestBytes+24])
	if activeRaw > 1 || sqlBytes != request.SQLBytes || sqlBytes+targetBytes+buildRequestBytes+24 != uint64(len(body)) {
		return BuildRequest{}, "", target, false, ErrInvalid
	}
	active := activeRaw == 1
	sqlStart := buildRequestBytes + 24
	sqlEnd := sqlStart + int(sqlBytes)
	sql := string(body[sqlStart:sqlEnd])
	if sha256.Sum256([]byte(sql)) != request.SQLDigest {
		return BuildRequest{}, "", target, false, ErrInvalid
	}
	if err = vibejson.Unmarshal(body[sqlEnd:], &target); err != nil {
		return BuildRequest{}, "", sqldriver.ReplicatedSchemaDDLTarget{}, false, err
	}
	targetCanonical, err := appendBuildReceipt(target, request)
	if err != nil || !bytes.Equal(targetCanonical, body[sqlEnd:]) {
		return BuildRequest{}, "", sqldriver.ReplicatedSchemaDDLTarget{}, false, errors.Join(err, ErrInvalid)
	}
	canonical, err := appendBuildResumeReceipt(nil, request, sql, target, active)
	if err != nil || !bytes.Equal(canonical, body) {
		return BuildRequest{}, "", sqldriver.ReplicatedSchemaDDLTarget{}, false, errors.Join(err, ErrInvalid)
	}
	return request, sql, target, active, nil
}
