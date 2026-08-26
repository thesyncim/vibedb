package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/internal/replication"
)

const (
	schemaTransitionFormat      uint16 = 0
	schemaTransitionHeaderBytes        = 512
	MaxSchemaTransitionBytes           = schemaTransitionHeaderBytes +
		2*replication.MaxIdentityBytes + recordChecksumLen
)

var schemaTransitionMagic = [8]byte{'V', 'D', 'B', 'S', 'C', 'H', 0, 0}
var schemaTransitionChecksumDomain = []byte("vibedb/replicated-state/schema-transition/format-0\x00")
var schemaTransitionDataChainDomain = []byte("vibedb/replicated-state/schema-data-chain/format-0\x00")

// SchemaTransition is the one Raft-ordered authorization to retire the live
// relation bundle and publish a prepared replacement. The target relation
// images are authenticated independently by MembershipSource/Target and are
// selected by the checkpoint membership certificate; they are never encoded
// in or copied through this control entry.
type SchemaTransition struct {
	From                      Binding
	ToSchemaGeneration        uint64
	ExpectedReplicaSetVersion uint64
	MembershipSequence        uint64
	MembershipSource          [sha256.Size]byte
	MembershipTarget          [sha256.Size]byte
	FromManifest              [sha256.Size]byte
	FromApplyContract         [sha256.Size]byte
	ToManifest                [sha256.Size]byte
	ToApplyContract           [sha256.Size]byte
	RequestDigest             [sha256.Size]byte
	AuthorizationDigest       [sha256.Size]byte
	CatalogCASDigest          [sha256.Size]byte
}

// SchemaTransitionView is a validated borrowed control envelope.
type SchemaTransitionView struct {
	SchemaTransition
	raw []byte
}

func (v SchemaTransitionView) Bytes() []byte { return v.raw[:len(v.raw):len(v.raw)] }

func AppendSchemaTransition(dst []byte, transition SchemaTransition) ([]byte, error) {
	if err := validateSchemaTransition(transition); err != nil {
		return dst, err
	}
	total := schemaTransitionHeaderBytes + len(transition.From.Distribution) +
		len(transition.From.Shard) + recordChecksumLen
	region := writableAppendRegion(dst, total)
	if byteStringOverlap(region, transition.From.Distribution) ||
		byteStringOverlap(region, transition.From.Shard) {
		return dst, ErrCodecAlias
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	copy(frame[:8], schemaTransitionMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], schemaTransitionFormat)
	binary.LittleEndian.PutUint16(frame[10:12], schemaTransitionHeaderBytes)
	binary.LittleEndian.PutUint32(frame[12:16], uint32(total))
	copy(frame[24:40], transition.From.ClusterID[:])
	copy(frame[40:56], transition.From.ClusterIncarnation[:])
	copy(frame[56:72], transition.From.ShardIncarnation[:])
	copy(frame[72:88], transition.From.GroupID[:])
	binary.LittleEndian.PutUint64(frame[88:96], transition.From.TopologyRecoveryEpoch)
	binary.LittleEndian.PutUint64(frame[96:104], transition.From.AllocationGeneration)
	binary.LittleEndian.PutUint64(frame[104:112], transition.From.ActivePolicyGeneration)
	binary.LittleEndian.PutUint64(frame[112:120], transition.From.ProtectionEpoch)
	binary.LittleEndian.PutUint64(frame[120:128], transition.From.OwnershipEpoch)
	binary.LittleEndian.PutUint64(frame[128:136], transition.From.SchemaGeneration)
	binary.LittleEndian.PutUint64(frame[136:144], transition.From.RoutingVersion)
	binary.LittleEndian.PutUint64(frame[144:152], transition.From.RouteGeneration)
	binary.LittleEndian.PutUint64(frame[152:160], transition.ToSchemaGeneration)
	binary.LittleEndian.PutUint64(frame[160:168], transition.ExpectedReplicaSetVersion)
	binary.LittleEndian.PutUint64(frame[168:176], transition.MembershipSequence)
	copy(frame[176:208], transition.MembershipSource[:])
	copy(frame[208:240], transition.MembershipTarget[:])
	copy(frame[240:272], transition.FromManifest[:])
	copy(frame[272:304], transition.FromApplyContract[:])
	copy(frame[304:336], transition.ToManifest[:])
	copy(frame[336:368], transition.ToApplyContract[:])
	copy(frame[368:400], transition.RequestDigest[:])
	copy(frame[400:432], transition.AuthorizationDigest[:])
	copy(frame[432:464], transition.CatalogCASDigest[:])
	binary.LittleEndian.PutUint16(frame[464:466], uint16(len(transition.From.Distribution)))
	binary.LittleEndian.PutUint16(frame[466:468], uint16(len(transition.From.Shard)))
	appendOwnershipRange(frame[468:485], transition.From.OwnedRange)
	cursor := schemaTransitionHeaderBytes
	cursor += copy(frame[cursor:], transition.From.Distribution)
	cursor += copy(frame[cursor:], transition.From.Shard)
	if cursor != total-recordChecksumLen {
		panic("replicatedstate: schema transition size diverged")
	}
	sealRecord(frame, schemaTransitionChecksumDomain)
	return dst, nil
}

func IsSchemaTransition(data []byte) bool {
	return len(data) >= len(schemaTransitionMagic) &&
		bytes.Equal(data[:len(schemaTransitionMagic)], schemaTransitionMagic[:])
}

func OpenSchemaTransition(data []byte) (SchemaTransitionView, error) {
	if len(data) < schemaTransitionHeaderBytes+recordChecksumLen ||
		len(data) > MaxSchemaTransitionBytes || !IsSchemaTransition(data) ||
		binary.LittleEndian.Uint16(data[8:10]) != schemaTransitionFormat ||
		binary.LittleEndian.Uint16(data[10:12]) != schemaTransitionHeaderBytes ||
		binary.LittleEndian.Uint32(data[12:16]) != uint32(len(data)) ||
		!zeroBytes(data[16:24]) || data[484] > 1 ||
		!zeroBytes(data[485:schemaTransitionHeaderBytes]) ||
		!verifyRecord(data, schemaTransitionChecksumDomain) {
		return SchemaTransitionView{}, fmt.Errorf("%w: schema transition envelope", ErrSchemaTransition)
	}
	distributionBytes := int(binary.LittleEndian.Uint16(data[464:466]))
	shardBytes := int(binary.LittleEndian.Uint16(data[466:468]))
	if distributionBytes == 0 || distributionBytes > replication.MaxIdentityBytes ||
		shardBytes == 0 || shardBytes > replication.MaxIdentityBytes ||
		schemaTransitionHeaderBytes+distributionBytes+shardBytes+recordChecksumLen != len(data) {
		return SchemaTransitionView{}, fmt.Errorf("%w: schema transition identity lengths", ErrSchemaTransition)
	}
	cursor := schemaTransitionHeaderBytes
	view := SchemaTransitionView{raw: data}
	t := &view.SchemaTransition
	copy(t.From.ClusterID[:], data[24:40])
	copy(t.From.ClusterIncarnation[:], data[40:56])
	copy(t.From.ShardIncarnation[:], data[56:72])
	copy(t.From.GroupID[:], data[72:88])
	t.From.TopologyRecoveryEpoch = binary.LittleEndian.Uint64(data[88:96])
	t.From.AllocationGeneration = binary.LittleEndian.Uint64(data[96:104])
	t.From.ActivePolicyGeneration = binary.LittleEndian.Uint64(data[104:112])
	t.From.ProtectionEpoch = binary.LittleEndian.Uint64(data[112:120])
	t.From.OwnershipEpoch = binary.LittleEndian.Uint64(data[120:128])
	t.From.SchemaGeneration = binary.LittleEndian.Uint64(data[128:136])
	t.From.RoutingVersion = binary.LittleEndian.Uint64(data[136:144])
	t.From.RouteGeneration = binary.LittleEndian.Uint64(data[144:152])
	t.ToSchemaGeneration = binary.LittleEndian.Uint64(data[152:160])
	t.ExpectedReplicaSetVersion = binary.LittleEndian.Uint64(data[160:168])
	t.MembershipSequence = binary.LittleEndian.Uint64(data[168:176])
	copy(t.MembershipSource[:], data[176:208])
	copy(t.MembershipTarget[:], data[208:240])
	copy(t.FromManifest[:], data[240:272])
	copy(t.FromApplyContract[:], data[272:304])
	copy(t.ToManifest[:], data[304:336])
	copy(t.ToApplyContract[:], data[336:368])
	copy(t.RequestDigest[:], data[368:400])
	copy(t.AuthorizationDigest[:], data[400:432])
	copy(t.CatalogCASDigest[:], data[432:464])
	t.From.OwnedRange = openOwnershipRange(data[468:485])
	t.From.Distribution = string(data[cursor : cursor+distributionBytes])
	t.From.Shard = string(data[cursor+distributionBytes : cursor+distributionBytes+shardBytes])
	if err := validateSchemaTransition(*t); err != nil {
		return SchemaTransitionView{}, err
	}
	return view, nil
}

func validateSchemaTransition(t SchemaTransition) error {
	if err := t.From.validate(); err != nil || t.From.SchemaGeneration == math.MaxUint64 ||
		t.ToSchemaGeneration != t.From.SchemaGeneration+1 ||
		t.ExpectedReplicaSetVersion == 0 || t.MembershipSequence == 0 ||
		t.MembershipSource == ([sha256.Size]byte{}) ||
		t.MembershipTarget == ([sha256.Size]byte{}) ||
		t.MembershipSource == t.MembershipTarget ||
		t.FromManifest == ([sha256.Size]byte{}) || t.FromApplyContract == ([sha256.Size]byte{}) ||
		t.ToManifest == ([sha256.Size]byte{}) || t.ToApplyContract == ([sha256.Size]byte{}) ||
		t.FromManifest == t.ToManifest || t.FromApplyContract == t.ToApplyContract ||
		t.RequestDigest == ([sha256.Size]byte{}) ||
		t.AuthorizationDigest == ([sha256.Size]byte{}) ||
		t.CatalogCASDigest == ([sha256.Size]byte{}) {
		return ErrSchemaTransition
	}
	return nil
}

func schemaTransitionBinding(t SchemaTransitionView) Binding {
	binding := t.From
	binding.SchemaGeneration = t.ToSchemaGeneration
	return binding
}

func schemaTransitionDataChain(previous [sha256.Size]byte, t SchemaTransitionView) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(schemaTransitionDataChainDomain)
	_, _ = h.Write(previous[:])
	_, _ = h.Write(t.Bytes())
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}
