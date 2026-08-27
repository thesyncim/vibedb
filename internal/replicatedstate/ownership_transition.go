package replicatedstate

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/replication"
	"go.etcd.io/raft/v3"
)

const (
	ownershipTransitionFormat      = uint16(2)
	ownershipTransitionHeaderBytes = 256
	MaxOwnershipTransitionBytes    = ownershipTransitionHeaderBytes +
		2*replication.MaxIdentityBytes + recordChecksumLen
)

var ownershipTransitionMagic = [8]byte{'V', 'D', 'B', 'O', 'W', 'N', 0, 0}
var ownershipTransitionChecksumDomain = []byte(
	"vibedb/replicated-state/ownership-transition-checksum\x00",
)

// OwnershipTransition is one topology-authorized, ordered change to the
// mutable serving fence of an existing shard allocation. Immutable shard and
// Raft-group identities cannot change. Membership must already contain both
// SourceMember and TargetMember as voters at ExpectedReplicaSetVersion.
type OwnershipTransition struct {
	From                      Binding
	ExpectedReplicaSetVersion uint64
	SourceMember              uint64
	TargetMember              uint64
	ToOwnershipEpoch          uint64
	ToRoutingVersion          uint64
	ToRouteGeneration         uint64
	ToOwnedRange              distribution.KeyRange
}

// OwnershipTransitionView is a validated borrowed transition envelope.
// Distribution and Shard alias the input and are valid for its lifetime.
type OwnershipTransitionView struct {
	ClusterID             replication.ID128
	ClusterIncarnation    replication.ID128
	TopologyRecoveryEpoch uint64
	Distribution          []byte
	Shard                 []byte
	AllocationGeneration  uint64
	ShardIncarnation      replication.ID128
	GroupID               replication.ID128

	ActivePolicyGeneration uint64
	ProtectionEpoch        uint64
	OwnershipEpoch         uint64
	SchemaGeneration       uint64
	RoutingVersion         uint64
	RouteGeneration        uint64

	ExpectedReplicaSetVersion uint64
	SourceMember              uint64
	TargetMember              uint64
	ToOwnershipEpoch          uint64
	ToRoutingVersion          uint64
	ToRouteGeneration         uint64
	FromOwnedRange            distribution.KeyRange
	ToOwnedRange              distribution.KeyRange
	raw                       []byte
}

// Bytes returns the exact validated envelope as a capacity-clamped borrowed
// view.
func (v OwnershipTransitionView) Bytes() []byte {
	return v.raw[:len(v.raw):len(v.raw)]
}

// AppendOwnershipTransition appends one deterministic binary control record.
// On validation failure dst is unchanged.
func AppendOwnershipTransition(dst []byte, transition OwnershipTransition) ([]byte, error) {
	if err := validateOwnershipTransitionInput(transition); err != nil {
		return dst, err
	}
	total := ownershipTransitionHeaderBytes + len(transition.From.Distribution) +
		len(transition.From.Shard) + recordChecksumLen
	region := writableAppendRegion(dst, total)
	if byteStringOverlap(region, transition.From.Distribution) ||
		byteStringOverlap(region, transition.From.Shard) {
		return dst, ErrCodecAlias
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	frame := dst[start:]
	copy(frame[0:8], ownershipTransitionMagic[:])
	binary.LittleEndian.PutUint16(frame[8:10], ownershipTransitionFormat)
	binary.LittleEndian.PutUint16(frame[10:12], ownershipTransitionHeaderBytes)
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
	binary.LittleEndian.PutUint64(frame[152:160], transition.ExpectedReplicaSetVersion)
	binary.LittleEndian.PutUint64(frame[160:168], transition.SourceMember)
	binary.LittleEndian.PutUint64(frame[168:176], transition.TargetMember)
	binary.LittleEndian.PutUint64(frame[176:184], transition.ToOwnershipEpoch)
	binary.LittleEndian.PutUint64(frame[184:192], transition.ToRoutingVersion)
	binary.LittleEndian.PutUint64(frame[192:200], transition.ToRouteGeneration)
	binary.LittleEndian.PutUint16(frame[200:202], uint16(len(transition.From.Distribution)))
	binary.LittleEndian.PutUint16(frame[202:204], uint16(len(transition.From.Shard)))
	appendOwnershipRange(frame[204:221], transition.From.OwnedRange)
	appendOwnershipRange(frame[221:238], transition.ToOwnedRange)
	cursor := ownershipTransitionHeaderBytes
	cursor += copy(frame[cursor:], transition.From.Distribution)
	cursor += copy(frame[cursor:], transition.From.Shard)
	if cursor != total-recordChecksumLen {
		panic("replicatedstate: ownership transition size diverged")
	}
	sealRecord(frame, ownershipTransitionChecksumDomain)
	return dst, nil
}

// IsOwnershipTransition reports whether data claims the dedicated ownership
// control grammar. A claimed but malformed record must be opened and rejected;
// it must never fall through to the user mutation decoder.
func IsOwnershipTransition(data []byte) bool {
	return len(data) >= len(ownershipTransitionMagic) &&
		bytes.Equal(data[:len(ownershipTransitionMagic)], ownershipTransitionMagic[:])
}

// OpenOwnershipTransition validates and borrows one exact control envelope.
func OpenOwnershipTransition(data []byte) (OwnershipTransitionView, error) {
	if len(data) < ownershipTransitionHeaderBytes+recordChecksumLen ||
		len(data) > MaxOwnershipTransitionBytes ||
		!IsOwnershipTransition(data) ||
		binary.LittleEndian.Uint16(data[8:10]) != ownershipTransitionFormat ||
		binary.LittleEndian.Uint16(data[10:12]) != ownershipTransitionHeaderBytes ||
		binary.LittleEndian.Uint32(data[12:16]) != uint32(len(data)) ||
		data[220] > 1 || data[237] > 1 ||
		!zeroBytes(data[16:24]) || !zeroBytes(data[238:ownershipTransitionHeaderBytes]) ||
		!verifyRecord(data, ownershipTransitionChecksumDomain) {
		return OwnershipTransitionView{}, fmt.Errorf("%w: ownership transition envelope", ErrOwnershipTransition)
	}
	distributionBytes := int(binary.LittleEndian.Uint16(data[200:202]))
	shardBytes := int(binary.LittleEndian.Uint16(data[202:204]))
	if distributionBytes == 0 || distributionBytes > replication.MaxIdentityBytes ||
		shardBytes == 0 || shardBytes > replication.MaxIdentityBytes ||
		ownershipTransitionHeaderBytes+distributionBytes+shardBytes+recordChecksumLen != len(data) {
		return OwnershipTransitionView{}, fmt.Errorf("%w: ownership transition identity lengths", ErrOwnershipTransition)
	}
	cursor := ownershipTransitionHeaderBytes
	view := OwnershipTransitionView{
		TopologyRecoveryEpoch:     binary.LittleEndian.Uint64(data[88:96]),
		AllocationGeneration:      binary.LittleEndian.Uint64(data[96:104]),
		ActivePolicyGeneration:    binary.LittleEndian.Uint64(data[104:112]),
		ProtectionEpoch:           binary.LittleEndian.Uint64(data[112:120]),
		OwnershipEpoch:            binary.LittleEndian.Uint64(data[120:128]),
		SchemaGeneration:          binary.LittleEndian.Uint64(data[128:136]),
		RoutingVersion:            binary.LittleEndian.Uint64(data[136:144]),
		RouteGeneration:           binary.LittleEndian.Uint64(data[144:152]),
		ExpectedReplicaSetVersion: binary.LittleEndian.Uint64(data[152:160]),
		SourceMember:              binary.LittleEndian.Uint64(data[160:168]),
		TargetMember:              binary.LittleEndian.Uint64(data[168:176]),
		ToOwnershipEpoch:          binary.LittleEndian.Uint64(data[176:184]),
		ToRoutingVersion:          binary.LittleEndian.Uint64(data[184:192]),
		ToRouteGeneration:         binary.LittleEndian.Uint64(data[192:200]),
		Distribution:              data[cursor : cursor+distributionBytes : cursor+distributionBytes],
		Shard:                     data[cursor+distributionBytes : cursor+distributionBytes+shardBytes : cursor+distributionBytes+shardBytes],
		raw:                       data,
	}
	view.FromOwnedRange = openOwnershipRange(data[204:221])
	view.ToOwnedRange = openOwnershipRange(data[221:238])
	copy(view.ClusterID[:], data[24:40])
	copy(view.ClusterIncarnation[:], data[40:56])
	copy(view.ShardIncarnation[:], data[56:72])
	copy(view.GroupID[:], data[72:88])
	if err := validateOwnershipTransitionView(view); err != nil {
		return OwnershipTransitionView{}, err
	}
	return view, nil
}

func validateOwnershipTransitionInput(transition OwnershipTransition) error {
	if err := transition.From.validate(); err != nil {
		return fmt.Errorf("%w: from binding: %v", ErrOwnershipTransition, err)
	}
	view := OwnershipTransitionView{
		ClusterID: transition.From.ClusterID, ClusterIncarnation: transition.From.ClusterIncarnation,
		TopologyRecoveryEpoch: transition.From.TopologyRecoveryEpoch,
		Distribution:          []byte(transition.From.Distribution), Shard: []byte(transition.From.Shard),
		AllocationGeneration: transition.From.AllocationGeneration,
		ShardIncarnation:     transition.From.ShardIncarnation, GroupID: transition.From.GroupID,
		ActivePolicyGeneration: transition.From.ActivePolicyGeneration,
		ProtectionEpoch:        transition.From.ProtectionEpoch, OwnershipEpoch: transition.From.OwnershipEpoch,
		SchemaGeneration: transition.From.SchemaGeneration, RoutingVersion: transition.From.RoutingVersion,
		RouteGeneration:           transition.From.RouteGeneration,
		ExpectedReplicaSetVersion: transition.ExpectedReplicaSetVersion,
		SourceMember:              transition.SourceMember, TargetMember: transition.TargetMember,
		ToOwnershipEpoch:  transition.ToOwnershipEpoch,
		ToRoutingVersion:  transition.ToRoutingVersion,
		ToRouteGeneration: transition.ToRouteGeneration,
		FromOwnedRange:    transition.From.OwnedRange, ToOwnedRange: transition.ToOwnedRange,
	}
	return validateOwnershipTransitionView(view)
}

func validateOwnershipTransitionView(view OwnershipTransitionView) error {
	if view.ClusterID == (replication.ID128{}) || view.ClusterIncarnation == (replication.ID128{}) ||
		view.ShardIncarnation == (replication.ID128{}) || view.GroupID == (replication.ID128{}) ||
		view.TopologyRecoveryEpoch == 0 || view.AllocationGeneration == 0 ||
		view.ActivePolicyGeneration == 0 || view.ProtectionEpoch == 0 ||
		view.OwnershipEpoch == 0 || view.SchemaGeneration == 0 || view.RoutingVersion == 0 ||
		view.RouteGeneration == 0 || view.ExpectedReplicaSetVersion == 0 ||
		raft.IsLocalMsgTarget(view.SourceMember) || raft.IsLocalMsgTarget(view.TargetMember) ||
		view.SourceMember == view.TargetMember ||
		len(view.Distribution) == 0 || len(view.Distribution) > replication.MaxIdentityBytes ||
		len(view.Shard) == 0 || len(view.Shard) > replication.MaxIdentityBytes ||
		view.OwnershipEpoch == math.MaxUint64 || view.RoutingVersion == math.MaxUint64 ||
		view.RouteGeneration == math.MaxUint64 ||
		view.ToOwnershipEpoch != view.OwnershipEpoch+1 ||
		view.ToRoutingVersion <= view.RoutingVersion ||
		view.ToRouteGeneration <= view.RouteGeneration ||
		!canonicalOwnershipRange(view.FromOwnedRange) || !canonicalOwnershipRange(view.ToOwnedRange) ||
		!ownershipRangeContains(view.FromOwnedRange, view.ToOwnedRange) {
		return fmt.Errorf("%w: ownership transition semantics", ErrOwnershipTransition)
	}
	return nil
}

func (m *Machine) ownershipTransitionBinding(
	transition OwnershipTransitionView,
) (Binding, error) {
	current := m.binding
	if m.state.ActiveTransactionCount != 0 ||
		transition.ClusterID != current.ClusterID ||
		transition.ClusterIncarnation != current.ClusterIncarnation ||
		transition.TopologyRecoveryEpoch != current.TopologyRecoveryEpoch ||
		!bytes.Equal(transition.Distribution, []byte(current.Distribution)) ||
		!bytes.Equal(transition.Shard, []byte(current.Shard)) ||
		transition.AllocationGeneration != current.AllocationGeneration ||
		transition.ShardIncarnation != current.ShardIncarnation ||
		transition.GroupID != current.GroupID ||
		transition.ActivePolicyGeneration != current.ActivePolicyGeneration ||
		transition.ProtectionEpoch != current.ProtectionEpoch ||
		transition.OwnershipEpoch != current.OwnershipEpoch ||
		transition.SchemaGeneration != current.SchemaGeneration ||
		transition.RoutingVersion != current.RoutingVersion ||
		transition.RouteGeneration != current.RouteGeneration ||
		transition.FromOwnedRange != current.OwnedRange ||
		transition.ExpectedReplicaSetVersion != m.state.ReplicaSetVersion {
		return Binding{}, ErrOwnershipTransition
	}
	if len(m.state.ConfState.GetVotersOutgoing()) != 0 ||
		len(m.state.ConfState.GetLearnersNext()) != 0 || m.state.ConfState.GetAutoLeave() ||
		!memberInSorted(m.state.ConfState.GetVoters(), transition.SourceMember) ||
		!memberInSorted(m.state.ConfState.GetVoters(), transition.TargetMember) {
		return Binding{}, ErrOwnershipTransition
	}
	if transition.ToRoutingVersion != current.RoutingVersion+1 ||
		transition.ToRouteGeneration != current.RouteGeneration+1 {
		// Catalog generations can advance without applying an ownership entry
		// to this shard. A jump is legal only for the exact target of its
		// committed capture recipe, never from monotonicity alone.
		authorizer, ok := m.capture.(interface {
			PartitionerDigest() [32]byte
			AuthorizesOwnershipTransition(OwnershipTransitionView) bool
		})
		activation := m.splitCaptureActivation
		if !ok || activation == nil ||
			activation.Command.BindingDigest != SplitCaptureBindingDigest(current) ||
			authorizer.PartitionerDigest() != activation.Command.PartitionerDigest ||
			!authorizer.AuthorizesOwnershipTransition(transition) {
			return Binding{}, ErrOwnershipTransition
		}
	}
	current.OwnershipEpoch = transition.ToOwnershipEpoch
	current.RoutingVersion = transition.ToRoutingVersion
	current.RouteGeneration = transition.ToRouteGeneration
	current.OwnedRange = transition.ToOwnedRange
	return current, nil
}

func appendOwnershipRange(dst []byte, owned distribution.KeyRange) {
	copy(dst[0:8], owned.Start[:])
	copy(dst[8:16], owned.End.Point[:])
	if owned.End.Max {
		dst[16] = 1
	}
}

func openOwnershipRange(src []byte) (owned distribution.KeyRange) {
	copy(owned.Start[:], src[0:8])
	copy(owned.End.Point[:], src[8:16])
	owned.End.Max = src[16] == 1
	return owned
}

func canonicalOwnershipRange(owned distribution.KeyRange) bool {
	return owned.Valid() && (!owned.End.Max || owned.End.Point == (distribution.KeyspacePoint{}))
}

func ownershipRangeContains(outer, inner distribution.KeyRange) bool {
	if distribution.ComparePoints(outer.Start, inner.Start) > 0 {
		return false
	}
	if outer.End.Max {
		return true
	}
	return !inner.End.Max && distribution.ComparePoints(inner.End.Point, outer.End.Point) <= 0
}

func memberInSorted(members []uint64, member uint64) bool {
	lo, hi := 0, len(members)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if members[mid] < member {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo < len(members) && members[lo] == member
}

func zeroBytes(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}
