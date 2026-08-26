package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"sync"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

var (
	ErrClusterCatalogDrainFence = errors.New("gateway: invalid cluster catalog drain fence")
	ErrClusterCatalogDrainAck   = errors.New("gateway: invalid cluster catalog drain acknowledgement")
	ErrClusterCatalogDrainAuth  = errors.New("gateway: catalog drain peer is not authorized")
	ErrClusterCatalogDrainState = errors.New("gateway: invalid cluster catalog drain state")
)

const (
	// MaxClusterCatalogDrainGateways is the wire-format bound. It is deliberately
	// much larger than a practical gateway fleet and is not a transaction or
	// shard-fanout limit.
	MaxClusterCatalogDrainGateways = math.MaxUint16
	ClusterCatalogDrainAckBytes    = 104
	clusterCatalogDrainHeaderBytes = 156
)

var (
	clusterCatalogDrainAckMagic   = [8]byte{'V', 'B', 'D', 'R', 'A', 'I', 'N', 'A'}
	clusterCatalogDrainStateMagic = [8]byte{'V', 'B', 'D', 'R', 'A', 'I', 'N', 'S'}
)

// ClusterCatalogDrainMember identifies one serving gateway incarnation. Node
// is authenticated by the TLS peer identity; Incarnation prevents an
// acknowledgement from a replaced process from satisfying a newer roster.
type ClusterCatalogDrainMember struct {
	Node        rafttransport.NodeID
	Incarnation uint64
}

// ClusterCatalogDrainFence is an immutable exact cluster-wide catalog cut.
// CatalogDigest is supplied by the replicated catalog authority and must name
// the exact published generation being drained.
type ClusterCatalogDrainFence struct {
	operation     [sha256.Size]byte
	generation    uint64
	catalogDigest [sha256.Size]byte
	trust         rafttransport.TrustDomain
	members       []ClusterCatalogDrainMember
	rosterDigest  [sha256.Size]byte
	digest        [sha256.Size]byte
}

func NewClusterCatalogDrainFence(
	operation [sha256.Size]byte,
	generation uint64,
	catalogDigest [sha256.Size]byte,
	trust rafttransport.TrustDomain,
	members []ClusterCatalogDrainMember,
) (ClusterCatalogDrainFence, error) {
	if operation == ([sha256.Size]byte{}) || generation == 0 ||
		catalogDigest == ([sha256.Size]byte{}) || trust.ClusterID == ([16]byte{}) ||
		trust.ClusterIncarnation == ([16]byte{}) || len(members) == 0 ||
		len(members) > MaxClusterCatalogDrainGateways {
		return ClusterCatalogDrainFence{}, ErrClusterCatalogDrainFence
	}
	canonical := slices.Clone(members)
	slices.SortFunc(canonical, compareClusterCatalogDrainMember)
	for index, member := range canonical {
		if member.Node == (rafttransport.NodeID{}) || member.Incarnation == 0 ||
			index != 0 && canonical[index-1].Node == member.Node {
			return ClusterCatalogDrainFence{}, ErrClusterCatalogDrainFence
		}
	}
	fence := ClusterCatalogDrainFence{
		operation: operation, generation: generation, catalogDigest: catalogDigest,
		trust: trust, members: canonical,
	}
	fence.rosterDigest = clusterCatalogDrainRosterDigest(canonical)
	fence.digest = clusterCatalogDrainFenceDigest(fence)
	return fence, nil
}

func compareClusterCatalogDrainMember(left, right ClusterCatalogDrainMember) int {
	if compared := bytes.Compare(left.Node[:], right.Node[:]); compared != 0 {
		return compared
	}
	if left.Incarnation < right.Incarnation {
		return -1
	}
	if left.Incarnation > right.Incarnation {
		return 1
	}
	return 0
}

func (f ClusterCatalogDrainFence) Operation() [sha256.Size]byte { return f.operation }
func (f ClusterCatalogDrainFence) Generation() uint64           { return f.generation }
func (f ClusterCatalogDrainFence) CatalogDigest() [sha256.Size]byte {
	return f.catalogDigest
}
func (f ClusterCatalogDrainFence) TrustDomain() rafttransport.TrustDomain { return f.trust }
func (f ClusterCatalogDrainFence) RosterDigest() [sha256.Size]byte        { return f.rosterDigest }
func (f ClusterCatalogDrainFence) Digest() [sha256.Size]byte              { return f.digest }
func (f ClusterCatalogDrainFence) MemberCount() int                       { return len(f.members) }
func (f ClusterCatalogDrainFence) Member(index int) (ClusterCatalogDrainMember, bool) {
	if index < 0 || index >= len(f.members) {
		return ClusterCatalogDrainMember{}, false
	}
	return f.members[index], true
}

func clusterCatalogDrainRosterDigest(members []ClusterCatalogDrainMember) [sha256.Size]byte {
	hash := sha256.New()
	hash.Write([]byte("vibedb/catalog-drain/roster\x00"))
	var count [4]byte
	binary.LittleEndian.PutUint32(count[:], uint32(len(members)))
	hash.Write(count[:])
	var incarnation [8]byte
	for _, member := range members {
		hash.Write(member.Node[:])
		binary.LittleEndian.PutUint64(incarnation[:], member.Incarnation)
		hash.Write(incarnation[:])
	}
	var digest [sha256.Size]byte
	hash.Sum(digest[:0])
	return digest
}

func clusterCatalogDrainFenceDigest(fence ClusterCatalogDrainFence) [sha256.Size]byte {
	hash := sha256.New()
	hash.Write([]byte("vibedb/catalog-drain/fence\x00"))
	hash.Write(fence.operation[:])
	var generation [8]byte
	binary.LittleEndian.PutUint64(generation[:], fence.generation)
	hash.Write(generation[:])
	hash.Write(fence.catalogDigest[:])
	hash.Write(fence.trust.ClusterID[:])
	hash.Write(fence.trust.ClusterIncarnation[:])
	hash.Write(fence.rosterDigest[:])
	var digest [sha256.Size]byte
	hash.Sum(digest[:0])
	return digest
}

// ClusterCatalogDrainAck is the fixed-size evidence emitted by one gateway.
// CurrentGeneration may exceed the fenced generation; once the holder has
// published a later immutable catalog, no operation can newly pin an older one.
type ClusterCatalogDrainAck struct {
	FenceDigest       [sha256.Size]byte
	Member            ClusterCatalogDrainMember
	CurrentGeneration uint64
}

// CollectClusterCatalogDrainAck closes the local old-plan admission race and
// returns an acknowledgement bound to the exact cluster fence.
func CollectClusterCatalogDrainAck(
	ctx context.Context,
	holder *CatalogHolder,
	fence ClusterCatalogDrainFence,
	member ClusterCatalogDrainMember,
) (ClusterCatalogDrainAck, error) {
	if ctx == nil || holder == nil || !fence.valid() || !fence.contains(member) {
		return ClusterCatalogDrainAck{}, ErrClusterCatalogDrainAck
	}
	status, err := holder.WaitOlderDrained(ctx, fence.generation)
	if err != nil {
		return ClusterCatalogDrainAck{}, err
	}
	if status.CurrentGeneration < fence.generation || status.ActiveOlderOperations != 0 {
		return ClusterCatalogDrainAck{}, ErrClusterCatalogDrainAck
	}
	return ClusterCatalogDrainAck{
		FenceDigest: fence.digest, Member: member,
		CurrentGeneration: status.CurrentGeneration,
	}, nil
}

func (f ClusterCatalogDrainFence) valid() bool {
	return f.operation != ([sha256.Size]byte{}) && f.generation != 0 &&
		f.catalogDigest != ([sha256.Size]byte{}) && len(f.members) != 0 &&
		len(f.members) <= MaxClusterCatalogDrainGateways &&
		f.rosterDigest == clusterCatalogDrainRosterDigest(f.members) &&
		f.digest == clusterCatalogDrainFenceDigest(f)
}

func (f ClusterCatalogDrainFence) memberIndex(member ClusterCatalogDrainMember) (int, bool) {
	index, found := slices.BinarySearchFunc(f.members, member, compareClusterCatalogDrainMember)
	return index, found
}

func (f ClusterCatalogDrainFence) contains(member ClusterCatalogDrainMember) bool {
	_, found := f.memberIndex(member)
	return found
}

// AppendClusterCatalogDrainAck appends the only canonical acknowledgement
// representation. A checksum catches torn or corrupted control frames.
func AppendClusterCatalogDrainAck(dst []byte, ack ClusterCatalogDrainAck) ([]byte, error) {
	if ack.FenceDigest == ([sha256.Size]byte{}) || ack.Member.Node == (rafttransport.NodeID{}) ||
		ack.Member.Incarnation == 0 || ack.CurrentGeneration == 0 ||
		len(dst) > math.MaxInt-ClusterCatalogDrainAckBytes {
		return dst, ErrClusterCatalogDrainAck
	}
	start := len(dst)
	dst = append(dst, make([]byte, ClusterCatalogDrainAckBytes)...)
	raw := dst[start:]
	copy(raw[:8], clusterCatalogDrainAckMagic[:])
	copy(raw[8:40], ack.FenceDigest[:])
	copy(raw[40:56], ack.Member.Node[:])
	binary.LittleEndian.PutUint64(raw[56:64], ack.Member.Incarnation)
	binary.LittleEndian.PutUint64(raw[64:72], ack.CurrentGeneration)
	checksum := sha256.Sum256(raw[:72])
	copy(raw[72:], checksum[:])
	return dst, nil
}

func OpenClusterCatalogDrainAck(raw []byte) (ClusterCatalogDrainAck, error) {
	if len(raw) != ClusterCatalogDrainAckBytes || !bytes.Equal(raw[:8], clusterCatalogDrainAckMagic[:]) ||
		sha256.Sum256(raw[:72]) != [sha256.Size]byte(raw[72:104]) {
		return ClusterCatalogDrainAck{}, ErrClusterCatalogDrainAck
	}
	var ack ClusterCatalogDrainAck
	copy(ack.FenceDigest[:], raw[8:40])
	copy(ack.Member.Node[:], raw[40:56])
	ack.Member.Incarnation = binary.LittleEndian.Uint64(raw[56:64])
	ack.CurrentGeneration = binary.LittleEndian.Uint64(raw[64:72])
	if ack.FenceDigest == ([sha256.Size]byte{}) || ack.Member.Node == (rafttransport.NodeID{}) ||
		ack.Member.Incarnation == 0 || ack.CurrentGeneration == 0 {
		return ClusterCatalogDrainAck{}, ErrClusterCatalogDrainAck
	}
	return ack, nil
}

// ClusterCatalogDrainMachine is a deterministic, snapshot-capable coordinator
// kernel. ApplyAuthenticated must receive the identity established by the mTLS
// control stream; request bytes alone never grant acknowledgement authority.
type ClusterCatalogDrainMachine struct {
	mu       sync.Mutex
	fence    ClusterCatalogDrainFence
	acked    []byte
	ackCount uint32
}

func NewClusterCatalogDrainMachine(fence ClusterCatalogDrainFence) (*ClusterCatalogDrainMachine, error) {
	if !fence.valid() {
		return nil, ErrClusterCatalogDrainFence
	}
	return &ClusterCatalogDrainMachine{
		fence: fence, acked: make([]byte, (len(fence.members)+7)/8),
	}, nil
}

func (machine *ClusterCatalogDrainMachine) ApplyAuthenticated(
	peer rafttransport.PeerIdentity,
	ack ClusterCatalogDrainAck,
) (bool, error) {
	if machine == nil {
		return false, ErrClusterCatalogDrainState
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if peer.TrustDomain != machine.fence.trust || peer.Node != ack.Member.Node {
		return false, ErrClusterCatalogDrainAuth
	}
	index, found := machine.fence.memberIndex(ack.Member)
	if !found || ack.FenceDigest != machine.fence.digest ||
		ack.CurrentGeneration < machine.fence.generation {
		return false, ErrClusterCatalogDrainAck
	}
	mask := byte(1 << (uint(index) & 7))
	if machine.acked[index>>3]&mask != 0 {
		return machine.ackCount == uint32(len(machine.fence.members)), nil
	}
	machine.acked[index>>3] |= mask
	machine.ackCount++
	return machine.ackCount == uint32(len(machine.fence.members)), nil
}

func (machine *ClusterCatalogDrainMachine) Progress() (acknowledged, required uint32) {
	if machine == nil {
		return 0, 0
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	return machine.ackCount, uint32(len(machine.fence.members))
}

func (machine *ClusterCatalogDrainMachine) Complete() bool {
	acknowledged, required := machine.Progress()
	return required != 0 && acknowledged == required
}

func (machine *ClusterCatalogDrainMachine) Certificate() ([sha256.Size]byte, bool) {
	if machine == nil {
		return [sha256.Size]byte{}, false
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if machine.ackCount != uint32(len(machine.fence.members)) {
		return [sha256.Size]byte{}, false
	}
	hash := sha256.New()
	hash.Write([]byte("vibedb/catalog-drain/certificate\x00"))
	hash.Write(machine.fence.digest[:])
	hash.Write(machine.fence.rosterDigest[:])
	var digest [sha256.Size]byte
	hash.Sum(digest[:0])
	return digest, true
}

// AppendClusterCatalogDrainState serializes a crash-replay checkpoint. The
// roster is canonical and acknowledgements are one bit per gateway.
func AppendClusterCatalogDrainState(dst []byte, machine *ClusterCatalogDrainMachine) ([]byte, error) {
	if machine == nil {
		return dst, ErrClusterCatalogDrainState
	}
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if !machine.fence.valid() || len(machine.acked) != (len(machine.fence.members)+7)/8 ||
		machine.ackCount > uint32(len(machine.fence.members)) {
		return dst, ErrClusterCatalogDrainState
	}
	size := clusterCatalogDrainHeaderBytes + len(machine.fence.members)*24 + len(machine.acked) + sha256.Size
	if len(dst) > math.MaxInt-size {
		return dst, ErrClusterCatalogDrainState
	}
	start := len(dst)
	dst = append(dst, make([]byte, size)...)
	raw := dst[start:]
	copy(raw[:8], clusterCatalogDrainStateMagic[:])
	copy(raw[8:40], machine.fence.operation[:])
	binary.LittleEndian.PutUint64(raw[40:48], machine.fence.generation)
	copy(raw[48:80], machine.fence.catalogDigest[:])
	copy(raw[80:96], machine.fence.trust.ClusterID[:])
	copy(raw[96:112], machine.fence.trust.ClusterIncarnation[:])
	copy(raw[112:144], machine.fence.rosterDigest[:])
	binary.LittleEndian.PutUint32(raw[144:148], uint32(len(machine.fence.members)))
	binary.LittleEndian.PutUint32(raw[148:152], machine.ackCount)
	binary.LittleEndian.PutUint32(raw[152:156], uint32(len(machine.acked)))
	offset := clusterCatalogDrainHeaderBytes
	for _, member := range machine.fence.members {
		copy(raw[offset:offset+16], member.Node[:])
		binary.LittleEndian.PutUint64(raw[offset+16:offset+24], member.Incarnation)
		offset += 24
	}
	copy(raw[offset:offset+len(machine.acked)], machine.acked)
	offset += len(machine.acked)
	checksum := sha256.Sum256(raw[:offset])
	copy(raw[offset:], checksum[:])
	return dst, nil
}

func OpenClusterCatalogDrainState(raw []byte) (*ClusterCatalogDrainMachine, error) {
	if len(raw) < clusterCatalogDrainHeaderBytes+1+sha256.Size ||
		!bytes.Equal(raw[:8], clusterCatalogDrainStateMagic[:]) {
		return nil, ErrClusterCatalogDrainState
	}
	count := binary.LittleEndian.Uint32(raw[144:148])
	ackCount := binary.LittleEndian.Uint32(raw[148:152])
	bitBytes := binary.LittleEndian.Uint32(raw[152:156])
	if count == 0 || count > MaxClusterCatalogDrainGateways ||
		bitBytes != (count+7)/8 || ackCount > count {
		return nil, ErrClusterCatalogDrainState
	}
	want := uint64(clusterCatalogDrainHeaderBytes) + uint64(count)*24 + uint64(bitBytes) + sha256.Size
	if want != uint64(len(raw)) {
		return nil, ErrClusterCatalogDrainState
	}
	checksumOffset := len(raw) - sha256.Size
	if sha256.Sum256(raw[:checksumOffset]) != [sha256.Size]byte(raw[checksumOffset:]) {
		return nil, ErrClusterCatalogDrainState
	}
	var operation, catalogDigest, encodedRoster [sha256.Size]byte
	copy(operation[:], raw[8:40])
	copy(catalogDigest[:], raw[48:80])
	var trust rafttransport.TrustDomain
	copy(trust.ClusterID[:], raw[80:96])
	copy(trust.ClusterIncarnation[:], raw[96:112])
	copy(encodedRoster[:], raw[112:144])
	members := make([]ClusterCatalogDrainMember, count)
	offset := clusterCatalogDrainHeaderBytes
	for index := range members {
		copy(members[index].Node[:], raw[offset:offset+16])
		members[index].Incarnation = binary.LittleEndian.Uint64(raw[offset+16 : offset+24])
		offset += 24
	}
	fence, err := NewClusterCatalogDrainFence(
		operation, binary.LittleEndian.Uint64(raw[40:48]), catalogDigest, trust, members,
	)
	if err != nil || fence.rosterDigest != encodedRoster {
		return nil, ErrClusterCatalogDrainState
	}
	acked := slices.Clone(raw[offset : offset+int(bitBytes)])
	if count&7 != 0 && acked[len(acked)-1]&^byte((1<<(count&7))-1) != 0 {
		return nil, ErrClusterCatalogDrainState
	}
	var actual uint32
	for _, bits := range acked {
		actual += uint32(bitsOnesCount8(bits))
	}
	if actual != ackCount {
		return nil, ErrClusterCatalogDrainState
	}
	return &ClusterCatalogDrainMachine{
		fence: fence, acked: acked, ackCount: ackCount,
	}, nil
}

func bitsOnesCount8(value byte) int {
	value = value - (value>>1)&0x55
	value = (value & 0x33) + (value>>2)&0x33
	return int((value + (value >> 4)) & 0x0f)
}
