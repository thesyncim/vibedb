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
	"sync/atomic"

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

// ClusterCatalogDrainRequest is the exact replica-move action cut presented to
// the cluster drainer. Step prevents a certificate from one journaled action
// attempt from settling another within the same operation and catalog.
type ClusterCatalogDrainRequest struct {
	Operation     [sha256.Size]byte
	Step          [sha256.Size]byte
	Generation    uint64
	CatalogDigest [sha256.Size]byte
}

func (request ClusterCatalogDrainRequest) Valid() bool {
	return request.Operation != ([sha256.Size]byte{}) &&
		request.Step != ([sha256.Size]byte{}) && request.Generation != 0 &&
		request.CatalogDigest != ([sha256.Size]byte{})
}

func (request ClusterCatalogDrainRequest) fenceOperation() [sha256.Size]byte {
	hash := sha256.New()
	hash.Write([]byte("vibedb/catalog-drain/move-step\x00"))
	hash.Write(request.Operation[:])
	hash.Write(request.Step[:])
	var operation [sha256.Size]byte
	hash.Sum(operation[:0])
	return operation
}

// ClusterCatalogDrainCertificate is emitted only after every exact roster
// member has authenticated and acknowledged the request cut.
type ClusterCatalogDrainCertificate struct {
	Request      ClusterCatalogDrainRequest
	FenceDigest  [sha256.Size]byte
	RosterDigest [sha256.Size]byte
	Proof        [sha256.Size]byte
}

func (certificate ClusterCatalogDrainCertificate) ValidFor(request ClusterCatalogDrainRequest) bool {
	return request.Valid() && certificate.Request == request &&
		certificate.FenceDigest != ([sha256.Size]byte{}) &&
		certificate.RosterDigest != ([sha256.Size]byte{}) &&
		certificate.Proof != ([sha256.Size]byte{})
}

// CatalogSnapshotDigest returns the digest of the sole canonical vibejson
// catalog representation. Replica-move drains use it to distinguish G+1 and
// G+2 even if a generation number is accidentally reused by a faulty caller.
func CatalogSnapshotDigest(snapshot *Snapshot) ([sha256.Size]byte, error) {
	raw, err := AppendSnapshotDocument(nil, snapshot)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(raw), nil
}

// ClusterCatalogDrainCollector fans one immutable fence to the serving
// gateway roster over authenticated control streams. The implementation must
// pass the mTLS peer identity with each acknowledgement, bound its own
// concurrency and bytes, and finish all accept calls before returning.
type ClusterCatalogDrainCollector interface {
	CollectClusterCatalogDrain(
		context.Context,
		ClusterCatalogDrainFence,
		func(rafttransport.PeerIdentity, ClusterCatalogDrainAck) error,
	) error
}

// ClusterCatalogDrainCoordinator composes transport collection with the exact
// replayable state machine. Retries may recollect the full roster: local drain
// is monotonic and acknowledgements are idempotent, so coordinator crashes do
// not weaken the fence or require a second local journal.
type ClusterCatalogDrainCoordinator struct {
	mu        sync.RWMutex
	trust     rafttransport.TrustDomain
	members   []ClusterCatalogDrainMember
	collector ClusterCatalogDrainCollector
}

// Members returns the current future-fence roster. A returned slice is a
// detached cut; certificates already in flight retain the immutable roster
// captured by their own ClusterCatalogDrainFence.
func (coordinator *ClusterCatalogDrainCoordinator) Members() []ClusterCatalogDrainMember {
	if coordinator == nil {
		return nil
	}
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	return slices.Clone(coordinator.members)
}

// UpdateMembers installs a complete authenticated gateway roster for future
// drain fences. It never mutates an existing fence or removes a participant
// from an outstanding obligation. The caller must retain historical endpoint
// addresses in its member-aware opener until those fences settle.
func (coordinator *ClusterCatalogDrainCoordinator) UpdateMembers(
	members []ClusterCatalogDrainMember,
) error {
	if coordinator == nil {
		return ErrClusterCatalogDrainFence
	}
	// An empty current roster is a valid directory state. It means no new
	// cluster drain fence can be certified, while already-created fences retain
	// their immutable member set and continue to settle through historical
	// endpoints.
	if len(members) == 0 {
		coordinator.mu.Lock()
		coordinator.members = nil
		coordinator.mu.Unlock()
		return nil
	}
	validation, err := NewClusterCatalogDrainFence(
		[sha256.Size]byte{1}, 1, [sha256.Size]byte{1}, coordinator.trust, members,
	)
	if err != nil {
		return err
	}
	coordinator.mu.Lock()
	coordinator.members = slices.Clone(validation.members)
	coordinator.mu.Unlock()
	return nil
}

func NewClusterCatalogDrainCoordinator(
	trust rafttransport.TrustDomain,
	members []ClusterCatalogDrainMember,
	collector ClusterCatalogDrainCollector,
) (*ClusterCatalogDrainCoordinator, error) {
	if collector == nil {
		return nil, ErrClusterCatalogDrainFence
	}
	validation, err := NewClusterCatalogDrainFence(
		[sha256.Size]byte{1}, 1, [sha256.Size]byte{1}, trust, members,
	)
	if err != nil {
		return nil, err
	}
	return &ClusterCatalogDrainCoordinator{
		trust: validation.trust, members: slices.Clone(validation.members), collector: collector,
	}, nil
}

func (coordinator *ClusterCatalogDrainCoordinator) CertifyClusterCatalogDrain(
	ctx context.Context,
	request ClusterCatalogDrainRequest,
) (ClusterCatalogDrainCertificate, error) {
	if coordinator == nil || coordinator.collector == nil || ctx == nil || !request.Valid() {
		return ClusterCatalogDrainCertificate{}, ErrClusterCatalogDrainFence
	}
	coordinator.mu.RLock()
	members := slices.Clone(coordinator.members)
	coordinator.mu.RUnlock()
	fence, err := NewClusterCatalogDrainFence(
		request.fenceOperation(), request.Generation, request.CatalogDigest,
		coordinator.trust, members,
	)
	if err != nil {
		return ClusterCatalogDrainCertificate{}, err
	}
	machine, err := NewClusterCatalogDrainMachine(fence)
	if err != nil {
		return ClusterCatalogDrainCertificate{}, err
	}
	var accepting atomic.Bool
	accepting.Store(true)
	accept := func(peer rafttransport.PeerIdentity, ack ClusterCatalogDrainAck) error {
		if !accepting.Load() {
			return ErrClusterCatalogDrainAck
		}
		_, applyErr := machine.ApplyAuthenticated(peer, ack)
		return applyErr
	}
	if requestCollector, ok := coordinator.collector.(ClusterCatalogDrainRequestCollector); ok {
		err = requestCollector.CollectClusterCatalogDrainRequest(ctx, request, fence, accept)
	} else {
		err = coordinator.collector.CollectClusterCatalogDrain(ctx, fence, accept)
	}
	accepting.Store(false)
	if err != nil {
		return ClusterCatalogDrainCertificate{}, err
	}
	proof, complete := machine.Certificate()
	if !complete {
		return ClusterCatalogDrainCertificate{}, ErrClusterCatalogDrainState
	}
	return ClusterCatalogDrainCertificate{
		Request: request, FenceDigest: fence.digest,
		RosterDigest: fence.rosterDigest, Proof: proof,
	}, nil
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
	return clusterCatalogDrainCertificateProof(
		machine.fence.digest, machine.fence.rosterDigest,
	), true
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
