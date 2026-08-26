// Package replicaaction exposes the two mutating shard-control operations used
// after replica catch-up: ownership transition and retired-source closure.
// Every request is fixed-grammar, authenticated, bounded, and durably journaled
// before it can reach the serialized raftservice Owner.
package replicaaction

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"

	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

var (
	ErrControl        = errors.New("replicaaction: invalid control request")
	ErrUnauthorized   = errors.New("replicaaction: control request is not authorized")
	ErrConflict       = errors.New("replicaaction: durable request identity conflicts")
	ErrMissing        = errors.New("replicaaction: durable request is missing")
	ErrOutcomeUnknown = errors.New("replicaaction: control outcome is unknown")
	ErrBound          = errors.New("replicaaction: control bound exceeded")
)

const (
	requestHeaderBytes    = 328
	ResponseBytes         = 96
	MaxRequestBytes       = requestHeaderBytes + replicatedstate.MaxOwnershipTransitionBytes
	AbsoluteMaxConcurrent = 256
)

var requestMagic = [8]byte{'V', 'B', 'R', 'A', 'C', 'T', 0, 0}
var responseMagic = [8]byte{'V', 'B', 'R', 'D', 'O', 'N', 'E', 0}

// RequestDiscriminator identifies this fixed grammar on the shared
// authenticated shard-control listener.
func RequestDiscriminator() [8]byte { return requestMagic }

type Kind uint8

const (
	OwnershipTransition Kind = iota + 1
	SourceRetirement
)

type State uint8

const (
	Running State = iota + 1
	Complete
)

type Request struct {
	Operation    [32]byte
	Step         [32]byte
	Kind         Kind
	Fence        raftservice.ServingFence
	SourceMember uint64
	TargetMember uint64
	Command      []byte
}

type Record struct {
	Request  Request
	Revision uint64
	State    State
}

// Journal durably publishes before returning nil. Publish may have committed
// when it returns an error; the service always resolves with an exact read.
type Journal interface {
	ReadReplicaAction(context.Context, [32]byte) (Record, error)
	PublishReplicaAction(context.Context, uint64, Record) error
}

type Owner interface {
	ProposeOwnershipTransition(context.Context, raftservice.ServingFence, []byte) error
	ObserveReplica(context.Context, raftmember.GroupKey, uint64) (raftservice.ReplicaObservation, error)
	RetireReplicaSource(context.Context, raftservice.ReplicaRetirementRequest) error
}

type AuthorizeFunc func(rafttransport.PeerIdentity, Request) bool

type Options struct {
	Journal       Journal
	Owner         Owner
	Authorize     AuthorizeFunc
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	MaxConcurrent int
}

type Service struct {
	journal                     Journal
	owner                       Owner
	authorize                   AuthorizeFunc
	readDeadline, writeDeadline rafttransport.DeadlineFunc
	slots                       chan struct{}
	stripes                     []sync.Mutex
}

func NewService(options Options) (*Service, error) {
	if options.Journal == nil || options.Owner == nil || options.Authorize == nil ||
		options.ReadDeadline == nil || options.WriteDeadline == nil ||
		options.MaxConcurrent <= 0 || options.MaxConcurrent > AbsoluteMaxConcurrent {
		return nil, ErrControl
	}
	return &Service{journal: options.Journal, owner: options.Owner, authorize: options.Authorize,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		slots: make(chan struct{}, options.MaxConcurrent), stripes: make([]sync.Mutex, options.MaxConcurrent)}, nil
}

func (service *Service) Serve(ctx context.Context, connection rafttransport.PeerConnection) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrUnauthorized
	}
	defer connection.Close()
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if deadline := service.readDeadline(); deadline.IsZero() {
		return ErrControl
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	request, err := ReadRequest(connection)
	if err != nil {
		return err
	}
	peer := connection.PeerIdentity()
	domain := rafttransport.TrustDomain{ClusterID: request.Fence.Group.ClusterID,
		ClusterIncarnation: request.Fence.Group.ClusterIncarnation}
	if peer.TrustDomain != domain || !service.authorize(peer, request) {
		return ErrUnauthorized
	}
	record, err := service.Execute(ctx, request)
	if err != nil {
		return err
	}
	if deadline := service.writeDeadline(); deadline.IsZero() {
		return ErrControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return WriteResponse(connection, record)
}

func (service *Service) Execute(ctx context.Context, request Request) (Record, error) {
	if service == nil || ctx == nil || !validRequest(request) {
		return Record{}, ErrControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return Record{}, cause
	}
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return Record{}, ErrBound
	}
	stripe := &service.stripes[binary.LittleEndian.Uint64(request.Operation[:8])%uint64(len(service.stripes))]
	stripe.Lock()
	defer stripe.Unlock()
	record, err := service.loadOrCreate(ctx, request)
	if err != nil || record.State == Complete {
		return record, err
	}
	switch request.Kind {
	case OwnershipTransition:
		settled, settleErr := service.ownershipSettled(ctx, request)
		if settleErr != nil {
			return Record{}, settleErr
		}
		if !settled {
			if err = service.owner.ProposeOwnershipTransition(ctx, request.Fence, request.Command); err != nil &&
				!errors.Is(err, raftservice.ErrOutcomeUnknown) {
				return Record{}, err
			}
			settled, settleErr = service.ownershipSettled(ctx, request)
			if settleErr != nil {
				return Record{}, settleErr
			}
			if !settled {
				return Record{}, errors.Join(ErrOutcomeUnknown, err)
			}
		}
	case SourceRetirement:
		err = service.owner.RetireReplicaSource(ctx, raftservice.ReplicaRetirementRequest{
			Operation: request.Operation, Step: request.Step, Fence: request.Fence,
			SourceMember: request.SourceMember, TargetMember: request.TargetMember})
		// The exact Running journal was durable before the call. A missing group
		// therefore proves this same retirement crossed the local close boundary.
		if err != nil && !errors.Is(err, multiraft.ErrGroupNotFound) {
			return Record{}, err
		}
	default:
		return Record{}, ErrControl
	}
	terminal := record
	terminal.Revision++
	terminal.State = Complete
	if err = service.publishExact(ctx, record.Revision, terminal); err != nil {
		return Record{}, err
	}
	return terminal, nil
}

func (service *Service) loadOrCreate(ctx context.Context, request Request) (Record, error) {
	record, err := service.journal.ReadReplicaAction(ctx, request.Operation)
	if err == nil {
		if !validRecord(record) || !equalRequest(record.Request, request) {
			return Record{}, ErrConflict
		}
		return record, nil
	}
	if !errors.Is(err, ErrMissing) {
		return Record{}, err
	}
	running := Record{Request: cloneRequest(request), Revision: 1, State: Running}
	if err = service.publishExact(ctx, 0, running); err != nil {
		return Record{}, err
	}
	return running, nil
}

func (service *Service) publishExact(ctx context.Context, expected uint64, next Record) error {
	err := service.journal.PublishReplicaAction(ctx, expected, next)
	if err == nil {
		return nil
	}
	observed, readErr := service.journal.ReadReplicaAction(ctx, next.Request.Operation)
	if readErr == nil && validRecord(observed) && equalRecord(observed, next) {
		return nil
	}
	return errors.Join(err, readErr, ErrOutcomeUnknown)
}

func (service *Service) ownershipSettled(ctx context.Context, request Request) (bool, error) {
	transition, err := replicatedstate.OpenOwnershipTransition(request.Command)
	if err != nil {
		return false, err
	}
	cut, err := service.owner.ObserveReplica(ctx, request.Fence.Group, request.TargetMember)
	if err != nil {
		return false, err
	}
	b := cut.State.Binding
	if !bindingIdentityMatchesTransition(b, transition) {
		return false, ErrConflict
	}
	if b.OwnershipEpoch == transition.ToOwnershipEpoch &&
		b.RoutingVersion == transition.ToRoutingVersion &&
		b.RouteGeneration == transition.ToRouteGeneration &&
		cut.State.ReplicaSetVersion == transition.ExpectedReplicaSetVersion {
		return true, nil
	}
	if b.OwnershipEpoch != transition.OwnershipEpoch || b.RoutingVersion != transition.RoutingVersion ||
		b.RouteGeneration != transition.RouteGeneration ||
		cut.State.ReplicaSetVersion != transition.ExpectedReplicaSetVersion {
		return false, ErrConflict
	}
	return false, nil
}

func bindingIdentityMatchesTransition(
	b replicatedstate.Binding,
	transition replicatedstate.OwnershipTransitionView,
) bool {
	return b.ClusterID == transition.ClusterID &&
		b.ClusterIncarnation == transition.ClusterIncarnation &&
		b.TopologyRecoveryEpoch == transition.TopologyRecoveryEpoch &&
		b.Distribution == string(transition.Distribution) && b.Shard == string(transition.Shard) &&
		b.AllocationGeneration == transition.AllocationGeneration &&
		b.ShardIncarnation == transition.ShardIncarnation && b.GroupID == transition.GroupID &&
		b.ActivePolicyGeneration == transition.ActivePolicyGeneration &&
		b.ProtectionEpoch == transition.ProtectionEpoch &&
		b.SchemaGeneration == transition.SchemaGeneration
}

func validRequest(request Request) bool {
	if request.Operation == ([32]byte{}) || request.Step == ([32]byte{}) ||
		request.SourceMember == 0 || request.TargetMember == 0 ||
		request.SourceMember == request.TargetMember || !request.Fence.Command.Valid() ||
		!validGroup(request.Fence.Group) || request.Fence.AllocationGeneration == 0 ||
		request.Fence.MemberID == 0 || request.Fence.StoreID == ([16]byte{}) ||
		request.Fence.NodeIncarnation == 0 || request.Fence.Term == 0 {
		return false
	}
	switch request.Kind {
	case OwnershipTransition:
		transition, err := replicatedstate.OpenOwnershipTransition(request.Command)
		return err == nil && transition.SourceMember == request.SourceMember &&
			transition.TargetMember == request.TargetMember &&
			transitionMatchesFence(transition, request.Fence)
	case SourceRetirement:
		return len(request.Command) == 0 && request.Fence.MemberID == request.SourceMember
	default:
		return false
	}
}

func validGroup(group raftmember.GroupKey) bool {
	return group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{})
}

func transitionMatchesFence(
	transition replicatedstate.OwnershipTransitionView,
	fence raftservice.ServingFence,
) bool {
	return transition.ClusterID == fence.Group.ClusterID &&
		transition.ClusterIncarnation == fence.Group.ClusterIncarnation &&
		transition.TopologyRecoveryEpoch == fence.Group.TopologyRecoveryEpoch &&
		transition.ShardIncarnation == fence.Group.ShardIncarnation &&
		transition.GroupID == fence.Group.GroupID &&
		transition.AllocationGeneration == fence.AllocationGeneration &&
		transition.ExpectedReplicaSetVersion == fence.Command.ReplicaSetVersion &&
		transition.ActivePolicyGeneration == fence.Command.ActivePolicyGeneration &&
		transition.ProtectionEpoch == fence.Command.ProtectionEpoch &&
		transition.OwnershipEpoch == fence.Command.OwnershipEpoch &&
		transition.SchemaGeneration == fence.Command.SchemaGeneration &&
		transition.RoutingVersion == fence.Command.RoutingVersion &&
		transition.RouteGeneration == fence.Command.RouteGeneration
}

func validRecord(record Record) bool {
	return record.Revision != 0 && record.State >= Running && record.State <= Complete && validRequest(record.Request)
}
func cloneRequest(request Request) Request {
	request.Command = append([]byte(nil), request.Command...)
	return request
}
func equalRequest(a, b Request) bool {
	return a.Operation == b.Operation && a.Step == b.Step && a.Kind == b.Kind &&
		a.Fence == b.Fence && a.SourceMember == b.SourceMember &&
		a.TargetMember == b.TargetMember && bytes.Equal(a.Command, b.Command)
}
func equalRecord(a, b Record) bool {
	return a.Revision == b.Revision && a.State == b.State && equalRequest(a.Request, b.Request)
}

func AppendRequest(dst []byte, request Request) ([]byte, error) {
	if !validRequest(request) || len(dst) > math.MaxInt-requestHeaderBytes-len(request.Command) {
		return dst, ErrControl
	}
	start := len(dst)
	dst = append(dst, make([]byte, requestHeaderBytes+len(request.Command))...)
	out := dst[start:]
	copy(out[:8], requestMagic[:])
	out[8] = byte(request.Kind)
	copy(out[16:48], request.Operation[:])
	copy(out[48:80], request.Step[:])
	appendGroup(out[80:168], request.Fence.Group)
	binary.LittleEndian.PutUint64(out[168:176], request.Fence.AllocationGeneration)
	values := [...]uint64{request.Fence.Command.ReplicaSetVersion, request.Fence.Command.ActivePolicyGeneration,
		request.Fence.Command.ProtectionEpoch, request.Fence.Command.OwnershipEpoch,
		request.Fence.Command.SchemaGeneration, request.Fence.Command.RoutingVersion,
		request.Fence.Command.RouteGeneration}
	for i, value := range values {
		binary.LittleEndian.PutUint64(out[176+i*8:184+i*8], value)
	}
	copy(out[232:264], request.Fence.Command.RelationManifestDigest[:])
	binary.LittleEndian.PutUint64(out[264:272], request.Fence.MemberID)
	copy(out[272:288], request.Fence.StoreID[:])
	binary.LittleEndian.PutUint64(out[288:296], request.Fence.NodeIncarnation)
	binary.LittleEndian.PutUint64(out[296:304], request.Fence.Term)
	binary.LittleEndian.PutUint64(out[304:312], request.SourceMember)
	binary.LittleEndian.PutUint64(out[312:320], request.TargetMember)
	binary.LittleEndian.PutUint32(out[320:324], uint32(len(request.Command)))
	copy(out[requestHeaderBytes:], request.Command)
	return dst, nil
}

func OpenRequest(raw []byte) (Request, error) {
	if len(raw) < requestHeaderBytes || len(raw) > MaxRequestBytes || !bytes.Equal(raw[:8], requestMagic[:]) ||
		!zero(raw[9:16]) || !zero(raw[152:168]) || !zero(raw[324:328]) || int(binary.LittleEndian.Uint32(raw[320:324])) != len(raw)-requestHeaderBytes {
		return Request{}, ErrControl
	}
	var request Request
	request.Kind = Kind(raw[8])
	copy(request.Operation[:], raw[16:48])
	copy(request.Step[:], raw[48:80])
	request.Fence.Group = openGroup(raw[80:168])
	request.Fence.AllocationGeneration = binary.LittleEndian.Uint64(raw[168:176])
	values := []*uint64{&request.Fence.Command.ReplicaSetVersion, &request.Fence.Command.ActivePolicyGeneration,
		&request.Fence.Command.ProtectionEpoch, &request.Fence.Command.OwnershipEpoch,
		&request.Fence.Command.SchemaGeneration, &request.Fence.Command.RoutingVersion,
		&request.Fence.Command.RouteGeneration}
	for i := range values {
		*values[i] = binary.LittleEndian.Uint64(raw[176+i*8 : 184+i*8])
	}
	copy(request.Fence.Command.RelationManifestDigest[:], raw[232:264])
	request.Fence.MemberID = binary.LittleEndian.Uint64(raw[264:272])
	copy(request.Fence.StoreID[:], raw[272:288])
	request.Fence.NodeIncarnation = binary.LittleEndian.Uint64(raw[288:296])
	request.Fence.Term = binary.LittleEndian.Uint64(raw[296:304])
	request.SourceMember = binary.LittleEndian.Uint64(raw[304:312])
	request.TargetMember = binary.LittleEndian.Uint64(raw[312:320])
	request.Command = raw[requestHeaderBytes:len(raw):len(raw)]
	if !validRequest(request) {
		return Request{}, ErrControl
	}
	return request, nil
}

func ReadRequest(reader io.Reader) (Request, error) {
	var header [requestHeaderBytes]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return Request{}, err
	}
	size := int(binary.LittleEndian.Uint32(header[320:324]))
	if size < 0 || size > replicatedstate.MaxOwnershipTransitionBytes {
		return Request{}, ErrBound
	}
	raw := make([]byte, requestHeaderBytes+size)
	copy(raw, header[:])
	if _, err := io.ReadFull(reader, raw[requestHeaderBytes:]); err != nil {
		return Request{}, err
	}
	return OpenRequest(raw)
}
func WriteRequest(writer io.Writer, request Request) error {
	raw, err := AppendRequest(nil, request)
	if err != nil {
		return err
	}
	return writeFull(writer, raw)
}

func AppendResponse(dst []byte, record Record) ([]byte, error) {
	if !validRecord(record) || record.State != Complete || len(dst) > math.MaxInt-ResponseBytes {
		return dst, ErrControl
	}
	start := len(dst)
	dst = append(dst, make([]byte, ResponseBytes)...)
	out := dst[start:]
	copy(out[:8], responseMagic[:])
	copy(out[8:40], record.Request.Operation[:])
	copy(out[40:72], record.Request.Step[:])
	out[72] = byte(record.Request.Kind)
	out[73] = byte(record.State)
	binary.LittleEndian.PutUint64(out[80:88], record.Revision)
	return dst, nil
}
func OpenResponse(raw []byte) (Record, error) {
	if len(raw) != ResponseBytes || !bytes.Equal(raw[:8], responseMagic[:]) || !zero(raw[74:80]) || !zero(raw[88:]) {
		return Record{}, ErrControl
	}
	var record Record
	copy(record.Request.Operation[:], raw[8:40])
	copy(record.Request.Step[:], raw[40:72])
	record.Request.Kind = Kind(raw[72])
	record.State = State(raw[73])
	record.Revision = binary.LittleEndian.Uint64(raw[80:88])
	if record.Request.Operation == ([32]byte{}) || record.Request.Step == ([32]byte{}) || record.Request.Kind < OwnershipTransition || record.Request.Kind > SourceRetirement || record.State != Complete || record.Revision < 2 {
		return Record{}, ErrControl
	}
	return record, nil
}
func ReadResponse(reader io.Reader) (Record, error) {
	var raw [ResponseBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return Record{}, err
	}
	return OpenResponse(raw[:])
}
func WriteResponse(writer io.Writer, record Record) error {
	var raw [ResponseBytes]byte
	encoded, err := AppendResponse(raw[:0], record)
	if err != nil {
		return err
	}
	return writeFull(writer, encoded)
}

func appendGroup(dst []byte, group raftmember.GroupKey) {
	copy(dst[0:16], group.ClusterID[:])
	copy(dst[16:32], group.ClusterIncarnation[:])
	binary.LittleEndian.PutUint64(dst[32:40], group.TopologyRecoveryEpoch)
	copy(dst[40:56], group.ShardIncarnation[:])
	copy(dst[56:72], group.GroupID[:])
}
func openGroup(src []byte) raftmember.GroupKey {
	var group raftmember.GroupKey
	copy(group.ClusterID[:], src[0:16])
	copy(group.ClusterIncarnation[:], src[16:32])
	group.TopologyRecoveryEpoch = binary.LittleEndian.Uint64(src[32:40])
	copy(group.ShardIncarnation[:], src[40:56])
	copy(group.GroupID[:], src[56:72])
	return group
}
func zero(raw []byte) bool {
	for _, b := range raw {
		if b != 0 {
			return false
		}
	}
	return true
}
func writeFull(writer io.Writer, raw []byte) error {
	for len(raw) != 0 {
		n, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(raw) {
			return io.ErrShortWrite
		}
		raw = raw[n:]
	}
	return nil
}
