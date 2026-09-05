// Package replicacontrol exposes bounded, authenticated, read-only Raft
// observation for restart-safe replica movement. It grants no data-serving,
// membership, or topology authority.
package replicacontrol

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftmodel"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var (
	ErrControl      = errors.New("replicacontrol: invalid control observation")
	ErrUnauthorized = errors.New("replicacontrol: observation is not authorized")
	ErrStale        = errors.New("replicacontrol: observation is stale")
	ErrBound        = errors.New("replicacontrol: observation bound exceeded")
)

// RequestDiscriminator identifies this fixed grammar on the shared
// authenticated shard-control listener.
func RequestDiscriminator() [8]byte { return requestMagic }

const (
	RequestBytes                   = 184
	responseHeaderBytes            = 280
	MaxSnapshotBaseEnvelopeBytes   = replicatedstate.MaxSnapshotBaseCertificateBytes + 1024
	MaxResponseBytes               = responseHeaderBytes + replicatedstate.MaxStateEnvelopeBytes + MaxSnapshotBaseEnvelopeBytes
	AbsoluteMaxConcurrentObservers = 256
)

var (
	requestMagic  = [8]byte{'V', 'B', 'R', 'O', 'B', 'S', 0, 0}
	responseMagic = [8]byte{'V', 'B', 'R', 'C', 'U', 'T', 0, 0}
)

// Request binds a read-only observation to one durable controller operation
// and exact reconciled step. A nonzero ExpectedReplicaSetVersion requires that
// exact version. Zero performs read-only discovery after a membership change;
// the response always carries the actual nonzero applied version. Discovery
// grants no mutation authority and callers must validate their durable cut.
type Request struct {
	Operation                 [32]byte
	Step                      [32]byte
	Group                     raftmember.GroupKey
	TargetMember              uint64
	ExpectedReplicaSetVersion uint64
	HealthOnly                bool
}

// Observation is a complete local cut. Status.LeaderID/Term/LeadTransferee are
// the transfer witness. Progress is authoritative only when ProgressFound is
// true; State is the local durable target-state witness.
type Observation struct {
	Request       Request
	Publication   raftmodel.Publication
	Status        raftmember.RuntimeStatus
	Progress      raftmodel.MemberProgress
	ProgressFound bool
	State         replicatedstate.State
	SnapshotBase  *replicatedstate.SnapshotBaseCertificate
}

func (observation Observation) TransferWitness(target uint64) (settled, inFlight bool) {
	if target == 0 || observation.Status.Term == 0 {
		return false, false
	}
	return observation.Status.LeaderID == target,
		observation.Status.LeadTransferee == target
}

type Observer interface {
	ObserveReplica(context.Context, raftmember.GroupKey, uint64) (raftservice.ReplicaObservation, error)
}

// HealthObserver supplies the bounded liveness cut used by revision
// controllers. Implementations must not substitute a full snapshot cut.
type HealthObserver interface {
	ObserveReplicaHealth(context.Context, raftmember.GroupKey, uint64) (raftservice.ReplicaHealthObservation, error)
}

type AuthorizeFunc func(rafttransport.PeerIdentity, Request) bool

type ServiceOptions struct {
	Observer      Observer
	Authorize     AuthorizeFunc
	ReadDeadline  rafttransport.DeadlineFunc
	WriteDeadline rafttransport.DeadlineFunc
	MaxConcurrent int
}

type Service struct {
	observer      Observer
	authorize     AuthorizeFunc
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	slots         chan struct{}
	stripes       []sync.Mutex
}

func NewService(options ServiceOptions) (*Service, error) {
	if options.Observer == nil || options.Authorize == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil || options.MaxConcurrent <= 0 ||
		options.MaxConcurrent > AbsoluteMaxConcurrentObservers {
		return nil, ErrControl
	}
	return &Service{
		observer: options.Observer, authorize: options.Authorize,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		slots:   make(chan struct{}, options.MaxConcurrent),
		stripes: make([]sync.Mutex, options.MaxConcurrent),
	}, nil
}

// Serve handles one full observation or a bounded sequence of health observations
// on one mutually authenticated shard-control stream. Independent operations are bounded; exact-operation retries are
// striped so a local cut cannot be reordered within the same durable move.
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
	for index := 0; index < 256; index++ {
		if deadline := boundedDeadline(ctx, service.readDeadline()); deadline.IsZero() {
			return ErrControl
		} else if err := connection.SetReadDeadline(deadline); err != nil {
			if index > 0 && errors.Is(err, io.ErrClosedPipe) {
				return nil
			}
			return err
		}
		request, err := ReadRequest(connection)
		if err != nil {
			if index > 0 && errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if index > 0 && !request.HealthOnly {
			return ErrControl
		}
		if err := service.serveRequest(ctx, connection, request); err != nil {
			return err
		}
		if !request.HealthOnly {
			return nil
		}
	}
	return nil
}

// Admission is held only while observing and replying, never during idle reads.
func (service *Service) serveRequest(ctx context.Context, connection rafttransport.PeerConnection, request Request) error {
	var err error
	peer := connection.PeerIdentity()
	wantDomain := rafttransport.TrustDomain{
		ClusterID: request.Group.ClusterID, ClusterIncarnation: request.Group.ClusterIncarnation,
	}
	if peer.TrustDomain != wantDomain || !service.authorize(peer, request) {
		return ErrUnauthorized
	}
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return ErrBound
	}
	stripe := &service.stripes[binary.BigEndian.Uint64(request.Operation[:8])%uint64(len(service.stripes))]
	stripe.Lock()
	if request.HealthOnly {
		healthObserver, supported := service.observer.(HealthObserver)
		if !supported {
			stripe.Unlock()
			return ErrControl
		}
		cut, observeErr := healthObserver.ObserveReplicaHealth(ctx, request.Group, request.TargetMember)
		stripe.Unlock()
		if observeErr != nil {
			return observeErr
		}
		health := HealthObservation{
			Request: request, MemberID: cut.Status.MemberID, LeaderID: cut.Status.LeaderID,
			Term: cut.Status.Term, Commit: cut.Status.Commit, Applied: cut.Status.Applied,
			ReplicaSetVersion: cut.Publication.ReplicaSetVersion,
		}
		if cut.Identity.MemberID != health.MemberID ||
			cut.Identity.Group != request.Group || !validHealthObservation(health) {
			return ErrStale
		}
		if deadline := boundedDeadline(ctx, service.writeDeadline()); deadline.IsZero() {
			return ErrControl
		} else if err = connection.SetWriteDeadline(deadline); err != nil {
			return err
		}
		return WriteHealthObservation(connection, health)
	}
	cut, err := service.observer.ObserveReplica(ctx, request.Group, request.TargetMember)
	stripe.Unlock()
	if err != nil {
		return err
	}
	observation := Observation{Request: request, Publication: cut.Publication,
		Status: cut.Status, Progress: cut.TargetProgress, ProgressFound: cut.ProgressFound,
		State: cut.State, SnapshotBase: cut.SnapshotBase}
	if request.ExpectedReplicaSetVersion == 0 {
		observation.Request.ExpectedReplicaSetVersion = cut.Publication.ReplicaSetVersion
	}
	if !validObservation(observation) ||
		(request.ExpectedReplicaSetVersion != 0 &&
			observation.Publication.ReplicaSetVersion != request.ExpectedReplicaSetVersion) {
		return ErrStale
	}
	if deadline := boundedDeadline(ctx, service.writeDeadline()); deadline.IsZero() {
		return ErrControl
	} else if err = connection.SetWriteDeadline(deadline); err != nil {
		return err
	}
	return WriteResponse(connection, observation)
}

func AppendRequest(dst []byte, request Request) ([]byte, error) {
	if !validRequest(request) || len(dst) > math.MaxInt-RequestBytes {
		return dst, ErrControl
	}
	start := len(dst)
	dst = append(dst, make([]byte, RequestBytes)...)
	b := dst[start:]
	copy(b[:8], requestMagic[:])
	copy(b[8:40], request.Operation[:])
	copy(b[40:72], request.Step[:])
	appendGroup(b[72:160], request.Group)
	binary.BigEndian.PutUint64(b[160:168], request.TargetMember)
	binary.BigEndian.PutUint64(b[168:176], request.ExpectedReplicaSetVersion)
	if request.HealthOnly {
		b[176] = 1
	}
	return dst, nil
}

func OpenRequest(raw []byte) (Request, error) {
	if len(raw) != RequestBytes || !bytes.Equal(raw[:8], requestMagic[:]) ||
		!allZero(raw[144:160]) || raw[176] > 1 || !allZero(raw[177:]) {
		return Request{}, ErrControl
	}
	var request Request
	copy(request.Operation[:], raw[8:40])
	copy(request.Step[:], raw[40:72])
	request.Group = openGroup(raw[72:160])
	request.TargetMember = binary.BigEndian.Uint64(raw[160:168])
	request.ExpectedReplicaSetVersion = binary.BigEndian.Uint64(raw[168:176])
	request.HealthOnly = raw[176] == 1
	if !validRequest(request) {
		return Request{}, ErrControl
	}
	return request, nil
}

func ReadRequest(reader io.Reader) (Request, error) {
	var raw [RequestBytes]byte
	if _, err := io.ReadFull(reader, raw[:]); err != nil {
		return Request{}, err
	}
	return OpenRequest(raw[:])
}

func WriteRequest(writer io.Writer, request Request) error {
	var raw [RequestBytes]byte
	encoded, err := AppendRequest(raw[:0], request)
	if err != nil {
		return err
	}
	return writeFull(writer, encoded)
}

func AppendResponse(dst []byte, observation Observation) ([]byte, error) {
	if !validObservation(observation) {
		return dst, ErrControl
	}
	var stateStorage [replicatedstate.MaxStateEnvelopeBytes]byte
	state, err := replicatedstate.AppendState(stateStorage[:0], observation.State)
	if err != nil || len(state) == 0 || len(state) > replicatedstate.MaxStateEnvelopeBytes {
		return dst, errors.Join(ErrControl, err)
	}
	var snapshotBase []byte
	if observation.SnapshotBase != nil {
		snapshot, buildErr := replicatedstate.BuildSnapshotBase(
			observation.SnapshotBase.Manifest,
			observation.SnapshotBase.StaticBootstrap,
		)
		if buildErr != nil {
			return dst, errors.Join(ErrControl, buildErr)
		}
		snapshotBase, err = proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
		if err != nil || len(snapshotBase) == 0 || len(snapshotBase) > MaxSnapshotBaseEnvelopeBytes {
			return dst, errors.Join(ErrBound, err)
		}
	}
	total := responseHeaderBytes + len(state) + len(snapshotBase)
	if len(dst) > math.MaxInt-total {
		return dst, ErrBound
	}
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	b := dst[start:]
	copy(b[:8], responseMagic[:])
	b[8] = 1
	if observation.ProgressFound {
		b[9] = 1
	}
	copy(b[16:48], observation.Request.Operation[:])
	copy(b[48:80], observation.Request.Step[:])
	appendGroup(b[80:168], observation.Request.Group)
	binary.BigEndian.PutUint64(b[168:176], observation.Request.TargetMember)
	status := [...]uint64{observation.Status.MemberID, observation.Status.LeaderID,
		observation.Status.Term, observation.Status.Commit, observation.Status.Applied,
		observation.Status.CheckpointApplied, observation.Status.LeadTransferee}
	for index, value := range status {
		binary.BigEndian.PutUint64(b[176+index*8:184+index*8], value)
	}
	b[232] = byte(observation.Status.RaftState)
	if observation.Progress.Learner {
		b[233] = 1
	}
	if observation.Progress.RecentActive {
		b[234] = 1
	}
	if observation.Progress.FlowPaused {
		b[235] = 1
	}
	binary.BigEndian.PutUint64(b[240:248], observation.Progress.Match)
	binary.BigEndian.PutUint64(b[248:256], observation.Progress.Next)
	binary.BigEndian.PutUint64(b[256:264], observation.Progress.PendingSnapshot)
	binary.BigEndian.PutUint32(b[264:268], uint32(len(state)))
	binary.BigEndian.PutUint32(b[268:272], uint32(len(snapshotBase)))
	binary.BigEndian.PutUint32(b[272:276], uint32(total))
	copy(b[responseHeaderBytes:], state)
	copy(b[responseHeaderBytes+len(state):], snapshotBase)
	return dst, nil
}

func OpenResponse(raw []byte) (Observation, error) {
	if len(raw) < responseHeaderBytes || len(raw) > MaxResponseBytes ||
		!bytes.Equal(raw[:8], responseMagic[:]) || raw[8] != 1 || raw[9] > 1 ||
		!allZero(raw[10:16]) || !allZero(raw[152:168]) || !allZero(raw[236:240]) ||
		!allZero(raw[276:280]) {
		return Observation{}, ErrControl
	}
	stateBytes := int(binary.BigEndian.Uint32(raw[264:268]))
	snapshotBaseBytes := int(binary.BigEndian.Uint32(raw[268:272]))
	total := int(binary.BigEndian.Uint32(raw[272:276]))
	if stateBytes <= 0 || stateBytes > replicatedstate.MaxStateEnvelopeBytes ||
		snapshotBaseBytes < 0 || snapshotBaseBytes > MaxSnapshotBaseEnvelopeBytes ||
		total != len(raw) || total != responseHeaderBytes+stateBytes+snapshotBaseBytes {
		return Observation{}, ErrControl
	}
	var observation Observation
	copy(observation.Request.Operation[:], raw[16:48])
	copy(observation.Request.Step[:], raw[48:80])
	observation.Request.Group = openGroup(raw[80:168])
	observation.Request.TargetMember = binary.BigEndian.Uint64(raw[168:176])
	status := []*uint64{&observation.Status.MemberID, &observation.Status.LeaderID,
		&observation.Status.Term, &observation.Status.Commit, &observation.Status.Applied,
		&observation.Status.CheckpointApplied, &observation.Status.LeadTransferee}
	for index := range status {
		*status[index] = binary.BigEndian.Uint64(raw[176+index*8 : 184+index*8])
	}
	observation.Status.RaftState = raft.StateType(raw[232])
	observation.ProgressFound = raw[9] == 1
	observation.Progress.Learner = raw[233] == 1
	observation.Progress.RecentActive = raw[234] == 1
	observation.Progress.FlowPaused = raw[235] == 1
	observation.Progress.Match = binary.BigEndian.Uint64(raw[240:248])
	observation.Progress.Next = binary.BigEndian.Uint64(raw[248:256])
	observation.Progress.PendingSnapshot = binary.BigEndian.Uint64(raw[256:264])
	stateEnd := responseHeaderBytes + stateBytes
	state, err := replicatedstate.OpenState(raw[responseHeaderBytes:stateEnd])
	if err != nil {
		return Observation{}, errors.Join(ErrControl, err)
	}
	observation.State = state
	if snapshotBaseBytes != 0 {
		snapshot := new(pb.Snapshot)
		certificateRaw := raw[stateEnd:]
		if err = proto.Unmarshal(certificateRaw, snapshot); err != nil ||
			len(snapshot.ProtoReflect().GetUnknown()) != 0 {
			return Observation{}, errors.Join(ErrControl, err)
		}
		canonical, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
		certificate, openErr := replicatedstate.OpenSnapshotBase(snapshot)
		if marshalErr != nil || openErr != nil || !bytes.Equal(canonical, certificateRaw) {
			return Observation{}, errors.Join(ErrControl, marshalErr, openErr)
		}
		observation.SnapshotBase = &certificate
	}
	observation.Publication = raftmodel.Publication{Applied: state.Applied,
		DataChainDigest: state.DataChainDigest, ConfState: state.ConfState,
		ReplicaSetVersion: state.ReplicaSetVersion}
	observation.Request.ExpectedReplicaSetVersion = state.ReplicaSetVersion
	if !validObservation(observation) {
		return Observation{}, ErrControl
	}
	return observation, nil
}

func ReadResponse(reader io.Reader) (Observation, error) {
	var header [responseHeaderBytes]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return Observation{}, err
	}
	total := binary.BigEndian.Uint32(header[272:276])
	if total < responseHeaderBytes || total > MaxResponseBytes {
		return Observation{}, ErrBound
	}
	raw := make([]byte, int(total))
	copy(raw, header[:])
	if _, err := io.ReadFull(reader, raw[responseHeaderBytes:]); err != nil {
		return Observation{}, err
	}
	return OpenResponse(raw)
}

func WriteResponse(writer io.Writer, observation Observation) error {
	var raw [MaxResponseBytes]byte
	encoded, err := AppendResponse(raw[:0], observation)
	if err != nil {
		return err
	}
	return writeFull(writer, encoded)
}

func validRequest(request Request) bool {
	return request.Operation != ([32]byte{}) && request.Step != ([32]byte{}) &&
		validGroup(request.Group) && request.TargetMember != 0 &&
		(!request.HealthOnly || request.ExpectedReplicaSetVersion != 0)
}

func validObservation(observation Observation) bool {
	state := observation.State
	if observation.Request.HealthOnly || !validRequest(observation.Request) || observation.Request.ExpectedReplicaSetVersion == 0 ||
		state.ConfState == nil || state.Applied == 0 ||
		state.ReplicaSetVersion != observation.Request.ExpectedReplicaSetVersion ||
		observation.Publication.Applied != state.Applied ||
		observation.Publication.ReplicaSetVersion != state.ReplicaSetVersion ||
		observation.Publication.DataChainDigest != state.DataChainDigest ||
		!proto.Equal(observation.Publication.ConfState, state.ConfState) ||
		observation.Status.MemberID == 0 || observation.Status.Term == 0 ||
		observation.Status.Applied != state.Applied || !stateMatchesGroup(state, observation.Request.Group) ||
		observation.Status.RaftState > raft.StatePreCandidate {
		return false
	}
	if certificate := observation.SnapshotBase; certificate != nil {
		baseState := certificate.Manifest.State
		if certificate.Digest == ([32]byte{}) || certificate.Digest != state.SnapshotBaseDigest ||
			!stateMatchesGroup(baseState, observation.Request.Group) ||
			baseState.Applied > state.Applied ||
			baseState.ReplicaSetVersion > state.ReplicaSetVersion ||
			baseState.ConfState == nil ||
			!slices.Contains(baseState.ConfState.GetLearners(), observation.Request.TargetMember) ||
			slices.Contains(baseState.ConfState.GetVoters(), observation.Request.TargetMember) {
			return false
		}
		rebuilt, err := replicatedstate.BuildSnapshotBase(
			certificate.Manifest, certificate.StaticBootstrap,
		)
		if err != nil {
			return false
		}
		reopened, err := replicatedstate.OpenSnapshotBase(rebuilt)
		if err != nil || reopened.Digest != certificate.Digest {
			return false
		}
	}
	if observation.ProgressFound {
		return observation.Status.MemberID == observation.Status.LeaderID &&
			observation.Progress.Next != 0
	}
	return observation.Progress == (raftmodel.MemberProgress{})
}

func validGroup(group raftmember.GroupKey) bool {
	return group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{})
}

func stateMatchesGroup(state replicatedstate.State, group raftmember.GroupKey) bool {
	return [16]byte(state.Binding.ClusterID) == group.ClusterID &&
		[16]byte(state.Binding.ClusterIncarnation) == group.ClusterIncarnation &&
		state.Binding.TopologyRecoveryEpoch == group.TopologyRecoveryEpoch &&
		[16]byte(state.Binding.ShardIncarnation) == group.ShardIncarnation &&
		[16]byte(state.Binding.GroupID) == group.GroupID
}

func appendGroup(dst []byte, group raftmember.GroupKey) {
	copy(dst[0:16], group.ClusterID[:])
	copy(dst[16:32], group.ClusterIncarnation[:])
	binary.BigEndian.PutUint64(dst[32:40], group.TopologyRecoveryEpoch)
	copy(dst[40:56], group.ShardIncarnation[:])
	copy(dst[56:72], group.GroupID[:])
}

func openGroup(raw []byte) raftmember.GroupKey {
	var group raftmember.GroupKey
	copy(group.ClusterID[:], raw[0:16])
	copy(group.ClusterIncarnation[:], raw[16:32])
	group.TopologyRecoveryEpoch = binary.BigEndian.Uint64(raw[32:40])
	copy(group.ShardIncarnation[:], raw[40:56])
	copy(group.GroupID[:], raw[56:72])
	return group
}

func boundedDeadline(ctx context.Context, configured time.Time) time.Time {
	if configured.IsZero() {
		return time.Time{}
	}
	if deadline, found := ctx.Deadline(); found && deadline.Before(configured) {
		return deadline
	}
	return configured
}

func allZero(raw []byte) bool {
	for _, value := range raw {
		if value != 0 {
			return false
		}
	}
	return true
}

func writeFull(writer io.Writer, raw []byte) error {
	for len(raw) != 0 {
		written, err := writer.Write(raw)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(raw) {
			return io.ErrShortWrite
		}
		raw = raw[written:]
	}
	return nil
}
