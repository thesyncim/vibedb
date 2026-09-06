package main

// This file owns the process-level receiver directory used by an empty
// physical node.  Enrollment activates only an authenticated pre-Raft target
// reservation.  A snapshot-transfer service is registered separately, after
// the source has committed AddLearner and produced its certified descriptor.
// Consequently a control request can never turn a prepared SQL directory
// into a serving Raft member by itself.

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/nodecontrol"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/snapshottransfer"
)

type rf3DynamicBootstrapRegistry struct {
	mu           sync.RWMutex
	trustDomain  rafttransport.TrustDomain
	readDeadline rafttransport.DeadlineFunc
	slots        chan struct{}
	reservations map[raftmember.GroupKey]rf3BootstrapReservation
	services     map[raftmember.GroupKey]*snapshottransfer.BootstrapControlService
}

type rf3BootstrapReservation struct {
	intent gateway.GroupEnrollmentIntent
	proof  gateway.PreparedReplicaProof
}

func newRF3DynamicBootstrapRegistry(
	domain rafttransport.TrustDomain,
	readDeadline rafttransport.DeadlineFunc,
	maxConnections int,
) (*rf3DynamicBootstrapRegistry, error) {
	if domain == (rafttransport.TrustDomain{}) || readDeadline == nil ||
		maxConnections <= 0 || maxConnections > snapshottransfer.AbsoluteMaxBootstrapConcurrency {
		return nil, errors.Join(nodecontrol.ErrControl, snapshottransfer.ErrBootstrapControl)
	}
	return &rf3DynamicBootstrapRegistry{
		trustDomain: domain, readDeadline: readDeadline,
		slots:        make(chan struct{}, maxConnections),
		reservations: make(map[raftmember.GroupKey]rf3BootstrapReservation),
		services:     make(map[raftmember.GroupKey]*snapshottransfer.BootstrapControlService),
	}, nil
}

// Activate records the exact enrolled intent and reservation proof.  It is
// called by nodecontrol only after a committed EnrollmentEnrolled read.  An
// identical retry is harmless; a different intent for the same group is a
// conflict and cannot replace the target identity in place.
func (registry *rf3DynamicBootstrapRegistry) Activate(
	ctx context.Context, intent gateway.GroupEnrollmentIntent, proof gateway.PreparedReplicaProof,
) error {
	if registry == nil || ctx == nil || !rf3BootstrapIntentProofMatches(intent, proof) ||
		intent.State < gateway.EnrollmentEnrolled || intent.State == gateway.EnrollmentComplete ||
		intent.Group.ClusterID != registry.trustDomain.ClusterID ||
		intent.Group.ClusterIncarnation != registry.trustDomain.ClusterIncarnation {
		return nodecontrol.ErrInvalidProof
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	want := rf3BootstrapReservation{intent: intent, proof: proof}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if prior, found := registry.reservations[intent.Group]; found {
		if prior.intent.Digest() != intent.Digest() || prior.proof != proof {
			return nodecontrol.ErrConflict
		}
		return nil
	}
	registry.reservations[intent.Group] = want
	return nil
}

// Register publishes the actual BootstrapControlService only after a
// certified post-AddLearner descriptor is available.  It is deliberately
// separate from Activate so a reservation cannot receive snapshot bytes
// before the source-side membership fence exists.
func (registry *rf3DynamicBootstrapRegistry) Register(
	ctx context.Context, intent gateway.GroupEnrollmentIntent,
	proof gateway.PreparedReplicaProof, service *snapshottransfer.BootstrapControlService,
) error {
	if registry == nil || ctx == nil || service == nil || !rf3BootstrapIntentProofMatches(intent, proof) ||
		intent.State < gateway.EnrollmentEnrolled || intent.State == gateway.EnrollmentComplete {
		return nodecontrol.ErrControl
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	prior, found := registry.reservations[intent.Group]
	if !found || prior.intent.Digest() != intent.Digest() || prior.proof != proof {
		return nodecontrol.ErrNotPrepared
	}
	if current := registry.services[intent.Group]; current != nil && current != service {
		return nodecontrol.ErrConflict
	}
	registry.services[intent.Group] = service
	return nil
}

func (registry *rf3DynamicBootstrapRegistry) Remove(
	ctx context.Context, intent gateway.GroupEnrollmentIntent, proof gateway.PreparedReplicaProof,
) error {
	if registry == nil || ctx == nil || !intent.Valid() || !proof.Valid() {
		return nodecontrol.ErrControl
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	prior, found := registry.reservations[intent.Group]
	if !found || prior.intent.Digest() != intent.Digest() || prior.proof != proof {
		return nodecontrol.ErrConflict
	}
	delete(registry.services, intent.Group)
	delete(registry.reservations, intent.Group)
	return nil
}

// rf3BootstrapIntentProofMatches repeats the immutable equality at this
// process boundary. Intent.Valid checks the same relation when the proof is
// embedded in the directory record, but callers also pass a copied proof; a
// stale response must never activate a receiver for a different command,
// roster, schema, target endpoint, or directory revision.
func rf3BootstrapIntentProofMatches(
	intent gateway.GroupEnrollmentIntent, proof gateway.PreparedReplicaProof,
) bool {
	return intent.Valid() && proof.Valid() &&
		proof.IntentID == intent.IntentID && proof.Group == intent.Group &&
		proof.Distribution == intent.Distribution && proof.Shard == intent.Shard &&
		proof.ReplicaOrdinal == intent.ReplicaOrdinal &&
		proof.AllocationGeneration == intent.AllocationGeneration &&
		proof.CatalogGeneration == intent.CatalogGeneration &&
		proof.TargetMember == intent.Target.Member && proof.TargetNode == intent.Target.Node &&
		proof.TargetNodeIncarnation == intent.Target.NodeIncarnation && proof.TargetStoreID == intent.Target.StoreID &&
		proof.TargetEndpoint == intent.Target.Endpoint && proof.TargetNativeEndpoint == intent.Target.NativeEndpoint &&
		proof.TargetControlEndpoint == intent.Target.ControlEndpoint &&
		proof.ExpectedRosterDigest == intent.ExpectedRosterDigest &&
		proof.ExpectedDescriptorDigest == intent.ExpectedDescriptorDigest &&
		proof.ExpectedManifestDigest == intent.ExpectedManifestDigest &&
		proof.DescriptorDigest == intent.ExpectedDescriptorDigest &&
		proof.ManifestDigest == intent.ExpectedManifestDigest &&
		proof.RelationManifestDigest == intent.ExpectedCommand.RelationManifestDigest &&
		proof.Command == intent.ExpectedCommand &&
		proof.CertifiedDirectoryRevision == intent.TargetNodeRevision &&
		proof.EnrollmentDigest == proof.ComputedEnrollmentDigest()
}

func (registry *rf3DynamicBootstrapRegistry) Serve(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	if registry == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl ||
		connection.PeerIdentity().TrustDomain != registry.trustDomain {
		if connection != nil {
			_ = connection.Close()
		}
		return snapshottransfer.ErrBootstrapUnauthorized
	}
	defer connection.Close()
	select {
	case registry.slots <- struct{}{}:
		defer func() { <-registry.slots }()
	default:
		return snapshottransfer.ErrBound
	}
	if deadline := registry.readDeadline(); deadline.IsZero() {
		return snapshottransfer.ErrBootstrapControl
	} else if err := connection.SetReadDeadline(deadline); err != nil {
		return err
	}
	var raw [snapshottransfer.BootstrapRequestBytes]byte
	if _, err := io.ReadFull(connection, raw[:]); err != nil {
		return err
	}
	request, err := snapshottransfer.OpenBootstrapRequest(raw[:])
	if err != nil {
		return err
	}
	registry.mu.RLock()
	reservation, reserved := registry.reservations[request.Descriptor.Group]
	service := registry.services[request.Descriptor.Group]
	registry.mu.RUnlock()
	if !reserved || service == nil ||
		request.Descriptor.TargetMember != reservation.intent.Target.Member ||
		request.Descriptor.TargetStore != reservation.intent.Target.StoreID ||
		request.Descriptor.TargetIncarnation != reservation.intent.Target.NodeIncarnation {
		return snapshottransfer.ErrBootstrapUnauthorized
	}
	// The outer shard-control mux consumed the discriminator. Replaying the
	// complete fixed request lets BootstrapControlService perform its own
	// bounded deadline, request authentication, journal CAS, and response
	// handling without exposing an unexported service method here.
	replay := &rf3FixedReplayConnection{PeerConnection: connection, prefix: raw[:]}
	return service.Serve(ctx, replay)
}

// rf3FixedReplayConnection restores bytes consumed by a process-level router.
// It embeds the authenticated connection so the selected service observes the
// original TLS identity, traffic class, deadlines, and close semantics.
type rf3FixedReplayConnection struct {
	rafttransport.PeerConnection
	prefix []byte
	offset int
}

func (connection *rf3FixedReplayConnection) Read(dst []byte) (int, error) {
	if connection == nil || len(dst) == 0 {
		return 0, nil
	}
	if connection.offset < len(connection.prefix) {
		count := copy(dst, connection.prefix[connection.offset:])
		connection.offset += count
		return count, nil
	}
	return connection.PeerConnection.Read(dst)
}

var _ nodecontrol.IntentReader = (*nodecontrol.IntentReaderSlot)(nil)
