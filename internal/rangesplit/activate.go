package rangesplit

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
	pb "go.etcd.io/raft/v3/raftpb"
)

var childActivationEntryDomain = []byte("vibedb/range-split/child-activation-entry\x00")

// ChildActivationTarget is the prepared local identity and hidden state store
// for one independent child Raft group. User.Target.Collection must be the
// exact non-serving collection owned by ChildStage.
type ChildActivationTarget struct {
	Binding         replicatedstate.Binding
	StaticBootstrap *pb.Snapshot
	System          replicatedstate.CollectionTarget
	User            replicatedstate.UserCollection
	TxnLog          *durable.TxnLog
	MachineOptions  replicatedstate.Options
	ArtifactOptions replicatedstate.SnapshotArtifactOptions
}

// InitializeReplicatedChild binds an authenticated sealed child image to a
// standard replicated-state snapshot base without a second durable user-row
// copy. The returned base still must be installed into the child's Raft runtime;
// this method grants no serving authority.
func (s *ChildStage) InitializeReplicatedChild(
	certificate CutoverCertificate,
	target ChildActivationTarget,
) (
	machine *replicatedstate.Machine,
	base *pb.Snapshot,
	manifest replicatedstate.SnapshotArtifactManifest,
	err error,
) {
	prepared, err := s.PrepareReplicatedChild(certificate, target)
	if err != nil {
		return nil, nil, replicatedstate.SnapshotArtifactManifest{}, err
	}
	return prepared.Initialize()
}

// PrepareReplicatedChild authenticates the sealed child and binds its already
// audited stage commitment to the exact durable collection image in O(1). The
// returned preparation can first be bound to a seeded checkpoint-group
// certificate, then finished without a child-image or canonical-image scan.
func (s *ChildStage) PrepareReplicatedChild(
	certificate CutoverCertificate,
	target ChildActivationTarget,
) (*replicatedstate.StagedSnapshotPreparation, error) {
	if s == nil || target.User.Target.Collection == nil {
		return nil, ErrChildStage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.validActivationLocked(certificate, target.Binding) ||
		target.User.Name != s.partitioner.collection ||
		target.User.Target.Collection != s.collection {
		return nil, ErrChildStage
	}
	if s.sealedRoot == ([sha256.Size]byte{}) ||
		!s.collection.MatchesDurableImage(s.sealedIdentity) {
		return nil, ErrChildStage
	}
	entryDigest := childActivationEntryDigest(
		certificate, s.cursor.child, target.Binding,
	)
	return replicatedstate.PrepareStagedSnapshotFromCertifiedImage(
		target.Binding, target.StaticBootstrap, target.System, target.User,
		target.TxnLog, target.MachineOptions,
		replicatedstate.StagedSnapshotCut{
			Applied: certificate.cut.Applied, Term: certificate.cut.Term,
			EntryDigest: entryDigest,
		},
		target.ArtifactOptions, replicatedstate.CertifiedStagedImage{
			Rows: s.cursor.imageRows, ImageDigest: s.sealedRoot,
			Identity: s.sealedIdentity,
		},
	)
}

func (s *ChildStage) validActivationLocked(
	certificate CutoverCertificate,
	binding replicatedstate.Binding,
) bool {
	return s.validActivationCoordinatesLocked(certificate, binding) &&
		s.sealedVerified && s.collection.MatchesDurableImage(s.sealedIdentity)
}

// CheckActivationCoordinates validates the sealed cursor, cutover
// certificate, and destination binding without rescanning the child image.
// PrepareReplicatedChild performs the full replicated-state image audit before
// mutation; the stage-layer sealed-image proof is reused from seal/reopen.
func (s *ChildStage) CheckActivationCoordinates(
	certificate CutoverCertificate,
	binding replicatedstate.Binding,
) error {
	if s == nil {
		return ErrChildStage
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.sealedVerified || !s.collection.MatchesDurableImage(s.sealedIdentity) ||
		!s.validActivationCoordinatesLocked(certificate, binding) {
		return ErrChildStage
	}
	return nil
}

func (s *ChildStage) validActivationCoordinatesLocked(
	certificate CutoverCertificate,
	binding replicatedstate.Binding,
) bool {
	return s.cursor != nil && s.cursor.phase == ChildStageSealed &&
		s.partitioner.VerifyCutoverCertificateWithWorkspace(
			certificate, &s.activation,
		) == nil &&
		s.activationBindingMatches(binding, certificate) &&
		s.cursor.SourceCut() == certificate.SourceCut() &&
		s.cursor.artifactDigest == certificate.childBases[s.cursor.child] &&
		s.cursor.lastBatchDigest == certificate.sealBatches[s.cursor.child] &&
		s.cursor.imageDigest == certificate.childImages[s.cursor.child]
}

func (s *ChildStage) activationBindingMatches(
	binding replicatedstate.Binding,
	certificate CutoverCertificate,
) bool {
	child := s.partitioner.children[s.cursor.child]
	return binding.Distribution == string(s.partitioner.source.Distribution) &&
		binding.Shard == string(child.Shard) &&
		binding.AllocationGeneration == uint64(child.AllocationGeneration) &&
		binding.OwnershipEpoch == uint64(child.OwnershipEpoch) &&
		binding.RoutingVersion == uint64(s.partitioner.target) &&
		binding.RouteGeneration == certificate.coordinates.RouteGeneration &&
		binding.OwnedRange == child.Range
}

func childActivationEntryDigest(
	certificate CutoverCertificate,
	child uint8,
	binding replicatedstate.Binding,
) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(childActivationEntryDomain)
	_, _ = h.Write(certificate.digest[:])
	_, _ = h.Write(certificate.childBases[child][:])
	_, _ = h.Write(certificate.sealBatches[child][:])
	_, _ = h.Write(certificate.childImages[child][:])
	writeActivationBinding(h, binding, child)
	var digest [sha256.Size]byte
	_ = h.Sum(digest[:0])
	return digest
}

func writeActivationBinding(h hash.Hash, binding replicatedstate.Binding, child uint8) {
	_, _ = h.Write(binding.ClusterID[:])
	_, _ = h.Write(binding.ClusterIncarnation[:])
	_, _ = h.Write(binding.ShardIncarnation[:])
	_, _ = h.Write(binding.GroupID[:])
	writeBytes(h, []byte(binding.Distribution))
	writeBytes(h, []byte(binding.Shard))
	var fixed [82]byte
	fixed[0] = child
	values := [...]uint64{
		binding.TopologyRecoveryEpoch, binding.AllocationGeneration,
		binding.ActivePolicyGeneration, binding.ProtectionEpoch,
		binding.OwnershipEpoch, binding.SchemaGeneration,
		binding.RoutingVersion, binding.RouteGeneration,
	}
	for index, value := range values {
		binary.LittleEndian.PutUint64(fixed[1+index*8:9+index*8], value)
	}
	copy(fixed[65:73], binding.OwnedRange.Start[:])
	copy(fixed[73:81], binding.OwnedRange.End.Point[:])
	if binding.OwnedRange.End.Max {
		fixed[81] = 1
	}
	_, _ = h.Write(fixed[:])
}
