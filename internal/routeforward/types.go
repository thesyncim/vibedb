// Package routeforward implements the bounded replicated catalog authority
// that forwards one exact old command after a topology transition. The old
// command is authenticated and wrapped, never rewritten.
package routeforward

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
)

const (
	// MaxSnapshotBytes is the hard canonical image bound for one catalog RF3
	// forwarding authority.
	MaxSnapshotBytes = uint64(64 << 20)
	// MaxCatalogGenerationTTL bounds a forwarding validity interval using the
	// replicated catalog clock. Wall time never participates in replay.
	MaxCatalogGenerationTTL = uint64(1 << 20)
)

var (
	ErrCorrupt = errors.New("routeforward: corrupt canonical record")
	ErrBound   = errors.New("routeforward: retained authority bound exceeded")
)

// Digest is one fixed byte-native identity or certificate.
type Digest [32]byte

// TopologyKind identifies why the exact old route becomes stale.
type TopologyKind uint8

const (
	TopologyInvalid TopologyKind = iota
	TopologySplit
	TopologyMove
	TopologyReplicaReplacement
)

// RouteAuthority is the exact old or target RF3 allocation and command fence.
// It contains no endpoint spelling or process-local address.
type RouteAuthority struct {
	Group                raftmember.GroupKey
	AllocationGeneration uint64
	Command              raftservice.CommandFence
}

// TargetRoute is the fixed control-plane route certificate. RouteSetDigest
// binds the authenticated member/endpoint directory without pinning one
// physical replica, so normal leader failover remains available.
type TargetRoute struct {
	Authority      RouteAuthority
	RouteSetDigest Digest
}

// Validity is the replicated, generation-based lifetime of an entry.
// TargetAppliedFloor prevents forwarding until the new group has caught up.
// GateEpoch is the source route-gate epoch whose pins must clear before prune.
type Validity struct {
	SourceAppliedFloor   uint64
	TargetAppliedFloor   uint64
	ValidFromCatalog     uint64
	RetainThroughCatalog uint64
	ExpiresAfterCatalog  uint64
	GateEpoch            uint64
}

// Entry binds one exact old command to one immutable target. PlanDigest binds
// the split/move/replacement proof retained by the catalog controller.
type Entry struct {
	Kind               TopologyKind
	Old                RouteAuthority
	CommandFingerprint Digest
	CommandDigest      Digest
	PlanDigest         Digest
	Target             TargetRoute
	Validity           Validity
}

// EntryState is the replicated publication state. Only active entries resolve.
type EntryState uint8

const (
	EntryInvalid EntryState = iota
	EntryPrepared
	EntryActive
)

// Operation is the closed catalog-Raft transition set.
type Operation uint8

const (
	OperationInvalid Operation = iota
	OperationPublish
	OperationActivate
	OperationPrune
	OperationCompactRetired
)

// Clearance is the replicated authority cut required to prune. Admission
// verifies the authenticated gate and retry producers before proposing it.
// The catalog TTL, source gate, and retry low-water mark must all have passed.
type Clearance struct {
	Key                Digest
	CatalogGeneration  uint64
	RouteGateEpoch     uint64
	RouteGateRevision  uint64
	ActivePins         uint64
	OldestRetryApplied uint64
	AuthorityRevision  uint64
	// GateCertificate is the digest of the exact route-gate settlement that
	// reports ActivePins. RetryCertificate similarly commits to the replicated
	// retry low-water witness. Their authenticated producers are verified by
	// control-plane admission before this command enters catalog Raft.
	GateCertificate  Digest
	RetryCertificate Digest
}

// RetryCut is the authenticated request-ledger low-water attestation consumed
// by BuildClearance. Certificate is opaque here and is verified by admission.
type RetryCut struct {
	OldestApplied uint64
	Certificate   Digest
}

// Command is one canonical replicated transition. Authority is the digest of
// the mTLS-authenticated controller principal/configuration bound at bootstrap.
type Command struct {
	Operation          Operation
	Authority          Digest
	AuthorityEpoch     uint64
	NextAuthorityEpoch uint64
	ExpectedRevision   uint64
	Key                Digest
	Entry              Entry
	Clearance          Clearance
}

// Reason is a deterministic replicated transition or lookup result.
type Reason uint8

const (
	ReasonInvalid Reason = iota
	ReasonPublished
	ReasonActivated
	ReasonPruned
	ReasonIdempotent
	ReasonUnauthorized
	ReasonStaleAuthority
	ReasonStaleRevision
	ReasonConflict
	ReasonNotFound
	ReasonRetired
	ReasonCapacity
	ReasonNotActive
	ReasonTooEarly
	ReasonExpired
	ReasonStaleRead
	ReasonTargetBehind
	ReasonPinsActive
	ReasonRetryWindow
	ReasonCompacted
)

// Outcome is the fixed transition settlement. Certificate commits to the
// exact resulting entry state (or prune tombstone) at Revision; authenticity
// comes from the catalog-Raft settlement channel, not from this unkeyed digest.
type Outcome struct {
	Reason      Reason
	Mutated     bool
	State       EntryState
	Revision    uint64
	Live        uint64
	Tombstones  uint64
	Key         Digest
	Certificate Digest
}

// ReadCut must be obtained from a linearizable catalog-RF3 ReadIndex path.
// ReadIndex==0 is never authority and lets stale former leaders fail closed.
type ReadCut struct {
	Authority         Digest
	AuthorityEpoch    uint64
	AppliedRevision   uint64
	ReadIndex         uint64
	CatalogGeneration uint64
	TargetApplied     uint64
}

// Decision borrows OriginalCommand unchanged and exposes the fixed target plus
// active-entry certificate for an authenticated forwarding wrapper.
type Decision struct {
	Target          TargetRoute
	MinimumApplied  uint64
	Certificate     Digest
	OriginalCommand []byte
}

func validGroup(group raftmember.GroupKey) bool {
	return group.ClusterID != ([16]byte{}) && group.ClusterIncarnation != ([16]byte{}) &&
		group.TopologyRecoveryEpoch != 0 && group.ShardIncarnation != ([16]byte{}) &&
		group.GroupID != ([16]byte{})
}

func validAuthority(authority RouteAuthority) bool {
	return validGroup(authority.Group) && authority.AllocationGeneration != 0 &&
		authority.Command.Valid()
}

func validTarget(target TargetRoute) bool {
	return validAuthority(target.Authority) && target.RouteSetDigest != (Digest{})
}

func validValidity(validity Validity) bool {
	return validity.SourceAppliedFloor != 0 && validity.TargetAppliedFloor != 0 &&
		validity.ValidFromCatalog != 0 &&
		validity.RetainThroughCatalog >= validity.ValidFromCatalog &&
		validity.ExpiresAfterCatalog > validity.RetainThroughCatalog &&
		validity.ExpiresAfterCatalog-validity.ValidFromCatalog <= MaxCatalogGenerationTTL &&
		validity.GateEpoch != 0
}

func validEntry(entry Entry) bool {
	return entry.Kind >= TopologySplit && entry.Kind <= TopologyReplicaReplacement &&
		validAuthority(entry.Old) && validTarget(entry.Target) &&
		entry.CommandFingerprint != (Digest{}) && entry.CommandDigest != (Digest{}) &&
		entry.PlanDigest != (Digest{}) && validValidity(entry.Validity) &&
		entry.Target.Authority != entry.Old
}
