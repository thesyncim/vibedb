package nodecontrol

import (
	"bytes"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibejson"
)

// PreparationSpecKind identifies the public, path-free preparation grammar.
// A controller can construct this document from replicated catalog data; the
// receiving node supplies every filesystem, TLS, WAL-key, and listener value
// from its own prepared physical-node manifest.
const PreparationSpecKind = "vibedb/replica-preparation/v1"

// MaxSourceBootstrapBytes bounds the small immutable Raft bootstrap envelope
// carried with a preparation claim. Bulk user/schema state is transferred by
// snapshottransfer; this field is only the source-certified index-one
// bootstrap needed to verify that later artifact manifests belong to the same
// lineage.
const MaxSourceBootstrapBytes = 1 << 20

// PreparationMember is a stable voter descriptor. It carries no credentials
// or filesystem paths. The target is separate from InitialVoters so preparing
// a learner can never be mistaken for creating a new genesis voter set.
type PreparationMember struct {
	PeerEndpoint    distribution.EndpointID `json:"peer_endpoint"`
	NativeEndpoint  distribution.EndpointID `json:"native_endpoint,omitempty"`
	ControlEndpoint distribution.EndpointID `json:"control_endpoint,omitempty"`
	MemberID        uint64                  `json:"member_id"`
	Node            rafttransport.NodeID    `json:"node"`
	PeerAddress     string                  `json:"peer_address"`
	NativeAddress   string                  `json:"native_address,omitempty"`
	ControlAddress  string                  `json:"control_address,omitempty"`
	SnapshotAddress string                  `json:"snapshot_address,omitempty"`
}

// PreparationApplyProfile contains only portable schema/apply limits. The
// node adapter maps it to the local SQL driver profile after validating the
// complete enrollment intent.
type PreparationApplyProfile struct {
	MaxSessions                      uint64   `json:"max_sessions"`
	RetryWindow                      uint16   `json:"retry_window"`
	MaxCollections                   int      `json:"max_collections"`
	MaxDocuments                     int      `json:"max_documents"`
	MaxBytes                         int64    `json:"max_bytes"`
	ShardKey                         string   `json:"shard_key"`
	RequestLedgerCapacityBytes       uint64   `json:"request_ledger_capacity_bytes"`
	RequestLedgerCleanupReserveBytes uint64   `json:"request_ledger_cleanup_reserve_bytes"`
	RequestLedgerRangeStart          [32]byte `json:"request_ledger_range_start"`
	RequestLedgerRangeEnd            [32]byte `json:"request_ledger_range_end"`
	RequestLedgerRangeIdentity       [32]byte `json:"request_ledger_range_identity"`
}

// PreparationLogProfile is the portable geometry of the group WAL. Paths and
// key material stay node-owned; these bounds are part of the source-certified
// schema contract and therefore cannot be inferred from target capacity.
type PreparationLogProfile struct {
	MaxFileBytes   int64  `json:"max_file_bytes"`
	MaxRecordBytes int    `json:"max_record_bytes"`
	MaxRecords     uint64 `json:"max_records"`
	MaxEntries     uint64 `json:"max_entries"`
	MaxLiveBytes   int64  `json:"max_live_bytes"`
}

// PreparationGlobalIndex is the portable identity of one maintained global
// index relation.  The target maps this value onto its local storage names;
// no path or credential is carried over the control boundary.
type PreparationGlobalIndex struct {
	Relation      uint16 `json:"relation"`
	Table         string `json:"table"`
	IndexID       uint64 `json:"index_id"`
	Incarnation   uint64 `json:"incarnation"`
	LocatorCount  uint8  `json:"locator_count"`
	Unique        bool   `json:"unique"`
	KeyEncoding   uint8  `json:"key_encoding"`
	KeyArity      uint8  `json:"key_arity"`
	TupleVersion  uint32 `json:"tuple_version"`
	MapperVersion uint32 `json:"mapper_version"`
	BucketBits    uint8  `json:"bucket_bits"`
}

// PreparationSpec is the complete logical input for a cold replica
// reservation. It intentionally excludes every node-owned path and secret.
// ExpectedManifestDigest in GroupEnrollmentIntent is the SHA-256 of this
// canonical document, so a node never accepts a controller-reconstructed
// variant under the same intent.
type PreparationSpec struct {
	Kind                  string                                 `json:"kind"`
	Group                 raftmember.GroupKey                    `json:"group"`
	Distribution          distribution.DistributionName          `json:"distribution"`
	Shard                 distribution.ShardID                   `json:"shard"`
	AllocationGeneration  distribution.ShardAllocationGeneration `json:"allocation_generation"`
	ReplicaOrdinal        uint8                                  `json:"replica_ordinal"`
	SourceCommand         raftservice.CommandFence               `json:"source_command"`
	LogicalSchemaDigest   replication.Digest                     `json:"logical_schema_digest"`
	InitialVoters         [3]PreparationMember                   `json:"initial_voters"`
	Target                PreparationMember                      `json:"target"`
	TargetNodeIncarnation uint64                                 `json:"target_node_incarnation"`
	TargetStoreID         [16]byte                               `json:"target_store_id"`
	Log                   PreparationLogProfile                  `json:"log"`
	Table                 string                                 `json:"table"`
	CreateTable           string                                 `json:"create_table"`
	SchemaStatements      []string                               `json:"schema_statements,omitempty"`
	GlobalIndexes         []PreparationGlobalIndex               `json:"global_indexes,omitempty"`
	Apply                 PreparationApplyProfile                `json:"apply"`
	// SourceBootstrap is a bounded, canonical protobuf envelope. It is not a
	// data snapshot; the receiver compares its digest to the streamed artifact
	// certificate before activation.
	SourceBootstrap       []byte             `json:"source_bootstrap,omitempty"`
	SourceBootstrapDigest replication.Digest `json:"source_bootstrap_digest,omitempty"`
}

// ValidateAgainst checks all immutable logical identities. It does not trust
// target endpoints, roots, or local credentials supplied by a caller.
func (spec PreparationSpec) ValidateAgainst(intent gateway.GroupEnrollmentIntent) error {
	if spec.Kind != PreparationSpecKind || !intent.Valid() ||
		spec.Group != intent.Group || spec.Distribution != intent.Distribution || spec.Shard != intent.Shard ||
		spec.AllocationGeneration != intent.AllocationGeneration || spec.ReplicaOrdinal != intent.ReplicaOrdinal ||
		spec.SourceCommand != intent.ExpectedCommand || spec.Target.MemberID != intent.Target.Member ||
		spec.Target.Node != intent.Target.Node || spec.TargetNodeIncarnation != intent.Target.NodeIncarnation ||
		spec.TargetStoreID != intent.Target.StoreID || spec.LogicalSchemaDigest != intent.ExpectedCommand.RelationManifestDigest ||
		spec.LogicalSchemaDigest == (replication.Digest{}) || spec.Target.PeerEndpoint != intent.Target.Endpoint ||
		spec.Target.NativeEndpoint != intent.Target.NativeEndpoint ||
		spec.Target.ControlEndpoint != intent.Target.ControlEndpoint ||
		spec.Table == "" || spec.CreateTable == "" || spec.Apply.ShardKey == "" {
		return ErrStale
	}
	if len(spec.SourceBootstrap) > MaxSourceBootstrapBytes ||
		(len(spec.SourceBootstrap) == 0) != (spec.SourceBootstrapDigest == (replication.Digest{})) ||
		len(spec.SourceBootstrap) != 0 && sha256.Sum256(spec.SourceBootstrap) != spec.SourceBootstrapDigest {
		return ErrControl
	}
	seenIDs := [4]uint64{}
	seenNodes := [4]rafttransport.NodeID{}
	for index, member := range append(append([]PreparationMember(nil), spec.InitialVoters[:]...), spec.Target) {
		if member.MemberID == 0 || member.Node == (rafttransport.NodeID{}) || member.PeerAddress == "" || member.PeerEndpoint == "" {
			return ErrControl
		}
		for prior := 0; prior < index; prior++ {
			if member.MemberID == seenIDs[prior] || member.Node == seenNodes[prior] {
				return ErrConflict
			}
		}
		seenIDs[index], seenNodes[index] = member.MemberID, member.Node
	}
	for index := 1; index < len(spec.InitialVoters); index++ {
		if spec.InitialVoters[index-1].MemberID >= spec.InitialVoters[index].MemberID {
			return ErrControl
		}
	}
	sourceFound := false
	for _, member := range spec.InitialVoters {
		if member.MemberID == intent.Source.Member {
			if member.Node != intent.Source.Node || member.PeerEndpoint != intent.Source.Endpoint {
				return ErrStale
			}
			sourceFound = true
		}
	}
	if !sourceFound || spec.Target.MemberID == intent.Source.Member || spec.Target.Node == intent.Source.Node {
		return ErrStale
	}
	if spec.Target.MemberID == spec.InitialVoters[0].MemberID ||
		spec.Target.MemberID == spec.InitialVoters[1].MemberID ||
		spec.Target.MemberID == spec.InitialVoters[2].MemberID {
		return ErrConflict
	}
	if spec.Apply.MaxSessions == 0 || spec.Apply.RetryWindow == 0 || spec.Apply.MaxCollections <= 0 ||
		spec.Apply.MaxDocuments <= 0 || spec.Apply.MaxBytes <= 0 || len(spec.SchemaStatements) > 256 ||
		len(spec.GlobalIndexes) > 255 || spec.Log.MaxFileBytes <= 0 || spec.Log.MaxRecordBytes <= 0 ||
		spec.Log.MaxRecords == 0 || spec.Log.MaxEntries == 0 || spec.Log.MaxLiveBytes <= 0 {
		return ErrControl
	}
	return nil
}

// AppendPreparationSpec returns the canonical wire bytes whose digest is
// committed in the enrollment intent.
func AppendPreparationSpec(dst []byte, spec PreparationSpec) ([]byte, error) {
	if err := spec.ValidateShape(); err != nil {
		return dst, err
	}
	raw, err := vibejson.Marshal(&spec)
	if err != nil {
		return dst, errors.Join(ErrControl, err)
	}
	return vibejson.AppendCanonicalize(dst, raw)
}

// OpenPreparationSpec accepts only one canonical bounded document.
func OpenPreparationSpec(raw []byte) (PreparationSpec, error) {
	if len(raw) == 0 || len(raw) > MaxPayloadBytes {
		return PreparationSpec{}, ErrBound
	}
	var spec PreparationSpec
	if err := vibejson.Unmarshal(raw, &spec); err != nil {
		return PreparationSpec{}, errors.Join(ErrControl, err)
	}
	canonical, err := AppendPreparationSpec(nil, spec)
	if err != nil || !bytes.Equal(raw, canonical) {
		return PreparationSpec{}, errors.Join(ErrControl, err)
	}
	return spec, nil
}

// Digest is the immutable payload digest used by ExpectedManifestDigest.
func (spec PreparationSpec) Digest() replication.Digest {
	raw, err := AppendPreparationSpec(nil, spec)
	if err != nil {
		return replication.Digest{}
	}
	return sha256.Sum256(raw)
}

// ValidateShape performs path-free structural bounds before intent binding.
func (spec PreparationSpec) ValidateShape() error {
	if spec.Kind != PreparationSpecKind || spec.Group == (raftmember.GroupKey{}) ||
		spec.Distribution == "" || spec.Shard == "" || spec.AllocationGeneration == 0 ||
		spec.ReplicaOrdinal >= gateway.ServingReplicaCount || !spec.SourceCommand.Valid() ||
		spec.LogicalSchemaDigest == (replication.Digest{}) || spec.TargetStoreID == ([16]byte{}) ||
		spec.TargetNodeIncarnation == 0 || spec.Table == "" || spec.CreateTable == "" ||
		spec.Apply.MaxSessions == 0 || spec.Apply.RetryWindow == 0 || spec.Apply.MaxCollections <= 0 ||
		spec.Apply.MaxDocuments <= 0 || spec.Apply.MaxBytes <= 0 || spec.Apply.ShardKey == "" ||
		len(spec.SchemaStatements) > 256 || len(spec.GlobalIndexes) > 255 || spec.Log.MaxFileBytes <= 0 ||
		spec.Log.MaxRecordBytes <= 0 || spec.Log.MaxRecords == 0 || spec.Log.MaxEntries == 0 || spec.Log.MaxLiveBytes <= 0 {
		return ErrControl
	}
	if spec.Target.MemberID == 0 || spec.Target.Node == (rafttransport.NodeID{}) || spec.Target.PeerAddress == "" ||
		spec.Target.NativeAddress == "" || spec.Target.ControlAddress == "" || spec.Target.PeerEndpoint == "" || spec.Target.NativeEndpoint == "" || spec.Target.ControlEndpoint == "" {
		return ErrControl
	}
	if len(spec.SourceBootstrap) > MaxSourceBootstrapBytes ||
		(len(spec.SourceBootstrap) == 0) != (spec.SourceBootstrapDigest == (replication.Digest{})) ||
		len(spec.SourceBootstrap) != 0 && sha256.Sum256(spec.SourceBootstrap) != spec.SourceBootstrapDigest {
		return ErrControl
	}
	for index, member := range spec.InitialVoters {
		if member.MemberID == 0 || member.Node == (rafttransport.NodeID{}) || member.PeerAddress == "" || member.PeerEndpoint == "" ||
			index > 0 && member.MemberID <= spec.InitialVoters[index-1].MemberID {
			return ErrControl
		}
	}
	return nil
}
