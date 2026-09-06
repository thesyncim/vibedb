package nodecontrol

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/replication"
)

// PreparationExportInput is the source-certified, path-free input to one
// preparation document.  The source adapter owns the values in this struct;
// callers must not derive schema or log geometry from target capacity.  The
// intent may be a draft while an enrollment is being built, so its manifest
// digest is intentionally allowed to be zero here.
type PreparationExportInput struct {
	Intent           gateway.GroupEnrollmentIntent
	InitialVoters    [3]PreparationMember
	Target           PreparationMember
	Log              PreparationLogProfile
	Table            string
	CreateTable      string
	SchemaStatements []string
	GlobalIndexes    []PreparationGlobalIndex
	Apply            PreparationApplyProfile
	SourceBootstrap  []byte
}

// PreparationPayload is the immutable canonical byte image and its digest.
// Bytes is always a private copy, so a caller cannot mutate the value after it
// has been used to derive GroupEnrollmentIntent.ExpectedManifestDigest.
type PreparationPayload struct {
	Spec   PreparationSpec
	Bytes  []byte
	Digest replication.Digest
}

// ExportPreparationSpec constructs one canonical public preparation document.
// Every identity that is also present in the enrollment intent is copied from
// that intent; a source adapter cannot accidentally emit a payload for a
// different target by changing a parallel argument.
func ExportPreparationSpec(input PreparationExportInput) (PreparationPayload, error) {
	intent := input.Intent
	if intent.Group == (raftmember.GroupKey{}) ||
		intent.Distribution == "" || intent.Shard == "" ||
		intent.AllocationGeneration == 0 || intent.ReplicaOrdinal >= gateway.ServingReplicaCount ||
		!intent.ExpectedCommand.Valid() || intent.Target.Member == 0 ||
		!intent.Target.Valid() {
		return PreparationPayload{}, ErrControl
	}
	// A draft can still carry a zero ExpectedManifestDigest, but all other
	// immutable command and identity fields must already be present.
	spec := PreparationSpec{
		Kind:                 PreparationSpecKind,
		Group:                intent.Group,
		Distribution:         intent.Distribution,
		Shard:                intent.Shard,
		AllocationGeneration: intent.AllocationGeneration,
		ReplicaOrdinal:       intent.ReplicaOrdinal,
		SourceCommand:        intent.ExpectedCommand,
		// RelationManifestDigest is the exact machine validation digest used by
		// the Raft command. It is deliberately copied from the certified fence;
		// a portable logical schema digest is already retained in the catalog
		// route and must not be invented in this document.
		LogicalSchemaDigest:   intent.ExpectedCommand.RelationManifestDigest,
		InitialVoters:         input.InitialVoters,
		Target:                input.Target,
		TargetNodeIncarnation: intent.Target.NodeIncarnation,
		TargetStoreID:         intent.Target.StoreID,
		Log:                   input.Log,
		Table:                 input.Table,
		CreateTable:           input.CreateTable,
		SchemaStatements:      append([]string(nil), input.SchemaStatements...),
		GlobalIndexes:         append([]PreparationGlobalIndex(nil), input.GlobalIndexes...),
		Apply:                 input.Apply,
		SourceBootstrap:       append([]byte(nil), input.SourceBootstrap...),
	}
	if len(spec.SourceBootstrap) != 0 {
		spec.SourceBootstrapDigest = sha256.Sum256(spec.SourceBootstrap)
	}
	// The intent is the authority for target identity. Keep the explicit input
	// useful for its endpoint addresses, but reject any attempted identity
	// substitution before bytes are committed.
	if spec.Target.MemberID != intent.Target.Member || spec.Target.Node != intent.Target.Node ||
		spec.Target.PeerEndpoint != intent.Target.Endpoint ||
		spec.Target.NativeEndpoint != intent.Target.NativeEndpoint || spec.Target.ControlEndpoint != intent.Target.ControlEndpoint ||
		spec.TargetNodeIncarnation != intent.Target.NodeIncarnation ||
		spec.TargetStoreID != intent.Target.StoreID {
		return PreparationPayload{}, ErrConflict
	}
	if err := spec.ValidateShape(); err != nil {
		return PreparationPayload{}, err
	}
	raw, err := AppendPreparationSpec(nil, spec)
	if err != nil {
		return PreparationPayload{}, err
	}
	digest := replication.Digest(sha256.Sum256(raw))
	return PreparationPayload{Spec: spec, Bytes: bytes.Clone(raw), Digest: digest}, nil
}

// VerifiedPayloadProvider wraps a source reader used by both Prepare and
// Adopt. The source is called on every attempt, then the canonical document is
// checked against the durable intent digest. This makes a restart refetch the
// exact source image and turns source catalog drift into a hard stale result.
func VerifiedPayloadProvider(source PayloadProvider) PayloadProvider {
	if source == nil {
		return nil
	}
	return func(ctx context.Context, intent gateway.GroupEnrollmentIntent) ([]byte, error) {
		if ctx == nil || !intent.Valid() {
			return nil, ErrControl
		}
		payload, err := source(ctx, intent)
		if err != nil {
			return nil, err
		}
		if len(payload) == 0 || len(payload) > MaxPayloadBytes {
			return nil, ErrBound
		}
		spec, err := OpenPreparationSpec(payload)
		if err != nil {
			return nil, err
		}
		if err = spec.ValidateAgainst(intent); err != nil {
			return nil, err
		}
		canonical, err := AppendPreparationSpec(nil, spec)
		if err != nil || !bytes.Equal(canonical, payload) {
			return nil, errors.Join(ErrControl, err)
		}
		if replication.Digest(sha256.Sum256(canonical)) != intent.ExpectedManifestDigest {
			return nil, ErrStale
		}
		return bytes.Clone(canonical), nil
	}
}
