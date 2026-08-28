package driver

import (
	"bytes"
	"errors"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// ErrSchemaSourceNotCommitted identifies an authenticated prepared source cut,
// not a corrupt, mismatched, or outcome-unknown recovery attempt.
var ErrSchemaSourceNotCommitted = replicatedstate.ErrSchemaSourceNotCommitted

// OpenReplicatedShardStoreWithSchemaSourceTransition opens the original source
// membership without selecting or publishing its prepared replacement. The
// subsequent OpenReplicatedApply authenticates the exact committed N+1 record
// and returns a permanently fenced claim, usable only to observe and publish
// that transition. An exact prepared-but-uncommitted source returns
// ErrSchemaSourceNotCommitted from OpenReplicatedApply.
func OpenReplicatedShardStoreWithSchemaSourceTransition(
	path string,
	expected ReplicatedShardStoreIdentity,
	expectedApply ReplicatedApplyIdentity,
	command []byte,
	opening ...ReplicatedOpenOptions,
) (*Database, error) {
	openOptions, err := replicatedOpeningOptions(opening)
	if err != nil {
		return nil, err
	}
	if err := validateReplicatedShardStoreIdentity(expected); err != nil {
		return nil, err
	}
	if err := validateReplicatedApplyIdentity(expectedApply, expected); err != nil {
		return nil, err
	}
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return nil, err
	}
	transition, found, err := ObservePersistedReplicatedSchemaTransition(absolute)
	if err != nil || !found || !bytes.Equal(command, transition.Bytes()) {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	raw, found, err := readCatalogFile(absolute)
	if err != nil || !found {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	image, err := ValidateReplicatedSchemaCatalogImage(raw)
	if err != nil || image.SchemaGeneration != transition.From.SchemaGeneration ||
		image.RelationManifestDigest != transition.FromManifest {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	selected, _, err := openReplicatedSchemaCatalogImage(raw)
	if err != nil || selected.ReplicatedShardStore == nil || selected.ReplicatedApply == nil ||
		!selected.ReplicatedShardStore.Equal(expected) || selected.ReplicatedApply.identity() != expectedApply {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	marker, found, err := readReplicatedSchemaStageMarker(absolute + ".tables")
	if err != nil || !found || marker.sourceApplied == ^uint64(0) ||
		replicatedSchemaCatalogCASDigest(image.Digest, marker.catalogDigest,
			transition.RequestDigest, transition.AuthorizationDigest) != transition.CatalogCASDigest {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	proof := &replicatedstate.SchemaSourceRecoveryProof{
		Command: bytes.Clone(command), Membership: marker.membership,
		AuthorizationDigest: transition.AuthorizationDigest,
		CatalogCASDigest:    transition.CatalogCASDigest, SourceApplied: marker.sourceApplied,
	}
	core, err := openDatabaseWithShardStorePolicy(path, nil, shardStoreOpenPolicy{
		mode:                    shardStoreOpenReplicatedApplyExisting,
		openOptions:             openOptions,
		expectedReplicated:      ownedReplicatedShardStoreIdentity(expected),
		expectedReplicatedApply: expectedApply,
		schemaSourceRecovery:    proof,
	})
	if err != nil {
		return nil, err
	}
	return &Database{connector: &dbConnector{db: core}}, nil
}
