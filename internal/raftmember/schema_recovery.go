package raftmember

import (
	"errors"

	"github.com/thesyncim/vibedb/internal/raftstore"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

// OpenBoundSQLWithApplyForSchemaSourceTransition opens only an authenticated
// retiring source for local catalog settlement before host registration. It
// grants no serving authority and preserves the ordinary WAL health, exact
// local binding and immutable bootstrap checks.
func OpenBoundSQLWithApplyForSchemaSourceTransition(
	path string, wal *raftstore.Store,
	authority sqldriver.ReplicatedAuthorityProfile,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	expectedApply sqldriver.ReplicatedApplyIdentity,
	command []byte,
	opening ...sqldriver.ReplicatedOpenOptions,
) (*sqldriver.Database, *sqldriver.ReplicatedApply, error) {
	if wal == nil {
		return nil, nil, ErrWALUnavailable
	}
	if _, err := wal.PendingGenerationActivation(); !errors.Is(err, raftstore.ErrGenerationActivationPending) {
		if err == nil {
			err = ErrRuntimeOwnership
		}
		return nil, nil, err
	}
	return openBoundSQLWithApply(path, wal, authority, expectedSQL, expectedApply,
		func(path string, base sqldriver.ReplicatedShardStoreIdentity, apply sqldriver.ReplicatedApplyIdentity, options ...sqldriver.ReplicatedOpenOptions) (*sqldriver.Database, error) {
			return sqldriver.OpenReplicatedShardStoreWithSchemaSourceTransition(path, base, apply, command, options...)
		}, opening...)
}

// OpenBoundNodeSQLWithApplyForSchemaSourceTransition opens the authenticated
// retiring source on the node log. Segmented node logs have no foreground WAL
// generation activation; the caller separately proves the exact committed
// transition entry from this GroupView before requesting catalog settlement.
func OpenBoundNodeSQLWithApplyForSchemaSourceTransition(
	path string, group *raftstore.GroupView,
	authority sqldriver.ReplicatedAuthorityProfile,
	expectedSQL sqldriver.ReplicatedShardStoreIdentity,
	expectedApply sqldriver.ReplicatedApplyIdentity,
	command []byte,
	opening ...sqldriver.ReplicatedOpenOptions,
) (*sqldriver.Database, *sqldriver.ReplicatedApply, error) {
	return openBoundNodeSQLWithApply(
		path, group, authority, expectedSQL, expectedApply,
		func(path string, base sqldriver.ReplicatedShardStoreIdentity, apply sqldriver.ReplicatedApplyIdentity, options ...sqldriver.ReplicatedOpenOptions) (*sqldriver.Database, error) {
			return sqldriver.OpenReplicatedShardStoreWithSchemaSourceTransition(path, base, apply, command, options...)
		}, opening...,
	)
}
