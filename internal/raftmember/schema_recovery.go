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
		func(path string, base sqldriver.ReplicatedShardStoreIdentity, apply sqldriver.ReplicatedApplyIdentity) (*sqldriver.Database, error) {
			return sqldriver.OpenReplicatedShardStoreWithSchemaSourceTransition(path, base, apply, command)
		})
}
