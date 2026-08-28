package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type rf3SchemaOwner interface {
	Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error)
	ProposeSchemaTransition(context.Context, raftservice.ServingFence, []byte) error
	ObserveSchemaTransition(context.Context, raftmember.GroupKey, []byte) (bool, error)
	QuiesceSchemaGeneration(context.Context, raftservice.ServingFence, []byte) error
	InstallSchemaGeneration(context.Context, raftmember.GroupKey, *sqldriver.Database,
		*sqldriver.ReplicatedApply, sqldriver.ReplicatedShardStoreIdentity,
		sqldriver.ReplicatedApplyIdentity) error
}

type rf3SchemaArtifactSource interface {
	OpenArtifact(schemainstall.Request) (*os.File, error)
}

type rf3SchemaGeneration struct {
	mu      sync.Mutex
	path    string
	wal     *raftstore.Store
	base    sqldriver.ReplicatedShardStoreIdentity
	applyID sqldriver.ReplicatedApplyIdentity
	apply   *sqldriver.ReplicatedApply
	// Closed after staging: retains only the opaque image audit/target proof,
	// never open files. Process recovery may reconstruct it by auditing again.
	verified *sqldriver.VerifiedReplicatedSchemaTarget
	quiesced bool
}

type rf3SchemaActivator struct {
	owners rf3SchemaOwner
	mu     sync.RWMutex
	files  rf3SchemaArtifactSource
	groups map[raftmember.GroupKey]*rf3SchemaGeneration
}

func newRF3SchemaActivator(
	owners rf3SchemaOwner, groups []preparedRF3Group,
) (*rf3SchemaActivator, error) {
	if owners == nil || len(groups) == 0 {
		return nil, errRF3Serving
	}
	result := &rf3SchemaActivator{owners: owners,
		groups: make(map[raftmember.GroupKey]*rf3SchemaGeneration, len(groups))}
	for i := range groups {
		item := &groups[i]
		group := groupFromBinding(item.base.Binding)
		if group == (raftmember.GroupKey{}) || item.wal == nil || item.apply == nil ||
			item.manifest.SQL.Path == "" {
			return nil, errRF3Serving
		}
		if _, exists := result.groups[group]; exists {
			return nil, errRF3Serving
		}
		result.groups[group] = &rf3SchemaGeneration{path: item.manifest.SQL.Path,
			wal: item.wal, base: item.base.Clone(), applyID: item.applyIdentity,
			apply: item.apply}
	}
	return result, nil
}

func (a *rf3SchemaActivator) bindArtifacts(source rf3SchemaArtifactSource) error {
	if a == nil || source == nil {
		return errRF3Serving
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.files != nil && a.files != source {
		return errRF3Serving
	}
	a.files = source
	return nil
}

func (a *rf3SchemaActivator) generation(
	request schemainstall.Request,
) (*rf3SchemaGeneration, error) {
	if a == nil {
		return nil, schemainstall.ErrInvalid
	}
	a.mu.RLock()
	state := a.groups[request.Group]
	a.mu.RUnlock()
	if state == nil {
		return nil, schemainstall.ErrInvalid
	}
	return state, nil
}

func (a *rf3SchemaActivator) BuildSchema(ctx context.Context, request schemainstall.BuildRequest, sql string) (sqldriver.ReplicatedSchemaDDLTarget, error) {
	var target sqldriver.ReplicatedSchemaDDLTarget
	digest, err := schemainstall.BuildRequestDigest(request)
	if err != nil || uint64(len(sql)) != request.SQLBytes || sha256.Sum256([]byte(sql)) != request.SQLDigest {
		return target, errors.Join(err, schemainstall.ErrInvalid)
	}
	state, err := a.generation(schemainstall.Request{Group: request.Group})
	if err != nil {
		return target, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.quiesced || state.apply == nil {
		return target, schemainstall.ErrConflict
	}
	manifest, err := state.apply.RangeSplitRelationManifestDigest()
	if err != nil || state.base.Binding.AllocationGeneration != uint64(request.AllocationGeneration) ||
		state.base.Binding.Authority.SchemaGeneration != request.FromSchemaGeneration || manifest != request.FromRelationManifestDigest {
		return target, errors.Join(err, schemainstall.ErrConflict)
	}
	return state.apply.BuildJournaledReplicatedSchemaDDLTarget(ctx, digest, request.SourceApplied, sql)
}

func (a *rf3SchemaActivator) artifact(
	request schemainstall.Request, digest [sha256.Size]byte,
) ([]byte, error) {
	a.mu.RLock()
	files := a.files
	a.mu.RUnlock()
	if files == nil || digest != request.BundleDigest || request.BundleBytes == 0 ||
		request.BundleBytes > schemainstall.AbsoluteMaxBundleBytes {
		return nil, schemainstall.ErrInvalid
	}
	file, err := files.OpenArtifact(request)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, int64(request.BundleBytes)+1))
	err = errors.Join(readErr, file.Close())
	if err != nil || uint64(len(raw)) != request.BundleBytes || sha256.Sum256(raw) != digest {
		clear(raw)
		return nil, errors.Join(err, schemainstall.ErrConflict)
	}
	return raw[:len(raw):len(raw)], nil
}

func validateRF3SchemaTarget(
	request schemainstall.Request, image sqldriver.ReplicatedSchemaCatalogImage,
	contract [sha256.Size]byte,
) error {
	if !image.MatchesRolloutTarget(request.BundleBytes, request.BundleDigest,
		request.ToSchemaGeneration, request.ToRelationManifestDigest) ||
		request.ToSchemaGeneration != request.FromSchemaGeneration+1 ||
		contract != request.ApplyContractDigest {
		return schemainstall.ErrConflict
	}
	return nil
}

func (a *rf3SchemaActivator) ObserveStaged(
	ctx context.Context, request schemainstall.Request, digest [32]byte, _ string,
) ([32]byte, bool, error) {
	state, err := a.generation(request)
	if err != nil {
		return [32]byte{}, false, err
	}
	raw, err := a.artifact(request, digest)
	if err != nil {
		return [32]byte{}, false, err
	}
	defer clear(raw)
	if cause := context.Cause(ctx); cause != nil {
		return [32]byte{}, false, cause
	}
	image, err := sqldriver.ValidateReplicatedSchemaCatalogImage(raw)
	if err != nil || validateRF3SchemaTarget(request, image, request.ApplyContractDigest) != nil {
		return [32]byte{}, false, errors.Join(err, schemainstall.ErrConflict)
	}
	witness, found, err := sqldriver.ObservePreparedReplicatedSchemaTarget(
		state.path, raw, request.Operation,
	)
	return witness, found, err
}

func (a *rf3SchemaActivator) Stage(
	ctx context.Context, request schemainstall.Request, digest [32]byte, _ string,
) ([32]byte, error) {
	state, err := a.generation(request)
	if err != nil {
		return [32]byte{}, err
	}
	raw, err := a.artifact(request, digest)
	if err != nil {
		return [32]byte{}, err
	}
	defer clear(raw)
	state.mu.Lock()
	defer state.mu.Unlock()
	manifest, err := state.apply.RangeSplitRelationManifestDigest()
	if err != nil || state.base.Binding.AllocationGeneration != uint64(request.AllocationGeneration) ||
		state.base.Binding.Authority.SchemaGeneration != request.FromSchemaGeneration ||
		manifest != request.FromRelationManifestDigest {
		return [32]byte{}, errors.Join(err, schemainstall.ErrConflict)
	}
	applied, built, err := state.apply.ReplicatedSchemaDDLSourceApplied(raw)
	if err != nil {
		return [32]byte{}, err
	}
	if !built {
		applied = state.apply.Applied()
	}
	verified, err := state.apply.PreflightReplicatedSchemaTarget(ctx, raw, applied)
	if err != nil {
		return [32]byte{}, errors.Join(err, schemainstall.ErrConflict)
	}
	proof, prepareErr := verified.Prepare(ctx, request.Operation)
	err = errors.Join(prepareErr, verified.Close())
	if err != nil || validateRF3SchemaTarget(request, proof.Catalog, proof.ApplyContract) != nil {
		return [32]byte{}, errors.Join(err, schemainstall.ErrConflict)
	}
	state.verified = verified
	return proof.Witness, nil
}

func (a *rf3SchemaActivator) exactInstallation(
	ctx context.Context, request schemainstall.Request, authorization schemainstall.Authorization,
	installation [32]byte,
) (*rf3SchemaGeneration, []byte, error) {
	state, err := a.generation(request)
	if err != nil {
		return nil, nil, err
	}
	raw, err := a.artifact(request, request.BundleDigest)
	if err != nil {
		return nil, nil, err
	}
	witness, found, err := sqldriver.ObservePreparedReplicatedSchemaTarget(
		state.path, raw, request.Operation,
	)
	if err != nil || !found ||
		schemainstall.InstallationDigest(request,
			schemainstall.MaterializedArtifactDigest(request.BundleDigest, witness)) != installation {
		clear(raw)
		return nil, nil, errors.Join(err, schemainstall.ErrConflict)
	}
	if cause := context.Cause(ctx); cause != nil {
		clear(raw)
		return nil, nil, cause
	}
	return state, raw, nil
}

func (a *rf3SchemaActivator) ObserveActive(
	ctx context.Context, request schemainstall.Request, authorization schemainstall.Authorization,
	installation [32]byte, _ string,
) (bool, error) {
	state, raw, err := a.exactInstallation(ctx, request, authorization, installation)
	if err != nil {
		return false, err
	}
	defer clear(raw)
	transition, found, err := sqldriver.ObservePublishedReplicatedSchemaTransition(state.path)
	if err != nil || !found {
		return false, err
	}
	if err := validateRF3SchemaTransition(request, authorization, transition); err != nil {
		return false, err
	}
	state.mu.Lock()
	live := state.base.Binding.Authority.SchemaGeneration == request.ToSchemaGeneration &&
		state.base.Binding.AllocationGeneration == uint64(request.AllocationGeneration) && state.apply != nil &&
		!state.quiesced
	if live {
		contract, contractErr := state.apply.SchemaApplyContractDigest()
		live, err = contractErr == nil && contract == request.ApplyContractDigest, contractErr
	}
	state.mu.Unlock()
	if err != nil || !live {
		return false, err
	}
	serving, err := a.owners.Probe(ctx, request.Group)
	if err != nil {
		return false, err
	}
	return serving.Command.SchemaGeneration == request.ToSchemaGeneration &&
		serving.Command.RelationManifestDigest == request.ToRelationManifestDigest, nil
}

func waitRF3SchemaCommit(
	ctx context.Context, owners rf3SchemaOwner, group raftmember.GroupKey, command []byte,
) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		committed, err := owners.ObserveSchemaTransition(ctx, group, command)
		if err == nil && committed {
			return nil
		}
		if err != nil && !errors.Is(err, raftservice.ErrServingFence) {
			return err
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-ticker.C:
		}
	}
}

func (a *rf3SchemaActivator) Activate(
	ctx context.Context, request schemainstall.Request, authorization schemainstall.Authorization,
	installation [32]byte, _ string,
) error {
	state, raw, err := a.exactInstallation(ctx, request, authorization, installation)
	if err != nil {
		return err
	}
	defer clear(raw)
	state.mu.Lock()
	defer state.mu.Unlock()
	authorizationDigest := schemainstall.AuthorizationDigest(authorization)
	transition, published, err := sqldriver.ObservePublishedReplicatedSchemaTransition(state.path)
	var command []byte
	if err != nil {
		return err
	}
	if published {
		command, err = rf3SchemaActivationCommand(request, authorization, transition, true, nil)
		if err != nil {
			return err
		}
	} else {
		persisted, found, readErr := sqldriver.ObservePersistedReplicatedSchemaTransition(state.path)
		if readErr != nil {
			return readErr
		}
		command, err = rf3SchemaActivationCommand(request, authorization, persisted, found, func() ([]byte, error) {
			var proof sqldriver.ReplicatedSchemaTargetProof
			var recoverErr error
			if state.verified != nil {
				proof, recoverErr = state.verified.ResumePrepared(ctx, state.apply, raw, request.Operation)
			} else {
				proof, recoverErr = state.apply.RecoverPreparedReplicatedSchemaTarget(raw, request.Operation)
			}
			if recoverErr != nil || validateRF3SchemaTarget(request, proof.Catalog, proof.ApplyContract) != nil {
				return nil, errors.Join(recoverErr, schemainstall.ErrConflict)
			}
			cas, casErr := state.apply.ReplicatedSchemaCatalogCASDigest(proof, request.Operation, authorizationDigest)
			if casErr != nil {
				return nil, casErr
			}
			fresh, appendErr := state.apply.AppendReplicatedSchemaTransition(nil, proof,
				sqldriver.ReplicatedSchemaTransitionAuthority{RequestDigest: request.Operation,
					AuthorizationDigest: authorizationDigest, CatalogCASDigest: cas})
			if appendErr != nil {
				return nil, appendErr
			}
			return fresh, nil
		})
		if err != nil {
			return err
		}
		if !found {
			if err = state.apply.PersistReplicatedSchemaTransition(command); err != nil {
				return err
			}
		}
		if err = settleRF3SchemaCommit(ctx, a.owners, request.Group, command); err != nil {
			return err
		}
		if _, err = state.apply.PublishReplicatedSchemaCatalog(); err != nil {
			return err
		}
	}
	if !state.quiesced {
		serving, probeErr := a.owners.Probe(ctx, request.Group)
		if probeErr != nil {
			return probeErr
		}
		if err = a.owners.QuiesceSchemaGeneration(ctx, serving.Fence(), command); err != nil {
			return err
		}
		state.quiesced = true
	}
	targetBase, targetApply, found, err := sqldriver.PublishedReplicatedSchemaActivationIdentity(state.path)
	if err != nil || !found {
		return errors.Join(err, schemainstall.ErrOutcomeUnknown)
	}
	database, err := sqldriver.OpenReplicatedShardStoreWithApply(state.path, targetBase, targetApply)
	if err != nil {
		return err
	}
	bootstrap, err := state.wal.Snapshot()
	if err != nil {
		_ = database.Close()
		return err
	}
	var apply *sqldriver.ReplicatedApply
	var got sqldriver.ReplicatedApplyIdentity
	if state.verified != nil {
		apply, got, err = state.verified.OpenActivatedApply(database, targetBase, bootstrap, replicatedApplyOptions(targetApply))
	} else {
		// A replacement process has no retained audit. Its existing cold
		// recovery path must validate the full target, never manufacture proof.
		apply, got, err = database.OpenReplicatedApply(targetBase, bootstrap, replicatedApplyOptions(targetApply))
	}
	if err != nil || got != targetApply {
		_ = database.Close()
		return errors.Join(err, schemainstall.ErrOutcomeUnknown)
	}
	if err = a.owners.InstallSchemaGeneration(
		ctx, request.Group, database, apply, targetBase, targetApply,
	); err != nil {
		_ = apply.Close()
		_ = database.Close()
		return err
	}
	state.base, state.applyID, state.apply = targetBase, targetApply, apply
	state.verified = nil
	state.quiesced = false
	return nil
}

func (a *rf3SchemaActivator) ObserveDrained(
	ctx context.Context, request schemainstall.Request, authorization schemainstall.Authorization,
	proof schemainstall.DrainProof, installation [32]byte,
) (bool, error) {
	state, err := a.generation(request)
	if err != nil {
		return false, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	_, raw, err := a.exactInstallation(ctx, request, authorization, installation)
	clear(raw)
	if err != nil || proof.ActivationAuthorizationDigest != schemainstall.AuthorizationDigest(authorization) {
		return false, errors.Join(err, schemainstall.ErrConflict)
	}
	transition, found, err := sqldriver.ObservePublishedReplicatedSchemaTransition(state.path)
	if err != nil || !found {
		return false, err
	}
	if err := validateRF3SchemaTransition(request, authorization, transition); err != nil {
		return false, err
	}
	return sqldriver.ObserveDrainedReplicatedSchemaSource(state.path, transition.Bytes())
}

func (a *rf3SchemaActivator) DrainOld(
	ctx context.Context, request schemainstall.Request, authorization schemainstall.Authorization,
	proof schemainstall.DrainProof, installation [32]byte,
) error {
	state, err := a.generation(request)
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	_, raw, err := a.exactInstallation(ctx, request, authorization, installation)
	clear(raw)
	if err != nil || proof.ActivationAuthorizationDigest != schemainstall.AuthorizationDigest(authorization) {
		return errors.Join(err, schemainstall.ErrConflict)
	}
	transition, found, err := sqldriver.ObservePublishedReplicatedSchemaTransition(state.path)
	if err != nil || !found {
		return errors.Join(err, schemainstall.ErrConflict)
	}
	if err := validateRF3SchemaTransition(request, authorization, transition); err != nil {
		return err
	}
	_, err = sqldriver.DrainPublishedReplicatedSchemaSource(state.path, transition.Bytes())
	return err
}

var _ schemainstall.Activator = (*rf3SchemaActivator)(nil)
