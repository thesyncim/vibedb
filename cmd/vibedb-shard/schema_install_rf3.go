package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/internal/multiraft"
	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/internal/raftstore"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/schemainstall"
	sqldriver "github.com/thesyncim/vibedb/sql/driver"
)

type rf3SchemaOwner interface {
	Probe(context.Context, raftmember.GroupKey) (raftservice.ServingState, error)
	ProposeSchemaTransition(context.Context, raftservice.ServingFence, []byte) error
	ObserveSchemaTransition(context.Context, raftmember.GroupKey, []byte) (bool, error)
	QuiesceSchemaGeneration(context.Context, raftservice.ServingFence, []byte) error
	QuiesceCommittedSchemaGeneration(context.Context, raftmember.GroupKey, []byte) error
	FenceCommittedSchemaGeneration(context.Context, raftmember.GroupKey, []byte) error
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

var rf3SchemaCoordinationTargetDomain = []byte("vibedb/rf3/schema-coordination-target/1\x00")

func rf3SchemaTransitionAuthority(request schemainstall.Request,
	authorization schemainstall.Authorization, authorizationDigest, catalogCAS [32]byte,
) sqldriver.ReplicatedSchemaTransitionAuthority {
	h := sha256.New()
	_, _ = h.Write(rf3SchemaCoordinationTargetDomain)
	_, _ = h.Write(authorization.PreparedGroupRoot[:])
	_, _ = h.Write(request.Group.ClusterID[:])
	_, _ = h.Write(request.Group.ClusterIncarnation[:])
	_, _ = h.Write(request.Group.ShardIncarnation[:])
	_, _ = h.Write(request.Group.GroupID[:])
	var target [32]byte
	h.Sum(target[:0])
	return sqldriver.ReplicatedSchemaTransitionAuthority{
		RequestDigest: request.Operation, AuthorizationDigest: authorizationDigest,
		CatalogCASDigest: catalogCAS, CoordinationSequence: authorization.TargetCatalogGeneration,
		CoordinationSource: authorization.PreparedGroupRoot, CoordinationTarget: target,
	}
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
	_, err := schemainstall.BuildRequestDigest(request)
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
	if err := settlePriorRF3SchemaGeneration(ctx, state); err != nil {
		return target, err
	}
	manifest, err := state.apply.RangeSplitRelationManifestDigest()
	if err != nil || state.base.Binding.AllocationGeneration != uint64(request.AllocationGeneration) ||
		state.base.Binding.Authority.SchemaGeneration != request.FromSchemaGeneration || manifest != request.FromRelationManifestDigest {
		return target, errors.Join(err, schemainstall.ErrConflict)
	}
	// The coordinator operation is the durable schema lineage. Every replica
	// may have a different source cut and therefore a different BuildRequest
	// digest, but all captures must remain attached to the one rollout identity
	// that later authorizes prepare, activation, recovery, and reclamation.
	return state.apply.BuildJournaledReplicatedSchemaDDLTarget(ctx, request.Operation, request.SourceApplied, sql)
}

func (a *rf3SchemaActivator) ResumeSchemaBuild(ctx context.Context, operation [32]byte,
	group raftmember.GroupKey,
) (schemainstall.BuildRequest, string, sqldriver.ReplicatedSchemaDDLTarget, bool, error) {
	var request schemainstall.BuildRequest
	var target sqldriver.ReplicatedSchemaDDLTarget
	if ctx == nil || operation == ([32]byte{}) {
		return request, "", target, false, schemainstall.ErrInvalid
	}
	state, err := a.generation(schemainstall.Request{Group: group})
	if err != nil {
		return request, "", target, false, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.quiesced || state.apply == nil {
		return request, "", target, false, schemainstall.ErrConflict
	}
	publishedTransition, published, err := sqldriver.ObservePublishedReplicatedSchemaTransition(state.path)
	if err != nil {
		return request, "", target, false, err
	}
	completed := published && publishedTransition.RequestDigest == operation &&
		publishedTransition.ToSchemaGeneration == state.base.Binding.Authority.SchemaGeneration
	// Publication precedes the in-memory generation swap. During that narrow
	// cut the current operation is not a predecessor to reclaim: doing so asks
	// the deliberately fenced source machine for ordinary capture work and
	// yields ErrSchemaTransitionPending forever. Leave it intact so the
	// coordinator can replay the exact authorized activation.
	currentPublished := published && publishedTransition.RequestDigest == operation &&
		publishedTransition.ToSchemaGeneration == state.base.Binding.Authority.SchemaGeneration+1
	if !completed && !currentPublished {
		if err := settlePriorRF3SchemaGeneration(ctx, state); err != nil {
			return request, "", target, false, err
		}
	}
	record, found, err := state.apply.ObserveJournaledReplicatedSchemaDDLBuild(operation)
	if err != nil || !found {
		return request, "", target, false, errors.Join(err, schemainstall.ErrMissing)
	}
	if completed {
		if record.Target.Proof.Catalog.SchemaGeneration != publishedTransition.ToSchemaGeneration ||
			record.Target.Proof.Catalog.RelationManifestDigest != publishedTransition.ToManifest ||
			record.Target.Proof.ApplyContract != publishedTransition.ToApplyContract {
			return request, "", target, false, schemainstall.ErrConflict
		}
	} else {
		currentApplied := state.apply.Applied()
		_, staged, stageErr := sqldriver.ObservePreparedReplicatedSchemaTarget(
			state.path, record.Target.Catalog, operation,
		)
		if stageErr != nil {
			return request, "", target, false, stageErr
		}
		if currentApplied > record.SourceApplied && !staged {
			rebased, rebaseErr := state.apply.BuildJournaledReplicatedSchemaDDLTarget(
				ctx, operation, currentApplied, record.SQL,
			)
			if rebaseErr != nil {
				return request, "", target, false, rebaseErr
			}
			record.SourceApplied, record.Target = currentApplied, rebased
		}
	}
	request = schemainstall.BuildRequest{
		Operation: operation, Group: group,
		AllocationGeneration:       distribution.ShardAllocationGeneration(state.base.Binding.AllocationGeneration),
		FromSchemaGeneration:       record.SourceSchemaGeneration,
		FromRelationManifestDigest: record.SourceRelationManifestDigest,
		SourceApplied:              record.SourceApplied,
		SQLBytes:                   uint64(len(record.SQL)),
		SQLDigest:                  sha256.Sum256([]byte(record.SQL)),
	}
	if _, err = schemainstall.BuildRequestDigest(request); err != nil {
		return schemainstall.BuildRequest{}, "", target, false, err
	}
	return request, record.SQL, record.Target, completed, nil
}

// settlePriorRF3SchemaGeneration reclaims the source of the already selected
// catalog before another target is staged. Callers hold the schema-generation
// mutex and the coordinator's exclusive route gate, so no old-generation
// execution pin can be admitted while the exact published transition is
// drained and its capture is reclaimed.
func settlePriorRF3SchemaGeneration(ctx context.Context, state *rf3SchemaGeneration) error {
	transition, found, err := sqldriver.ObservePublishedReplicatedSchemaTransition(state.path)
	if err != nil {
		return fmt.Errorf("observe prior published transition: %w", err)
	}
	if !found {
		return nil
	}
	drained, err := sqldriver.ObserveDrainedReplicatedSchemaSource(state.path, transition.Bytes())
	if err != nil {
		return fmt.Errorf("observe prior drained source: %w", err)
	}
	if !drained {
		if _, err = sqldriver.DrainPublishedReplicatedSchemaSource(state.path, transition.Bytes()); err != nil {
			return fmt.Errorf("drain prior published source: %w", err)
		}
	}
	for {
		done, reclaimErr := state.apply.ReclaimReplicatedSchemaCapture(ctx, transition.Bytes(), 1024)
		if reclaimErr != nil {
			return fmt.Errorf("reclaim prior schema capture: %w", reclaimErr)
		}
		if done {
			return nil
		}
	}
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
		return nil, nil, fmt.Errorf("schema activation generation: %w", err)
	}
	raw, err := a.artifact(request, request.BundleDigest)
	if err != nil {
		return nil, nil, fmt.Errorf("schema activation artifact: %w", err)
	}
	witness, found, err := sqldriver.ObservePreparedReplicatedSchemaTarget(
		state.path, raw, request.Operation,
	)
	wantInstallation := schemainstall.InstallationDigest(request,
		schemainstall.MaterializedArtifactDigest(request.BundleDigest, witness))
	if err != nil || !found || wantInstallation != installation {
		clear(raw)
		return nil, nil, fmt.Errorf("schema activation prepared target found=%t installation-match=%t: %w",
			found, wantInstallation == installation, errors.Join(err, schemainstall.ErrConflict))
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
	if rf3SchemaTransitionIsPredecessor(request, transition) {
		return false, nil
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
	alias func() (bool, error),
) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		committed, err := owners.ObserveSchemaTransition(ctx, group, command)
		if err == nil && committed {
			return nil
		}
		if err == nil && alias != nil {
			committed, err = alias()
			if err == nil && committed {
				return nil
			}
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

func (a *rf3SchemaActivator) Commit(
	ctx context.Context, request schemainstall.Request, authorization schemainstall.Authorization,
	installation [32]byte, _ string,
) error {
	if err := a.activate(ctx, request, authorization, installation, false); err != nil {
		return err
	}
	state, err := a.generation(request)
	if err != nil {
		return err
	}
	transition, found, err := sqldriver.ObservePublishedReplicatedSchemaTransition(state.path)
	if err != nil || !found {
		return errors.Join(err, schemainstall.ErrOutcomeUnknown)
	}
	err = a.owners.FenceCommittedSchemaGeneration(ctx, request.Group, transition.Bytes())
	return err
}

func (a *rf3SchemaActivator) Activate(
	ctx context.Context, request schemainstall.Request, authorization schemainstall.Authorization,
	installation [32]byte, _ string,
) error {
	return a.activate(ctx, request, authorization, installation, true)
}

func (a *rf3SchemaActivator) activate(
	ctx context.Context, request schemainstall.Request, authorization schemainstall.Authorization,
	installation [32]byte, install bool,
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
	predecessor := published && rf3SchemaTransitionIsPredecessor(request, transition)
	if predecessor {
		published = false
	}
	var preparedApplied, preCommandApplied uint64
	var emptySuffix bool
	if published {
		command, err = rf3SchemaActivationCommand(request, authorization, transition, true, nil)
		if err != nil {
			return err
		}
	} else {
		var persisted replicatedstate.SchemaTransitionView
		var found bool
		if !predecessor {
			var readErr error
			persisted, found, readErr = sqldriver.ObservePersistedReplicatedSchemaTransition(state.path)
			if readErr != nil {
				return readErr
			}
		}
		if !found {
			var built bool
			var sourceErr error
			preparedApplied, built, sourceErr = state.apply.ReplicatedSchemaDDLSourceApplied(raw)
			if sourceErr != nil || !built {
				return errors.Join(sourceErr, schemainstall.ErrConflict)
			}
			preCommandApplied = state.apply.Applied()
			emptySuffix = preCommandApplied > preparedApplied
			if emptySuffix {
				if err := rf3SchemaReplayNeutralSuffix(state.wal, preparedApplied, preCommandApplied, request.Operation, request.Group); err != nil {
					return err
				}
			}
		}
		command, err = rf3SchemaActivationCommand(request, authorization, persisted, found, func() ([]byte, error) {
			var proof sqldriver.ReplicatedSchemaTargetProof
			var recoverErr error
			if state.verified != nil && emptySuffix {
				proof, recoverErr = state.verified.ResumePreparedAfterEmptySuffix(
					ctx, state.apply, raw, request.Operation, preCommandApplied,
				)
			} else if state.verified != nil {
				proof, recoverErr = state.verified.ResumePrepared(ctx, state.apply, raw, request.Operation)
			} else if emptySuffix {
				proof, recoverErr = state.apply.RecoverPreparedReplicatedSchemaTargetAfterEmptySuffix(raw, request.Operation)
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
			authority := rf3SchemaTransitionAuthority(request, authorization, authorizationDigest, cas)
			var fresh []byte
			var appendErr error
			if emptySuffix {
				fresh, appendErr = state.apply.AppendReplicatedSchemaTransitionAfterEmptySuffix(
					nil, proof, authority, preCommandApplied,
				)
			} else {
				fresh, appendErr = state.apply.AppendReplicatedSchemaTransition(nil, proof, authority)
			}
			if appendErr != nil {
				return nil, appendErr
			}
			return fresh, nil
		})
		if err != nil {
			return fmt.Errorf("schema activation recover command: %w", err)
		}
		if !found {
			if emptySuffix {
				err = state.apply.PersistReplicatedSchemaTransitionAfterEmptySuffix(
					command, preparedApplied, preCommandApplied,
				)
			} else {
				err = state.apply.PersistReplicatedSchemaTransition(command)
			}
			if err != nil {
				return fmt.Errorf("schema activation persist command: %w", err)
			}
		}
		if err = settleRF3SchemaCommitWithAlias(ctx, a.owners, request.Group, command, func() (bool, error) {
			applied := state.apply.Applied()
			committed, aliasErr := rf3SchemaCommittedTransitionAlias(state.wal, applied)
			if aliasErr != nil {
				return false, aliasErr
			}
			if !replicatedstate.IsSchemaTransition(committed) {
				return false, nil
			}
			_, observed, aliasErr := state.apply.ObserveReplicatedSchemaTransitionAlias(command, committed)
			return observed, aliasErr
		}); err != nil {
			return fmt.Errorf("schema activation commit command: %w", err)
		}
		if _, err = state.apply.PublishReplicatedSchemaCatalog(); err != nil {
			return fmt.Errorf("schema activation publish catalog: %w", err)
		}
	}
	if !install {
		return nil
	}
	if !state.quiesced {
		if err = waitRF3SchemaGenerationQuiesced(ctx, a.owners, request.Group, command); err != nil {
			return fmt.Errorf("schema activation quiesce committed source: %w", err)
		}
		state.quiesced = true
	}
	targetBase, targetApply, found, err := sqldriver.PublishedReplicatedSchemaActivationIdentity(state.path)
	if err != nil || !found {
		return errors.Join(err, schemainstall.ErrOutcomeUnknown)
	}
	opening, err := rf3SchemaTargetRecoveryOpening(state.path, state.wal, nil)
	if err != nil {
		return fmt.Errorf("schema activation bind committed transition: %w", err)
	}
	database, err := sqldriver.OpenReplicatedShardStoreWithApply(
		state.path, targetBase, targetApply, opening...,
	)
	if err != nil {
		return fmt.Errorf("schema activation open target database: %w", err)
	}
	bootstrap, err := state.wal.Snapshot()
	if err != nil {
		_ = database.Close()
		return fmt.Errorf("schema activation read target bootstrap: %w", err)
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
		return fmt.Errorf("schema activation open target apply identity-match=%t: %w",
			got == targetApply, errors.Join(err, schemainstall.ErrOutcomeUnknown))
	}
	if err = a.owners.InstallSchemaGeneration(
		ctx, request.Group, database, apply, targetBase, targetApply,
	); err != nil {
		_ = apply.Close()
		_ = database.Close()
		return fmt.Errorf("schema activation install target generation: %w", err)
	}
	state.base, state.applyID, state.apply = targetBase, targetApply, apply
	state.verified = nil
	state.quiesced = false
	return nil
}

// The schema command can become visible one scheduling turn before its
// applied-batch result settlement releases the multiraft group. Quiescing at
// that instant is a retryable local scheduling race, not an unknown schema
// outcome. Retry the same authenticated fence while refreshing leadership;
// no command is reproposed and the serving hot path remains allocation-free.
func waitRF3SchemaGenerationQuiesced(ctx context.Context, owners rf3SchemaOwner,
	group raftmember.GroupKey, command []byte,
) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		err := owners.QuiesceCommittedSchemaGeneration(ctx, group, command)
		if err == nil {
			return nil
		}
		if !errors.Is(err, raftservice.ErrServingFence) && !errors.Is(err, multiraft.ErrGroupBusy) {
			return err
		}
		select {
		case <-ctx.Done():
			return errors.Join(err, context.Cause(ctx))
		case <-ticker.C:
		}
	}
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
	drained, err := sqldriver.ObserveDrainedReplicatedSchemaSource(state.path, transition.Bytes())
	if err != nil || !drained {
		return false, err
	}
	return state.apply.ObserveReclaimedReplicatedSchemaCapture(ctx, transition.Bytes())
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
	if _, err = sqldriver.DrainPublishedReplicatedSchemaSource(state.path, transition.Bytes()); err != nil {
		return err
	}
	for {
		done, err := state.apply.ReclaimReplicatedSchemaCapture(ctx, transition.Bytes(), 1024)
		if err != nil || done {
			return err
		}
	}
}

var _ schemainstall.Activator = (*rf3SchemaActivator)(nil)
