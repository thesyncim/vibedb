package driver

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/internal/rangesplit"
	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/schemachange"
)

// restoreSchemaCaptureLocked runs only during apply-claim construction, before
// serving or Raft replay can advance beyond an unfinished local build stream.
func (a *ReplicatedApply) restoreSchemaCaptureLocked() error {
	if a.rangeSplitCapture != nil || a.database.replicatedCaptureCollection == nil || a.database.replicatedCaptureCollection.Len() == 0 {
		return nil
	}
	target := replicatedstate.TransitionCaptureTarget{Name: replicatedstate.TransitionCaptureCollectionName, Collection: a.database.replicatedCaptureCollection}
	var key [8]byte
	raw, found, err := target.Collection.AppendRaw(nil, key[:])
	if err != nil || !found {
		return errors.Join(err, schemachange.ErrCapture)
	}
	// Legacy split controllers also support a locally installed capture. They
	// recover it from their retained recipe, not from the schema grammar.
	if rangesplit.RecognizesSourceCaptureHeader(raw) {
		return nil
	}
	capture, found, err := schemachange.RestoreSourceCapture(target)
	if err != nil || !found {
		return errors.Join(err, schemachange.ErrCapture)
	}
	if err := a.machine.BeginTransitionCapture(capture); err != nil {
		return err
	}
	descriptor, err := capture.Descriptor()
	if err != nil {
		return err
	}
	if !capture.CaptureStopped() {
		fence, err := a.machine.SnapshotAuthorizationFence()
		if err != nil || descriptor.Config.ManifestDigest != fence.RelationManifestDigest {
			return errors.Join(err, schemachange.ErrCapture)
		}
	}
	a.schemaCapture = capture
	return nil
}

// BeginReplicatedSchemaCapture installs a bounded, replica-local build stream.
// It changes no serving authority, source rows, session identity or frozen
// provisioning limit. Each replica may start at its own cut; the coordinator
// must reconcile every target to one common cut before schema activation.
func (a *ReplicatedApply) BeginReplicatedSchemaCapture(ctx context.Context, operation, plan [32]byte, maxRecords, maxBytes uint64) (schemachange.CaptureDescriptor, error) {
	if a == nil || a.database == nil || ctx == nil {
		return schemachange.CaptureDescriptor{}, ErrReplicatedApplyClosed
	}
	if err := ctx.Err(); err != nil {
		return schemachange.CaptureDescriptor{}, err
	}
	a.database.mu.Lock()
	defer a.database.mu.Unlock()
	if err := a.checkLocked(); err != nil {
		return schemachange.CaptureDescriptor{}, err
	}
	if err := a.checkActivationBaseLocked(); err != nil {
		return schemachange.CaptureDescriptor{}, err
	}
	if a.schemaCapture != nil {
		d, err := a.schemaCapture.Descriptor()
		if err != nil || d.Config.Operation != operation || d.Config.PlanDigest != plan || d.Config.MaxRecords != maxRecords || d.Config.MaxBytes != maxBytes {
			return schemachange.CaptureDescriptor{}, errors.Join(err, ErrReplicatedSchemaDDLConflict)
		}
		return d, nil
	}
	if a.rangeSplitCapture != nil || a.database.replicatedCaptureCollection == nil || a.database.replicatedCaptureCollection.Len() != 0 {
		return schemachange.CaptureDescriptor{}, ErrReplicatedSchemaDDLConflict
	}
	fence, err := a.machine.SnapshotAuthorizationFence()
	if err != nil {
		return schemachange.CaptureDescriptor{}, err
	}
	config := schemachange.CaptureConfig{Operation: operation, PlanDigest: plan,
		BindingDigest: replicatedstate.SplitCaptureBindingDigest(fence.Binding), ManifestDigest: fence.RelationManifestDigest,
		SchemaGeneration: fence.Binding.SchemaGeneration, MaxRecords: maxRecords, MaxBytes: maxBytes}
	capture, err := schemachange.NewSourceCapture(config, replicatedstate.TransitionCaptureTarget{
		Name: replicatedstate.TransitionCaptureCollectionName, Collection: a.database.replicatedCaptureCollection,
	})
	if err != nil {
		return schemachange.CaptureDescriptor{}, err
	}
	if err := a.machine.BeginTransitionCapture(capture); err != nil {
		return schemachange.CaptureDescriptor{}, err
	}
	a.schemaCapture = capture
	return capture.Descriptor()
}

func (a *ReplicatedApply) ReplicatedSchemaCaptureDescriptor(operation [32]byte) (schemachange.CaptureDescriptor, error) {
	if a == nil || a.database == nil {
		return schemachange.CaptureDescriptor{}, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return schemachange.CaptureDescriptor{}, err
	}
	if a.schemaCapture == nil {
		return schemachange.CaptureDescriptor{}, ErrReplicatedSchemaDDLConflict
	}
	d, err := a.schemaCapture.Descriptor()
	if err != nil || d.Config.Operation != operation {
		return schemachange.CaptureDescriptor{}, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	return d, nil
}

func (a *ReplicatedApply) ReadReplicatedSchemaCapture(ctx context.Context, operation [32]byte, cursor schemachange.Cursor, workspace *schemachange.CaptureWorkspace) (schemachange.Entry, bool, error) {
	if a == nil || a.database == nil || ctx == nil {
		return schemachange.Entry{}, false, ErrReplicatedApplyClosed
	}
	if err := ctx.Err(); err != nil {
		return schemachange.Entry{}, false, err
	}
	a.database.mu.RLock()
	err := a.checkLocked()
	capture := a.schemaCapture
	a.database.mu.RUnlock()
	if err != nil || capture == nil {
		return schemachange.Entry{}, false, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	d, err := capture.Descriptor()
	if err != nil || d.Config.Operation != operation {
		return schemachange.Entry{}, false, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	// No database or capture-publication lock is held across the storage read.
	return capture.Next(cursor, workspace)
}

// FinishReplicatedSchemaCapture is historical/idempotent after a successful
// seal. Its returned cut is not proof that serving still equals that cut;
// schema prepare/activation must perform their own exact applied comparison.
func (a *ReplicatedApply) FinishReplicatedSchemaCapture(ctx context.Context, operation [32]byte, expectedApplied uint64) (schemachange.CaptureDescriptor, error) {
	if a == nil || a.database == nil || ctx == nil {
		return schemachange.CaptureDescriptor{}, ErrReplicatedApplyClosed
	}
	if err := ctx.Err(); err != nil {
		return schemachange.CaptureDescriptor{}, err
	}
	a.database.mu.Lock()
	defer a.database.mu.Unlock()
	if err := a.checkLocked(); err != nil {
		return schemachange.CaptureDescriptor{}, err
	}
	if a.schemaCapture == nil {
		return schemachange.CaptureDescriptor{}, ErrReplicatedSchemaDDLConflict
	}
	d, err := a.schemaCapture.Descriptor()
	if err != nil || d.Config.Operation != operation || d.Abort != schemachange.NotAborted || d.Head.Publication.Applied != expectedApplied {
		return schemachange.CaptureDescriptor{}, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	if d.SealDigest != [32]byte{} {
		return d, nil
	}
	if err := a.machine.FinishTransitionCapture(a.schemaCapture, expectedApplied); err != nil {
		return schemachange.CaptureDescriptor{}, err
	}
	return a.schemaCapture.Descriptor()
}
