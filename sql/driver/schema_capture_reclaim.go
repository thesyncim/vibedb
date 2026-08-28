package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/schemachange"
)

func schemaCaptureMatchesRetiredLineage(config schemachange.CaptureConfig, lineage replicatedSchemaLineage) bool {
	return schemaCaptureMatchesTransition(config, lineage.activation.command)
}

func schemaCaptureMatchesTransition(config schemachange.CaptureConfig, command []byte) bool {
	t, err := replicatedstate.OpenSchemaTransition(command)
	return err == nil && config.Operation == t.RequestDigest && config.SchemaGeneration == t.From.SchemaGeneration &&
		config.BindingDigest == replicatedstate.SplitCaptureBindingDigest(t.From) && config.ManifestDigest == t.FromManifest
}

func schemaShadowMatchesRetiredLineage(shadow schemaDDLShadowRecord, lineage replicatedSchemaLineage) bool {
	t, err := replicatedstate.OpenSchemaTransition(lineage.activation.command)
	return err == nil && shadow.Ready && schemaCaptureMatchesRetiredLineage(shadow.Capture, lineage) &&
		bytes.Equal(shadow.Shadow.Catalog, lineage.catalog) && shadow.Shadow.Cursor.Publication.Applied == lineage.marker.sourceApplied &&
		replicatedSchemaCatalogCASDigest(shadow.SourceDigest, lineage.activation.targetDigest, t.RequestDigest, t.AuthorizationDigest) == t.CatalogCASDigest
}

// A selected, drained lineage is durable permission to ignore a partially
// reclaimed capture prefix on restart. An active or foreign capture still
// goes through full stream validation; no row absence is treated as authority.
func (a *ReplicatedApply) schemaCaptureIsRetired(config schemachange.CaptureConfig) (bool, error) {
	lineage, selected, err := selectedSchemaLineage(a.database.path)
	if err != nil || !selected {
		return false, err
	}
	return schemaCaptureMatchesRetiredLineage(config, lineage), nil
}

// ObserveReclaimedReplicatedSchemaCapture reports completion only after both
// capture rows and the completed build journal are absent. Drained relation
// files alone do not prove that a partially reclaimed capture is finished.
func (a *ReplicatedApply) ObserveReclaimedReplicatedSchemaCapture(ctx context.Context, command []byte) (bool, error) {
	root, unlock, err := a.lockSchemaDDLShadow(ctx)
	if err != nil {
		return false, err
	}
	defer unlock()
	a.database.mu.RLock()
	defer a.database.mu.RUnlock()
	if err := a.checkLocked(); err != nil {
		return false, err
	}
	if _, err := a.machine.SnapshotAuthorizationFence(); err != nil {
		return false, err
	}
	lineage, selected, err := selectedSchemaLineage(a.database.path)
	if err != nil || !selected || !bytes.Equal(command, lineage.activation.command) {
		return false, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	_, found, err := readSchemaDDLShadowRecord(root)
	if err != nil || found {
		return false, err
	}
	collection := a.database.replicatedCaptureCollection
	if collection == nil {
		return false, ErrReplicatedSchemaDDLConflict
	}
	if collection.Len() != 0 {
		return false, nil
	}
	if err := syncSchemaNamespaceDirectory(root, "."); err != nil {
		return false, err
	}
	return true, nil
}

// ReclaimReplicatedSchemaCapture reclaims at most maxEntries retired capture
// rows, then removes the exact completed shadow journal. It requires selected,
// drained schema lineage, not just a prepared/activated target. Call repeatedly
// outside the cutover fence; each call releases the serving lock after one
// bounded durability batch. Interrupted cleanup resumes using the same lineage.
func (a *ReplicatedApply) ReclaimReplicatedSchemaCapture(ctx context.Context, command []byte, maxEntries int) (bool, error) {
	if maxEntries < 1 || maxEntries > 1024 {
		return false, ErrReplicatedSchemaDDLConflict
	}
	root, unlock, err := a.lockSchemaDDLShadow(ctx)
	if err != nil {
		return false, err
	}
	defer unlock()
	a.database.mu.Lock()
	defer a.database.mu.Unlock()
	if err := a.checkLocked(); err != nil {
		return false, err
	}
	if _, err := a.machine.SnapshotAuthorizationFence(); err != nil {
		return false, err
	}
	lineage, selected, err := selectedSchemaLineage(a.database.path)
	if err != nil || !selected || !bytes.Equal(command, lineage.activation.command) {
		return false, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	shadow, shadowFound, err := readSchemaDDLShadowRecord(root)
	if err != nil {
		return false, err
	}
	if shadowFound && !schemaShadowMatchesRetiredLineage(shadow, lineage) {
		return false, ErrReplicatedSchemaDDLConflict
	}
	collection := a.database.replicatedCaptureCollection
	if collection == nil {
		return false, ErrReplicatedSchemaDDLConflict
	}
	if collection.Len() != 0 {
		target := replicatedstate.TransitionCaptureTarget{Name: replicatedstate.TransitionCaptureCollectionName, Collection: collection}
		capture, found, err := schemachange.RestoreSourceCapture(target)
		if err != nil || !found || !schemaCaptureMatchesRetiredLineage(capture.Configuration(), lineage) {
			return false, errors.Join(err, ErrReplicatedSchemaDDLConflict)
		}
		if shadowFound && capture.Configuration() != shadow.Capture {
			return false, ErrReplicatedSchemaDDLConflict
		}
		var key [8]byte
		header, found, err := collection.AppendRaw(nil, key[:])
		if err != nil || !found {
			return false, errors.Join(err, ErrReplicatedSchemaDDLConflict)
		}
		done, err := a.machine.ReclaimRetiredTransitionCapture(sha256.Sum256(header), capture.Configuration().SchemaGeneration, maxEntries)
		if err != nil || !done {
			return false, err
		}
	}
	a.schemaCapture = nil
	if shadowFound {
		if err := schemaDDLShadowFault("reclaim-journal"); err != nil {
			return false, err
		}
		if err := root.Remove(schemaDDLShadowName); err != nil && !os.IsNotExist(err) {
			return false, err
		}
		if err := schemaDDLShadowFault("reclaim-journal-removed"); err != nil {
			return false, err
		}
	}
	// Also fence a retry that observes a previous unsynced removal as absent.
	if err := syncSchemaNamespaceDirectory(root, "."); err != nil {
		return false, err
	}
	return true, nil
}
