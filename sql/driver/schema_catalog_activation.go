package driver

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	replicatedSchemaActivationName          = ".schema-activation"
	replicatedSchemaActivationTemp          = ".schema-activation.tmp"
	replicatedSchemaActivationHeaderBytes   = 48
	replicatedSchemaActivationV1HeaderBytes = 64
)

var replicatedSchemaActivationMagic = [8]byte{'V', 'D', 'B', 'S', 'A', 'C', 'T', 0}
var replicatedSchemaActivationChecksumDomain = []byte(
	"vibedb/sql/schema-activation/format-0\x00",
)
var replicatedSchemaCatalogCASDomain = []byte(
	"vibedb/sql/schema-catalog-cas/format-0\x00",
)

func replicatedSchemaCatalogCASDigest(
	currentDigest, targetDigest, requestDigest, authorizationDigest [sha256.Size]byte,
) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write(replicatedSchemaCatalogCASDomain)
	_, _ = h.Write(currentDigest[:])
	_, _ = h.Write(targetDigest[:])
	_, _ = h.Write(requestDigest[:])
	_, _ = h.Write(authorizationDigest[:])
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result
}

type replicatedSchemaActivation struct {
	targetDigest      [sha256.Size]byte
	preparedApplied   uint64
	preCommandApplied uint64
	command           []byte
}

func replicatedSchemaActivationCommittedApplied(
	record replicatedSchemaActivation, preparedApplied uint64,
) (uint64, error) {
	if preparedApplied == 0 || preparedApplied == ^uint64(0) {
		return 0, ErrReplicatedSchemaCatalogImage
	}
	if record.preparedApplied == 0 && record.preCommandApplied == 0 {
		return preparedApplied + 1, nil
	}
	if record.preparedApplied != preparedApplied ||
		record.preCommandApplied <= preparedApplied || record.preCommandApplied == ^uint64(0) {
		return 0, ErrReplicatedSchemaCatalogImage
	}
	return record.preCommandApplied + 1, nil
}

func replicatedSchemaActivationMatchesCatalog(catalogPath string) (bool, error) {
	if _, selected, err := selectedSchemaLineage(catalogPath); err != nil || selected {
		return false, err
	}
	raw, found, err := readCatalogFile(catalogPath)
	if err != nil || !found {
		return false, err
	}
	record, activationFound, err := readReplicatedSchemaActivation(catalogPath + ".tables")
	if err != nil || !activationFound {
		return false, err
	}
	image, err := ValidateReplicatedSchemaCatalogImage(raw)
	if err != nil {
		return false, err
	}
	return record.targetDigest == image.Digest, nil
}

// PublishedReplicatedSchemaActivationIdentity returns the exact SQL and apply
// identities authenticated by a catalog whose digest matches the durable
// activation record. It is the crash-atomic restart authority; external
// identity files may still describe the drained source generation.
func PublishedReplicatedSchemaActivationIdentity(
	path string,
) (ReplicatedShardStoreIdentity, ReplicatedApplyIdentity, bool, error) {
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return ReplicatedShardStoreIdentity{}, ReplicatedApplyIdentity{}, false, err
	}
	raw, found, err := readCatalogFile(absolute)
	if err != nil || !found {
		return ReplicatedShardStoreIdentity{}, ReplicatedApplyIdentity{}, false, err
	}
	// A cold snapshot target has a reserved child apply identity, not an active
	// schema image. Only an actual activation record authorizes interpreting
	// the catalog as a published schema transition.
	record, activationFound, err := readReplicatedSchemaActivation(absolute + ".tables")
	if err != nil || !activationFound {
		return ReplicatedShardStoreIdentity{}, ReplicatedApplyIdentity{}, false, err
	}
	catalog, image, err := openReplicatedSchemaCatalogImage(raw)
	if err != nil || record.targetDigest != image.Digest {
		return ReplicatedShardStoreIdentity{}, ReplicatedApplyIdentity{}, false, err
	}
	transition, err := replicatedstate.OpenSchemaTransition(record.command)
	if err != nil || transition.ToSchemaGeneration != image.SchemaGeneration ||
		transition.ToManifest != image.RelationManifestDigest ||
		catalog.ReplicatedShardStore == nil || catalog.ReplicatedApply == nil {
		return ReplicatedShardStoreIdentity{}, ReplicatedApplyIdentity{}, false,
			errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	if err := fencePublishedReplicatedSchemaCatalog(absolute); err != nil {
		return ReplicatedShardStoreIdentity{}, ReplicatedApplyIdentity{}, false, err
	}
	return catalog.ReplicatedShardStore.Clone(), catalog.ReplicatedApply.identity(), true, nil
}

func encodeReplicatedSchemaActivation(record replicatedSchemaActivation) ([]byte, error) {
	if record.targetDigest == ([sha256.Size]byte{}) || len(record.command) == 0 ||
		len(record.command) > replicatedstate.MaxSchemaTransitionBytes ||
		(record.preparedApplied == 0) != (record.preCommandApplied == 0) ||
		record.preCommandApplied != 0 && record.preCommandApplied < record.preparedApplied {
		return nil, ErrReplicatedSchemaCatalogImage
	}
	if _, err := replicatedstate.OpenSchemaTransition(record.command); err != nil {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	version, header := uint16(0), replicatedSchemaActivationHeaderBytes
	if record.preparedApplied != 0 {
		version, header = 1, replicatedSchemaActivationV1HeaderBytes
	}
	total := header + len(record.command) + sha256.Size
	raw := make([]byte, total)
	copy(raw[:8], replicatedSchemaActivationMagic[:])
	binary.LittleEndian.PutUint16(raw[8:10], version)
	binary.LittleEndian.PutUint16(raw[10:12], uint16(len(record.command)))
	binary.LittleEndian.PutUint32(raw[12:16], uint32(total))
	copy(raw[16:48], record.targetDigest[:])
	if version == 1 {
		binary.LittleEndian.PutUint64(raw[48:56], record.preparedApplied)
		binary.LittleEndian.PutUint64(raw[56:64], record.preCommandApplied)
	}
	copy(raw[header:], record.command)
	h := sha256.New()
	_, _ = h.Write(replicatedSchemaActivationChecksumDomain)
	_, _ = h.Write(raw[:total-sha256.Size])
	_ = h.Sum(raw[total-sha256.Size : total-sha256.Size])
	return raw, nil
}

func decodeReplicatedSchemaActivation(raw []byte) (replicatedSchemaActivation, error) {
	if len(raw) < replicatedSchemaActivationHeaderBytes+sha256.Size ||
		!bytes.Equal(raw[:8], replicatedSchemaActivationMagic[:]) ||
		binary.LittleEndian.Uint32(raw[12:16]) != uint32(len(raw)) {
		return replicatedSchemaActivation{}, ErrReplicatedSchemaCatalogImage
	}
	version := binary.LittleEndian.Uint16(raw[8:10])
	header := replicatedSchemaActivationHeaderBytes
	if version == 1 {
		header = replicatedSchemaActivationV1HeaderBytes
	} else if version != 0 {
		return replicatedSchemaActivation{}, ErrReplicatedSchemaCatalogImage
	}
	commandBytes := int(binary.LittleEndian.Uint16(raw[10:12]))
	if commandBytes == 0 || commandBytes > replicatedstate.MaxSchemaTransitionBytes ||
		header+commandBytes+sha256.Size != len(raw) {
		return replicatedSchemaActivation{}, ErrReplicatedSchemaCatalogImage
	}
	checksumAt := len(raw) - sha256.Size
	h := sha256.New()
	_, _ = h.Write(replicatedSchemaActivationChecksumDomain)
	_, _ = h.Write(raw[:checksumAt])
	var checksum [sha256.Size]byte
	_ = h.Sum(checksum[:0])
	if !bytes.Equal(checksum[:], raw[checksumAt:]) {
		return replicatedSchemaActivation{}, ErrReplicatedSchemaCatalogImage
	}
	record := replicatedSchemaActivation{command: make([]byte, commandBytes)}
	copy(record.targetDigest[:], raw[16:48])
	if version == 1 {
		record.preparedApplied = binary.LittleEndian.Uint64(raw[48:56])
		record.preCommandApplied = binary.LittleEndian.Uint64(raw[56:64])
	}
	copy(record.command, raw[header:checksumAt])
	canonical, err := encodeReplicatedSchemaActivation(record)
	if err != nil || !bytes.Equal(canonical, raw) {
		return replicatedSchemaActivation{}, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	record.command = record.command[:len(record.command):len(record.command)]
	return record, nil
}

func readReplicatedSchemaActivation(dataDir string) (replicatedSchemaActivation, bool, error) {
	root, err := os.OpenRoot(dataDir)
	if err != nil {
		return replicatedSchemaActivation{}, false, err
	}
	defer root.Close()
	file, err := root.Open(replicatedSchemaActivationName)
	if os.IsNotExist(err) {
		return replicatedSchemaActivation{}, false, nil
	}
	if err != nil {
		return replicatedSchemaActivation{}, false, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file,
		int64(replicatedSchemaActivationV1HeaderBytes+replicatedstate.MaxSchemaTransitionBytes+
			sha256.Size+1)))
	err = errors.Join(readErr, file.Close())
	if err != nil {
		return replicatedSchemaActivation{}, false, err
	}
	record, err := decodeReplicatedSchemaActivation(raw)
	return record, err == nil, err
}

func writeReplicatedSchemaActivation(dataDir string, record replicatedSchemaActivation) error {
	raw, err := encodeReplicatedSchemaActivation(record)
	if err != nil {
		return err
	}
	if existing, found, readErr := readReplicatedSchemaActivation(dataDir); readErr != nil {
		return readErr
	} else if found {
		existingRaw, encodeErr := encodeReplicatedSchemaActivation(existing)
		if encodeErr == nil && bytes.Equal(existingRaw, raw) {
			return fenceReplicatedSchemaFiles(dataDir, replicatedSchemaActivationName)
		}
		old, err := replicatedstate.OpenSchemaTransition(existing.command)
		if err != nil {
			return err
		}
		retired, err := schemaProofRetired(dataDir, existing.targetDigest, old.ToSchemaGeneration)
		next, openErr := replicatedstate.OpenSchemaTransition(record.command)
		if err != nil || openErr != nil || !retired || next.From.SchemaGeneration != old.ToSchemaGeneration {
			return errors.Join(err, openErr, ErrReplicatedSchemaCatalogImage)
		}
	}
	root, err := os.OpenRoot(dataDir)
	if err != nil {
		return err
	}
	defer root.Close()
	_ = root.Remove(replicatedSchemaActivationTemp)
	file, err := root.OpenFile(
		replicatedSchemaActivationTemp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600,
	)
	if err == nil {
		_, err = file.Write(raw)
	}
	if err == nil {
		err = file.Sync()
	}
	if file != nil {
		err = errors.Join(err, file.Close())
	}
	if err == nil {
		err = root.Rename(replicatedSchemaActivationTemp, replicatedSchemaActivationName)
	}
	if err == nil {
		err = syncReplicatedSchemaDirectory(dataDir)
	}
	return err
}

// ReplicatedSchemaCatalogCASDigest authenticates one exact canonical current
// catalog, prepared target, rollout request, and final authorization. It reads
// only the bounded catalog file and never scans relation data.
func (a *ReplicatedApply) ReplicatedSchemaCatalogCASDigest(
	proof ReplicatedSchemaTargetProof,
	requestDigest, authorizationDigest [sha256.Size]byte,
) ([sha256.Size]byte, error) {
	if a == nil || a.database == nil || requestDigest == ([sha256.Size]byte{}) ||
		authorizationDigest == ([sha256.Size]byte{}) {
		return [sha256.Size]byte{}, ErrReplicatedSchemaCatalogImage
	}
	a.database.mu.RLock()
	if err := a.checkLocked(); err != nil {
		a.database.mu.RUnlock()
		return [sha256.Size]byte{}, err
	}
	path := a.database.path
	a.database.mu.RUnlock()
	raw, found, err := readCatalogFile(path)
	if err != nil || !found {
		return [sha256.Size]byte{}, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	currentDigest := sha256.Sum256(raw)
	return replicatedSchemaCatalogCASDigest(
		currentDigest, proof.Catalog.Digest, requestDigest, authorizationDigest,
	), nil
}

// ObservePublishedReplicatedSchemaTransition returns the exact authenticated
// command only when its target catalog is the currently durable catalog. This
// settles activation after a process crash without trusting retained CLI
// identity files or controller memory.
func ObservePublishedReplicatedSchemaTransition(
	path string,
) (replicatedstate.SchemaTransitionView, bool, error) {
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return replicatedstate.SchemaTransitionView{}, false, err
	}
	record, found, err := readReplicatedSchemaActivation(absolute + ".tables")
	if err != nil || !found {
		return replicatedstate.SchemaTransitionView{}, found, err
	}
	transition, err := replicatedstate.OpenSchemaTransition(record.command)
	if err != nil {
		return replicatedstate.SchemaTransitionView{}, false, err
	}
	raw, found, err := readCatalogFile(absolute)
	if err != nil || !found {
		return replicatedstate.SchemaTransitionView{}, false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	image, err := ValidateReplicatedSchemaCatalogImage(raw)
	if err != nil {
		return replicatedstate.SchemaTransitionView{}, false, err
	}
	if image.SchemaGeneration == transition.From.SchemaGeneration &&
		image.RelationManifestDigest == transition.FromManifest &&
		replicatedSchemaCatalogCASDigest(image.Digest, record.targetDigest,
			transition.RequestDigest, transition.AuthorizationDigest) == transition.CatalogCASDigest {
		// Persist-before-proposal is a normal restart cut. Only the exact CAS
		// source is an unpublished rollout; an unrelated catalog remains an error.
		return replicatedstate.SchemaTransitionView{}, false, nil
	}
	if image.Digest != record.targetDigest ||
		image.SchemaGeneration != transition.ToSchemaGeneration ||
		image.RelationManifestDigest != transition.ToManifest {
		return replicatedstate.SchemaTransitionView{}, false,
			errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	if err := fencePublishedReplicatedSchemaCatalog(absolute); err != nil {
		return replicatedstate.SchemaTransitionView{}, false, err
	}
	return transition, true, nil
}

// ObservePersistedReplicatedSchemaTransition returns the exact prepared
// command, including at the crash cut after its Raft commit but before catalog
// publication. Recovery must observe/propose these original bytes instead of
// rebuilding a command against an already advanced source applied index.
func ObservePersistedReplicatedSchemaTransition(
	path string,
) (replicatedstate.SchemaTransitionView, bool, error) {
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return replicatedstate.SchemaTransitionView{}, false, err
	}
	record, found, err := readReplicatedSchemaActivation(absolute + ".tables")
	if err != nil || !found {
		return replicatedstate.SchemaTransitionView{}, found, err
	}
	transition, err := replicatedstate.OpenSchemaTransition(record.command)
	if err != nil {
		return replicatedstate.SchemaTransitionView{}, false, err
	}
	marker, found, err := readReplicatedSchemaStageMarker(absolute + ".tables")
	lineageSelected := false
	if err == nil && found && (marker.catalogDigest != record.targetDigest ||
		marker.schemaGeneration != transition.ToSchemaGeneration ||
		marker.authorization != transition.RequestDigest ||
		marker.applyContract != transition.ToApplyContract ||
		marker.placementDigest != transition.ToPlacementDigest) {
		// A completed N rollout may remain the selected restart authority while
		// N+1 has only staged its target/marker. The bounded lineage slot retains
		// N's exact marker and activation before the single staging slot is
		// replaced. Recover N from that selected proof; N+1 has no persisted
		// transition yet and cannot supersede it.
		lineage, selected, lineageErr := selectedSchemaLineage(absolute)
		if lineageErr != nil || !selected ||
			!bytes.Equal(lineage.activation.command, record.command) ||
			lineage.activation.targetDigest != record.targetDigest {
			return replicatedstate.SchemaTransitionView{}, false,
				errors.Join(err, lineageErr, ErrReplicatedSchemaCatalogImage)
		}
		marker, lineageSelected = lineage.marker, true
	}
	if err != nil || !found || marker.catalogDigest != record.targetDigest ||
		marker.schemaGeneration != transition.ToSchemaGeneration ||
		marker.authorization != transition.RequestDigest ||
		marker.applyContract != transition.ToApplyContract ||
		marker.placementDigest != transition.ToPlacementDigest {
		return replicatedstate.SchemaTransitionView{}, false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	var raw []byte
	if lineageSelected {
		raw, found, err = readCatalogFile(absolute)
		if err != nil || !found {
			return replicatedstate.SchemaTransitionView{}, false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
		}
	} else {
		root, openErr := os.OpenRoot(absolute + ".tables")
		if openErr != nil {
			return replicatedstate.SchemaTransitionView{}, false, openErr
		}
		file, openErr := root.Open(replicatedSchemaTargetCatalogName)
		if openErr != nil {
			return replicatedstate.SchemaTransitionView{}, false, errors.Join(openErr, root.Close())
		}
		raw, openErr = io.ReadAll(io.LimitReader(file, maxCatalogBytes+1))
		openErr = errors.Join(openErr, file.Close(), root.Close())
		if openErr != nil {
			return replicatedstate.SchemaTransitionView{}, false, openErr
		}
	}
	image, err := ValidateReplicatedSchemaCatalogImage(raw)
	if err != nil || image.Digest != record.targetDigest ||
		image.SchemaGeneration != transition.ToSchemaGeneration ||
		image.RelationManifestDigest != transition.ToManifest {
		return replicatedstate.SchemaTransitionView{}, false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	if err := fenceReplicatedSchemaFiles(absolute+".tables", replicatedSchemaActivationName,
		replicatedSchemaStageMarkerName, replicatedSchemaTargetCatalogName); err != nil {
		return replicatedstate.SchemaTransitionView{}, false, err
	}
	return transition, true, nil
}

// ObservePersistedReplicatedSchemaEmptySuffix returns the activation's exact
// prepared and pre-command indexes. Nonzero bounds are usable only after the
// RF3 owner verifies the corresponding WAL entries as empty normal entries.
func ObservePersistedReplicatedSchemaEmptySuffix(
	path string, command []byte,
) (preparedApplied, preCommandApplied uint64, found bool, err error) {
	transition, found, err := ObservePersistedReplicatedSchemaTransition(path)
	if err != nil || !found || !bytes.Equal(transition.Bytes(), command) {
		if err == nil && found {
			err = ErrReplicatedSchemaCatalogImage
		}
		return 0, 0, false, err
	}
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return 0, 0, false, err
	}
	record, found, err := readReplicatedSchemaActivation(absolute + ".tables")
	if err != nil || !found || !bytes.Equal(record.command, command) {
		return 0, 0, false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	marker, markerFound, err := readReplicatedSchemaStageMarker(absolute + ".tables")
	if err != nil || !markerFound {
		return 0, 0, false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	if record.preparedApplied == 0 && record.preCommandApplied == 0 {
		return 0, 0, false, nil
	}
	if record.preparedApplied != marker.sourceApplied ||
		record.preCommandApplied <= record.preparedApplied {
		return 0, 0, false, ErrReplicatedSchemaCatalogImage
	}
	return record.preparedApplied, record.preCommandApplied, true, nil
}

// ObservePersistedReplicatedSchemaCommittedApplied returns the sole Raft index
// at which the authenticated activation command must appear. The caller uses
// it only to bind the exact WAL command bytes supplied to cold recovery.
func ObservePersistedReplicatedSchemaCommittedApplied(path string, command []byte) (uint64, error) {
	transition, found, err := ObservePersistedReplicatedSchemaTransition(path)
	if err != nil || !found || !bytes.Equal(transition.Bytes(), command) {
		return 0, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return 0, err
	}
	record, found, err := readReplicatedSchemaActivation(absolute + ".tables")
	if err != nil || !found || !bytes.Equal(record.command, command) {
		return 0, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	marker, found, err := readReplicatedSchemaStageMarker(absolute + ".tables")
	if err != nil || !found {
		return 0, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	return replicatedSchemaActivationCommittedApplied(record, marker.sourceApplied)
}

// ObserveDrainedReplicatedSchemaSource reports whether every source relation
// protected by the exact activation certificate has been reclaimed. Hidden
// apply sidecars are intentionally outside this relation-generation drain.
func ObserveDrainedReplicatedSchemaSource(path string, command []byte) (bool, error) {
	return drainPublishedReplicatedSchemaSource(path, command, false)
}

// DrainPublishedReplicatedSchemaSource reclaims only old relation files after
// the caller has validated the catalog DrainProof and quiesced every old
// execution pin. The activation and current target catalog are reauthenticated
// locally before any removal; target storage identities can never be removed.
func DrainPublishedReplicatedSchemaSource(path string, command []byte) (bool, error) {
	return drainPublishedReplicatedSchemaSource(path, command, true)
}

func drainPublishedReplicatedSchemaSource(
	path string, command []byte, remove bool,
) (bool, error) {
	absolute, err := canonicalCatalogPath(path)
	if err != nil || len(command) == 0 || len(command) > replicatedstate.MaxSchemaTransitionBytes {
		return false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	if lineage, selected, err := selectedSchemaLineage(absolute); err != nil {
		return false, fmt.Errorf("observe selected schema lineage: %w", err)
	} else if selected && bytes.Equal(lineage.activation.command, command) {
		return true, nil
	}
	record, found, err := readReplicatedSchemaActivation(absolute + ".tables")
	if err != nil || !found || !bytes.Equal(record.command, command) {
		return false, fmt.Errorf("%w: drain activation found=%t command=%t: %v",
			ErrReplicatedSchemaCatalogImage, found, bytes.Equal(record.command, command), err)
	}
	if _, active, observeErr := ObservePublishedReplicatedSchemaTransition(path); observeErr != nil || !active {
		return false, fmt.Errorf("%w: drain published active=%t: %v", ErrReplicatedSchemaCatalogImage, active, observeErr)
	}
	marker, found, err := readReplicatedSchemaStageMarker(absolute + ".tables")
	if err != nil || !found || len(marker.sourceStorages) == 0 {
		return false, fmt.Errorf("%w: drain marker found=%t source-storages=%d: %v",
			ErrReplicatedSchemaCatalogImage, found, len(marker.sourceStorages), err)
	}
	committedApplied, err := replicatedSchemaActivationCommittedApplied(record, marker.sourceApplied)
	if err != nil {
		return false, fmt.Errorf("resolve drain committed applied: %w", err)
	}
	if err := durable.ValidateSelectedCheckpointMembershipTransition(
		absolute+".tables", marker.membership, marker.authorization,
		committedApplied, sha256.Sum256(record.command),
	); err != nil {
		return false, fmt.Errorf("validate selected drain checkpoint: %w", err)
	}
	targets := make(map[[32]byte]struct{}, len(marker.storages))
	for _, storage := range marker.storages {
		targets[storage] = struct{}{}
	}
	root, err := os.OpenRoot(filepath.Join(absolute+".tables", replicatedSchemaSourcesDirectory))
	if err != nil {
		return false, fmt.Errorf("open drain source directory: %w", err)
	}
	defer root.Close()
	for _, storage := range marker.sourceStorages {
		if _, target := targets[storage]; target {
			return false, fmt.Errorf("%w: drain source aliases target", ErrReplicatedSchemaCatalogImage)
		}
		base := hex.EncodeToString(storage[:]) + ".vjc"
		for _, name := range [...]string{base, base + ".rjournal"} {
			_, statErr := root.Stat(name)
			if os.IsNotExist(statErr) {
				continue
			}
			if statErr != nil {
				return false, fmt.Errorf("stat drain source %q: %w", name, statErr)
			}
			if !remove {
				return false, nil
			}
			if err = root.Remove(name); err != nil && !os.IsNotExist(err) {
				return false, fmt.Errorf("remove drain source %q: %w", name, err)
			}
		}
	}
	// Absence on a retry can be the readable result of an unfenced deletion.
	// Fence even when this attempt removed nothing before authorizing GC.
	if err = syncReplicatedSchemaDirectory(filepath.Join(absolute+".tables", replicatedSchemaSourcesDirectory)); err != nil {
		return false, fmt.Errorf("fence drained source directory: %w", err)
	}
	// Absence of old files is not yet completed drain authority: the prior
	// attempt may have failed before retaining the restart lineage. Observe
	// must force the authorized Drain caller to finish that publication.
	if !remove {
		return false, nil
	}
	if err := retainDrainedSchemaLineage(absolute, marker, record); err != nil {
		return false, fmt.Errorf("retain drained schema lineage: %w", err)
	}
	return true, nil
}

// PublishReplicatedSchemaCatalog performs the exact old->new catalog CAS only
// after the persisted machine proves the authorized schema command is its
// final durable Raft entry. Success deliberately leaves the old in-process
// bundle fenced; the runtime must close it and activate the target membership
// before serving resumes.
func (a *ReplicatedApply) PublishReplicatedSchemaCatalog() (published bool, err error) {
	if a == nil || a.database == nil {
		return false, ErrReplicatedApplyClosed
	}
	record, found, err := readReplicatedSchemaActivation(a.database.dataDir)
	if err != nil || !found {
		return false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	transition, err := replicatedstate.OpenSchemaTransition(record.command)
	if err != nil || record.targetDigest == ([32]byte{}) {
		return false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	marker, markerFound, err := readReplicatedSchemaStageMarker(a.database.dataDir)
	if err != nil || !markerFound || marker.catalogDigest != record.targetDigest ||
		marker.placementDigest != transition.ToPlacementDigest ||
		marker.sourceApplied == ^uint64(0) {
		return false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	if err := fenceReplicatedSchemaFiles(a.database.dataDir, replicatedSchemaActivationName,
		replicatedSchemaStageMarkerName, replicatedSchemaTargetCatalogName); err != nil {
		return false, err
	}
	root, err := os.OpenRoot(a.database.dataDir)
	if err != nil {
		return false, err
	}
	file, err := root.Open(replicatedSchemaTargetCatalogName)
	if err != nil {
		_ = root.Close()
		return false, err
	}
	targetRaw, err := io.ReadAll(io.LimitReader(file, maxCatalogBytes+1))
	err = errors.Join(err, file.Close(), root.Close())
	if err != nil {
		return false, err
	}
	targetImage, err := ValidateReplicatedSchemaCatalogImage(targetRaw)
	if err != nil || targetImage.Digest != record.targetDigest ||
		targetImage.SchemaGeneration != transition.ToSchemaGeneration ||
		targetImage.RelationManifestDigest != transition.ToManifest {
		return false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}

	d := a.database
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := a.checkLocked(); err != nil {
		return false, err
	}
	applied, committed, err := a.machine.ObserveSchemaTransition(record.command)
	wantApplied, boundErr := replicatedSchemaActivationCommittedApplied(record, marker.sourceApplied)
	emptySuffix := record.preparedApplied != 0
	if boundErr != nil {
		return false, boundErr
	}
	if err != nil || !committed || applied != wantApplied {
		return false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	if d.checkpointGroup == nil {
		return false, ErrReplicatedSchemaCatalogImage
	}
	commandDigest := sha256.Sum256(record.command)
	if emptySuffix {
		err = d.checkpointGroup.FinalizeMembershipTransitionAfterEmptySuffix(
			marker.membership, marker.authorization, record.preparedApplied, applied, commandDigest,
		)
	} else {
		err = d.checkpointGroup.FinalizeMembershipTransition(
			marker.membership, marker.authorization, applied, commandDigest,
		)
	}
	if err != nil {
		return false, err
	}
	bound, err := catalogSizeUpperBound(d.catalog)
	if err != nil {
		return false, err
	}
	currentMemory, err := appendCatalogJSON(make([]byte, 0, bound), d.catalog)
	if err != nil {
		return false, err
	}
	currentDisk, exists, err := readCatalogFile(d.path)
	if err != nil || !exists ||
		!bytes.Equal(currentMemory, currentDisk) && !bytes.Equal(targetRaw, currentDisk) {
		return false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	currentDigest := sha256.Sum256(currentMemory)
	wantCAS := replicatedSchemaCatalogCASDigest(
		currentDigest, targetImage.Digest, transition.RequestDigest,
		transition.AuthorizationDigest,
	)
	if wantCAS != transition.CatalogCASDigest {
		return false, ErrReplicatedSchemaCatalogImage
	}
	if bytes.Equal(targetRaw, currentDisk) {
		// A previous rename may be visible despite a failed directory fence.
		// Retain the source in-memory CAS witness and settle that exact rename.
		if err := d.directorySync(filepath.Dir(d.path)); err != nil {
			return true, errors.Join(durable.ErrCommitOutcomeUnknown, err)
		}
		return true, nil
	}
	tmp, err := os.CreateTemp(filepath.Dir(d.path), "."+filepath.Base(d.path)+".schema-*")
	if err != nil {
		return false, err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		_ = tmp.Close()
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if err = tmp.Chmod(0o600); err == nil {
		_, err = tmp.Write(targetRaw)
	}
	if err == nil {
		err = tmp.Sync()
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return false, err
	}
	if err = replaceCatalogFile(tmpName, d.path); err != nil {
		return false, err
	}
	cleanup = false
	if err = d.directorySync(filepath.Dir(d.path)); err != nil {
		return true, errors.Join(durable.ErrCommitOutcomeUnknown, err)
	}
	return true, nil
}

// OpenReplicatedShardStoreWithSchemaTransition is the sole target-generation
// opener after catalog publication. It recovers the exact checkpoint
// membership and authenticates the persisted Raft transition while opening;
// ordinary replicated open continues to reject the unselected target slot.
func OpenReplicatedShardStoreWithSchemaTransition(
	path string,
	expected ReplicatedShardStoreIdentity,
	expectedApply ReplicatedApplyIdentity,
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
	raw, found, err := readCatalogFile(absolute)
	if err != nil || !found {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	image, err := ValidateReplicatedSchemaCatalogImage(raw)
	if err != nil || image.SchemaGeneration != expected.RelationSchemaGeneration ||
		image.Digest == ([32]byte{}) {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	dataDir := absolute + ".tables"
	record, activationFound, err := readReplicatedSchemaActivation(dataDir)
	if err != nil || !activationFound || record.targetDigest != image.Digest {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	transition, err := replicatedstate.OpenSchemaTransition(record.command)
	if err != nil || transition.ToSchemaGeneration != image.SchemaGeneration ||
		transition.ToManifest != image.RelationManifestDigest ||
		transition.ToApplyContract == ([32]byte{}) {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	machineCommand := record.command
	machineTransition := transition
	if openOptions.SchemaCommittedTransition != "" {
		committed, committedErr := replicatedstate.OpenSchemaTransition([]byte(openOptions.SchemaCommittedTransition))
		if committedErr != nil || !replicatedSchemaTransitionEqualExceptCatalogCAS(transition, committed) {
			return nil, errors.Join(committedErr, ErrReplicatedSchemaCatalogImage)
		}
		machineCommand = []byte(openOptions.SchemaCommittedTransition)
		machineTransition = committed
	}
	marker, markerFound, err := readReplicatedSchemaStageMarker(dataDir)
	if err != nil || !markerFound || marker.catalogDigest != image.Digest ||
		marker.schemaGeneration != image.SchemaGeneration ||
		marker.authorization != transition.RequestDigest ||
		marker.applyContract != transition.ToApplyContract || marker.placementDigest != transition.ToPlacementDigest {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	selected, _, err := openReplicatedSchemaCatalogImage(raw)
	if err != nil || selected.ReplicatedShardStore == nil || selected.ReplicatedApply == nil ||
		!selected.ReplicatedShardStore.Equal(expected) || selected.ReplicatedApply.identity() != expectedApply {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	committedApplied, err := replicatedSchemaActivationCommittedApplied(record, marker.sourceApplied)
	if err != nil {
		return nil, err
	}
	if err := fencePublishedReplicatedSchemaCatalog(absolute); err != nil {
		return nil, err
	}
	if err := durable.ValidateFinalizedCheckpointMembershipTransition(
		dataDir, marker.membership, marker.authorization, committedApplied, sha256.Sum256(record.command),
	); err != nil {
		return nil, err
	}
	if err := activateReplicatedSchemaNamespace(dataDir, marker, expected, openOptions); err != nil {
		return nil, err
	}
	core, err := openDatabaseWithShardStorePolicy(path, nil, shardStoreOpenPolicy{
		mode:                      shardStoreOpenReplicatedSchemaTransition,
		openOptions:               openOptions,
		expectedReplicated:        ownedReplicatedShardStoreIdentity(expected),
		expectedReplicatedApply:   expectedApply,
		schemaTransition:          machineCommand,
		schemaMembership:          marker.membership,
		schemaCheckpointAuthority: transition.RequestDigest,
		schemaAuthorization:       machineTransition.AuthorizationDigest,
		schemaCatalogCAS:          machineTransition.CatalogCASDigest,
	})
	if err != nil {
		return nil, err
	}
	return &Database{connector: &dbConnector{db: core}}, nil
}

func replicatedSchemaTransitionEqualExceptCatalogCAS(
	a, b replicatedstate.SchemaTransitionView,
) bool {
	left, right := a.SchemaTransition, b.SchemaTransition
	left.CatalogCASDigest, right.CatalogCASDigest = [sha256.Size]byte{}, [sha256.Size]byte{}
	return left == right
}

// PersistReplicatedSchemaTransition records the exact authorized command
// before proposal. A committed transition can therefore be recovered without
// consulting an external controller or reconstructing semantically equivalent
// bytes.
func (a *ReplicatedApply) PersistReplicatedSchemaTransition(command []byte) error {
	return a.persistReplicatedSchemaTransition(command, 0, 0)
}

// PersistReplicatedSchemaTransitionAfterEmptySuffix records the exact source
// cut and the last independently verified empty Raft entry preceding command.
// The caller must have read and rejected every non-empty or non-normal entry in
// this closed interval. Publication rechecks these bounds before allowing the
// prepared membership's one semantic transition.
func (a *ReplicatedApply) PersistReplicatedSchemaTransitionAfterEmptySuffix(
	command []byte, preparedApplied, preCommandApplied uint64,
) error {
	if preparedApplied == 0 || preCommandApplied <= preparedApplied {
		return ErrReplicatedSchemaCatalogImage
	}
	return a.persistReplicatedSchemaTransition(command, preparedApplied, preCommandApplied)
}

func (a *ReplicatedApply) persistReplicatedSchemaTransition(
	command []byte, preparedApplied, preCommandApplied uint64,
) error {
	if a == nil || a.database == nil {
		return ErrReplicatedApplyClosed
	}
	transition, err := replicatedstate.OpenSchemaTransition(command)
	if err != nil {
		return fmt.Errorf("persist schema transition decode: %w", err)
	}
	marker, found, err := readReplicatedSchemaStageMarker(a.database.dataDir)
	if err != nil || !found || marker.schemaGeneration != transition.ToSchemaGeneration ||
		marker.catalogDigest == ([32]byte{}) || marker.applyContract != transition.ToApplyContract ||
		marker.placementDigest != transition.ToPlacementDigest ||
		marker.authorization != transition.RequestDigest {
		return fmt.Errorf("persist schema transition stage marker found=%t generation=%t catalog=%t contract=%t placement=%t request=%t: %w",
			found, marker.schemaGeneration == transition.ToSchemaGeneration,
			marker.catalogDigest != ([32]byte{}), marker.applyContract == transition.ToApplyContract,
			marker.placementDigest == transition.ToPlacementDigest,
			marker.authorization == transition.RequestDigest,
			errors.Join(err, ErrReplicatedSchemaCatalogImage))
	}
	root, err := os.OpenRoot(a.database.dataDir)
	if err != nil {
		return fmt.Errorf("persist schema transition open target: %w", err)
	}
	file, err := root.Open(replicatedSchemaTargetCatalogName)
	if err != nil {
		_ = root.Close()
		return err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maxCatalogBytes+1))
	err = errors.Join(readErr, file.Close(), root.Close())
	if err != nil {
		return fmt.Errorf("persist schema transition read target: %w", err)
	}
	image, err := ValidateReplicatedSchemaCatalogImage(raw)
	if err != nil || image.Digest != marker.catalogDigest ||
		image.SchemaGeneration != marker.schemaGeneration ||
		image.RelationManifestDigest != transition.ToManifest {
		return fmt.Errorf("persist schema transition target identity: %w", errors.Join(err, ErrReplicatedSchemaCatalogImage))
	}
	if err := fenceReplicatedSchemaFiles(a.database.dataDir,
		replicatedSchemaStageMarkerName, replicatedSchemaTargetCatalogName); err != nil {
		return fmt.Errorf("persist schema transition fence target: %w", err)
	}
	err = writeReplicatedSchemaActivation(a.database.dataDir, replicatedSchemaActivation{
		targetDigest: marker.catalogDigest, preparedApplied: preparedApplied,
		preCommandApplied: preCommandApplied, command: command,
	})
	if err != nil {
		return fmt.Errorf("persist schema transition activation record: %w", err)
	}
	return nil
}
