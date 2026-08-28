package driver

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

const (
	replicatedSchemaOriginName      = ".schema-origin"
	replicatedSchemaLineageName     = ".schema-lineage"
	replicatedSchemaLineageMaxBytes = maxCatalogBytes + (1 << 20)
)

// The bounded lineage slot replaces only a fully selected, drained rollout.
// Its origin remains immutable. It is a local durable authority, like the
// activation record, not a signature or permission to bypass the catalog gate.
type replicatedSchemaLineage struct {
	origin     [32]byte
	catalog    []byte
	marker     replicatedSchemaStageMarker
	activation replicatedSchemaActivation
}

func readSchemaLineageFile(directory, name string, maximum int64) ([]byte, bool, error) {
	root, err := os.OpenRoot(directory)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer root.Close()
	info, err := root.Lstat(name)
	if os.IsNotExist(err) {
		return nil, false, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum {
		return nil, false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	file, err := root.Open(name)
	if err != nil {
		return nil, false, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, false, errors.Join(err, file.Close(), ErrReplicatedSchemaCatalogImage)
	}
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	err = errors.Join(err, file.Close())
	if len(raw) > int(maximum) {
		err = errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	return raw, err == nil, err
}

// Callers serialize through the SQL catalog/shard lifecycle lock and validate
// the exact prior value before replacement. Rename then directory sync is the
// sole publication boundary; a readable retry must fence the name again.
func writeSchemaLineageFile(directory, name string, raw []byte) (err error) {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	temp := name + ".tmp"
	if err := root.Remove(temp); err != nil && !os.IsNotExist(err) {
		return err
	}
	file, err := root.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = file.Write(raw)
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err == nil {
		err = root.Rename(temp, name)
	}
	if err == nil {
		err = syncReplicatedSchemaDirectory(directory)
	}
	return err
}

func encodeReplicatedSchemaLineage(record replicatedSchemaLineage) ([]byte, error) {
	image, err := ValidateReplicatedSchemaCatalogImage(record.catalog)
	if err != nil || record.origin == ([32]byte{}) || record.marker.catalogDigest != image.Digest || record.activation.targetDigest != image.Digest {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	transition, err := replicatedstate.OpenSchemaTransition(record.activation.command)
	m := record.marker
	if err != nil || image.SchemaGeneration != transition.ToSchemaGeneration || image.RelationManifestDigest != transition.ToManifest ||
		m.schemaGeneration != image.SchemaGeneration || m.membership.Sequence != transition.MembershipSequence ||
		m.membership.Source != transition.MembershipSource || m.membership.Target != transition.MembershipTarget ||
		m.authorization != transition.RequestDigest || m.applyContract != transition.ToApplyContract || m.placementDigest != transition.ToPlacementDigest {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	marker, err := encodeReplicatedSchemaStageMarker(m)
	if err != nil {
		return nil, err
	}
	activation, err := encodeReplicatedSchemaActivation(record.activation)
	if err != nil {
		return nil, err
	}
	total := 56 + len(record.catalog) + len(marker) + len(activation) + sha256.Size
	if total > replicatedSchemaLineageMaxBytes {
		return nil, ErrReplicatedSchemaCatalogImage
	}
	raw := make([]byte, total)
	copy(raw, "VDBSLIN1")
	copy(raw[8:40], record.origin[:])
	binary.LittleEndian.PutUint32(raw[40:44], uint32(len(record.catalog)))
	binary.LittleEndian.PutUint32(raw[44:48], uint32(len(marker)))
	binary.LittleEndian.PutUint32(raw[48:52], uint32(len(activation)))
	at := 56
	at += copy(raw[at:], record.catalog)
	at += copy(raw[at:], marker)
	at += copy(raw[at:], activation)
	digest := sha256.Sum256(raw[:at])
	copy(raw[at:], digest[:])
	return raw, nil
}

func readReplicatedSchemaLineage(directory string) (replicatedSchemaLineage, bool, error) {
	var record replicatedSchemaLineage
	raw, found, err := readSchemaLineageFile(directory, replicatedSchemaLineageName, replicatedSchemaLineageMaxBytes)
	if err != nil || !found {
		return record, false, err
	}
	if len(raw) < 88 || string(raw[:8]) != "VDBSLIN1" || binary.LittleEndian.Uint32(raw[52:56]) != 0 {
		return record, false, ErrReplicatedSchemaCatalogImage
	}
	catalogBytes, markerBytes, activationBytes := uint64(binary.LittleEndian.Uint32(raw[40:44])), uint64(binary.LittleEndian.Uint32(raw[44:48])), uint64(binary.LittleEndian.Uint32(raw[48:52]))
	if 88+catalogBytes+markerBytes+activationBytes != uint64(len(raw)) {
		return record, false, ErrReplicatedSchemaCatalogImage
	}
	digest := sha256.Sum256(raw[:len(raw)-sha256.Size])
	if !bytes.Equal(digest[:], raw[len(raw)-sha256.Size:]) {
		return record, false, ErrReplicatedSchemaCatalogImage
	}
	copy(record.origin[:], raw[8:40])
	at := uint64(56)
	record.catalog = raw[at : at+catalogBytes : at+catalogBytes]
	at += catalogBytes
	record.marker, err = decodeReplicatedSchemaStageMarker(raw[at : at+markerBytes])
	if err != nil {
		return record, false, err
	}
	at += markerBytes
	record.activation, err = decodeReplicatedSchemaActivation(raw[at : at+activationBytes])
	if err != nil {
		return record, false, err
	}
	canonical, err := encodeReplicatedSchemaLineage(record)
	if err != nil || !bytes.Equal(canonical, raw) {
		return record, false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	origin, found, err := readSchemaLineageFile(directory, replicatedSchemaOriginName, maxCatalogBytes)
	if err != nil || !found || sha256.Sum256(origin) != record.origin {
		return record, false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	if _, err := ValidateReplicatedSchemaCatalogImage(origin); err != nil {
		return record, false, err
	}
	return record, true, nil
}

// ensureReplicatedSchemaOrigin runs under the bound source's catalog lock,
// before preparing membership. It never adopts an unrelated current catalog.
func ensureReplicatedSchemaOrigin(path string) error {
	raw, found, err := readCatalogFile(path)
	if err != nil || !found {
		return errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	if _, err := ValidateReplicatedSchemaCatalogImage(raw); err != nil {
		return err
	}
	directory := path + ".tables"
	origin, found, err := readSchemaLineageFile(directory, replicatedSchemaOriginName, maxCatalogBytes)
	if err != nil {
		return err
	}
	if !found {
		if _, exists, err := readSchemaLineageFile(directory, replicatedSchemaLineageName, replicatedSchemaLineageMaxBytes); err != nil || exists {
			return errors.Join(err, ErrReplicatedSchemaCatalogImage)
		}
		return writeSchemaLineageFile(directory, replicatedSchemaOriginName, raw)
	}
	if bytes.Equal(origin, raw) {
		return fenceReplicatedSchemaFiles(directory, replicatedSchemaOriginName)
	}
	lineage, found, err := readReplicatedSchemaLineage(directory)
	if err != nil || !found || !bytes.Equal(lineage.catalog, raw) {
		return errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	return fenceReplicatedSchemaFiles(directory, replicatedSchemaOriginName, replicatedSchemaLineageName)
}

// A proof slot can be replaced only after its exact target was durably selected
// and drained. Generation numbers alone never grant overwrite permission.
func schemaProofRetired(directory string, digest [32]byte, generation uint64) (bool, error) {
	if !strings.HasSuffix(directory, ".tables") {
		return false, ErrReplicatedSchemaCatalogImage
	}
	lineage, found, err := selectedSchemaLineage(strings.TrimSuffix(directory, ".tables"))
	if err != nil || !found {
		return false, err
	}
	if lineage.marker.catalogDigest != digest || lineage.marker.schemaGeneration != generation {
		return false, nil
	}
	return true, nil
}

func retainDrainedSchemaLineage(path string, marker replicatedSchemaStageMarker, activation replicatedSchemaActivation) error {
	raw, found, err := readCatalogFile(path)
	if err != nil || !found {
		return errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	directory := path + ".tables"
	origin, found, err := readSchemaLineageFile(directory, replicatedSchemaOriginName, maxCatalogBytes)
	if err != nil || !found {
		return errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	prior, exists, err := readReplicatedSchemaLineage(directory)
	if err != nil {
		return err
	}
	transition, err := replicatedstate.OpenSchemaTransition(activation.command)
	if err != nil {
		return err
	}
	source := origin
	if exists {
		if bytes.Equal(prior.catalog, raw) && bytes.Equal(prior.activation.command, activation.command) {
			return fenceReplicatedSchemaFiles(directory, replicatedSchemaOriginName, replicatedSchemaLineageName)
		}
		source = prior.catalog
	}
	sourceImage, err := ValidateReplicatedSchemaCatalogImage(source)
	if err != nil || sourceImage.SchemaGeneration != transition.From.SchemaGeneration || sourceImage.RelationManifestDigest != transition.FromManifest ||
		replicatedSchemaCatalogCASDigest(sourceImage.Digest, activation.targetDigest, transition.RequestDigest, transition.AuthorizationDigest) != transition.CatalogCASDigest {
		return errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	encoded, err := encodeReplicatedSchemaLineage(replicatedSchemaLineage{origin: sha256.Sum256(origin), catalog: raw, marker: marker, activation: activation})
	if err != nil {
		return err
	}
	return writeSchemaLineageFile(directory, replicatedSchemaLineageName, encoded)
}

// RetainedReplicatedSchemaLineageIdentity advances an external immutable
// startup anchor only through the locally selected, drained lineage slot.
// Generic successor validation stays limited to exactly one generation.
func RetainedReplicatedSchemaLineageIdentity(path string, retained ReplicatedShardStoreIdentity, retainedApply ReplicatedApplyIdentity) (ReplicatedShardStoreIdentity, ReplicatedApplyIdentity, bool, error) {
	absolute, err := canonicalCatalogPath(path)
	if err != nil {
		return retained, retainedApply, false, err
	}
	directory := absolute + ".tables"
	lineage, found, err := readReplicatedSchemaLineage(directory)
	if err != nil || !found {
		return retained, retainedApply, false, err
	}
	origin, _, err := readSchemaLineageFile(directory, replicatedSchemaOriginName, maxCatalogBytes)
	if err != nil {
		return retained, retainedApply, false, err
	}
	first, _, err := openReplicatedSchemaCatalogImage(origin)
	if err != nil || !first.ReplicatedShardStore.Equal(retained) || first.ReplicatedApply.identity() != retainedApply {
		return retained, retainedApply, false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	selected, _, err := openReplicatedSchemaCatalogImage(lineage.catalog)
	if err != nil {
		return retained, retainedApply, false, err
	}
	if err := fenceReplicatedSchemaFiles(directory, replicatedSchemaOriginName, replicatedSchemaLineageName); err != nil {
		return retained, retainedApply, false, err
	}
	return selected.ReplicatedShardStore.Clone(), selected.ReplicatedApply.identity(), true, nil
}

func selectedSchemaLineage(path string) (replicatedSchemaLineage, bool, error) {
	lineage, found, err := readReplicatedSchemaLineage(path + ".tables")
	if err != nil || !found {
		return lineage, false, err
	}
	raw, found, err := readCatalogFile(path)
	if err != nil || !found {
		return lineage, false, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	if !bytes.Equal(lineage.catalog, raw) {
		return lineage, false, nil
	}
	if err := fenceReplicatedSchemaFiles(path+".tables", replicatedSchemaOriginName, replicatedSchemaLineageName); err != nil {
		return lineage, false, err
	}
	return lineage, true, fenceReplicatedSchemaFiles(filepath.Dir(path), filepath.Base(path))
}
