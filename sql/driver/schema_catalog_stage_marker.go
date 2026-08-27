package driver

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/store/durable"
)

const (
	replicatedSchemaStageMarkerName    = ".schema-membership-stage"
	replicatedSchemaStageMarkerTemp    = ".schema-membership-stage.tmp"
	replicatedSchemaTargetCatalogName  = ".schema-target-catalog"
	replicatedSchemaTargetCatalogTemp  = ".schema-target-catalog.tmp"
	replicatedSchemaStageHeaderBytes   = 304
	replicatedSchemaStageChecksumBytes = sha256.Size
)

var replicatedSchemaStageMagic = [8]byte{'V', 'D', 'B', 'S', 'T', 'G', 0, 0}
var replicatedSchemaStageChecksumDomain = []byte("vibedb/sql/schema-stage-marker/format-0\x00")

type replicatedSchemaStageMarker struct {
	schemaGeneration uint64
	sourceApplied    uint64
	membership       durable.CheckpointMembershipWitness
	catalogDigest    [sha256.Size]byte
	relationWitness  [sha256.Size]byte
	placementDigest  [sha256.Size]byte
	applyContract    [sha256.Size]byte
	authorization    [sha256.Size]byte
	targetWitness    [sha256.Size]byte
	storages         [][32]byte
	sourceStorages   [][32]byte
}

func encodeReplicatedSchemaStageMarker(marker replicatedSchemaStageMarker) ([]byte, error) {
	if marker.schemaGeneration == 0 || marker.sourceApplied == 0 ||
		marker.membership.Sequence == 0 || marker.membership.Source == ([32]byte{}) ||
		marker.membership.Target == ([32]byte{}) || marker.catalogDigest == ([32]byte{}) ||
		marker.relationWitness == ([32]byte{}) || marker.applyContract == ([32]byte{}) ||
		marker.authorization == ([32]byte{}) || marker.targetWitness == ([32]byte{}) ||
		len(marker.storages) == 0 || len(marker.storages) > replication.MaxRelationsPerBundle ||
		len(marker.sourceStorages) == 0 || len(marker.sourceStorages) > replication.MaxRelationsPerBundle {
		return nil, ErrReplicatedSchemaCatalogImage
	}
	total := replicatedSchemaStageHeaderBytes + 32*(len(marker.storages)+len(marker.sourceStorages)) +
		replicatedSchemaStageChecksumBytes
	raw := make([]byte, total)
	copy(raw[0:8], replicatedSchemaStageMagic[:])
	binary.LittleEndian.PutUint16(raw[8:10], 0)
	binary.LittleEndian.PutUint16(raw[10:12], uint16(len(marker.storages)))
	binary.LittleEndian.PutUint32(raw[12:16], uint32(total))
	binary.LittleEndian.PutUint64(raw[16:24], marker.schemaGeneration)
	binary.LittleEndian.PutUint64(raw[24:32], marker.sourceApplied)
	binary.LittleEndian.PutUint64(raw[32:40], marker.membership.Sequence)
	copy(raw[40:72], marker.catalogDigest[:])
	copy(raw[72:104], marker.relationWitness[:])
	copy(raw[104:136], marker.authorization[:])
	copy(raw[136:168], marker.applyContract[:])
	copy(raw[168:200], marker.membership.Source[:])
	copy(raw[200:232], marker.membership.Target[:])
	copy(raw[232:264], marker.targetWitness[:])
	binary.LittleEndian.PutUint16(raw[264:266], uint16(len(marker.sourceStorages)))
	copy(raw[272:304], marker.placementDigest[:])
	at := replicatedSchemaStageHeaderBytes
	for i := range marker.storages {
		if marker.storages[i] == ([32]byte{}) {
			return nil, ErrReplicatedSchemaCatalogImage
		}
		copy(raw[at:at+32], marker.storages[i][:])
		at += 32
	}
	for i := range marker.sourceStorages {
		if marker.sourceStorages[i] == ([32]byte{}) {
			return nil, ErrReplicatedSchemaCatalogImage
		}
		copy(raw[at:at+32], marker.sourceStorages[i][:])
		at += 32
	}
	h := sha256.New()
	_, _ = h.Write(replicatedSchemaStageChecksumDomain)
	_, _ = h.Write(raw[:at])
	_ = h.Sum(raw[at:at])
	return raw, nil
}

func decodeReplicatedSchemaStageMarker(raw []byte) (replicatedSchemaStageMarker, error) {
	if len(raw) < replicatedSchemaStageHeaderBytes+replicatedSchemaStageChecksumBytes ||
		!bytes.Equal(raw[:8], replicatedSchemaStageMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != 0 ||
		binary.LittleEndian.Uint32(raw[12:16]) != uint32(len(raw)) {
		return replicatedSchemaStageMarker{}, ErrReplicatedSchemaCatalogImage
	}
	count := int(binary.LittleEndian.Uint16(raw[10:12]))
	sourceCount := int(binary.LittleEndian.Uint16(raw[264:266]))
	if count == 0 || count > replication.MaxRelationsPerBundle || sourceCount == 0 ||
		sourceCount > replication.MaxRelationsPerBundle ||
		binary.LittleEndian.Uint16(raw[266:268]) != 0 ||
		binary.LittleEndian.Uint32(raw[268:272]) != 0 ||
		replicatedSchemaStageHeaderBytes+32*(count+sourceCount)+replicatedSchemaStageChecksumBytes != len(raw) {
		return replicatedSchemaStageMarker{}, ErrReplicatedSchemaCatalogImage
	}
	checksumAt := len(raw) - replicatedSchemaStageChecksumBytes
	h := sha256.New()
	_, _ = h.Write(replicatedSchemaStageChecksumDomain)
	_, _ = h.Write(raw[:checksumAt])
	var checksum [sha256.Size]byte
	_ = h.Sum(checksum[:0])
	if !bytes.Equal(checksum[:], raw[checksumAt:]) {
		return replicatedSchemaStageMarker{}, ErrReplicatedSchemaCatalogImage
	}
	marker := replicatedSchemaStageMarker{
		schemaGeneration: binary.LittleEndian.Uint64(raw[16:24]),
		sourceApplied:    binary.LittleEndian.Uint64(raw[24:32]),
		membership: durable.CheckpointMembershipWitness{
			Sequence: binary.LittleEndian.Uint64(raw[32:40]),
		},
		storages: make([][32]byte, count), sourceStorages: make([][32]byte, sourceCount),
	}
	copy(marker.catalogDigest[:], raw[40:72])
	copy(marker.relationWitness[:], raw[72:104])
	copy(marker.authorization[:], raw[104:136])
	copy(marker.applyContract[:], raw[136:168])
	copy(marker.membership.Source[:], raw[168:200])
	copy(marker.membership.Target[:], raw[200:232])
	copy(marker.targetWitness[:], raw[232:264])
	copy(marker.placementDigest[:], raw[272:304])
	at := replicatedSchemaStageHeaderBytes
	for i := range marker.storages {
		copy(marker.storages[i][:], raw[at:at+32])
		at += 32
	}
	for i := range marker.sourceStorages {
		copy(marker.sourceStorages[i][:], raw[at:at+32])
		at += 32
	}
	canonical, err := encodeReplicatedSchemaStageMarker(marker)
	if err != nil || !bytes.Equal(canonical, raw) {
		return replicatedSchemaStageMarker{}, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	return marker, nil
}

func readReplicatedSchemaStageMarker(dataDir string) (replicatedSchemaStageMarker, bool, error) {
	root, err := os.OpenRoot(dataDir)
	if os.IsNotExist(err) {
		return replicatedSchemaStageMarker{}, false, nil
	}
	if err != nil {
		return replicatedSchemaStageMarker{}, false, err
	}
	defer root.Close()
	file, err := root.Open(replicatedSchemaStageMarkerName)
	if os.IsNotExist(err) {
		if removeErr := root.Remove(replicatedSchemaStageMarkerTemp); removeErr != nil &&
			!os.IsNotExist(removeErr) {
			return replicatedSchemaStageMarker{}, false, removeErr
		} else if removeErr == nil {
			if syncErr := syncDirectory(dataDir); syncErr != nil {
				return replicatedSchemaStageMarker{}, false, syncErr
			}
		}
		return replicatedSchemaStageMarker{}, false, nil
	}
	if err != nil {
		return replicatedSchemaStageMarker{}, false, err
	}
	raw, err := io.ReadAll(io.LimitReader(file, int64(replicatedSchemaStageHeaderBytes+
		64*replication.MaxRelationsPerBundle+replicatedSchemaStageChecksumBytes+1)))
	err = errors.Join(err, file.Close())
	if err != nil {
		return replicatedSchemaStageMarker{}, false, err
	}
	marker, err := decodeReplicatedSchemaStageMarker(raw)
	return marker, err == nil, err
}

func addReplicatedSchemaStageProtection(
	dataDir string,
	protected map[string]string,
) error {
	marker, found, err := readReplicatedSchemaStageMarker(dataDir)
	if err != nil || !found {
		return err
	}
	for i := range marker.storages {
		name := hex.EncodeToString(marker.storages[i][:]) + ".vjc"
		protected[filepath.Join(dataDir, replicatedSchemaTargetsDirectory, name)] = "prepared schema target"
	}
	for i := range marker.sourceStorages {
		name := hex.EncodeToString(marker.sourceStorages[i][:]) + ".vjc"
		protected[filepath.Join(dataDir, replicatedSchemaSourcesDirectory, name)] = "draining schema source"
	}
	return nil
}

func writeReplicatedSchemaStageMarker(dataDir string, marker replicatedSchemaStageMarker) error {
	raw, err := encodeReplicatedSchemaStageMarker(marker)
	if err != nil {
		return err
	}
	if existing, found, readErr := readReplicatedSchemaStageMarker(dataDir); readErr != nil {
		return readErr
	} else if found {
		existingRaw, encodeErr := encodeReplicatedSchemaStageMarker(existing)
		if encodeErr == nil && bytes.Equal(existingRaw, raw) {
			return fenceReplicatedSchemaFiles(dataDir, replicatedSchemaStageMarkerName)
		}
		return ErrReplicatedSchemaCatalogImage
	}
	root, err := os.OpenRoot(dataDir)
	if err != nil {
		return err
	}
	defer root.Close()
	_ = root.Remove(replicatedSchemaStageMarkerTemp)
	file, err := root.OpenFile(
		replicatedSchemaStageMarkerTemp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600,
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
		err = root.Rename(replicatedSchemaStageMarkerTemp, replicatedSchemaStageMarkerName)
	}
	if err == nil {
		err = syncReplicatedSchemaDirectory(dataDir)
	}
	return err
}

func readReplicatedSchemaTargetCatalog(
	dataDir string,
	expected ReplicatedSchemaCatalogImage,
) ([]byte, error) {
	root, err := os.OpenRoot(dataDir)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	file, err := root.Open(replicatedSchemaTargetCatalogName)
	if err != nil {
		return nil, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maxCatalogBytes+1))
	err = errors.Join(readErr, file.Close())
	if err != nil {
		return nil, err
	}
	image, err := ValidateReplicatedSchemaCatalogImage(raw)
	if err != nil || image != expected {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	return raw[:len(raw):len(raw)], nil
}

// writeReplicatedSchemaTargetCatalog retains the exact canonical target bytes
// beside the membership marker before any Raft proposal. Recovery therefore
// never depends on an external installer artifact after the old bundle has
// been durably fenced. Exact retries are idempotent; substitution fails closed.
func writeReplicatedSchemaTargetCatalog(
	dataDir string,
	raw []byte,
	expected ReplicatedSchemaCatalogImage,
) error {
	image, err := ValidateReplicatedSchemaCatalogImage(raw)
	if err != nil || image != expected {
		return errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	if existing, readErr := readReplicatedSchemaTargetCatalog(dataDir, expected); readErr == nil {
		if bytes.Equal(existing, raw) {
			return fenceReplicatedSchemaFiles(dataDir, replicatedSchemaTargetCatalogName)
		}
		return ErrReplicatedSchemaCatalogImage
	} else if !os.IsNotExist(readErr) {
		return readErr
	}
	root, err := os.OpenRoot(dataDir)
	if err != nil {
		return err
	}
	defer root.Close()
	_ = root.Remove(replicatedSchemaTargetCatalogTemp)
	file, err := root.OpenFile(
		replicatedSchemaTargetCatalogTemp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600,
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
		err = root.Rename(replicatedSchemaTargetCatalogTemp, replicatedSchemaTargetCatalogName)
	}
	if err == nil {
		err = syncReplicatedSchemaDirectory(dataDir)
	}
	return err
}

func schemaStageStorageIDs(identity ReplicatedShardStoreIdentity) ([][32]byte, error) {
	result := make([][32]byte, len(identity.Relations))
	for i := range identity.Relations {
		if _, err := hex.Decode(result[i][:], []byte(identity.Relations[i].Storage)); err != nil {
			return nil, ErrReplicatedSchemaCatalogImage
		}
	}
	return result, nil
}
