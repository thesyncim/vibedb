package driver

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

const (
	replicatedSchemaActivationName        = ".schema-activation"
	replicatedSchemaActivationTemp        = ".schema-activation.tmp"
	replicatedSchemaActivationHeaderBytes = 48
)

var replicatedSchemaActivationMagic = [8]byte{'V', 'D', 'B', 'S', 'A', 'C', 'T', 0}
var replicatedSchemaActivationChecksumDomain = []byte(
	"vibedb/sql/schema-activation/format-0\x00",
)
var replicatedSchemaCatalogCASDomain = []byte(
	"vibedb/sql/schema-catalog-cas/format-0\x00",
)

type replicatedSchemaActivation struct {
	targetDigest [sha256.Size]byte
	command      []byte
}

func encodeReplicatedSchemaActivation(record replicatedSchemaActivation) ([]byte, error) {
	if record.targetDigest == ([sha256.Size]byte{}) || len(record.command) == 0 ||
		len(record.command) > replicatedstate.MaxSchemaTransitionBytes {
		return nil, ErrReplicatedSchemaCatalogImage
	}
	if _, err := replicatedstate.OpenSchemaTransition(record.command); err != nil {
		return nil, errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	total := replicatedSchemaActivationHeaderBytes + len(record.command) + sha256.Size
	raw := make([]byte, total)
	copy(raw[:8], replicatedSchemaActivationMagic[:])
	binary.LittleEndian.PutUint16(raw[8:10], 0)
	binary.LittleEndian.PutUint16(raw[10:12], uint16(len(record.command)))
	binary.LittleEndian.PutUint32(raw[12:16], uint32(total))
	copy(raw[16:48], record.targetDigest[:])
	copy(raw[48:], record.command)
	h := sha256.New()
	_, _ = h.Write(replicatedSchemaActivationChecksumDomain)
	_, _ = h.Write(raw[:total-sha256.Size])
	_ = h.Sum(raw[total-sha256.Size : total-sha256.Size])
	return raw, nil
}

func decodeReplicatedSchemaActivation(raw []byte) (replicatedSchemaActivation, error) {
	if len(raw) < replicatedSchemaActivationHeaderBytes+sha256.Size ||
		!bytes.Equal(raw[:8], replicatedSchemaActivationMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != 0 ||
		binary.LittleEndian.Uint32(raw[12:16]) != uint32(len(raw)) {
		return replicatedSchemaActivation{}, ErrReplicatedSchemaCatalogImage
	}
	commandBytes := int(binary.LittleEndian.Uint16(raw[10:12]))
	if commandBytes == 0 || commandBytes > replicatedstate.MaxSchemaTransitionBytes ||
		replicatedSchemaActivationHeaderBytes+commandBytes+sha256.Size != len(raw) {
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
	copy(record.command, raw[48:checksumAt])
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
		int64(replicatedSchemaActivationHeaderBytes+replicatedstate.MaxSchemaTransitionBytes+
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
			return nil
		}
		return ErrReplicatedSchemaCatalogImage
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
		err = syncDirectory(dataDir)
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
	h := sha256.New()
	_, _ = h.Write(replicatedSchemaCatalogCASDomain)
	_, _ = h.Write(currentDigest[:])
	_, _ = h.Write(proof.Catalog.Digest[:])
	_, _ = h.Write(requestDigest[:])
	_, _ = h.Write(authorizationDigest[:])
	var result [sha256.Size]byte
	_ = h.Sum(result[:0])
	return result, nil
}

// PersistReplicatedSchemaTransition records the exact authorized command
// before proposal. A committed transition can therefore be recovered without
// consulting an external controller or reconstructing semantically equivalent
// bytes.
func (a *ReplicatedApply) PersistReplicatedSchemaTransition(command []byte) error {
	if a == nil || a.database == nil {
		return ErrReplicatedApplyClosed
	}
	transition, err := replicatedstate.OpenSchemaTransition(command)
	if err != nil {
		return err
	}
	marker, found, err := readReplicatedSchemaStageMarker(a.database.dataDir)
	if err != nil || !found || marker.schemaGeneration != transition.ToSchemaGeneration ||
		marker.membership.Sequence != transition.MembershipSequence ||
		marker.membership.Source != transition.MembershipSource ||
		marker.membership.Target != transition.MembershipTarget ||
		marker.catalogDigest == ([32]byte{}) || marker.applyContract != transition.ToApplyContract ||
		marker.authorization != transition.RequestDigest {
		return errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	root, err := os.OpenRoot(a.database.dataDir)
	if err != nil {
		return err
	}
	file, err := root.Open(replicatedSchemaTargetCatalogName)
	if err != nil {
		_ = root.Close()
		return err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, maxCatalogBytes+1))
	err = errors.Join(readErr, file.Close(), root.Close())
	if err != nil {
		return err
	}
	image, err := ValidateReplicatedSchemaCatalogImage(raw)
	if err != nil || image.Digest != marker.catalogDigest ||
		image.SchemaGeneration != marker.schemaGeneration ||
		image.RelationManifestDigest != transition.ToManifest {
		return errors.Join(err, ErrReplicatedSchemaCatalogImage)
	}
	return writeReplicatedSchemaActivation(a.database.dataDir, replicatedSchemaActivation{
		targetDigest: marker.catalogDigest, command: command,
	})
}
