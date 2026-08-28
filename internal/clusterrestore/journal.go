package clusterrestore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

var progressMagic = [8]byte{'V', 'B', 'R', 'S', 'P', 'R', 'O', 'G'}

type activationProgress struct {
	Operation [sha256.Size]byte
	Roots     []RootWitness
	Catalog   [sha256.Size]byte
}

func readProgress(root *os.Root, operation Operation) (activationProgress, error) {
	file, err := root.Open("progress")
	if errors.Is(err, os.ErrNotExist) {
		return activationProgress{Operation: operation.Digest}, nil
	}
	if err != nil {
		return activationProgress{}, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file,
		int64(56+len(operation.Targets)*rootWitnessBytes+2*sha256.Size+1)))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return activationProgress{}, errors.Join(ErrActivation, readErr, closeErr)
	}
	progress, err := openProgress(raw, len(operation.Targets))
	if err != nil || progress.Operation != operation.Digest {
		return activationProgress{}, errors.Join(ErrActivation, err)
	}
	for ordinal, witness := range progress.Roots {
		if !validRootWitness(operation, ordinal, witness) {
			return activationProgress{}, ErrActivation
		}
	}
	if progress.Catalog != ([sha256.Size]byte{}) && len(progress.Roots) != len(operation.Targets) {
		return activationProgress{}, ErrActivation
	}
	return progress, nil
}

func writeProgress(root *os.Root, progress activationProgress) error {
	raw := appendProgress(nil, progress)
	return replaceExact(root, "progress", raw)
}

func appendProgress(dst []byte, progress activationProgress) []byte {
	start := len(dst)
	catalogBytes := 0
	if progress.Catalog != ([sha256.Size]byte{}) {
		catalogBytes = sha256.Size
	}
	dst = append(dst, make([]byte, 56+len(progress.Roots)*rootWitnessBytes+catalogBytes+sha256.Size)...)
	raw := dst[start:]
	copy(raw[:8], progressMagic[:])
	binary.BigEndian.PutUint16(raw[8:10], 1)
	binary.BigEndian.PutUint32(raw[12:16], uint32(len(progress.Roots)))
	if catalogBytes != 0 {
		raw[16] = 1
	}
	copy(raw[24:56], progress.Operation[:])
	offset := 56
	for _, witness := range progress.Roots {
		copy(raw[offset:offset+rootWitnessBytes], appendRootWitness(nil, witness))
		offset += rootWitnessBytes
	}
	if catalogBytes != 0 {
		copy(raw[offset:offset+sha256.Size], progress.Catalog[:])
		offset += sha256.Size
	}
	digest := sha256.Sum256(raw[:offset])
	copy(raw[offset:], digest[:])
	return dst
}

func openProgress(raw []byte, maximumRoots int) (activationProgress, error) {
	if len(raw) < 56+sha256.Size || [8]byte(raw[:8]) != progressMagic ||
		binary.BigEndian.Uint16(raw[8:10]) != 1 || !allZero(raw[10:12]) ||
		!allZero(raw[17:24]) || raw[16] > 1 {
		return activationProgress{}, ErrActivation
	}
	count := int(binary.BigEndian.Uint32(raw[12:16]))
	catalogBytes := int(raw[16]) * sha256.Size
	if count < 0 || count > maximumRoots ||
		len(raw) != 56+count*rootWitnessBytes+catalogBytes+sha256.Size {
		return activationProgress{}, ErrActivation
	}
	digestOffset := len(raw) - sha256.Size
	if sha256.Sum256(raw[:digestOffset]) != [sha256.Size]byte(raw[digestOffset:]) {
		return activationProgress{}, ErrActivation
	}
	progress := activationProgress{Roots: make([]RootWitness, count)}
	copy(progress.Operation[:], raw[24:56])
	offset := 56
	for index := range progress.Roots {
		progress.Roots[index] = openRootWitness(raw[offset : offset+rootWitnessBytes])
		offset += rootWitnessBytes
	}
	if catalogBytes != 0 {
		copy(progress.Catalog[:], raw[offset:offset+sha256.Size])
	}
	return progress, nil
}

func appendRootWitness(dst []byte, witness RootWitness) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, rootWitnessBytes)...)
	raw := dst[start:]
	copy(raw[:72], witness.TargetGroup[:])
	copy(raw[72:104], witness.ArtifactManifest[:])
	copy(raw[104:136], witness.SanitizedImageDigest[:])
	copy(raw[136:168], witness.GenesisProof[:])
	offset := 168
	for _, root := range witness.ReplicaRoots {
		copy(raw[offset:offset+sha256.Size], root[:])
		offset += sha256.Size
	}
	binary.BigEndian.PutUint64(raw[264:272], witness.SnapshotIndex)
	binary.BigEndian.PutUint64(raw[272:280], witness.SnapshotTerm)
	return dst
}

func openRootWitness(raw []byte) (witness RootWitness) {
	copy(witness.TargetGroup[:], raw[:72])
	copy(witness.ArtifactManifest[:], raw[72:104])
	copy(witness.SanitizedImageDigest[:], raw[104:136])
	copy(witness.GenesisProof[:], raw[136:168])
	offset := 168
	for ordinal := range witness.ReplicaRoots {
		copy(witness.ReplicaRoots[ordinal][:], raw[offset:offset+sha256.Size])
		offset += sha256.Size
	}
	witness.SnapshotIndex = binary.BigEndian.Uint64(raw[264:272])
	witness.SnapshotTerm = binary.BigEndian.Uint64(raw[272:280])
	return
}

func publishExact(root *os.Root, name string, raw []byte) error {
	file, err := root.Open(name)
	if err == nil {
		existing, readErr := io.ReadAll(io.LimitReader(file, int64(len(raw)+1)))
		closeErr := file.Close()
		if readErr == nil && closeErr == nil && bytes.Equal(existing, raw) {
			return nil
		}
		return errors.Join(ErrActivation, readErr, closeErr)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return replaceExact(root, name, raw)
}

func replaceExact(root *os.Root, name string, raw []byte) error {
	_ = root.Remove(".write")
	file, err := root.OpenFile(".write", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(raw)
	if writeErr == nil && written != len(raw) {
		writeErr = io.ErrShortWrite
	}
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return errors.Join(ErrActivation, writeErr, syncErr, closeErr)
	}
	if err = root.Rename(".write", name); err != nil {
		return err
	}
	directory, err := root.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
