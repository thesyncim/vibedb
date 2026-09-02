package raftstore

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/raftstore/seglog"
)

const (
	descriptorCatalogHeaderBytes  = 128
	descriptorCatalogRecordBytes  = 16
	descriptorCatalogTrailerBytes = sha256.Size
	descriptorCatalogMarker       = uint16(1)
)

var descriptorCatalogMagic = [8]byte{'V', 'D', 'B', 'N', 'C', 'A', 'T', 0}

type DescriptorCheckpointPhase uint8

const (
	DescriptorCheckpointTempWritten DescriptorCheckpointPhase = iota + 1
	DescriptorCheckpointFileSynced
	DescriptorCheckpointRenamed
	DescriptorCheckpointDirectorySynced
	DescriptorCheckpointBeforeLogReference
	DescriptorCheckpointLogReferenceDurable
)

type descriptorCatalogCandidate struct {
	id      [16]byte
	through uint64
}

func descriptorCatalogName(id [16]byte) string {
	return "descriptor-catalog-" + hex.EncodeToString(id[:]) + ".chk"
}

func descriptorCatalogPath(dir string, id [16]byte) string {
	return filepath.Join(dir, nodeCheckpointDir, descriptorCatalogName(id))
}

func descriptorCatalogTempPath(dir string, id [16]byte) string {
	return filepath.Join(dir, nodeCheckpointDir, "."+descriptorCatalogName(id)+".tmp")
}

func descriptorCatalogHeader(identity NodeIdentity, logID, id [16]byte, through, payloadBytes uint64) [descriptorCatalogHeaderBytes]byte {
	var header [descriptorCatalogHeaderBytes]byte
	copy(header[:8], descriptorCatalogMagic[:])
	binary.LittleEndian.PutUint16(header[8:10], descriptorCatalogMarker)
	binary.LittleEndian.PutUint16(header[10:12], descriptorCatalogHeaderBytes)
	copy(header[16:32], logID[:])
	copy(header[32:48], id[:])
	copy(header[48:64], identity.ClusterID[:])
	copy(header[64:80], identity.ClusterIncarnation[:])
	copy(header[80:96], identity.NodeID[:])
	binary.LittleEndian.PutUint64(header[96:104], through)
	binary.LittleEndian.PutUint64(header[104:112], through)
	binary.LittleEndian.PutUint64(header[112:120], payloadBytes)
	return header
}

func descriptorCatalogHeaderValid(header []byte, identity NodeIdentity, logID [16]byte, checkpoint seglog.Checkpoint, limit int, fileBytes int64) (uint64, uint64, bool) {
	if len(header) != descriptorCatalogHeaderBytes || !bytes.Equal(header[:8], descriptorCatalogMagic[:]) ||
		binary.LittleEndian.Uint16(header[8:10]) != descriptorCatalogMarker || binary.LittleEndian.Uint16(header[10:12]) != descriptorCatalogHeaderBytes ||
		!allZero(header[12:16]) || !bytes.Equal(header[16:32], logID[:]) || !bytes.Equal(header[32:48], checkpoint.ID[:]) ||
		!bytes.Equal(header[48:64], identity.ClusterID[:]) || !bytes.Equal(header[64:80], identity.ClusterIncarnation[:]) || !bytes.Equal(header[80:96], identity.NodeID[:]) || !allZero(header[120:128]) {
		return 0, 0, false
	}
	through, count := binary.LittleEndian.Uint64(header[96:104]), binary.LittleEndian.Uint64(header[104:112])
	payloadBytes := binary.LittleEndian.Uint64(header[112:120])
	if checkpoint.Term != 1 || through != checkpoint.Index || count != through || through == 0 || through > uint64(limit) || payloadBytes < descriptorCatalogRecordBytes ||
		payloadBytes > uint64(fileBytes) || payloadBytes+descriptorCatalogHeaderBytes+descriptorCatalogTrailerBytes != uint64(fileBytes) {
		return 0, 0, false
	}
	return count, payloadBytes, true
}

func descriptorCatalogPayloadBytes(descriptors []GroupDescriptor) (uint64, error) {
	var total uint64
	for i := range descriptors {
		if descriptors[i].LogKey != uint64(i+1) || validateGroupDescriptor(descriptors[i], false) != nil {
			return 0, ErrCorrupt
		}
		plainBytes := nodeDescriptorFixed + len(descriptors[i].Distribution) + len(descriptors[i].Shard)
		recordBytes := uint64(descriptorCatalogRecordBytes + plainBytes + 16)
		if total > ^uint64(0)-recordBytes {
			return 0, ErrBounds
		}
		total += recordBytes
	}
	return total, nil
}

func writeCatalogPart(file *os.File, part []byte, mac io.Writer) error {
	for len(part) != 0 {
		written, err := file.Write(part)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		if _, err = mac.Write(part[:written]); err != nil {
			return err
		}
		part = part[written:]
	}
	return nil
}

func writeCatalogTrailer(file *os.File, trailer []byte) error {
	for len(trailer) != 0 {
		written, err := file.Write(trailer)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		trailer = trailer[written:]
	}
	return nil
}

// CheckpointDescriptorCatalog streams an immutable descriptor prefix into an
// authenticated encrypted checkpoint. Publishing the file alone has no
// authority: recovery uses it only after the descriptor group's checkpoint
// reference and logical truncation are durable in the node log.
func (s *NodeStore) CheckpointDescriptorCatalog() error {
	if s == nil {
		return ErrInvalid
	}
	s.catalogMu.Lock()
	defer s.catalogMu.Unlock()

	s.mu.Lock()
	if err := s.usable(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.maintenance.Add(1)
	defer s.maintenance.Done()
	through := uint64(len(s.descriptors))
	if through == 0 {
		s.mu.Unlock()
		return ErrCorrupt
	}
	metadata, exists := s.engine.Metadata(nodeDescriptorGroup)
	if !exists || metadata.LastIndex != through || metadata.Hard.Term != 1 || metadata.Hard.Commit != through || metadata.Checkpoint.Index > through {
		s.mu.Unlock()
		return ErrCorrupt
	}
	if metadata.Checkpoint.Index == through {
		if metadata.Checkpoint.ID == ([16]byte{}) || metadata.Checkpoint.Term != 1 || metadata.TruncateIndex != through || metadata.TruncateTerm != 1 {
			s.mu.Unlock()
			return ErrCorrupt
		}
		keep := metadata.Checkpoint.ID
		s.mu.Unlock()
		return s.removeUnreferencedDescriptorCatalogs(keep)
	}
	descriptors := s.descriptors[:through:through]
	identity, logID := s.identity, s.engine.LogID()
	material := s.key.Material
	hook := s.descriptorCheckpointHookTest
	leaveTemp := s.descriptorCheckpointLeaveTempTest
	s.mu.Unlock()

	payloadBytes, err := descriptorCatalogPayloadBytes(descriptors)
	if err != nil {
		return err
	}
	var id [16]byte
	if _, err = rand.Read(id[:]); err != nil || id == ([16]byte{}) {
		return errors.Join(ErrInvalid, err)
	}
	header := descriptorCatalogHeader(identity, logID, id, through, payloadBytes)
	candidate := descriptorCatalogCandidate{id: id, through: through}
	final := descriptorCatalogPath(s.dir, id)
	tmp := descriptorCatalogTempPath(s.dir, id)
	if _, statErr := os.Stat(final); statErr == nil {
		return os.ErrExist
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	ok, renamed, simulatedCrash := false, false, false
	defer func() {
		_ = file.Close()
		if !ok && !renamed && !leaveTemp && !simulatedCrash {
			_ = os.Remove(tmp)
		}
	}()
	authKey := deriveFileSecret(material, logID, "node-descriptor-catalog-auth")
	mac := hmac.New(sha256.New, authKey[:])
	if err = writeCatalogPart(file, header[:], mac); err != nil {
		return err
	}
	workspace := newObjectCryptoWorkspace(s.crypto.dataKey, s.crypto.nonceKey)
	headerDigest := sha256.Sum256(header[:])
	plain := make([]byte, 0, nodeDescriptorFixed+2*MaxIdentityComponentBytes)
	ciphertext := make([]byte, 0, cap(plain)+s.crypto.aead.Overhead())
	var record [descriptorCatalogRecordBytes]byte
	var aad [descriptorCatalogHeaderBytes + descriptorCatalogRecordBytes]byte
	copy(aad[:descriptorCatalogHeaderBytes], header[:])
	for i := range descriptors {
		plain = plain[:0]
		plain, err = appendGroupDescriptor(plain, descriptors[i])
		if err != nil {
			return err
		}
		clear(record[:])
		binary.LittleEndian.PutUint64(record[0:8], uint64(i+1))
		binary.LittleEndian.PutUint32(record[8:12], uint32(len(plain)+s.crypto.aead.Overhead()))
		copy(aad[descriptorCatalogHeaderBytes:], record[:])
		nonce := workspace.deriveObjectNonce("node-catalog-entry", uint64(i+1), headerDigest)
		ciphertext = s.crypto.aead.Seal(ciphertext[:0], nonce[:], plain, aad[:])
		if err = writeCatalogPart(file, record[:], mac); err == nil {
			err = writeCatalogPart(file, ciphertext, mac)
		}
		if err != nil {
			return err
		}
	}
	trailer := mac.Sum(nil)
	if len(trailer) != descriptorCatalogTrailerBytes {
		return ErrCorrupt
	}
	if err = writeCatalogTrailer(file, trailer); err != nil {
		return err
	}
	if hook != nil {
		if err = hook(DescriptorCheckpointTempWritten); err != nil {
			simulatedCrash = true
			return err
		}
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if hook != nil {
		if err = hook(DescriptorCheckpointFileSynced); err != nil {
			simulatedCrash = true
			return err
		}
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, final); err != nil {
		return err
	}
	renamed = true
	if hook != nil {
		if err = hook(DescriptorCheckpointRenamed); err != nil {
			return err
		}
	}
	if err = syncNodeDirectory(filepath.Join(s.dir, nodeCheckpointDir)); err != nil {
		return err
	}
	if hook != nil {
		if err = hook(DescriptorCheckpointDirectorySynced); err != nil {
			return err
		}
		if err = hook(DescriptorCheckpointBeforeLogReference); err != nil {
			return err
		}
	}
	if err = s.publishDescriptorCatalogReference(candidate); err != nil {
		return err
	}
	if hook != nil {
		if err = hook(DescriptorCheckpointLogReferenceDurable); err != nil {
			return err
		}
	}
	ok = true
	return s.removeUnreferencedDescriptorCatalogs(id)
}

// ReclaimDeadNodeLogPrefix asks the serial device-maintenance lane to recycle
// a whole dead prefix. ErrBounds means the engine's count/byte threshold is not
// yet met; no foreground append performs this work.
func (s *NodeStore) ReclaimDeadNodeLogPrefix() error {
	if s == nil {
		return ErrInvalid
	}
	s.mu.Lock()
	if err := s.usable(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.maintenance.Add(1)
	engine := s.engine
	s.mu.Unlock()
	defer s.maintenance.Done()
	return engine.ReclaimDeadPrefix()
}

func (s *NodeStore) publishDescriptorCatalogReference(candidate descriptorCatalogCandidate) error {
	s.mu.Lock()
	sequencer := s.sequencer
	s.mu.Unlock()
	if sequencer == nil {
		return s.publishDescriptorCatalogReferenceLocked(candidate, false)
	}
	var submission Submission
	if err := submission.Initialize(); err != nil {
		return err
	}
	if err := submission.prepareDescriptorCatalog(candidate); err != nil {
		return err
	}
	if _, err := sequencer.TrySubmit(&submission); err != nil {
		return err
	}
	_, err := submission.Wait()
	return err
}

func (s *NodeStore) publishDescriptorCatalogReferenceLocked(candidate descriptorCatalogCandidate, sequenced bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if sequenced != (s.sequencer != nil) {
		if s.sequencer != nil {
			return ErrSequencerActive
		}
		return ErrInvalid
	}
	if err := s.proveNamespace(); err != nil {
		s.poisoned = err
		return err
	}
	metadata, ok := s.engine.Metadata(nodeDescriptorGroup)
	if !ok || candidate.through == 0 || candidate.through > uint64(len(s.descriptors)) || candidate.through > metadata.LastIndex ||
		metadata.Hard.Term != 1 || metadata.Hard.Commit < candidate.through {
		return ErrCorrupt
	}
	cp := seglog.Checkpoint{ID: candidate.id, Index: candidate.through, Term: 1}
	s.waveBatches[0] = seglog.ReadyBatch{GroupID: nodeDescriptorGroup, Checkpoint: &cp, TruncateIndex: candidate.through, TruncateTerm: 1}
	var canonical [48]byte
	copy(canonical[:24], []byte("descriptor-catalog-cut"))
	copy(canonical[24:40], candidate.id[:])
	binary.LittleEndian.PutUint64(canonical[40:48], candidate.through)
	digest := sha256.Sum256(canonical[:])
	var waveID seglog.WaveID
	copy(waveID[:], digest[:16])
	if err := s.engine.PersistWave(seglog.Wave{ID: waveID, Batches: s.waveBatches[:1]}); err != nil {
		if fatal := s.engine.FatalError(); fatal != nil {
			s.poisoned = fatal
			return errors.Join(ErrPersistenceUnknown, err, fatal)
		}
		return err
	}
	s.cacheValid = false
	if err := s.proveNamespace(); err != nil {
		s.poisoned = err
		return errors.Join(ErrPersistenceUnknown, err)
	}
	return nil
}

func (s *NodeStore) readDescriptorCatalog(checkpoint seglog.Checkpoint, limit int) ([]GroupDescriptor, error) {
	path := descriptorCatalogPath(s.dir, checkpoint.ID)
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Join(ErrCorrupt, err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() < descriptorCatalogHeaderBytes+descriptorCatalogTrailerBytes {
		return nil, errors.Join(ErrCorrupt, err)
	}
	var header [descriptorCatalogHeaderBytes]byte
	if _, err = io.ReadFull(file, header[:]); err != nil {
		return nil, ErrCorrupt
	}
	count, payloadBytes, valid := descriptorCatalogHeaderValid(header[:], s.identity, s.engine.LogID(), checkpoint, limit, stat.Size())
	if !valid {
		return nil, ErrCorrupt
	}
	authKey := deriveFileSecret(s.key.Material, s.engine.LogID(), "node-descriptor-catalog-auth")
	mac := hmac.New(sha256.New, authKey[:])
	_, _ = mac.Write(header[:])
	workspace := newObjectCryptoWorkspace(s.crypto.dataKey, s.crypto.nonceKey)
	headerDigest := sha256.Sum256(header[:])
	descriptors := make([]GroupDescriptor, 0, min(int(count), limit))
	ciphertext := make([]byte, nodeDescriptorFixed+2*MaxIdentityComponentBytes+s.crypto.aead.Overhead())
	plain := make([]byte, 0, nodeDescriptorFixed+2*MaxIdentityComponentBytes)
	var record [descriptorCatalogRecordBytes]byte
	var aad [descriptorCatalogHeaderBytes + descriptorCatalogRecordBytes]byte
	copy(aad[:descriptorCatalogHeaderBytes], header[:])
	consumed := uint64(0)
	for ordinal := uint64(1); ordinal <= count; ordinal++ {
		if _, err = io.ReadFull(file, record[:]); err != nil {
			return nil, ErrCorrupt
		}
		_, _ = mac.Write(record[:])
		encodedOrdinal := binary.LittleEndian.Uint64(record[0:8])
		cipherBytes := uint64(binary.LittleEndian.Uint32(record[8:12]))
		if encodedOrdinal != ordinal || !allZero(record[12:16]) || cipherBytes < uint64(nodeDescriptorFixed+s.crypto.aead.Overhead()) || cipherBytes > uint64(len(ciphertext)) ||
			consumed > payloadBytes-descriptorCatalogRecordBytes || cipherBytes > payloadBytes-consumed-descriptorCatalogRecordBytes {
			return nil, ErrCorrupt
		}
		cipher := ciphertext[:cipherBytes]
		if _, err = io.ReadFull(file, cipher); err != nil {
			return nil, ErrCorrupt
		}
		_, _ = mac.Write(cipher)
		copy(aad[descriptorCatalogHeaderBytes:], record[:])
		nonce := workspace.deriveObjectNonce("node-catalog-entry", ordinal, headerDigest)
		plain, err = s.crypto.aead.Open(plain[:0], nonce[:], cipher, aad[:])
		if err != nil {
			return nil, ErrCorrupt
		}
		descriptor, decodeErr := decodeGroupDescriptor(plain)
		if decodeErr != nil || descriptor.LogKey != ordinal {
			return nil, ErrCorrupt
		}
		descriptors = append(descriptors, descriptor)
		consumed += descriptorCatalogRecordBytes + cipherBytes
	}
	if consumed != payloadBytes {
		return nil, ErrCorrupt
	}
	var trailer [descriptorCatalogTrailerBytes]byte
	if _, err = io.ReadFull(file, trailer[:]); err != nil {
		return nil, ErrCorrupt
	}
	var extra [1]byte
	if n, readErr := file.Read(extra[:]); n != 0 || readErr != io.EOF {
		return nil, ErrCorrupt
	}
	if !hmac.Equal(trailer[:], mac.Sum(nil)) {
		return nil, ErrCorrupt
	}
	return descriptors, nil
}

func isDescriptorCatalogArtifact(name string) bool {
	base := name
	if len(base) > 0 && base[0] == '.' {
		base = base[1:]
		if len(base) < 4 || base[len(base)-4:] != ".tmp" {
			return false
		}
		base = base[:len(base)-4]
	}
	const prefix, suffix = "descriptor-catalog-", ".chk"
	if len(base) != len(prefix)+32+len(suffix) || base[:len(prefix)] != prefix || base[len(base)-len(suffix):] != suffix {
		return false
	}
	_, err := hex.DecodeString(base[len(prefix) : len(prefix)+32])
	return err == nil
}

func (s *NodeStore) removeUnreferencedDescriptorCatalogs(keep [16]byte) error {
	dir := filepath.Join(s.dir, nodeCheckpointDir)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	keepName := descriptorCatalogName(keep)
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if name == keepName || !isDescriptorCatalogArtifact(name) {
			continue
		}
		if err = os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		removed = true
	}
	if removed {
		return syncNodeDirectory(dir)
	}
	return nil
}
