package storeio

import (
	"errors"
	"fmt"
	"io"
	"os"
)

var ErrGenerationMigrationManifestNotFound = errors.New("vibedb: generation migration manifest not found")

// GenerationMigrationManifestStore is a two-slot, fsync-ordered durable
// migration record. Each slot is independently authenticated; a torn newest
// slot falls back to the preceding sequence after restart.
type GenerationMigrationManifestStore struct {
	file   *os.File
	offset int64
}

func OpenGenerationMigrationManifestStore(file *os.File, offset int64) (*GenerationMigrationManifestStore, error) {
	if file == nil || offset < 0 || offset%GenerationMigrationManifestBytes != 0 {
		return nil, fmt.Errorf("%w: migration manifest store", ErrInvalidWrite)
	}
	return &GenerationMigrationManifestStore{file: file, offset: offset}, nil
}

func (s *GenerationMigrationManifestStore) Load() (GenerationMigrationManifest, error) {
	var best GenerationMigrationManifest
	found := false
	for slot := 0; slot < 2; slot++ {
		var image [GenerationMigrationManifestBytes]byte
		_, readErr := s.file.ReadAt(image[:], s.offset+int64(slot*GenerationMigrationManifestBytes))
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return GenerationMigrationManifest{}, readErr
		}
		candidate, err := OpenGenerationMigrationManifest(image[:])
		if err != nil || candidate.ManifestSequence == 0 {
			continue
		}
		if found && (candidate.StoreID != best.StoreID || candidate.MigrationID != best.MigrationID) {
			return GenerationMigrationManifest{}, ErrGenerationMigrationManifestCorrupt
		}
		if !found || candidate.ManifestSequence > best.ManifestSequence {
			best, found = candidate, true
		}
	}
	if !found {
		return GenerationMigrationManifest{}, ErrGenerationMigrationManifestNotFound
	}
	best.Cursor = append([]byte(nil), best.Cursor...)
	return best, nil
}

func (s *GenerationMigrationManifestStore) Create(manifest GenerationMigrationManifest) error {
	if _, err := s.Load(); !errors.Is(err, ErrGenerationMigrationManifestNotFound) {
		if err == nil {
			return fmt.Errorf("%w: migration manifest already exists", ErrInvalidWrite)
		}
		return err
	}
	manifest.ManifestSequence = 1
	return s.write(manifest)
}

// Advance validates identity and monotonic progress before writing the other
// slot and synchronizing it. The supplied sequence is ignored.
func (s *GenerationMigrationManifestStore) Advance(next GenerationMigrationManifest) error {
	previous, err := s.Load()
	if err != nil {
		return err
	}
	if previous.ManifestSequence == ^uint64(0) {
		return fmt.Errorf("%w: manifest sequence exhausted", ErrInvalidWrite)
	}
	next.ManifestSequence = previous.ManifestSequence + 1
	if err := ValidateGenerationMigrationAdvance(previous, next); err != nil {
		return err
	}
	return s.write(next)
}

func (s *GenerationMigrationManifestStore) write(manifest GenerationMigrationManifest) error {
	var image [GenerationMigrationManifestBytes]byte
	encoded, err := EncodeGenerationMigrationManifest(image[:], manifest)
	if err != nil {
		return err
	}
	slot := manifest.ManifestSequence & 1
	if _, err := s.file.WriteAt(encoded, s.offset+int64(slot)*GenerationMigrationManifestBytes); err != nil {
		return err
	}
	return s.file.Sync()
}
