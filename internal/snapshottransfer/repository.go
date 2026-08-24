package snapshottransfer

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"math"
	"os"
	"sync"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
	"github.com/thesyncim/vibedb/internal/storeio"
)

const cursorBytes = 48

var cursorMagic = [8]byte{'V', 'B', 'S', 'C', 'U', 'R', 0, 0}

type Limits struct {
	MaxArtifacts     int
	MaxArtifactBytes uint64
	MaxDiskBytes     uint64
}

type record struct {
	descriptor Descriptor
	stage      string
	published  string
	offset     uint64
	complete   bool
	file       *os.File
}

type RepositoryStats struct {
	Artifacts        int
	Staged           int
	Published        int
	DiskBytes        uint64
	ArtifactCapacity int
	DiskCapacity     uint64
}

// Repository owns resumable artifact bytes but never activates them.
type Repository struct {
	mu        sync.RWMutex
	root      *os.Root
	lock      *os.File
	limits    Limits
	records   map[[sha256.Size]byte]*record
	diskBytes uint64
	closed    bool
	verify    func(*os.File, Descriptor) error
}

func OpenRepository(path string, limits Limits) (*Repository, error) {
	return openRepository(path, limits, verifyReplicatedArtifact)
}

func openRepository(
	path string,
	limits Limits,
	verify func(*os.File, Descriptor) error,
) (*Repository, error) {
	if path == "" || limits.MaxArtifacts <= 0 || limits.MaxArtifacts > 4096 ||
		limits.MaxArtifactBytes == 0 || limits.MaxDiskBytes < limits.MaxArtifactBytes || verify == nil {
		return nil, ErrBound
	}
	if limits.MaxArtifactBytes > math.MaxInt64-DescriptorBytes ||
		limits.MaxDiskBytes < limits.MaxArtifactBytes+DescriptorBytes {
		return nil, ErrBound
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	lock, err := openRegular(root, "repository.lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	if err = storeio.LockWriter(lock); err != nil {
		_ = lock.Close()
		_ = root.Close()
		return nil, errors.Join(ErrRepository, err)
	}
	r := &Repository{root: root, lock: lock, limits: limits,
		records: make(map[[sha256.Size]byte]*record, limits.MaxArtifacts),
		verify:  verify}
	if err := r.recover(); err != nil {
		_ = r.Close()
		return nil, err
	}
	return r, nil
}

func artifactNames(hash [sha256.Size]byte) (stage, cursor, temporary, published string) {
	var encoded [sha256.Size * 2]byte
	hex.Encode(encoded[:], hash[:])
	tail := string(encoded[:])
	return "s-" + tail, "c-" + tail, "t-" + tail, "p-" + tail
}

func parseArtifactName(name string) (kind byte, hash [sha256.Size]byte, ok bool) {
	if len(name) != 2+sha256.Size*2 || name[1] != '-' ||
		(name[0] != 's' && name[0] != 'c' && name[0] != 't' && name[0] != 'p') {
		return 0, hash, false
	}
	decoded, err := hex.Decode(hash[:], []byte(name[2:]))
	return name[0], hash, err == nil && decoded == sha256.Size
}

func (r *Repository) recover() error {
	entries, err := fs.ReadDir(r.root.FS(), ".")
	if err != nil {
		return err
	}
	// Cursor temporaries are never authoritative. A synced cursor or published
	// artifact is the only recovery boundary.
	for _, entry := range entries {
		kind, _, ok := parseArtifactName(entry.Name())
		if ok && kind == 't' {
			if err := r.root.Remove(entry.Name()); err != nil {
				return err
			}
		}
	}
	entries, err = fs.ReadDir(r.root.FS(), ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		kind, hash, ok := parseArtifactName(entry.Name())
		if !ok || (kind != 's' && kind != 'p') {
			continue
		}
		if entry.Type()&fs.ModeType != 0 {
			return ErrRepository
		}
		if existing := r.records[hash]; existing != nil {
			if existing.complete && kind == 's' {
				if err := r.root.Remove(entry.Name()); err != nil {
					return err
				}
				_, cursor, _, _ := artifactNames(hash)
				_ = r.root.Remove(cursor)
				continue
			}
			return ErrRepository
		}
		if len(r.records) == r.limits.MaxArtifacts {
			return ErrBound
		}
		file, err := openRegular(r.root, entry.Name(), os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		var raw [DescriptorBytes]byte
		_, readErr := io.ReadFull(file, raw[:])
		info, statErr := file.Stat()
		_ = file.Close()
		if readErr != nil || statErr != nil {
			return errors.Join(ErrRepository, readErr, statErr)
		}
		d, err := OpenDescriptor(raw[:])
		if err != nil || d.ArtifactHash != hash || d.ArtifactBytes > r.limits.MaxArtifactBytes ||
			info.Size() < DescriptorBytes || uint64(info.Size()-DescriptorBytes) > d.ArtifactBytes {
			return errors.Join(ErrRepository, err)
		}
		stage, cursor, _, published := artifactNames(hash)
		rec := &record{descriptor: d, stage: stage, published: published,
			complete: kind == 'p'}
		if rec.complete {
			rec.offset = d.ArtifactBytes
			publishedFile, openErr := openRegular(r.root, published, os.O_RDONLY, 0)
			if openErr != nil {
				return openErr
			}
			verifyErr := verifyFileHash(publishedFile, d)
			if verifyErr == nil {
				verifyErr = r.verify(publishedFile, d)
			}
			if verifyErr != nil {
				_ = publishedFile.Close()
				return verifyErr
			}
			rec.file = publishedFile
			_ = r.root.Remove(cursor)
		} else {
			rec.offset, err = r.readCursor(cursor, hash)
			if errors.Is(err, os.ErrNotExist) {
				rec.offset, err = 0, nil
			}
			if err != nil || rec.offset > uint64(info.Size()-DescriptorBytes) {
				return errors.Join(ErrRepository, err)
			}
			if err := r.truncateStage(stage, rec.offset); err != nil {
				return err
			}
		}
		r.records[hash] = rec
		r.diskBytes += DescriptorBytes + rec.offset
		if r.diskBytes > r.limits.MaxDiskBytes {
			return ErrBound
		}
	}
	entries, err = fs.ReadDir(r.root.FS(), ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		kind, hash, ok := parseArtifactName(entry.Name())
		if ok && kind == 'c' {
			rec := r.records[hash]
			if rec == nil || rec.complete {
				if err := r.root.Remove(entry.Name()); err != nil {
					return err
				}
			}
		}
	}
	for _, rec := range r.records {
		if !rec.complete && rec.offset == rec.descriptor.ArtifactBytes {
			if err := r.finish(rec); err != nil {
				return err
			}
		}
	}
	return syncRoot(r.root)
}

func (r *Repository) readCursor(name string, hash [sha256.Size]byte) (uint64, error) {
	f, err := openRegular(r.root, name, os.O_RDONLY, 0)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var raw [cursorBytes]byte
	if _, err = io.ReadFull(f, raw[:]); err != nil {
		return 0, err
	}
	var extra [1]byte
	if n, readErr := f.Read(extra[:]); n != 0 || !errors.Is(readErr, io.EOF) {
		return 0, ErrRepository
	}
	if !bytes.Equal(raw[:8], cursorMagic[:]) || !bytes.Equal(raw[8:40], hash[:]) {
		return 0, ErrRepository
	}
	return binary.BigEndian.Uint64(raw[40:48]), nil
}

func (r *Repository) persistCursor(hash [sha256.Size]byte, offset uint64) error {
	_, cursor, temporary, _ := artifactNames(hash)
	f, err := r.root.OpenFile(temporary, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	renamed := false
	defer func() {
		if !renamed {
			_ = r.root.Remove(temporary)
		}
	}()
	var raw [cursorBytes]byte
	copy(raw[:8], cursorMagic[:])
	copy(raw[8:40], hash[:])
	binary.BigEndian.PutUint64(raw[40:48], offset)
	if err = writeFull(f, raw[:]); err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	if err = replaceRepositoryEntry(r.root, temporary, cursor); err != nil {
		return err
	}
	renamed = true
	if err = syncRoot(r.root); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	return nil
}

func (r *Repository) truncateStage(name string, offset uint64) error {
	f, err := openRegular(r.root, name, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	err = errors.Join(f.Truncate(int64(DescriptorBytes)+int64(offset)), f.Sync(), f.Close())
	return err
}

// Offset returns the exact durably acknowledged resume point.
func (r *Repository) Offset(d Descriptor) (uint64, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || !d.Valid() || d.ArtifactBytes > r.limits.MaxArtifactBytes {
		return 0, false, ErrDescriptor
	}
	rec := r.records[d.ArtifactHash]
	if rec == nil {
		return 0, false, nil
	}
	if rec.descriptor != d {
		return 0, false, ErrStaleFence
	}
	if !rec.complete && rec.offset == d.ArtifactBytes {
		if err := r.finish(rec); err != nil {
			return rec.offset, false, err
		}
	}
	return rec.offset, rec.complete, nil
}

// Append verifies, writes, fsyncs, and advances one exact contiguous chunk.
// An exact retry is idempotent. Reordered or overlapping bytes fail closed.
func (r *Repository) Append(d Descriptor, offset uint64, chunk []byte, digest [sha256.Size]byte) (uint64, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || !d.Valid() || d.ArtifactBytes > r.limits.MaxArtifactBytes ||
		len(chunk) == 0 || len(chunk) > int(d.ChunkBytes) || sha256.Sum256(chunk) != digest ||
		offset > d.ArtifactBytes || uint64(len(chunk)) > d.ArtifactBytes-offset {
		return 0, false, ErrChunk
	}
	rec, err := r.ensure(d)
	if err != nil {
		return 0, false, err
	}
	if offset < rec.offset || rec.complete {
		if offset <= rec.offset && uint64(len(chunk)) <= rec.offset-offset &&
			r.recordBytesEqual(rec, offset, chunk) {
			return rec.offset, rec.complete, nil
		}
		return rec.offset, rec.complete, ErrChunk
	}
	if offset > rec.offset {
		return rec.offset, false, ErrChunk
	}
	if r.diskBytes > r.limits.MaxDiskBytes-uint64(len(chunk)) {
		return rec.offset, false, ErrBound
	}
	f, err := openRegular(r.root, rec.stage, os.O_RDWR, 0)
	if err != nil {
		return rec.offset, false, err
	}
	_, err = f.WriteAt(chunk, int64(DescriptorBytes)+int64(offset))
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return rec.offset, false, err
	}
	next := offset + uint64(len(chunk))
	if err = r.persistCursor(d.ArtifactHash, next); err != nil {
		return rec.offset, false, err
	}
	rec.offset, r.diskBytes = next, r.diskBytes+uint64(len(chunk))
	if next == d.ArtifactBytes {
		if err = r.finish(rec); err != nil {
			return next, false, err
		}
	}
	return rec.offset, rec.complete, nil
}

func (r *Repository) recordBytesEqual(rec *record, offset uint64, expected []byte) bool {
	var f *os.File
	closeFile := false
	if rec.complete {
		f = rec.file
	} else {
		var err error
		f, err = openRegular(r.root, rec.stage, os.O_RDONLY, 0)
		if err != nil {
			return false
		}
		closeFile = true
	}
	if f == nil {
		return false
	}
	if closeFile {
		defer f.Close()
	}
	var scratch [32 << 10]byte
	position := int64(DescriptorBytes) + int64(offset)
	for len(expected) != 0 {
		count := min(len(expected), len(scratch))
		part := scratch[:count]
		if _, err := f.ReadAt(part, position); err != nil {
			return false
		}
		if !bytes.Equal(part, expected[:count]) {
			return false
		}
		expected = expected[count:]
		position += int64(count)
	}
	return true
}

func (r *Repository) ensure(d Descriptor) (*record, error) {
	if rec := r.records[d.ArtifactHash]; rec != nil {
		if rec.descriptor != d {
			return nil, ErrStaleFence
		}
		return rec, nil
	}
	if len(r.records) == r.limits.MaxArtifacts ||
		r.diskBytes > r.limits.MaxDiskBytes-DescriptorBytes {
		return nil, ErrBound
	}
	stage, _, _, published := artifactNames(d.ArtifactHash)
	f, err := r.root.OpenFile(stage, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, err
	}
	var raw [DescriptorBytes]byte
	encoded, _ := AppendDescriptor(raw[:0], d)
	err = writeFull(f, encoded)
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		_ = r.root.Remove(stage)
		return nil, err
	}
	rec := &record{descriptor: d, stage: stage, published: published}
	r.records[d.ArtifactHash], r.diskBytes = rec, r.diskBytes+DescriptorBytes
	if err = syncRoot(r.root); err != nil {
		return rec, errors.Join(ErrOutcomeUnknown, err)
	}
	return rec, nil
}

func (r *Repository) finish(rec *record) error {
	f, err := openRegular(r.root, rec.stage, os.O_RDONLY, 0)
	fromStage := err == nil
	if errors.Is(err, os.ErrNotExist) {
		f, err = openRegular(r.root, rec.published, os.O_RDONLY, 0)
	}
	if err != nil {
		return err
	}
	err = verifyFileHash(f, rec.descriptor)
	if err == nil {
		err = r.verify(f, rec.descriptor)
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	_, cursor, _, _ := artifactNames(rec.descriptor.ArtifactHash)
	if fromStage {
		if err = r.root.Rename(rec.stage, rec.published); err != nil {
			return err
		}
	}
	if removeErr := r.root.Remove(cursor); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return removeErr
	}
	if err = syncRoot(r.root); err != nil {
		return errors.Join(ErrOutcomeUnknown, err)
	}
	publishedFile, err := openRegular(r.root, rec.published, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	rec.file = publishedFile
	rec.complete = true
	return nil
}

func verifyFileHash(f *os.File, d Descriptor) error {
	if _, err := f.Seek(DescriptorBytes, io.SeekStart); err != nil {
		return err
	}
	h := sha256.New()
	if _, err := io.CopyN(h, f, int64(d.ArtifactBytes)); err != nil {
		return err
	}
	var got [sha256.Size]byte
	copy(got[:], h.Sum(nil))
	if got != d.ArtifactHash {
		return ErrChunk
	}
	return nil
}

func verifyReplicatedArtifact(f *os.File, d Descriptor) error {
	if _, err := f.Seek(DescriptorBytes, io.SeekStart); err != nil {
		return err
	}
	manifest, err := replicatedstate.VerifySnapshotArtifact(io.LimitReader(f, int64(d.ArtifactBytes)), replicatedstate.SnapshotArtifactCallbacks{})
	if err != nil || manifest.EncodedBytes != d.ArtifactBytes ||
		manifest.State.Applied != d.SnapshotIndex || manifest.State.LastTerm != d.SnapshotTerm ||
		manifest.State.ReplicaSetVersion != d.ReplicaSetVersion ||
		manifest.State.Binding.SchemaGeneration != d.SchemaGeneration ||
		manifest.State.LastEntryDigest != d.Lineage ||
		manifest.State.Binding.ClusterID != d.Group.ClusterID ||
		manifest.State.Binding.ClusterIncarnation != d.Group.ClusterIncarnation ||
		manifest.State.Binding.TopologyRecoveryEpoch != d.Group.TopologyRecoveryEpoch ||
		manifest.State.Binding.ShardIncarnation != d.Group.ShardIncarnation ||
		manifest.State.Binding.GroupID != d.Group.GroupID {
		return errors.Join(ErrDescriptor, err)
	}
	return nil
}

// ReadChunk returns published bytes only; staged artifacts are never served.
func (r *Repository) ReadChunk(d Descriptor, offset uint64, dst []byte) ([]byte, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec := r.records[d.ArtifactHash]
	if r.closed || rec == nil || !rec.complete || rec.descriptor != d {
		return dst[:0], false, ErrStaleFence
	}
	if offset > d.ArtifactBytes {
		return dst[:0], false, ErrChunk
	}
	remaining := d.ArtifactBytes - offset
	if remaining == 0 {
		return dst[:0], true, nil
	}
	want := min(uint64(d.ChunkBytes), remaining)
	if cap(dst) < int(want) {
		return dst[:0], false, ErrBound
	}
	dst = dst[:int(want)]
	if rec.file == nil {
		return dst[:0], false, ErrRepository
	}
	if _, err := rec.file.ReadAt(dst, int64(DescriptorBytes)+int64(offset)); err != nil {
		return dst[:0], false, err
	}
	return dst, offset+want == d.ArtifactBytes, nil
}

func (r *Repository) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	var fileErr error
	for _, rec := range r.records {
		if rec.file != nil {
			fileErr = errors.Join(fileErr, rec.file.Close())
			rec.file = nil
		}
	}
	err := errors.Join(fileErr, storeio.UnlockWriter(r.lock), r.lock.Close(), r.root.Close())
	r.lock, r.root = nil, nil
	return err
}

func (r *Repository) Stats() RepositoryStats {
	if r == nil {
		return RepositoryStats{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	stats := RepositoryStats{Artifacts: len(r.records), DiskBytes: r.diskBytes,
		ArtifactCapacity: r.limits.MaxArtifacts, DiskCapacity: r.limits.MaxDiskBytes}
	for _, rec := range r.records {
		if rec.complete {
			stats.Published++
		} else {
			stats.Staged++
		}
	}
	return stats
}

func writeFull(w io.Writer, b []byte) error {
	for len(b) != 0 {
		n, err := w.Write(b)
		if n < 0 || n > len(b) {
			return io.ErrShortWrite
		}
		b = b[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func openRegular(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
	var before os.FileInfo
	var err error
	if flags&os.O_CREATE == 0 {
		before, err = root.Lstat(name)
		if err != nil {
			return nil, err
		}
		if !before.Mode().IsRegular() {
			return nil, ErrRepository
		}
	}
	f, err := root.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}
	opened, openErr := f.Stat()
	entry, entryErr := root.Lstat(name)
	stable := openErr == nil && entryErr == nil && opened.Mode().IsRegular() && entry.Mode().IsRegular() && os.SameFile(opened, entry)
	if before != nil {
		stable = stable && os.SameFile(before, entry)
	}
	if !stable {
		_ = f.Close()
		return nil, errors.Join(ErrRepository, openErr, entryErr)
	}
	return f, nil
}
