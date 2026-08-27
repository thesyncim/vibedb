package clusterbackup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"math"
	"os"
	"strconv"
	"sync"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/storeio"
)

var (
	ErrRepository = errors.New("clusterbackup: repository")
	ErrBound      = errors.New("clusterbackup: repository bound")
	ErrNotFound   = errors.New("clusterbackup: backup not found")
)

type RepositoryLimits struct {
	MaxBackups       int
	MaxArtifacts     int
	MaxArtifactBytes uint64
	MaxDiskBytes     uint64
}

type ArtifactInput struct {
	Reader io.Reader
}

type RepositoryStats struct {
	Backups          int
	Artifacts        int
	DiskBytes        uint64
	BackupCapacity   int
	ArtifactCapacity int
	DiskCapacity     uint64
}

type backupRecord struct {
	certificate Certificate
	rawBytes    uint64
}

type BackupRepository struct {
	mu        sync.RWMutex
	root      *os.Root
	lock      *os.File
	limits    RepositoryLimits
	records   map[[sha256.Size]byte]backupRecord
	artifacts int
	diskBytes uint64
	closed    bool
	failed    bool
	fault     func(repositoryFault) error
}

type repositoryFault uint8

const (
	faultAfterArtifactsSync repositoryFault = iota + 1
	faultAfterCertificateRename
	faultAfterReleaseRename
)

// PublishedArtifact is an independently opened exact artifact view. Closing
// the repository does not invalidate an already opened view.
type PublishedArtifact struct {
	file      *os.File
	remaining uint64
}

func (a *PublishedArtifact) Read(dst []byte) (int, error) {
	if a == nil || a.file == nil || a.remaining == 0 {
		return 0, io.EOF
	}
	if uint64(len(dst)) > a.remaining {
		dst = dst[:a.remaining]
	}
	n, err := a.file.Read(dst)
	a.remaining -= uint64(n)
	if err == nil && a.remaining == 0 {
		err = io.EOF
	}
	return n, err
}

func (a *PublishedArtifact) Close() error {
	if a == nil || a.file == nil {
		return nil
	}
	err := a.file.Close()
	a.file = nil
	a.remaining = 0
	return err
}

func OpenBackupRepository(path string, limits RepositoryLimits) (*BackupRepository, error) {
	return openBackupRepository(path, limits, nil)
}

func openBackupRepository(path string, limits RepositoryLimits, fault func(repositoryFault) error) (*BackupRepository, error) {
	if path == "" || limits.MaxBackups <= 0 || limits.MaxBackups > 4096 ||
		limits.MaxArtifacts <= 0 || limits.MaxArtifacts > AbsoluteMaxGroupCuts ||
		limits.MaxArtifactBytes == 0 || limits.MaxArtifactBytes > math.MaxInt64 ||
		limits.MaxDiskBytes < limits.MaxArtifactBytes || limits.MaxDiskBytes > math.MaxInt64 {
		return nil, ErrBound
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	lock, err := openBackupRegular(root, "repository.lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	if err = storeio.LockWriter(lock); err != nil {
		_ = lock.Close()
		_ = root.Close()
		return nil, errors.Join(ErrRepository, err)
	}
	r := &BackupRepository{root: root, lock: lock, limits: limits,
		records: make(map[[sha256.Size]byte]backupRecord, limits.MaxBackups), fault: fault}
	if err = r.recover(); err != nil {
		_ = r.Close()
		return nil, err
	}
	return r, nil
}

func digestText(digest [sha256.Size]byte) string {
	var raw [sha256.Size * 2]byte
	hex.Encode(raw[:], digest[:])
	return string(raw[:])
}

func certificateName(digest [sha256.Size]byte) string { return "c-" + digestText(digest) }
func deletingName(digest [sha256.Size]byte) string    { return "d-" + digestText(digest) }
func certificateTempName(digest [sha256.Size]byte) string {
	return "tc-" + digestText(digest)
}
func artifactName(digest [sha256.Size]byte, index int) string {
	return "a-" + digestText(digest) + "-" + strconv.FormatUint(uint64(index), 16)
}
func artifactTempName(digest [sha256.Size]byte, index int) string {
	return "ta-" + digestText(digest) + "-" + strconv.FormatUint(uint64(index), 16)
}

func parseDigestName(name string, prefix string) (digest [sha256.Size]byte, ok bool) {
	if len(name) != len(prefix)+sha256.Size*2 || name[:len(prefix)] != prefix {
		return digest, false
	}
	n, err := hex.Decode(digest[:], []byte(name[len(prefix):]))
	return digest, err == nil && n == sha256.Size && digestText(digest) == name[len(prefix):]
}

func parseArtifactName(name string, prefix string) (digest [sha256.Size]byte, index int, ok bool) {
	base := len(prefix) + sha256.Size*2
	if len(name) <= base+1 || name[:len(prefix)] != prefix || name[base] != '-' {
		return digest, 0, false
	}
	n, err := hex.Decode(digest[:], []byte(name[len(prefix):base]))
	if err != nil || n != sha256.Size || name[base+1] == '0' && len(name) != base+2 {
		return digest, 0, false
	}
	value, err := strconv.ParseUint(name[base+1:], 16, 31)
	return digest, int(value), err == nil && digestText(digest) == name[len(prefix):base] &&
		strconv.FormatUint(value, 16) == name[base+1:]
}

func (r *BackupRepository) recover() error {
	entries, err := fs.ReadDir(r.root.FS(), ".")
	if err != nil {
		return err
	}
	// Temporary files and artifacts without a published certificate are never
	// authoritative. Deletion markers are durable release commit points.
	deleting := make(map[[sha256.Size]byte]struct{})
	certificates := make(map[[sha256.Size]byte]struct{})
	for _, entry := range entries {
		if entry.Name() == "repository.lock" {
			continue
		}
		if entry.Type()&fs.ModeType != 0 {
			return ErrRepository
		}
		if digest, ok := parseDigestName(entry.Name(), "c-"); ok {
			certificates[digest] = struct{}{}
			continue
		}
		if digest, ok := parseDigestName(entry.Name(), "d-"); ok {
			deleting[digest] = struct{}{}
			continue
		}
		if _, ok := parseDigestName(entry.Name(), "tc-"); ok {
			if err := r.root.Remove(entry.Name()); err != nil {
				return err
			}
			continue
		}
		if _, _, ok := parseArtifactName(entry.Name(), "ta-"); ok {
			if err := r.root.Remove(entry.Name()); err != nil {
				return err
			}
			continue
		}
		if _, _, ok := parseArtifactName(entry.Name(), "a-"); !ok {
			return ErrRepository
		}
	}
	for digest := range deleting {
		if err := r.finishDelete(digest); err != nil {
			return err
		}
		delete(certificates, digest)
	}
	// Orphan artifacts are incomplete publication state.
	entries, err = fs.ReadDir(r.root.FS(), ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		digest, _, ok := parseArtifactName(entry.Name(), "a-")
		if ok {
			if _, live := certificates[digest]; !live {
				if err := r.root.Remove(entry.Name()); err != nil {
					return err
				}
			}
		}
	}
	if err := syncBackupRoot(r.root); err != nil {
		return err
	}
	for digest := range certificates {
		if err := r.loadCertificate(digest); err != nil {
			return err
		}
	}
	return nil
}

func (r *BackupRepository) loadCertificate(digest [sha256.Size]byte) error {
	file, err := openBackupRegular(r.root, certificateName(digest), os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	info, statErr := file.Stat()
	if statErr != nil || info.Size() < HeaderBytes+GroupCutBytes+TrailerBytes || info.Size() > AbsoluteMaxCertificateBytes {
		_ = file.Close()
		return errors.Join(ErrRepository, statErr)
	}
	raw := make([]byte, info.Size())
	_, readErr := io.ReadFull(file, raw)
	closeErr := file.Close()
	certificate, openErr := OpenCertificate(raw)
	if readErr != nil || closeErr != nil || openErr != nil || certificate.Digest != digest ||
		len(certificate.Groups) > r.limits.MaxArtifacts || len(r.records) >= r.limits.MaxBackups {
		return errors.Join(ErrRepository, readErr, closeErr, openErr)
	}
	bytes := uint64(len(raw))
	for index, cut := range certificate.Groups {
		if cut.ArtifactBytes > r.limits.MaxArtifactBytes {
			return ErrBound
		}
		artifact, err := openBackupRegular(r.root, artifactName(digest, index), os.O_RDONLY, 0)
		if err != nil {
			return errors.Join(ErrRepository, err)
		}
		actual, verifyErr := verifyArtifact(artifact, cut)
		closeErr = artifact.Close()
		if verifyErr != nil || closeErr != nil || actual != cut.ArtifactBytes {
			return errors.Join(ErrRepository, verifyErr, closeErr)
		}
		bytes += actual
	}
	if r.artifacts > r.limits.MaxArtifacts-len(certificate.Groups) || bytes > r.limits.MaxDiskBytes-r.diskBytes {
		return ErrBound
	}
	r.records[digest] = backupRecord{certificate: certificate, rawBytes: bytes}
	r.artifacts += len(certificate.Groups)
	r.diskBytes += bytes
	return nil
}

func verifyArtifact(file *os.File, cut GroupCut) (uint64, error) {
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() < 0 || uint64(info.Size()) != cut.ArtifactBytes {
		return 0, errors.Join(ErrArtifactEvidence, err)
	}
	hash := sha256.New()
	n, err := io.Copy(hash, file)
	if err != nil || uint64(n) != cut.ArtifactBytes {
		return uint64(n), errors.Join(ErrArtifactEvidence, err)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	if digest != cut.ArtifactHash {
		return uint64(n), ErrArtifactEvidence
	}
	return uint64(n), nil
}

// Publish streams and authenticates every artifact before atomically making
// the certificate visible. Repeating an already committed publication is
// idempotent and performs no writes.
func (r *BackupRepository) Publish(certificate Certificate, artifacts ...ArtifactInput) error {
	if r == nil {
		return ErrRepository
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.failed {
		return ErrRepository
	}
	raw, err := AppendCertificate(nil, certificate)
	if err != nil || certificate.Digest == ([sha256.Size]byte{}) ||
		certificate.Digest != sha256.Sum256(raw[:len(raw)-TrailerBytes]) || len(artifacts) != len(certificate.Groups) {
		return errors.Join(ErrCertificate, err)
	}
	if _, exists := r.records[certificate.Digest]; exists {
		return nil
	}
	if len(r.records) >= r.limits.MaxBackups || len(artifacts) > r.limits.MaxArtifacts-r.artifacts {
		return ErrBound
	}
	need := uint64(len(raw))
	for _, cut := range certificate.Groups {
		if cut.ArtifactBytes > r.limits.MaxArtifactBytes || cut.ArtifactBytes > r.limits.MaxDiskBytes-need {
			return ErrBound
		}
		need += cut.ArtifactBytes
	}
	if need > r.limits.MaxDiskBytes-r.diskBytes {
		return ErrBound
	}
	cleanup := true
	defer func() {
		if cleanup {
			for index := range artifacts {
				_ = r.root.Remove(artifactTempName(certificate.Digest, index))
				_ = r.root.Remove(artifactName(certificate.Digest, index))
			}
			_ = r.root.Remove(certificateTempName(certificate.Digest))
		}
	}()
	for index, input := range artifacts {
		if input.Reader == nil {
			return ErrArtifactEvidence
		}
		name := artifactTempName(certificate.Digest, index)
		file, err := openBackupRegular(r.root, name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		hash := sha256.New()
		written, copyErr := copyExact(io.MultiWriter(file, hash), input.Reader, certificate.Groups[index].ArtifactBytes)
		syncErr := file.Sync()
		closeErr := file.Close()
		var digest [sha256.Size]byte
		copy(digest[:], hash.Sum(nil))
		if copyErr != nil || syncErr != nil || closeErr != nil || written != certificate.Groups[index].ArtifactBytes || digest != certificate.Groups[index].ArtifactHash {
			return errors.Join(ErrArtifactEvidence, copyErr, syncErr, closeErr)
		}
		if err = replaceBackupEntry(r.root, name, artifactName(certificate.Digest, index)); err != nil {
			return err
		}
	}
	if err = syncBackupRoot(r.root); err != nil {
		return err
	}
	if r.fault != nil {
		if err = r.fault(faultAfterArtifactsSync); err != nil {
			return err
		}
	}
	certificateTemp := certificateTempName(certificate.Digest)
	file, err := openBackupRegular(r.root, certificateTemp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	writeErr := writeBackupFull(file, raw)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return errors.Join(writeErr, syncErr, closeErr)
	}
	if err = replaceBackupEntry(r.root, certificateTemp, certificateName(certificate.Digest)); err != nil {
		return err
	}
	cleanup = false // The certificate is now the commit point; recovery owns it.
	if r.fault != nil {
		if err = r.fault(faultAfterCertificateRename); err != nil {
			r.failed = true
			return err
		}
	}
	if err = syncBackupRoot(r.root); err != nil {
		r.failed = true
		return err
	}
	r.records[certificate.Digest] = backupRecord{certificate: certificate, rawBytes: need}
	r.artifacts += len(artifacts)
	r.diskBytes += need
	return nil
}

func copyExact(dst io.Writer, src io.Reader, size uint64) (uint64, error) {
	n, err := io.CopyN(dst, src, int64(size))
	if err != nil {
		return uint64(n), err
	}
	var extra [1]byte
	extraN, extraErr := src.Read(extra[:])
	if extraN != 0 || extraErr == nil {
		return uint64(n), ErrArtifactEvidence
	}
	if !errors.Is(extraErr, io.EOF) {
		return uint64(n), extraErr
	}
	return uint64(n), nil
}

func (r *BackupRepository) Certificate(digest [sha256.Size]byte) (Certificate, error) {
	if r == nil {
		return Certificate{}, ErrRepository
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.records[digest]
	if r.closed || r.failed || !ok {
		return Certificate{}, ErrNotFound
	}
	result := record.certificate
	result.Groups = append([]GroupCut(nil), result.Groups...)
	return result, nil
}

func (r *BackupRepository) OpenArtifact(digest [sha256.Size]byte, index int) (*PublishedArtifact, error) {
	if r == nil {
		return nil, ErrRepository
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	record, ok := r.records[digest]
	if r.closed || r.failed || !ok || index < 0 || index >= len(record.certificate.Groups) {
		return nil, ErrNotFound
	}
	file, err := openBackupRegular(r.root, artifactName(digest, index), os.O_RDONLY, 0)
	if err != nil {
		return nil, errors.Join(ErrRepository, err)
	}
	if _, err = verifyArtifact(file, record.certificate.Groups[index]); err != nil {
		_ = file.Close()
		return nil, errors.Join(ErrRepository, err)
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &PublishedArtifact{file: file, remaining: record.certificate.Groups[index].ArtifactBytes}, nil
}

// OpenBackupArtifact implements ArtifactSource without allowing a restore
// caller to select a path or a "latest" file. Operation and complete portable
// group identity must resolve to exactly one published certificate entry;
// ambiguity fails closed.
func (r *BackupRepository) OpenBackupArtifact(ctx context.Context, operation [sha256.Size]byte,
	group raftmember.GroupKey,
) (io.ReadCloser, error) {
	if r == nil || ctx == nil || operation == ([sha256.Size]byte{}) || !validGroup(group) {
		return nil, ErrRepository
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed || r.failed {
		return nil, ErrRepository
	}
	var digest [sha256.Size]byte
	index, matches := 0, 0
	for candidateDigest, record := range r.records {
		if record.certificate.Operation != operation {
			continue
		}
		for ordinal, cut := range record.certificate.Groups {
			if cut.Group == group {
				digest, index = candidateDigest, ordinal
				matches++
			}
		}
	}
	if matches != 1 {
		return nil, ErrNotFound
	}
	record := r.records[digest]
	file, err := openBackupRegular(r.root, artifactName(digest, index), os.O_RDONLY, 0)
	if err != nil {
		return nil, errors.Join(ErrRepository, err)
	}
	if _, err = verifyArtifact(file, record.certificate.Groups[index]); err != nil {
		_ = file.Close()
		return nil, errors.Join(ErrRepository, err)
	}
	if _, err = file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &PublishedArtifact{file: file, remaining: record.certificate.Groups[index].ArtifactBytes}, nil
}

// Release durably removes publication authority before reclaiming bytes.
func (r *BackupRepository) Release(digest [sha256.Size]byte) error {
	if r == nil {
		return ErrRepository
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || r.failed {
		return ErrRepository
	}
	record, ok := r.records[digest]
	if !ok {
		return ErrNotFound
	}
	if err := replaceBackupEntry(r.root, certificateName(digest), deletingName(digest)); err != nil {
		return err
	}
	if err := syncBackupRoot(r.root); err != nil {
		r.failed = true
		return err
	}
	delete(r.records, digest)
	if r.fault != nil {
		if err := r.fault(faultAfterReleaseRename); err != nil {
			r.failed = true
			return err
		}
	}
	if err := r.finishDelete(digest); err != nil {
		r.failed = true
		return err
	}
	r.artifacts -= len(record.certificate.Groups)
	r.diskBytes -= record.rawBytes
	return nil
}

func (r *BackupRepository) finishDelete(digest [sha256.Size]byte) error {
	entries, err := fs.ReadDir(r.root.FS(), ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		artifactDigest, _, ok := parseArtifactName(entry.Name(), "a-")
		if ok && artifactDigest == digest {
			if err := r.root.Remove(entry.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
	}
	if err := r.root.Remove(deletingName(digest)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncBackupRoot(r.root)
}

func (r *BackupRepository) Stats() RepositoryStats {
	if r == nil {
		return RepositoryStats{}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return RepositoryStats{Backups: len(r.records), Artifacts: r.artifacts, DiskBytes: r.diskBytes,
		BackupCapacity: r.limits.MaxBackups, ArtifactCapacity: r.limits.MaxArtifacts,
		DiskCapacity: r.limits.MaxDiskBytes}
}

func (r *BackupRepository) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	err := errors.Join(storeio.UnlockWriter(r.lock), r.lock.Close(), r.root.Close())
	r.lock, r.root = nil, nil
	return err
}

func openBackupRegular(root *os.Root, name string, flags int, mode os.FileMode) (*os.File, error) {
	var before os.FileInfo
	var err error
	if flags&os.O_CREATE == 0 {
		before, err = root.Lstat(name)
		if err != nil || !before.Mode().IsRegular() {
			return nil, errors.Join(ErrRepository, err)
		}
	}
	file, err := root.OpenFile(name, flags, mode)
	if err != nil {
		return nil, err
	}
	opened, openErr := file.Stat()
	entry, entryErr := root.Lstat(name)
	stable := openErr == nil && entryErr == nil && opened.Mode().IsRegular() && entry.Mode().IsRegular() && os.SameFile(opened, entry)
	if before != nil {
		stable = stable && os.SameFile(before, entry)
	}
	if !stable {
		_ = file.Close()
		return nil, errors.Join(ErrRepository, openErr, entryErr)
	}
	return file, nil
}

func writeBackupFull(dst io.Writer, raw []byte) error {
	for len(raw) != 0 {
		n, err := dst.Write(raw)
		if n < 0 || n > len(raw) {
			return io.ErrShortWrite
		}
		raw = raw[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
