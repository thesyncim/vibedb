package clusterbackup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/thesyncim/vibedb/internal/raftmember"
)

const RestoreStagingPermitBytes = 184

var (
	ErrRestoreStagingRoot = errors.New("clusterbackup: restore staging root")
	restorePermitMagic    = [8]byte{'V', 'B', 'R', 'S', 'T', 'A', 'G', 'E'}
)

// RestoreStagingRoot is an authenticated, explicitly non-serving artifact
// root. It carries no database handles, Raft membership, store identity,
// ownership epoch, route, or grant.
type RestoreStagingRoot struct {
	Permit     RestoreStagingPermit
	Repository *BackupRepository
	path       string
}

func AppendRestoreStagingPermit(dst []byte, permit RestoreStagingPermit) ([]byte, error) {
	if permit.Restore == ([sha256.Size]byte{}) || permit.CertificateDigest == ([sha256.Size]byte{}) ||
		permit.CatalogGeneration == 0 || permit.CatalogDigest == ([sha256.Size]byte{}) ||
		permit.TargetClusterID == ([16]byte{}) || permit.TargetClusterIncarnation == ([16]byte{}) ||
		permit.Groups == 0 {
		return dst, ErrRestoreStagingRoot
	}
	start := len(dst)
	dst = append(dst, make([]byte, RestoreStagingPermitBytes)...)
	raw := dst[start:]
	copy(raw[:8], restorePermitMagic[:])
	copy(raw[8:40], permit.Restore[:])
	copy(raw[40:72], permit.CertificateDigest[:])
	binary.BigEndian.PutUint64(raw[72:80], permit.CatalogGeneration)
	copy(raw[80:112], permit.CatalogDigest[:])
	copy(raw[112:128], permit.TargetClusterID[:])
	copy(raw[128:144], permit.TargetClusterIncarnation[:])
	binary.BigEndian.PutUint32(raw[144:148], permit.Groups)
	digest := sha256Sum(raw[:152])
	copy(raw[152:], digest[:])
	return dst, nil
}

func OpenRestoreStagingPermit(raw []byte) (RestoreStagingPermit, error) {
	if len(raw) != RestoreStagingPermitBytes || [8]byte(raw[:8]) != restorePermitMagic ||
		binary.BigEndian.Uint32(raw[148:152]) != 0 || sha256Sum(raw[:152]) != [sha256.Size]byte(raw[152:]) {
		return RestoreStagingPermit{}, ErrRestoreStagingRoot
	}
	permit := RestoreStagingPermit{CatalogGeneration: binary.BigEndian.Uint64(raw[72:80]),
		Groups: binary.BigEndian.Uint32(raw[144:148])}
	copy(permit.Restore[:], raw[8:40])
	copy(permit.CertificateDigest[:], raw[40:72])
	copy(permit.CatalogDigest[:], raw[80:112])
	copy(permit.TargetClusterID[:], raw[112:128])
	copy(permit.TargetClusterIncarnation[:], raw[128:144])
	if _, err := AppendRestoreStagingPermit(nil, permit); err != nil {
		return RestoreStagingPermit{}, err
	}
	return permit, nil
}

func sha256Sum(raw []byte) [sha256.Size]byte { return sha256.Sum256(raw) }

type lazyArtifactInput struct {
	ctx       context.Context
	source    ArtifactSource
	operation [sha256.Size]byte
	group     raftmember.GroupKey
	opened    io.ReadCloser
	finished  bool
}

func (input *lazyArtifactInput) Read(dst []byte) (int, error) {
	if input.finished {
		return 0, io.EOF
	}
	if input.opened == nil {
		reader, err := input.source.OpenBackupArtifact(input.ctx, input.operation, input.group)
		if err != nil {
			return 0, err
		}
		input.opened = reader
	}
	n, err := input.opened.Read(dst)
	if errors.Is(err, io.EOF) {
		closeErr := input.opened.Close()
		input.opened = nil
		input.finished = true
		return n, errors.Join(err, closeErr)
	}
	return n, err
}

func (input *lazyArtifactInput) close() error {
	if input.opened == nil {
		return nil
	}
	err := input.opened.Close()
	input.opened = nil
	return err
}

// BuildRestoreStagingRoot copies one already verified complete vector into an
// isolated root. At most one source artifact is open at once. The fixed permit
// is renamed only after the artifact repository and its directory are durable.
// Replaying after a crash is idempotent.
func BuildRestoreStagingRoot(ctx context.Context, path string, limits RepositoryLimits,
	certificate Certificate, permit RestoreStagingPermit, source ArtifactSource,
) (*RestoreStagingRoot, error) {
	if ctx == nil || source == nil || path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path ||
		permit.CertificateDigest != certificate.Digest || permit.CatalogGeneration != certificate.CatalogGeneration ||
		permit.CatalogDigest != certificate.CatalogDigest || permit.Groups != uint32(len(certificate.Groups)) {
		return nil, ErrRestoreStagingRoot
	}
	if len(certificate.Groups) == 0 ||
		permit.TargetClusterID == certificate.Groups[0].Group.ClusterID &&
			permit.TargetClusterIncarnation == certificate.Groups[0].Group.ClusterIncarnation {
		return nil, ErrRestoreStagingRoot
	}
	if _, err := AppendRestoreStagingPermit(nil, permit); err != nil {
		return nil, err
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if err := ensureStagingDirectory(path); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if err = validateStagingEntries(root); err != nil {
		return nil, err
	}
	repository, err := OpenBackupRepository(filepath.Join(path, "artifacts"), limits)
	if err != nil {
		return nil, err
	}
	inputs := make([]lazyArtifactInput, len(certificate.Groups))
	artifacts := make([]ArtifactInput, len(certificate.Groups))
	for index, cut := range certificate.Groups {
		inputs[index] = lazyArtifactInput{ctx: ctx, source: source, operation: certificate.Operation, group: cut.Group}
		artifacts[index].Reader = &inputs[index]
	}
	if err = repository.Publish(certificate, artifacts...); err != nil {
		for index := range inputs {
			err = errors.Join(err, inputs[index].close())
		}
		_ = repository.Close()
		return nil, errors.Join(ErrRestoreStagingRoot, err)
	}
	for index := range inputs {
		if closeErr := inputs[index].close(); closeErr != nil {
			_ = repository.Close()
			return nil, errors.Join(ErrRestoreStagingRoot, closeErr)
		}
	}
	raw, err := AppendRestoreStagingPermit(nil, permit)
	if err != nil {
		_ = repository.Close()
		return nil, err
	}
	if err = publishStagingPermit(root, raw); err != nil {
		_ = repository.Close()
		return nil, err
	}
	return &RestoreStagingRoot{Permit: permit, Repository: repository, path: path}, nil
}

func OpenRestoreStagingRoot(path string, limits RepositoryLimits) (*RestoreStagingRoot, error) {
	if err := ensureStagingDirectory(path); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	if err = validateStagingEntries(root); err != nil {
		return nil, err
	}
	file, err := openBackupRegular(root, "permit", os.O_RDONLY, 0)
	if err != nil {
		return nil, errors.Join(ErrRestoreStagingRoot, err)
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, RestoreStagingPermitBytes+1))
	closeErr := file.Close()
	permit, permitErr := OpenRestoreStagingPermit(raw)
	if readErr != nil || closeErr != nil || permitErr != nil {
		return nil, errors.Join(ErrRestoreStagingRoot, readErr, closeErr, permitErr)
	}
	repository, err := OpenBackupRepository(filepath.Join(path, "artifacts"), limits)
	if err != nil {
		return nil, err
	}
	certificate, err := repository.Certificate(permit.CertificateDigest)
	if err != nil || certificate.CatalogGeneration != permit.CatalogGeneration ||
		certificate.CatalogDigest != permit.CatalogDigest || len(certificate.Groups) != int(permit.Groups) {
		_ = repository.Close()
		return nil, errors.Join(ErrRestoreStagingRoot, err)
	}
	return &RestoreStagingRoot{Permit: permit, Repository: repository, path: path}, nil
}

func (root *RestoreStagingRoot) Close() error {
	if root == nil || root.Repository == nil {
		return nil
	}
	err := root.Repository.Close()
	root.Repository = nil
	return err
}

func ensureStagingDirectory(path string) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return ErrRestoreStagingRoot
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err = os.Mkdir(path, 0o700); err != nil {
			return err
		}
		info, err = os.Lstat(path)
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.Join(ErrRestoreStagingRoot, err)
	}
	return nil
}

func validateStagingEntries(root *os.Root) error {
	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == "artifacts" && entry.IsDir() ||
			(entry.Name() == "permit" || entry.Name() == "permit.tmp") && entry.Type().IsRegular() {
			continue
		}
		return ErrRestoreStagingRoot
	}
	return nil
}

func publishStagingPermit(root *os.Root, raw []byte) error {
	if existing, err := openBackupRegular(root, "permit", os.O_RDONLY, 0); err == nil {
		got, readErr := io.ReadAll(io.LimitReader(existing, RestoreStagingPermitBytes+1))
		closeErr := existing.Close()
		if readErr == nil && closeErr == nil && bytes.Equal(got, raw) {
			return nil
		}
		return errors.Join(ErrRestoreStagingRoot, readErr, closeErr)
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.Join(ErrRestoreStagingRoot, err)
	}
	_ = root.Remove("permit.tmp")
	file, err := openBackupRegular(root, "permit.tmp", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	writeErr := writeBackupFull(file, raw)
	syncErr := file.Sync()
	closeErr := file.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return errors.Join(ErrRestoreStagingRoot, writeErr, syncErr, closeErr)
	}
	if err = replaceBackupEntry(root, "permit.tmp", "permit"); err != nil {
		return err
	}
	return syncBackupRoot(root)
}
