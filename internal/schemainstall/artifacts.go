package schemainstall

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/thesyncim/vibedb/internal/storeio"
)

// Activator is the serving-runtime-specific generation swap. DirectoryBackend
// deliberately does not guess how a live SQL machine is fenced or replaced.
// The implementation must atomically exchange generations and retain old
// readers until DrainOld; this narrow boundary keeps artifact IO off the Raft
// owner lane without weakening activation semantics.
type Activator interface {
	// Stage must materialize the exact target relation/checkpoint images off the
	// serving path. Its witness is folded into the prepared receipt, so a shard
	// cannot claim readiness from the opaque bundle file alone.
	ObserveStaged(context.Context, Request, [32]byte, string) (witness [32]byte, found bool, err error)
	Stage(context.Context, Request, [32]byte, string) (witness [32]byte, err error)
	ObserveActive(context.Context, Request, Authorization, [32]byte, string) (bool, error)
	Activate(context.Context, Request, Authorization, [32]byte, string) error
	ObserveDrained(context.Context, Request, Authorization, DrainProof, [32]byte) (bool, error)
	DrainOld(context.Context, Request, Authorization, DrainProof, [32]byte) error
}

type DirectoryOptions struct {
	Path         string
	MaxArtifacts int
	MaxDiskBytes uint64
	Activator    Activator
}

// DirectoryBackend stores immutable target bundles with create-fsync-rename-
// directory-fsync ordering. Recovery rejects unknown names, links, malformed
// sizes, and over-bound retained space rather than silently deleting evidence.
type DirectoryBackend struct {
	mu           sync.Mutex
	root         *os.Root
	lock         *os.File
	maxArtifacts int
	maxDiskBytes uint64
	artifacts    map[[32]byte]artifactMeta
	activator    Activator
	closed       bool
}

type artifactMeta struct {
	bytes  uint64
	digest [32]byte
}

func OpenDirectoryBackend(options DirectoryOptions) (*DirectoryBackend, error) {
	if options.Path == "" || options.MaxArtifacts <= 0 || options.MaxArtifacts > AbsoluteMaxRecords ||
		options.MaxDiskBytes == 0 || options.Activator == nil {
		return nil, ErrInvalid
	}
	path := filepath.Clean(options.Path)
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(ErrInvalid, err)
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	lock, err := openRegular(root, "artifacts.lock", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	if err = storeio.LockWriter(lock); err != nil {
		_ = lock.Close()
		_ = root.Close()
		return nil, err
	}
	backend := &DirectoryBackend{root: root, lock: lock, maxArtifacts: options.MaxArtifacts,
		maxDiskBytes: options.MaxDiskBytes, artifacts: make(map[[32]byte]artifactMeta, options.MaxArtifacts),
		activator: options.Activator}
	if err = backend.recover(); err != nil {
		_ = backend.Close()
		return nil, err
	}
	return backend, nil
}

func (backend *DirectoryBackend) ObservePrepared(
	ctx context.Context, request Request,
) ([32]byte, bool, error) {
	if backend == nil || ctx == nil || !validRequest(request) {
		return [32]byte{}, false, ErrInvalid
	}
	if cause := context.Cause(ctx); cause != nil {
		return [32]byte{}, false, cause
	}
	backend.mu.Lock()
	if backend.closed {
		backend.mu.Unlock()
		return [32]byte{}, false, ErrClosed
	}
	meta, found := backend.artifacts[request.Operation]
	if !found {
		backend.mu.Unlock()
		return [32]byte{}, false, nil
	}
	if meta.bytes != request.BundleBytes || meta.digest != request.BundleDigest {
		backend.mu.Unlock()
		return [32]byte{}, false, ErrConflict
	}
	name := artifactName(request.Operation)
	digest, err := backend.digestLocked(request.Operation, request.BundleBytes)
	backend.mu.Unlock()
	if err != nil || digest != meta.digest {
		return [32]byte{}, false, errors.Join(ErrConflict, err)
	}
	witness, staged, err := backend.activator.ObserveStaged(ctx, request, meta.digest, name)
	if err != nil || !staged {
		return [32]byte{}, staged, err
	}
	materialized := MaterializedArtifactDigest(meta.digest, witness)
	if materialized == ([32]byte{}) {
		return [32]byte{}, false, ErrInvalid
	}
	return materialized, true, nil
}

func (backend *DirectoryBackend) Prepare(
	ctx context.Context, request Request, bundle []byte,
) ([32]byte, error) {
	if backend == nil || ctx == nil || !validRequest(request) ||
		uint64(len(bundle)) != request.BundleBytes || sha256.Sum256(bundle) != request.BundleDigest {
		return [32]byte{}, ErrInvalid
	}
	if cause := context.Cause(ctx); cause != nil {
		return [32]byte{}, cause
	}
	digest, name, err := backend.prepareArtifact(ctx, request, bundle)
	if err != nil {
		return [32]byte{}, err
	}
	witness, found, err := backend.activator.ObserveStaged(ctx, request, digest, name)
	if err != nil {
		return [32]byte{}, err
	}
	if !found {
		witness, err = backend.activator.Stage(ctx, request, digest, name)
		if err != nil {
			observed, observedFound, observeErr := backend.activator.ObserveStaged(
				ctx, request, digest, name,
			)
			if observeErr != nil || !observedFound {
				return [32]byte{}, errors.Join(ErrOutcomeUnknown, err, observeErr)
			}
			witness = observed
		}
	}
	materialized := MaterializedArtifactDigest(digest, witness)
	if materialized == ([32]byte{}) {
		return [32]byte{}, ErrInvalid
	}
	return materialized, nil
}

func (backend *DirectoryBackend) prepareArtifact(
	ctx context.Context, request Request, bundle []byte,
) ([32]byte, string, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.closed {
		return [32]byte{}, "", ErrClosed
	}
	if meta, found := backend.artifacts[request.Operation]; found {
		if meta.bytes != request.BundleBytes || meta.digest != request.BundleDigest {
			return [32]byte{}, "", ErrConflict
		}
		digest, err := backend.digestLocked(request.Operation, request.BundleBytes)
		if err != nil || digest != meta.digest {
			return [32]byte{}, "", errors.Join(ErrConflict, err)
		}
		return meta.digest, artifactName(request.Operation), nil
	}
	if len(backend.artifacts) == backend.maxArtifacts {
		return [32]byte{}, "", ErrBound
	}
	var live uint64
	for _, meta := range backend.artifacts {
		live += meta.bytes
	}
	if request.BundleBytes > backend.maxDiskBytes-live {
		return [32]byte{}, "", ErrBound
	}
	name := artifactName(request.Operation)
	temporary := "." + name + ".tmp"
	file, err := openRegular(backend.root, temporary, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return [32]byte{}, "", err
	}
	landed := false
	defer func() {
		_ = file.Close()
		if !landed {
			_ = backend.root.Remove(temporary)
		}
	}()
	if err = writeAll(file, bundle); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = backend.root.Rename(temporary, name)
	}
	if err == nil {
		err = syncRoot(backend.root)
	}
	if err != nil {
		return [32]byte{}, "", err
	}
	landed = true
	backend.artifacts[request.Operation] = artifactMeta{bytes: request.BundleBytes, digest: request.BundleDigest}
	return request.BundleDigest, name, nil
}

func (backend *DirectoryBackend) ObserveActive(ctx context.Context, request Request, authorization Authorization, installation [32]byte) (bool, error) {
	path, err := backend.artifactPath(request)
	if err != nil {
		return false, err
	}
	return backend.activator.ObserveActive(ctx, request, authorization, installation, path)
}

func (backend *DirectoryBackend) Activate(ctx context.Context, request Request, authorization Authorization, installation [32]byte) error {
	path, err := backend.artifactPath(request)
	if err != nil {
		return err
	}
	return backend.activator.Activate(ctx, request, authorization, installation, path)
}

func (backend *DirectoryBackend) ObserveDrained(ctx context.Context, request Request, authorization Authorization, proof DrainProof, installation [32]byte) (bool, error) {
	return backend.activator.ObserveDrained(ctx, request, authorization, proof, installation)
}

func (backend *DirectoryBackend) DrainOld(ctx context.Context, request Request, authorization Authorization, proof DrainProof, installation [32]byte) error {
	return backend.activator.DrainOld(ctx, request, authorization, proof, installation)
}

func (backend *DirectoryBackend) artifactPath(request Request) (string, error) {
	if backend == nil || !validRequest(request) {
		return "", ErrInvalid
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.closed {
		return "", ErrClosed
	}
	meta, found := backend.artifacts[request.Operation]
	if !found ||
		meta.bytes != request.BundleBytes || meta.digest != request.BundleDigest {
		return "", ErrMissing
	}
	digest, err := backend.digestLocked(request.Operation, request.BundleBytes)
	if err != nil || digest != meta.digest {
		return "", errors.Join(ErrConflict, err)
	}
	// os.Root.Name is intentionally unavailable. The activator receives a path
	// only after the opened artifact was revalidated, so retain the configured
	// absolute root separately would add no authority. Use /proc-free descriptor
	// reopening through OpenArtifact instead.
	return artifactName(request.Operation), nil
}

// OpenArtifact returns a bounded, already revalidated descriptor. Activators
// that need filesystem access should type-assert this backend and use this API;
// the string supplied to Activator is only the stable logical artifact name.
func (backend *DirectoryBackend) OpenArtifact(request Request) (*os.File, error) {
	if backend == nil || !validRequest(request) {
		return nil, ErrInvalid
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.closed {
		return nil, ErrClosed
	}
	meta, found := backend.artifacts[request.Operation]
	if !found || meta.bytes != request.BundleBytes || meta.digest != request.BundleDigest {
		return nil, ErrMissing
	}
	file, err := openRegular(backend.root, artifactName(request.Operation), os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	info, statErr := file.Stat()
	if statErr != nil || uint64(info.Size()) != request.BundleBytes {
		_ = file.Close()
		return nil, errors.Join(ErrConflict, statErr)
	}
	return file, nil
}

func (backend *DirectoryBackend) digestLocked(operation [32]byte, size uint64) ([32]byte, error) {
	file, err := openRegular(backend.root, artifactName(operation), os.O_RDONLY, 0)
	if err != nil {
		return [32]byte{}, err
	}
	defer file.Close()
	hasher := sha256.New()
	read, err := io.CopyN(hasher, file, int64(size))
	if err != nil || read != int64(size) {
		return [32]byte{}, errors.Join(ErrInvalid, err)
	}
	var trailing [1]byte
	count, trailingErr := file.Read(trailing[:])
	if count != 0 || !errors.Is(trailingErr, io.EOF) {
		return [32]byte{}, ErrInvalid
	}
	var digest [32]byte
	hasher.Sum(digest[:0])
	return digest, nil
}

func (backend *DirectoryBackend) recover() error {
	entries, err := fs.ReadDir(backend.root.FS(), ".")
	if err != nil {
		return err
	}
	var live uint64
	for _, entry := range entries {
		name := entry.Name()
		if name == "artifacts.lock" {
			continue
		}
		if len(name) > 1 && name[0] == '.' {
			if _, ok := openArtifactTemporaryName(name); !ok || entry.Type()&os.ModeSymlink != 0 {
				return ErrInvalid
			}
			if err = backend.root.Remove(name); err != nil {
				return err
			}
			continue
		}
		operation, ok := openArtifactName(name)
		if !ok || entry.Type()&os.ModeSymlink != 0 || entry.IsDir() {
			return ErrInvalid
		}
		if len(backend.artifacts) == backend.maxArtifacts {
			return ErrBound
		}
		file, openErr := openRegular(backend.root, name, os.O_RDONLY, 0)
		if openErr != nil {
			return openErr
		}
		info, statErr := file.Stat()
		closeErr := file.Close()
		if statErr != nil || closeErr != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > AbsoluteMaxBundleBytes {
			return errors.Join(ErrInvalid, statErr, closeErr)
		}
		size := uint64(info.Size())
		if size > backend.maxDiskBytes-live {
			return ErrBound
		}
		digest, digestErr := backend.digestLocked(operation, size)
		if digestErr != nil {
			return digestErr
		}
		live += size
		backend.artifacts[operation] = artifactMeta{bytes: size, digest: digest}
	}
	return syncRoot(backend.root)
}

func (backend *DirectoryBackend) Close() error {
	if backend == nil {
		return nil
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.closed {
		return nil
	}
	backend.closed = true
	err := storeio.UnlockWriter(backend.lock)
	return errors.Join(err, backend.lock.Close(), backend.root.Close())
}

func artifactName(operation [32]byte) string {
	var encoded [64]byte
	hex.Encode(encoded[:], operation[:])
	return "b-" + string(encoded[:])
}

func openArtifactName(name string) ([32]byte, bool) {
	var operation [32]byte
	if len(name) != 66 || name[:2] != "b-" {
		return operation, false
	}
	n, err := hex.Decode(operation[:], []byte(name[2:]))
	return operation, err == nil && n == len(operation) && operation != ([32]byte{}) && artifactName(operation) == name
}

func openArtifactTemporaryName(name string) ([32]byte, bool) {
	if len(name) != 72 || name[0] != '.' || name[len(name)-4:] != ".tmp" {
		return [32]byte{}, false
	}
	return openArtifactName(name[1 : len(name)-4])
}

var _ Backend = (*DirectoryBackend)(nil)
