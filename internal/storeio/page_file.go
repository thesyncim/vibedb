package storeio

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

// ErrDirectIOUnsupported reports a platform or filesystem that cannot honor
// required direct page I/O.
var ErrDirectIOUnsupported = errors.New("vibejson: direct Store page I/O unsupported")

// DirectMode controls explicit direct-I/O admission. DirectTry falls back only
// when the platform or filesystem rejects direct I/O; unrelated open errors
// remain errors.
type DirectMode uint8

const (
	DirectOff DirectMode = iota
	DirectTry
	DirectRequire
)

// PageFileOptions configures an owned read-only file and its bounded cache.
type PageFileOptions struct {
	Cache  PageCacheOptions
	Direct DirectMode
}

// OpenPageCacheFile returns a descriptor suitable for PageCache reads. Off
// preserves the caller-owned descriptor. On Linux, Try and Require reopen the
// same inode through /proc/self/fd with O_DIRECT, producing an independent open
// file description so read policy cannot alter the writer's flags. The caller
// owns and must close a returned descriptor only when it differs from file.
func OpenPageCacheFile(file *os.File, mode DirectMode) (*os.File, bool, error) {
	if file == nil {
		return nil, false, fmt.Errorf("%w: nil file", ErrPageReference)
	}
	if mode > DirectRequire {
		return nil, false, fmt.Errorf("%w: direct mode %d", ErrPageReference, mode)
	}
	return openPageCacheFile(file, mode)
}

// OpenPageCommitFile returns a descriptor suitable for Device writes. Off
// preserves the caller-owned descriptor. On Linux, Try and Require reopen the
// same inode through /proc/self/fd with O_DIRECT. The returned open file
// description is independent, so the direct policy cannot alter the caller's
// flags or offset. The caller owns and must close a returned descriptor only
// when it differs from file.
func OpenPageCommitFile(file *os.File, mode DirectMode) (*os.File, bool, error) {
	if file == nil {
		return nil, false, fmt.Errorf("%w: nil file", ErrInvalidWrite)
	}
	if mode > DirectRequire {
		return nil, false, fmt.Errorf("%w: direct mode %d", ErrInvalidWrite, mode)
	}
	return openPageCommitFile(file, mode)
}

// PageFile owns a read-only file and its PageCache. Direct reports whether the
// file was actually opened with explicit direct-I/O semantics.
type PageFile struct {
	file      *os.File
	cache     *PageCache
	direct    bool
	closeOnce sync.Once
	closeErr  error
}

// OpenPageFile opens path read-only and constructs its bounded page cache.
// DirectTry is explicit, observable fallback; DirectRequire never silently
// changes I/O semantics. The cache's anonymous frame arena satisfies the
// alignment and lifetime requirements of Linux O_DIRECT reads.
func OpenPageFile(path string, options PageFileOptions) (*PageFile, error) {
	if options.Direct > DirectRequire {
		return nil, fmt.Errorf("%w: direct mode %d", ErrPageReference, options.Direct)
	}
	file, direct, err := openPageFile(path, options.Direct)
	if err != nil {
		return nil, err
	}
	cache, err := NewPageCache(file, options.Cache)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	return &PageFile{file: file, cache: cache, direct: direct}, nil
}

// Cache returns the owned bounded cache. It becomes invalid when PageFile is
// closed.
func (f *PageFile) Cache() *PageCache {
	if f == nil {
		return nil
	}
	return f.cache
}

// Direct reports whether direct I/O is active rather than merely requested.
func (f *PageFile) Direct() bool { return f != nil && f.direct }

// Close atomically stops admission, waits for page leases, releases cache
// frames, and closes the file. Concurrent calls are safe and return one result.
func (f *PageFile) Close() error {
	if f == nil {
		return nil
	}
	f.closeOnce.Do(func() {
		// Keep both pointers immutable. A reader that acquired PageFile just
		// before Close can safely observe the cache's closing state; the cache
		// rejects new admission and drains in-flight loads on the first call.
		// PageCache.Close deliberately reports a live caller-owned lease
		// instead of blocking; PageFile owns both resources, so it must wait
		// until those leases drain before releasing the arena or descriptor.
		//
		// The retry is entirely on the cold close path. Yield first so a
		// racing short-lived view can release immediately, then back off to
		// avoid burning a core when an application intentionally holds a view
		// for longer. Read, pin, and release paths pay no extra synchronization.
		for retry := 0; ; retry++ {
			cacheErr := f.cache.Close()
			if !errors.Is(cacheErr, ErrPageCachePinned) {
				f.closeErr = errors.Join(cacheErr, f.file.Close())
				break
			}
			if retry < 16 {
				runtime.Gosched()
				continue
			}
			time.Sleep(time.Millisecond)
		}
	})
	return f.closeErr
}
