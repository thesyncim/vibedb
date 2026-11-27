// Package resourceguard provides lock-free, bounded-interval process memory
// and filesystem free-space watermark admission control.
//
// Samples are refreshed at most once per SampleInterval using Go's lock-free
// runtime/metrics (/memory/classes/total:bytes, avoiding ReadMemStats STW
// pauses) and platform free-disk queries. Hot-path AdmitRead and AdmitWrite
// calls perform only atomic loads in steady state.
package resourceguard

import (
	"errors"
	"fmt"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrResourceWatermarkExceeded is the parent sentinel for all watermark rejections.
	ErrResourceWatermarkExceeded = errors.New("vibedb: resource watermark exceeded")
	// ErrMemoryWatermarkExceeded is returned when process memory exceeds MaxMemoryBytes.
	ErrMemoryWatermarkExceeded = fmt.Errorf("%w: memory ceiling exceeded", ErrResourceWatermarkExceeded)
	// ErrDiskWatermarkExceeded is returned when available disk space falls below MinFreeDiskBytes.
	ErrDiskWatermarkExceeded = fmt.Errorf("%w: disk free-space floor breached", ErrResourceWatermarkExceeded)
)

const defaultSampleInterval = 250 * time.Millisecond

// Options configures hard process memory and disk free-space watermarks.
// A zero limit disables that check.
type Options struct {
	MaxMemoryBytes   uint64
	DiskPath         string
	MinFreeDiskBytes uint64
	SampleInterval   time.Duration

	// Optional test hooks overriding the OS/runtime samplers.
	sampleMemory func() uint64
	sampleDisk   func(path string) (uint64, error)
}

// Snapshot reports the latest observed resource measurements and cumulative
// watermark rejections.
type Snapshot struct {
	MemoryBytes      uint64
	FreeDiskBytes    uint64
	MaxMemoryBytes   uint64
	MinFreeDiskBytes uint64
	MemoryTripped    bool
	DiskTripped      bool
	MemoryRejections uint64
	DiskRejections   uint64
}

// Guard enforces process memory and disk free-space admission ceilings with
// cached atomic sampling so hot-path checks incur no syscalls or STW pauses.
type Guard struct {
	maxMemoryBytes   uint64
	diskPath         string
	minFreeDiskBytes uint64
	sampleIntervalNS int64

	sampleMemory func() uint64
	sampleDisk   func(path string) (uint64, error)

	refreshMu   sync.Mutex
	lastSample  atomic.Int64
	memoryBytes atomic.Uint64
	freeDisk    atomic.Uint64
	memTripped  atomic.Bool
	diskTripped atomic.Bool
	memRejects  atomic.Uint64
	diskRejects atomic.Uint64
}

// New constructs a Guard and performs one initial sample so watermarks are
// enforced immediately.
func New(opts Options) *Guard {
	interval := opts.SampleInterval
	if interval <= 0 {
		interval = defaultSampleInterval
	}
	memFn := opts.sampleMemory
	if memFn == nil {
		memFn = defaultSampleMemoryBytes
	}
	diskFn := opts.sampleDisk
	if diskFn == nil {
		diskFn = freeDiskBytes
	}
	g := &Guard{
		maxMemoryBytes:   opts.MaxMemoryBytes,
		diskPath:         opts.DiskPath,
		minFreeDiskBytes: opts.MinFreeDiskBytes,
		sampleIntervalNS: interval.Nanoseconds(),
		sampleMemory:     memFn,
		sampleDisk:       diskFn,
	}
	g.Refresh()
	return g
}

// AdmitRead admits a read operation unless the process memory ceiling is
// currently tripped. Low disk space never blocks reads.
func (g *Guard) AdmitRead() error {
	if g == nil || g.maxMemoryBytes == 0 {
		return nil
	}
	g.maybeRefresh()
	if g.memTripped.Load() {
		g.memRejects.Add(1)
		return ErrMemoryWatermarkExceeded
	}
	return nil
}

// AdmitWrite admits a write operation unless either the process memory ceiling
// or the disk free-space floor is currently tripped.
func (g *Guard) AdmitWrite() error {
	if g == nil || (g.maxMemoryBytes == 0 && g.minFreeDiskBytes == 0) {
		return nil
	}
	g.maybeRefresh()
	if g.maxMemoryBytes > 0 && g.memTripped.Load() {
		g.memRejects.Add(1)
		return ErrMemoryWatermarkExceeded
	}
	if g.minFreeDiskBytes > 0 && g.diskTripped.Load() {
		g.diskRejects.Add(1)
		return ErrDiskWatermarkExceeded
	}
	return nil
}

// Refresh forces an immediate synchronous sample of configured resources.
func (g *Guard) Refresh() {
	if g == nil {
		return
	}
	g.refreshMu.Lock()
	defer g.refreshMu.Unlock()
	g.refreshLocked(time.Now().UnixNano())
}

// Snapshot returns the most recently sampled values and rejection counts.
func (g *Guard) Snapshot() Snapshot {
	if g == nil {
		return Snapshot{}
	}
	return Snapshot{
		MemoryBytes:      g.memoryBytes.Load(),
		FreeDiskBytes:    g.freeDisk.Load(),
		MaxMemoryBytes:   g.maxMemoryBytes,
		MinFreeDiskBytes: g.minFreeDiskBytes,
		MemoryTripped:    g.memTripped.Load(),
		DiskTripped:      g.diskTripped.Load(),
		MemoryRejections: g.memRejects.Load(),
		DiskRejections:   g.diskRejects.Load(),
	}
}

func (g *Guard) maybeRefresh() {
	now := time.Now().UnixNano()
	last := g.lastSample.Load()
	if now-last < g.sampleIntervalNS {
		return
	}
	if !g.refreshMu.TryLock() {
		return
	}
	defer g.refreshMu.Unlock()
	if now-g.lastSample.Load() < g.sampleIntervalNS {
		return
	}
	g.refreshLocked(now)
}

func (g *Guard) refreshLocked(nowNS int64) {
	if g.maxMemoryBytes > 0 {
		mem := g.sampleMemory()
		g.memoryBytes.Store(mem)
		g.memTripped.Store(mem > g.maxMemoryBytes)
	}
	if g.minFreeDiskBytes > 0 && g.diskPath != "" {
		if avail, err := g.sampleDisk(g.diskPath); err == nil {
			g.freeDisk.Store(avail)
			g.diskTripped.Store(avail < g.minFreeDiskBytes)
		}
	}
	g.lastSample.Store(nowNS)
}

func defaultSampleMemoryBytes() uint64 {
	samples := [1]metrics.Sample{{Name: "/memory/classes/total:bytes"}}
	metrics.Read(samples[:])
	if samples[0].Value.Kind() == metrics.KindUint64 {
		return samples[0].Value.Uint64()
	}
	return 0
}
