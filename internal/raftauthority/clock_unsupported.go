//go:build !linux

package raftauthority

// NewQualifiedElapsedClock deliberately refuses unsupported platforms. The
// ReadIndex path remains the required fallback until a platform-specific
// suspend-aware implementation and qualification evidence exist.
func NewQualifiedElapsedClock() (ElapsedClock, error) { return nil, ErrClockUnavailable }
