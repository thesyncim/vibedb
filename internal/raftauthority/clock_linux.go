//go:build linux

package raftauthority

import (
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// bootTimeClock is the qualified production elapsed clock on Linux. Unlike
// CLOCK_MONOTONIC, CLOCK_BOOTTIME includes suspend intervals, so a stopped
// process cannot resume with an authority window that was longer than policy.
type bootTimeClock struct{}

// NewQualifiedElapsedClock returns the Linux suspend-aware clock. The caller
// still needs an explicit deployment policy; constructing this clock does not
// auto-enable read authority.
func NewQualifiedElapsedClock() (ElapsedClock, error) { return bootTimeClock{}, nil }

func (bootTimeClock) Now() (time.Duration, error) {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts); err != nil {
		return 0, fmt.Errorf("%w: CLOCK_BOOTTIME: %v", ErrClockUnavailable, err)
	}
	nanos := unix.TimespecToNsec(ts)
	if nanos < 0 {
		return 0, fmt.Errorf("%w: CLOCK_BOOTTIME returned %d", ErrClockFault, nanos)
	}
	return time.Duration(nanos), nil
}
