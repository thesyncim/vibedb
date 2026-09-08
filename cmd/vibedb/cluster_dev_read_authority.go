package main

import (
	"errors"
	"fmt"

	"github.com/thesyncim/vibedb/internal/raftauthority"
)

// newDevQualifiedElapsedClock is a narrow seam for deterministic launcher
// tests. Production uses the platform-qualified CLOCK_BOOTTIME source (or the
// explicit unsupported-platform implementation) directly.
var newDevQualifiedElapsedClock = raftauthority.NewQualifiedElapsedClock

// devReadAuthorityClockQualified performs the same constructor-plus-first
// sample that a serving shard performs before it creates or repairs a durable
// authority marker. ErrClockUnavailable means this platform keeps a fresh
// deployment on ReadIndex; faults and all other errors must remain visible.
func devReadAuthorityClockQualified() (bool, error) {
	source, err := newDevQualifiedElapsedClock()
	if err != nil {
		if errors.Is(err, raftauthority.ErrClockUnavailable) && !errors.Is(err, raftauthority.ErrClockFault) {
			return false, nil
		}
		return false, err
	}
	if source == nil {
		return false, raftauthority.ErrClockUnavailable
	}
	if sample, err := source.Now(); err != nil {
		if errors.Is(err, raftauthority.ErrClockUnavailable) && !errors.Is(err, raftauthority.ErrClockFault) {
			return false, nil
		}
		return false, err
	} else if sample < 0 {
		return false, fmt.Errorf("%w: negative elapsed-clock sample %s", raftauthority.ErrClockFault, sample)
	}
	return true, nil
}

// resolveDevReadAuthority applies the durable policy selection rules before
// any existing physical manifest or fresh child artifact can be reconciled.
// A retained section is authoritative when the flag is omitted; an explicit
// flag must match its presence exactly. Fresh RF3 physical clusters opt in
// only after the platform clock has passed its first sample.
func resolveDevReadAuthority(options devClusterOptions, retained *devClusterManifest) (devClusterOptions, error) {
	// Direct callers historically represented an explicit true request with the
	// bool alone. Treat it as set while preserving explicit --read-authority=false
	// through the separate readAuthoritySet bit.
	if options.readAuthority {
		options.readAuthoritySet = true
	}
	if retained != nil {
		recorded := retained.ReadAuthority != nil
		if options.readAuthoritySet && options.readAuthority != recorded {
			return options, fmt.Errorf("%w: read authority flag differs from retained cluster policy", errDevCluster)
		}
		options.readAuthority = recorded
		if !recorded {
			return options, nil
		}
		if !validDevReadAuthority(*retained.ReadAuthority) {
			return options, fmt.Errorf("%w: retained read authority policy is invalid", errDevCluster)
		}
		qualified, err := devReadAuthorityClockQualified()
		if err != nil {
			return options, errors.Join(errDevCluster, err)
		}
		if !qualified {
			return options, fmt.Errorf("%w: retained read authority requires a qualified elapsed clock", errDevCluster)
		}
		return options, nil
	}
	if !options.readAuthoritySet {
		eligible := options.replicas == devClusterRF3 &&
			(options.physicalNodes == devClusterPhysicalNodes3 || options.physicalNodes == devClusterPhysicalNodes6)
		if !eligible {
			options.readAuthority = false
			return options, nil
		}
		qualified, err := devReadAuthorityClockQualified()
		if err != nil {
			return options, errors.Join(errDevCluster, err)
		}
		options.readAuthority = qualified
		return options, nil
	}
	if !options.readAuthority {
		return options, nil
	}
	qualified, err := devReadAuthorityClockQualified()
	if err != nil {
		return options, errors.Join(errDevCluster, err)
	}
	if !qualified {
		return options, fmt.Errorf("%w: explicit read authority requires a qualified elapsed clock", errDevCluster)
	}
	return options, nil
}
