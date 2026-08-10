package shardservice

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/thesyncim/vibedb/distribution"
)

// MaxPositionIdentityBytes bounds each variable-width identity component of a
// logical position. The log and index components are fixed width, so a valid
// position always has a small, predictable wire and retained-memory footprint.
const MaxPositionIdentityBytes = 255

// ErrInvalidPosition is the sentinel every malformed logical position matches
// under errors.Is.
var ErrInvalidPosition = errors.New("shardservice: invalid logical position")

// PositionValidationError reports why a logical position was rejected.
type PositionValidationError struct {
	Reason string
}

func (e *PositionValidationError) Error() string {
	return "shardservice: invalid logical position: " + e.Reason
}

func (e *PositionValidationError) Unwrap() error { return ErrInvalidPosition }

// Position identifies one applied entry in one physical shard log. Index is
// meaningful only within the exact (Distribution, Shard, LogID) lineage; an
// index from a different LogID is never comparable, even when the shard name is
// reused after recovery or resharding.
//
// The zero value represents no position only at optional API boundaries. A
// present Position must pass Validate; in particular Index and LogID cannot be
// zero.
type Position struct {
	Distribution distribution.DistributionName
	Shard        distribution.ShardID
	LogID        [16]byte
	Index        uint64
}

// IsZero reports whether every position field is unset. Optional wire fields
// use this as the sole canonical payload when their explicit presence bit is
// false.
func (p Position) IsZero() bool { return p == (Position{}) }

// Validate checks the complete logical identity and its fixed-width index.
func (p Position) Validate() error {
	if p.Distribution == "" {
		return &PositionValidationError{Reason: "distribution is empty"}
	}
	if len(p.Distribution) > MaxPositionIdentityBytes {
		return &PositionValidationError{Reason: fmt.Sprintf(
			"distribution is %d bytes; limit is %d", len(p.Distribution), MaxPositionIdentityBytes)}
	}
	if !utf8.ValidString(string(p.Distribution)) {
		return &PositionValidationError{Reason: "distribution is not valid UTF-8"}
	}
	if p.Shard == "" {
		return &PositionValidationError{Reason: "shard is empty"}
	}
	if len(p.Shard) > MaxPositionIdentityBytes {
		return &PositionValidationError{Reason: fmt.Sprintf(
			"shard is %d bytes; limit is %d", len(p.Shard), MaxPositionIdentityBytes)}
	}
	if !utf8.ValidString(string(p.Shard)) {
		return &PositionValidationError{Reason: "shard is not valid UTF-8"}
	}
	if p.LogID == ([16]byte{}) {
		return &PositionValidationError{Reason: "log identity is zero"}
	}
	if p.Index == 0 {
		return &PositionValidationError{Reason: "index is zero"}
	}
	return nil
}

// SameSource reports whether p and other name the same distribution and shard.
// A true result does not imply comparable indexes; SameLog must also hold.
func (p Position) SameSource(other Position) bool {
	return p.Distribution == other.Distribution && p.Shard == other.Shard
}

// SameLog reports whether p and other belong to the exact same log lineage.
func (p Position) SameLog(other Position) bool {
	return p.SameSource(other) && p.LogID == other.LogID
}
