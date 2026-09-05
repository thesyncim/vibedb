package gateway

import (
	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/shardservice"
)

// IsReplicatedReadRetryable accepts only a complete error tree consisting of
// known read/discovery availability or applied-floor failures. An outer
// ErrReplicatedLeader never hides an independent malformed route, stale fence,
// authorization failure, pending catalog command, or unknown outcome.
//
// This predicate does not authorize replay of writes. Callers must retain the
// operation's existing exact pending-command protocol and caller deadline.
// Unknown error classes and oversized/cyclic trees conservatively return false.
func IsReplicatedReadRetryable(err error) bool {
	if err == nil {
		return false
	}
	const maximum = 128
	var pending [maximum]error
	pending[0] = err
	count, examined := 1, 0
	for count != 0 {
		count--
		current := pending[count]
		pending[count] = nil
		examined++
		if examined > maximum || current == nil {
			return false
		}
		if refusal, ok := current.(*ReplicatedRefusalError); ok {
			if refusal == nil || refusal.Outcome != (raftserve.Outcome{}) ||
				(refusal.Code != shardservice.ReplicatedRefusalUnavailable && refusal.Code != shardservice.ReplicatedRefusalReadBehind) {
				return false
			}
			continue
		}
		if current == ErrReplicatedLeader || current == ErrReplicatedReadBehind || current == errReplicatedLeaderUnobserved {
			continue
		}
		// A custom classification may add a terminal cause independently of
		// the wrapped error. Only the explicitly handled refusal type is known.
		if _, ok := current.(interface{ Is(error) bool }); ok {
			return false
		}
		if joined, ok := current.(interface{ Unwrap() []error }); ok {
			children := joined.Unwrap()
			if len(children) == 0 || len(children) > maximum-examined-count {
				return false
			}
			for _, child := range children {
				if child == nil {
					return false
				}
				pending[count] = child
				count++
			}
			continue
		}
		if wrapped, ok := current.(interface{ Unwrap() error }); ok && wrapped.Unwrap() != nil {
			pending[count] = wrapped.Unwrap()
			count++
			continue
		}
		return false
	}
	return true
}
