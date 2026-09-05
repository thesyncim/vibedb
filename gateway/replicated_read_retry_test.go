package gateway

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftserve"
	"github.com/thesyncim/vibedb/internal/raftservice"
	"github.com/thesyncim/vibedb/shardservice"
	"github.com/thesyncim/vibedb/store/durable"
)

func TestReplicatedReadRetryRequiresEveryCauseToBeTransient(t *testing.T) {
	for _, err := range []error{ErrReplicatedLeader, ErrReplicatedReadBehind, errReplicatedLeaderUnobserved,
		fmt.Errorf("discover: %w", errors.Join(ErrReplicatedLeader, errReplicatedLeaderUnobserved)),
		errors.Join(ErrReplicatedLeader, &ReplicatedRefusalError{Code: shardservice.ReplicatedRefusalUnavailable},
			&ReplicatedRefusalError{Code: shardservice.ReplicatedRefusalReadBehind}),
	} {
		if !IsReplicatedReadRetryable(err) {
			t.Fatalf("read transient rejected: %v", err)
		}
	}
	for _, terminal := range []error{ErrReplicatedRoute, ErrReplicatedUnauthorized, ErrReplicatedCatalogPending,
		ErrNativeCommandPending, ErrReplicatedReadBufferBound, raftservice.ErrServingFence,
		raftservice.ErrOutcomeUnknown, durable.ErrCommitOutcomeUnknown, context.Canceled, context.DeadlineExceeded,
		errors.New("unknown failure"),
		&ReplicatedRefusalError{Code: shardservice.ReplicatedRefusalUnavailable, Outcome: raftserve.Outcome{Code: raftserve.OutcomeNotLeader}},
	} {
		for _, err := range []error{terminal, errors.Join(ErrReplicatedLeader, terminal),
			fmt.Errorf("registration: %w", errors.Join(ErrReplicatedLeader,
				&ReplicatedRefusalError{Code: shardservice.ReplicatedRefusalUnavailable}, terminal)),
		} {
			if IsReplicatedReadRetryable(err) {
				t.Fatalf("terminal failure hidden: %v", err)
			}
		}
	}
	for code := shardservice.ReplicatedRefusalNone; code <= shardservice.ReplicatedRefusalRetryRetired; code++ {
		if code == shardservice.ReplicatedRefusalUnavailable || code == shardservice.ReplicatedRefusalReadBehind {
			continue
		}
		err := errors.Join(ErrReplicatedLeader, &ReplicatedRefusalError{Code: code})
		if IsReplicatedReadRetryable(err) {
			t.Fatalf("refusal %d accepted for read retry", code)
		}
	}
}

type cyclicReadRetryError struct{}

func (*cyclicReadRetryError) Error() string   { return "cyclic read error" }
func (e *cyclicReadRetryError) Unwrap() error { return e }

type classifiedReadRetryError struct {
	terminal error
}

func (e classifiedReadRetryError) Error() string        { return "independently classified read error" }
func (e classifiedReadRetryError) Unwrap() error        { return ErrReplicatedLeader }
func (e classifiedReadRetryError) Is(target error) bool { return target == e.terminal }

func TestReplicatedReadRetryRejectsIndependentWrapperClassification(t *testing.T) {
	for _, terminal := range []error{ErrReplicatedUnauthorized, durable.ErrCommitOutcomeUnknown} {
		err := classifiedReadRetryError{terminal: terminal}
		if !errors.Is(err, terminal) || !errors.Is(err, ErrReplicatedLeader) {
			t.Fatal("fixture must carry both classifications")
		}
		if IsReplicatedReadRetryable(fmt.Errorf("discover: %w", err)) {
			t.Fatal("wrapper's independent terminal classification was hidden")
		}
	}
}

func TestReplicatedReadRetryBoundsMalformedErrorTrees(t *testing.T) {
	var typedNil *ReplicatedRefusalError
	wide := make([]error, 129)
	for index := range wide {
		wide[index] = ErrReplicatedLeader
	}
	for _, err := range []error{nil, typedNil, &cyclicReadRetryError{}, errors.Join(wide...)} {
		if IsReplicatedReadRetryable(err) {
			t.Fatal("malformed or unbounded error tree accepted")
		}
	}
}
