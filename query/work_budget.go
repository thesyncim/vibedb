package query

import (
	"errors"
	"fmt"
)

// ErrWorkBudget is the sentinel wrapped by [WorkBudgetError].
var ErrWorkBudget = errors.New("query: execution workspace exceeds budget")

// ErrSpillBudget is the sentinel wrapped by [SpillBudgetError].
var ErrSpillBudget = errors.New("query: spill storage exceeds execution budget")

// ErrSpillCorrupt reports that an execution's temporary run could not be
// decoded back into the exact values the engine wrote. It is distinct from a
// storage quota failure: callers may classify filesystem interference or
// corruption without string matching.
var ErrSpillCorrupt = errors.New("query: corrupt spill run")

// WorkBudgetError reports a bounded intermediate that cannot be completed
// within its configured resource limit.
type WorkBudgetError struct {
	Resource string
	Bytes    int64
	Limit    int64
}

func (e *WorkBudgetError) Error() string {
	return fmt.Sprintf(
		"query: %s needs %d bytes, exceeding the execution limit of %d: %v",
		e.Resource, e.Bytes, e.Limit, ErrWorkBudget,
	)
}

// Unwrap lets callers classify the error with errors.Is.
func (e *WorkBudgetError) Unwrap() error { return ErrWorkBudget }

// SpillBudgetError reports that an external sort or grouping run would exceed
// the configured live temporary-file quota.
type SpillBudgetError struct {
	Bytes int64
	Limit int64
}

func (e *SpillBudgetError) Error() string {
	return fmt.Sprintf(
		"query: spill storage needs more than %d bytes, exceeding the "+
			"execution limit of %d: %v",
		e.Bytes, e.Limit, ErrSpillBudget,
	)
}

// Unwrap lets callers classify the error with errors.Is.
func (e *SpillBudgetError) Unwrap() error { return ErrSpillBudget }
