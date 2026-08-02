package query

import (
	"errors"
	"fmt"
	"math"
	"unsafe"
)

// DefaultIntermediateBytes is the statement-wide logical storage allowance
// for relation-valued subplans. It is deliberately independent of ResultBytes:
// the latter bounds rows returned to the caller, while this bound covers rows
// that exist only while one relational operator feeds another.
const DefaultIntermediateBytes int64 = 64 << 20

// ErrIntermediateBudget is matched by [IntermediateBudgetError].
var ErrIntermediateBudget = errors.New("query: intermediate relation budget exceeded")

// IntermediateBudgetError reports a relation-valued subplan that could not be
// materialized inside ExecOptions.IntermediateBytes. The executor returns the
// error before growing the rejected spool.
type IntermediateBudgetError struct {
	Resource string
	Bytes    int64
	Limit    int64
}

func (e *IntermediateBudgetError) Error() string {
	return fmt.Sprintf(
		"query: %s needs %d bytes, exceeding the statement intermediate limit of %d: %v",
		e.Resource, e.Bytes, e.Limit, ErrIntermediateBudget,
	)
}

func (e *IntermediateBudgetError) Unwrap() error { return ErrIntermediateBudget }

// statementFrame owns accounts shared by every nested relation in one SQL
// statement. It is intentionally separate from Exec: nested Statements retain
// distinct warm workspaces, but none receives a fresh intermediate allowance.
type statementFrame struct {
	intermediate intermediateBudget
	// epoch identifies one top-level execution. Correlated children use it to
	// lower authored parameters once for the whole lexical APPLY tree rather than
	// once per outer row.
	epoch uint64
	// args is the top-level binding. CTE definitions retain absolute placeholder
	// ranges even when reached through a later definition or predicate subquery.
	// The slice is borrowed for one synchronous RunInto call.
	args []any
}

type intermediateBudget struct {
	limit int64
	used  int64
}

func newStatementFrame(options ExecOptions) (*statementFrame, error) {
	frame := new(statementFrame)
	if err := frame.begin(options); err != nil {
		return nil, err
	}
	return frame, nil
}

func (f *statementFrame) begin(options ExecOptions) error {
	limit, err := normalizeIntermediateBytes(options)
	if err != nil {
		return err
	}
	f.intermediate.limit = limit
	f.intermediate.used = 0
	f.epoch++
	if f.epoch == 0 {
		f.epoch = 1
	}
	return nil
}

func normalizeIntermediateBytes(options ExecOptions) (int64, error) {
	bytes := options.IntermediateBytes
	switch {
	case bytes < -1:
		return 0, fmt.Errorf(
			"query: IntermediateBytes must be -1, zero, or positive, got %d",
			bytes,
		)
	case bytes == 0:
		return DefaultIntermediateBytes, nil
	default:
		return bytes, nil
	}
}

func (b *intermediateBudget) remaining() int64 {
	if b.limit < 0 {
		return -1
	}
	return max(b.limit-b.used, 0)
}

func (b *intermediateBudget) reserve(resource string, bytes int64) error {
	if bytes <= 0 {
		return nil
	}
	required := saturatedBytes(b.used, bytes)
	if b.limit >= 0 && required > b.limit {
		return &IntermediateBudgetError{
			Resource: resource,
			Bytes:    required,
			Limit:    b.limit,
		}
	}
	b.used = required
	return nil
}

func (b *intermediateBudget) release(bytes int64) {
	if bytes <= 0 {
		return
	}
	if bytes >= b.used {
		b.used = 0
		return
	}
	b.used -= bytes
}

// predicateValuesRetainedBytes accounts the owned scalar slots and interface
// vector a predicate subquery keeps while the outer plan is compiled and run.
// payload is decoded string or exact non-integer number storage copied out of
// the child Result. The count is checked before either backing slice grows.
func predicateValuesRetainedBytes(known int, payload int64) int64 {
	if known < 0 || payload < 0 {
		return math.MaxInt64
	}
	perValue := int64(unsafe.Sizeof(subqueryScalar{})) +
		int64(unsafe.Sizeof(any(nil)))
	return saturatedBytes(
		saturatedProduct(int64(known), perValue), payload,
	)
}
