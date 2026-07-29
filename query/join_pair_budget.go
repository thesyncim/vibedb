package query

import (
	"errors"
	"fmt"
	"math"
	"unsafe"

	"github.com/thesyncim/vibedb/store"
	"github.com/thesyncim/vibejson"
)

const defaultJoinPairBytes int64 = 64 << 20

// ErrJoinPairBudget is the sentinel wrapped by [JoinPairBudgetError].
var ErrJoinPairBudget = errors.New("query: join pair workspace exceeds execution budget")

// JoinPairBudgetError reports that an inner join's multiplicative pair space
// would exceed its configured workspace. The executor fails rather than
// truncating, because ordered, grouped, and aggregate plans must observe every
// pair to remain exact.
type JoinPairBudgetError struct {
	Pairs int
	Bytes int64
	Limit int64
}

func (e *JoinPairBudgetError) Error() string {
	return fmt.Sprintf(
		"query: join pair workspace needs at least %d bytes for %d pairs, "+
			"exceeding the execution limit of %d: %v",
		e.Bytes, e.Pairs, e.Limit, ErrJoinPairBudget,
	)
}

// Unwrap lets callers classify the error with errors.Is.
func (e *JoinPairBudgetError) Unwrap() error { return ErrJoinPairBudget }

type joinPairBudget struct {
	limit   int64
	used    int64
	base    int64
	perPair int64
	active  bool
}

func normalizeJoinPairBytes(options ExecOptions) (int64, error) {
	bytes := options.JoinPairBytes
	switch {
	case bytes < -1:
		return 0, fmt.Errorf(
			"query: JoinPairBytes must be -1, zero, or positive, got %d", bytes,
		)
	case bytes == 0:
		return defaultJoinPairBytes, nil
	default:
		return bytes, nil
	}
}

func (b *joinPairBudget) configure(limit int64) {
	b.limit = limit
	b.used = 0
	b.base = 0
	b.perPair = 0
	b.active = false
}

func (b *joinPairBudget) begin(p *plan, join *planJoin) {
	b.base = b.used
	b.perPair = joinPairBytesPerRow(p, join)
	b.active = true
}

func (b *joinPairBudget) maxPairs() int {
	if b.limit < 0 {
		return -1
	}
	if b.perPair <= 0 {
		return int(^uint(0) >> 1)
	}
	remaining := max(b.limit-b.used, 0)
	n := remaining / b.perPair
	if n > int64(int(^uint(0)>>1)) {
		return int(^uint(0) >> 1)
	}
	return int(n)
}

func (b *joinPairBudget) commitPairs(pairs int) {
	if b.perPair > 0 && int64(pairs) <= math.MaxInt64/b.perPair {
		b.used = saturatedBytes(b.base, int64(pairs)*b.perPair)
	} else {
		b.used = math.MaxInt64
	}
}

func (b *joinPairBudget) pairError(pairs int) error {
	bytes := int64(math.MaxInt64)
	if b.perPair > 0 && int64(pairs) <= math.MaxInt64/b.perPair {
		bytes = saturatedBytes(b.base, int64(pairs)*b.perPair)
	}
	return &JoinPairBudgetError{
		Pairs: pairs,
		Bytes: bytes,
		Limit: b.limit,
	}
}

func (b *joinPairBudget) admitBuild(bytes int64) error {
	if bytes < 0 || b.used > math.MaxInt64-bytes {
		return &JoinPairBudgetError{
			Bytes: math.MaxInt64,
			Limit: b.limit,
		}
	}
	required := b.used + bytes
	if b.limit >= 0 && required > b.limit {
		return &JoinPairBudgetError{
			Bytes: required,
			Limit: b.limit,
		}
	}
	b.used = required
	return nil
}

func joinBuildEntryBytes(cell scalar) int64 {
	const directoryBytesPerEntry = int64(16) // next power-of-two table is <4n.
	bytes := int64(unsafe.Sizeof(scalar{})) +
		int64(unsafe.Sizeof(store.Location{})) +
		int64(unsafe.Sizeof(int32(0))) +
		directoryBytesPerEntry
	switch cell.kind {
	case kindNumber:
		bytes += int64(len(cell.num))
	case kindString:
		bytes += int64(len(cell.sval))
	default:
		bytes += int64(len(cell.raw))
	}
	return bytes
}

func (b *joinPairBudget) admitText(bytes int) error {
	if !b.active || bytes == 0 || b.limit < 0 {
		return nil
	}
	add := int64(bytes)
	if add < 0 || b.used > math.MaxInt64-add || b.used+add > b.limit {
		need := int64(math.MaxInt64)
		if add >= 0 && b.used <= math.MaxInt64-add {
			need = b.used + add
		}
		return &JoinPairBudgetError{
			Pairs: 0,
			Bytes: need,
			Limit: b.limit,
		}
	}
	b.used += add
	return nil
}

func joinPairBytesPerRow(p *plan, join *planJoin) int64 {
	// Fixed pair addresses, the identity selection, one scalar per value and
	// numeric column, the borrowed RawValue columns used to classify late
	// values, and the one reused numeric RawValue gather. Decoded escaped text
	// is charged at its exact size by admitText before that arena grows.
	bytes := int64(unsafe.Sizeof(int(0))) * 2
	if join.left && (len(join.innerCols) != 0 || len(join.innerNums) != 0) {
		bytes += int64(unsafe.Sizeof(int(0)))
	}
	bytes += int64(unsafe.Sizeof(store.Location{})) * 2
	bytes += int64(len(p.valuePaths)+len(p.numPaths)) * int64(unsafe.Sizeof(scalar{}))
	rawColumns := len(p.lateCols) + len(join.innerCols)
	if len(p.numPaths) != 0 {
		rawColumns++
	}
	bytes += int64(rawColumns) * int64(unsafe.Sizeof(vibejson.RawValue{}))
	return max(bytes, 1)
}
