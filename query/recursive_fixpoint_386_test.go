//go:build 386

package query

import (
	"errors"
	"math"
	"testing"
)

func TestRecursiveFixpoint386SizingRejectsIntOverflow(t *testing.T) {
	if _, ok := checkedRecursiveAdd(math.MaxInt, 1); ok {
		t.Fatal("checkedRecursiveAdd accepted a 32-bit integer overflow")
	}
	if _, err := recursiveIdentityTableCapacity(math.MaxInt); !errors.Is(err, ErrRecursiveSize) {
		t.Fatalf("identity capacity error = %v, want recursive size error", err)
	}

	spool := recursiveSpool{columns: 2}
	if err := spool.appendMeasuredRow([]Cell{{}, {}}, int64(math.MaxInt)+1, nil); !errors.Is(err, ErrRecursiveSize) {
		t.Fatalf("oversized payload error = %v, want recursive size error", err)
	}
}
