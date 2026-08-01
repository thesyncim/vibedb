//go:build 386

package query

import (
	"errors"
	"math"
	"testing"
)

func TestSetTree386SizingAndRowOverflow(t *testing.T) {
	if got := setTreeControlRetainedBytes(math.MaxInt, math.MaxInt); got != math.MaxInt64 {
		t.Fatalf("control bytes = %d, want MaxInt64", got)
	}
	var executor SetTreeExecutor
	executor.totalRows = math.MaxInt64
	if err := executor.admitRows(7, 1); !errors.Is(err, ErrSetTreeSize) {
		t.Fatalf("row overflow error = %v, want set-tree size", err)
	}
	if _, err := reserveSetTreeSlots(nil, -1); !errors.Is(err, ErrSetTreeSize) {
		t.Fatalf("negative slot reservation error = %v, want set-tree size", err)
	}
}
