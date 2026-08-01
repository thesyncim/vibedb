//go:build 386

package query

import (
	"errors"
	"math"
	"testing"
)

func TestApplyKernel386SizingRejectsIntOverflow(t *testing.T) {
	if _, err := applyMemoCapacity(math.MaxInt); !errors.Is(err, ErrApplySize) {
		t.Fatalf("memo capacity error = %v, want apply size error", err)
	}
	if got := applyMemoIndexBytes(math.MaxInt, math.MaxInt); got != math.MaxInt64 {
		t.Fatalf("saturated memo bytes = %d, want MaxInt64", got)
	}

	source := &applyShapeSource{rows: 1, columns: math.MaxInt}
	program := &applyTestProgram{}
	var kernel ApplyKernel
	if _, err := kernel.Run(source, program, ApplyOptions{
		Kind: ApplyCross, RightColumns: 1,
	}); !errors.Is(err, ErrApplySize) {
		t.Fatalf("overflow source error = %v, want apply size error", err)
	}
}
