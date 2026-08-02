//go:build 386

package query

import (
	"errors"
	"math"
	"testing"
)

func TestRecursiveCTEViewSizingOverflow386(t *testing.T) {
	view := recursiveView{
		spool: &recursiveSpool{columns: math.MaxInt},
		rows:  math.MaxInt,
	}
	frame := beginRecursiveCTEFrame(t, ExecOptions{IntermediateBytes: -1})
	var runtime RecursiveCTERuntime
	var destination relationSpool
	charge, err := runtime.bindRecursiveView(
		view, &destination, frame, nil, "386 overflow",
	)
	if !errors.Is(err, ErrRecursiveSize) || charge != 0 ||
		frame.intermediate.used != 0 || destination.rows != 0 {
		t.Fatalf("overflow bind = charge %d used %d rows %d err %v",
			charge, frame.intermediate.used, destination.rows, err)
	}
}
