//go:build 386

package query

import (
	"errors"
	"math"
	"testing"
)

func TestSetStatement386SaturatingMetadataAccounting(t *testing.T) {
	if got := saturatedSetStatementParamEnd(math.MaxInt, 1); got != math.MaxInt {
		t.Fatalf("parameter end = %d, want MaxInt", got)
	}
	if got := saturatedSetStatementInt(math.MaxInt, 1); got != math.MaxInt {
		t.Fatalf("positive stat sum = %d, want MaxInt", got)
	}
	if got := saturatedSetStatementInt(math.MinInt, -1); got != math.MinInt {
		t.Fatalf("negative stat sum = %d, want MinInt", got)
	}
}

func TestSetStatement386RejectsMaxRowMalformedRunnerBeforeIndexing(t *testing.T) {
	var exec Exec
	exec.Result.Columns = []ResultColumn{{}}
	exec.Result.RowCount = math.MaxInt
	statement := Statement{outputs: 1}
	err := validateSetStatementLeafResult(0, 1, &exec, statement.cursor(&exec.Result))
	if !errors.Is(err, ErrSetTreeSource) {
		t.Fatalf("MaxInt malformed row count error = %T %v", err, err)
	}
}
