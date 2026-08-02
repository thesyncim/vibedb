package query

import (
	"errors"
	"slices"
	"sort"
	"testing"
)

func FuzzSQLSetLoweringStateful(f *testing.F) {
	for mode := byte(0); mode < 6; mode++ {
		f.Add(mode, int8(1), int8(4), mode&1 == 0, uint8(3), uint8(1))
	}
	f.Fuzz(func(
		t *testing.T,
		mode byte,
		minimum, maximum int8,
		withTail bool,
		limitByte, offsetByte uint8,
	) {
		mode %= 6
		operation := []string{
			"UNION ALL", "UNION DISTINCT", "INTERSECT ALL",
			"INTERSECT DISTINCT", "EXCEPT ALL", "EXCEPT DISTINCT",
		}[mode]
		sql := `SELECT v AS value FROM docs WHERE v >= ? ` + operation +
			` SELECT v FROM docs WHERE v <= ?`
		if withTail {
			sql += ` ORDER BY value DESC LIMIT ? OFFSET ?`
		}
		statement, err := PrepareStatement(sql)
		if err != nil {
			t.Fatal(err)
		}
		defer statement.Release()
		segment := mustSegment(t,
			`{"v":1}`, `{"v":2}`, `{"v":2}`, `{"v":4}`,
		)
		low, high := int64(minimum%6), int64(maximum%6)
		limit, offset := int64(limitByte%8), int64(offsetByte%8)
		args := []any{&low, &high}
		if withTail {
			args = append(args, &limit, &offset)
		}
		want := oracleSQLSetValues(mode, low, high)
		if withTail {
			sort.SliceStable(want, func(i, j int) bool { return want[i] > want[j] })
			start := min(int(offset), len(want))
			end := min(start+int(limit), len(want))
			want = want[start:end]
		}

		var cancel CancelFlag
		var execution Exec
		execution.Options.Cancel = &cancel
		run := func() ([]int64, error) {
			cursor, runErr := statement.RunInto(
				&execution, FromSegment(segment), args,
			)
			if runErr != nil {
				return nil, runErr
			}
			got := make([]int64, 0, execution.Result.RowCount)
			for cursor.Next() {
				value, ok := cursor.Cell(0).Int64()
				if !ok {
					t.Fatalf("set result is not an integer: %s", cursor.Cell(0).JSON())
				}
				got = append(got, value)
			}
			return got, nil
		}

		got, err := run()
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("first run mode=%d got=%v want=%v err=%v", mode, got, want, err)
		}
		execution.Options.ResultRows = -1
		execution.Options.ResultBytes = 1
		_, err = run()
		if !errors.Is(err, ErrResultBudget) || execution.Result.RowCount != 0 {
			t.Fatalf("budget failure rows/error = %d/%v", execution.Result.RowCount, err)
		}
		execution.Options.ResultBytes = -1
		cancel.Cancel()
		_, err = run()
		if !errors.Is(err, ErrCanceled) || execution.Result.RowCount != 0 {
			t.Fatalf("cancel failure rows/error = %d/%v", execution.Result.RowCount, err)
		}
		cancel.Reset()
		got, err = run()
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("reuse got=%v want=%v err=%v", got, want, err)
		}
		if statement.nested.frame.intermediate.used != 0 {
			t.Fatalf("reuse retained %d frame bytes", statement.nested.frame.intermediate.used)
		}
	})
}

func oracleSQLSetValues(mode byte, minimum, maximum int64) []int64 {
	source := []int64{1, 2, 2, 4}
	left := make([]int64, 0, len(source))
	right := make([]int64, 0, len(source))
	for _, value := range source {
		if value >= minimum {
			left = append(left, value)
		}
		if value <= maximum {
			right = append(right, value)
		}
	}
	counts := func(values []int64) map[int64]int {
		result := make(map[int64]int)
		for _, value := range values {
			result[value]++
		}
		return result
	}
	switch mode {
	case 0:
		return append(append([]int64(nil), left...), right...)
	case 1:
		seen := make(map[int64]bool)
		result := make([]int64, 0, len(left)+len(right))
		for _, values := range [][]int64{left, right} {
			for _, value := range values {
				if !seen[value] {
					seen[value] = true
					result = append(result, value)
				}
			}
		}
		return result
	case 2:
		rightCounts := counts(right)
		result := make([]int64, 0, len(left))
		for _, value := range left {
			if rightCounts[value] != 0 {
				rightCounts[value]--
				result = append(result, value)
			}
		}
		return result
	case 3:
		rightCounts, seen := counts(right), make(map[int64]bool)
		result := make([]int64, 0, len(left))
		for _, value := range left {
			if rightCounts[value] != 0 && !seen[value] {
				seen[value] = true
				result = append(result, value)
			}
		}
		return result
	case 4:
		rightCounts := counts(right)
		result := make([]int64, 0, len(left))
		for _, value := range left {
			if rightCounts[value] != 0 {
				rightCounts[value]--
			} else {
				result = append(result, value)
			}
		}
		return result
	default:
		rightCounts, seen := counts(right), make(map[int64]bool)
		result := make([]int64, 0, len(left))
		for _, value := range left {
			if rightCounts[value] == 0 && !seen[value] {
				seen[value] = true
				result = append(result, value)
			}
		}
		return result
	}
}
