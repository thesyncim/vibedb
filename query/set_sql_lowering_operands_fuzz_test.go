package query

import (
	"errors"
	"slices"
	"testing"
)

func FuzzSQLSetValuesOperandsStateful(f *testing.F) {
	for mode := byte(0); mode < 6; mode++ {
		f.Add(mode, int8(1), int8(2), int8(2), int8(3))
	}
	f.Fuzz(func(t *testing.T, mode byte, a, b, c, d int8) {
		mode %= 6
		operation := []string{
			"UNION ALL", "UNION DISTINCT", "INTERSECT ALL",
			"INTERSECT DISTINCT", "EXCEPT ALL", "EXCEPT DISTINCT",
		}[mode]
		statement, err := PrepareStatement(
			`VALUES (?), (?), (NULL) ` + operation + ` VALUES (?), (?), (NULL)`,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer statement.Release()
		av, bv, cv, dv := int64(a%5), int64(b%5), int64(c%5), int64(d%5)
		args := []any{&av, &bv, &cv, &dv}
		left := []string{setFuzzUnknownText(av), setFuzzUnknownText(bv), "null"}
		right := []string{setFuzzUnknownText(cv), setFuzzUnknownText(dv), "null"}
		want := oracleSetFuzzStrings(mode, left, right)

		var cancel CancelFlag
		var execution Exec
		execution.Options.Cancel = &cancel
		run := func() ([]string, error) {
			cursor, runErr := statement.RunInto(&execution, Source{}, args)
			if runErr != nil {
				return nil, runErr
			}
			return setStatementCursorJSON(cursor), nil
		}
		got, err := run()
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("VALUES mode=%d got=%v want=%v err=%v", mode, got, want, err)
		}
		execution.Options.ResultBytes = 1
		_, err = run()
		if !errors.Is(err, ErrResultBudget) || execution.Result.RowCount != 0 {
			t.Fatalf("budget reuse rows/error = %d/%v", execution.Result.RowCount, err)
		}
		execution.Options.ResultBytes = -1
		cancel.Cancel()
		_, err = run()
		if !errors.Is(err, ErrCanceled) || execution.Result.RowCount != 0 {
			t.Fatalf("cancel reuse rows/error = %d/%v", execution.Result.RowCount, err)
		}
		cancel.Reset()
		got, err = run()
		if err != nil || !slices.Equal(got, want) ||
			statement.nested.frame.intermediate.used != 0 {
			t.Fatalf("recovery got=%v want=%v bytes=%d err=%v", got, want,
				statement.nested.frame.intermediate.used, err)
		}
	})
}

func setFuzzUnknownText(value int64) string {
	return `"` + setFuzzInt(value) + `"`
}

func setFuzzInt(value int64) string {
	if value == 0 {
		return "0"
	}
	negative := value < 0
	if negative {
		value = -value
	}
	var raw [4]byte
	at := len(raw)
	for value != 0 {
		at--
		raw[at] = byte(value%10) + '0'
		value /= 10
	}
	if negative {
		at--
		raw[at] = '-'
	}
	return string(raw[at:])
}

func oracleSetFuzzStrings(mode byte, left, right []string) []string {
	counts := func(values []string) map[string]int {
		result := make(map[string]int)
		for _, value := range values {
			result[value]++
		}
		return result
	}
	switch mode {
	case 0:
		return append(append([]string(nil), left...), right...)
	case 1:
		seen := make(map[string]bool)
		var result []string
		for _, values := range [][]string{left, right} {
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
		var result []string
		for _, value := range left {
			if rightCounts[value] != 0 {
				rightCounts[value]--
				result = append(result, value)
			}
		}
		return result
	case 3:
		rightCounts, seen := counts(right), make(map[string]bool)
		var result []string
		for _, value := range left {
			if rightCounts[value] != 0 && !seen[value] {
				seen[value] = true
				result = append(result, value)
			}
		}
		return result
	case 4:
		rightCounts := counts(right)
		var result []string
		for _, value := range left {
			if rightCounts[value] != 0 {
				rightCounts[value]--
			} else {
				result = append(result, value)
			}
		}
		return result
	default:
		rightCounts, seen := counts(right), make(map[string]bool)
		var result []string
		for _, value := range left {
			if rightCounts[value] == 0 && !seen[value] {
				seen[value] = true
				result = append(result, value)
			}
		}
		return result
	}
}
