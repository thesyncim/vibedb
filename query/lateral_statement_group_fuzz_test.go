package query

import (
	"bytes"
	"strconv"
	"testing"
)

func FuzzLateralMultiplyDecimal(f *testing.F) {
	for _, seed := range []struct {
		coefficient int64
		scale       uint8
		count       uint8
	}{
		{1, 1, 3}, {-375, 2, 17}, {9007199254740993, 12, 31},
	} {
		f.Add(seed.coefficient, seed.scale, seed.count)
	}
	f.Fuzz(func(t *testing.T, coefficient int64, rawScale, rawCount uint8) {
		scale := int(rawScale % 19)
		count := int(rawCount%31) + 1
		spelling := strconv.FormatInt(coefficient, 10) + "e-" + strconv.Itoa(scale)
		value := joinNumberScalar([]byte(spelling))

		var direct, repeated aggAcc
		var directBudget, repeatedBudget aggregateBudget
		directBudget.begin(defaultAggregateBytes)
		repeatedBudget.begin(defaultAggregateBytes)
		number, err := direct.number(&directBudget)
		if err != nil {
			t.Fatal(err)
		}
		if err := number.sum.add(value, &direct.lease, &directBudget); err != nil {
			t.Fatal(err)
		}
		number.n = count
		if err := lateralMultiplyDecimal(
			&number.sum, count, &direct.lease, &directBudget,
		); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < count; i++ {
			if err := repeated.accumulateNumber(aggSum, value, &repeatedBudget); err != nil {
				t.Fatal(err)
			}
		}
		var directWork, repeatedWork Workspace
		directWork.aggregateBudget.begin(defaultAggregateBytes)
		repeatedWork.aggregateBudget.begin(defaultAggregateBytes)
		got, err := directWork.exactSumCell(&direct)
		if err != nil {
			t.Fatal(err)
		}
		want, err := repeatedWork.exactSumCell(&repeated)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.AppendJSON(nil), want.AppendJSON(nil)) {
			t.Fatalf("%s x %d = %s, want %s", spelling, count, got.String(), want.String())
		}
	})
}
