package query

import (
	"hash/maphash"
	"strings"
	"testing"

	"github.com/thesyncim/vibedb/store"
)

func TestExactJoinNumberHashCanonicalSpellings(t *testing.T) {
	seed := maphash.MakeSeed()
	for _, spellings := range [][]string{
		{"0", "-0", "0.0", "0e999999999999999999999999"},
		{"1", "1.0", "10e-1", "0.1e1"},
		{"-125", "-125.0", "-1.25e2", "-1250e-1"},
		{
			"1e100000000000000000000",
			"10e99999999999999999999",
			"0.1e100000000000000000001",
		},
	} {
		want := hashExactJoinNumber(seed, []byte(spellings[0]))
		for _, spelling := range spellings[1:] {
			if got := hashExactJoinNumber(seed, []byte(spelling)); got != want {
				t.Fatalf("hash(%s) = %#x, hash(%s) = %#x",
					spelling, got, spellings[0], want)
			}
		}
	}
}

func TestExactJoinNumberHashWideValuesDoNotCollapse(t *testing.T) {
	seed := maphash.MakeSeed()
	exponents := []string{
		"1" + strings.Repeat("0", 80),
		"1" + strings.Repeat("0", 79) + "1",
		"9" + strings.Repeat("9", 79),
	}
	hashes := make(map[uint64]string, len(exponents)*2)
	for _, exponent := range exponents {
		for _, coefficient := range []string{"1", "2"} {
			spelling := coefficient + "e" + exponent
			hash := hashExactJoinNumber(seed, []byte(spelling))
			if previous, exists := hashes[hash]; exists {
				t.Fatalf("wide values %s and %s collapsed to hash %#x",
					previous, spelling, hash)
			}
			hashes[hash] = spelling
		}
	}
}

func TestExactJoinNumberHashWarmAllocations(t *testing.T) {
	seed := maphash.MakeSeed()
	number := []byte("123456789012345678901234567890e" +
		"10000000000000000000000000000000000000000")
	hashSink = hashExactJoinNumber(seed, number)
	if got := testing.AllocsPerRun(1_000, func() {
		hashSink = hashExactJoinNumber(seed, number)
	}); got != 0 {
		t.Fatalf("exact numeric join hash allocated %.2f times per call", got)
	}
}

func TestExactNumericJoinFeedsExactAggregates(t *testing.T) {
	exponent := "1" + strings.Repeat("0", 40)
	previousExponent := strings.Repeat("9", 40)

	db := &store.Database{}
	outer, err := db.CreateCollection("outer", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for key, document := range map[string]string{
		"o1": `{"k":9007199254740993,"v":9007199254740993}`,
		"o2": `{"k":9007199254740994,"v":1}`,
		"o3": `{"k":1e` + exponent + `,"v":7}`,
	} {
		if _, err := outer.Put(key, []byte(document)); err != nil {
			t.Fatal(err)
		}
	}
	inner, err := db.CreateCollection("inner", store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	for key, document := range map[string]string{
		"i1": `{"code":9007199254740993.0,"amount":0.25}`,
		"i2": `{"code":9007199254740994e0,"amount":0.75}`,
		"i3": `{"code":10e` + previousExponent + `,"amount":0.5}`,
	} {
		if _, err := inner.Put(key, []byte(document)); err != nil {
			t.Fatal(err)
		}
	}

	result, err := Select(
		Sum("v"), Sum("i.amount"), Min("i.code"), Max("i.code"),
	).Join(
		JoinOn("inner", "k", "code").As("i"),
	).Run(FromDatabase(db.Snapshot(), "outer"))
	if err != nil {
		t.Fatal(err)
	}
	assertAggregateJSON(t, result, "sum(v)", "9007199254741001")
	assertAggregateJSON(t, result, "sum(i.amount)", "1.5")
	assertAggregateJSON(t, result, "min(i.code)", "9007199254740993.0")
	assertAggregateJSON(t, result, "max(i.code)", "10e"+previousExponent)
}

var hashSink uint64
