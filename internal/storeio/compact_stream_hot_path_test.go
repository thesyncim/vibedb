package storeio

import (
	"encoding/binary"
	"testing"
)

func rankAffineNumberViewForHotPathTest() compactStreamView {
	data := make([]byte, 18)
	data[0] = 2
	binary.LittleEndian.PutUint64(data[2:], 10)
	binary.LittleEndian.PutUint64(data[10:], 3)
	return compactStreamView{
		kind:      compactStreamRankAffine,
		count:     4,
		dictCount: 2,
		dictDir:   []byte{0, 0, 0, 0},
		// The structural fast path intentionally does not inspect trailing
		// dictionary bytes after two zero endpoints.
		dictData: []byte("unused"),
		data:     data,
	}
}

func TestCompactPrefixIntegerDispatchAndBounds(t *testing.T) {
	ordinary := barePrefixIntegerDescriptor(t, 4, 10, 2)
	for row, want := range []int64{10, 12, 14, 16} {
		if got, ok := ordinary.prefixInteger(row); !ok || got != want {
			t.Fatalf("ordinary row=%d got=%d ok=%v want=%d", row, got, ok, want)
		}
	}
	for _, row := range []int{-1, ordinary.count} {
		if got, ok := ordinary.prefixInteger(row); ok || got != 0 {
			t.Fatalf("ordinary out-of-bounds row=%d got=%d ok=%v", row, got, ok)
		}
	}
	shortOrdinary := ordinary
	shortOrdinary.data = shortOrdinary.data[:1]
	if got, ok := shortOrdinary.prefixInteger(0); ok || got != 0 {
		t.Fatalf("short ordinary descriptor got=%d ok=%v", got, ok)
	}

	rank := rankAffineNumberViewForHotPathTest()
	for row, want := range []int64{10, 13, 16, 19} {
		if got, ok := rank.prefixInteger(row); !ok || got != want {
			t.Fatalf("rank row=%d got=%d ok=%v want=%d", row, got, ok, want)
		}
	}
	for _, row := range []int{-1, rank.count} {
		if got, ok := rank.prefixInteger(row); ok || got != 0 {
			t.Fatalf("rank out-of-bounds row=%d got=%d ok=%v", row, got, ok)
		}
	}
	shortRank := rank
	shortRank.data = shortRank.data[:17]
	if got, ok := shortRank.prefixInteger(0); ok || got != 0 {
		t.Fatalf("short rank descriptor got=%d ok=%v", got, ok)
	}
	unknown := rank
	unknown.kind = compactStreamKindLimit
	if got, ok := unknown.prefixInteger(0); ok || got != 0 {
		t.Fatalf("unknown kind got=%d ok=%v", got, ok)
	}
}

func TestCompactRankAffineIsNumberLayout(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*compactStreamView)
		want   bool
	}{
		{
			name: "bare-with-trailing-dictionary-data",
			// Two zero directory endpoints are the complete bare proof; an
			// already-constructed view may still carry unused trailing bytes.
			want: true,
		},
		{
			name: "affixed",
			mutate: func(v *compactStreamView) {
				v.dictDir = []byte{3, 0, 3, 0}
				v.dictData = []byte("id:")
			},
		},
		{
			name: "padded",
			mutate: func(v *compactStreamView) {
				v.data[0], v.data[1] = 3, 4
			},
		},
		{
			name: "wrong-kind",
			mutate: func(v *compactStreamView) {
				v.kind = compactStreamPrefixInt
			},
		},
		{
			name: "unknown-kind",
			mutate: func(v *compactStreamView) {
				v.kind = compactStreamKindLimit
			},
		},
		{
			name: "short-data",
			mutate: func(v *compactStreamView) {
				v.data = v.data[:17]
			},
		},
		{
			name: "wrong-dictionary-count",
			mutate: func(v *compactStreamView) {
				v.dictCount = 1
			},
		},
		{
			name: "short-directory",
			mutate: func(v *compactStreamView) {
				v.dictDir = v.dictDir[:2]
			},
		},
		{
			name: "nonzero-prefix-endpoint",
			mutate: func(v *compactStreamView) {
				v.dictDir = []byte{1, 0, 0, 0}
			},
		},
		{
			name: "nonzero-suffix-endpoint",
			mutate: func(v *compactStreamView) {
				v.dictDir = []byte{0, 0, 1, 0}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			view := rankAffineNumberViewForHotPathTest()
			if test.mutate != nil {
				test.mutate(&view)
			}
			if got := view.rankAffineIsNumber(); got != test.want {
				t.Fatalf("rankAffineIsNumber=%v want %v", got, test.want)
			}
		})
	}
}
