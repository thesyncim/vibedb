package query

import (
	"fmt"
	"sort"
	"testing"
)

func assertRecursiveDefinitionDifferential(
	tb testing.TB,
	start int,
	edges [][2]int,
	materialization RecursiveCTEMaterialization,
) {
	tb.Helper()
	_, snapshot := recursiveStatementDatabase(tb, edges)
	fixture := prepareRecursiveDefinitionFixture(
		tb, materialization,
		RecursiveCTELimits{MaxIterations: 32, MaxRows: 256, MaxBytes: -1},
	)
	defer fixture.release()
	var exec Exec
	got := runRecursiveDefinitionFixture(tb, fixture, &exec, snapshot, start)
	want := recursiveStatementGraphOracle(start, edges)
	sort.Ints(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		tb.Fatalf("owning recursive definition = %v, want %v for %v",
			got, want, edges)
	}
}

func TestRecursiveCTEDefinitionRandomizedDifferential(t *testing.T) {
	state := uint64(0xd1b54a32d192ed03)
	next := func() uint64 {
		state ^= state << 7
		state ^= state >> 9
		return state
	}
	for trial := 0; trial < 60; trial++ {
		edges := make([][2]int, int(next()%25))
		for edge := range edges {
			edges[edge] = [2]int{int(next() % 8), int(next() % 8)}
		}
		mode := RecursiveCTEReferenceLocal
		if trial&1 == 0 {
			mode = RecursiveCTEShared
		}
		assertRecursiveDefinitionDifferential(t, int(next()%8), edges, mode)
	}
}

func FuzzRecursiveCTEDefinitionDifferential(f *testing.F) {
	f.Add(byte(0), true, []byte{0, 1, 1, 2, 2, 3, 3, 0})
	f.Add(byte(3), false, []byte{3, 1, 3, 2, 1, 4, 2, 4})
	f.Add(byte(0), true, []byte{})
	f.Fuzz(func(t *testing.T, startByte byte, shared bool, data []byte) {
		if len(data) > 48 {
			data = data[:48]
		}
		edges := make([][2]int, len(data)/2)
		for edge := range edges {
			edges[edge] = [2]int{
				int(data[edge*2]) % 8,
				int(data[edge*2+1]) % 8,
			}
		}
		mode := RecursiveCTEReferenceLocal
		if shared {
			mode = RecursiveCTEShared
		}
		assertRecursiveDefinitionDifferential(t, int(startByte)%8, edges, mode)
	})
}
