package query

import (
	"fmt"
	"sort"
	"testing"
)

func assertRecursiveSQLGraphDifferential(
	tb testing.TB,
	start int,
	edges [][2]int,
) {
	tb.Helper()
	_, snapshot := recursiveStatementDatabase(tb, edges)
	statement := prepareRecursiveSQLGraph(tb)
	defer statement.Release()
	var exec Exec
	got := runRecursiveSQLGraph(
		tb, statement, &exec,
		FromDatabase(snapshot, statement.Collection()), start,
	)
	want := recursiveStatementGraphOracle(start, edges)
	sort.Ints(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		tb.Fatalf("recursive SQL rows = %v, want %v for edges %v", got, want, edges)
	}
}

func TestRecursiveSQLBridgeRandomizedDifferential(t *testing.T) {
	state := uint64(0x9e3779b97f4a7c15)
	next := func() uint64 {
		state ^= state << 7
		state ^= state >> 9
		return state
	}
	for trial := 0; trial < 40; trial++ {
		edges := make([][2]int, int(next()%24))
		for i := range edges {
			edges[i] = [2]int{int(next() % 8), int(next() % 8)}
		}
		assertRecursiveSQLGraphDifferential(t, int(next()%8), edges)
	}
}

func FuzzRecursiveSQLBridgeDifferential(f *testing.F) {
	f.Add(byte(0), []byte{0, 1, 1, 2, 2, 3})
	f.Add(byte(3), []byte{3, 4, 3, 5, 4, 6, 5, 6})
	f.Add(byte(0), []byte{})
	f.Fuzz(func(t *testing.T, start byte, data []byte) {
		if len(data) > 48 {
			data = data[:48]
		}
		edges := make([][2]int, len(data)/2)
		for i := range edges {
			edges[i] = [2]int{int(data[i*2]) % 8, int(data[i*2+1]) % 8}
		}
		assertRecursiveSQLGraphDifferential(t, int(start)%8, edges)
	})
}
