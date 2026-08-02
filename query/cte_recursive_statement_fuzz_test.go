package query

import (
	"fmt"
	"sort"
	"testing"
)

func recursiveStatementGraphOracle(start int, edges [][2]int) []int {
	seen := [8]bool{}
	seen[start] = true
	result := []int{start}
	delta := []int{start}
	for len(delta) != 0 {
		candidates := make([]int, 0, len(edges))
		for _, edge := range edges {
			matches := false
			for _, node := range delta {
				if edge[0] == node {
					matches = true
					break
				}
			}
			if matches {
				candidates = append(candidates, edge[1])
			}
		}
		// The prepared recursive Statement orders each breadth-first term by
		// destination; UNION DISTINCT then preserves the first exact occurrence.
		sort.Ints(candidates)
		delta = delta[:0]
		for _, node := range candidates {
			if seen[node] {
				continue
			}
			seen[node] = true
			delta = append(delta, node)
			result = append(result, node)
		}
	}
	return result
}

func assertRecursiveStatementGraphDifferential(
	tb testing.TB,
	start int,
	edges [][2]int,
) {
	tb.Helper()
	_, snapshot := recursiveStatementDatabase(tb, edges)
	graph := prepareRecursiveStatementGraph(tb, 0, 1)
	defer graph.release()
	got, _ := executeRecursiveStatementGraph(tb, graph, snapshot, start)
	want := recursiveStatementGraphOracle(start, edges)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		tb.Fatalf("prepared Statement closure = %v, want %v for edges %v",
			got, want, edges)
	}
}

func TestRecursiveCTEStatementRandomizedDifferential(t *testing.T) {
	state := uint64(0x9e3779b97f4a7c15)
	next := func() uint64 {
		state ^= state << 7
		state ^= state >> 9
		return state
	}
	for trial := 0; trial < 80; trial++ {
		edges := make([][2]int, int(next()%25))
		for edge := range edges {
			edges[edge] = [2]int{int(next() % 8), int(next() % 8)}
		}
		assertRecursiveStatementGraphDifferential(t, int(next()%8), edges)
	}
}

func FuzzRecursiveCTEStatementGraphDifferential(f *testing.F) {
	f.Add(byte(0), []byte{0, 1, 1, 2, 2, 3, 3, 0})
	f.Add(byte(3), []byte{3, 1, 3, 2, 1, 4, 2, 4})
	f.Add(byte(0), []byte{})
	f.Fuzz(func(t *testing.T, startByte byte, data []byte) {
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
		assertRecursiveStatementGraphDifferential(
			t, int(startByte)%8, edges,
		)
	})
}
