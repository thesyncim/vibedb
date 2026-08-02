//go:build 386

package query

import (
	"errors"
	"math"
	"testing"
)

func TestRecursiveCTEDefinitionRejectsTermRangeOverflow386(t *testing.T) {
	owner, err := PrepareStatement(`
		WITH reachable(node) AS MATERIALIZED (
			SELECT node FROM seeds WHERE node = ? OR node = ?
		)
		SELECT node FROM reachable`)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Release()
	graph := prepareRecursiveStatementGraph(t, 0, 1)
	defer graph.release()
	graph.anchor.paramBase = math.MaxInt
	descriptor, err := PrepareRecursiveCTEDescriptor(
		"reachable", []string{"node"}, graph.anchor, graph.recursive,
		RecursiveUnionDistinct, RecursiveCTEShared, RecursiveCTELimits{},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = installStatementRecursiveDefinition(
		owner, owner.cteCatalog().defs[0], descriptor, "seeds",
	)
	if !errors.Is(err, errStatementRecursiveDefinition) {
		t.Fatalf("386 term range overflow error = %v", err)
	}
}
