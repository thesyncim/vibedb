package shardservice

import (
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestReplicatedGatewayAllowlistRequiresUniqueBinaryNodes(t *testing.T) {
	node := rafttransport.NodeID{1}
	allowed, err := replicatedGatewayAllowlist([]rafttransport.NodeID{node, {2}})
	if err != nil || len(allowed) != 2 {
		t.Fatalf("allowlist = %v, %v", allowed, err)
	}
	for _, input := range [][]rafttransport.NodeID{nil, {{0}}, {node, node}} {
		if _, err := replicatedGatewayAllowlist(input); !errors.Is(err, ErrReplicatedAuthentication) {
			t.Fatalf("input %v error = %v", input, err)
		}
	}
}
