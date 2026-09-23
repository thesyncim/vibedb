package servicetls

import (
	"encoding/binary"
	"errors"
	"slices"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func TestNodeAuthorizerMergeBoundsUniqueIdentities(t *testing.T) {
	nodes := make([]rafttransport.NodeID, AbsoluteMaxIdentities+1)
	for index := range nodes {
		binary.BigEndian.PutUint64(nodes[index][8:], uint64(index+1))
	}
	for _, test := range []struct {
		name     string
		initial  []rafttransport.NodeID
		incoming []rafttransport.NodeID
		want     []rafttransport.NodeID
		wantErr  error
	}{
		{
			name: "full roster is idempotent", initial: nodes[:AbsoluteMaxIdentities],
			incoming: nodes[:AbsoluteMaxIdentities], want: nodes[:AbsoluteMaxIdentities],
		},
		{
			name: "overlap fills roster", initial: nodes[:AbsoluteMaxIdentities-1],
			incoming: nodes[AbsoluteMaxIdentities-2 : AbsoluteMaxIdentities], want: nodes[:AbsoluteMaxIdentities],
		},
		{
			name: "full roster accepts subset", initial: nodes[:AbsoluteMaxIdentities],
			incoming: nodes[1:3], want: nodes[:AbsoluteMaxIdentities],
		},
		{
			name: "overflow preserves roster", initial: nodes[:AbsoluteMaxIdentities],
			incoming: nodes[AbsoluteMaxIdentities-1:], want: nodes[:AbsoluteMaxIdentities], wantErr: ErrBound,
		},
		{
			name: "overflow during interleaved union preserves roster", initial: nodes[1:],
			incoming: []rafttransport.NodeID{nodes[0], nodes[AbsoluteMaxIdentities]}, want: nodes[1:], wantErr: ErrBound,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			authorizer, err := NewNodeAuthorizer(test.initial)
			if err != nil {
				t.Fatal(err)
			}
			if err := authorizer.Merge(test.incoming); !errors.Is(err, test.wantErr) {
				t.Fatalf("Merge() = %v, want %v", err, test.wantErr)
			}
			if got := authorizer.Nodes(); !slices.Equal(got, test.want) {
				t.Fatalf("merged roster differs: got %d identities, want %d", len(got), len(test.want))
			}
		})
	}
}

func TestNodeAuthorizerOwnsUniqueExactBinaryNodes(t *testing.T) {
	first, second := rafttransport.NodeID{1}, rafttransport.NodeID{2}
	input := []rafttransport.NodeID{second, first}
	authorizer, err := NewNodeAuthorizer(input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = rafttransport.NodeID{3}
	if !authorizer.allows(rafttransport.PeerIdentity{Node: first}) ||
		!authorizer.allows(rafttransport.PeerIdentity{Node: second}) ||
		authorizer.allows(rafttransport.PeerIdentity{Node: rafttransport.NodeID{3}}) {
		t.Fatal("authorizer did not retain the exact detached binary set")
	}
	for _, nodes := range [][]rafttransport.NodeID{nil, {{}}, {first, first}} {
		if _, err := NewNodeAuthorizer(nodes); !errors.Is(err, ErrInvalidProfile) {
			t.Fatalf("nodes=%v err=%v", nodes, err)
		}
	}
}

func TestNodeAuthorizationAndStatsAreAllocationFree(t *testing.T) {
	node := rafttransport.NodeID{1}
	authorizer, err := NewNodeAuthorizer([]rafttransport.NodeID{node})
	if err != nil {
		t.Fatal(err)
	}
	identity := rafttransport.PeerIdentity{Node: node}
	if allocations := testing.AllocsPerRun(1000, func() {
		if !authorizer.allows(identity) {
			panic("identity disappeared")
		}
	}); allocations != 0 {
		t.Fatalf("authorization allocations=%v", allocations)
	}
}

func TestParseNodeIDRequiresExactNonzeroHex(t *testing.T) {
	for _, value := range []string{"", "00", "00000000000000000000000000000000", "zz000000000000000000000000000000"} {
		if _, err := ParseNodeID(value); !errors.Is(err, ErrInvalidProfile) {
			t.Fatalf("value=%q err=%v", value, err)
		}
	}
	if node, err := ParseNodeID("0102030405060708090a0b0c0d0e0f10"); err != nil || node[0] != 1 || node[15] != 16 {
		t.Fatalf("node=%x err=%v", node, err)
	}
}
