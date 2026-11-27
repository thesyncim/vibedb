package servicetls

import (
	"encoding/binary"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func BenchmarkNodeAuthorizerAllowsParallel(b *testing.B) {
	nodes := make([]rafttransport.NodeID, 64)
	for i := range nodes {
		binary.LittleEndian.PutUint64(nodes[i][:8], uint64(i+1))
	}
	auth, err := NewNodeAuthorizer(nodes)
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]rafttransport.PeerIdentity, len(nodes))
	for i, n := range nodes {
		ids[i] = rafttransport.PeerIdentity{Node: n}
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if !auth.allows(ids[i&63]) {
				b.Fatal("expected authorized")
			}
			i++
		}
	})
}
