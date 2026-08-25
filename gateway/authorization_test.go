package gateway

import (
	"context"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
	"github.com/thesyncim/vibedb/shardservice"
)

func TestForwardedShardRequestCarriesExactContextAuthorityWithoutMutatingInput(t *testing.T) {
	var node rafttransport.NodeID
	node[0] = 9
	authority := serviceauthz.Authority{Node: node, Generation: 17}
	ctx, err := serviceauthz.WithAuthority(context.Background(), authority)
	if err != nil {
		t.Fatal(err)
	}
	original := &shardservice.ShardRequest{ExecutionMode: shardservice.ExecutionReadOnly}
	forwarded := forwardedShardRequest(ctx, original)
	if forwarded == original || forwarded.Authority != authority ||
		original.Authority != (serviceauthz.Authority{}) {
		t.Fatalf("forwarded=%+v original=%+v", forwarded.Authority, original.Authority)
	}
}
