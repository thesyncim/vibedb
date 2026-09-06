package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// CatalogServiceFences grants the registered gateway sessions only the exact
// catalog group and relation owned by this authenticated catalog authority.
// Receivers derive the same scope from the decoded request and serving fence.
func (authority *ReplicatedCatalogAuthority) CatalogServiceFences(ctx context.Context) ([]serviceauthz.ServiceFence, error) {
	if authority == nil || ctx == nil {
		return nil, ErrReplicatedCatalog
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return nil, err
	}
	route, err := authority.executor.catalogOperationalRoute(ctx, authority.route, authority.holder.Current())
	if err != nil {
		return nil, err
	}
	digest := route.Command.RelationManifestDigest
	var manifest, relation [16]byte
	copy(manifest[:], digest[:16])
	binary.BigEndian.PutUint16(relation[14:], uint16(authority.relation))
	read := serviceauthz.ServiceFence{Action: serviceauthz.ServiceActionGatewayCatalogRead, Operation: serviceauthz.ServiceOperationCatalogRead, Group: route.Group, Relation: manifest, IntentID: digest, FenceDigest: digest}
	point := read
	point.Relation = relation
	write := read
	write.Action = serviceauthz.ServiceActionGatewayCatalogWrite
	write.Operation = serviceauthz.ServiceOperationCatalogWrite
	if bytes.Compare(read.Relation[:], point.Relation[:]) < 0 {
		return []serviceauthz.ServiceFence{read, point, write}, nil
	}
	return []serviceauthz.ServiceFence{point, read, write}, nil
}
