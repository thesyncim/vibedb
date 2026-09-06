package gateway

import (
	"context"
	"encoding/binary"
	"slices"

	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

// CatalogServiceFences grants the registered gateway sessions only the exact
// catalog and ledger groups in the already published authenticated catalog cut.
// Receivers derive the same scope from the decoded request and serving fence.
func (authority *ReplicatedCatalogAuthority) CatalogServiceFences(ctx context.Context) ([]serviceauthz.ServiceFence, uint64, error) {
	if authority == nil || ctx == nil {
		return nil, 0, ErrReplicatedCatalog
	}
	ctx, err := authority.authorizedContext(ctx)
	if err != nil {
		return nil, 0, err
	}
	snapshot := authority.holder.Current()
	if snapshot == nil {
		return nil, 0, ErrReplicatedCatalog
	}
	route, err := authority.executor.catalogOperationalRoute(ctx, authority.route, snapshot)
	if err != nil {
		return nil, 0, err
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
	fences := []serviceauthz.ServiceFence{read, point, write}
	for _, descriptor := range snapshot.replicatedDescriptors() {
		if len(descriptor.RequestLedgerRanges) == 0 {
			continue
		}
		digest := descriptor.Command.RelationManifestDigest
		var relation [16]byte
		copy(relation[:], digest[:16])
		fences = append(fences, serviceauthz.ServiceFence{
			Action:    serviceauthz.ServiceActionGatewayRequestLedger,
			Operation: serviceauthz.ServiceOperationRequestLedger,
			Group:     descriptor.Group, Relation: relation, IntentID: digest, FenceDigest: digest,
		})
	}
	slices.SortFunc(fences, serviceauthz.CompareServiceFences)
	fences = slices.Compact(fences)
	return fences, snapshot.Generation(), nil
}
