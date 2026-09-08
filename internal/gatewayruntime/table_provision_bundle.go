package gatewayruntime

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibejson"
)

const gatewayTableProvisionBundleSuffix = "-split-source.vibejson"

type gatewayTableCatalogRegistration struct {
	Addition *gateway.Snapshot
	Source   *gatewaySplitSource
}

// openGatewayTableCatalog accepts the explicit legacy fragment grammar and
// the newer paired local bundle grammar. The filename selects the grammar so
// a crash after a new plan's fragment write cannot silently downgrade that
// plan to legacy startup.
func openGatewayTableCatalog(path string) (gatewayTableCatalogRegistration, error) {
	file, err := os.Open(path)
	if err != nil {
		return gatewayTableCatalogRegistration{}, err
	}
	raw, readErr := io.ReadAll(io.LimitReader(file, (4<<20)+1))
	closeErr := file.Close()
	if err = errors.Join(readErr, closeErr); err != nil {
		return gatewayTableCatalogRegistration{}, err
	}
	if strings.HasSuffix(filepath.Base(path), gatewayTableProvisionBundleSuffix) {
		catalogRaw, sourceRaw, err := gateway.OpenReplicatedTableProvisionBundle(raw)
		if err != nil {
			return gatewayTableCatalogRegistration{}, err
		}
		addition, err := gateway.OpenReplicatedTableProvision(catalogRaw)
		if err != nil {
			return gatewayTableCatalogRegistration{}, err
		}
		source, err := openGatewayProvisionedSplitSource(sourceRaw)
		if err != nil {
			return gatewayTableCatalogRegistration{}, err
		}
		return gatewayTableCatalogRegistration{Addition: addition, Source: &source}, nil
	}
	addition, err := gateway.OpenReplicatedTableProvision(raw)
	if err != nil {
		return gatewayTableCatalogRegistration{}, err
	}
	return gatewayTableCatalogRegistration{Addition: addition}, nil
}

// openGatewayProvisionedSplitSource validates the canonical source grammar
// and its internal SQL/machine schema proof. Roster/catalog validation is
// deliberately repeated by gatewayHotSplitFactory against the matching
// immutable fragment once startup has loaded the enrolled roster.
func openGatewayProvisionedSplitSource(raw []byte) (gatewaySplitSource, error) {
	var encoded persistedGatewaySplitSource
	if len(raw) == 0 || len(raw) > 4<<20 || vibejson.Unmarshal(raw, &encoded) != nil {
		return gatewaySplitSource{}, errGatewayHotSplitSourceRegistration
	}
	canonical, err := vibejson.Marshal(&encoded)
	if err != nil || !bytes.Equal(canonical, raw) {
		return gatewaySplitSource{}, errGatewayHotSplitSourceRegistration
	}
	endpoints := make([]gateway.ReplicatedEndpoint, len(encoded.Replicas))
	seen := make(map[rafttransport.NodeID]struct{}, len(encoded.Replicas))
	for index, replica := range encoded.Replicas {
		node, parseErr := parseGatewayReplicaNode(replica.Node)
		if parseErr != nil {
			return gatewaySplitSource{}, errGatewayHotSplitSourceRegistration
		}
		if _, duplicate := seen[node]; duplicate {
			return gatewaySplitSource{}, errGatewayHotSplitSourceRegistration
		}
		seen[node] = struct{}{}
		endpoints[index] = gateway.ReplicatedEndpoint{Node: node}
	}
	slices.SortFunc(endpoints, func(left, right gateway.ReplicatedEndpoint) int {
		return bytes.Compare(left.Node[:], right.Node[:])
	})
	opened, err := openGatewaySplitSources([]persistedGatewaySplitSource{encoded}, endpoints)
	if err != nil || len(opened) != 1 {
		return gatewaySplitSource{}, errors.Join(errGatewayHotSplitSourceRegistration, err)
	}
	return opened[0], nil
}
