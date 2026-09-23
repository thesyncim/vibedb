package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/servicetls"
	"github.com/thesyncim/vibejson"
)

// Provision the original directory from the prepared inventory and its exact
// authenticated certificates. The immutable file is shared by all frontends;
// runtime startup cannot manufacture membership or capacity records.
func writeDevInitialNodeDirectory(cluster devClusterManifest) error {
	path := filepath.Join(filepath.Dir(cluster.CatalogPath), "initial-node-directory.vibejson")
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	snapshot, err := gateway.LoadSnapshot(cluster.CatalogPath)
	if err != nil {
		return err
	}
	records := make([]gateway.NodeRecord, 0, len(cluster.NodeManifests))
	for index, node := range cluster.NodeManifests {
		physical, err := servicetls.LoadProfile(node.Certificate, node.Key, cluster.Roots, devClusterOID, time.Now)
		if err != nil {
			return err
		}
		frontend, err := servicetls.LoadProfile(node.GatewayCertificate, node.GatewayKey, cluster.Roots, devClusterOID, time.Now)
		if err != nil {
			return err
		}
		if physical.LocalIdentity().TrustDomain != frontend.LocalIdentity().TrustDomain {
			return errDevCluster
		}
		record := gateway.NodeRecord{NodeID: physical.LocalIdentity().Node, Incarnation: 1,
			ServiceKeyDigest: replication.Digest(physical.LocalServiceKeyDigest()),
			Roles:            gateway.NodeRoleStorage | gateway.NodeRoleControl | gateway.NodeRoleGateway,
			FailureDomain:    fmt.Sprintf("dev-node-%d", index+1), Lifecycle: gateway.NodeActive, Revision: 1, CatalogGeneration: snapshot.Generation(),
			GatewayEndpoint: distribution.EndpointID(fmt.Sprintf("gateway-%d-control", index+1)), GatewayAddress: node.GatewayControl,
			Gateway: gateway.GatewayIdentity{NodeID: frontend.LocalIdentity().Node, Incarnation: 1, ServiceKeyDigest: replication.Digest(frontend.LocalServiceKeyDigest()), SessionRevision: 1}}
		// Provision durable service/session IDs once, independently of physical IDs.
		var service, session [16]byte
		if _, err := rand.Read(service[:]); err != nil {
			return err
		}
		if _, err := rand.Read(session[:]); err != nil {
			return err
		}
		record.Gateway.ServiceID, record.Gateway.SessionID = service, session
		raw, err := vibejson.Marshal(&record.Gateway)
		if err != nil {
			return err
		}
		record.Gateway.ParticipantDigest = sha256.Sum256(raw)
		found := false
		for roleIndex, members := range [][]devClusterMember{cluster.Members, cluster.LedgerMembers, cluster.DataMembers} {
			for ordinal, member := range members {
				if member.Node != node.Node {
					continue
				}
				if roleIndex == 0 {
					record.Roles |= gateway.NodeRoleCatalog
				}
				if found {
					continue
				}
				prefix := fmt.Sprintf("%s-member-%d", []string{"catalog", "ledger", "data"}[roleIndex], ordinal+1)
				record.DataEndpoint, record.NativeEndpoint, record.ControlEndpoint = distribution.EndpointID(prefix), distribution.EndpointID(prefix+"-native"), distribution.EndpointID(prefix+"-control")
				record.DataAddress, record.NativeAddress, record.ControlAddress = member.Peer, member.Native, member.Control
				found = true
			}
		}
		// Capacity remains unknown until the node-info observer supplies measured
		// evidence. Zero capacity prevents placement from assuming spare resources.
		if !found || !record.Valid() {
			return errDevCluster
		}
		records = append(records, record)
	}
	slices.SortFunc(records, func(a, b gateway.NodeRecord) int { return bytes.Compare(a.NodeID[:], b.NodeID[:]) })
	raw, err := vibejson.Marshal(&records)
	if err != nil {
		return err
	}
	return writeDevExclusive(path, raw, 0600)
}
