package main

import (
	"bytes"
	"fmt"
	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibejson"
)

func loadRF3InitialNodeDirectory(path string) ([]gateway.NodeRecord, error) {
	if path == "" {
		return nil, nil
	}
	raw, err := readRF3ManifestFile(path)
	if err != nil {
		return nil, err
	}
	var records []gateway.NodeRecord
	if err := vibejson.Unmarshal(raw, &records); err != nil {
		return nil, err
	}
	if len(records) == 0 || len(records) > gateway.MaxScalingNodes {
		return nil, errInvalidRF3Manifest
	}
	canonical, err := vibejson.Marshal(&records)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, errInvalidRF3Manifest
	}
	for i, record := range records {
		if !record.Valid() || record.Lifecycle != gateway.NodeActive || record.Revision != 1 || i > 0 && bytes.Compare(records[i-1].NodeID[:], record.NodeID[:]) >= 0 {
			return nil, errInvalidRF3Manifest
		}
	}
	return records, nil
}

func newRF3ProvisionedRegistry(manifest rf3Manifest, profile *rafttransport.PeerTLS, members []rafttransport.Member, limits rafttransport.Limits) (*rafttransport.StaticRegistry, error) {
	if manifest.Gateway == nil || manifest.Gateway.InitialNodeDirectoryPath == "" {
		return rafttransport.NewStaticRegistry(profile.LocalIdentity().Node, members, limits)
	}
	records, err := loadRF3InitialNodeDirectory(manifest.Gateway.InitialNodeDirectoryPath)
	if err != nil {
		return nil, err
	}
	peers := make([]rafttransport.PhysicalPeer, 0, len(records))
	for _, record := range records {
		if record.NodeID == profile.LocalIdentity().Node && [32]byte(record.ServiceKeyDigest) != profile.LocalServiceKeyDigest() {
			return nil, fmt.Errorf("%w: prepared local certificate pin", errInvalidRF3Manifest)
		}
		peers = append(peers, rafttransport.PhysicalPeer{NodeID: record.NodeID, TrustDomain: profile.LocalIdentity().TrustDomain, Incarnation: record.Incarnation, Revision: record.Revision, ServiceKeyDigest: [32]byte(record.ServiceKeyDigest), Endpoint: record.DataAddress, State: rafttransport.PeerEnrolled})
	}
	return rafttransport.NewStaticRegistryWithDirectory(profile.LocalIdentity().Node, members, peers, 1, limits)
}
