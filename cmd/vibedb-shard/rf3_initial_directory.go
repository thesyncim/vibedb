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
		return newRF3PinnedStaticRegistry(manifest, profile, members, limits)
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

// Static compositions carry their initial certificate pins in the prepared
// manifest. Dynamic membership still uses the committed physical directory.
func newRF3PinnedStaticRegistry(manifest rf3Manifest, profile *rafttransport.PeerTLS, members []rafttransport.Member, limits rafttransport.Limits) (*rafttransport.StaticRegistry, error) {
	if len(manifest.TLS.PeerKeys) == 0 {
		return nil, fmt.Errorf("%w: initial peer key pins required", errInvalidRF3Manifest)
	}
	pins := make(map[rafttransport.NodeID][32]byte, len(manifest.TLS.PeerKeys))
	for _, pin := range manifest.TLS.PeerKeys {
		var node rafttransport.NodeID
		var digest [32]byte
		if !decodeRF3FixedHex(pin.NodeID, node[:], false) || !decodeRF3FixedHex(pin.KeyDigest, digest[:], false) {
			return nil, errInvalidRF3Manifest
		}
		pins[node] = digest
	}
	if pins[profile.LocalIdentity().Node] != profile.LocalServiceKeyDigest() {
		return nil, fmt.Errorf("%w: prepared local certificate pin", errInvalidRF3Manifest)
	}
	peers := make([]rafttransport.PhysicalPeer, 0, len(members))
	seen := make(map[rafttransport.NodeID]bool, len(members))
	for _, member := range members {
		if seen[member.Node] {
			continue
		}
		seen[member.Node] = true
		digest, found := pins[member.Node]
		if !found {
			return nil, fmt.Errorf("%w: missing member certificate pin", errInvalidRF3Manifest)
		}
		peers = append(peers, rafttransport.PhysicalPeer{NodeID: member.Node, TrustDomain: profile.LocalIdentity().TrustDomain, Incarnation: 1, Revision: 1, ServiceKeyDigest: digest, Endpoint: peerAddressForRF3Member(manifest, member.MemberID), State: rafttransport.PeerEnrolled})
	}
	return rafttransport.NewStaticRegistryWithDirectory(profile.LocalIdentity().Node, members, peers, 1, limits)
}
