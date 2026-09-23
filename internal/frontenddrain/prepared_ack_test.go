package frontenddrain

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/raftmember"
	"github.com/thesyncim/vibedb/internal/rafttransport"
	"github.com/thesyncim/vibedb/internal/serviceauthz"
)

func preparedAckTestCut(t *testing.T) (PreparedAckCut, [32]byte) {
	t.Helper()
	trust := rafttransport.TrustDomain{ClusterID: [16]byte{1}, ClusterIncarnation: [16]byte{2}}
	storage := rafttransport.NodeID{3}
	gateway := rafttransport.NodeID{4}
	group := raftmember.GroupKey{ClusterID: [16]byte{5}, ClusterIncarnation: [16]byte{6}, ShardIncarnation: [16]byte{7}, GroupID: [16]byte{8}}
	drainID := [32]byte{9}
	scope := serviceauthz.FrontendContinuationScopeRecord{
		Protocol: serviceauthz.FrontendScopeNative, Action: serviceauthz.FrontendActionForwardedData,
		Capability: serviceauthz.CapabilityDataRead, Operation: serviceauthz.ServiceOperationForwardedRead,
		Group: group,
	}
	grant, err := serviceauthz.NewCommittedFrontendContinuationGrant(serviceauthz.CommittedFrontendContinuationGrant{
		TrustDomain: trust, PhysicalNode: storage, PhysicalIncarnation: 2,
		PeerKeyDigest: [32]byte{11}, GatewayServiceID: gateway,
		GatewaySessionID: [16]byte{12}, GatewaySessionRevision: 13, DrainID: drainID,
		AdmissionEpoch: 14, AcceptedConnectionTokens: []serviceauthz.FrontendConnToken{{15}},
		AcceptedConnectionProtocols: []serviceauthz.FrontendContinuationScope{serviceauthz.FrontendScopeNative},
		AdmissionClosedProofDigest:  [32]byte{16}, Revision: 17,
		State: serviceauthz.ContinuationGrantPrepared,
	})
	if err != nil {
		t.Fatal(err)
	}
	bindings := []serviceauthz.ServiceBinding{
		{Principal: storage, PhysicalNode: storage, PhysicalIncarnation: 2, KeyDigest: [32]byte{10},
			Roles: serviceauthz.ServiceRoleStorage, Lifecycle: serviceauthz.ServiceActive},
		{Principal: gateway, PhysicalNode: storage, PhysicalIncarnation: 2, KeyDigest: [32]byte{11},
			Roles: serviceauthz.ServiceRoleGateway, Lifecycle: serviceauthz.ServiceActive,
			GatewayIncarnation: 3, SessionID: [16]byte{12}, SessionRevision: 13, ParticipantDigest: [32]byte{18}},
	}
	cut := PreparedAckCut{
		DirectoryRevision: 19, DirectoryDigest: [32]byte{20}, CatalogGeneration: 21,
		CatalogHeadDigest: [32]byte{22}, ServiceDirectoryRevision: 23,
		ServiceDirectory: serviceauthz.ServiceDirectoryCut{
			CatalogGeneration: 21, Revision: 23, TrustDomain: trust, PolicyGeneration: 24,
			Bindings: bindings, ForwardedScopes: []serviceauthz.FrontendContinuationScopeRecord{scope},
			ContinuationGrants: []serviceauthz.CommittedFrontendContinuationGrant{grant},
		},
	}
	request := [32]byte{}
	copy(request[:], grant.GrantDigest[:])
	return cut, request
}

func TestPreparedAckRequestRoundTripBindsCanonicalCut(t *testing.T) {
	cut, grantDigest := preparedAckTestCut(t)
	request := PreparedAckRequest{
		Nonce: [16]byte{25}, DrainID: [32]byte{9}, GrantDigest: grantDigest,
		SourcePrincipal: rafttransport.NodeID{26}, SourcePrincipalKeyDigest: [32]byte{27},
		ReceiverNode: rafttransport.NodeID{3}, ReceiverIncarnation: 2,
		ReceiverServiceKeyDigest: [32]byte{10}, ReceiverNodeRevision: 28, SourceCut: cut,
	}
	raw, err := request.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= preparedAckHeaderBytes+preparedAckDigestBytes || !bytes.Equal(raw[:8], PreparedAckDiscriminator[:]) {
		t.Fatalf("invalid prepared ACK frame length=%d", len(raw))
	}
	opened, err := OpenPreparedAckRequest(raw)
	if err != nil || opened.SourceCut.Digest() != request.SourceCut.Digest() {
		t.Fatalf("opened request=%+v err=%v", opened, err)
	}
	if opened.SourceCutDigest() != request.SourceCutDigest() {
		t.Fatal("source cut digest changed across canonical round trip")
	}
	mutated := append([]byte(nil), raw...)
	mutated[240]++ // catalog generation in the authenticated header
	if _, err := OpenPreparedAckRequest(mutated); !errors.Is(err, ErrPreparedAckWire) {
		t.Fatalf("header mutation accepted: %v", err)
	}
	mutated = append([]byte(nil), raw...)
	mutated[len(mutated)-1]++ // frame digest
	if _, err := OpenPreparedAckRequest(mutated); !errors.Is(err, ErrPreparedAckWire) {
		t.Fatalf("frame digest mutation accepted: %v", err)
	}
}

func TestPreparedAckResponseBindsRequestAndAppliedRevision(t *testing.T) {
	cut, grantDigest := preparedAckTestCut(t)
	request := PreparedAckRequest{Nonce: [16]byte{25}, DrainID: [32]byte{9}, GrantDigest: grantDigest,
		SourcePrincipal: rafttransport.NodeID{26}, SourcePrincipalKeyDigest: [32]byte{27},
		ReceiverNode: rafttransport.NodeID{3}, ReceiverIncarnation: 2, ReceiverServiceKeyDigest: [32]byte{10},
		ReceiverNodeRevision: 28, SourceCut: cut}
	response := PreparedAckResponse{
		Nonce: request.Nonce, DrainID: request.DrainID, GrantDigest: request.GrantDigest,
		ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
		ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest,
		ReceiverNodeRevision:     request.ReceiverNodeRevision,
		DirectoryRevision:        cut.DirectoryRevision, DirectoryDigest: cut.DirectoryDigest,
		CatalogGeneration: cut.CatalogGeneration, CatalogHeadDigest: cut.CatalogHeadDigest,
		ServiceDirectoryRevision: cut.ServiceDirectoryRevision,
		ServiceDirectoryDigest:   cut.ServiceDirectoryDigestValue(), AppliedRevision: 29,
		SourceCutDigest: request.SourceCutDigest(),
	}
	if !response.Valid(request) {
		t.Fatal("valid response rejected")
	}
	opened, err := OpenPreparedAckResponse(response.Marshal(), request)
	if err != nil || opened != response {
		t.Fatalf("opened response=%+v err=%v", opened, err)
	}
	wrong := response
	wrong.ReceiverIncarnation++
	if _, err := OpenPreparedAckResponse(wrong.Marshal(), request); !errors.Is(err, ErrPreparedAckWire) {
		t.Fatalf("wrong receiver incarnation accepted: %v", err)
	}
	wrong = response
	wrong.AppliedRevision = 0
	if _, err := OpenPreparedAckResponse(wrong.Marshal(), request); !errors.Is(err, ErrPreparedAckWire) {
		t.Fatalf("zero applied revision accepted: %v", err)
	}
}
