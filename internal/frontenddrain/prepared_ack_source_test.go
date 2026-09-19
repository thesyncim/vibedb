package frontenddrain

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

func preparedAckCutReadTestRequest(t *testing.T) (PreparedAckCutReadRequest, PreparedAckCut) {
	t.Helper()
	cut, grantDigest := preparedAckTestCut(t)
	request := PreparedAckCutReadRequest{
		Operation:                CutOperationInstallExact,
		Nonce:                    [16]byte{31},
		DrainID:                  [32]byte{9},
		GrantDigest:              grantDigest,
		ReceiverNode:             rafttransport.NodeID{3},
		ReceiverIncarnation:      2,
		ReceiverServiceKeyDigest: [32]byte{10},
		ReceiverNodeRevision:     28,
		SourceFloor:              cut.ReadFloor(),
		SourceCutDigest:          cut.Digest(),
	}
	return request, cut
}

func TestPreparedAckCutReadRequestRoundTripBindsReceiverAndDigest(t *testing.T) {
	request, _ := preparedAckCutReadTestRequest(t)
	raw := request.Marshal()
	if len(raw) != PreparedAckCutReadRequestBytes ||
		!bytes.Equal(raw[:8], PreparedAckCutReadDiscriminator[:]) {
		t.Fatalf("source-cut query length/discriminator = %d/%q", len(raw), raw[:8])
	}
	opened, err := OpenPreparedAckCutReadRequest(raw)
	if err != nil || opened != request {
		t.Fatalf("opened source-cut query = %+v, err=%v", opened, err)
	}
	for _, mutate := range []func([]byte){
		func(raw []byte) { raw[104]++ },
		func(raw []byte) { raw[len(raw)-1]++ },
	} {
		mutated := append([]byte(nil), raw...)
		mutate(mutated)
		if _, err := OpenPreparedAckCutReadRequest(mutated); !errors.Is(err, ErrPreparedAckWire) {
			t.Fatalf("mutated source-cut query accepted: %v", err)
		}
	}
}

func TestPreparedAckCutDigestBindsSourceRosterIdentityAndEndpoint(t *testing.T) {
	_, cut := preparedAckCutReadTestRequest(t)
	cut.SourceRoster = []PreparedAckSource{{
		NodeID: rafttransport.NodeID{40}, Incarnation: 2, ControlAddress: "source-control",
		SPKIPinDigest: [32]byte{41},
	}}
	if !cut.Valid() {
		t.Fatal("source roster cut is invalid")
	}
	digest := cut.Digest()
	if digest == ([32]byte{}) {
		t.Fatal("source roster cut has zero digest")
	}
	changed := cut
	changed.SourceRoster = append([]PreparedAckSource(nil), cut.SourceRoster...)
	changed.SourceRoster[0].ControlAddress = "replacement-control"
	if changed.Digest() == digest {
		t.Fatal("source roster endpoint mutation did not change cut digest")
	}
	changed = cut
	changed.SourceRoster = append([]PreparedAckSource(nil), cut.SourceRoster...)
	changed.SourceRoster[0].NodeID = rafttransport.NodeID{42}
	if changed.Digest() == digest {
		t.Fatal("source roster identity mutation did not change cut digest")
	}
}

func TestPreparedAckCutReadResponseRoundTripRejectsDifferentCut(t *testing.T) {
	request, cut := preparedAckCutReadTestRequest(t)
	response := PreparedAckCutReadResponse{
		Operation: request.Operation,
		Nonce:     request.Nonce, DrainID: request.DrainID, GrantDigest: request.GrantDigest,
		ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
		ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest,
		ReceiverNodeRevision:     request.ReceiverNodeRevision,
		DirectoryRevision:        cut.DirectoryRevision, DirectoryDigest: cut.DirectoryDigest,
		CatalogGeneration: cut.CatalogGeneration, CatalogHeadDigest: cut.CatalogHeadDigest,
		ServiceDirectoryRevision: cut.ServiceDirectoryRevision,
		ServiceDirectoryDigest:   cut.ServiceDirectoryDigestValue(), SourceCutDigest: request.SourceCutDigest,
		Cut: cut,
	}
	encoded, err := response.Marshal()
	response.SourceCutDigest = response.Cut.Digest()
	if err != nil || !response.Valid(request) {
		t.Fatalf("source-cut response marshal/valid err=%v response=%+v request=%+v cutdigest=%x reqdigest=%x floordigest=%x", err, response, request, response.Cut.Digest(), request.SourceCutDigest, request.SourceFloor.ServiceDirectoryDigest)
	}
	opened, err := OpenPreparedAckCutReadResponse(encoded, request)
	if err != nil || opened.Nonce != response.Nonce || opened.DrainID != response.DrainID ||
		opened.GrantDigest != response.GrantDigest || opened.ReceiverNode != response.ReceiverNode ||
		opened.ReceiverIncarnation != response.ReceiverIncarnation ||
		opened.ReceiverServiceKeyDigest != response.ReceiverServiceKeyDigest ||
		opened.ReceiverNodeRevision != response.ReceiverNodeRevision ||
		opened.SourceCutDigest != response.SourceCutDigest || opened.Cut.Digest() != response.Cut.Digest() {
		t.Fatalf("opened source-cut response = %+v, err=%v", opened, err)
	}
	mutated := response
	mutated.Cut.DirectoryRevision++
	if _, err := mutated.Marshal(); !errors.Is(err, ErrPreparedAckWire) {
		t.Fatalf("response with digest-mismatched cut encoded: %v", err)
	}
	mutated = response
	mutated.SourceCutDigest[0]++
	if _, err := mutated.Marshal(); !errors.Is(err, ErrPreparedAckWire) {
		t.Fatalf("response with forged source digest encoded: %v", err)
	}
}

func TestPreparedAckCutReadInstallExactAllowsAbsentDrainSubject(t *testing.T) {
	request, cut := preparedAckCutReadTestRequest(t)
	request.DrainID = [32]byte{}
	request.GrantDigest = [32]byte{}
	request.RequirePrepared = false
	request.SourceFloor = cut.ReadFloor()
	request.SourceCutDigest = cut.Digest()
	if !request.Valid() {
		t.Fatal("no-drain InstallExact request rejected by canonical shape")
	}
	response := PreparedAckCutReadResponse{
		Operation: request.Operation, RequirePrepared: false,
		Nonce: request.Nonce, ReceiverNode: request.ReceiverNode,
		ReceiverIncarnation:      request.ReceiverIncarnation,
		ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest,
		ReceiverNodeRevision:     request.ReceiverNodeRevision,
		DirectoryRevision:        cut.DirectoryRevision, DirectoryDigest: cut.DirectoryDigest,
		CatalogGeneration: cut.CatalogGeneration, CatalogHeadDigest: cut.CatalogHeadDigest,
		ServiceDirectoryRevision: cut.ServiceDirectoryRevision,
		ServiceDirectoryDigest:   cut.ServiceDirectoryDigestValue(), SourceCutDigest: cut.Digest(), Cut: cut,
	}
	raw, err := response.Marshal()
	if err != nil {
		t.Fatalf("marshal no-drain InstallExact response: %v", err)
	}
	opened, err := OpenPreparedAckCutReadResponse(raw, request)
	if err != nil || !opened.Valid(request) || opened.DrainID != ([32]byte{}) || opened.GrantDigest != ([32]byte{}) {
		t.Fatalf("no-drain InstallExact response=%+v err=%v", opened, err)
	}
}

func TestPreparedAckCutReadMovedResponseBindsNewerFloor(t *testing.T) {
	request, cut := preparedAckCutReadTestRequest(t)
	floor := cut.ReadFloor()
	floor.DirectoryRevision++
	response := PreparedAckCutReadMovedResponse{
		Operation: request.Operation, RequirePrepared: request.RequirePrepared,
		Nonce: request.Nonce, DrainID: request.DrainID, GrantDigest: request.GrantDigest,
		ReceiverNode: request.ReceiverNode, ReceiverIncarnation: request.ReceiverIncarnation,
		ReceiverServiceKeyDigest: request.ReceiverServiceKeyDigest,
		ReceiverNodeRevision:     request.ReceiverNodeRevision, SourceFloor: floor,
		SourceCutDigest: [32]byte{77},
	}
	raw, err := response.Marshal()
	if err != nil || !response.Valid(request) {
		t.Fatalf("moved response marshal/valid err=%v response=%+v", err, response)
	}
	opened, err := OpenPreparedAckCutReadMovedResponse(raw, request)
	if err != nil || !opened.Valid(request) || opened.SourceFloor != floor || opened.SourceCutDigest != response.SourceCutDigest {
		t.Fatalf("opened moved response=%+v err=%v", opened, err)
	}
	mutatedRaw := append([]byte(nil), raw...)
	mutatedRaw[176]++
	if _, err := OpenPreparedAckCutReadMovedResponse(mutatedRaw, request); err == nil {
		t.Fatal("moved response with tampered floor accepted")
	}
}
