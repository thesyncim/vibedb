package shardcontrol

import (
	"bytes"
	"testing"
)

func testRequest() Request {
	return Request{
		Action: ActionSealSource, Child: 1,
		Operation: [32]byte{1}, Step: [32]byte{2}, PlanDigest: [32]byte{3},
		Fence: Fence{
			CatalogGeneration: 4, Allocation: 5, OwnershipEpoch: 6,
			SchemaGeneration: 7, RoutingVersion: 8, RouteGeneration: 9,
			ReplicaSetVersion: 10, Applied: 11,
		},
		Payload: []byte(`{"cut":11,"sealed":true}`),
	}
}

func TestRequestCanonicalRoundTripAndDamage(t *testing.T) {
	request := testRequest()
	frame, err := AppendRequest(nil, &request)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenRequest(frame)
	if err != nil || opened.Action != request.Action || opened.Child != request.Child ||
		opened.Operation != request.Operation || opened.Step != request.Step ||
		opened.PlanDigest != request.PlanDigest || opened.Fence != request.Fence ||
		!bytes.Equal(opened.Payload, request.Payload) || len(opened.Payload) != cap(opened.Payload) {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	again, err := AppendRequest(nil, &opened)
	if err != nil || !bytes.Equal(frame, again) {
		t.Fatal("request did not retain one canonical frame")
	}
	for _, damage := range []func([]byte){
		func(raw []byte) { raw[0]++ },
		func(raw []byte) { raw[5]++ },
		func(raw []byte) { raw[8] = 1 },
		func(raw []byte) { raw[9] = 0 },
		func(raw []byte) { raw[173] = 0 },
	} {
		broken := append([]byte(nil), frame...)
		damage(broken)
		if _, err = OpenRequest(broken); err == nil {
			t.Fatal("damaged request accepted")
		}
	}
	if _, err = OpenRequest(append(append([]byte(nil), frame...), 0)); err == nil {
		t.Fatal("trailing request byte accepted")
	}
	noncanonical := request
	noncanonical.Payload = []byte(`{ "sealed": true, "cut": 11 }`)
	if _, err = AppendRequest(nil, &noncanonical); err == nil {
		t.Fatal("noncanonical payload accepted")
	}
	oversized := request
	oversized.Payload = make([]byte, MaxPayloadBytes+1)
	if _, err = AppendRequest(nil, &oversized); err == nil {
		t.Fatal("oversized payload accepted")
	}
}

func TestResponseCanonicalRoundTripAndBoundedRead(t *testing.T) {
	response := Response{
		Code: ResultAccepted, Operation: [32]byte{1}, Step: [32]byte{2},
		ResultDigest: [32]byte{3}, Payload: []byte(`{"applied":42}`),
	}
	frame, err := AppendResponse(nil, &response)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := ReadResponse(bytes.NewReader(frame))
	if err != nil || opened.Code != response.Code || opened.Operation != response.Operation ||
		opened.Step != response.Step || opened.ResultDigest != response.ResultDigest ||
		!bytes.Equal(opened.Payload, response.Payload) {
		t.Fatalf("opened=%+v err=%v", opened, err)
	}
	var oversized [5]byte
	oversized[0] = responseTag
	for index := 1; index < len(oversized); index++ {
		oversized[index] = 0xff
	}
	if _, err = ReadResponse(bytes.NewReader(oversized[:])); err == nil {
		t.Fatal("oversized frame header accepted")
	}
}
