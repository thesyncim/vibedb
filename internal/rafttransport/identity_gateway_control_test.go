package rafttransport

import "testing"

func TestGatewayControlTrafficClassHasIsolatedALPN(t *testing.T) {
	control, err := TrafficGatewayControl.alpn()
	if err != nil || control == "" {
		t.Fatalf("gateway control ALPN=%q err=%v", control, err)
	}
	for _, class := range []TrafficClass{
		TrafficOrdinary, TrafficSnapshot, TrafficShardNative,
		TrafficGatewayClient, TrafficShardSQL, TrafficShardControl,
	} {
		candidate, candidateErr := class.alpn()
		if candidateErr != nil || candidate == control {
			t.Fatalf("class=%d ALPN=%q err=%v", class, candidate, candidateErr)
		}
	}
}
