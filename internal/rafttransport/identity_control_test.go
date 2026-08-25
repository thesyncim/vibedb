package rafttransport

import "testing"

func TestShardControlTrafficClassHasIsolatedALPN(t *testing.T) {
	control, err := TrafficShardControl.alpn()
	if err != nil || control == "" {
		t.Fatalf("control ALPN=%q err=%v", control, err)
	}
	for _, class := range []TrafficClass{
		TrafficOrdinary, TrafficSnapshot, TrafficShardNative,
		TrafficGatewayClient, TrafficShardSQL,
	} {
		candidate, candidateErr := class.alpn()
		if candidateErr != nil || candidate == control {
			t.Fatalf("class=%d ALPN=%q err=%v", class, candidate, candidateErr)
		}
	}
}
