package gateway

import (
	"reflect"
	"testing"

	"github.com/thesyncim/vibedb/internal/requestledger"
)

// This assertion intentionally fails at compile/review time if the current
// parent lifecycle silently grows a second route-pin/pending authority. Real
// parallel participant proposals belong to independently homed child ledgers,
// not an unreviewed relaxation of the single-wave safety invariant.
func TestDurableRequestParentLifecycleRemainsSingleWave(t *testing.T) {
	pending := reflect.TypeOf(requestledger.PendingWaveRecord{})
	for _, field := range []string{"RoutePinDigest", "ForwardingWitnessDigest", "PayloadBuildDigest"} {
		if member, ok := pending.FieldByName(field); !ok || member.Type.Kind() != reflect.Array {
			t.Fatalf("missing singular %s witness", field)
		}
	}
	head := reflect.TypeOf(requestledger.HeadRecord{})
	if member, ok := head.FieldByName("OutstandingRoutePinDigest"); !ok || member.Type.Kind() != reflect.Array {
		t.Fatal("parent head lost singular outstanding route-pin fence")
	}
}
