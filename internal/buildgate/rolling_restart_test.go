package buildgate

import (
	"errors"
	"testing"
)

// TestUnreleasedRollingRestartBoundary qualifies the only restart contract
// offered before VibeDB has a released format: every process and every durable
// image must name the exact current grammar. There is deliberately no numeric
// version ordering, downgrade path, or best-effort mixed-build admission.
func TestUnreleasedRollingRestartBoundary(t *testing.T) {
	current := CurrentProfile()

	t.Run("same build peer and disk", func(t *testing.T) {
		if agreed, err := CheckCompatibility(current, current); err != nil ||
			!agreed.Has(CapabilityRaftTransport) ||
			!agreed.Has(CapabilityGatewayShardTransport) {
			t.Fatalf("same-build peer admission = %#v, %v", agreed, err)
		}

		gate, err := NewCurrentDiskGate(current)
		if err != nil {
			t.Fatal(err)
		}
		target := &recordingDiskTarget{identity: CurrentDiskIdentity()}
		if err = AdoptDisk(gate, target); err != nil {
			t.Fatalf("same-build disk adoption: %v", err)
		}
		if target.inspected != 1 || target.mutated != 1 ||
			!target.permit.allows(target.identity) {
			t.Fatalf("same-build inspect/mutate/permit = %d/%d/%v",
				target.inspected, target.mutated, target.permit.allows(target.identity))
		}
	})

	t.Run("different wire grammar", func(t *testing.T) {
		other := current
		other.WireGrammar[0] ^= 0x80
		if _, err := CheckCompatibility(current, other); !errors.Is(err, ErrWireGrammar) {
			t.Fatalf("mixed wire grammar = %v, want ErrWireGrammar", err)
		}
	})

	t.Run("different disk grammar before mutation", func(t *testing.T) {
		other := current
		other.DiskGrammar[0] ^= 0x80
		if _, err := CheckCompatibility(current, other); !errors.Is(err, ErrDiskGrammar) {
			t.Fatalf("mixed disk grammar peer = %v, want ErrDiskGrammar", err)
		}

		gate, err := NewCurrentDiskGate(current)
		if err != nil {
			t.Fatal(err)
		}
		target := &recordingDiskTarget{identity: DiskIdentity{
			Grammar: other.DiskGrammar, Required: other.Required,
		}}
		if err = AdoptDisk(gate, target); !errors.Is(err, ErrDiskGrammar) {
			t.Fatalf("mixed disk grammar adoption = %v, want ErrDiskGrammar", err)
		}
		if target.inspected != 1 || target.mutated != 0 {
			t.Fatalf("mixed disk inspect/mutate = %d/%d", target.inspected, target.mutated)
		}
	})

	t.Run("unavailable required capability", func(t *testing.T) {
		other := current
		var ok bool
		other.Provided, ok = other.Provided.With(255)
		if !ok {
			t.Fatal("capability 255 is outside the fixed bitmap")
		}
		other.Required, ok = other.Required.With(255)
		if !ok {
			t.Fatal("capability 255 is outside the fixed bitmap")
		}
		if _, err := CheckCompatibility(current, other); !errors.Is(err, ErrRequiredCapabilities) {
			t.Fatalf("mixed required capability = %v, want ErrRequiredCapabilities", err)
		}
	})
}
