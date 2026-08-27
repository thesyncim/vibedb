package serviceauthz

import (
	"errors"
	"strings"
	"testing"
)

func TestLoadVibeJSONBoundsAndCanonicalPolicy(t *testing.T) {
	node := strings.Repeat("01", 16)
	policy, err := Load([]byte(`{"generation":5,"principals":[{"node":"` + node +
		`","capabilities":["data_read","schema","delegate","membership","topology","transaction_recovery","request_ledger","execution_pin"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if policy.Generation() != 5 || len(policy.Nodes()) != 1 ||
		policy.Check(policy.Nodes()[0], CapabilityDataRead|CapabilitySchema|
			CapabilityDelegate|CapabilityMembership|CapabilityTopology|
			CapabilityTransactionRecovery|CapabilityRequestLedger|
			CapabilityExecutionPin) != DecisionAllow {
		t.Fatalf("loaded policy mismatch")
	}
	backupPolicy, err := Load([]byte(`{"generation":6,"principals":[{"node":"` + node +
		`","capabilities":["backup"]}]}`))
	if err != nil || backupPolicy.Check(backupPolicy.Nodes()[0], CapabilityBackup) != DecisionAllow {
		t.Fatalf("backup capability policy=%v err=%v", backupPolicy, err)
	}
	restorePolicy, err := Load([]byte(`{"generation":7,"principals":[{"node":"` + node +
		`","capabilities":["restore_activate"]}]}`))
	if err != nil || restorePolicy.Check(
		restorePolicy.Nodes()[0], CapabilityRestoreActivate,
	) != DecisionAllow {
		t.Fatalf("restore activation capability policy=%v err=%v", restorePolicy, err)
	}
	for _, raw := range [][]byte{
		nil,
		[]byte(`{"generation":0,"principals":[]}`),
		[]byte(`{"generation":1,"principals":[{"node":"` + node + `","capabilities":["data_read","data_read"]}]}`),
		[]byte(`{"generation":1,"principals":[{"node":"` + node + `","capabilities":["unknown"]}]}`),
		[]byte(`{"generation":1,"generation":2,"principals":[{"node":"` + node + `","capabilities":["data_read"]}]}`),
		[]byte(`{"principals":[{"node":"` + node + `","capabilities":["data_read"]}],"generation":1}`),
		[]byte(`{"g\u0065neration":1,"principals":[{"node":"` + node + `","capabilities":["data_read"]}]}`),
		[]byte(`{"generation":1,"principals":[{"node":"` + node + `","node":"` + node + `","capabilities":["data_read"]}]}`),
		[]byte(`{"generation":1,"principals":[{"node":"` + node + `","capabilities":["delegate","data_read"]}]}`),
		[]byte(`{"generation":1,"principals":[{"node":"` + node + `","capabilities":["transaction_recovery","topology"]}]}`),
		[]byte(`{"generation":1,"principals":[{"node":"` + node + `","capabilities":["request_ledger","topology"]}]}`),
		[]byte(`{"generation":1,"principals":[{"node":"` + node + `","capabilities":["data_read"],"unknown":1}]}`),
	} {
		if _, err := Load(raw); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("Load(%q) err=%v", raw, err)
		}
	}
}
