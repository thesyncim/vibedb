package driver

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/thesyncim/vibedb/internal/requestledger"
	"github.com/thesyncim/vibejson"
)

func testReplicatedRequestLedgerOptions() ReplicatedApplyOptions {
	options := testReplicatedApplyOptions()
	options.RequestLedgerCapacityBytes = 64 << 20
	options.RequestLedgerCleanupReserveBytes = 8 << 20
	options.RequestLedgerRangeStart[0] = 0x20
	options.RequestLedgerRangeEnd[0] = 0x90
	options.RequestLedgerRangeIdentity[0] = 0x5a
	return options
}

func TestReplicatedApplyRequestLedgerCatalogRoundTripAndMachinePlumbing(t *testing.T) {
	path, database, base := bindReplicatedApplyTestRoot(t, "request-ledger-catalog")
	options := testReplicatedRequestLedgerOptions()
	bootstrap := testReplicatedApplyBootstrap()
	claim, identity, err := database.OpenReplicatedApply(base, bootstrap, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := claim.InstallSnapshot(bootstrap); err != nil {
		t.Fatal(err)
	}
	usage, err := claim.machine.RequestLedgerUsage()
	if err != nil || !usage.Enabled || usage.CapacityBytes != options.RequestLedgerCapacityBytes ||
		usage.CleanupReserveBytes != options.RequestLedgerCleanupReserveBytes ||
		usage.Range.Start != requestledger.LedgerHome(options.RequestLedgerRangeStart) ||
		usage.Range.End != requestledger.LedgerHome(options.RequestLedgerRangeEnd) ||
		usage.Range.Identity != requestledger.Digest(options.RequestLedgerRangeIdentity) {
		t.Fatalf("request-ledger machine options = %+v, %v", usage, err)
	}
	if identity.RequestLedgerCapacityBytes != options.RequestLedgerCapacityBytes ||
		identity.RequestLedgerCleanupReserveBytes != options.RequestLedgerCleanupReserveBytes ||
		identity.RequestLedgerRangeStart != options.RequestLedgerRangeStart ||
		identity.RequestLedgerRangeEnd != options.RequestLedgerRangeEnd ||
		identity.RequestLedgerRangeIdentity != options.RequestLedgerRangeIdentity {
		t.Fatalf("request-ledger retained identity = %+v", identity)
	}
	encoded, err := identity.MarshalJSON()
	if err != nil || vibejson.Validate(encoded) != nil {
		t.Fatalf("request-ledger retained grammar = %s, %v", encoded, err)
	}
	wantFields := []byte(`"request_ledger_capacity_bytes":67108864,"request_ledger_cleanup_reserve_bytes":8388608,"request_ledger_range_start":"2000000000000000000000000000000000000000000000000000000000000000","request_ledger_range_end":"9000000000000000000000000000000000000000000000000000000000000000","request_ledger_range_identity":"5a00000000000000000000000000000000000000000000000000000000000000"`)
	if !bytes.Contains(encoded, wantFields) {
		t.Fatalf("request-ledger fields are not canonical and adjacent: %s", encoded)
	}
	var decoded ReplicatedApplyIdentity
	if err := decoded.UnmarshalJSON(encoded); err != nil || decoded != identity {
		t.Fatalf("request-ledger identity round trip = %+v, %v", decoded, err)
	}
	reencoded, err := decoded.MarshalJSON()
	if err != nil || !bytes.Equal(reencoded, encoded) {
		t.Fatalf("request-ledger canonical re-encode = %s, %v; want %s", reencoded, err, encoded)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenReplicatedShardStoreWithApply(path, base, identity)
	if err != nil {
		t.Fatal(err)
	}
	reopenedClaim, reopenedIdentity, err := reopened.OpenReplicatedApply(base, bootstrap, options)
	if err != nil || reopenedIdentity != identity {
		t.Fatalf("request-ledger catalog reopen = %+v, %v", reopenedIdentity, err)
	}
	if err := reopenedClaim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reopened.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicatedApplyRequestLedgerBoundsAndDefaultDisabled(t *testing.T) {
	disabled := testReplicatedApplyOptions()
	if err := validateReplicatedRequestLedgerOptions(disabled); err != nil {
		t.Fatalf("default-disabled request ledger = %v", err)
	}
	_, database, base := bindReplicatedApplyTestRoot(t, "request-ledger-disabled")
	claim, identity, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), disabled,
	)
	if err != nil {
		t.Fatal(err)
	}
	usage, err := claim.machine.RequestLedgerUsage()
	if err != nil || usage.Enabled || usage.CapacityBytes != 0 ||
		usage.CleanupReserveBytes != 0 ||
		usage.Range.Start != (requestledger.LedgerHome{}) ||
		usage.Range.End != (requestledger.LedgerHome{}) ||
		usage.Range.Identity != (requestledger.Digest{}) ||
		identity.RequestLedgerCapacityBytes != 0 ||
		identity.RequestLedgerCleanupReserveBytes != 0 ||
		identity.RequestLedgerRangeStart != ([32]byte{}) ||
		identity.RequestLedgerRangeEnd != ([32]byte{}) ||
		identity.RequestLedgerRangeIdentity != ([32]byte{}) {
		t.Fatalf("default request ledger is not disabled: usage=%+v identity=%+v err=%v", usage, identity, err)
	}
	if err := claim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	enabled := testReplicatedRequestLedgerOptions()
	unbounded := enabled
	unbounded.RequestLedgerRangeEnd = [32]byte{}
	if err := validateReplicatedRequestLedgerOptions(unbounded); err != nil {
		t.Fatalf("unbounded request-ledger range = %v", err)
	}
	maximum := enabled
	maximum.RequestLedgerCapacityBytes = math.MaxInt64
	if err := validateReplicatedRequestLedgerOptions(maximum); err != nil {
		t.Fatalf("maximum request-ledger capacity = %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*ReplicatedApplyOptions)
	}{
		{name: "capacity_without_profile", mutate: func(value *ReplicatedApplyOptions) {
			*value = disabled
			value.RequestLedgerCapacityBytes = 1
		}},
		{name: "cleanup_zero", mutate: func(value *ReplicatedApplyOptions) {
			value.RequestLedgerCleanupReserveBytes = 0
		}},
		{name: "cleanup_equals_capacity", mutate: func(value *ReplicatedApplyOptions) {
			value.RequestLedgerCleanupReserveBytes = value.RequestLedgerCapacityBytes
		}},
		{name: "capacity_overflow", mutate: func(value *ReplicatedApplyOptions) {
			value.RequestLedgerCapacityBytes = uint64(math.MaxInt64) + 1
		}},
		{name: "identity_zero", mutate: func(value *ReplicatedApplyOptions) {
			value.RequestLedgerRangeIdentity = [32]byte{}
		}},
		{name: "empty_bounded_range", mutate: func(value *ReplicatedApplyOptions) {
			value.RequestLedgerRangeEnd = value.RequestLedgerRangeStart
		}},
		{name: "inverted_range", mutate: func(value *ReplicatedApplyOptions) {
			value.RequestLedgerRangeStart[0] = 0xa0
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := enabled
			test.mutate(&changed)
			if err := validateReplicatedRequestLedgerOptions(changed); !errors.Is(err, ErrReplicatedApplyMismatch) {
				t.Fatalf("request-ledger invalid profile = %v, want mismatch", err)
			}
		})
	}
}

func TestReplicatedApplyRequestLedgerGrammarRejectsNoncanonicalFields(t *testing.T) {
	_, database, base := bindReplicatedApplyTestRoot(t, "request-ledger-grammar")
	options := testReplicatedRequestLedgerOptions()
	claim, identity, err := database.OpenReplicatedApply(
		base, testReplicatedApplyBootstrap(), options,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = claim.Close()
		_ = database.Close()
	})
	encoded, err := identity.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		old  []byte
		new  []byte
	}{
		{name: "short_start", old: []byte(`"request_ledger_range_start":"2000000000000000000000000000000000000000000000000000000000000000"`), new: []byte(`"request_ledger_range_start":"20"`)},
		{name: "uppercase_identity", old: []byte(`"request_ledger_range_identity":"5a00000000000000000000000000000000000000000000000000000000000000"`), new: []byte(`"request_ledger_range_identity":"5A00000000000000000000000000000000000000000000000000000000000000"`)},
		{name: "zero_capacity", old: []byte(`"request_ledger_capacity_bytes":67108864`), new: []byte(`"request_ledger_capacity_bytes":0`)},
		{name: "cleanup_equals_capacity", old: []byte(`"request_ledger_cleanup_reserve_bytes":8388608`), new: []byte(`"request_ledger_cleanup_reserve_bytes":67108864`)},
		{name: "zero_identity", old: []byte(`"request_ledger_range_identity":"5a00000000000000000000000000000000000000000000000000000000000000"`), new: []byte(`"request_ledger_range_identity":"0000000000000000000000000000000000000000000000000000000000000000"`)},
		{name: "inverted_range", old: []byte(`"request_ledger_range_start":"2000000000000000000000000000000000000000000000000000000000000000"`), new: []byte(`"request_ledger_range_start":"a000000000000000000000000000000000000000000000000000000000000000"`)},
		{name: "missing_capacity", old: []byte(`,"request_ledger_capacity_bytes":67108864`), new: nil},
		{name: "duplicate_capacity", old: []byte(`"request_ledger_capacity_bytes":67108864`), new: []byte(`"request_ledger_capacity_bytes":67108864,"request_ledger_capacity_bytes":67108864`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := bytes.Replace(encoded, test.old, test.new, 1)
			if bytes.Equal(changed, encoded) {
				t.Fatal("grammar mutation did not match")
			}
			if err := new(ReplicatedApplyIdentity).UnmarshalJSON(changed); err == nil {
				t.Fatalf("accepted noncanonical request-ledger grammar: %s", changed)
			}
		})
	}
}
