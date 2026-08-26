package replicatedstate

import (
	"crypto/sha256"
	"testing"

	"github.com/thesyncim/vibedb/internal/requestledger"
)

func TestRequestLedgerRangeHalfOpenAuthority(t *testing.T) {
	rangeAuthority := RequestLedgerRange{
		Start:    requestledger.LedgerHome{0x40},
		End:      requestledger.LedgerHome{0x80},
		Identity: requestledger.Digest{1},
	}
	for _, tc := range []struct {
		name string
		home requestledger.LedgerHome
		want bool
	}{
		{"below", requestledger.LedgerHome{0x3f, 0xff}, false},
		{"start", requestledger.LedgerHome{0x40}, true},
		{"inside", requestledger.LedgerHome{0x7f, 0xff}, true},
		{"end", requestledger.LedgerHome{0x80}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := rangeAuthority.contains(tc.home); got != tc.want {
				t.Fatalf("contains(%x) = %v, want %v", tc.home, got, tc.want)
			}
		})
	}
	full := RequestLedgerRange{Identity: requestledger.Digest{2}}
	if !full.valid() || !full.contains(requestledger.LedgerHome{}) ||
		!full.contains(requestledger.LedgerHome{0xff, 0xff, 0xff}) {
		t.Fatal("zero start/end did not represent the complete digest space")
	}
}

func TestRequestLedgerRangeAndSemanticsBindApplyContract(t *testing.T) {
	manifest := sha256.Sum256([]byte("ledger relation manifest"))
	relations := []relationCollection{{id: 1}}
	base := RequestLedgerRange{Start: requestledger.LedgerHome{0x20},
		End: requestledger.LedgerHome{0x40}, Identity: requestledger.Digest{1}}
	digest, err := bundleApplyContractDigest(manifest, relations, 8, 4, 1<<20, 4096, base)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*RequestLedgerRange){
		func(value *RequestLedgerRange) { value.Start[1]++ },
		func(value *RequestLedgerRange) { value.End[1]++ },
		func(value *RequestLedgerRange) { value.Identity[1]++ },
	} {
		candidate := base
		mutate(&candidate)
		other, otherErr := bundleApplyContractDigest(
			manifest, relations, 8, 4, 1<<20, 4096, candidate,
		)
		if otherErr != nil {
			t.Fatal(otherErr)
		}
		if other == digest {
			t.Fatal("ledger authority mutation did not change apply contract")
		}
	}
	disabled, err := bundleApplyContractDigest(
		manifest, relations, 8, 4, 0, 0, RequestLedgerRange{},
	)
	if err != nil || disabled == digest {
		t.Fatalf("disabled contract = %x, %v", disabled, err)
	}
}

func TestRequestLedgerRangeValidationFailsClosed(t *testing.T) {
	base := Options{
		MaxSessions: 1, RetryWindow: 1,
		RequestLedgerCapacityBytes:       4096,
		RequestLedgerCleanupReserveBytes: 512,
		RequestLedgerRange:               RequestLedgerRange{Identity: requestledger.Digest{1}},
	}
	// Txn limits are checked before the ledger fields. Give this narrow test a
	// minimally valid cross-collection budget.
	base.TxnLimits.MaxCollections = 2
	base.TxnLimits.MaxDocuments = 4
	base.TxnLimits.MaxBytes = 1
	if err := base.validate(); err != nil {
		t.Fatalf("valid ledger range: %v", err)
	}
	for _, mutate := range []func(*Options){
		func(options *Options) { options.RequestLedgerRange.Identity = requestledger.Digest{} },
		func(options *Options) {
			options.RequestLedgerRange.Start = requestledger.LedgerHome{0x80}
			options.RequestLedgerRange.End = requestledger.LedgerHome{0x40}
		},
		func(options *Options) { options.RequestLedgerCleanupReserveBytes = 4096 },
		func(options *Options) { options.RequestLedgerCapacityBytes = 0 },
	} {
		candidate := base
		mutate(&candidate)
		if err := candidate.validate(); err == nil {
			t.Fatalf("accepted invalid ledger range/options: %+v", candidate)
		}
	}
}
