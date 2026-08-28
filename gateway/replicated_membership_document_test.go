package gateway

import (
	"bytes"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/internal/orderedkey"
	"github.com/thesyncim/vibedb/internal/raftmember"
)

func TestMembershipLifecycleKeyConstructionIsBoundedAndStable(t *testing.T) {
	authority, _, _ := newCatalogAuthorityFixture(t)
	group := authority.route.Group
	var record replicatedMembershipRecordKey
	var page replicatedMembershipPageKey
	allocations := testing.AllocsPerRun(1000, func() {
		record, page = replicatedMembershipGrantKeys(group)
	})
	if allocations != 0 || len(record) != 77 || len(page) != 15 || page.bucket() != 37 {
		t.Fatalf("allocations=%v record=%d page=%d bucket=%d", allocations, len(record), len(page), page.bucket())
	}
	// This digest is the pre-existing membership-grant hash of the fixed test
	// group; changing the SQL key shape must not alter collision placement.
	want := []byte("member/03/a5550a6eae8c458a87610eb10eccf4c59c07a6f46c9b4e73116061a612fe04c7")
	if !bytes.Equal(record[:], fixedControlPlaneKey(want)) ||
		!bytes.Equal(page[:], fixedControlPlaneKey([]byte("member/04/25"))) {
		t.Fatalf("changed group identity or bucket: record=%x page=%x", record, page)
	}
	receipt, receiptPage := replicatedReplicaReplacementReceiptKeys(group)
	if receipt == record || receiptPage == page || receiptPage.bucket() != page.bucket() {
		t.Fatal("grant and receipt namespaces collide or changed bucket placement")
	}
}

func TestMembershipLifecycleRowsUseExactSQLPrimaryKey(t *testing.T) {
	authority, _, _ := newCatalogAuthorityFixture(t)
	grant := testReplicatedMembershipGrant(authority.route.Group)
	grantKey, grantPageKey := replicatedMembershipGrantKeys(grant.Group)
	receiptKey, receiptPageKey := replicatedReplicaReplacementReceiptKeys(grant.Group)
	grantRaw, err := appendReplicatedMembershipGrant(nil, grant)
	if err != nil {
		t.Fatal(err)
	}
	grantPage, err := appendReplicatedMembershipGrantPage(nil, grantPageKey.bucket(), []raftmember.GroupKey{grant.Group})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := appendReplicaReplacementReceipt(nil, grant, []byte("old-head"), []byte("new-head"), 2)
	if err != nil {
		t.Fatal(err)
	}
	receiptPage, err := appendReplicaReplacementReceiptPage(nil, receiptPageKey.bucket(), []raftmember.GroupKey{grant.Group})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		name     string
		key, raw []byte
		open     func([]byte) error
	}{
		{"grant", grantKey[:], grantRaw, func(raw []byte) error { _, err := openReplicatedMembershipGrant(raw); return err }},
		{"grant-page", grantPageKey[:], grantPage, func(raw []byte) error {
			_, err := openReplicatedMembershipGrantPage(grantPageKey.bucket(), raw)
			return err
		}},
		{"receipt", receiptKey[:], receipt, func(raw []byte) error { _, err := openReplicaReplacementReceipt(raw); return err }},
		{"receipt-page", receiptPageKey[:], receiptPage, func(raw []byte) error {
			_, err := openReplicaReplacementReceiptPage(receiptPageKey.bucket(), raw)
			return err
		}},
	} {
		t.Run(row.name, func(t *testing.T) {
			// This is the same key decoder used by replicated SQL point-ownership
			// validation, including missing-row reads before initial insertion.
			component, decoded, next, err := orderedkey.DecodeComponent(nil, row.key, 0)
			if err != nil || next != len(row.key) || component.Descending || component.Kind != orderedkey.KindString {
				t.Fatalf("not a SQL string primary key: key=%x next=%d err=%v", row.key, next, err)
			}
			id := decoded[component.PayloadStart:component.PayloadEnd]
			payload, err := openTypedControlPlaneDocument(row.raw, id, maxReplicatedCatalogBytes)
			if err != nil {
				t.Fatalf("row /id does not match its SQL key: %v", err)
			}
			if !bytes.Equal(fixedControlPlaneKey(id), row.key) {
				t.Fatal("noncanonical SQL primary key")
			}
			if err := row.open(row.raw); err != nil {
				t.Fatalf("canonical row did not reopen: %v", err)
			}
			if err := row.open(payload); !errors.Is(err, ErrReplicatedCatalog) {
				t.Fatalf("untyped payload accepted: %v", err)
			}
			for _, index := range []int{len("member/0"), len(id) - 1} {
				alteredID := bytes.Clone(id)
				alteredID[index] = '0'
				if alteredID[index] == id[index] {
					alteredID[index] = '1'
				}
				altered, err := appendControlPlaneDocument(nil, alteredID, payload, maxReplicatedCatalogBytes)
				if err != nil {
					t.Fatal(err)
				}
				if err := row.open(altered); !errors.Is(err, ErrReplicatedCatalog) {
					t.Fatalf("substituted row kind/group/bucket accepted: %v", err)
				}
			}
		})
	}
}
