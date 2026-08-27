package splitcontroller

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func TestChildPreparationCanonicalRF3Receipt(t *testing.T) {
	plan, _, target, split := testPlanWithChildLeaders(t, []distribution.EndpointID{"node-b", "node-c", "node-d"})
	descriptor, ok := split.Child(int(target.Child))
	if !ok {
		t.Fatal("missing child")
	}
	allocation := sha256.Sum256([]byte("allocator-issued-rf3-child"))
	preparation, err := NewChildPreparation(
		plan.OperationID(), allocation, descriptor, "docs", target, 1,
	)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := AppendChildPreparation(nil, preparation)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := OpenChildPreparation(raw)
	if err != nil {
		t.Fatal(err)
	}
	again, err := AppendChildPreparation(nil, opened)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatalf("canonical replay changed: equal=%v err=%v", bytes.Equal(raw, again), err)
	}
	if opened.ReplicaTarget().Node != target.Replicas[1].Node ||
		opened.ReplicaTarget().SQLPath != target.Replicas[1].SQLPath {
		t.Fatal("opened preparation changed selected replica")
	}

	receipt, err := NewChildPrepareReceipt(opened, target.Replicas[1])
	if err != nil {
		t.Fatal(err)
	}
	receiptRaw, err := AppendChildPrepareReceipt(nil, receipt)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := OpenChildPrepareReceipt(receiptRaw)
	if err != nil || recovered.ReceiptDigest != receipt.ReceiptDigest ||
		recovered.RequestDigest != receipt.RequestDigest {
		t.Fatalf("receipt recovery = %+v, %v", recovered, err)
	}

	for name, mutate := range map[string]func([]byte){
		"trailing": func(candidate []byte) { candidate[len(candidate)-1] = ' ' },
		"forgery":  func(candidate []byte) { candidate[len(candidate)/2] ^= 1 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := append([]byte(nil), receiptRaw...)
			mutate(candidate)
			if _, openErr := OpenChildPrepareReceipt(candidate); !errors.Is(openErr, ErrChildPreparation) {
				t.Fatalf("open err=%v", openErr)
			}
		})
	}
}

func TestChildPreparationRejectsReplicaSubstitution(t *testing.T) {
	plan, _, target, split := testPlanWithChildLeaders(t, []distribution.EndpointID{"node-b", "node-c", "node-d"})
	descriptor, _ := split.Child(int(target.Child))
	preparation, err := NewChildPreparation(
		plan.OperationID(), sha256.Sum256([]byte("allocation")), descriptor, "docs", target, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	forged := target.Replicas[0]
	forged.StoreID[0]++
	if _, err = NewChildPrepareReceipt(preparation, forged); !errors.Is(err, ErrChildPreparation) {
		t.Fatalf("substitution err=%v", err)
	}
}
