package splitcontroller

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/thesyncim/vibedb/distribution"
)

func TestChildPreparationCanonicalRF3Receipt(t *testing.T) {
	plan, _, target, split := testPlanWithChildLeaders(t, []distribution.EndpointID{"node-b", "node-c", "node-d"})
	for index := range target.Replicas {
		replica := &target.Replicas[index]
		replica.PeerAddress = "127.0.0.1:" + strconv.Itoa(1100+index*3)
		replica.NativeAddress = "127.0.0.1:" + strconv.Itoa(1101+index*3)
		replica.ControlAddress = "127.0.0.1:" + strconv.Itoa(1102+index*3)
	}
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
	if !reflect.DeepEqual(opened.ReplicaTarget(), target.Replicas[1]) {
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
		recovered.RequestDigest != receipt.RequestDigest ||
		preparedChildReplicaDigest(recovered.Target) != preparedChildReplicaDigest(receipt.Target) {
		t.Fatalf("receipt recovery changed canonical identity: %v", err)
	}
	for _, mutate := range []func(*ChildReplicaTarget){
		func(replica *ChildReplicaTarget) { replica.PeerAddress = "127.0.0.1:8888" },
		func(replica *ChildReplicaTarget) { replica.NativeAddress = "127.0.0.1:8888" },
		func(replica *ChildReplicaTarget) { replica.ControlAddress = "127.0.0.1:8888" },
	} {
		forged := target.Replicas[1]
		mutate(&forged)
		if _, receiptErr := NewChildPrepareReceipt(opened, forged); !errors.Is(receiptErr, ErrChildPreparation) {
			t.Fatalf("receipt accepted substituted transport: %v", receiptErr)
		}
		if targetMatchesPreparedReplica(target, forged) {
			t.Fatal("remote target accepted substituted transport")
		}
	}
	for _, invalid := range []string{"", "127.0.0.1:0", "127.0.0.1:65536", "127.0.0.1:no-port", "127.0.0.1", "bad host:1111", target.Replicas[1].NativeAddress} {
		candidate := cloneChildTarget(target)
		candidate.Replicas[1].PeerAddress = invalid
		if _, prepareErr := NewChildPreparation(plan.OperationID(), allocation, descriptor, "docs", candidate, 1); !errors.Is(prepareErr, ErrChildPreparation) {
			t.Fatalf("invalid transport address %q accepted: %v", invalid, prepareErr)
		}
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
