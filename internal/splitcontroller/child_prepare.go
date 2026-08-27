package splitcontroller

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"slices"

	"github.com/thesyncim/vibedb/autosplit"
	"github.com/thesyncim/vibedb/distribution"
	"github.com/thesyncim/vibedb/gateway"
	vibejson "github.com/thesyncim/vibejson"
)

const MaxChildPreparationBytes = MaxPlanIntentBytes

var (
	ErrChildPreparation       = errors.New("splitcontroller: invalid child preparation")
	childPreparationDomain    = []byte("vibedb/splitcontroller/child-preparation\x00")
	childPrepareReceiptDomain = []byte("vibedb/splitcontroller/child-prepare-receipt\x00")
)

// ChildPreparation is the bounded allocator-issued authority presented before
// PlanIntent exists. It freezes a complete RF3 child and selects exactly one
// destination replica. The shard may prepare only that replica and must return
// its byte-identical identity in a durable receipt.
type ChildPreparation struct {
	operation        OperationID
	allocationDigest [sha256.Size]byte
	descriptor       autosplit.SplitChild
	collection       string
	target           ChildTarget
	replica          uint8
}

type childPreparationWire struct {
	Operation        [32]byte             `json:"operation"`
	AllocationDigest [32]byte             `json:"allocation_digest"`
	Descriptor       autosplit.SplitChild `json:"descriptor"`
	Collection       string               `json:"collection"`
	Target           persistedChildTarget `json:"target"`
	Replica          uint8                `json:"replica"`
}

type childPrepareReceiptWire struct {
	Operation        [32]byte              `json:"operation"`
	AllocationDigest [32]byte              `json:"allocation_digest"`
	Child            uint8                 `json:"child"`
	Replica          uint8                 `json:"replica"`
	Target           persistedChildReplica `json:"target"`
	RequestDigest    [32]byte              `json:"request_digest"`
	ReceiptDigest    [32]byte              `json:"receipt_digest"`
}

type ChildPrepareReceipt struct {
	Operation        OperationID
	AllocationDigest [sha256.Size]byte
	Child            uint8
	Replica          uint8
	Target           ChildReplicaTarget
	RequestDigest    [sha256.Size]byte
	ReceiptDigest    [sha256.Size]byte
}

func NewChildPreparation(
	operation OperationID,
	allocationDigest [sha256.Size]byte,
	descriptor autosplit.SplitChild,
	collection string,
	target ChildTarget,
	replica uint8,
) (ChildPreparation, error) {
	preparation := ChildPreparation{
		operation: operation, allocationDigest: allocationDigest, descriptor: descriptor,
		collection: collection, target: cloneChildTarget(target), replica: replica,
	}
	if !validChildPreparation(preparation) {
		return ChildPreparation{}, ErrChildPreparation
	}
	return preparation, nil
}

func (preparation ChildPreparation) OperationID() OperationID { return preparation.operation }
func (preparation ChildPreparation) AllocationDigest() [sha256.Size]byte {
	return preparation.allocationDigest
}
func (preparation ChildPreparation) Child() uint8        { return preparation.target.Child }
func (preparation ChildPreparation) ReplicaIndex() uint8 { return preparation.replica }
func (preparation ChildPreparation) ReplicaTarget() ChildReplicaTarget {
	return cloneChildTarget(preparation.target).Replicas[preparation.replica]
}
func (preparation ChildPreparation) Target() ChildTarget {
	return cloneChildTarget(preparation.target)
}
func (preparation ChildPreparation) Descriptor() autosplit.SplitChild {
	result := preparation.descriptor
	result.Leaders = append([]distribution.EndpointID(nil), result.Leaders...)
	return result
}
func (preparation ChildPreparation) Collection() string { return preparation.collection }

func AppendChildPreparation(dst []byte, preparation ChildPreparation) ([]byte, error) {
	if !validChildPreparation(preparation) {
		return dst, ErrChildPreparation
	}
	wire := childPreparationWire{
		Operation: [32]byte(preparation.operation), AllocationDigest: preparation.allocationDigest,
		Descriptor: autosplit.SplitChild{
			Range: preparation.descriptor.Range, Shard: preparation.descriptor.Shard,
			AllocationGeneration: preparation.descriptor.AllocationGeneration,
			OwnershipEpoch:       preparation.descriptor.OwnershipEpoch,
			Leaders: append(
				[]distribution.EndpointID(nil), preparation.descriptor.Leaders...,
			),
		},
		Collection: preparation.collection, Replica: preparation.replica,
		Target: persistedChildTarget{
			Child: preparation.target.Child, Endpoint: preparation.target.Endpoint,
			Replicas:               persistChildReplicas(preparation.target.Replicas),
			ReplicaSetVersion:      preparation.target.ReplicaSetVersion,
			RelationManifestDigest: preparation.target.RelationManifestDigest,
			TopologyRecoveryEpoch:  preparation.target.TopologyRecoveryEpoch,
			Authority:              preparation.target.Authority,
			LocalIndexes:           cloneSplitLocalIndexes(preparation.target.LocalIndexes),
		},
	}
	raw, err := vibejson.Marshal(&wire)
	if err != nil {
		return dst, errors.Join(ErrChildPreparation, err)
	}
	raw, err = vibejson.AppendCanonicalize(nil, raw)
	if err != nil || len(raw) == 0 || len(raw) > MaxChildPreparationBytes {
		return dst, errors.Join(ErrChildPreparation, err)
	}
	return append(dst, raw...), nil
}

func OpenChildPreparation(raw []byte) (ChildPreparation, error) {
	if len(raw) == 0 || len(raw) > MaxChildPreparationBytes {
		return ChildPreparation{}, ErrChildPreparation
	}
	var wire childPreparationWire
	if err := vibejson.Unmarshal(raw, &wire); err != nil {
		return ChildPreparation{}, errors.Join(ErrChildPreparation, err)
	}
	canonical, err := vibejson.Marshal(&wire)
	if err != nil {
		return ChildPreparation{}, errors.Join(ErrChildPreparation, err)
	}
	canonical, err = vibejson.AppendCanonicalize(nil, canonical)
	if err != nil || !bytes.Equal(raw, canonical) {
		return ChildPreparation{}, errors.Join(ErrChildPreparation, err)
	}
	replicas := openPersistedChildReplicas(wire.Target.Replicas)
	target := ChildTarget{
		Child: wire.Target.Child, Endpoint: wire.Target.Endpoint, Replicas: replicas,
		LocalIndexes:           cloneSplitLocalIndexes(wire.Target.LocalIndexes),
		ReplicaSetVersion:      wire.Target.ReplicaSetVersion,
		RelationManifestDigest: wire.Target.RelationManifestDigest,
		TopologyRecoveryEpoch:  wire.Target.TopologyRecoveryEpoch, Authority: wire.Target.Authority,
	}
	if len(replicas) != 0 {
		target.WAL, target.SQL = replicas[0].WAL, replicas[0].SQL.Clone()
	}
	return NewChildPreparation(
		OperationID(wire.Operation), wire.AllocationDigest,
		autosplit.SplitChild{
			Range: wire.Descriptor.Range, Shard: wire.Descriptor.Shard,
			AllocationGeneration: wire.Descriptor.AllocationGeneration,
			OwnershipEpoch:       wire.Descriptor.OwnershipEpoch,
			Leaders:              append([]distribution.EndpointID(nil), wire.Descriptor.Leaders...),
		},
		wire.Collection, target, wire.Replica,
	)
}

func ChildPreparationDigest(preparation ChildPreparation) ([sha256.Size]byte, error) {
	raw, err := AppendChildPreparation(nil, preparation)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	hash := sha256.New()
	_, _ = hash.Write(childPreparationDomain)
	_, _ = hash.Write(raw)
	var digest [sha256.Size]byte
	_ = hash.Sum(digest[:0])
	return digest, nil
}

func NewChildPrepareReceipt(
	preparation ChildPreparation, target ChildReplicaTarget,
) (ChildPrepareReceipt, error) {
	if !validChildPreparation(preparation) ||
		preparedChildReplicaDigest(target) != preparedChildReplicaDigest(preparation.ReplicaTarget()) {
		return ChildPrepareReceipt{}, ErrChildPreparation
	}
	requestDigest, err := ChildPreparationDigest(preparation)
	if err != nil {
		return ChildPrepareReceipt{}, err
	}
	receipt := ChildPrepareReceipt{
		Operation: preparation.operation, AllocationDigest: preparation.allocationDigest,
		Child: preparation.target.Child, Replica: preparation.replica,
		Target:        cloneChildTarget(ChildTarget{Replicas: []ChildReplicaTarget{target}}).Replicas[0],
		RequestDigest: requestDigest,
	}
	receipt.ReceiptDigest, err = childPrepareReceiptDigest(receipt)
	return receipt, err
}

func AppendChildPrepareReceipt(dst []byte, receipt ChildPrepareReceipt) ([]byte, error) {
	digest, err := childPrepareReceiptDigest(receipt)
	if err != nil || digest != receipt.ReceiptDigest {
		return dst, errors.Join(ErrChildPreparation, err)
	}
	wire, err := childPrepareReceiptWireFor(receipt, true)
	if err != nil {
		return dst, err
	}
	raw, err := vibejson.Marshal(&wire)
	if err != nil {
		return dst, errors.Join(ErrChildPreparation, err)
	}
	raw, err = vibejson.AppendCanonicalize(nil, raw)
	if err != nil || len(raw) == 0 || len(raw) > MaxChildPreparationBytes {
		return dst, errors.Join(ErrChildPreparation, err)
	}
	return append(dst, raw...), nil
}

func OpenChildPrepareReceipt(raw []byte) (ChildPrepareReceipt, error) {
	if len(raw) == 0 || len(raw) > MaxChildPreparationBytes {
		return ChildPrepareReceipt{}, ErrChildPreparation
	}
	var wire childPrepareReceiptWire
	if err := vibejson.Unmarshal(raw, &wire); err != nil {
		return ChildPrepareReceipt{}, errors.Join(ErrChildPreparation, err)
	}
	canonical, err := vibejson.Marshal(&wire)
	if err != nil {
		return ChildPrepareReceipt{}, errors.Join(ErrChildPreparation, err)
	}
	canonical, err = vibejson.AppendCanonicalize(nil, canonical)
	if err != nil || !bytes.Equal(raw, canonical) {
		return ChildPrepareReceipt{}, errors.Join(ErrChildPreparation, err)
	}
	receipt := ChildPrepareReceipt{
		Operation: OperationID(wire.Operation), AllocationDigest: wire.AllocationDigest,
		Child: wire.Child, Replica: wire.Replica,
		Target:        openPersistedChildReplicas([]persistedChildReplica{wire.Target})[0],
		RequestDigest: wire.RequestDigest, ReceiptDigest: wire.ReceiptDigest,
	}
	digest, digestErr := childPrepareReceiptDigest(receipt)
	if digestErr != nil || digest != receipt.ReceiptDigest {
		return ChildPrepareReceipt{}, errors.Join(ErrChildPreparation, digestErr)
	}
	return receipt, nil
}

func childPrepareReceiptDigest(receipt ChildPrepareReceipt) ([sha256.Size]byte, error) {
	wire, err := childPrepareReceiptWireFor(receipt, false)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	raw, err := vibejson.Marshal(&wire)
	if err != nil {
		return [sha256.Size]byte{}, errors.Join(ErrChildPreparation, err)
	}
	raw, err = vibejson.AppendCanonicalize(nil, raw)
	if err != nil {
		return [sha256.Size]byte{}, errors.Join(ErrChildPreparation, err)
	}
	hash := sha256.New()
	_, _ = hash.Write(childPrepareReceiptDomain)
	_, _ = hash.Write(raw)
	var digest [sha256.Size]byte
	_ = hash.Sum(digest[:0])
	return digest, nil
}

func childPrepareReceiptWireFor(
	receipt ChildPrepareReceipt, includeDigest bool,
) (childPrepareReceiptWire, error) {
	if receipt.Operation == (OperationID{}) || receipt.AllocationDigest == ([32]byte{}) ||
		receipt.RequestDigest == ([32]byte{}) || receipt.Child >= autosplit.MaxSplitChildren ||
		receipt.Replica >= gateway.ServingReplicaCount ||
		preparedChildReplicaDigest(receipt.Target) == ([32]byte{}) {
		return childPrepareReceiptWire{}, ErrChildPreparation
	}
	wire := childPrepareReceiptWire{
		Operation: [32]byte(receipt.Operation), AllocationDigest: receipt.AllocationDigest,
		Child: receipt.Child, Replica: receipt.Replica,
		Target:        persistChildReplicas([]ChildReplicaTarget{receipt.Target})[0],
		RequestDigest: receipt.RequestDigest,
	}
	if includeDigest {
		if receipt.ReceiptDigest == ([32]byte{}) {
			return childPrepareReceiptWire{}, ErrChildPreparation
		}
		wire.ReceiptDigest = receipt.ReceiptDigest
	}
	return wire, nil
}

func validChildPreparation(preparation ChildPreparation) bool {
	if preparation.operation == (OperationID{}) || preparation.allocationDigest == ([32]byte{}) ||
		preparation.collection == "" || preparation.target.Child >= autosplit.MaxSplitChildren ||
		preparation.descriptor.Retained || preparation.replica >= uint8(len(preparation.target.Replicas)) ||
		len(preparation.target.Replicas) != gateway.ServingReplicaCount ||
		len(preparation.descriptor.Leaders) != len(preparation.target.Replicas) ||
		string(preparation.descriptor.Shard) != preparation.target.SQL.Binding.Shard ||
		uint64(preparation.descriptor.AllocationGeneration) != preparation.target.SQL.Binding.AllocationGeneration ||
		!validPreparedChildReplicas(autosplit.SplitChildIdentity{
			Range: preparation.descriptor.Range, Shard: preparation.descriptor.Shard,
			AllocationGeneration: preparation.descriptor.AllocationGeneration,
			OwnershipEpoch:       preparation.descriptor.OwnershipEpoch,
		}, preparation.collection, preparation.target) {
		return false
	}
	for index, replica := range preparation.target.Replicas {
		if replica.Member == 0 || replica.Node == ([16]byte{}) || replica.StoreID == ([16]byte{}) ||
			replica.NodeIncarnation == 0 || replica.Endpoint == "" || replica.NativeEndpoint == "" ||
			replica.ControlEndpoint == "" || !validChildReplicaAddresses(replica) ||
			replica.NativeEndpoint != preparation.descriptor.Leaders[index] {
			return false
		}
		for prior := 0; prior < index; prior++ {
			other := preparation.target.Replicas[prior]
			if other.Member == replica.Member || other.Node == replica.Node || other.StoreID == replica.StoreID ||
				other.Endpoint == replica.Endpoint || other.NativeEndpoint == replica.NativeEndpoint ||
				other.ControlEndpoint == replica.ControlEndpoint || other.SnapshotAddress == replica.SnapshotAddress {
				return false
			}
		}
	}
	return preparation.target.Endpoint == preparation.target.Replicas[0].NativeEndpoint &&
		slices.Equal(preparation.descriptor.Leaders, func() []distribution.EndpointID {
			leaders := make([]distribution.EndpointID, len(preparation.target.Replicas))
			for index := range leaders {
				leaders[index] = preparation.target.Replicas[index].NativeEndpoint
			}
			return leaders
		}())
}
