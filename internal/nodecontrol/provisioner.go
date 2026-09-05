package nodecontrol

import (
	"context"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
)

// PayloadProvider returns the exact canonical preparation document committed
// for an intent. It must read the same durable source used to compute
// GroupEnrollmentIntent.ExpectedManifestDigest; deriving a document from
// endpoint capacity is deliberately unsupported.
type PayloadProvider func(context.Context, gateway.GroupEnrollmentIntent) ([]byte, error)

// Provisioner adapts the authenticated Client to gateway.NodeProvisioner.
// The controller owns the metadata CAS; this adapter only transports a
// committed intent's immutable preparation document to its target node.
type Provisioner struct {
	client  *Client
	payload PayloadProvider
}

func NewProvisioner(client *Client, payload PayloadProvider) (*Provisioner, error) {
	if client == nil || payload == nil {
		return nil, ErrControl
	}
	return &Provisioner{client: client, payload: payload}, nil
}

func (provisioner *Provisioner) PrepareReplica(ctx context.Context, intent gateway.GroupEnrollmentIntent) (gateway.PreparedReplicaProof, error) {
	if provisioner == nil || ctx == nil || !intent.Valid() {
		return gateway.PreparedReplicaProof{}, ErrControl
	}
	payload, err := provisioner.payload(ctx, intent)
	if err != nil {
		return gateway.PreparedReplicaProof{}, err
	}
	request, err := NewRequest(PhasePrepare, intent, payload)
	if err != nil {
		return gateway.PreparedReplicaProof{}, err
	}
	record, err := provisioner.client.Execute(ctx, intent.Target.Node, request)
	if err != nil {
		return gateway.PreparedReplicaProof{}, err
	}
	return record.Proof, nil
}

func (provisioner *Provisioner) EnrollReplica(ctx context.Context, intent gateway.GroupEnrollmentIntent, proof gateway.PreparedReplicaProof) error {
	if provisioner == nil || ctx == nil || !intent.Valid() || intent.State < gateway.EnrollmentEnrolled ||
		!proofMatchesIntent(proof, intent) {
		return ErrNotCommitted
	}
	payload, err := provisioner.payload(ctx, intent)
	if err != nil {
		return err
	}
	request, err := NewRequest(PhaseAdopt, intent, payload)
	if err != nil {
		return err
	}
	record, err := provisioner.client.Execute(ctx, intent.Target.Node, request)
	if err != nil {
		return err
	}
	if record.Proof != proof {
		return errors.Join(ErrConflict, ErrInvalidProof)
	}
	return nil
}

func (provisioner *Provisioner) VerifyReplica(ctx context.Context, intent gateway.GroupEnrollmentIntent) (gateway.PreparedReplicaProof, error) {
	return provisioner.PrepareReplica(ctx, intent)
}

// AbortPreparedReplica intentionally has no destructive wire operation. A
// prepared artifact is harmless while the replicated intent is cancelled, and
// deleting it remotely would make a late exact retry ambiguous. The controller
// can garbage-collect it through a separately committed retirement operation.
func (provisioner *Provisioner) AbortPreparedReplica(context.Context, gateway.GroupEnrollmentIntent) error {
	return ErrControl
}

var _ gateway.NodeProvisioner = (*Provisioner)(nil)
