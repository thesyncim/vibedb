package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"

	"github.com/thesyncim/vibedb/internal/rafttransport"
)

const (
	childPrepareWireFormat    = uint16(0)
	childPrepareHeaderBytes   = 64
	MaxChildPrepareWireBytes  = MaxChildPreparationBytes + childPrepareHeaderBytes
	MaxChildPrepareConcurrent = 64
)

var childPrepareRequestMagic = [8]byte{'V', 'D', 'B', 'S', 'C', 'P', 'R', 0}

func ChildPrepareRequestDiscriminator() [8]byte { return childPrepareRequestMagic }

type ChildPrepareAuthorizeFunc func(rafttransport.PeerIdentity, ChildPreparation) bool

type ChildPreparer interface {
	PrepareChild(context.Context, ChildPreparation) (ChildPrepareReceipt, error)
}

type ChildPrepareServiceOptions struct {
	Preparer         ChildPreparer
	Authorize        ChildPrepareAuthorizeFunc
	ReadDeadline     rafttransport.DeadlineFunc
	WriteDeadline    rafttransport.DeadlineFunc
	MaxConcurrent    int
	MaxInflightBytes uint64
}

type ChildPrepareService struct {
	preparer      ChildPreparer
	authorize     ChildPrepareAuthorizeFunc
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	slots         chan struct{}
	budget        planAdmissionByteBudget
}

type ChildPrepareClientOptions struct {
	Opener           PlanObservationStreamOpener
	ReadDeadline     rafttransport.DeadlineFunc
	WriteDeadline    rafttransport.DeadlineFunc
	MaxConcurrent    int
	MaxInflightBytes uint64
}

type ChildPrepareClient struct {
	opener        PlanObservationStreamOpener
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	slots         chan struct{}
	budget        planAdmissionByteBudget
}

func NewChildPrepareService(options ChildPrepareServiceOptions) (*ChildPrepareService, error) {
	if options.Preparer == nil || options.Authorize == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil || options.MaxConcurrent <= 0 ||
		options.MaxConcurrent > MaxChildPrepareConcurrent ||
		options.MaxInflightBytes < MaxChildPrepareWireBytes ||
		options.MaxInflightBytes > uint64(MaxChildPrepareWireBytes)*uint64(options.MaxConcurrent) {
		return nil, ErrChildPreparation
	}
	return &ChildPrepareService{
		preparer: options.Preparer, authorize: options.Authorize,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		slots:  make(chan struct{}, options.MaxConcurrent),
		budget: planAdmissionByteBudget{max: options.MaxInflightBytes},
	}, nil
}

func NewChildPrepareClient(options ChildPrepareClientOptions) (*ChildPrepareClient, error) {
	if options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil ||
		options.MaxConcurrent <= 0 || options.MaxConcurrent > MaxChildPrepareConcurrent ||
		options.MaxInflightBytes < MaxChildPrepareWireBytes ||
		options.MaxInflightBytes > uint64(MaxChildPrepareWireBytes)*uint64(options.MaxConcurrent) {
		return nil, ErrChildPreparation
	}
	return &ChildPrepareClient{
		opener: options.Opener, readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		slots:  make(chan struct{}, options.MaxConcurrent),
		budget: planAdmissionByteBudget{max: options.MaxInflightBytes},
	}, nil
}

func (service *ChildPrepareService) Serve(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrChildPreparation
	}
	defer connection.Close()
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return ErrChildPreparation
	}
	if err := setPlanObservationReadDeadline(ctx, connection, service.readDeadline); err != nil {
		return err
	}
	var header [childPrepareHeaderBytes]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return errors.Join(ErrChildPreparation, err)
	}
	payloadBytes, requestDigest, err := openChildPrepareHeader(header[:])
	if err != nil || !service.budget.acquire(uint64(childPrepareHeaderBytes+payloadBytes)) {
		return ErrChildPreparation
	}
	defer service.budget.release(uint64(childPrepareHeaderBytes + payloadBytes))
	payload := make([]byte, payloadBytes)
	if _, err = io.ReadFull(connection, payload); err != nil {
		return errors.Join(ErrChildPreparation, err)
	}
	preparation, err := OpenChildPreparation(payload)
	if err != nil {
		return err
	}
	digest, err := ChildPreparationDigest(preparation)
	if err != nil || digest != requestDigest || !service.authorize(connection.PeerIdentity(), preparation) {
		return errors.Join(ErrChildPreparation, err)
	}
	receipt, err := service.preparer.PrepareChild(ctx, preparation)
	if err != nil || receipt.RequestDigest != requestDigest {
		return errors.Join(ErrChildPreparation, err)
	}
	response, err := appendChildPrepareFrame(nil, receipt)
	if err != nil {
		return err
	}
	if err = setPlanObservationWriteDeadline(ctx, connection, service.writeDeadline); err != nil {
		return err
	}
	return writePlanObservationFull(connection, response)
}

func (client *ChildPrepareClient) Prepare(
	ctx context.Context, node rafttransport.NodeID, preparation ChildPreparation,
) (ChildPrepareReceipt, error) {
	if client == nil || ctx == nil || node == (rafttransport.NodeID{}) {
		return ChildPrepareReceipt{}, ErrChildPreparation
	}
	select {
	case client.slots <- struct{}{}:
		defer func() { <-client.slots }()
	case <-ctx.Done():
		return ChildPrepareReceipt{}, context.Cause(ctx)
	}
	request, err := appendChildPrepareRequestFrame(nil, preparation)
	if err != nil || !client.budget.acquire(uint64(len(request))) {
		return ChildPrepareReceipt{}, errors.Join(ErrChildPreparation, err)
	}
	connection, err := client.opener.OpenShardControl(ctx, node)
	if err != nil {
		client.budget.release(uint64(len(request)))
		return ChildPrepareReceipt{}, errors.Join(ErrChildPreparation, err)
	}
	defer connection.Close()
	if err = setPlanObservationWriteDeadline(ctx, connection, client.writeDeadline); err != nil {
		client.budget.release(uint64(len(request)))
		return ChildPrepareReceipt{}, err
	}
	if err = writePlanObservationFull(connection, request); err != nil {
		client.budget.release(uint64(len(request)))
		return ChildPrepareReceipt{}, errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	client.budget.release(uint64(len(request)))
	if err = setPlanObservationReadDeadline(ctx, connection, client.readDeadline); err != nil {
		return ChildPrepareReceipt{}, errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	var header [childPrepareHeaderBytes]byte
	if _, err = io.ReadFull(connection, header[:]); err != nil {
		return ChildPrepareReceipt{}, errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	payloadBytes, requestDigest, err := openChildPrepareHeader(header[:])
	if err != nil || !client.budget.acquire(uint64(payloadBytes)) {
		return ChildPrepareReceipt{}, errors.Join(ErrChildPreparation, err)
	}
	defer client.budget.release(uint64(payloadBytes))
	payload := make([]byte, payloadBytes)
	if _, err = io.ReadFull(connection, payload); err != nil {
		return ChildPrepareReceipt{}, errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	receipt, err := OpenChildPrepareReceipt(payload)
	if err != nil || receipt.RequestDigest != requestDigest {
		return ChildPrepareReceipt{}, errors.Join(ErrChildPreparation, err)
	}
	want, err := ChildPreparationDigest(preparation)
	if err != nil || requestDigest != want {
		return ChildPrepareReceipt{}, errors.Join(ErrChildPreparation, err)
	}
	return receipt, nil
}

func appendChildPrepareRequestFrame(dst []byte, preparation ChildPreparation) ([]byte, error) {
	payload, err := AppendChildPreparation(nil, preparation)
	if err != nil {
		return dst, err
	}
	digest, err := ChildPreparationDigest(preparation)
	if err != nil {
		return dst, err
	}
	return appendChildPrepareWireFrame(dst, payload, digest), nil
}

func appendChildPrepareFrame(dst []byte, receipt ChildPrepareReceipt) ([]byte, error) {
	payload, err := AppendChildPrepareReceipt(nil, receipt)
	if err != nil {
		return dst, err
	}
	return appendChildPrepareWireFrame(dst, payload, receipt.RequestDigest), nil
}

func appendChildPrepareWireFrame(dst, payload []byte, digest [sha256.Size]byte) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, childPrepareHeaderBytes+len(payload))...)
	raw := dst[start:]
	copy(raw[:8], childPrepareRequestMagic[:])
	binary.LittleEndian.PutUint16(raw[8:10], childPrepareWireFormat)
	binary.LittleEndian.PutUint32(raw[12:16], uint32(len(raw)))
	binary.LittleEndian.PutUint32(raw[16:20], uint32(len(payload)))
	copy(raw[24:56], digest[:])
	copy(raw[childPrepareHeaderBytes:], payload)
	return dst
}

func openChildPrepareHeader(raw []byte) (int, [sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	if len(raw) != childPrepareHeaderBytes || !bytes.Equal(raw[:8], childPrepareRequestMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != childPrepareWireFormat ||
		binary.LittleEndian.Uint16(raw[10:12]) != 0 || binary.LittleEndian.Uint32(raw[20:24]) != 0 ||
		!bytes.Equal(raw[56:64], make([]byte, 8)) {
		return 0, digest, ErrChildPreparation
	}
	total := int(binary.LittleEndian.Uint32(raw[12:16]))
	payload := int(binary.LittleEndian.Uint32(raw[16:20]))
	copy(digest[:], raw[24:56])
	if payload <= 0 || payload > MaxChildPreparationBytes || total != childPrepareHeaderBytes+payload ||
		total > MaxChildPrepareWireBytes || digest == ([sha256.Size]byte{}) {
		return 0, [sha256.Size]byte{}, ErrChildPreparation
	}
	return payload, digest, nil
}
