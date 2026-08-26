package splitcontroller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/thesyncim/vibedb/gateway"
	"github.com/thesyncim/vibedb/internal/rafttransport"
)

const (
	planAdmissionWireFormat      = uint16(0)
	planAdmissionRequestHeader   = 128
	planAdmissionRequestTail     = sha256.Size
	planAdmissionResponseBytes   = 144
	MaxPlanAdmissionCatalogBytes = 16 << 20
	MaxPlanAdmissionRequestBytes = MaxPlanAdmissionCatalogBytes + MaxPlanAdmissionControlBytes +
		planAdmissionRequestHeader + planAdmissionRequestTail
	AbsoluteMaxPlanAdmissionConcurrency = 64
)

var (
	planAdmissionRequestMagic  = [8]byte{'V', 'B', 'S', 'P', 'A', 'D', 'M', 0}
	planAdmissionResponseMagic = [8]byte{'V', 'B', 'S', 'P', 'A', 'C', 'K', 0}
	planAdmissionRequestDomain = []byte("vibedb/splitcontroller/plan-admission-request\x00")
	planAdmissionReceiptDomain = []byte("vibedb/splitcontroller/plan-admission-receipt\x00")
)

func PlanAdmissionRequestDiscriminator() [8]byte { return planAdmissionRequestMagic }

type PlanAdmissionEnvelope struct {
	Operation         OperationID
	CatalogGeneration uint64
	CatalogDigest     [sha256.Size]byte
	PlanDigest        [sha256.Size]byte
	CatalogBytes      uint32
	AdmissionBytes    uint32
}

type PlanAdmissionAuthorizeFunc func(rafttransport.PeerIdentity, PlanAdmissionEnvelope) bool

type PlanAdmissionServiceOptions struct {
	Installer        *PlanAdmissionInstaller
	Authorize        PlanAdmissionAuthorizeFunc
	ReadDeadline     rafttransport.DeadlineFunc
	WriteDeadline    rafttransport.DeadlineFunc
	MaxConcurrent    int
	MaxInflightBytes uint64
}

type PlanAdmissionService struct {
	installer     *PlanAdmissionInstaller
	authorize     PlanAdmissionAuthorizeFunc
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	slots         chan struct{}
	budget        planAdmissionByteBudget
}

type PlanAdmissionClientOptions struct {
	Opener           PlanObservationStreamOpener
	ReadDeadline     rafttransport.DeadlineFunc
	WriteDeadline    rafttransport.DeadlineFunc
	MaxConcurrent    int
	MaxInflightBytes uint64
}

type PlanAdmissionClient struct {
	opener        PlanObservationStreamOpener
	readDeadline  rafttransport.DeadlineFunc
	writeDeadline rafttransport.DeadlineFunc
	slots         chan struct{}
	budget        planAdmissionByteBudget
}

func NewPlanAdmissionClient(options PlanAdmissionClientOptions) (*PlanAdmissionClient, error) {
	if options.Opener == nil || options.ReadDeadline == nil || options.WriteDeadline == nil ||
		options.MaxConcurrent <= 0 || options.MaxConcurrent > AbsoluteMaxPlanAdmissionConcurrency ||
		options.MaxInflightBytes < uint64(MaxPlanAdmissionRequestBytes) ||
		options.MaxInflightBytes > uint64(MaxPlanAdmissionRequestBytes)*uint64(options.MaxConcurrent) {
		return nil, ErrPlanAdmission
	}
	return &PlanAdmissionClient{
		opener: options.Opener, readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		slots:  make(chan struct{}, options.MaxConcurrent),
		budget: planAdmissionByteBudget{max: options.MaxInflightBytes},
	}, nil
}

func (client *PlanAdmissionClient) Install(
	ctx context.Context,
	node rafttransport.NodeID,
	catalog *gateway.Snapshot,
	admission PlanAdmission,
) error {
	if client == nil || ctx == nil || node == (rafttransport.NodeID{}) || catalog == nil {
		return ErrPlanAdmission
	}
	select {
	case client.slots <- struct{}{}:
		defer func() { <-client.slots }()
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	request, err := AppendPlanAdmissionRequest(nil, catalog, admission)
	if err != nil || !client.budget.acquire(uint64(len(request))) {
		return errors.Join(ErrPlanAdmission, err)
	}
	defer client.budget.release(uint64(len(request)))
	connection, err := client.opener.OpenShardControl(ctx, node)
	if err != nil {
		return errors.Join(ErrPlanAdmission, err)
	}
	defer connection.Close()
	if err = setPlanObservationWriteDeadline(ctx, connection, client.writeDeadline); err != nil {
		return err
	}
	if err = writePlanObservationFull(connection, request); err != nil {
		return errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	if err = setPlanObservationReadDeadline(ctx, connection, client.readDeadline); err != nil {
		return errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	var response [planAdmissionResponseBytes]byte
	if _, err = io.ReadFull(connection, response[:]); err != nil {
		return errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	if !validPlanAdmissionResponse(response[:], admission) {
		return ErrPlanAdmission
	}
	return nil
}

type planAdmissionByteBudget struct {
	mu        sync.Mutex
	used, max uint64
}

func NewPlanAdmissionService(options PlanAdmissionServiceOptions) (*PlanAdmissionService, error) {
	if options.Installer == nil || options.Authorize == nil || options.ReadDeadline == nil ||
		options.WriteDeadline == nil || options.MaxConcurrent <= 0 ||
		options.MaxConcurrent > AbsoluteMaxPlanAdmissionConcurrency ||
		options.MaxInflightBytes < uint64(MaxPlanAdmissionRequestBytes) ||
		options.MaxInflightBytes > uint64(MaxPlanAdmissionRequestBytes)*uint64(options.MaxConcurrent) {
		return nil, ErrPlanAdmission
	}
	return &PlanAdmissionService{
		installer: options.Installer, authorize: options.Authorize,
		readDeadline: options.ReadDeadline, writeDeadline: options.WriteDeadline,
		slots:  make(chan struct{}, options.MaxConcurrent),
		budget: planAdmissionByteBudget{max: options.MaxInflightBytes},
	}, nil
}

func (service *PlanAdmissionService) Serve(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		if connection != nil {
			_ = connection.Close()
		}
		return ErrPlanAdmission
	}
	defer connection.Close()
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	default:
		return ErrPlanAdmission
	}
	if err := setPlanObservationReadDeadline(ctx, connection, service.readDeadline); err != nil {
		return err
	}
	var header [planAdmissionRequestHeader]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return errors.Join(ErrPlanAdmission, err)
	}
	envelope, total, err := openPlanAdmissionRequestHeader(header[:])
	if err != nil || !service.authorize(connection.PeerIdentity(), envelope) ||
		!service.budget.acquire(uint64(total)) {
		return ErrPlanAdmission
	}
	defer service.budget.release(uint64(total))
	raw := make([]byte, total)
	copy(raw, header[:])
	if _, err = io.ReadFull(connection, raw[len(header):]); err != nil {
		return errors.Join(ErrPlanAdmission, err)
	}
	catalog, admission, err := openPlanAdmissionRequest(raw, envelope)
	if err != nil {
		return err
	}
	if err = service.installer.Install(ctx, catalog, admission); err != nil {
		return err
	}
	response := appendPlanAdmissionResponse(nil, admission)
	if err = setPlanObservationWriteDeadline(ctx, connection, service.writeDeadline); err != nil {
		return err
	}
	return writePlanObservationFull(connection, response)
}

func AppendPlanAdmissionRequest(
	dst []byte, catalog *gateway.Snapshot, admission PlanAdmission,
) ([]byte, error) {
	if catalog == nil {
		return dst, ErrPlanAdmission
	}
	if _, err := admission.Open(catalog); err != nil {
		return dst, err
	}
	catalogRaw, err := gateway.AppendSnapshotDocument(nil, catalog)
	if err != nil || len(catalogRaw) == 0 || len(catalogRaw) > MaxPlanAdmissionCatalogBytes {
		return dst, errors.Join(ErrPlanAdmission, err)
	}
	admissionRaw, err := AppendPlanAdmission(nil, admission)
	if err != nil {
		return dst, err
	}
	total := planAdmissionRequestHeader + len(catalogRaw) + len(admissionRaw) + planAdmissionRequestTail
	start := len(dst)
	dst = append(dst, make([]byte, total)...)
	raw := dst[start:]
	copy(raw[:8], planAdmissionRequestMagic[:])
	binary.LittleEndian.PutUint16(raw[8:10], planAdmissionWireFormat)
	binary.LittleEndian.PutUint32(raw[12:16], uint32(total))
	binary.LittleEndian.PutUint32(raw[16:20], uint32(len(catalogRaw)))
	binary.LittleEndian.PutUint32(raw[20:24], uint32(len(admissionRaw)))
	copy(raw[24:56], admission.Operation[:])
	binary.LittleEndian.PutUint64(raw[56:64], admission.CatalogGeneration)
	copy(raw[64:96], admission.CatalogDigest[:])
	copy(raw[96:128], admission.PlanDigest[:])
	copy(raw[planAdmissionRequestHeader:], catalogRaw)
	copy(raw[planAdmissionRequestHeader+len(catalogRaw):], admissionRaw)
	digest := sha256.New()
	_, _ = digest.Write(planAdmissionRequestDomain)
	_, _ = digest.Write(raw[:total-planAdmissionRequestTail])
	_ = digest.Sum(raw[total-planAdmissionRequestTail : total-planAdmissionRequestTail])
	return dst, nil
}

func openPlanAdmissionRequestHeader(raw []byte) (PlanAdmissionEnvelope, int, error) {
	if len(raw) != planAdmissionRequestHeader || !bytes.Equal(raw[:8], planAdmissionRequestMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != planAdmissionWireFormat ||
		binary.LittleEndian.Uint16(raw[10:12]) != 0 {
		return PlanAdmissionEnvelope{}, 0, ErrPlanAdmission
	}
	total := int(binary.LittleEndian.Uint32(raw[12:16]))
	envelope := PlanAdmissionEnvelope{
		CatalogBytes:      binary.LittleEndian.Uint32(raw[16:20]),
		AdmissionBytes:    binary.LittleEndian.Uint32(raw[20:24]),
		CatalogGeneration: binary.LittleEndian.Uint64(raw[56:64]),
	}
	copy(envelope.Operation[:], raw[24:56])
	copy(envelope.CatalogDigest[:], raw[64:96])
	copy(envelope.PlanDigest[:], raw[96:128])
	if envelope.Operation == (OperationID{}) || envelope.CatalogGeneration == 0 ||
		envelope.CatalogDigest == ([32]byte{}) || envelope.PlanDigest == ([32]byte{}) ||
		envelope.CatalogBytes == 0 || envelope.CatalogBytes > MaxPlanAdmissionCatalogBytes ||
		envelope.AdmissionBytes == 0 || envelope.AdmissionBytes > MaxPlanAdmissionControlBytes ||
		total != planAdmissionRequestHeader+int(envelope.CatalogBytes)+
			int(envelope.AdmissionBytes)+planAdmissionRequestTail || total > MaxPlanAdmissionRequestBytes {
		return PlanAdmissionEnvelope{}, 0, ErrPlanAdmission
	}
	return envelope, total, nil
}

func openPlanAdmissionRequest(
	raw []byte, envelope PlanAdmissionEnvelope,
) (*gateway.Snapshot, PlanAdmission, error) {
	opened, total, err := openPlanAdmissionRequestHeader(raw[:planAdmissionRequestHeader])
	if err != nil || opened != envelope || total != len(raw) {
		return nil, PlanAdmission{}, ErrPlanAdmission
	}
	digest := sha256.New()
	_, _ = digest.Write(planAdmissionRequestDomain)
	_, _ = digest.Write(raw[:len(raw)-planAdmissionRequestTail])
	var computed [32]byte
	_ = digest.Sum(computed[:0])
	if !bytes.Equal(computed[:], raw[len(raw)-planAdmissionRequestTail:]) {
		return nil, PlanAdmission{}, ErrPlanAdmission
	}
	catalogEnd := planAdmissionRequestHeader + int(envelope.CatalogBytes)
	catalog, err := gateway.OpenSnapshotDocument(raw[planAdmissionRequestHeader:catalogEnd])
	if err != nil || catalog.Generation() != envelope.CatalogGeneration {
		return nil, PlanAdmission{}, errors.Join(ErrPlanAdmission, err)
	}
	catalogDigest, err := gateway.CatalogSnapshotDigest(catalog)
	if err != nil || catalogDigest != envelope.CatalogDigest {
		return nil, PlanAdmission{}, errors.Join(ErrPlanAdmission, err)
	}
	admission, err := OpenPlanAdmission(raw[catalogEnd : len(raw)-planAdmissionRequestTail])
	if err != nil || admission.Operation != envelope.Operation ||
		admission.CatalogGeneration != envelope.CatalogGeneration ||
		admission.CatalogDigest != envelope.CatalogDigest || admission.PlanDigest != envelope.PlanDigest {
		return nil, PlanAdmission{}, errors.Join(ErrPlanAdmission, err)
	}
	return catalog, admission, nil
}

func appendPlanAdmissionResponse(dst []byte, admission PlanAdmission) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, planAdmissionResponseBytes)...)
	raw := dst[start:]
	copy(raw[:8], planAdmissionResponseMagic[:])
	binary.LittleEndian.PutUint16(raw[8:10], planAdmissionWireFormat)
	raw[10] = 1
	binary.LittleEndian.PutUint32(raw[12:16], planAdmissionResponseBytes)
	copy(raw[16:48], admission.Operation[:])
	copy(raw[48:80], admission.PlanDigest[:])
	digest := sha256.New()
	_, _ = digest.Write(planAdmissionReceiptDomain)
	_, _ = digest.Write(raw[:80])
	_ = digest.Sum(raw[80:80])
	digest.Reset()
	_, _ = digest.Write(planAdmissionReceiptDomain)
	_, _ = digest.Write(raw[:112])
	_ = digest.Sum(raw[112:112])
	return dst
}

func validPlanAdmissionResponse(raw []byte, admission PlanAdmission) bool {
	if len(raw) != planAdmissionResponseBytes || !bytes.Equal(raw[:8], planAdmissionResponseMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != planAdmissionWireFormat || raw[10] != 1 || raw[11] != 0 ||
		binary.LittleEndian.Uint32(raw[12:16]) != planAdmissionResponseBytes ||
		!bytes.Equal(raw[16:48], admission.Operation[:]) || !bytes.Equal(raw[48:80], admission.PlanDigest[:]) {
		return false
	}
	digest := sha256.New()
	_, _ = digest.Write(planAdmissionReceiptDomain)
	_, _ = digest.Write(raw[:80])
	var receipt [32]byte
	_ = digest.Sum(receipt[:0])
	if !bytes.Equal(receipt[:], raw[80:112]) {
		return false
	}
	digest.Reset()
	_, _ = digest.Write(planAdmissionReceiptDomain)
	_, _ = digest.Write(raw[:112])
	var tail [32]byte
	_ = digest.Sum(tail[:0])
	return bytes.Equal(tail[:], raw[112:])
}

func (budget *planAdmissionByteBudget) acquire(size uint64) bool {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if size == 0 || size > budget.max-budget.used {
		return false
	}
	budget.used += size
	return true
}

func (budget *planAdmissionByteBudget) release(size uint64) {
	budget.mu.Lock()
	if size > budget.used {
		panic("splitcontroller: plan admission byte budget underflow")
	}
	budget.used -= size
	budget.mu.Unlock()
}
