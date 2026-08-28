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
	terminalRetirementRequestBytes  = 176
	terminalRetirementResponseBytes = 104
)

var (
	terminalRetirementRequestMagic  = [8]byte{'V', 'D', 'B', 'S', 'T', 'R', 'M', 0}
	terminalRetirementResponseMagic = [8]byte{'V', 'D', 'B', 'S', 'T', 'A', 'K', 0}
	terminalRetirementDomain        = []byte("vibedb/splitcontroller/terminal-retirement\x00")
	terminalRetirementProofDomain   = []byte("vibedb/splitcontroller/terminal-proof\x00")
)

func TerminalRetirementRequestDiscriminator() [8]byte { return terminalRetirementRequestMagic }

type TerminalRetirement struct {
	Operation         OperationID
	PlanDigest        [32]byte
	CatalogGeneration uint64
	CatalogDigest     [32]byte
	Proof             [32]byte
}

func DeriveTerminalRetirement(
	catalog *gateway.Snapshot, plan *Plan, observed Observation,
) (TerminalRetirement, error) {
	if catalog == nil || plan == nil || observed.Certificate == nil ||
		catalog.Generation() == 0 || observed.Catalog != catalog {
		return TerminalRetirement{}, ErrSplitOperationRetirement
	}
	intent, err := AppendPlanIntent(nil, catalog, plan)
	if err != nil {
		return TerminalRetirement{}, err
	}
	catalogDigest, err := gateway.CatalogSnapshotDigest(catalog)
	if err != nil {
		return TerminalRetirement{}, err
	}
	result := TerminalRetirement{
		Operation: plan.operation, PlanDigest: sha256.Sum256(intent),
		CatalogGeneration: catalog.Generation(), CatalogDigest: catalogDigest,
	}
	hash := sha256.New()
	_, _ = hash.Write(terminalRetirementProofDomain)
	_, _ = hash.Write(result.Operation[:])
	_, _ = hash.Write(result.PlanDigest[:])
	var scalar [8]byte
	binary.LittleEndian.PutUint64(scalar[:], result.CatalogGeneration)
	_, _ = hash.Write(scalar[:])
	_, _ = hash.Write(result.CatalogDigest[:])
	certificate := observed.Certificate.Digest()
	_, _ = hash.Write(certificate[:])
	drain, err := gateway.AppendClusterCatalogDrainCertificate(nil, observed.CatalogDrainCertificate)
	if err != nil {
		return TerminalRetirement{}, err
	}
	_, _ = hash.Write(drain)
	_ = hash.Sum(result.Proof[:0])
	if result.Proof == ([32]byte{}) {
		return TerminalRetirement{}, ErrSplitOperationRetirement
	}
	return result, nil
}

type LocalTerminalRetirer struct {
	binder *BoundPlanAdmissionBinder
	grants *DynamicShardActionGrants
	data   *DynamicSplitData
}

func NewLocalTerminalRetirer(
	binder *BoundPlanAdmissionBinder, grants *DynamicShardActionGrants, data *DynamicSplitData,
) (*LocalTerminalRetirer, error) {
	if binder == nil || grants == nil || data == nil || binder.grants != grants {
		return nil, ErrSplitOperationRetirement
	}
	return &LocalTerminalRetirer{binder: binder, grants: grants, data: data}, nil
}

func (retirer *LocalTerminalRetirer) RetireTerminal(
	retirement TerminalRetirement,
) error {
	if retirer == nil || !validTerminalRetirement(retirement) {
		return ErrSplitOperationRetirement
	}
	if err := retirer.binder.validateCertifiedRetirement(
		retirement.Operation, retirement.PlanDigest, retirement.CatalogGeneration,
		retirement.CatalogDigest, retirement.Proof,
	); err != nil {
		return err
	}
	retirer.grants.retire(retirement.Operation, retirement.PlanDigest)
	if err := retirer.data.retire(retirement.Operation, retirement.PlanDigest); err != nil {
		return err
	}
	return retirer.binder.retireCertified(
		retirement.Operation, retirement.PlanDigest, retirement.CatalogGeneration,
		retirement.CatalogDigest, retirement.Proof,
	)
}

func validTerminalRetirement(value TerminalRetirement) bool {
	return value.Operation != (OperationID{}) && value.PlanDigest != ([32]byte{}) &&
		value.CatalogGeneration != 0 && value.CatalogDigest != ([32]byte{}) && value.Proof != ([32]byte{})
}

type TerminalRetirementService struct {
	retirer     TerminalRetirer
	authorize   func(rafttransport.PeerIdentity, TerminalRetirement) bool
	read, write rafttransport.DeadlineFunc
	slots       chan struct{}
}

// TerminalRetirer settles certified local retirement before a response.
type TerminalRetirer interface {
	RetireTerminal(TerminalRetirement) error
}

func NewTerminalRetirementService(
	retirer TerminalRetirer,
	authorize func(rafttransport.PeerIdentity, TerminalRetirement) bool,
	read, write rafttransport.DeadlineFunc,
) (*TerminalRetirementService, error) {
	if retirer == nil || authorize == nil || read == nil || write == nil {
		return nil, ErrSplitOperationRetirement
	}
	return &TerminalRetirementService{
		retirer: retirer, authorize: authorize, read: read, write: write,
		slots: make(chan struct{}, MaxPlanObservationEndpoints),
	}, nil
}

func (service *TerminalRetirementService) Serve(
	ctx context.Context, connection rafttransport.PeerConnection,
) error {
	if service == nil || ctx == nil || connection == nil ||
		connection.TrafficClass() != rafttransport.TrafficShardControl {
		return ErrSplitOperationRetirement
	}
	defer connection.Close()
	select {
	case service.slots <- struct{}{}:
		defer func() { <-service.slots }()
	case <-ctx.Done():
		return errors.Join(ErrSplitOperationRetirement, ctx.Err())
	}
	if err := setPlanObservationReadDeadline(ctx, connection, service.read); err != nil {
		return err
	}
	var raw [terminalRetirementRequestBytes]byte
	if _, err := io.ReadFull(connection, raw[:]); err != nil {
		return err
	}
	retirement, err := openTerminalRetirement(raw[:])
	if err != nil || !service.authorize(connection.PeerIdentity(), retirement) {
		return errors.Join(ErrSplitOperationRetirement, err)
	}
	if err = service.retirer.RetireTerminal(retirement); err != nil {
		return err
	}
	response := appendTerminalRetirementResponse(nil, retirement)
	if err = setPlanObservationWriteDeadline(ctx, connection, service.write); err != nil {
		return err
	}
	return writePlanObservationFull(connection, response)
}

type TerminalRetirementClient struct {
	opener      PlanObservationStreamOpener
	read, write rafttransport.DeadlineFunc
}

type RF3TerminalRetirementCoordinator struct {
	client   *TerminalRetirementClient
	attempts int
}

func NewRF3TerminalRetirementCoordinator(
	client *TerminalRetirementClient, attempts int,
) (*RF3TerminalRetirementCoordinator, error) {
	if client == nil || attempts <= 0 || attempts > 16 {
		return nil, ErrSplitOperationRetirement
	}
	return &RF3TerminalRetirementCoordinator{client: client, attempts: attempts}, nil
}

func (coordinator *RF3TerminalRetirementCoordinator) RetirePlan(
	ctx context.Context, catalog *gateway.Snapshot, plan *Plan, observed Observation,
) error {
	if coordinator == nil || ctx == nil {
		return ErrSplitOperationRetirement
	}
	retirement, err := DeriveTerminalRetirement(catalog, plan, observed)
	if err != nil {
		return err
	}
	nodes, err := exactPlanAdmissionNodes(catalog, plan)
	if err != nil {
		return err
	}
	errorsByNode := make([]error, len(nodes))
	var group sync.WaitGroup
	group.Add(len(nodes))
	for index := range nodes {
		go func(index int) {
			defer group.Done()
			for attempt := 0; attempt < coordinator.attempts; attempt++ {
				err := coordinator.client.Retire(ctx, nodes[index], retirement)
				if err == nil {
					return
				}
				errorsByNode[index] = errors.Join(errorsByNode[index], err)
			}
		}(index)
	}
	group.Wait()
	return errors.Join(errorsByNode...)
}

func NewTerminalRetirementClient(
	opener PlanObservationStreamOpener, read, write rafttransport.DeadlineFunc,
) (*TerminalRetirementClient, error) {
	if opener == nil || read == nil || write == nil {
		return nil, ErrSplitOperationRetirement
	}
	return &TerminalRetirementClient{opener: opener, read: read, write: write}, nil
}

func (client *TerminalRetirementClient) Retire(
	ctx context.Context, node rafttransport.NodeID, retirement TerminalRetirement,
) error {
	if client == nil || ctx == nil || node == (rafttransport.NodeID{}) || !validTerminalRetirement(retirement) {
		return ErrSplitOperationRetirement
	}
	connection, err := client.opener.OpenShardControl(ctx, node)
	if err != nil {
		return err
	}
	defer connection.Close()
	request := appendTerminalRetirement(nil, retirement)
	if err = setPlanObservationWriteDeadline(ctx, connection, client.write); err != nil {
		return err
	}
	if err = writePlanObservationFull(connection, request); err != nil {
		return errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	if err = setPlanObservationReadDeadline(ctx, connection, client.read); err != nil {
		return errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	var response [terminalRetirementResponseBytes]byte
	if _, err = io.ReadFull(connection, response[:]); err != nil ||
		!validTerminalRetirementResponse(response[:], retirement) {
		return errors.Join(ErrRuntimeStoreOutcomeUnknown, err)
	}
	return nil
}

func appendTerminalRetirement(dst []byte, value TerminalRetirement) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, terminalRetirementRequestBytes)...)
	raw := dst[start:]
	copy(raw[:8], terminalRetirementRequestMagic[:])
	copy(raw[8:40], value.Operation[:])
	copy(raw[40:72], value.PlanDigest[:])
	binary.LittleEndian.PutUint64(raw[72:80], value.CatalogGeneration)
	copy(raw[80:112], value.CatalogDigest[:])
	copy(raw[112:144], value.Proof[:])
	hash := sha256.New()
	_, _ = hash.Write(terminalRetirementDomain)
	_, _ = hash.Write(raw[:144])
	_ = hash.Sum(raw[144:144])
	return dst
}

func openTerminalRetirement(raw []byte) (TerminalRetirement, error) {
	var result TerminalRetirement
	if len(raw) != terminalRetirementRequestBytes || !bytes.Equal(raw[:8], terminalRetirementRequestMagic[:]) {
		return result, ErrSplitOperationRetirement
	}
	copy(result.Operation[:], raw[8:40])
	copy(result.PlanDigest[:], raw[40:72])
	result.CatalogGeneration = binary.LittleEndian.Uint64(raw[72:80])
	copy(result.CatalogDigest[:], raw[80:112])
	copy(result.Proof[:], raw[112:144])
	hash := sha256.New()
	_, _ = hash.Write(terminalRetirementDomain)
	_, _ = hash.Write(raw[:144])
	var digest [32]byte
	_ = hash.Sum(digest[:0])
	if !validTerminalRetirement(result) || !bytes.Equal(digest[:], raw[144:]) {
		return TerminalRetirement{}, ErrSplitOperationRetirement
	}
	return result, nil
}

func appendTerminalRetirementResponse(dst []byte, value TerminalRetirement) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, terminalRetirementResponseBytes)...)
	raw := dst[start:]
	copy(raw[:8], terminalRetirementResponseMagic[:])
	copy(raw[8:40], value.Operation[:])
	copy(raw[40:72], value.Proof[:])
	hash := sha256.New()
	_, _ = hash.Write(terminalRetirementDomain)
	_, _ = hash.Write(raw[:72])
	_ = hash.Sum(raw[72:72])
	return dst
}

func validTerminalRetirementResponse(raw []byte, value TerminalRetirement) bool {
	if len(raw) != terminalRetirementResponseBytes || !bytes.Equal(raw[:8], terminalRetirementResponseMagic[:]) ||
		!bytes.Equal(raw[8:40], value.Operation[:]) || !bytes.Equal(raw[40:72], value.Proof[:]) {
		return false
	}
	hash := sha256.New()
	_, _ = hash.Write(terminalRetirementDomain)
	_, _ = hash.Write(raw[:72])
	var digest [32]byte
	_ = hash.Sum(digest[:0])
	return bytes.Equal(digest[:], raw[72:])
}
