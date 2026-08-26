package splitcontroller

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/gateway"
	vibejson "github.com/thesyncim/vibejson"
)

var ErrPlanAdmission = errors.New("splitcontroller: invalid durable plan admission")

const (
	planAdmissionFormat     = uint16(0)
	planAdmissionHeaderSize = 128
	planAdmissionDigestSize = sha256.Size
)

var (
	planAdmissionMagic  = [8]byte{'V', 'D', 'B', 'S', 'P', 'A', 'D', 0}
	planAdmissionDomain = []byte("vibedb/splitcontroller/plan-admission\x00")
)

// PlanAdmission is the compact shard-local restart witness for one plan. The
// catalog document is authenticated transiently during installation and is
// intentionally omitted here; replay after a shard restart requires the
// gateway catalog authority to re-present that exact image before actions are
// enabled again.
type PlanAdmission struct {
	Operation         OperationID
	CatalogGeneration uint64
	CatalogDigest     [sha256.Size]byte
	PlanDigest        [sha256.Size]byte
	Intent            []byte
}

func NewPlanAdmission(catalog *gateway.Snapshot, plan *Plan) (PlanAdmission, error) {
	if catalog == nil || plan == nil {
		return PlanAdmission{}, ErrPlanAdmission
	}
	intent, err := AppendPlanIntent(nil, catalog, plan)
	if err != nil {
		return PlanAdmission{}, errors.Join(ErrPlanAdmission, err)
	}
	digest, err := gateway.CatalogSnapshotDigest(catalog)
	if err != nil {
		return PlanAdmission{}, errors.Join(ErrPlanAdmission, err)
	}
	return PlanAdmission{
		Operation: plan.OperationID(), CatalogGeneration: catalog.Generation(),
		CatalogDigest: digest, PlanDigest: sha256.Sum256(intent), Intent: intent,
	}, nil
}

func AppendPlanAdmission(dst []byte, admission PlanAdmission) ([]byte, error) {
	if !validPlanAdmission(admission) {
		return dst, ErrPlanAdmission
	}
	start := len(dst)
	total := planAdmissionHeaderSize + len(admission.Intent) + planAdmissionDigestSize
	dst = append(dst, make([]byte, total)...)
	raw := dst[start:]
	copy(raw[:8], planAdmissionMagic[:])
	binary.LittleEndian.PutUint16(raw[8:10], planAdmissionFormat)
	binary.LittleEndian.PutUint32(raw[12:16], uint32(total))
	copy(raw[16:48], admission.Operation[:])
	binary.LittleEndian.PutUint64(raw[48:56], admission.CatalogGeneration)
	copy(raw[56:88], admission.CatalogDigest[:])
	copy(raw[88:120], admission.PlanDigest[:])
	binary.LittleEndian.PutUint32(raw[120:124], uint32(len(admission.Intent)))
	copy(raw[planAdmissionHeaderSize:], admission.Intent)
	digest := sha256.New()
	_, _ = digest.Write(planAdmissionDomain)
	_, _ = digest.Write(raw[:total-planAdmissionDigestSize])
	_ = digest.Sum(raw[total-planAdmissionDigestSize : total-planAdmissionDigestSize])
	return dst, nil
}

func OpenPlanAdmission(raw []byte) (PlanAdmission, error) {
	if len(raw) < planAdmissionHeaderSize+planAdmissionDigestSize ||
		len(raw) > MaxPlanAdmissionControlBytes || !bytes.Equal(raw[:8], planAdmissionMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != planAdmissionFormat ||
		binary.LittleEndian.Uint16(raw[10:12]) != 0 ||
		binary.LittleEndian.Uint32(raw[12:16]) != uint32(len(raw)) ||
		binary.LittleEndian.Uint32(raw[124:128]) != 0 {
		return PlanAdmission{}, ErrPlanAdmission
	}
	intentBytes := int(binary.LittleEndian.Uint32(raw[120:124]))
	if intentBytes == 0 || intentBytes > MaxPlanIntentBytes ||
		planAdmissionHeaderSize+intentBytes+planAdmissionDigestSize != len(raw) {
		return PlanAdmission{}, ErrPlanAdmission
	}
	digest := sha256.New()
	_, _ = digest.Write(planAdmissionDomain)
	_, _ = digest.Write(raw[:len(raw)-planAdmissionDigestSize])
	var computed [sha256.Size]byte
	_ = digest.Sum(computed[:0])
	if !bytes.Equal(computed[:], raw[len(raw)-planAdmissionDigestSize:]) {
		return PlanAdmission{}, ErrPlanAdmission
	}
	admission := PlanAdmission{CatalogGeneration: binary.LittleEndian.Uint64(raw[48:56])}
	copy(admission.Operation[:], raw[16:48])
	copy(admission.CatalogDigest[:], raw[56:88])
	copy(admission.PlanDigest[:], raw[88:120])
	admission.Intent = bytes.Clone(raw[planAdmissionHeaderSize : len(raw)-planAdmissionDigestSize])
	if !validPlanAdmission(admission) {
		return PlanAdmission{}, ErrPlanAdmission
	}
	canonical, err := vibejson.AppendCanonicalize(nil, admission.Intent)
	if err != nil || !bytes.Equal(canonical, admission.Intent) {
		return PlanAdmission{}, errors.Join(ErrPlanAdmission, err)
	}
	return admission, nil
}

func (admission PlanAdmission) Open(catalog *gateway.Snapshot) (*Plan, error) {
	if !validPlanAdmission(admission) || catalog == nil ||
		catalog.Generation() != admission.CatalogGeneration {
		return nil, ErrPlanAdmission
	}
	digest, err := gateway.CatalogSnapshotDigest(catalog)
	if err != nil || digest != admission.CatalogDigest {
		return nil, errors.Join(ErrPlanAdmission, err)
	}
	plan, err := OpenPlanIntent(admission.Intent, catalog)
	if err != nil || plan.OperationID() != admission.Operation {
		return nil, errors.Join(ErrPlanAdmission, err)
	}
	return plan, nil
}

// PersistPlanAdmission durably settles the compact operation witness before a
// caller publishes an in-memory action capability. Conflicting reinstallation
// fails closed; an exact retry is idempotent, including after outcome unknown.
func PersistPlanAdmission(lease *RuntimeStoreLease, admission PlanAdmission) error {
	if lease == nil || !validPlanAdmission(admission) {
		return ErrPlanAdmission
	}
	raw, err := AppendPlanAdmission(nil, admission)
	if err != nil {
		return err
	}
	current, present, err := lease.Load(RuntimeStatePlanAdmission, 0)
	if err != nil {
		return err
	}
	if present {
		if current.Revision == 1 && bytes.Equal(current.Payload, raw) {
			return nil
		}
		return ErrPlanAdmission
	}
	if err = lease.Persist(RuntimeStatePlanAdmission, 0, 1, raw); err == nil {
		return nil
	}
	settled, present, settleErr := lease.Load(RuntimeStatePlanAdmission, 0)
	if settleErr == nil && present && settled.Revision == 1 && bytes.Equal(settled.Payload, raw) {
		return nil
	}
	return errors.Join(ErrRuntimeStoreOutcomeUnknown, err, settleErr)
}

func validPlanAdmission(admission PlanAdmission) bool {
	return admission.Operation != (OperationID{}) && admission.CatalogGeneration != 0 &&
		admission.CatalogDigest != ([sha256.Size]byte{}) &&
		admission.PlanDigest != ([sha256.Size]byte{}) && len(admission.Intent) != 0 &&
		len(admission.Intent) <= MaxPlanIntentBytes && sha256.Sum256(admission.Intent) == admission.PlanDigest
}
