package splitcontroller

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const (
	MaxActionWitnessControlBytes = 232
	actionWitnessFormat          = uint16(0)
	actionWitnessIntent          = uint8(1)
	actionWitnessSettled         = uint8(2)
)

var (
	actionWitnessMagic  = [8]byte{'V', 'D', 'B', 'S', 'P', 'A', 'W', 0}
	actionWitnessDomain = []byte("vibedb/splitcontroller/action-witness\x00")
)

// ActionWitness binds one gateway-authorized local step to the exact admitted
// plan and predecessor cut. Sequence is monotonic within an operation; Step is
// the replay identity already persisted by the outer shard-control journal.
type ActionWitness struct {
	Operation         OperationID
	PlanDigest        [sha256.Size]byte
	CatalogGeneration uint64
	CatalogDigest     [sha256.Size]byte
	Sequence          uint64
	PredecessorDigest [sha256.Size]byte
	Step              [sha256.Size]byte
	Settled           bool
}

func AppendActionWitness(dst []byte, witness ActionWitness) ([]byte, error) {
	if !validActionWitness(witness) {
		return dst, ErrRemoteExecution
	}
	start := len(dst)
	dst = append(dst, make([]byte, MaxActionWitnessControlBytes)...)
	raw := dst[start:]
	copy(raw[:8], actionWitnessMagic[:])
	binary.LittleEndian.PutUint16(raw[8:10], actionWitnessFormat)
	binary.LittleEndian.PutUint32(raw[12:16], MaxActionWitnessControlBytes)
	copy(raw[16:48], witness.Operation[:])
	copy(raw[48:80], witness.PlanDigest[:])
	binary.LittleEndian.PutUint64(raw[80:88], witness.CatalogGeneration)
	copy(raw[88:120], witness.CatalogDigest[:])
	binary.LittleEndian.PutUint64(raw[120:128], witness.Sequence)
	copy(raw[128:160], witness.PredecessorDigest[:])
	copy(raw[160:192], witness.Step[:])
	raw[192] = actionWitnessIntent
	if witness.Settled {
		raw[192] = actionWitnessSettled
	}
	hash := sha256.New()
	_, _ = hash.Write(actionWitnessDomain)
	_, _ = hash.Write(raw[:200])
	_ = hash.Sum(raw[200:200])
	return dst, nil
}

func OpenActionWitness(raw []byte) (ActionWitness, error) {
	if len(raw) != MaxActionWitnessControlBytes || !bytes.Equal(raw[:8], actionWitnessMagic[:]) ||
		binary.LittleEndian.Uint16(raw[8:10]) != actionWitnessFormat ||
		binary.LittleEndian.Uint16(raw[10:12]) != 0 ||
		binary.LittleEndian.Uint32(raw[12:16]) != MaxActionWitnessControlBytes ||
		(raw[192] != actionWitnessIntent && raw[192] != actionWitnessSettled) ||
		!allZero(raw[193:200]) {
		return ActionWitness{}, ErrRemoteExecution
	}
	hash := sha256.New()
	_, _ = hash.Write(actionWitnessDomain)
	_, _ = hash.Write(raw[:200])
	var digest [sha256.Size]byte
	_ = hash.Sum(digest[:0])
	if !bytes.Equal(digest[:], raw[200:]) {
		return ActionWitness{}, ErrRemoteExecution
	}
	result := ActionWitness{
		CatalogGeneration: binary.LittleEndian.Uint64(raw[80:88]),
		Sequence:          binary.LittleEndian.Uint64(raw[120:128]),
		Settled:           raw[192] == actionWitnessSettled,
	}
	copy(result.Operation[:], raw[16:48])
	copy(result.PlanDigest[:], raw[48:80])
	copy(result.CatalogDigest[:], raw[88:120])
	copy(result.PredecessorDigest[:], raw[128:160])
	copy(result.Step[:], raw[160:192])
	if !validActionWitness(result) {
		return ActionWitness{}, ErrRemoteExecution
	}
	canonical, err := AppendActionWitness(nil, result)
	if err != nil || !bytes.Equal(canonical, raw) {
		return ActionWitness{}, errors.Join(ErrRemoteExecution, err)
	}
	return result, nil
}

func validActionWitness(witness ActionWitness) bool {
	return witness.Operation != (OperationID{}) && witness.PlanDigest != ([32]byte{}) &&
		witness.CatalogGeneration != 0 && witness.CatalogDigest != ([32]byte{}) &&
		witness.Sequence != 0 && witness.PredecessorDigest != ([32]byte{}) &&
		witness.Step != ([32]byte{})
}

func allZero(raw []byte) bool {
	for _, value := range raw {
		if value != 0 {
			return false
		}
	}
	return true
}

// BeginActionWitness durably installs an intent on every local operation
// store. Exact retries are accepted; skips behind an unsettled predecessor,
// regressions, and same-sequence substitutions fail closed.
func BeginActionWitness(leases []*RuntimeStoreLease, witness ActionWitness) error {
	if len(leases) == 0 || !validActionWitness(witness) || witness.Settled {
		return ErrRemoteExecution
	}
	raw, _ := AppendActionWitness(nil, witness)
	for _, lease := range leases {
		if lease == nil {
			return ErrRemoteExecution
		}
		current, present, err := lease.Load(RuntimeStateActionWitness, 0)
		if err != nil {
			return err
		}
		if present {
			prior, openErr := OpenActionWitness(current.Payload)
			if openErr != nil || prior.Operation != witness.Operation || prior.PlanDigest != witness.PlanDigest ||
				prior.CatalogGeneration > witness.CatalogGeneration || prior.Sequence > witness.Sequence ||
				prior.Sequence == witness.Sequence && !sameActionWitnessStep(prior, witness) ||
				prior.Sequence < witness.Sequence && !prior.Settled {
				return errors.Join(ErrRemoteExecution, openErr)
			}
			if prior.Sequence == witness.Sequence {
				continue
			}
		}
		if err = lease.Persist(RuntimeStateActionWitness, 0, current.Revision+1, raw); err != nil {
			return err
		}
	}
	return nil
}

func SettleActionWitness(leases []*RuntimeStoreLease, witness ActionWitness) error {
	if len(leases) == 0 || !validActionWitness(witness) {
		return ErrRemoteExecution
	}
	witness.Settled = true
	raw, _ := AppendActionWitness(nil, witness)
	for _, lease := range leases {
		current, present, err := lease.Load(RuntimeStateActionWitness, 0)
		if err != nil || !present {
			return errors.Join(ErrRemoteExecution, err)
		}
		prior, err := OpenActionWitness(current.Payload)
		if err != nil || !sameActionWitnessStep(prior, witness) {
			return errors.Join(ErrRemoteExecution, err)
		}
		if prior.Settled {
			continue
		}
		if err = lease.Persist(RuntimeStateActionWitness, 0, current.Revision+1, raw); err != nil {
			return err
		}
	}
	return nil
}

func sameActionWitnessStep(left, right ActionWitness) bool {
	return left.Operation == right.Operation && left.PlanDigest == right.PlanDigest &&
		left.CatalogGeneration == right.CatalogGeneration && left.CatalogDigest == right.CatalogDigest &&
		left.Sequence == right.Sequence && left.PredecessorDigest == right.PredecessorDigest &&
		left.Step == right.Step
}
