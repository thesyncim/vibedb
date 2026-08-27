package replicatedstate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"

	"github.com/thesyncim/vibedb/internal/replication"
	"github.com/thesyncim/vibedb/internal/splitcapture"
)

var splitCaptureBindingDomain = []byte("vibedb/replicated-state/split-capture-binding\x00")

func SplitCaptureBindingDigest(b Binding) [32]byte {
	h := sha256.New()
	_, _ = h.Write(splitCaptureBindingDomain)
	_, _ = h.Write(b.ClusterID[:])
	_, _ = h.Write(b.ClusterIncarnation[:])
	_, _ = h.Write(b.ShardIncarnation[:])
	_, _ = h.Write(b.GroupID[:])
	var fixed [8]byte
	for _, v := range []uint64{b.TopologyRecoveryEpoch, b.AllocationGeneration, b.ActivePolicyGeneration, b.ProtectionEpoch, b.OwnershipEpoch, b.SchemaGeneration, b.RoutingVersion, b.RouteGeneration} {
		binary.LittleEndian.PutUint64(fixed[:], v)
		_, _ = h.Write(fixed[:])
	}
	_, _ = h.Write([]byte(b.Distribution))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(b.Shard))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(b.OwnedRange.Start[:])
	_, _ = h.Write(b.OwnedRange.End.Point[:])
	if b.OwnedRange.End.Max {
		_, _ = h.Write([]byte{1})
	} else {
		_, _ = h.Write([]byte{0})
	}
	var out [32]byte
	_ = h.Sum(out[:0])
	return out
}

var ErrSplitCaptureActivation = errors.New("replicatedstate: invalid split capture activation")

var splitCaptureActivationKey = [...]byte{13}

// SplitCaptureActivation is the durable witness materialized at the committed
// entry. Command aliases persisted storage only during Open; callers receive a clone.
type SplitCaptureActivation struct {
	Applied uint64
	Command splitcapture.Command
}

func appendSplitCaptureActivation(dst []byte, applied uint64, raw []byte) ([]byte, error) {
	if applied == 0 {
		return dst, ErrSplitCaptureActivation
	}
	if _, err := splitcapture.OpenCommand(raw); err != nil {
		return dst, errors.Join(err, ErrSplitCaptureActivation)
	}
	dst = binary.LittleEndian.AppendUint64(dst, applied)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(raw)))
	dst = append(dst, raw...)
	return dst, nil
}

func openSplitCaptureActivation(raw []byte) (SplitCaptureActivation, error) {
	if len(raw) < 12 || int(binary.LittleEndian.Uint32(raw[8:12])) != len(raw)-12 {
		return SplitCaptureActivation{}, ErrSplitCaptureActivation
	}
	applied := binary.LittleEndian.Uint64(raw[:8])
	view, err := splitcapture.OpenCommand(raw[12:])
	if err != nil || applied == 0 || view.PriorApplied+1 != applied {
		return SplitCaptureActivation{}, errors.Join(err, ErrSplitCaptureActivation)
	}
	c := view.Command
	c.Spec = bytes.Clone(c.Spec)
	return SplitCaptureActivation{Applied: applied, Command: c}, nil
}

func (m *Machine) SplitCaptureActivation() (SplitCaptureActivation, bool) {
	if m == nil {
		return SplitCaptureActivation{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.splitCaptureActivation == nil {
		return SplitCaptureActivation{}, false
	}
	a := *m.splitCaptureActivation
	a.Command.Spec = bytes.Clone(a.Command.Spec)
	return a, true
}

func (m *Machine) planSplitCaptureActivation(plan commandPlan, command replication.CommandView, applied uint64, state State, snapshot pointSnapshot) (commandPlan, error) {
	nested, err := command.OpenSplitCaptureActivation()
	if err != nil {
		return commandPlan{}, err
	}
	c := nested.Command
	raw, found, err := snapshot.appendRaw(nil, splitCaptureActivationKey[:])
	if err != nil {
		return commandPlan{}, err
	}
	if found {
		existing, openErr := openSplitCaptureActivation(raw)
		if openErr != nil {
			return commandPlan{}, openErr
		}
		if existing.Command.Operation != c.Operation || existing.Command.PlanDigest != c.PlanDigest || existing.Command.PartitionerDigest != c.PartitionerDigest || existing.Command.RelationManifestDigest != c.RelationManifestDigest || existing.Command.LineageDigest != c.LineageDigest || !bytes.Equal(existing.Command.Spec, c.Spec) {
			plan.conflict = true
			return plan, nil
		}
		plan.resultCode = ResultApplied
		return plan, nil
	}
	if c.PriorApplied != state.Applied || c.PriorTerm != state.LastTerm || c.PriorEntryDigest != state.LastEntryDigest || c.PriorDataChainDigest != state.DataChainDigest || c.SourceGeneration != state.Binding.RouteGeneration || c.SchemaGeneration != state.Binding.SchemaGeneration || c.BindingDigest != SplitCaptureBindingDigest(state.Binding) {
		return commandPlan{}, ErrSplitCaptureActivation
	}
	value, err := appendSplitCaptureActivation(nil, applied, command.SplitCaptureActivationBytes())
	if err != nil {
		return commandPlan{}, err
	}
	plan.systemRows = append(plan.systemRows, transactionRowMutation{key: splitCaptureActivationKey[:], value: value})
	plan.resultCode = ResultApplied
	return plan, nil
}

func (m *Machine) activateAppliedSplitCapture(command replication.CommandView, applied uint64) error {
	if m.options.TransitionCaptureFactory == nil || m.capture != nil {
		return ErrSplitCaptureActivation
	}
	view, err := command.OpenSplitCaptureActivation()
	if err != nil {
		return err
	}
	c := view.Command
	c.Spec = bytes.Clone(c.Spec)
	a := SplitCaptureActivation{Applied: applied, Command: c}
	capture, err := m.options.TransitionCaptureFactory(a)
	if err != nil || capture == nil {
		return errors.Join(err, ErrSplitCaptureActivation)
	}
	m.splitCaptureActivation = &a
	return m.beginTransitionCapture(capture)
}
