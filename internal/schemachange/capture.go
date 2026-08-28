package schemachange

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"
	"sync"

	"github.com/thesyncim/vibedb/internal/replicatedstate"
)

// Cursor binds replay to both the exact publication and its capture-chain root.
type Cursor struct {
	Publication Publication
	Digest      [32]byte
}

type CaptureDescriptor struct {
	Config         CaptureConfig
	Base, Head     Cursor
	Records, Bytes uint64
	Abort          AbortReason
	// SealDigest is nonzero only after an exact-cut finish. It binds the full
	// capture chain, but does not replace the target's data/cutover certificate.
	SealDigest [32]byte
}

// SourceCapture has bounded retained storage and no background goroutine. The
// machine owns all publication. On exhaustion it atomically records an abort
// alongside the source write and then leaves the ordinary write path entirely.
// The coordinator must discard an aborted target, never certify its partial
// stream. Activation and eventual bounded reclamation belong to the coordinator.
type SourceCapture struct {
	mu           sync.Mutex
	target       replicatedstate.TransitionCaptureTarget
	config       CaptureConfig
	descriptor   CaptureDescriptor
	begun        bool
	pending      Cursor
	pendingBytes uint64
	pendingAbort AbortReason
	workspace    CaptureWorkspace
}

func NewSourceCapture(config CaptureConfig, target replicatedstate.TransitionCaptureTarget) (*SourceCapture, error) {
	if !config.valid() || target.Name == "" || target.Collection == nil || !target.Collection.HasOpaqueValues() ||
		target.Collection.MaxDocumentBytes() < headerBytes || target.Collection.MaxKeyBytes() < 8 {
		return nil, ErrCapture
	}
	return &SourceCapture{config: config, target: target}, nil
}

func (c *SourceCapture) Target() replicatedstate.TransitionCaptureTarget { return c.target }
func (*SourceCapture) CaptureAllRelations() bool                         { return true }
func (*SourceCapture) MaxEncodedBytes(b replicatedstate.TransitionCaptureBounds) (int, error) {
	return recordBytes(b)
}

func (c *SourceCapture) CaptureStopped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.begun && (c.descriptor.Abort != NotAborted || c.descriptor.SealDigest != [32]byte{})
}

func (c *SourceCapture) Descriptor() (CaptureDescriptor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.begun {
		return CaptureDescriptor{}, ErrCapture
	}
	return c.descriptor, nil
}

func (c *SourceCapture) Begin(state replicatedstate.State, publish func(key, value []byte) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.begun {
		return ErrCapture
	}
	if c.target.Collection.Len() == 0 {
		if publish == nil || state.Binding.SchemaGeneration != c.config.SchemaGeneration ||
			replicatedstate.SplitCaptureBindingDigest(state.Binding) != c.config.BindingDigest {
			return ErrCapture
		}
		base := statePublication(state)
		raw, err := appendHeader(c.workspace.raw[:0], c.config, base)
		if err != nil {
			return err
		}
		var key [8]byte
		if err := publish(key[:], raw); err != nil {
			return err
		}
		c.workspace.raw = raw
		cursor := Cursor{base, [32]byte(raw[len(raw)-32:])}
		c.descriptor = CaptureDescriptor{Config: c.config, Base: cursor, Head: cursor, Bytes: headerBytes}
	} else if err := c.recover(state); err != nil {
		return err
	}
	// Recovery scratch is not steady-state memory. Readers own their own
	// workspaces, and the machine supplies the append buffer.
	c.workspace = CaptureWorkspace{}
	c.begun = true
	return nil
}

func (c *SourceCapture) recover(state replicatedstate.State) (resultErr error) {
	snapshot, err := c.target.Collection.Snapshot()
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, snapshot.Close()) }()
	var descriptor CaptureDescriptor
	seenHeader := false
	err = snapshot.RangeRaw(func(key, raw []byte) error {
		if len(key) != 8 {
			return ErrCapture
		}
		applied := binary.BigEndian.Uint64(key)
		if !seenHeader {
			if applied != 0 {
				return ErrCapture
			}
			config, publication, digest, err := openHeader(raw)
			if err != nil || config != c.config {
				return ErrCapture
			}
			cursor := Cursor{publication, digest}
			descriptor = CaptureDescriptor{Config: config, Base: cursor, Head: cursor, Bytes: uint64(len(raw))}
			seenHeader = true
			return nil
		}
		if descriptor.Abort != NotAborted || descriptor.SealDigest != [32]byte{} || descriptor.Records >= c.config.MaxRecords ||
			uint64(len(raw)) > c.config.MaxBytes-descriptor.Bytes {
			return ErrCapture
		}
		if applied == math.MaxUint64 {
			digest, err := openSeal(raw, descriptor.Head)
			if err != nil {
				return err
			}
			descriptor.SealDigest = digest
			descriptor.Records++
			descriptor.Bytes += uint64(len(raw))
			return nil
		}
		entry, err := openEntry(raw, &c.workspace)
		if err != nil || applied != entry.After.Applied || entry.Before != descriptor.Head.Publication || entry.PreviousDigest != descriptor.Head.Digest {
			return ErrCapture
		}
		descriptor.Head = Cursor{entry.After, entry.Digest}
		descriptor.Abort = entry.Abort
		descriptor.Records++
		descriptor.Bytes += uint64(len(raw))
		return nil
	})
	c.workspace.clearMutations()
	if err != nil || !seenHeader || descriptor.Head.Publication.Applied > state.Applied {
		return errors.Join(err, ErrCapture)
	}
	if descriptor.Abort == NotAborted && descriptor.SealDigest == [32]byte{} && (descriptor.Head.Publication != statePublication(state) ||
		state.Binding.SchemaGeneration != c.config.SchemaGeneration || replicatedstate.SplitCaptureBindingDigest(state.Binding) != c.config.BindingDigest ||
		descriptor.Records >= c.config.MaxRecords || descriptor.Bytes > c.config.MaxBytes-entryBytes) {
		return ErrCapture
	}
	c.descriptor = descriptor
	return nil
}

func (c *SourceCapture) AppendTransition(dst []byte, t replicatedstate.CapturedTransition) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.begun || c.pending != (Cursor{}) || c.descriptor.Abort != NotAborted || c.descriptor.SealDigest != [32]byte{} {
		return dst, ErrCapture
	}
	before := c.descriptor.Head.Publication
	after := Publication{t.Applied, t.Term, t.AfterOwnershipEpoch, t.AfterRoutingVersion, t.AfterRouteGeneration, t.EntryDigest, t.AfterDataChainDigest}
	if !after.valid() || t.Applied != before.Applied+1 || t.Term < before.Term || t.BeforeOwnershipEpoch != before.Ownership ||
		t.BeforeRoutingVersion != before.Routing || t.BeforeRouteGeneration != before.Route ||
		t.PreviousEntryDigest != before.EntryDigest || t.BeforeDataChainDigest != before.DataDigest ||
		t.BeforeSchemaGeneration != c.config.SchemaGeneration || t.AfterSchemaGeneration == 0 {
		return dst, ErrCapture
	}
	size, err := recordBytes(t.Bounds())
	if err != nil {
		return dst, err
	}
	abort := NotAborted
	if after.Ownership != before.Ownership || after.Routing != before.Routing || after.Route != before.Route || t.AfterSchemaGeneration != c.config.SchemaGeneration {
		abort = AbortSourceChanged
	} else if c.descriptor.Records+1 >= c.config.MaxRecords || uint64(size) > c.config.MaxBytes-c.descriptor.Bytes-entryBytes {
		abort = AbortCapacity
	}
	count := t.MutationCount()
	if abort != NotAborted {
		size, count = entryBytes, 0
	}
	start := len(dst)
	dst = append(dst, make([]byte, size)...)
	raw := dst[start:]
	putEnvelope(raw, 2, abort)
	binary.LittleEndian.PutUint32(raw[16:20], uint32(count))
	putPublication(raw[24:128], before)
	putPublication(raw[128:232], after)
	copy(raw[232:264], c.descriptor.Head.Digest[:])
	offset := 264
	for i := 0; i < count; i++ {
		m := t.Mutation(i)
		frame := raw[offset : offset+mutationBytes]
		binary.LittleEndian.PutUint16(frame, uint16(m.Relation))
		binary.LittleEndian.PutUint32(frame[4:8], uint32(len(m.Key)))
		if m.Before != nil {
			frame[2] |= 1
			binary.LittleEndian.PutUint32(frame[8:12], uint32(len(m.Before)))
			digest := sha256.Sum256(m.Before)
			copy(frame[16:48], digest[:])
		}
		if m.After != nil {
			frame[2] |= 2
		}
		binary.LittleEndian.PutUint32(frame[12:16], uint32(len(m.After)))
		offset += mutationBytes
		offset += copy(raw[offset:], m.Key)
		offset += copy(raw[offset:], m.After)
	}
	if offset != len(raw)-32 {
		return dst[:start], ErrCapture
	}
	digest := recordDigest(raw[:offset])
	copy(raw[offset:], digest[:])
	c.pending, c.pendingBytes, c.pendingAbort = Cursor{after, digest}, uint64(size), abort
	return dst, nil
}

func (c *SourceCapture) Published(t replicatedstate.CapturedTransition) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	want := Publication{t.Applied, t.Term, t.AfterOwnershipEpoch, t.AfterRoutingVersion, t.AfterRouteGeneration, t.EntryDigest, t.AfterDataChainDigest}
	if !c.begun || c.pending == (Cursor{}) || c.pending.Publication != want {
		return ErrCapture
	}
	c.descriptor.Head = c.pending
	c.descriptor.Bytes += c.pendingBytes
	c.descriptor.Records++
	c.descriptor.Abort = c.pendingAbort
	c.pending, c.pendingBytes, c.pendingAbort = Cursor{}, 0, NotAborted
	return nil
}

// Finish is called only by Machine.FinishTransitionCapture. The high sentinel
// key holds a bounded closure record without inventing a Raft publication.
// Writes after closure are not captured: activation must compare its source
// index and fail if a write overtook the coordinator's cutover fence.
func (c *SourceCapture) Finish(state replicatedstate.State, publish func(key, value []byte) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.begun || publish == nil || c.pending != (Cursor{}) || c.descriptor.Abort != NotAborted ||
		c.descriptor.SealDigest != [32]byte{} || statePublication(state) != c.descriptor.Head.Publication ||
		state.Binding.SchemaGeneration != c.config.SchemaGeneration || replicatedstate.SplitCaptureBindingDigest(state.Binding) != c.config.BindingDigest ||
		c.descriptor.Records >= c.config.MaxRecords || c.descriptor.Bytes > c.config.MaxBytes-entryBytes {
		return ErrCapture
	}
	raw, err := appendSeal(nil, c.descriptor.Head)
	if err != nil {
		return err
	}
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], math.MaxUint64)
	if err := publish(key[:], raw); err != nil {
		return err
	}
	c.descriptor.SealDigest = [32]byte(raw[len(raw)-32:])
	c.descriptor.Bytes += uint64(len(raw))
	c.descriptor.Records++
	return nil
}

// Next returns one committed entry after cursor. Missing or substituted cursors
// fail closed. A caller must reject Abort before using a target image.
func (c *SourceCapture) Next(cursor Cursor, w *CaptureWorkspace) (Entry, bool, error) {
	if w == nil {
		return Entry{}, false, ErrCapture
	}
	c.mu.Lock()
	d := c.descriptor
	begun := c.begun
	c.mu.Unlock()
	// Read immutable committed rows without holding the capture publication
	// lock. A slow reader must not stall the next foreground write.
	w.clearMutations()
	if !begun || cursor.Publication.Applied < d.Base.Publication.Applied || cursor.Publication.Applied > d.Head.Publication.Applied ||
		cursor.Publication.Applied == d.Base.Publication.Applied && cursor != d.Base {
		return Entry{}, false, ErrCapture
	}
	if cursor.Publication.Applied == d.Head.Publication.Applied {
		if cursor != d.Head {
			return Entry{}, false, ErrCapture
		}
		return Entry{}, false, nil
	}
	var key [8]byte
	binary.BigEndian.PutUint64(key[:], cursor.Publication.Applied+1)
	raw, found, err := c.target.Collection.AppendRaw(w.raw[:0], key[:])
	w.raw = raw
	if err != nil || !found {
		return Entry{}, false, errors.Join(err, ErrCapture)
	}
	entry, err := openEntry(raw, w)
	if err != nil || entry.Before != cursor.Publication || entry.PreviousDigest != cursor.Digest {
		w.clearMutations()
		return Entry{}, false, errors.Join(err, ErrCapture)
	}
	return entry, true, nil
}
