package storeio

import "fmt"

// PrimaryGraphBuildPage is one page borrowed from an incremental graph sink.
// Builders must Stage it before requesting another page.
type PrimaryGraphBuildPage interface {
	Bytes() []byte
	Ref() PageRef
	Stage() error
}

// PrimaryGraphBuildSink is the allocation surface shared by creation and
// reserved-generation builders.
type PrimaryGraphBuildSink interface {
	AllocatePage(kind PageKind, length uint32, logicalID uint64) (PrimaryGraphBuildPage, error)
	StoreIdentity() [16]byte
	BuildGeneration() uint64
	BuildFileEnd() uint64
	BuildNextLogicalID() uint64
	MaxBuildPageBytes() int
}

type transactionPrimaryGraphSink struct{ tx *WriteTransaction }

// AllocatePage and the Build* accessors let a live transaction participate in
// the same sink abstraction directly. They are deliberately thin aliases over
// Allocate and its immutable transaction options.
func (t *WriteTransaction) AllocatePage(
	kind PageKind, length uint32, logicalID uint64,
) (PrimaryGraphBuildPage, error) {
	p, err := t.Allocate(kind, length, logicalID)
	if err != nil {
		return nil, err
	}
	return &p, nil
}
func (t *WriteTransaction) StoreIdentity() [16]byte    { return t.StoreID() }
func (t *WriteTransaction) BuildGeneration() uint64    { return t.Generation() }
func (t *WriteTransaction) BuildFileEnd() uint64       { return t.FileEnd() }
func (t *WriteTransaction) BuildNextLogicalID() uint64 { return t.NextLogicalID() }
func (t *WriteTransaction) MaxBuildPageBytes() int {
	if t == nil || t.committer == nil {
		return 0
	}
	return t.committer.bufferSize
}

// NewTransactionPrimaryGraphSink exposes the same allocation sink used by the
// primary builder to sibling schema/index/overflow builders. The adapter owns
// nothing and remains valid only while tx is active.
func NewTransactionPrimaryGraphSink(tx *WriteTransaction) (PrimaryGraphBuildSink, error) {
	if tx == nil || !tx.active || tx.batch == nil {
		return nil, ErrBatchState
	}
	return transactionPrimaryGraphSink{tx: tx}, nil
}

func (s transactionPrimaryGraphSink) AllocatePage(kind PageKind, length uint32, logicalID uint64) (PrimaryGraphBuildPage, error) {
	p, err := s.tx.Allocate(kind, length, logicalID)
	if err != nil {
		return nil, err
	}
	return &p, nil
}
func (s transactionPrimaryGraphSink) StoreIdentity() [16]byte    { return s.tx.options.StoreID }
func (s transactionPrimaryGraphSink) BuildGeneration() uint64    { return s.tx.options.Generation }
func (s transactionPrimaryGraphSink) BuildFileEnd() uint64       { return s.tx.fileEnd }
func (s transactionPrimaryGraphSink) BuildNextLogicalID() uint64 { return s.tx.nextID }
func (s transactionPrimaryGraphSink) MaxBuildPageBytes() int     { return s.tx.committer.bufferSize }

type UnrootedPrimaryGraphSink struct {
	writer                                  *UnrootedGenerationWriter
	storeID                                 [16]byte
	generation, nextLogicalID, finalFileEnd uint64
	scratch                                 []byte
	scratchUsed                             int
	pendingBytes                            uint64
}

func NewUnrootedPrimaryGraphSink(writer *UnrootedGenerationWriter, storeID [16]byte, generation, nextLogicalID, finalFileEnd uint64, scratch []byte) (*UnrootedPrimaryGraphSink, error) {
	if writer == nil || storeID == ([16]byte{}) || generation == 0 || nextLogicalID == 0 || finalFileEnd == 0 || len(scratch) < CommonPrimaryLeafMaxExtentBytes {
		return nil, fmt.Errorf("%w: unrooted primary graph sink", ErrInvalidWrite)
	}
	return &UnrootedPrimaryGraphSink{writer: writer, storeID: storeID, generation: generation, nextLogicalID: nextLogicalID, finalFileEnd: finalFileEnd, scratch: scratch}, nil
}

type unrootedPrimaryGraphPage struct {
	owner *UnrootedPrimaryGraphSink
	ref   PageRef
	image []byte
}

func (p *unrootedPrimaryGraphPage) Bytes() []byte { return p.image }
func (p *unrootedPrimaryGraphPage) Ref() PageRef  { return p.ref }
func (p *unrootedPrimaryGraphPage) Stage() error {
	if p == nil || p.owner == nil || p.ref.Offset != p.owner.writer.reservation.Offset+p.owner.writer.written {
		return ErrBatchState
	}
	err := p.owner.writer.Append(p.ref, p.image)
	if err == nil && p.owner.writer.written == p.owner.pendingBytes {
		p.owner.scratchUsed = 0
	}
	return err
}

func (s *UnrootedPrimaryGraphSink) AllocatePage(kind PageKind, length uint32, logicalID uint64) (PrimaryGraphBuildPage, error) {
	if s == nil || length == 0 || int(length) > len(s.scratch)-s.scratchUsed {
		return nil, ErrBatchState
	}
	if logicalID == 0 {
		logicalID = s.nextLogicalID
		s.nextLogicalID++
	}
	ref := PageRef{Offset: s.writer.reservation.Offset + s.pendingBytes, LogicalID: logicalID, Generation: s.generation, Length: length, Kind: kind}
	if uint64(length) > s.writer.reservation.Length-s.pendingBytes {
		return nil, ErrTooManyPages
	}
	image := s.scratch[s.scratchUsed : s.scratchUsed+int(length)]
	clear(image)
	s.scratchUsed += int(length)
	s.pendingBytes += uint64(length)
	return &unrootedPrimaryGraphPage{owner: s, ref: ref, image: image}, nil
}
func (s *UnrootedPrimaryGraphSink) StoreIdentity() [16]byte    { return s.storeID }
func (s *UnrootedPrimaryGraphSink) BuildGeneration() uint64    { return s.generation }
func (s *UnrootedPrimaryGraphSink) BuildFileEnd() uint64       { return s.finalFileEnd }
func (s *UnrootedPrimaryGraphSink) BuildNextLogicalID() uint64 { return s.nextLogicalID }
func (s *UnrootedPrimaryGraphSink) MaxBuildPageBytes() int     { return len(s.scratch) }
