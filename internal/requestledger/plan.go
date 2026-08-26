package requestledger

import (
	"encoding/binary"
	"hash/crc32"
)

const planHeaderBytes = 24

var planMagic = [4]byte{'V', 'R', 'P', 'L'}

// PlanStreamFramer is the sole incremental wrapper grammar for large recipe
// emitters. It observes arbitrary recipe fragments without retaining them and
// emits the exact header/trailer accepted by OpenPlan.
type PlanStreamFramer struct {
	recipeBytes uint64
	written     uint64
	crc         uint32
	header      bool
}

func NewPlanStreamFramer(recipeBytes uint64) (PlanStreamFramer, error) {
	if recipeBytes == 0 || recipeBytes > MaxPlanBytes-planHeaderBytes-checksumBytes {
		return PlanStreamFramer{}, ErrTooLarge
	}
	return PlanStreamFramer{recipeBytes: recipeBytes}, nil
}

func (framer *PlanStreamFramer) AppendHeader(dst []byte) ([]byte, error) {
	if framer.header || framer.written != 0 {
		return dst, ErrInvalidState
	}
	start := len(dst)
	dst = append(dst, make([]byte, planHeaderBytes)...)
	copy(dst[start:start+4], planMagic[:])
	binary.LittleEndian.PutUint64(dst[start+8:start+16], framer.recipeBytes+planHeaderBytes+checksumBytes)
	binary.LittleEndian.PutUint64(dst[start+16:start+24], framer.recipeBytes)
	framer.crc = crc32.Update(0, castagnoli, dst[start:])
	framer.header = true
	return dst, nil
}

func (framer *PlanStreamFramer) ObserveRecipe(fragment []byte) error {
	if !framer.header || framer.written > framer.recipeBytes ||
		uint64(len(fragment)) > framer.recipeBytes-framer.written {
		return ErrInvalidState
	}
	framer.crc = crc32.Update(framer.crc, castagnoli, fragment)
	framer.written += uint64(len(fragment))
	return nil
}

func (framer *PlanStreamFramer) AppendTrailer(dst []byte) ([]byte, error) {
	if !framer.header || framer.written != framer.recipeBytes {
		return dst, ErrIncomplete
	}
	return binary.LittleEndian.AppendUint32(dst, framer.crc), nil
}

func (framer PlanStreamFramer) TotalBytes() uint64 {
	return framer.recipeBytes + planHeaderBytes + checksumBytes
}

// PlanView is the opaque, canonical protocol recipe owned by the gateway. The
// ledger authenticates and pages it but does not interpret transaction branch,
// wave, route, or mutation semantics.
type PlanView struct {
	raw    []byte
	recipe []byte
}

func (view PlanView) Bytes() []byte  { return view.raw[:len(view.raw):len(view.raw)] }
func (view PlanView) Recipe() []byte { return view.recipe[:len(view.recipe):len(view.recipe)] }

// AppendPlan wraps one already-canonical protocol recipe. Large callers stream
// the same bytes through page records without creating a second aggregate copy.
func AppendPlan(dst, recipe []byte) ([]byte, error) {
	framer, err := NewPlanStreamFramer(uint64(len(recipe)))
	if err != nil {
		return dst, err
	}
	dst, err = framer.AppendHeader(dst)
	if err != nil {
		return dst, err
	}
	dst = append(dst, recipe...)
	if err = framer.ObserveRecipe(recipe); err != nil {
		return dst, err
	}
	return framer.AppendTrailer(dst)
}

func OpenPlan(raw []byte) (PlanView, error) {
	if len(raw) < planHeaderBytes+1+checksumBytes || len(raw) > MaxPlanBytes ||
		!magicOK(raw, planMagic) || !zeroBytes(raw[4:8]) || !checksumOK(raw) {
		return PlanView{}, ErrCorrupt
	}
	recipeBytes := binary.LittleEndian.Uint64(raw[16:24])
	if binary.LittleEndian.Uint64(raw[8:16]) != uint64(len(raw)) || recipeBytes == 0 ||
		recipeBytes != uint64(len(raw)-planHeaderBytes-checksumBytes) {
		return PlanView{}, ErrCorrupt
	}
	return PlanView{
		raw:    raw[:len(raw):len(raw)],
		recipe: raw[planHeaderBytes : len(raw)-checksumBytes : len(raw)-checksumBytes],
	}, nil
}

// PlanRoot computes the same canonical page-chain root used by paged ingestion.
func PlanRoot(key Digest, raw []byte) (Digest, error) {
	if _, err := OpenPlan(raw); err != nil {
		return Digest{}, err
	}
	if !nonzeroDigest(key) {
		return Digest{}, ErrCorrupt
	}
	count := uint64((len(raw) + MaxPlanPageBytes - 1) / MaxPlanPageBytes)
	var chain Digest
	for ordinal, offset := uint64(0), 0; offset < len(raw); ordinal++ {
		end := min(offset+MaxPlanPageBytes, len(raw))
		chain = PlanPageChain(key, ordinal, count, uint64(offset), uint64(len(raw)), chain, raw[offset:end])
		offset = end
	}
	return chain, nil
}

// PlanRootAccumulator computes the identical root from exact 512 KiB chunks.
// It is the first pass for a large recipe; Resetting the caller's source and
// feeding the same chunks to NewPlanPageData is the allocation-free second pass.
type PlanRootAccumulator struct {
	key     Digest
	total   uint64
	count   uint64
	offset  uint64
	ordinal uint64
	chain   Digest
	failed  bool
}

func NewPlanRootAccumulator(key Digest, total uint64) (PlanRootAccumulator, error) {
	if !nonzeroDigest(key) || total <= MaxInlinePlanBytes || total > MaxPlanBytes {
		return PlanRootAccumulator{}, ErrCorrupt
	}
	return PlanRootAccumulator{
		key: key, total: total,
		count: (total + MaxPlanPageBytes - 1) / MaxPlanPageBytes,
	}, nil
}

func (accumulator *PlanRootAccumulator) Append(data []byte) error {
	if accumulator.failed || accumulator.offset >= accumulator.total {
		return ErrInvalidState
	}
	want := min(uint64(MaxPlanPageBytes), accumulator.total-accumulator.offset)
	if uint64(len(data)) != want {
		accumulator.failed = true
		return ErrCorrupt
	}
	accumulator.chain = PlanPageChain(
		accumulator.key, accumulator.ordinal, accumulator.count,
		accumulator.offset, accumulator.total, accumulator.chain, data,
	)
	accumulator.ordinal++
	accumulator.offset += uint64(len(data))
	return nil
}

func (accumulator *PlanRootAccumulator) Root() (Digest, error) {
	if accumulator.failed || accumulator.offset != accumulator.total ||
		accumulator.ordinal != accumulator.count || !nonzeroDigest(accumulator.chain) {
		return Digest{}, ErrIncomplete
	}
	return accumulator.chain, nil
}
