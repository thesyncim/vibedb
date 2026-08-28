package requestledger

import "encoding/binary"

const pageBatchHeaderBytes = 24

var pageBatchMagic = [4]byte{'V', 'R', 'L', 'B'}

type PlanPageBatchView struct {
	raw   []byte
	count uint64
}

func (view PlanPageBatchView) Bytes() []byte { return view.raw[:len(view.raw):len(view.raw)] }
func (view PlanPageBatchView) Count() uint64 { return view.count }
func (view PlanPageBatchView) Iter() PlanPageBatchIter {
	return PlanPageBatchIter{raw: view.raw, at: pageBatchHeaderBytes}
}

type PlanPageBatchIter struct {
	raw     []byte
	at      int
	ordinal uint64
}

func (iter *PlanPageBatchIter) Next() (PlanPageRecord, uint64, bool) {
	if iter.at == len(iter.raw)-checksumBytes {
		return PlanPageRecord{}, 0, false
	}
	length := int(binary.LittleEndian.Uint32(iter.raw[iter.at : iter.at+4]))
	iter.at += 4
	page, err := OpenPlanPage(iter.raw[iter.at : iter.at+length])
	if err != nil {
		return PlanPageRecord{}, 0, false
	}
	iter.at += length
	ordinal := iter.ordinal
	iter.ordinal++
	return page, ordinal, true
}

func AppendPlanPageBatch(dst []byte, pages []PlanPageRecord) ([]byte, error) {
	if len(pages) == 0 {
		return dst, ErrCorrupt
	}
	start := len(dst)
	dst = append(dst, make([]byte, pageBatchHeaderBytes)...)
	out := dst[start:]
	copy(out[:4], pageBatchMagic[:])
	binary.LittleEndian.PutUint64(out[8:16], uint64(len(pages)))
	var previous PlanPageRecord
	for i := range pages {
		page := pages[i]
		if err := validatePlanPage(page); err != nil || i != 0 &&
			(page.KeyDigest != previous.KeyDigest || page.PlanRoot != previous.PlanRoot ||
				page.PlanBuildID != previous.PlanBuildID ||
				page.Ordinal != previous.Ordinal+1 || page.Offset != previous.Offset+uint64(len(previous.Data)) ||
				page.PreviousChain != previous.Chain) {
			return dst[:start], ErrCorrupt
		}
		lengthAt := len(dst)
		dst = append(dst, 0, 0, 0, 0)
		before := len(dst)
		var err error
		dst, err = AppendPlanPage(dst, page)
		if err != nil {
			return dst[:start], err
		}
		binary.LittleEndian.PutUint32(dst[lengthAt:lengthAt+4], uint32(len(dst)-before))
		if len(dst)-start+checksumBytes > MaxCommandBytes {
			return dst[:start], ErrTooLarge
		}
		previous = page
	}
	binary.LittleEndian.PutUint64(dst[start+16:start+24], uint64(len(dst)-start+checksumBytes))
	dst = appendChecksum(dst, start)
	return dst, nil
}

func OpenPlanPageBatch(raw []byte) (PlanPageBatchView, error) {
	if len(raw) < pageBatchHeaderBytes+4+pageHeaderBytes+1+2*checksumBytes ||
		len(raw) > MaxCommandBytes || !magicOK(raw, pageBatchMagic) ||
		!zeroBytes(raw[4:8]) || !checksumOK(raw) ||
		binary.LittleEndian.Uint64(raw[16:24]) != uint64(len(raw)) {
		return PlanPageBatchView{}, ErrCorrupt
	}
	count := binary.LittleEndian.Uint64(raw[8:16])
	if count == 0 || count > uint64((len(raw)-pageBatchHeaderBytes-checksumBytes)/
		(4+pageHeaderBytes+1+checksumBytes)) {
		return PlanPageBatchView{}, ErrCorrupt
	}
	at := pageBatchHeaderBytes
	end := len(raw) - checksumBytes
	var previous PlanPageRecord
	for i := uint64(0); i < count; i++ {
		if at > end-4 {
			return PlanPageBatchView{}, ErrCorrupt
		}
		length := uint64(binary.LittleEndian.Uint32(raw[at : at+4]))
		at += 4
		if length > uint64(end-at) {
			return PlanPageBatchView{}, ErrCorrupt
		}
		page, err := OpenPlanPage(raw[at : at+int(length)])
		if err != nil || i != 0 &&
			(page.KeyDigest != previous.KeyDigest || page.PlanRoot != previous.PlanRoot ||
				page.PlanBuildID != previous.PlanBuildID ||
				page.Ordinal != previous.Ordinal+1 || page.Offset != previous.Offset+uint64(len(previous.Data)) ||
				page.PreviousChain != previous.Chain) {
			return PlanPageBatchView{}, ErrCorrupt
		}
		at += int(length)
		previous = page
	}
	if at != end {
		return PlanPageBatchView{}, ErrCorrupt
	}
	return PlanPageBatchView{raw: raw[:len(raw):len(raw)], count: count}, nil
}
