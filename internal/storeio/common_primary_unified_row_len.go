package storeio

// AdmittedRowBodyLen returns the exact canonical document length encoded by an
// inline row body borrowed from this admitted unified leaf. It is the
// length-only counterpart of AppendAdmittedRowBody: PageCache admission has
// already proved the template id, token stream, dictionary references, literal
// lengths, and varints, so a mutation that only needs an envelope delta need
// not render and copy the old document into scratch.
//
// Overflow descriptors are not row bodies and must stay on their existing
// structural path. Like AppendAdmittedRowBody, callers must pass a non-empty
// inline body borrowed from this view while its page lease is live.
func (v *CommonPrimaryUnifiedLeafView) AdmittedRowBodyLen(body []byte) int {
	if body[0] == unifiedRowTrivial {
		return len(body) - 1
	}
	entry := v.admittedTemplateEntry(int(body[0]))
	rendered := len(entry.static)
	cursor := 1
	for range entry.holes {
		tag := body[cursor]
		cursor++
		switch {
		case tag < unifiedTokenDictLimit:
			rendered += len(v.admittedDictionaryEntry(int(tag)))
		case tag >= unifiedTokenShortBase &&
			tag < unifiedTokenShortBase+unifiedTokenShortMax:
			length := int(tag-unifiedTokenShortBase) + 1
			cursor += length
			rendered += length
		case tag == unifiedTokenLongLiteral:
			length, n, _ := readUnifiedTokenUvarint(body[cursor:])
			cursor += n + int(length)
			rendered += int(length)
		case tag == unifiedTokenTrue, tag == unifiedTokenNull:
			rendered += 4
		case tag == unifiedTokenFalse:
			rendered += 5
		case tag == unifiedTokenInt:
			value, n := DecodeZigzagVarint(body[cursor:])
			cursor += n
			rendered += canonicalIntRenderedLen(value)
		}
	}
	return rendered
}

// canonicalIntRenderedLen is the byte count of AppendCanonicalInt without
// formatting into temporary storage. Although unified admission currently caps
// integers at 18 digits, handling the complete int64 domain keeps this helper
// equivalent to the renderer by construction.
func canonicalIntRenderedLen(value int64) int {
	sign := 0
	var magnitude uint64
	if value < 0 {
		sign = 1
		// This form also handles MinInt64 without signed overflow.
		magnitude = uint64(-(value + 1)) + 1
	} else {
		magnitude = uint64(value)
	}
	digits := 1
	for magnitude >= 10 {
		magnitude /= 10
		digits++
	}
	return sign + digits
}
