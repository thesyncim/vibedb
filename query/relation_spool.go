package query

import (
	"errors"
	"fmt"
	"math"
	"unsafe"

	"github.com/thesyncim/vibejson/x/byteview"
)

var errRelationSpoolSizing = errors.New(
	"query: relation spool publication exceeded its sizing pass",
)

// relationSpool is the statement-owned, columnar ownership boundary between a
// relation-valued child and its consumer. Columns are addressed by ordinal,
// never by display name, so duplicate SQL output names remain distinct. Each
// scalar owns every variable-width byte through data; a child Result and all
// of its borrowed workspaces may therefore be reset as soon as publication is
// complete.
//
// The zero value is ready to use. Reset keeps the warmed high-water storage,
// while release drops it. A spool is deliberately not a public Source: its
// lifetime is tied to the single-consumer Statement that owns it, which is the
// same ownership model future shared and reference-local CTE runtimes need.
type relationSpool struct {
	columns     [][]scalar
	data        []byte
	rows        int
	plannedData int
	joinStats   relationSpoolJoinStats
}

type relationSpoolJoinStats struct {
	builds    int
	buildRows uint64
	pairs     uint64
}

func (s *relationSpool) reset() {
	if s == nil {
		return
	}
	for i := range s.columns {
		clear(s.columns[i])
		s.columns[i] = s.columns[i][:0]
	}
	s.data = s.data[:0]
	s.rows = 0
	s.plannedData = 0
	s.joinStats = relationSpoolJoinStats{}
}

func (s *relationSpool) release() {
	if s == nil {
		return
	}
	for i := range s.columns {
		clear(s.columns[i])
	}
	*s = relationSpool{}
}

// materialize performs a read-only sizing pass, reserves the statement-wide
// intermediate account, and only then grows storage. The second pass publishes
// fully owned columns. Any error leaves the spool logically empty and releases
// the complete reservation.
func (s *relationSpool) materialize(
	cursor Cursor,
	columns int,
	frame *statementFrame,
	cancel *CancelFlag,
	resource string,
) (int64, error) {
	s.reset()
	rows, payload, err := measureRelationSpool(cursor, columns, cancel)
	if err != nil {
		return 0, err
	}
	charge := relationSpoolRetainedBytes(rows, columns, payload)
	if charge == math.MaxInt64 {
		return 0, &IntermediateBudgetError{
			Resource: resource,
			Bytes:    math.MaxInt64,
			Limit:    frame.intermediate.limit,
		}
	}
	if err := frame.intermediate.reserve(resource, charge); err != nil {
		return 0, err
	}
	if err := cancellationError(cancel); err != nil {
		frame.intermediate.release(charge)
		return 0, err
	}
	if err := s.begin(rows, columns, payload); err != nil {
		frame.intermediate.release(charge)
		return 0, err
	}
	if err := s.fill(cursor, columns, rows, cancel); err != nil {
		s.reset()
		frame.intermediate.release(charge)
		return 0, err
	}
	return charge, nil
}

func measureRelationSpool(
	cursor Cursor,
	columns int,
	cancel *CancelFlag,
) (rows int, payload int64, err error) {
	for {
		next, nextErr := cursor.nextWithCancel(cancel)
		if nextErr != nil {
			return 0, 0, nextErr
		}
		if !next {
			return rows, payload, nil
		}
		if rows == math.MaxInt {
			return 0, 0, fmt.Errorf("query: relation spool row count overflows int")
		}
		rows++
		for column := 0; column < columns; column++ {
			if err := cancellationCheckpoint(cancel, column); err != nil {
				return 0, 0, err
			}
			cellBytes, err := relationCellOwnedBytesCancelable(
				cursor.Cell(column), cancel,
			)
			if err != nil {
				return 0, 0, err
			}
			payload = saturatedBytes(payload, int64(cellBytes))
			if payload == math.MaxInt64 {
				return rows, payload, nil
			}
		}
	}
}

func relationCellOwnedBytes(cell Cell) int {
	bytes, _ := relationCellOwnedBytesCancelable(cell, nil)
	return bytes
}

func relationCellOwnedBytesCancelable(
	cell Cell,
	cancel *CancelFlag,
) (int, error) {
	if cell.kind == TypeNull && cell.flag&cellMissing != 0 {
		return 0, nil
	}
	if cell.raw == nil {
		switch cell.kind {
		case TypeNumber:
			return encodedCellJSONBytes(cell), cancellationError(cancel)
		case TypeString:
			encoded, escaped, err := relationEncodedStringBytesCancelable(
				cell.text, cancel,
			)
			if err != nil {
				return 0, err
			}
			if encoded == math.MaxInt {
				return math.MaxInt, nil
			}
			if escaped {
				if len(cell.text) > math.MaxInt-encoded {
					return math.MaxInt, nil
				}
				encoded += len(cell.text)
			}
			return encoded, nil
		}
		return 0, cancellationError(cancel)
	}
	bytes := len(cell.raw)
	if cell.kind == TypeString {
		escaped, err := relationJSONStringEscapedCancelable(cell.raw, cancel)
		if err != nil {
			return 0, err
		}
		if escaped {
			if len(cell.text) > math.MaxInt-bytes {
				return math.MaxInt, nil
			}
			bytes += len(cell.text)
		}
	}
	return bytes, cancellationError(cancel)
}

func relationJSONStringEscapedCancelable(
	raw []byte,
	cancel *CancelFlag,
) (bool, error) {
	for i, b := range raw {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return false, err
		}
		if b == '\\' {
			return true, nil
		}
	}
	return false, cancellationError(cancel)
}

// relationEncodedStringBytesCancelable returns the exact minimal JSON encoding
// width of text and whether that encoding cannot lend its quoted interior as
// decoded text. It is the allocation-free sizing half of appendJSONString.
func relationEncodedStringBytesCancelable(
	text string,
	cancel *CancelFlag,
) (bytes int, escaped bool, err error) {
	bytes = 2 // quotes
	for i := 0; i < len(text); i++ {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return 0, false, err
		}
		additional := 1
		switch text[i] {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			additional = 2
			escaped = true
		default:
			if text[i] < 0x20 {
				additional = 6 // \u00XX
				escaped = true
			}
		}
		if additional > math.MaxInt-bytes {
			return math.MaxInt, true, nil
		}
		bytes += additional
	}
	return bytes, escaped, cancellationError(cancel)
}

func relationSpoolRetainedBytes(rows, columns int, payload int64) int64 {
	if rows < 0 || columns < 0 || payload < 0 {
		return math.MaxInt64
	}
	columnHeaders := saturatedProduct(
		int64(columns), int64(unsafe.Sizeof([]scalar(nil))),
	)
	cells := saturatedProduct(int64(rows), int64(columns))
	cellBytes := saturatedProduct(cells, int64(unsafe.Sizeof(scalar{})))
	return saturatedBytes(saturatedBytes(columnHeaders, cellBytes), payload)
}

func (s *relationSpool) begin(rows, columns int, payload int64) error {
	if payload > int64(math.MaxInt) {
		return fmt.Errorf("query: relation spool payload exceeds address space")
	}
	if cap(s.columns) < columns {
		s.columns = make([][]scalar, columns)
	} else {
		s.columns = s.columns[:columns]
	}
	for i := range s.columns {
		if cap(s.columns[i]) < rows {
			s.columns[i] = make([]scalar, rows)
		} else {
			s.columns[i] = s.columns[i][:rows]
			clear(s.columns[i])
		}
	}
	need := int(payload)
	if cap(s.data) < need {
		s.data = make([]byte, 0, need)
	} else {
		s.data = s.data[:0]
	}
	s.rows = rows
	s.plannedData = need
	return nil
}

func (s *relationSpool) fill(
	cursor Cursor,
	columns, rows int,
	cancel *CancelFlag,
) error {
	row := 0
	for {
		next, err := cursor.nextWithCancel(cancel)
		if err != nil {
			return err
		}
		if !next {
			if row != rows {
				return fmt.Errorf(
					"query: relation spool changed between sizing and publication",
				)
			}
			if len(s.data) != s.plannedData {
				return s.sizingError(0)
			}
			return nil
		}
		if row >= rows {
			return fmt.Errorf(
				"query: relation spool grew between sizing and publication",
			)
		}
		for column := 0; column < columns; column++ {
			if err := cancellationError(cancel); err != nil {
				return err
			}
			owned, err := s.ownCell(cursor.Cell(column), cancel)
			if err != nil {
				return err
			}
			s.columns[column][row] = owned
		}
		row++
	}
}

func (s *relationSpool) ownCell(cell Cell, cancel *CancelFlag) (scalar, error) {
	var raw []byte
	if cell.kind == TypeNull && cell.flag&cellMissing != 0 {
		raw = nil
	} else if len(cell.raw) != 0 {
		start := len(s.data)
		var err error
		err = s.appendOwnedBytes(cell.raw, cancel)
		if err != nil {
			return scalar{}, err
		}
		raw = s.data[start:len(s.data):len(s.data)]
	} else if cell.kind == TypeNumber {
		need := encodedCellJSONBytes(cell)
		if err := s.ensureData(need); err != nil {
			return scalar{}, err
		}
		start := len(s.data)
		s.data = cell.AppendJSON(s.data)
		if len(s.data)-start != need {
			return scalar{}, s.sizingError(0)
		}
		raw = s.data[start:len(s.data):len(s.data)]
	} else if cell.kind == TypeString {
		text, _ := cell.Text()
		start := len(s.data)
		if err := s.appendJSONString(text, cancel); err != nil {
			return scalar{}, err
		}
		raw = s.data[start:len(s.data):len(s.data)]
	}

	switch cell.kind {
	case TypeNull:
		// A nil raw spelling is the child's absent-path marker. Keep it nil so
		// the spool retains missing versus explicit JSON null internally even
		// though SQL's scalar semantics intentionally classify both as NULL.
		return scalar{kind: kindNull, raw: raw}, nil
	case TypeBool:
		value, _ := cell.Bool()
		return scalar{kind: kindBool, bval: value, raw: raw}, nil
	case TypeNumber:
		value := scalar{kind: kindNumber, num: raw, raw: raw}
		if integer, ok := cell.Int64(); ok {
			value.isInt, value.ival = true, integer
		}
		return value, nil
	case TypeString:
		text, _ := cell.Text()
		escaped, err := relationJSONStringEscapedCancelable(raw, cancel)
		if err != nil {
			return scalar{}, err
		}
		if len(raw) >= 2 && !escaped {
			return scalar{
				kind: kindString,
				sval: byteview.String(raw[1 : len(raw)-1]),
				raw:  raw,
			}, nil
		}
		start := len(s.data)
		if err := s.appendOwnedBytes(byteview.Bytes(text), cancel); err != nil {
			return scalar{}, err
		}
		return scalar{
			kind: kindString,
			sval: byteview.String(s.data[start:len(s.data):len(s.data)]),
			raw:  raw,
		}, nil
	default:
		return scalar{kind: kindContainer, raw: raw}, nil
	}
}

func (s *relationSpool) ensureData(additional int) error {
	if additional < 0 || len(s.data) > s.plannedData ||
		additional > s.plannedData-len(s.data) ||
		additional > cap(s.data)-len(s.data) {
		return s.sizingError(additional)
	}
	return nil
}

func (s *relationSpool) sizingError(additional int) error {
	return fmt.Errorf(
		"%w: planned=%d used=%d additional=%d capacity=%d",
		errRelationSpoolSizing, s.plannedData, len(s.data), additional,
		cap(s.data),
	)
}

func (s *relationSpool) appendOwnedBytes(src []byte, cancel *CancelFlag) error {
	if err := s.ensureData(len(src)); err != nil {
		return err
	}
	if cancel == nil {
		s.data = append(s.data, src...)
		return nil
	}
	const chunk = 32 << 10
	for len(src) != 0 {
		if err := cancellationError(cancel); err != nil {
			return err
		}
		n := min(len(src), chunk)
		s.data = append(s.data, src[:n]...)
		src = src[n:]
	}
	return cancellationError(cancel)
}

func (s *relationSpool) appendJSONString(text string, cancel *CancelFlag) error {
	need, _, err := relationEncodedStringBytesCancelable(text, cancel)
	if err != nil {
		return err
	}
	if err := s.ensureData(need); err != nil {
		return err
	}
	s.data = append(s.data, '"')
	const hex = "0123456789abcdef"
	for i := 0; i < len(text); i++ {
		if err := cancellationCheckpoint(cancel, i); err != nil {
			return err
		}
		switch b := text[i]; b {
		case '"', '\\':
			s.data = append(s.data, '\\', b)
		case '\b':
			s.data = append(s.data, '\\', 'b')
		case '\f':
			s.data = append(s.data, '\\', 'f')
		case '\n':
			s.data = append(s.data, '\\', 'n')
		case '\r':
			s.data = append(s.data, '\\', 'r')
		case '\t':
			s.data = append(s.data, '\\', 't')
		default:
			if b < 0x20 {
				s.data = append(
					s.data, '\\', 'u', '0', '0', hex[b>>4], hex[b&0x0f],
				)
				continue
			}
			s.data = append(s.data, b)
		}
	}
	s.data = append(s.data, '"')
	return cancellationError(cancel)
}
