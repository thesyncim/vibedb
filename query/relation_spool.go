package query

import (
	"fmt"
	"math"
	"unsafe"

	"github.com/thesyncim/vibejson/x/byteview"
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
	columns [][]scalar
	data    []byte
	rows    int
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
			payload = saturatedBytes(
				payload,
				int64(relationCellOwnedBytes(cursor.Cell(column))),
			)
			if payload == math.MaxInt64 {
				return rows, payload, nil
			}
		}
	}
}

func relationCellOwnedBytes(cell Cell) int {
	if cell.kind == TypeNull && cell.flag&cellMissing != 0 {
		return 0
	}
	if cell.raw == nil {
		if cell.kind == TypeNumber {
			return encodedCellJSONBytes(cell)
		}
		return 0
	}
	bytes := len(cell.raw)
	if cell.kind == TypeString && relationJSONStringEscaped(cell.raw) {
		if len(cell.text) > math.MaxInt-bytes {
			return math.MaxInt
		}
		bytes += len(cell.text)
	}
	return bytes
}

func relationJSONStringEscaped(raw []byte) bool {
	for _, b := range raw {
		if b == '\\' {
			return true
		}
	}
	return false
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
		s.data, err = appendRelationOwnedBytes(s.data, cell.raw, cancel)
		if err != nil {
			return scalar{}, err
		}
		raw = s.data[start:len(s.data):len(s.data)]
	} else if cell.kind == TypeNumber {
		start := len(s.data)
		s.data = cell.AppendJSON(s.data)
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
		if len(raw) >= 2 && !relationJSONStringEscaped(raw) {
			return scalar{
				kind: kindString,
				sval: byteview.String(raw[1 : len(raw)-1]),
				raw:  raw,
			}, nil
		}
		start := len(s.data)
		var err error
		s.data, err = appendRelationOwnedBytes(
			s.data, byteview.Bytes(text), cancel,
		)
		if err != nil {
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

func appendRelationOwnedBytes(
	dst, src []byte,
	cancel *CancelFlag,
) ([]byte, error) {
	if cancel == nil {
		return append(dst, src...), nil
	}
	const chunk = 32 << 10
	for len(src) != 0 {
		if err := cancellationError(cancel); err != nil {
			return dst, err
		}
		n := min(len(src), chunk)
		dst = append(dst, src[:n]...)
		src = src[n:]
	}
	return dst, cancellationError(cancel)
}
