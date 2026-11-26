package shardservice

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"

	"github.com/thesyncim/vibedb/query"
)

var errSQLReadFrameBound = errors.New("shardservice: encoded SQL result exceeds its frame budget")

// encodeSQLReadCursor produces the ordinary ResponseRows grammar directly from
// the borrowed cursor. Preflight counts only visible rows (including HAVING,
// OFFSET and LIMIT), bounds every byte, and reserves one owned frame before
// encoding. The frame outlives the cursor without a detached row/cell matrix.
func encodeSQLReadCursor(cursor query.Cursor, names []string, limit int, cancel *query.CancelFlag) ([]byte, error) {
	if len(names) > maxColumns {
		return nil, errFieldTooLarge
	}
	limit = min(limit, maxFrameBody+5)
	// Frame header, version, kind, column count, row count, absent position.
	size := 16
	take := func(n int) bool {
		if n < 0 || n > limit-size {
			return false
		}
		size += n
		return true
	}
	if !take(0) {
		return nil, errSQLReadFrameBound
	}
	for _, name := range names {
		if !take(8) || !take(len(name)) {
			return nil, errSQLReadFrameBound
		}
	}
	rows := 0
	probe := cursor
	for {
		next, err := probe.NextWithCancel(cancel)
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}
		rows++
		if rows > maxRows || len(names) == 0 {
			return nil, errSQLReadFrameBound
		}
		for col := range names {
			cell := probe.Cell(col)
			if !take(1) {
				return nil, errSQLReadFrameBound
			}
			if cell.IsNull() {
				continue
			}
			payload := cell.Payload()
			var scratch [32]byte
			if payload == nil {
				payload = cell.AppendJSON(scratch[:0])
			}
			if !take(4) || !take(len(payload)) {
				return nil, errSQLReadFrameBound
			}
		}
	}
	e := encbuf{b: make([]byte, 5, size)}
	e.u8(wireVersion)
	e.u8(uint8(ResponseRows))
	e.u32(uint32(len(names)))
	for _, name := range names {
		e.str(name)
		e.u32(pgOIDJSON)
	}
	e.u32(uint32(rows))
	for {
		next, err := cursor.NextWithCancel(cancel)
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}
		for col := range names {
			cell := cursor.Cell(col)
			if cell.IsNull() {
				e.u8(1)
				continue
			}
			e.u8(0)
			lengthAt := len(e.b)
			e.u32(0)
			e.b = cell.AppendJSON(e.b)
			binary.BigEndian.PutUint32(e.b[lengthAt:lengthAt+4], uint32(len(e.b)-lengthAt-4))
		}
	}
	e.u8(0) // no inner read position; the RF3 envelope carries the fenced cut
	e.b[0] = tagResponse
	binary.BigEndian.PutUint32(e.b[1:5], uint32(len(e.b)-1))
	return e.b, nil
}

// Error replies retain the ordinary encoder and identical frame admission.
func encodeSQLReadError(response *ShardResponse, limit int) ([]byte, error) {
	if !replicatedSQLResponseFits(response, limit) {
		return nil, errSQLReadFrameBound
	}
	var encoded bytes.Buffer
	if err := EncodeResponse(&encoded, response); err != nil {
		return nil, err
	}
	return encoded.Bytes(), nil
}

// materializeSQLReadCursor materializes an owned, detached ShardResponse and
// parallel flat TypedCells matrix directly from cursor for in-process semantic
// dispatch, skipping wire framing, decoding, cloning, and gateway JSON
// reparsing while enforcing the exact same frame-size and row bounds as
// encodeSQLReadCursor.
func materializeSQLReadCursor(cursor query.Cursor, names []string, limit int, cancel *query.CancelFlag) (*ShardResponse, []query.Cell, error) {
	if len(names) > maxColumns {
		return nil, nil, errFieldTooLarge
	}
	limit = min(limit, maxFrameBody+5)
	size := 16
	take := func(n int) bool {
		if n < 0 || n > limit-size {
			return false
		}
		size += n
		return true
	}
	if !take(0) {
		return nil, nil, errSQLReadFrameBound
	}
	for _, name := range names {
		if !take(8) || !take(len(name)) {
			return nil, nil, errSQLReadFrameBound
		}
	}
	rows := 0
	var arenaBytes int64
	probe := cursor
	for {
		next, err := probe.NextWithCancel(cancel)
		if err != nil {
			return nil, nil, err
		}
		if !next {
			break
		}
		rows++
		if rows > maxRows || len(names) == 0 {
			return nil, nil, errSQLReadFrameBound
		}
		for col := range names {
			cell := probe.Cell(col)
			if !take(1) {
				return nil, nil, errSQLReadFrameBound
			}
			if cell.IsNull() {
				continue
			}
			payload := cell.Payload()
			var scratch [32]byte
			if payload == nil {
				payload = cell.AppendJSON(scratch[:0])
				arenaBytes += int64(len(payload))
			} else {
				arenaBytes += query.OwnedCellBytes(cell)
			}
			if !take(4) || !take(len(payload)) {
				return nil, nil, errSQLReadFrameBound
			}
		}
	}
	cols := len(names)
	columns := make([]Column, cols)
	for i, name := range names {
		columns[i] = Column{Name: strings.Clone(name), TypeOID: pgOIDJSON}
	}
	resp := &ShardResponse{
		Kind:    ResponseRows,
		Columns: columns,
	}
	if rows == 0 {
		return resp, nil, nil
	}
	totalCells := rows * cols
	cellArena := make([]Cell, totalCells)
	typedCells := make([]query.Cell, totalCells)
	rowSlices := make([][]Cell, rows)
	var byteArena []byte
	if arenaBytes > 0 {
		byteArena = make([]byte, 0, arenaBytes)
	}
	rowIdx := 0
	for {
		next, err := cursor.NextWithCancel(cancel)
		if err != nil {
			return nil, nil, err
		}
		if !next {
			break
		}
		base := rowIdx * cols
		row := cellArena[base : base+cols : base+cols]
		for col := range cols {
			cell := cursor.Cell(col)
			if cell.IsNull() {
				row[col] = Cell{Null: true}
				typedCells[base+col] = query.NullCell()
				continue
			}
			owned, nextArena := query.OwnCellInto(byteArena, cell)
			byteArena = nextArena
			typedCells[base+col] = owned
			if payload := owned.Payload(); len(payload) != 0 {
				row[col] = Cell{Bytes: payload}
			} else {
				start := len(byteArena)
				byteArena = owned.AppendJSON(byteArena)
				row[col] = Cell{Bytes: byteArena[start:len(byteArena):len(byteArena)]}
			}
		}
		rowSlices[rowIdx] = row
		rowIdx++
	}
	resp.Rows = rowSlices
	return resp, typedCells, nil
}
