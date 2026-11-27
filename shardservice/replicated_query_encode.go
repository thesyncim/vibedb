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
	cols := len(names)
	columns := make([]Column, cols)
	for i, name := range names {
		columns[i] = Column{Name: strings.Clone(name), TypeOID: pgOIDJSON}
	}
	resp := &ShardResponse{
		Kind:    ResponseRows,
		Columns: columns,
	}
	next, err := cursor.NextWithCancel(cancel)
	if err != nil {
		return nil, nil, err
	}
	if !next {
		return resp, nil, nil
	}
	if cols == 0 {
		return nil, nil, errSQLReadFrameBound
	}
	cellArena := make([]Cell, 0, cols)
	typedCells := make([]query.Cell, 0, cols)
	var byteArena []byte
	rows := 0
	for {
		rows++
		if rows > maxRows {
			return nil, nil, errSQLReadFrameBound
		}
		if rows == 2 && cap(cellArena) < 64*cols {
			nextCells := make([]Cell, len(cellArena), 64*cols)
			copy(nextCells, cellArena)
			cellArena = nextCells
			nextTyped := make([]query.Cell, len(typedCells), 64*cols)
			copy(nextTyped, typedCells)
			typedCells = nextTyped
		}
		for col := range cols {
			if !take(1) {
				return nil, nil, errSQLReadFrameBound
			}
			cell := cursor.Cell(col)
			if cell.IsNull() {
				cellArena = append(cellArena, Cell{Null: true})
				typedCells = append(typedCells, query.NullCell())
				continue
			}
			owned, nextArena := query.OwnCellInto(byteArena, cell)
			byteArena = nextArena
			typedCells = append(typedCells, owned)
			var raw []byte
			if payload := owned.Payload(); len(payload) != 0 {
				raw = payload
			} else {
				start := len(byteArena)
				byteArena = owned.AppendJSON(byteArena)
				raw = byteArena[start:len(byteArena):len(byteArena)]
			}
			if !take(4) || !take(len(raw)) {
				return nil, nil, errSQLReadFrameBound
			}
			cellArena = append(cellArena, Cell{Bytes: raw})
		}
		next, err = cursor.NextWithCancel(cancel)
		if err != nil {
			return nil, nil, err
		}
		if !next {
			break
		}
	}
	rowSlices := make([][]Cell, rows)
	for r := range rows {
		base := r * cols
		rowSlices[r] = cellArena[base : base+cols : base+cols]
	}
	resp.Rows = rowSlices
	return resp, typedCells, nil
}

var directSQLRowsResponse = &ShardResponse{Kind: ResponseRows}

// materializeSQLReadCursorDirect writes owned query.Cell values directly into
// target.Result and target.Arena on the local fused SQL read path, avoiding
// intermediate ShardResponse row/cell slices, TypedCells transpose arrays,
// unused JSON conversions, and per-query arena allocations while enforcing the
// exact same frame-size and row bounds as encodeSQLReadCursor.
func materializeSQLReadCursorDirect(
	cursor query.Cursor,
	names []string,
	limit int,
	cancel *query.CancelFlag,
	target *DirectSQLTarget,
) (*ShardResponse, error) {
	target.Populated = false
	target.RowCount = 0
	target.WireBytes = 0
	if len(names) > maxColumns {
		return nil, errFieldTooLarge
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
		return nil, errSQLReadFrameBound
	}
	for _, name := range names {
		if !take(8) || !take(len(name)) {
			return nil, errSQLReadFrameBound
		}
	}
	cols := len(names)
	dst := target.Result
	prevCols := dst.Columns
	if cap(prevCols) < cols {
		for i := range prevCols {
			clear(prevCols[i].Cells)
		}
		dst.Columns = make([]query.ResultColumn, cols)
	} else {
		for i := cols; i < len(prevCols); i++ {
			clear(prevCols[i].Cells)
			prevCols[i] = query.ResultColumn{}
		}
		dst.Columns = prevCols[:cols]
	}
	for i, name := range names {
		cells := dst.Columns[i].Cells
		clear(cells)
		dst.Columns[i] = query.ResultColumn{Header: name, Cells: cells[:0]}
	}
	dst.RowCount = 0
	var arena []byte
	if target.Arena != nil {
		arena = (*target.Arena)[:0]
	}
	next, err := cursor.NextWithCancel(cancel)
	if err != nil {
		return nil, err
	}
	if !next {
		if target.Arena != nil {
			*target.Arena = arena
		}
		target.Populated = true
		target.RowCount = 0
		target.WireBytes = uint64(size)
		return directSQLRowsResponse, nil
	}
	if cols == 0 {
		return nil, errSQLReadFrameBound
	}
	rows := 0
	for {
		rows++
		if rows > maxRows {
			return nil, errSQLReadFrameBound
		}
		for col := range cols {
			if !take(1) {
				return nil, errSQLReadFrameBound
			}
			cell := cursor.Cell(col)
			if cell.IsNull() {
				dst.Columns[col].Cells = append(dst.Columns[col].Cells, query.NullCell())
				continue
			}
			owned, nextArena := query.OwnCellInto(arena, cell)
			arena = nextArena
			dst.Columns[col].Cells = append(dst.Columns[col].Cells, owned)
			wireLen := len(owned.Payload())
			if wireLen == 0 {
				var scratch [32]byte
				wireLen = len(owned.AppendJSON(scratch[:0]))
			}
			if !take(4) || !take(wireLen) {
				return nil, errSQLReadFrameBound
			}
		}
		next, err = cursor.NextWithCancel(cancel)
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}
	}
	dst.RowCount = rows
	if target.Arena != nil {
		*target.Arena = arena
	}
	target.Populated = true
	target.RowCount = rows
	target.WireBytes = uint64(size)
	return directSQLRowsResponse, nil
}
