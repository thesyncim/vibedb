package distributedagg

import (
	"bytes"
	"fmt"

	"github.com/thesyncim/vibedb/internal/exchange"
	queryengine "github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store"
	vibejson "github.com/thesyncim/vibejson"
)

var nullJSON = [...]byte{'n', 'u', 'l', 'l'}

type outputArena struct {
	chunks  [][]byte
	current []byte
}

func (a *outputArena) reserve(need int) int {
	if cap(a.current)-len(a.current) < need {
		size := max(4<<10, 2*cap(a.current))
		if size > 64<<10 {
			size = 64 << 10
		}
		if size < need {
			size = need
		}
		a.current = make([]byte, 0, size)
		a.chunks = append(a.chunks, a.current)
	}
	return len(a.current)
}

func (a *outputArena) appendCount(value *exactCount) []byte {
	need := 21
	if value.large {
		need = value.wide.BitLen()/3 + 2
	}
	start := a.reserve(need)
	a.current = value.appendTo(a.current)
	return a.current[start:len(a.current):len(a.current)]
}

func (a *outputArena) appendInteger(value *exactSignedInteger) []byte {
	need := 21
	if value.large {
		need = value.wide.BitLen()/3 + 3
	}
	start := a.reserve(need)
	a.current = value.appendTo(a.current)
	return a.current[start:len(a.current):len(a.current)]
}

func (a *outputArena) appendRat(value *exactSum, maxBytes uint64) ([]byte, error) {
	start := a.reserve(64)
	var err error
	a.current, err = appendExactDecimal(a.current, &value.rational, maxBytes)
	if err != nil {
		return nil, err
	}
	return a.current[start:len(a.current):len(a.current)], nil
}

func (a *outputArena) appendBytes(value []byte) []byte {
	start := a.reserve(len(value))
	a.current = append(a.current, value...)
	return a.current[start:len(a.current):len(a.current)]
}

// Merger is a dense columnar grouped final-aggregation state. It retains one
// packed copy per distinct group key and one accumulator per aggregate column.
type Merger struct {
	kinds     []Kind
	groupKeys []uint16
	rows      []exchange.Cell
	width     int
	counts    [][]exactCount
	sums      [][]exactSum
	extrema   [][]extreme

	interner      store.KeyInterner
	key           []byte
	numberScratch []byte
	keyEncoder    queryengine.JSONGroupKeyEncoder
	input         outputArena
	output        outputArena
	retained      uint64
	maxBytes      uint64
}

// NewMerger validates program and reserves only fixed per-column directories;
// group payloads grow lazily under maxBytes.
func NewMerger(kinds []Kind, groupKeys []uint16, maxBytes uint64) (*Merger, error) {
	if len(kinds) == 0 || len(groupKeys) == 0 || maxBytes == 0 {
		return nil, fmt.Errorf("%w: grouped program has no columns, keys, or memory bound", ErrAggregate)
	}
	seen := make([]bool, len(kinds))
	for _, column := range groupKeys {
		if int(column) >= len(kinds) || seen[column] || kinds[column] != None {
			return nil, fmt.Errorf("%w: grouped key program is invalid", ErrAggregate)
		}
		seen[column] = true
	}
	for column, kind := range kinds {
		if !kind.Valid() || (kind == None && !seen[column]) {
			return nil, fmt.Errorf("%w: grouped projection column %d is invalid", ErrAggregate, column)
		}
	}
	return &Merger{
		kinds: append([]Kind(nil), kinds...), groupKeys: append([]uint16(nil), groupKeys...),
		maxBytes: maxBytes, counts: make([][]exactCount, len(kinds)),
		sums: make([][]exactSum, len(kinds)), extrema: make([][]extreme, len(kinds)),
	}, nil
}

// Add consumes one borrowed partial row. All retained keys and extrema are
// copied into packed arenas before Add returns.
func (m *Merger) Add(row []exchange.Cell) error {
	if m == nil || len(row) != len(m.kinds) {
		return fmt.Errorf("%w: grouped row width is invalid", ErrAggregate)
	}
	m.key = m.key[:0]
	var rawKeyBytes uint64
	for keyOrdinal, column := range m.groupKeys {
		value := row[column].Bytes
		if row[column].Null {
			value = nullJSON[:]
		}
		if ^uint64(0)-rawKeyBytes < uint64(len(value))+16 {
			return fmt.Errorf("%w: group key byte accounting overflow", ErrLimit)
		}
		rawKeyBytes += uint64(len(value)) + 16
		if m.retained > m.maxBytes || rawKeyBytes > m.maxBytes-m.retained {
			return fmt.Errorf("%w: group key %d exceeds grouped state byte cap", ErrLimit, keyOrdinal)
		}
		var ok bool
		m.key, ok = m.keyEncoder.Append(m.key, value)
		if !ok {
			return fmt.Errorf("%w: group key column %d is not valid JSON", ErrAggregate, column)
		}
	}
	before := m.interner.Len()
	id := m.interner.Intern(m.key)
	if int(id) == before {
		if err := m.addGroup(row, len(m.key)); err != nil {
			return err
		}
	}
	group := int(id)
	for column, kind := range m.kinds {
		cell := row[column]
		switch kind {
		case None:
		case Count:
			previous := m.counts[column][group].retainedBytes()
			if err := addCount(&m.counts[column][group], cell, m.maxBytes); err != nil {
				return err
			}
			if err := m.growState(previous, m.counts[column][group].retainedBytes()); err != nil {
				return err
			}
		case Sum:
			previous := m.sums[column][group].retainedBytes()
			var err error
			m.numberScratch, err = m.sums[column][group].add(cell, m.maxBytes, m.numberScratch[:0])
			if err != nil {
				return err
			}
			if err := m.growState(previous, m.sums[column][group].retainedBytes()); err != nil {
				return err
			}
		case Min, Max:
			if err := m.addExtreme(&m.extrema[column][group], cell, kind == Max); err != nil {
				return err
			}
		default:
			return fmt.Errorf("%w: aggregate kind %d has no grouped combiner", ErrAggregate, kind)
		}
	}
	return nil
}

func (m *Merger) addGroup(row []exchange.Cell, keyBytes int) error {
	inputBytes := 0
	for column, kind := range m.kinds {
		if kind == None && !row[column].Null {
			inputBytes += len(row[column].Bytes)
		}
	}
	charge := uint64(96 + keyBytes + inputBytes + len(row)*192)
	if err := m.charge(charge); err != nil {
		return err
	}
	if m.width == 0 {
		m.width = len(row)
	}
	for column, kind := range m.kinds {
		if kind == None {
			cell := row[column]
			if !cell.Null {
				cell.Bytes = m.input.appendBytes(cell.Bytes)
			}
			m.rows = append(m.rows, cell)
		} else {
			m.rows = append(m.rows, exchange.Cell{})
		}
		switch kind {
		case Count:
			m.counts[column] = append(m.counts[column], exactCount{})
		case Sum:
			m.sums[column] = append(m.sums[column], exactSum{})
		case Min, Max:
			m.extrema[column] = append(m.extrema[column], extreme{})
		}
	}
	return nil
}

func (m *Merger) addExtreme(state *extreme, cell exchange.Cell, maximum bool) error {
	if cell.Null {
		return nil
	}
	spelling := bytes.TrimSpace(cell.Bytes)
	if !vibejson.Valid(spelling) || len(spelling) == 0 ||
		(spelling[0] != '-' && (spelling[0] < '0' || spelling[0] > '9')) {
		return fmt.Errorf("%w: MIN/MAX state is not numeric", ErrAggregate)
	}
	replace := !state.set
	if state.set {
		comparison := queryengine.CompareValidatedJSONNumbers(spelling, state.number)
		replace = (maximum && comparison > 0) || (!maximum && comparison < 0)
	}
	if !replace {
		return nil
	}
	if err := m.charge(uint64(len(cell.Bytes))); err != nil {
		return err
	}
	owned := exchange.Cell{Bytes: m.input.appendBytes(cell.Bytes)}
	state.cell, state.number, state.set = owned, bytes.TrimSpace(owned.Bytes), true
	return nil
}

func (m *Merger) growState(previous, current uint64) error {
	if current <= previous {
		return nil
	}
	return m.charge(current - previous)
}

func (m *Merger) charge(n uint64) error {
	if ^uint64(0)-m.retained < n {
		return fmt.Errorf("%w: grouped state byte accounting overflow", ErrLimit)
	}
	m.retained += n
	if m.retained > m.maxBytes {
		return fmt.Errorf("%w: grouped state %d bytes exceeds %d-byte cap", ErrLimit, m.retained, m.maxBytes)
	}
	return nil
}

// Finish owns every output spelling and returns row slices over one dense cell
// slab. The merger remains the owner and must live as long as the rows.
func (m *Merger) Finish() ([][]exchange.Cell, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: nil grouped merger", ErrAggregate)
	}
	groups := m.interner.Len()
	var outputBytes uint64
	for group := 0; group < groups; group++ {
		for column, kind := range m.kinds {
			cell := &m.rows[group*m.width+column]
			switch kind {
			case None:
			case Count:
				*cell = exchange.Cell{Bytes: m.output.appendCount(&m.counts[column][group])}
			case Sum:
				state := &m.sums[column][group]
				if !state.set {
					*cell = exchange.Cell{Null: true}
				} else if !state.fraction {
					*cell = exchange.Cell{Bytes: m.output.appendInteger(&state.integer)}
				} else {
					spelling, err := m.output.appendRat(state, m.maxBytes)
					if err != nil {
						return nil, err
					}
					*cell = exchange.Cell{Bytes: spelling}
				}
			case Min, Max:
				state := &m.extrema[column][group]
				if state.set {
					*cell = state.cell
				} else {
					*cell = exchange.Cell{Null: true}
				}
			}
			if ^uint64(0)-outputBytes < uint64(len(cell.Bytes)) {
				return nil, fmt.Errorf("%w: grouped output byte accounting overflow", ErrLimit)
			}
			outputBytes += uint64(len(cell.Bytes))
			if m.retained > m.maxBytes || outputBytes > m.maxBytes-m.retained {
				return nil, fmt.Errorf("%w: grouped output %d bytes exceeds %d-byte cap", ErrLimit, outputBytes, m.maxBytes)
			}
		}
	}
	rows := make([][]exchange.Cell, groups)
	for group := range rows {
		start := group * m.width
		rows[group] = m.rows[start : start+m.width : start+m.width]
	}
	return rows, nil
}
