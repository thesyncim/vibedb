package exchange

import (
	"encoding/binary"
	"errors"
)

const (
	blockMagic      uint32 = 0x56425842 // "VBXB"
	blockHeader            = 12
	MaxBlockColumns uint32 = 1 << 16
	MaxBlockCells   uint64 = 1 << 20
)

var (
	ErrBlockShape = errors.New("exchange: row block has an invalid shape")
	ErrBlockLimit = errors.New("exchange: row block exceeds its row, cell, or byte bound")
	ErrBlockData  = errors.New("exchange: row block data is malformed")
)

// PartitionedBlocks owns one reusable block builder per output partition. It
// reserves a worst-case memory promise at construction but grows each arena
// lazily, so sparse repartition stages do not immediately allocate degree times
// block size.
type PartitionedBlocks struct {
	builders []BlockBuilder
	active   []bool
	columns  uint32
	maxRows  uint32
	maxBytes uint32
}

func NewPartitionedBlocks(
	partitions, columns, maxRows, maxBytes uint32,
	maxMemory uint64,
) (*PartitionedBlocks, error) {
	if partitions == 0 || partitions > MaxPartitions || maxMemory == 0 ||
		uint64(partitions)*uint64(maxBytes) > maxMemory {
		return nil, ErrBlockLimit
	}
	if !validBlockLimits(columns, maxRows, maxBytes) {
		return nil, ErrBlockLimit
	}
	return &PartitionedBlocks{
		builders: make([]BlockBuilder, partitions), active: make([]bool, partitions),
		columns: columns, maxRows: maxRows, maxBytes: maxBytes,
	}, nil
}

// Append appends row to partition. flush is true only when an existing
// non-empty block must be synchronously sent and reset before retrying the same
// row. An individually impossible row returns ErrBlockLimit directly.
func (p *PartitionedBlocks) Append(partition uint32, row []Cell) (flush bool, err error) {
	if p == nil || int(partition) >= len(p.builders) {
		return false, ErrPartitions
	}
	if !p.active[partition] {
		if err := p.builders[partition].Reset(nil, p.columns, p.maxRows, p.maxBytes); err != nil {
			return false, err
		}
		p.active[partition] = true
	}
	err = p.builders[partition].AppendRow(row)
	if !errors.Is(err, ErrBlockLimit) {
		return false, err
	}
	_, rows := p.builders[partition].Bytes()
	if rows == 0 {
		return false, err
	}
	return true, nil
}

// Block returns the current borrowed block. A zero row count means this
// partition has nothing to flush.
func (p *PartitionedBlocks) Block(partition uint32) ([]byte, uint32, error) {
	if p == nil || int(partition) >= len(p.builders) {
		return nil, 0, ErrPartitions
	}
	if !p.active[partition] {
		return nil, 0, nil
	}
	data, rows := p.builders[partition].Bytes()
	return data, rows, nil
}

// ResetPartition releases the logical contents after a synchronous push while
// retaining its arena capacity for the next block.
func (p *PartitionedBlocks) ResetPartition(partition uint32) error {
	if p == nil || int(partition) >= len(p.builders) {
		return ErrPartitions
	}
	if !p.active[partition] {
		return nil
	}
	storage, _ := p.builders[partition].Bytes()
	return p.builders[partition].Reset(storage, p.columns, p.maxRows, p.maxBytes)
}

func (p *PartitionedBlocks) Partitions() uint32 {
	if p == nil {
		return 0
	}
	return uint32(len(p.builders))
}

func (p *PartitionedBlocks) ReservedBytes() uint64 {
	if p == nil {
		return 0
	}
	return uint64(len(p.builders)) * uint64(p.maxBytes)
}

// Cell is one opaque canonical value. Null cells carry no bytes; every other
// cell borrows already-encoded value bytes. The block codec deliberately does
// not interpret JSON, allocate strings, or retain per-cell objects.
type Cell struct {
	Null  bool
	Bytes []byte
}

// BlockBuilder appends rows into one reusable byte arena. Reset retains caller
// storage, and AppendRow performs no allocation once that storage has reached
// the negotiated block size.
type BlockBuilder struct {
	data     []byte
	columns  uint32
	rows     uint32
	maxRows  uint32
	maxBytes uint32
}

// Reset starts one block. storage may be the result of a previous Bytes call;
// its capacity is reused. Limits cannot exceed the exchange wire bounds.
func (b *BlockBuilder) Reset(storage []byte, columns, maxRows, maxBytes uint32) error {
	if b == nil || !validBlockLimits(columns, maxRows, maxBytes) {
		return ErrBlockLimit
	}
	b.data = storage[:0]
	if cap(b.data) < blockHeader {
		b.data = make([]byte, blockHeader, min(int(maxBytes), 4<<10))
	} else {
		b.data = b.data[:blockHeader]
		clear(b.data)
	}
	binary.BigEndian.PutUint32(b.data, blockMagic)
	binary.BigEndian.PutUint32(b.data[4:], columns)
	b.columns, b.rows, b.maxRows, b.maxBytes = columns, 0, maxRows, maxBytes
	return nil
}

func validBlockLimits(columns, maxRows, maxBytes uint32) bool {
	return columns != 0 && columns <= MaxBlockColumns &&
		maxRows != 0 && maxRows <= MaxBatchRows && maxBytes >= blockHeader &&
		maxBytes <= MaxBatchBytes && uint64(columns)*uint64(maxRows) <= MaxBlockCells
}

// ValidBlockLimits reports whether one negotiated intermediate block shape is
// within the hard exchange bounds.
func ValidBlockLimits(columns, maxRows, maxBytes uint32) bool {
	return validBlockLimits(columns, maxRows, maxBytes)
}

// AppendRow appends one exact-width row. It is atomic with respect to a limit
// refusal: the builder remains unchanged when the row cannot fit.
func (b *BlockBuilder) AppendRow(row []Cell) error {
	if b == nil || b.columns == 0 || len(row) != int(b.columns) {
		return ErrBlockShape
	}
	if b.rows == b.maxRows {
		return ErrBlockLimit
	}
	need := len(row)
	for i := range row {
		if row[i].Null {
			if len(row[i].Bytes) != 0 {
				return ErrBlockShape
			}
			continue
		}
		if len(row[i].Bytes) > int(MaxBatchBytes)-4 || need > int(b.maxBytes)-4-len(row[i].Bytes) {
			return ErrBlockLimit
		}
		need += 4 + len(row[i].Bytes)
	}
	if need > int(b.maxBytes)-len(b.data) {
		return ErrBlockLimit
	}
	if need > cap(b.data)-len(b.data) {
		// Grow once to the admitted ceiling instead of relying on append's
		// geometric policy, whose spare capacity could exceed the reservation.
		data := make([]byte, len(b.data), b.maxBytes)
		copy(data, b.data)
		b.data = data
	}
	for i := range row {
		if row[i].Null {
			b.data = append(b.data, 0)
			continue
		}
		b.data = append(b.data, 1)
		b.data = binary.BigEndian.AppendUint32(b.data, uint32(len(row[i].Bytes)))
		b.data = append(b.data, row[i].Bytes...)
	}
	b.rows++
	binary.BigEndian.PutUint32(b.data[8:], b.rows)
	return nil
}

// Bytes returns the complete block and its row count. The bytes remain owned
// by the builder and may be reused by the next Reset only after the transport
// has synchronously consumed them.
func (b *BlockBuilder) Bytes() ([]byte, uint32) {
	if b == nil || b.columns == 0 {
		return nil, 0
	}
	return b.data, b.rows
}

// Block is one validated, borrowed row block. OpenBlock performs a full bounded
// validation once; NextInto can then decode rows without error branches or
// allocation. A Block value is single-consumer.
type Block struct {
	data    []byte
	columns uint32
	rows    uint32
	offset  int
	row     uint32
}

// OpenBlock validates data and returns a borrowed decoder. No field or payload
// bytes are copied.
func OpenBlock(data []byte) (Block, error) {
	if len(data) < blockHeader || len(data) > int(MaxBatchBytes) ||
		binary.BigEndian.Uint32(data) != blockMagic {
		return Block{}, ErrBlockData
	}
	columns := binary.BigEndian.Uint32(data[4:])
	rows := binary.BigEndian.Uint32(data[8:])
	if columns == 0 || columns > MaxBlockColumns || rows > MaxBatchRows ||
		uint64(columns)*uint64(rows) > MaxBlockCells {
		return Block{}, ErrBlockLimit
	}
	offset := blockHeader
	for range uint64(columns) * uint64(rows) {
		if offset >= len(data) {
			return Block{}, ErrBlockData
		}
		kind := data[offset]
		offset++
		switch kind {
		case 0:
		case 1:
			if len(data)-offset < 4 {
				return Block{}, ErrBlockData
			}
			size := binary.BigEndian.Uint32(data[offset:])
			offset += 4
			if uint64(size) > uint64(len(data)-offset) {
				return Block{}, ErrBlockData
			}
			offset += int(size)
		default:
			return Block{}, ErrBlockData
		}
	}
	if offset != len(data) {
		return Block{}, ErrBlockData
	}
	return Block{data: data, columns: columns, rows: rows, offset: blockHeader}, nil
}

func (b *Block) Columns() uint32 {
	if b == nil {
		return 0
	}
	return b.columns
}

func (b *Block) Rows() uint32 {
	if b == nil {
		return 0
	}
	return b.rows
}

// NextInto decodes the next row into exactly Columns cells. Cell bytes borrow
// the block backing and remain valid for the block lifetime.
func (b *Block) NextInto(row []Cell) bool {
	if b == nil || b.row == b.rows || len(row) != int(b.columns) {
		return false
	}
	for i := range row {
		kind := b.data[b.offset]
		b.offset++
		if kind == 0 {
			row[i] = Cell{Null: true}
			continue
		}
		size := int(binary.BigEndian.Uint32(b.data[b.offset:]))
		b.offset += 4
		row[i] = Cell{Bytes: b.data[b.offset : b.offset+size : b.offset+size]}
		b.offset += size
	}
	b.row++
	return true
}
