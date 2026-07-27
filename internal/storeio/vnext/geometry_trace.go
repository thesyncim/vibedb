package vnext

import (
	"fmt"
	"math/bits"
	"slices"
)

// TraceKind is one logical mutation for the block-geometry laboratory.
type TraceKind uint8

const (
	TracePut TraceKind = iota + 1
	TraceDelete
)

// TraceOperation addresses one logical key. RecordBytes is the exact key plus
// JSON byte count after a put; framing is accounted by the raw-block codec.
type TraceOperation struct {
	Kind        TraceKind
	Key         uint64
	RecordBytes int
}

// TraceConfig fixes hot mutation geometry and cold canonical maintenance.
// PlacementCandidateLimit bounds the number of recent blocks examined for a
// new key. MaintenanceSteps bounds adjacent-pair examinations, while
// MaintenanceRelocationBudget bounds records whose stable block ID may change.
// Maintenance is disabled when both maintenance bounds are zero.
type TraceConfig struct {
	HotMaxSpan                  int
	ColdMaxSpan                 int
	PlacementCandidateLimit     int
	MaintenanceSteps            int
	MaintenanceRelocationBudget int
}

// TraceMetrics compares exact-quantum spans with the current power-of-two
// policy for the same logical data-block rewrites. It intentionally excludes
// directory, block-map, publication-root, allocator, and retirement traffic.
type TraceMetrics struct {
	Operations                     uint64
	Documents                      uint64
	LogicalBytes                   uint64
	Blocks                         uint64
	ExactSpanBytes                 uint64
	PowerOfTwoSpanBytes            uint64
	ExactDataExtentWriteBytes      uint64
	PowerOfTwoDataExtentWriteBytes uint64
	PlacementProbes                uint64
	SplitRelocations               uint64
	MaintenancePairProbes          uint64
	MaintenanceMerges              uint64
	MaintenanceRelocations         uint64
	FreshRebuildDataExtentBytes    uint64
}

type traceRecord struct {
	key   uint64
	bytes int
}

type traceBlock struct {
	records []traceRecord
}

// SimulateTrace replays mutations into byte-sized stable-slot blocks. New-key
// placement and adjacent maintenance are explicitly bounded. Fresh-rebuild
// geometry is calculated only as a comparison and is never installed as the
// simulated state.
func SimulateTrace(operations []TraceOperation, config TraceConfig) (TraceMetrics, error) {
	if config.HotMaxSpan == 0 {
		config.HotMaxSpan = 8 << 10
	}
	if config.ColdMaxSpan == 0 {
		config.ColdMaxSpan = 16 << 10
	}
	if config.PlacementCandidateLimit == 0 {
		config.PlacementCandidateLimit = 4
	}
	if !validSpan(config.HotMaxSpan) || !validSpan(config.ColdMaxSpan) ||
		config.HotMaxSpan > config.ColdMaxSpan ||
		config.PlacementCandidateLimit < 0 ||
		config.MaintenanceSteps < 0 ||
		config.MaintenanceRelocationBudget < 0 ||
		(config.MaintenanceSteps == 0) != (config.MaintenanceRelocationBudget == 0) {
		return TraceMetrics{}, fmt.Errorf("%w: trace geometry", ErrInvalidFrame)
	}
	blocks := make([]*traceBlock, 0)
	locations := make(map[uint64]*traceBlock)
	var metrics TraceMetrics
	for _, operation := range operations {
		metrics.Operations++
		switch operation.Kind {
		case TracePut:
			if operation.RecordBytes <= 0 ||
				operation.RecordBytes+RawBlockFixedBytes > config.HotMaxSpan {
				return TraceMetrics{}, fmt.Errorf("%w: trace record", ErrInvalidFrame)
			}
			block := locations[operation.Key]
			if block == nil {
				var probes int
				block, probes = boundedTraceBlock(
					blocks,
					operation.RecordBytes,
					config.HotMaxSpan,
					config.PlacementCandidateLimit,
				)
				metrics.PlacementProbes += uint64(probes)
				if block == nil {
					block = &traceBlock{}
					blocks = append(blocks, block)
				}
				block.records = append(block.records, traceRecord{
					key: operation.Key, bytes: operation.RecordBytes,
				})
				locations[operation.Key] = block
			} else {
				for i := range block.records {
					if block.records[i].key == operation.Key {
						block.records[i].bytes = operation.RecordBytes
						break
					}
				}
			}
			if traceBlockBytes(block) > config.HotMaxSpan && len(block.records) > 1 {
				left, right := splitTraceBlock(block, operation.Key, config.HotMaxSpan)
				if traceBlockBytes(left) > config.HotMaxSpan ||
					traceBlockBytes(right) > config.HotMaxSpan {
					return TraceMetrics{}, fmt.Errorf("%w: trace split", ErrInvalidFrame)
				}
				metrics.SplitRelocations += uint64(len(right.records))
				metrics.accountWrite(traceBlockBytes(left))
				metrics.accountWrite(traceBlockBytes(right))
				block.records = left.records
				blocks = append(blocks, right)
				for _, record := range right.records {
					locations[record.key] = right
				}
			} else {
				metrics.accountWrite(traceBlockBytes(block))
			}
		case TraceDelete:
			block := locations[operation.Key]
			if block == nil {
				continue
			}
			for i := range block.records {
				if block.records[i].key != operation.Key {
					continue
				}
				copy(block.records[i:], block.records[i+1:])
				block.records = block.records[:len(block.records)-1]
				delete(locations, operation.Key)
				break
			}
			metrics.accountWrite(traceBlockBytes(block))
		default:
			return TraceMetrics{}, fmt.Errorf("%w: trace operation", ErrInvalidFrame)
		}
	}
	blocks = liveTraceBlocks(blocks)
	fresh := packTraceRecords(allTraceRecords(blocks), config.ColdMaxSpan)
	metrics.FreshRebuildDataExtentBytes = traceBlocksExactBytes(fresh)
	if config.MaintenanceSteps != 0 {
		blocks = maintainTraceBlocks(blocks, locations, config, &metrics)
	}
	for _, block := range blocks {
		encoded := traceBlockBytes(block)
		metrics.Blocks++
		metrics.ExactSpanBytes += uint64(exactTraceSpan(encoded))
		metrics.PowerOfTwoSpanBytes += uint64(powerOfTwoTraceSpan(encoded))
		for _, record := range block.records {
			metrics.Documents++
			metrics.LogicalBytes += uint64(record.bytes)
		}
	}
	return metrics, nil
}

func (m *TraceMetrics) accountWrite(encoded int) {
	if encoded == 0 {
		return
	}
	m.ExactDataExtentWriteBytes += uint64(exactTraceSpan(encoded))
	m.PowerOfTwoDataExtentWriteBytes += uint64(powerOfTwoTraceSpan(encoded))
}

func boundedTraceBlock(
	blocks []*traceBlock,
	recordBytes, maxSpan, candidateLimit int,
) (*traceBlock, int) {
	var best *traceBlock
	bestBytes := -1
	start := 0
	if len(blocks) > candidateLimit {
		start = len(blocks) - candidateLimit
	}
	probes := 0
	for i := len(blocks) - 1; i >= start; i-- {
		probes++
		block := blocks[i]
		if len(block.records) >= RawBlockSlotCount {
			continue
		}
		encoded := RawBlockFixedBytes + recordBytes
		if len(block.records) != 0 {
			encoded = traceBlockBytes(block) + recordBytes
		}
		if encoded <= maxSpan && encoded > bestBytes {
			best, bestBytes = block, encoded
		}
	}
	return best, probes
}

func maintainTraceBlocks(
	blocks []*traceBlock,
	locations map[uint64]*traceBlock,
	config TraceConfig,
	metrics *TraceMetrics,
) []*traceBlock {
	cursor := 0
	remainingRelocations := config.MaintenanceRelocationBudget
	for cursor+1 < len(blocks) &&
		int(metrics.MaintenancePairProbes) < config.MaintenanceSteps &&
		remainingRelocations != 0 {
		metrics.MaintenancePairProbes++
		left, right := blocks[cursor], blocks[cursor+1]
		if len(left.records)+len(right.records) > RawBlockSlotCount ||
			traceBlockBytes(left)+traceBlockBytes(right)-RawBlockFixedBytes >
				config.ColdMaxSpan {
			cursor++
			continue
		}

		moveLeft := len(left.records) < len(right.records)
		relocations := len(right.records)
		if moveLeft {
			relocations = len(left.records)
		}
		if relocations > remainingRelocations {
			cursor++
			continue
		}

		if moveLeft {
			combined := make([]traceRecord, 0, len(left.records)+len(right.records))
			combined = append(combined, left.records...)
			combined = append(combined, right.records...)
			right.records = combined
			for _, record := range left.records {
				locations[record.key] = right
			}
			copy(blocks[cursor:], blocks[cursor+1:])
			blocks = blocks[:len(blocks)-1]
		} else {
			left.records = append(left.records, right.records...)
			for _, record := range right.records {
				locations[record.key] = left
			}
			copy(blocks[cursor+1:], blocks[cursor+2:])
			blocks = blocks[:len(blocks)-1]
		}
		metrics.MaintenanceMerges++
		metrics.MaintenanceRelocations += uint64(relocations)
		remainingRelocations -= relocations
		metrics.accountWrite(traceBlockBytes(blocks[cursor]))
		if cursor != 0 {
			cursor--
		}
	}
	return blocks
}

func splitTraceBlock(
	block *traceBlock,
	changedKey uint64,
	maxSpan int,
) (*traceBlock, *traceBlock) {
	total := 0
	for _, record := range block.records {
		total += record.bytes
	}
	leftBytes, split := 0, 1
	for i, record := range block.records[:len(block.records)-1] {
		leftBytes += record.bytes
		split = i + 1
		if leftBytes*2 >= total {
			break
		}
	}
	left := &traceBlock{records: slices.Clone(block.records[:split])}
	right := &traceBlock{records: slices.Clone(block.records[split:])}
	if traceBlockBytes(left) <= maxSpan && traceBlockBytes(right) <= maxSpan {
		return left, right
	}

	// The block fit before this operation and every individual record fits the
	// hot maximum. Isolating the inserted or replaced record therefore provides
	// a valid fallback when its position makes every contiguous balanced cut
	// overflow one side.
	left.records = left.records[:0]
	right.records = right.records[:0]
	for _, record := range block.records {
		if record.key == changedKey {
			right.records = append(right.records, record)
		} else {
			left.records = append(left.records, record)
		}
	}
	return left, right
}

func liveTraceBlocks(blocks []*traceBlock) []*traceBlock {
	out := blocks[:0]
	for _, block := range blocks {
		if len(block.records) != 0 {
			out = append(out, block)
		}
	}
	return out
}

func allTraceRecords(blocks []*traceBlock) []traceRecord {
	count := 0
	for _, block := range blocks {
		count += len(block.records)
	}
	records := make([]traceRecord, 0, count)
	for _, block := range blocks {
		records = append(records, block.records...)
	}
	slices.SortFunc(records, func(a, b traceRecord) int {
		switch {
		case a.key < b.key:
			return -1
		case a.key > b.key:
			return 1
		default:
			return 0
		}
	})
	return records
}

func packTraceRecords(records []traceRecord, maxSpan int) []*traceBlock {
	blocks := make([]*traceBlock, 0)
	for _, record := range records {
		var block *traceBlock
		if len(blocks) != 0 {
			candidate := blocks[len(blocks)-1]
			if len(candidate.records) < RawBlockSlotCount &&
				traceBlockBytes(candidate)+record.bytes <= maxSpan {
				block = candidate
			}
		}
		if block == nil {
			block = &traceBlock{}
			blocks = append(blocks, block)
		}
		block.records = append(block.records, record)
	}
	return blocks
}

func traceBlockBytes(block *traceBlock) int {
	if block == nil || len(block.records) == 0 {
		return 0
	}
	bytes := RawBlockFixedBytes
	for _, record := range block.records {
		bytes += record.bytes
	}
	return bytes
}

func traceBlocksExactBytes(blocks []*traceBlock) uint64 {
	var bytes uint64
	for _, block := range blocks {
		bytes += uint64(exactTraceSpan(traceBlockBytes(block)))
	}
	return bytes
}

func exactTraceSpan(encoded int) int {
	if encoded == 0 {
		return 0
	}
	return (encoded + Quantum - 1) / Quantum * Quantum
}

func powerOfTwoTraceSpan(encoded int) int {
	if encoded <= Quantum {
		return Quantum
	}
	return 1 << bits.Len(uint(encoded-1))
}
