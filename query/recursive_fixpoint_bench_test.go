package query

import "testing"

type recursiveChainBenchmarkProgram struct {
	limit int64
	row   [1]Cell
}

func (p *recursiveChainBenchmarkProgram) Anchor(out *RecursiveAppender) error {
	p.row[0] = Cell{kind: TypeNumber, flag: cellInteger, word: 1}
	return out.AppendRow(p.row[:])
}

func (p *recursiveChainBenchmarkProgram) Step(
	delta RecursiveDelta,
	out *RecursiveAppender,
) error {
	value, _ := delta.Cell(0, 0).Int64()
	if value >= p.limit {
		return nil
	}
	p.row[0] = Cell{kind: TypeNumber, flag: cellInteger, word: uint64(value + 1)}
	return out.AppendRow(p.row[:])
}

type recursiveCycleBenchmarkProgram struct {
	limit int64
	row   [1]Cell
}

func (p *recursiveCycleBenchmarkProgram) Anchor(out *RecursiveAppender) error {
	p.row[0] = Cell{kind: TypeNumber, flag: cellInteger, word: 0}
	return out.AppendRow(p.row[:])
}

func (p *recursiveCycleBenchmarkProgram) Step(
	delta RecursiveDelta,
	out *RecursiveAppender,
) error {
	for source := 0; source < delta.Rows(); source++ {
		value, _ := delta.Cell(source, 0).Int64()
		p.row[0] = Cell{kind: TypeNumber, flag: cellInteger, word: uint64(value)}
		if err := out.AppendRow(p.row[:]); err != nil {
			return err
		}
		if value+1 < p.limit {
			p.row[0] = Cell{kind: TypeNumber, flag: cellInteger, word: uint64(value + 1)}
			if err := out.AppendRow(p.row[:]); err != nil {
				return err
			}
		}
	}
	return nil
}

func BenchmarkRecursiveFixpointUnionAllWarm(b *testing.B) {
	program := &recursiveChainBenchmarkProgram{limit: 64}
	options := RecursiveFixpointOptions{Columns: 1, Union: RecursiveUnionAll}
	var kernel RecursiveFixpoint
	result, err := kernel.Run(program, options)
	if err != nil || result.Rows() != int(program.limit) {
		b.Fatalf("warm-up rows=%d err=%v", result.Rows(), err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err = kernel.Run(program, options)
		if err != nil || result.Rows() != int(program.limit) {
			b.Fatalf("rows=%d err=%v", result.Rows(), err)
		}
	}
}

func BenchmarkRecursiveFixpointUnionDistinctWarm(b *testing.B) {
	program := &recursiveCycleBenchmarkProgram{limit: 64}
	options := RecursiveFixpointOptions{Columns: 1, Union: RecursiveUnionDistinct}
	var kernel RecursiveFixpoint
	result, err := kernel.Run(program, options)
	if err != nil || result.Rows() != int(program.limit) {
		b.Fatalf("warm-up rows=%d err=%v", result.Rows(), err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err = kernel.Run(program, options)
		if err != nil || result.Rows() != int(program.limit) {
			b.Fatalf("rows=%d err=%v", result.Rows(), err)
		}
	}
}
