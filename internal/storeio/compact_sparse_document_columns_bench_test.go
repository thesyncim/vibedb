package storeio

import (
	"math"
	"testing"
)

var compactSparseFloat64BenchmarkValue float64

func compactSparseFloat64BenchmarkViews(b *testing.B) (DocumentPageView, SparseDocumentPageView, CompactSparseDocumentPageView, CompactSparseDocumentPageView, DocumentPageHeader, [SparseDocumentPageSlotCount]DocumentRecord, DocumentFloat64Columns) {
	b.Helper()
	rows := sparseDocumentTestRows()
	header := testDocumentPageHeader(^uint64(0))
	columns := compactSparseFloat64TestColumns(header.Live)
	fileEnd := uint64(32 * testSuperblockPageSize)

	densePage := make([]byte, header.PageSize)
	if _, err := EncodeDocumentPageWithColumns(
		densePage, header, rows[:], columns,
		testDocumentNextLogicalID, fileEnd, testSuperblockPageSize,
	); err != nil {
		b.Fatal(err)
	}
	widePage, _ := encodeSparseDocumentTestPage(b, rows[:])
	compactPage, _ := encodeCompactSparseDocumentTestPage(b, rows[:])
	columnPage := make([]byte, header.PageSize)
	if _, err := EncodeCompactSparseDocumentPageWithColumns(
		columnPage, header, rows[:], columns,
		testDocumentNextLogicalID, fileEnd, testSuperblockPageSize,
	); err != nil {
		b.Fatal(err)
	}
	return AdmittedDocumentPage(densePage),
		AdmittedSparseDocumentPage(widePage),
		AdmittedCompactSparseDocumentPage(compactPage),
		AdmittedCompactSparseDocumentPage(columnPage),
		header, rows, columns
}

func BenchmarkCompactSparseFloat64PointJSON(b *testing.B) {
	dense, wide, compact, columns, _, _, _ := compactSparseFloat64BenchmarkViews(b)
	b.Run("Current8ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			sparseDocumentBenchmarkBytes, _ = dense.LookupJSON(uint8(index & 63))
		}
	})
	b.Run("SDP1_6ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			sparseDocumentBenchmarkBytes, _ = wide.LookupJSON(uint8(index & 63))
		}
	})
	b.Run("SDP2_4ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			sparseDocumentBenchmarkBytes, _ = compact.LookupJSON(uint8(index & 63))
		}
	})
	b.Run("SDP3_4BytePlusColumns", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			sparseDocumentBenchmarkBytes, _ = columns.LookupJSON(uint8(index & 63))
		}
	})
}

func BenchmarkCompactSparseFloat64ColumnLookup(b *testing.B) {
	dense, _, _, compact, _, _, _ := compactSparseFloat64BenchmarkViews(b)
	b.Run("CurrentResolveAndPoint", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			column, _ := dense.Float64Column(index % 3)
			compactSparseFloat64BenchmarkValue, _ = column.Lookup(uint8(index & 63))
		}
	})
	b.Run("SDP3ResolveAndPoint", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			column, _ := compact.Float64Column(index % 3)
			compactSparseFloat64BenchmarkValue, _ = column.Lookup(uint8(index & 63))
		}
	})
	denseColumn, _ := dense.Float64Column(0)
	compactColumn, _ := compact.Float64Column(0)
	b.Run("CurrentAdmittedColumnPoint", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			compactSparseFloat64BenchmarkValue, _ = denseColumn.Lookup(uint8((index * 2) & 63))
		}
	})
	b.Run("SDP3AdmittedColumnPoint", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			compactSparseFloat64BenchmarkValue, _ = compactColumn.Lookup(uint8((index * 2) & 63))
		}
	})
}

func BenchmarkCompactSparseFloat64ColumnScan(b *testing.B) {
	dense, _, _, compact, _, _, _ := compactSparseFloat64BenchmarkViews(b)
	for name, view := range map[string]DocumentFloat64ColumnView{
		"Current": func() DocumentFloat64ColumnView {
			column, _ := dense.Float64Column(0)
			return column
		}(),
		"SDP3": func() DocumentFloat64ColumnView {
			column, _ := compact.Float64Column(0)
			return column
		}(),
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(32 * 8)
			for range b.N {
				iterator := view.Iterator()
				for {
					slot, value, ok := iterator.Next()
					if !ok {
						break
					}
					sparseDocumentBenchmarkSlot = slot
					compactSparseFloat64BenchmarkValue = value
				}
			}
		})
	}
}

func BenchmarkCompactSparseFloat64OrderedFullScan(b *testing.B) {
	dense, wide, compact, columns, header, _, _ := compactSparseFloat64BenchmarkViews(b)
	b.Run("Current8ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(header.Live))
		for range b.N {
			iterator := dense.Rows(^uint64(0))
			for {
				slot, _, json, _, ok := iterator.Next()
				if !ok {
					break
				}
				sparseDocumentBenchmarkSlot = slot
				sparseDocumentBenchmarkBytes = json
			}
		}
	})
	for name, view := range map[string]CompactSparseDocumentPageView{
		"SDP2_4ByteDescriptor":  compact,
		"SDP3_4BytePlusColumns": columns,
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(header.Live))
			for range b.N {
				iterator := view.AllRows()
				for {
					slot, _, json, _, ok := iterator.Next()
					if !ok {
						break
					}
					sparseDocumentBenchmarkSlot = slot
					sparseDocumentBenchmarkBytes = json
				}
			}
		})
	}
	b.Run("SDP1_6ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(header.Live))
		for range b.N {
			iterator := wide.AllRows()
			for {
				slot, _, json, _, ok := iterator.Next()
				if !ok {
					break
				}
				sparseDocumentBenchmarkSlot = slot
				sparseDocumentBenchmarkBytes = json
			}
		}
	})
}

func BenchmarkCompactSparseFloat64Encode(b *testing.B) {
	_, _, _, _, header, rows, columns := compactSparseFloat64BenchmarkViews(b)
	fileEnd := uint64(32 * testSuperblockPageSize)
	b.Run("CurrentWithColumns", func(b *testing.B) {
		page := make([]byte, header.PageSize)
		b.ReportAllocs()
		for range b.N {
			if _, err := EncodeDocumentPageWithColumns(
				page, header, rows[:], columns,
				testDocumentNextLogicalID, fileEnd, testSuperblockPageSize,
			); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("SDP3WithColumns", func(b *testing.B) {
		page := make([]byte, header.PageSize)
		b.ReportAllocs()
		b.ReportMetric(4.25, "document_metadata_B/row")
		b.ReportMetric(308.0/64.0, "fixture_column_B/row")
		for range b.N {
			if _, err := EncodeCompactSparseDocumentPageWithColumns(
				page, header, rows[:], columns,
				testDocumentNextLogicalID, fileEnd, testSuperblockPageSize,
			); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCompactSparseFloat64Mutation(b *testing.B) {
	const slot = uint8(26)
	_, wide, _, compact, header, rows, columns := compactSparseFloat64BenchmarkViews(b)
	options := sparseDocumentMutationTestOptions(header.Generation + 1)
	updated := rows[slot]
	updated.JSON = []byte(`{"value":9999999999999999999999}`)
	replacement := CompactSparseFloat64Row{
		Present: []uint64{5},
		Values:  []float64{math.Copysign(0, -1), 0, math.MaxFloat64},
	}
	b.Run("SDP1Update", func(b *testing.B) {
		after := make([]byte, header.PageSize)
		var workspace SparseDocumentWorkspace
		plan, err := PlanAdmittedSparseDocumentPageUpdate(
			after, wide, updated, options, &workspace,
		)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := PlanAdmittedSparseDocumentPageUpdate(
				after, wide, updated, options, &workspace,
			); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(plan.ChangedBytes), "write_B/op")
	})
	b.Run("SDP3DocumentAndColumnsUpdate", func(b *testing.B) {
		after := make([]byte, header.PageSize)
		var workspace SparseDocumentWorkspace
		plan, err := PlanAdmittedCompactSparseDocumentPageUpdateWithFloat64(
			after, compact, updated, replacement, options, &workspace,
		)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := PlanAdmittedCompactSparseDocumentPageUpdateWithFloat64(
				after, compact, updated, replacement, options, &workspace,
			); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(plan.ChangedBytes), "write_B/op")
	})
	b.Run("CurrentFullPageUpdateWithColumns", func(b *testing.B) {
		after := make([]byte, header.PageSize)
		replacedRows := rows
		replacedRows[slot] = updated
		replacedColumns := DocumentFloat64Columns{
			Masks:  append([]uint64(nil), columns.Masks...),
			Values: append([]float64(nil), columns.Values...),
		}
		for column := range replacedColumns.Masks {
			bit := uint64(1) << slot
			replacedColumns.Masks[column] &^= bit
			if replacement.Present[0]&(uint64(1)<<uint(column)) != 0 {
				replacedColumns.Masks[column] |= bit
				replacedColumns.Values[column*64+int(slot)] = replacement.Values[column]
			}
		}
		b.ReportMetric(float64(len(after)), "write_B/op")
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := EncodeDocumentPageWithColumns(
				after, header, replacedRows[:], replacedColumns,
				testDocumentNextLogicalID, uint64(32*testSuperblockPageSize),
				testSuperblockPageSize,
			); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("SDP1Delete", func(b *testing.B) {
		after := make([]byte, header.PageSize)
		var workspace SparseDocumentWorkspace
		plan, err := PlanAdmittedSparseDocumentPageDelete(
			after, wide, slot, options, &workspace,
		)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := PlanAdmittedSparseDocumentPageDelete(
				after, wide, slot, options, &workspace,
			); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(plan.ChangedBytes), "write_B/op")
	})
	b.Run("SDP3DeleteWithColumns", func(b *testing.B) {
		after := make([]byte, header.PageSize)
		var workspace SparseDocumentWorkspace
		plan, err := PlanAdmittedCompactSparseDocumentPageDelete(
			after, compact, slot, options, &workspace,
		)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := PlanAdmittedCompactSparseDocumentPageDelete(
				after, compact, slot, options, &workspace,
			); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(plan.ChangedBytes), "write_B/op")
	})
	b.Run("CurrentFullPageDeleteWithColumns", func(b *testing.B) {
		after := make([]byte, header.PageSize)
		deletedRows := make([]DocumentRecord, 0, len(rows)-1)
		deletedRows = append(deletedRows, rows[:slot]...)
		deletedRows = append(deletedRows, rows[slot+1:]...)
		deletedHeader := header
		deletedHeader.Live &^= uint64(1) << slot
		deletedColumns := DocumentFloat64Columns{
			Masks:  append([]uint64(nil), columns.Masks...),
			Values: append([]float64(nil), columns.Values...),
		}
		for column := range deletedColumns.Masks {
			deletedColumns.Masks[column] &^= uint64(1) << slot
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := EncodeDocumentPageWithColumns(
				after, deletedHeader, deletedRows, deletedColumns,
				testDocumentNextLogicalID, uint64(32*testSuperblockPageSize),
				testSuperblockPageSize,
			); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(len(after)), "write_B/op")
	})
}
