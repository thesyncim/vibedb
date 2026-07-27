package storeio

import "testing"

func BenchmarkCompactSparseDocumentPagePointLookup(b *testing.B) {
	rows := sparseDocumentTestRows()
	widePage, _ := encodeSparseDocumentTestPage(b, rows[:])
	wide := AdmittedSparseDocumentPage(widePage)
	compactPage, _ := encodeCompactSparseDocumentTestPage(b, rows[:])
	compact := AdmittedCompactSparseDocumentPage(compactPage)

	b.Run("Sparse6ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			sparseDocumentBenchmarkBytes, _ = wide.LookupJSON(uint8(index & 63))
		}
	})
	b.Run("Compact4ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			sparseDocumentBenchmarkBytes, _ = compact.LookupJSON(uint8(index & 63))
		}
	})
}

func BenchmarkCompactSparseDocumentPageFullScan(b *testing.B) {
	rows := sparseDocumentTestRows()
	widePage, _ := encodeSparseDocumentTestPage(b, rows[:])
	wide := AdmittedSparseDocumentPage(widePage)
	compactPage, _ := encodeCompactSparseDocumentTestPage(b, rows[:])
	compact := AdmittedCompactSparseDocumentPage(compactPage)

	b.Run("Sparse6ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(rows)))
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
	b.Run("Compact4ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(rows)))
		for range b.N {
			iterator := compact.AllRows()
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

func BenchmarkCompactSparseDocumentPageEncode(b *testing.B) {
	rows := sparseDocumentTestRows()
	header := testDocumentPageHeader(^uint64(0))
	wide := make([]byte, header.PageSize)
	compact := make([]byte, header.PageSize)

	b.Run("Sparse6ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(
			float64(SparseDocumentPageHeapStart-PageHeaderSize)/float64(len(rows)),
			"metadata_B/row",
		)
		b.ReportMetric(
			float64(int(header.PageSize)-PageTrailerSize-SparseDocumentPageHeapStart),
			"heap_capacity_B",
		)
		for range b.N {
			if _, err := EncodeSparseDocumentPage(
				wide, header, rows[:], testDocumentNextLogicalID,
			); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Compact4ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(
			float64(CompactSparseDocumentPageHeapStart-PageHeaderSize)/float64(len(rows)),
			"metadata_B/row",
		)
		b.ReportMetric(
			float64(int(header.PageSize)-PageTrailerSize-CompactSparseDocumentPageHeapStart),
			"heap_capacity_B",
		)
		for range b.N {
			if _, err := EncodeCompactSparseDocumentPage(
				compact, header, rows[:], testDocumentNextLogicalID,
			); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCompactSparseDocumentPageSmallMutation(b *testing.B) {
	const slot = 26
	rows := sparseDocumentTestRows()
	header := testDocumentPageHeader(^uint64(0))
	widePage, _ := encodeSparseDocumentTestPage(b, rows[:])
	wide := AdmittedSparseDocumentPage(widePage)
	compactPage, _ := encodeCompactSparseDocumentTestPage(b, rows[:])
	compact := AdmittedCompactSparseDocumentPage(compactPage)
	options := sparseDocumentMutationTestOptions(header.Generation + 1)
	updated := rows[slot]
	updated.JSON = []byte(`{"value":9999999999999999999999}`)

	b.Run("Sparse6ByteUpdatePlan", func(b *testing.B) {
		after := make([]byte, len(widePage))
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
		b.ReportMetric(float64(len(plan.Changed)), "write_runs/op")
	})
	b.Run("Compact4ByteUpdatePlan", func(b *testing.B) {
		after := make([]byte, len(compactPage))
		var workspace SparseDocumentWorkspace
		plan, err := PlanAdmittedCompactSparseDocumentPageUpdate(
			after, compact, updated, options, &workspace,
		)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := PlanAdmittedCompactSparseDocumentPageUpdate(
				after, compact, updated, options, &workspace,
			); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(plan.ChangedBytes), "write_B/op")
		b.ReportMetric(float64(len(plan.Changed)), "write_runs/op")
	})
	b.Run("Sparse6ByteDeletePlan", func(b *testing.B) {
		after := make([]byte, len(widePage))
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
		b.ReportMetric(float64(len(plan.Changed)), "write_runs/op")
	})
	b.Run("Compact4ByteDeletePlan", func(b *testing.B) {
		after := make([]byte, len(compactPage))
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
		b.ReportMetric(float64(len(plan.Changed)), "write_runs/op")
	})
}
