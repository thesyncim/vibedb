package storeio

import "testing"

var (
	sparseDocumentBenchmarkBytes []byte
	sparseDocumentBenchmarkSlot  uint8
)

func BenchmarkSparseDocumentPagePointLookup(b *testing.B) {
	rows := sparseDocumentTestRows()
	header := testDocumentPageHeader(^uint64(0))

	currentPage := make([]byte, header.PageSize)
	if _, err := EncodeDocumentPage(currentPage, header, rows[:], testDocumentNextLogicalID); err != nil {
		b.Fatal(err)
	}
	current := AdmittedDocumentPage(currentPage)

	sparsePage := make([]byte, header.PageSize)
	if _, err := EncodeSparseDocumentPage(sparsePage, header, rows[:], testDocumentNextLogicalID); err != nil {
		b.Fatal(err)
	}
	sparse, err := OpenSparseDocumentPage(sparsePage, header.ChunkID+1, testDocumentNextLogicalID)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Current8ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			sparseDocumentBenchmarkBytes, _ = current.LookupJSON(uint8(index & 63))
		}
	})
	b.Run("Sparse6ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			sparseDocumentBenchmarkBytes, _ = sparse.LookupJSON(uint8(index & 63))
		}
	})
}

func BenchmarkSparseDocumentPageFullScan(b *testing.B) {
	rows := sparseDocumentTestRows()
	header := testDocumentPageHeader(^uint64(0))

	currentPage := make([]byte, header.PageSize)
	if _, err := EncodeDocumentPage(currentPage, header, rows[:], testDocumentNextLogicalID); err != nil {
		b.Fatal(err)
	}
	current := AdmittedDocumentPage(currentPage)

	sparsePage := make([]byte, header.PageSize)
	if _, err := EncodeSparseDocumentPage(sparsePage, header, rows[:], testDocumentNextLogicalID); err != nil {
		b.Fatal(err)
	}
	sparse, err := OpenSparseDocumentPage(sparsePage, header.ChunkID+1, testDocumentNextLogicalID)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Current8ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(rows)))
		for range b.N {
			iterator := current.Rows(^uint64(0))
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
	b.Run("Sparse6ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(rows)))
		for range b.N {
			iterator := sparse.AllRows()
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

func BenchmarkSparseDocumentPageSmallMutation(b *testing.B) {
	const slot = 26 // Deliberately crosses a 512-byte heap-sector boundary.
	rows := sparseDocumentTestRows()
	header := testDocumentPageHeader(^uint64(0))
	page, _ := encodeSparseDocumentTestPage(b, rows[:])
	admitted := AdmittedSparseDocumentPage(page)
	var workspace SparseDocumentWorkspace
	options := sparseDocumentMutationTestOptions(header.Generation + 1)

	b.Run("CurrentFullPageUpdate", func(b *testing.B) {
		updated := rows
		updated[slot].JSON = []byte(`{"value":9999999999999999999999}`)
		after := make([]byte, len(page))
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := EncodeDocumentPage(after, header, updated[:], testDocumentNextLogicalID); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(len(page)), "write_B/op")
	})
	b.Run("SparseSameSizeUpdatePlan", func(b *testing.B) {
		updated := rows[slot]
		updated.JSON = []byte(`{"value":9999999999999999999999}`)
		after := make([]byte, len(page))
		plan, err := PlanAdmittedSparseDocumentPageUpdate(after, admitted, updated, options, &workspace)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := PlanAdmittedSparseDocumentPageUpdate(after, admitted, updated, options, &workspace); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(plan.ChangedBytes), "write_B/op")
		b.ReportMetric(float64(len(plan.Changed)), "write_runs/op")
	})
	b.Run("CurrentFullPageDelete", func(b *testing.B) {
		after := make([]byte, len(page))
		var remaining [SparseDocumentPageSlotCount - 1]DocumentRecord
		copy(remaining[:slot], rows[:slot])
		copy(remaining[slot:], rows[slot+1:])
		deleteHeader := header
		deleteHeader.Generation++
		deleteHeader.Live &^= uint64(1) << slot
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := EncodeDocumentPage(after, deleteHeader, remaining[:], testDocumentNextLogicalID); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(len(page)), "write_B/op")
	})
	b.Run("SparseDeletePlan", func(b *testing.B) {
		after := make([]byte, len(page))
		plan, err := PlanAdmittedSparseDocumentPageDelete(after, admitted, slot, options, &workspace)
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			if _, err := PlanAdmittedSparseDocumentPageDelete(after, admitted, slot, options, &workspace); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(plan.ChangedBytes), "write_B/op")
		b.ReportMetric(float64(len(plan.Changed)), "write_runs/op")
	})
}

func BenchmarkSparseDocumentPageDenseSpace(b *testing.B) {
	rows := sparseDocumentTestRows()
	header := testDocumentPageHeader(^uint64(0))
	current := make([]byte, header.PageSize)
	sparse := make([]byte, header.PageSize)
	b.Run("Current8ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(DocumentPagePayloadHeaderSize+len(rows)*DocumentPageRecordSize)/float64(len(rows)), "metadata_B/row")
		b.ReportMetric(float64(int(header.PageSize)-PageHeaderSize-PageTrailerSize-DocumentPagePayloadHeaderSize-len(rows)*DocumentPageRecordSize), "heap_capacity_B")
		for range b.N {
			if _, err := EncodeDocumentPage(current, header, rows[:], testDocumentNextLogicalID); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Sparse6ByteDescriptor", func(b *testing.B) {
		b.ReportAllocs()
		b.ReportMetric(float64(SparseDocumentPageHeapStart-PageHeaderSize)/float64(len(rows)), "metadata_B/row")
		b.ReportMetric(float64(int(header.PageSize)-PageTrailerSize-SparseDocumentPageHeapStart), "heap_capacity_B")
		for range b.N {
			if _, err := EncodeSparseDocumentPage(sparse, header, rows[:], testDocumentNextLogicalID); err != nil {
				b.Fatal(err)
			}
		}
	})
}
