package storeio

import (
	"bytes"
	"testing"
)

var compactAlphabetBenchmarkSink []byte

// BenchmarkCompactAlphabetFinishVariedV1 isolates the variable-length
// 64-symbol alphabet character packer used by the varied-v1 workload. It
// reports payload throughput for the frozen scalar reference and the
// reservoir implementation after each scratch buffer has been warmed.
func BenchmarkCompactAlphabetFinishVariedV1(b *testing.B) {
	values := compactVariedV1Values(2*compactStreamRestart + 2)
	var setup compactStreamScratch
	plan, ok := setup.measureAlphabet(0, values, 0)
	if !ok || plan.width != 6 || len(setup.alphabet[0]) != 64 {
		b.Fatalf("benchmark fixture did not select 64-symbol width-6 alphabet: plan=%+v alphabet=%d",
			plan, len(setup.alphabet[0]))
	}
	alphabet := append([]byte(nil), setup.alphabet[0]...)
	bytesPerOp := len(values) * 256
	winner := encodeCompactScalarStream(values)
	if winner.kind != compactStreamAlphabet || winner.width != 6 ||
		len(winner.dict) == 0 || len(winner.dict[0]) != 64 {
		b.Fatalf("normal encoder did not select width-6 64-symbol alphabet: kind=%d width=%d dictionary=%d",
			winner.kind, winner.width, len(winner.dict))
	}

	b.Run("scalar-reference", func(b *testing.B) {
		var scratch compactStreamScratch
		scratch.alphabet[0] = append(scratch.alphabet[0][:0], alphabet...)
		warm := finishCompactAlphabetScalarReference(&scratch, 0, values, plan)
		warmExpected := append([]byte(nil), warm.data...)
		if !bytes.Equal(winner.data, warmExpected) {
			b.Fatal("normal encoder winner differs from scalar reference")
		}
		b.ReportAllocs()
		b.SetBytes(int64(bytesPerOp))
		b.ResetTimer()
		for range b.N {
			encoded := finishCompactAlphabetScalarReference(&scratch, 0, values, plan)
			compactAlphabetBenchmarkSink = encoded.data
		}
		b.StopTimer()
		if !bytes.Equal(compactAlphabetBenchmarkSink, warmExpected) {
			b.Fatal("scalar reference output changed")
		}
	})

	b.Run("reservoir", func(b *testing.B) {
		var scratch compactStreamScratch
		scratch.alphabet[0] = append(scratch.alphabet[0][:0], alphabet...)
		warm := scratch.finishAlphabet(0, values, plan)
		warmExpected := append([]byte(nil), warm.data...)
		reference := finishCompactAlphabetScalarReference(&setup, 0, values, plan)
		if !bytes.Equal(warmExpected, reference.data) || !bytes.Equal(winner.data, warmExpected) {
			b.Fatal("reservoir output differs from scalar reference")
		}
		b.ReportAllocs()
		b.SetBytes(int64(bytesPerOp))
		b.ResetTimer()
		for range b.N {
			encoded := scratch.finishAlphabet(0, values, plan)
			compactAlphabetBenchmarkSink = encoded.data
		}
		b.StopTimer()
		if !bytes.Equal(compactAlphabetBenchmarkSink, warmExpected) {
			b.Fatal("reservoir output changed")
		}
	})
}
