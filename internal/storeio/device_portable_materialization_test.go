package storeio

import (
	"errors"
	"os"
	"testing"
)

func TestPortableMaterializationSyncFailuresFailClosed(
	t *testing.T,
) {
	const size = 4096
	for failPhase := -1; failPhase < 3; failPhase++ {
		name := "success"
		if failPhase >= 0 {
			name = [...]string{"journal", "targets", "root"}[failPhase]
		}
		t.Run(name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "portable-materialization-*")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			arena := make([]byte, 3*size)
			for rank, value := range []byte{0x11, 0x22, 0x33} {
				for index := range size {
					arena[rank*size+index] = value
				}
			}
			device := &portableDevice{
				file: file, arena: arena, bufferSize: size, buffers: 3,
				seen: make([]uint64, 1),
			}
			sentinel := errors.New("injected phase failure")
			barrierCall := 0
			verifyByte := func(offset int64, want byte) {
				t.Helper()
				var got [1]byte
				if _, err := file.ReadAt(got[:], offset); err != nil {
					t.Fatal(err)
				}
				if got[0] != want {
					t.Fatalf("byte at %d = %#x, want %#x",
						offset, got[0], want)
				}
			}
			device.materializationBarrier = func(*os.File) error {
				switch barrierCall {
				case 0:
					verifyByte(0, 0x11)
				case 1:
					verifyByte(size, 0x22)
				default:
					t.Fatalf("unexpected barrier call %d", barrierCall)
				}
				phase := barrierCall
				barrierCall++
				if failPhase == phase {
					return sentinel
				}
				return nil
			}
			finalCalls := 0
			device.materializationFinalSync = func(*os.File) error {
				verifyByte(2*size, 0x33)
				finalCalls++
				if failPhase == 2 {
					return sentinel
				}
				return nil
			}
			result, commitErr := device.CommitMaterialized(
				Write{Buffer: 0, Offset: 0, Length: size},
				[]Write{{Buffer: 1, Offset: size, Length: 512}},
				Write{Buffer: 2, Offset: 2 * size, Length: size},
				materializationHybrid,
			)
			if failPhase < 0 {
				if commitErr != nil || result.CompletedPhases != 3 ||
					result.CompletedBarriers != 3 || !result.RootAttempted ||
					barrierCall != 2 || finalCalls != 1 {
					t.Fatalf(
						"success = result %+v, barriers %d, finals %d, err %v",
						result, barrierCall, finalCalls, commitErr,
					)
				}
				return
			}
			if !errors.Is(commitErr, sentinel) ||
				result.CompletedPhases != uint8(failPhase) ||
				result.CompletedBarriers != uint8(failPhase) ||
				result.RootAttempted != (failPhase == 2) {
				t.Fatalf("phase %d failure = result %+v, err %v",
					failPhase, result, commitErr)
			}
			if want := min(failPhase+1, 2); barrierCall != want {
				t.Fatalf("phase %d barrier calls = %d, want %d",
					failPhase, barrierCall, want)
			}
			wantFinal := 0
			if failPhase == 2 {
				wantFinal = 1
			}
			if finalCalls != wantFinal {
				t.Fatalf("phase %d final calls = %d, want %d",
					failPhase, finalCalls, wantFinal)
			}
		})
	}
}

func TestPortablePatchOnlyMaterializationUsesTwoBarriers(t *testing.T) {
	const size = 4096
	for _, failFinal := range []bool{false, true} {
		name := "success"
		if failFinal {
			name = "final-sync-failure"
		}
		t.Run(name, func(t *testing.T) {
			file, err := os.CreateTemp(t.TempDir(), "portable-two-fence-*")
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			arena := make([]byte, 3*size)
			for rank, value := range []byte{0x41, 0x52, 0x63} {
				for index := range size {
					arena[rank*size+index] = value
				}
			}
			device := &portableDevice{
				file: file, arena: arena, bufferSize: size, buffers: 3,
				seen: make([]uint64, 1),
			}
			barriers := 0
			device.materializationBarrier = func(*os.File) error {
				barriers++
				return nil
			}
			finals := 0
			sentinel := errors.New("final sync failed")
			device.materializationFinalSync = func(*os.File) error {
				finals++
				if failFinal {
					return sentinel
				}
				return nil
			}
			result, commitErr := device.CommitMaterialized(
				Write{Buffer: 0, Offset: 0, Length: size},
				[]Write{{Buffer: 1, Offset: size, Length: 512}},
				Write{Buffer: 2, Offset: 2 * size, Length: size},
				materializationPatchOnly,
			)
			if barriers != 1 || finals != 1 || !result.RootAttempted {
				t.Fatalf(
					"calls=(barriers=%d finals=%d) result=%+v",
					barriers, finals, result,
				)
			}
			if failFinal {
				if !errors.Is(commitErr, sentinel) ||
					result.CompletedPhases != 2 ||
					result.CompletedBarriers != 1 {
					t.Fatalf("final failure = (%+v,%v)", result, commitErr)
				}
				return
			}
			if commitErr != nil || result.CompletedPhases != 3 ||
				result.CompletedBarriers != 2 {
				t.Fatalf("success = (%+v,%v)", result, commitErr)
			}
		})
	}
}
