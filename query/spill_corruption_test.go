package query

import (
	"encoding/gob"
	"errors"
	"os"
	"testing"
)

func TestCorruptSpillRowsFailClosed(t *testing.T) {
	t.Run("decoder", func(t *testing.T) {
		path := t.TempDir() + "/bad-gob"
		if err := os.WriteFile(path, []byte("not a gob stream"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := openRowCursor(
			&plan{}, spillRun{path: path},
		); !errors.Is(err, ErrSpillCorrupt) {
			t.Fatalf("decoder error = %v, want ErrSpillCorrupt", err)
		}
	})

	t.Run("shape", func(t *testing.T) {
		run := writeTestSpillValue(t, diskRow{})
		p := &plan{columns: make([]planColumn, 1)}
		if _, err := openRowCursor(p, run); !errors.Is(err, ErrSpillCorrupt) {
			t.Fatalf("shape error = %v, want ErrSpillCorrupt", err)
		}
	})

	t.Run("number", func(t *testing.T) {
		run := writeTestSpillValue(t, diskRow{
			Values: []diskScalar{{Kind: uint8(kindNumber), Num: []byte("01")}},
		})
		p := &plan{columns: make([]planColumn, 1)}
		if _, err := openRowCursor(p, run); !errors.Is(err, ErrSpillCorrupt) {
			t.Fatalf("number error = %v, want ErrSpillCorrupt", err)
		}
	})
}

func TestCorruptSpillGroupShapeFailsClosed(t *testing.T) {
	run := writeTestSpillValue(t, diskGroup{})
	var budget aggregateBudget
	budget.begin(defaultAggregateBytes)
	if _, err := openGroupCursor(
		run, &budget, 1, 1,
	); !errors.Is(err, ErrSpillCorrupt) {
		t.Fatalf("group shape error = %v, want ErrSpillCorrupt", err)
	}
}

func writeTestSpillValue(t testing.TB, value any) spillRun {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "spill-*")
	if err != nil {
		t.Fatal(err)
	}
	if err := gob.NewEncoder(file).Encode(value); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	info, err := file.Stat()
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return spillRun{path: file.Name(), size: info.Size()}
}
