package sql

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"unsafe"
)

func TestParserReadPreparationRetainedBytesCountsGrowth(t *testing.T) {
	var parser Parser
	var tree SelectStmt
	if err := parser.Parse(&tree, `SELECT id FROM docs WHERE id = ?`); err != nil {
		t.Fatalf("Parse: %v", err)
	}
	small, ok := parser.ReadPreparationRetainedBytes()
	if !ok {
		t.Fatal("ordinary direct parser shape declined")
	}
	if small < int64(unsafe.Sizeof(parser)) {
		t.Fatalf("retained bytes = %d, parser object = %d", small, unsafe.Sizeof(parser))
	}

	longName := strings.Repeat("x", 1200)
	if err := parser.Parse(&tree, `SELECT `+longName+` FROM docs WHERE `+longName+` = ?`); err == nil {
		// The long identifier is intentionally not a valid physical schema name
		// for lowering, but the parser itself must retain it when the syntax is
		// valid. The parser accepts it as a field name.
	} else {
		t.Fatalf("Parse(long identifier): %v", err)
	}
	large, ok := parser.ReadPreparationRetainedBytes()
	if !ok {
		t.Fatal("long direct parser shape declined")
	}
	if large <= small {
		t.Fatalf("long parse retained bytes = %d, warm small parse = %d", large, small)
	}

	parser.Release()
	if got, ok := parser.ReadPreparationRetainedBytes(); !ok || got != int64(unsafe.Sizeof(parser)) {
		t.Fatalf("released parser retained bytes = (%d, %v), want parser object only", got, ok)
	}
}

func TestParserReadPreparationCountsEveryDirectSliceCapacity(t *testing.T) {
	// Inventory the struct independently of the accountant's field list, so
	// future retained slice fields cannot silently disappear from the charge.
	typ := reflect.TypeFor[Parser]()
	for field := 0; field < typ.NumField(); field++ {
		if typ.Field(field).Type.Kind() != reflect.Slice {
			continue
		}
		t.Run(typ.Field(field).Name, func(t *testing.T) {
			var parser Parser
			v := reflect.ValueOf(&parser).Elem().Field(field)
			owned := reflect.MakeSlice(v.Type(), 0, 3)
			reflect.NewAt(v.Type(), unsafe.Pointer(v.UnsafeAddr())).Elem().Set(owned)
			got, ok := parser.ReadPreparationRetainedBytes()
			want := int64(unsafe.Sizeof(parser)) + int64(3*v.Type().Elem().Size())
			if !ok || got != want {
				t.Fatalf("retained=%d/%v want=%d", got, ok, want)
			}
		})
	}
}

func TestParserReadPreparationClearsCanceledRequest(t *testing.T) {
	var parser Parser
	canceled := errors.New("request canceled")
	parser.SetCancellationCheck(func() error { return canceled })
	var tree SelectStmt
	if err := parser.Parse(&tree, `SELECT id FROM docs WHERE id = ?`); !errors.Is(err, canceled) {
		t.Fatalf("parse error=%v", err)
	}
	parser.SetCancellationCheck(nil)
	if parser.cancel != nil || parser.lx.cancel != nil || parser.lx.cancelErr != nil {
		t.Fatal("canceled request remained reachable")
	}
	if err := parser.Parse(&tree, `SELECT id FROM docs WHERE id = ?`); err != nil {
		t.Fatal(err)
	}
	if _, ok := parser.ReadPreparationRetainedBytes(); !ok {
		t.Fatal("fresh parse after cancellation declined")
	}
	if readReuseProduct(-1, 8) >= 0 {
		t.Fatal("negative capacity accepted")
	}
	if readReuseAdd(1<<63-1, 1) >= 0 {
		t.Fatal("overflowing total accepted")
	}
}

func TestParserReadPreparationRetainedBytesDeclinesUnownedState(t *testing.T) {
	var canceled Parser
	canceled.SetCancellationCheck(func() error { return nil })
	var tree SelectStmt
	if err := canceled.Parse(&tree, `SELECT id FROM docs WHERE id = ?`); err != nil {
		t.Fatalf("Parse(cancellation hook): %v", err)
	}
	if got, ok := canceled.ReadPreparationRetainedBytes(); ok || got != 0 {
		t.Fatalf("cancellation parser retained bytes = (%d, %v), want (0, false)", got, ok)
	}
	canceled.SetCancellationCheck(nil)
	if got, ok := canceled.ReadPreparationRetainedBytes(); !ok || got <= int64(unsafe.Sizeof(canceled)) {
		t.Fatalf("cleared cancellation parser retained bytes = (%d, %v), want retained arenas", got, ok)
	}

	var scalar Parser
	if err := scalar.Parse(&tree, `SELECT id + 1 FROM docs WHERE id = ?`); err != nil {
		t.Fatalf("Parse(scalar): %v", err)
	}
	if got, ok := scalar.ReadPreparationRetainedBytes(); ok || got != 0 {
		t.Fatalf("scalar parser retained bytes = (%d, %v), want (0, false)", got, ok)
	}
}
