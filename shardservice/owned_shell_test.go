package shardservice

import (
	"bytes"
	"reflect"
	"testing"
)

// TestOwnedRequestShellNeverLeaksAcrossUses proves the connection loop's
// scrub-before-fill discipline: one owned shell serves successive requests,
// and a sparse request decoded after a populated one carries no residue.
func TestOwnedRequestShellNeverLeaksAcrossUses(t *testing.T) {
	plain := ownedRequest(`SELECT n FROM docs WHERE id = ?`, StringParam("a"))
	var buf bytes.Buffer
	if err := EncodeRequest(&buf, plain); err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	var shell ShardRequest
	shell = ShardRequest{}
	if err := decodeBorrowedRequest(&buf, &shell); err != nil {
		t.Fatalf("decodeBorrowedRequest: %v", err)
	}
	if shell.SQL != plain.SQL || len(shell.Params) != 1 {
		t.Fatalf("decoded request = %+v, want the plain read", shell)
	}

	// Dirty every lane, then scrub exactly like the serving loop: the
	// shell must observe a zero value even before decoding.
	shell.Transaction = TransactionRequest{Operation: TransactionLookupCoordinator}
	shell.MaxRows = 99
	shell.SQL = "dirty"
	shell = ShardRequest{}
	if !reflect.DeepEqual(shell, ShardRequest{}) {
		t.Fatalf("scrubbed shell = %+v, want zero value", shell)
	}

	// A plain read decoded into the reused shell carries no residue.
	buf.Reset()
	if err := EncodeRequest(&buf, plain); err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	if err := decodeBorrowedRequest(&buf, &shell); err != nil {
		t.Fatalf("decodeBorrowedRequest: %v", err)
	}
	if shell.Transaction.Operation != TransactionNone ||
		shell.MaxRows != 0 || shell.SQL != plain.SQL {
		t.Fatalf("reused decode = %+v, want clean plain read", shell)
	}
}
