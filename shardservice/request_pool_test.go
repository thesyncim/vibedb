package shardservice

import (
	"bytes"
	"reflect"
	"testing"
)

// TestBorrowedRequestShellNeverLeaksAcrossUses proves pool hygiene: a shell
// released with populated lanes decodes clean when borrowed again, so an
// absent optional lane can never serve the previous request's values.
func TestBorrowedRequestShellNeverLeaksAcrossUses(t *testing.T) {
	plain := ownedRequest(`SELECT n FROM docs WHERE id = ?`, StringParam("a"))
	var buf bytes.Buffer
	if err := EncodeRequest(&buf, plain); err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	first := borrowShardRequest()
	if err := decodeBorrowedRequest(&buf, first); err != nil {
		t.Fatalf("decodeBorrowedRequest: %v", err)
	}
	if first.SQL != plain.SQL || len(first.Params) != 1 {
		t.Fatalf("decoded request = %+v, want the plain read", first)
	}

	// Dirty every lane, then release: the next borrow must observe a zero
	// shell even before decoding.
	first.Transaction = TransactionRequest{Operation: TransactionLookupCoordinator}
	first.MaxRows = 99
	first.SQL = "dirty"
	releaseShardRequest(first)

	second := borrowShardRequest()
	if !reflect.DeepEqual(*second, ShardRequest{}) {
		t.Fatalf("borrowed shell = %+v, want zero value", second)
	}

	// A plain read decoded into the recycled shell carries no residue.
	buf.Reset()
	if err := EncodeRequest(&buf, plain); err != nil {
		t.Fatalf("EncodeRequest: %v", err)
	}
	if err := decodeBorrowedRequest(&buf, second); err != nil {
		t.Fatalf("decodeBorrowedRequest: %v", err)
	}
	if second.Transaction.Operation != TransactionNone ||
		second.MaxRows != 0 || second.SQL != plain.SQL {
		t.Fatalf("recycled decode = %+v, want clean plain read", second)
	}
	releaseShardRequest(second)
}
