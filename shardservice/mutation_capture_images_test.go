package shardservice

import (
	"bytes"
	"errors"
	"testing"
)

func TestMutationCaptureWireReturnsExactImagesWithoutPublication(t *testing.T) {
	srv, _ := newServer(t, Options{})
	conn := dial(t, srv)

	exec(t, conn, ownedRequest(`
		CREATE TABLE mutation_capture_images (
			id STRING PRIMARY KEY,
			n INTEGER NOT NULL,
			mirror INTEGER NOT NULL
		)`))
	const before = `{"id":"a","mirror":0,"n":5}`
	exec(t, conn, ownedRequest(
		`INSERT INTO mutation_capture_images VALUES (?)`,
		DocumentParam(before),
	))
	legacy := ownedRequest(`
		UPDATE mutation_capture_images SET n = 7 WHERE id = 'a'`)
	legacy.ExecutionMode = ExecutionReadOnly
	legacy.MutationCapture = true
	legacyResponse := exec(t, conn, legacy)
	if len(legacyResponse.Columns) != 2 ||
		legacyResponse.Columns[0] != (Column{Name: "primary_key", TypeOID: pgOIDJSON}) ||
		legacyResponse.Columns[1] != (Column{Name: "document", TypeOID: pgOIDJSON}) ||
		len(legacyResponse.Rows) != 1 || len(legacyResponse.Rows[0]) != 2 ||
		legacyResponse.Rows[0][0].Null || legacyResponse.Rows[0][1].Null ||
		string(legacyResponse.Rows[0][1].Bytes) != before {
		t.Fatalf("legacy mutation capture = %+v", legacyResponse)
	}

	update := ownedRequest(`
		UPDATE mutation_capture_images
		SET n = n + 1, mirror = n
		WHERE id = 'a'`)
	update.ExecutionMode = ExecutionReadOnly
	update.MutationImageCapture = true
	var encoded bytes.Buffer
	if err := EncodeRequest(&encoded, update); err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeRequest(&encoded)
	if err != nil || !decoded.MutationImageCapture || decoded.MutationCapture {
		t.Fatalf("image-capture wire round trip = %+v, %v", decoded, err)
	}
	illegal := *update
	illegal.MutationCapture = true
	if err := EncodeRequest(&encoded, &illegal); !errors.Is(err, errBadMutationCapture) {
		t.Fatalf("combined capture modes = %v, want errBadMutationCapture", err)
	}
	response := exec(t, conn, update)
	if len(response.Columns) != 3 ||
		response.Columns[0] != (Column{Name: "primary_key", TypeOID: pgOIDJSON}) ||
		response.Columns[1] != (Column{Name: "before_document", TypeOID: pgOIDJSON}) ||
		response.Columns[2] != (Column{Name: "after_document", TypeOID: pgOIDJSON}) ||
		len(response.Rows) != 1 || len(response.Rows[0]) != 3 {
		t.Fatalf("computed UPDATE capture shape = %+v", response)
	}
	row := response.Rows[0]
	const after = `{"id":"a","mirror":5,"n":6}`
	if row[0].Null || len(row[0].Bytes) == 0 || row[1].Null ||
		string(row[1].Bytes) != before || row[2].Null ||
		string(row[2].Bytes) != after {
		t.Fatalf(
			"computed UPDATE capture = key:%x before:%s/null=%v after:%s/null=%v",
			row[0].Bytes, row[1].Bytes, row[1].Null, row[2].Bytes, row[2].Null,
		)
	}

	bounded := *update
	bounded.MaxResultBytes = uint64(
		len(row[0].Bytes) + len(row[1].Bytes) + len(row[2].Bytes) - 1,
	)
	limited := roundTrip(t, conn, &bounded)
	if limited.Kind != ResponseError || limited.ErrorKind != ErrorResourceLimit {
		t.Fatalf("postimage-bounded capture = %+v, want ResourceLimit", limited)
	}

	read := ownedRequest(`SELECT "$doc" FROM mutation_capture_images WHERE id = 'a'`)
	read.ExecutionMode = ExecutionReadOnly
	if got := cellText(t, exec(t, conn, read), 0, 0); got != before {
		t.Fatalf("computed UPDATE capture published %s, want original %s", got, before)
	}

	deleteCapture := ownedRequest(
		`DELETE FROM mutation_capture_images WHERE id = 'a'`,
	)
	deleteCapture.ExecutionMode = ExecutionReadOnly
	deleteCapture.MutationImageCapture = true
	deleted := exec(t, conn, deleteCapture)
	if len(deleted.Columns) != 3 || len(deleted.Rows) != 1 ||
		len(deleted.Rows[0]) != 3 {
		t.Fatalf("DELETE capture shape = %+v", deleted)
	}
	deleteRow := deleted.Rows[0]
	if !bytes.Equal(deleteRow[0].Bytes, row[0].Bytes) || deleteRow[0].Null ||
		deleteRow[1].Null || string(deleteRow[1].Bytes) != before ||
		!deleteRow[2].Null || len(deleteRow[2].Bytes) != 0 {
		t.Fatalf(
			"DELETE capture = key:%x before:%s/null=%v after:%s/null=%v",
			deleteRow[0].Bytes, deleteRow[1].Bytes, deleteRow[1].Null,
			deleteRow[2].Bytes, deleteRow[2].Null,
		)
	}
	if got := cellText(t, exec(t, conn, read), 0, 0); got != before {
		t.Fatalf("DELETE capture published %s, want original %s", got, before)
	}
}
