package driver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibejson"
)

type capturedMutationImage struct {
	key    []byte
	before []byte
	after  []byte
}

func TestMutationImageCaptureEvaluatesComputedUpdateOnceAndPublishesNothing(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	t.Cleanup(func() {
		_ = session.Close()
		_ = database.Close()
	})

	for _, statement := range []string{
		`CREATE TABLE mutation_images (` +
			`id STRING PRIMARY KEY, n INTEGER NOT NULL, mirror INTEGER NOT NULL)`,
		`INSERT INTO mutation_images VALUES ` +
			`('{"mirror":0,"n":10,"id":"a"}'), ` +
			`('{"n":20,"id":"b","mirror":0}')`,
	} {
		if err := testRuntimeExec(session, statement, nil); err != nil {
			t.Fatal(err)
		}
	}

	capture := runtimePrepare(t, session, `
		UPDATE mutation_images
		SET n = n + ?, mirror = n
		WHERE id IN ('a', 'b')
		ORDER BY id LIMIT 2`)
	delta := int64(1)
	var images []capturedMutationImage
	if err := capture.CaptureMutationImagesInto(
		ctx, []any{&delta},
		func(key, before, after []byte) error {
			images = append(images, capturedMutationImage{
				key:    append([]byte(nil), key...),
				before: append([]byte(nil), before...),
				after:  append([]byte(nil), after...),
			})
			// Every postimage must already be staged. If capture evaluated a
			// later row during callbacks, this mutation would leak into it.
			delta = 100
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if len(images) != 2 || len(images[0].key) == 0 || len(images[1].key) == 0 {
		t.Fatalf("captured images = %+v, want two keyed rows", images)
	}

	wantBefore := [][]byte{
		canonicalCaptureTestDocument(t, []byte(`{"mirror":0,"n":10,"id":"a"}`)),
		canonicalCaptureTestDocument(t, []byte(`{"n":20,"id":"b","mirror":0}`)),
	}
	wantAfter := [][]byte{
		canonicalCaptureTestDocument(t, []byte(`{"id":"a","n":11,"mirror":10}`)),
		canonicalCaptureTestDocument(t, []byte(`{"id":"b","n":21,"mirror":20}`)),
	}
	for i := range images {
		if !bytes.Equal(images[i].before, wantBefore[i]) ||
			!bytes.Equal(images[i].after, wantAfter[i]) {
			t.Fatalf(
				"image %d = before:%s after:%s, want before:%s after:%s",
				i, images[i].before, images[i].after,
				wantBefore[i], wantAfter[i],
			)
		}
	}

	var persisted [][]byte
	_, complete, err := session.ScanDocumentsAfter(
		ctx, "mutation_images", nil, 8, 1<<20,
		func(_ []byte, document []byte) error {
			persisted = append(persisted, append([]byte(nil), document...))
			return nil
		},
	)
	if err != nil || !complete || len(persisted) != len(wantBefore) {
		t.Fatalf(
			"persisted scan = complete:%v rows:%d err:%v",
			complete, len(persisted), err,
		)
	}
	for i := range persisted {
		if !bytes.Equal(persisted[i], wantBefore[i]) {
			t.Fatalf("capture published row %d: got %s want %s", i, persisted[i], wantBefore[i])
		}
	}
}

func TestMutationImageCaptureCanonicalizesWholeDocumentAndDeleteHasNoAfter(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	t.Cleanup(func() {
		_ = session.Close()
		_ = database.Close()
	})

	if err := testRuntimeExec(session,
		`CREATE TABLE canonical_images (`+
			`id STRING PRIMARY KEY, label STRING NOT NULL, n INTEGER NOT NULL)`,
		nil,
	); err != nil {
		t.Fatal(err)
	}
	const beforeText = `{"n":1,"id":"a","label":"old"}`
	if err := testRuntimeExec(session,
		`INSERT INTO canonical_images VALUES (?)`, []any{beforeText},
	); err != nil {
		t.Fatal(err)
	}

	rawAfter := []byte(" { \"label\" : \"line\u2028separator\", \"n\" : 7, \"id\" : \"a\" } ")
	wantAfter := canonicalCaptureTestDocument(t, rawAfter)
	update := runtimePrepare(t, session,
		`UPDATE canonical_images SET "$doc" = ? WHERE id = 'a'`)
	visits := 0
	if err := update.CaptureMutationImagesInto(
		ctx, []any{rawAfter},
		func(_ []byte, before, after []byte) error {
			visits++
			if !bytes.Equal(before,
				canonicalCaptureTestDocument(t, []byte(beforeText))) {
				t.Fatalf("whole-document before = %s", before)
			}
			if !bytes.Equal(after, wantAfter) || bytes.Equal(after, rawAfter) {
				t.Fatalf("whole-document after = %q, want canonical %q", after, wantAfter)
			}
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if visits != 1 {
		t.Fatalf("whole-document visits = %d, want 1", visits)
	}

	deleteStatement := runtimePrepare(t, session,
		`DELETE FROM canonical_images WHERE id = 'a'`)
	visits = 0
	if err := deleteStatement.CaptureMutationImagesInto(
		ctx, nil,
		func(_ []byte, before, after []byte) error {
			visits++
			if len(before) == 0 || after != nil {
				t.Fatalf("DELETE images = before:%s after:%v", before, after)
			}
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if visits != 1 {
		t.Fatalf("DELETE visits = %d, want 1", visits)
	}

	var persisted [][]byte
	_, complete, err := session.ScanDocumentsAfter(
		ctx, "canonical_images", nil, 2, 1<<20,
		func(_ []byte, document []byte) error {
			persisted = append(persisted, append([]byte(nil), document...))
			return nil
		},
	)
	if err != nil || !complete || len(persisted) != 1 ||
		!bytes.Equal(persisted[0], canonicalCaptureTestDocument(
			t, []byte(beforeText),
		)) {
		t.Fatalf("capture publication = rows:%q complete:%v err:%v", persisted, complete, err)
	}
}

func TestMutationImageCapturePreflightsCompleteBatchBeforeCallbacks(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	t.Cleanup(func() {
		_ = session.Close()
		_ = database.Close()
	})

	for _, statement := range []string{
		`CREATE TABLE preflight_images (` +
			`id STRING PRIMARY KEY, n INTEGER NOT NULL, divisor INTEGER NOT NULL, ` +
			`label STRING NOT NULL)`,
		`CREATE UNIQUE INDEX preflight_label ON preflight_images (label)`,
		`INSERT INTO preflight_images VALUES ` +
			`('{"id":"a","n":4,"divisor":2,"label":"a"}'), ` +
			`('{"id":"b","n":8,"divisor":0,"label":"b"}')`,
	} {
		if err := testRuntimeExec(session, statement, nil); err != nil {
			t.Fatal(err)
		}
	}

	division := runtimePrepare(t, session, `
		UPDATE preflight_images SET n = 16 / divisor
		ORDER BY id LIMIT 2`)
	visits := 0
	err := division.CaptureMutationImagesInto(
		ctx, nil, func(_, _, _ []byte) error {
			visits++
			return nil
		},
	)
	if !errors.Is(err, query.ErrScalarDivisionByZero) || visits != 0 {
		t.Fatalf("later expression failure = %v, visits=%d", err, visits)
	}

	unique := runtimePrepare(t, session, `
		UPDATE preflight_images SET label = 'same'
		ORDER BY id LIMIT 2`)
	visits = 0
	err = unique.CaptureMutationImagesInto(
		ctx, nil, func(_, _, _ []byte) error {
			visits++
			return nil
		},
	)
	if !errors.Is(err, ErrUniqueConstraint) || visits != 0 {
		t.Fatalf("unique postimage failure = %v, visits=%d", err, visits)
	}

	var persisted [][]byte
	_, complete, scanErr := session.ScanDocumentsAfter(
		ctx, "preflight_images", nil, 4, 1<<20,
		func(_ []byte, document []byte) error {
			persisted = append(persisted, append([]byte(nil), document...))
			return nil
		},
	)
	if scanErr != nil || !complete || len(persisted) != 2 {
		t.Fatalf("post-error scan = rows:%q complete:%v err:%v", persisted, complete, scanErr)
	}
	for i, label := range []string{`"label":"a"`, `"label":"b"`} {
		if !bytes.Contains(persisted[i], []byte(label)) {
			t.Fatalf("capture published row %d: %s", i, persisted[i])
		}
	}
}

func TestPrimaryDocumentDigestAcceptsCanonicalStagedOverlay(t *testing.T) {
	ctx := context.Background()
	database, session := openRuntimeSession(t)
	t.Cleanup(func() {
		_ = session.Close()
		_ = database.Close()
	})

	for _, statement := range []string{
		`CREATE TABLE digest_images (` +
			`id STRING PRIMARY KEY, label STRING NOT NULL, n INTEGER NOT NULL)`,
		`INSERT INTO digest_images VALUES (` +
			`'{"id":"a","label":"old","n":1}')`,
	} {
		if err := testRuntimeExec(session, statement, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := session.Begin(ctx, TxOptions{Isolation: IsolationSerializable}); err != nil {
		t.Fatal(err)
	}
	defer session.Rollback(ctx) //nolint:errcheck

	rawAfter := []byte(" { \"n\" : 7, \"label\" : \"line\u2028separator\", \"id\" : \"a\" } ")
	update := runtimePrepare(t, session,
		`UPDATE digest_images SET "$doc" = ? WHERE id = 'a'`)
	if _, err := update.Exec(ctx, []any{rawAfter}); err != nil {
		t.Fatal(err)
	}
	key, err := primaryScalarKey("a")
	if err != nil {
		t.Fatal(err)
	}
	state := session.conn.tx.tables["digest_images"]
	visible, found, err := state.appendRaw(nil, key)
	if err != nil || !found {
		t.Fatalf("staged overlay = %s, found:%v err:%v", visible, found, err)
	}
	canonical := canonicalCaptureTestDocument(t, rawAfter)
	if bytes.Equal(visible, canonical) {
		t.Fatalf("test requires a noncanonical staged overlay, got %q", visible)
	}

	checks := [][sha256.Size]byte{
		sha256.Sum256(visible),
		sha256.Sum256(canonical),
	}
	for i := range checks {
		if err := session.CheckPrimaryDocumentDigests(
			ctx, "digest_images", []byte("/id"),
			[][]byte{[]byte(key)}, checks[i:i+1],
		); err != nil {
			t.Fatalf("digest check %d = %v", i, err)
		}
	}
	wrong := [][sha256.Size]byte{sha256.Sum256([]byte(`{"id":"a","label":"wrong","n":7}`))}
	if err := session.CheckPrimaryDocumentDigests(
		ctx, "digest_images", []byte("/id"),
		[][]byte{[]byte(key)}, wrong,
	); !errors.Is(err, ErrDistributedTransactionConflict) {
		t.Fatalf("wrong canonical digest = %v", err)
	}
}

func canonicalCaptureTestDocument(t testing.TB, document []byte) []byte {
	t.Helper()
	canonical, err := vibejson.AppendCanonicalize(nil, document)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}
