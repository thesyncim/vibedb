package driver

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"

	"github.com/thesyncim/vibedb/internal/schemachange"
	"github.com/thesyncim/vibedb/query"
	sqlast "github.com/thesyncim/vibedb/sql"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

// Cold-path fault seam; never consulted by source mutation/apply.
var schemaDDLShadowFaultHook func(string) error

func schemaDDLShadowFault(stage string) error {
	if schemaDDLShadowFaultHook != nil {
		return schemaDDLShadowFaultHook(stage)
	}
	return nil
}

func validateSchemaDDLShadowRecord(r schemaDDLShadowRecord) error {
	c, s := r.Capture, r.Shadow
	if r.Version != 1 || r.SQL == "" || len(r.SQL) > ReplicatedChildSchemaMaxBytes || r.SourceDigest == [32]byte{} ||
		r.SourceGeneration == 0 || r.SourceGeneration == ^uint64(0) || s.Operation == [32]byte{} || s.NoOp ||
		c.Operation != s.Operation || c.PlanDigest != schemaDDLShadowPlan(r.SQL, r.SourceDigest) ||
		c.Validate() != nil || c.SchemaGeneration != r.SourceGeneration {
		return ErrReplicatedSchemaDDLConflict
	}
	statement, err := query.PrepareDML(r.SQL)
	if err != nil {
		return errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	defer statement.Release()
	kind := statement.Tree().Kind
	if kind != sqlast.KindAlterTable && kind != sqlast.KindCreateIndex && kind != sqlast.KindDropIndex && kind != sqlast.KindTruncate ||
		r.Truncate != (kind == sqlast.KindTruncate) {
		return ErrReplicatedSchemaDDLConflict
	}
	for _, cursor := range []schemachange.Cursor{s.Snapshot, s.Cursor} {
		p := cursor.Publication
		if cursor.Digest == [32]byte{} || p.Applied == 0 || p.Applied == ^uint64(0) || p.Term == 0 ||
			p.EntryDigest == [32]byte{} || p.DataDigest == [32]byte{} || p.Ownership == 0 || p.Route == 0 || p.Routing == 0 {
			return ErrReplicatedSchemaDDLConflict
		}
	}
	if s.Cursor.Publication.Applied < s.Snapshot.Publication.Applied ||
		s.Cursor.Publication.Applied == s.Snapshot.Publication.Applied && s.Cursor != s.Snapshot ||
		s.Cursor.Publication.Term < s.Snapshot.Publication.Term ||
		s.Cursor.Publication.Ownership != s.Snapshot.Publication.Ownership ||
		s.Cursor.Publication.Route != s.Snapshot.Publication.Route || s.Cursor.Publication.Routing != s.Snapshot.Publication.Routing ||
		!r.Ready && s.Cursor != s.Snapshot {
		return ErrReplicatedSchemaDDLConflict
	}
	image, err := ValidateReplicatedSchemaCatalogImage(s.Catalog)
	if err != nil || image.SchemaGeneration != r.SourceGeneration+1 {
		return errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	return nil
}

func readSchemaDDLShadowRecord(root *os.Root) (schemaDDLShadowRecord, bool, error) {
	var record schemaDDLShadowRecord
	file, err := openSchemaDDLRegular(root, schemaDDLShadowName, os.O_RDONLY)
	if os.IsNotExist(err) {
		return record, false, nil
	}
	if err != nil {
		return record, false, err
	}
	raw, err := io.ReadAll(io.LimitReader(file, schemaDDLJournalMaxBytes+1))
	if err == nil {
		err = file.Sync()
	}
	err = errors.Join(err, file.Close())
	if err != nil {
		return record, false, err
	}
	if len(raw) <= sha256.Size || len(raw) > schemaDDLJournalMaxBytes {
		return record, false, ErrReplicatedSchemaDDLConflict
	}
	h := sha256.New()
	_, _ = h.Write(schemaDDLShadowDomain)
	_, _ = h.Write(raw[sha256.Size:])
	if !bytes.Equal(raw[:sha256.Size], h.Sum(nil)) {
		return record, false, ErrReplicatedSchemaDDLConflict
	}
	if err := vibejson.Unmarshal(raw[sha256.Size:], &record); err != nil {
		return record, false, err
	}
	if err := validateSchemaDDLShadowRecord(record); err != nil {
		return record, false, err
	}
	canonical, err := vibejson.Marshal(&record)
	if err != nil || !bytes.Equal(canonical, raw[sha256.Size:]) {
		return record, false, errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	if err := syncSchemaNamespaceDirectory(root, "."); err != nil {
		return record, false, err
	}
	return record, true, nil
}

func writeSchemaDDLShadowRecord(root *os.Root, record schemaDDLShadowRecord) error {
	if err := validateSchemaDDLShadowRecord(record); err != nil {
		return err
	}
	body, err := vibejson.Marshal(&record)
	if err != nil {
		return err
	}
	if len(body)+sha256.Size > schemaDDLJournalMaxBytes {
		return ErrReplicatedSchemaDDLConflict
	}
	h := sha256.New()
	_, _ = h.Write(schemaDDLShadowDomain)
	_, _ = h.Write(body)
	raw := append(h.Sum(nil), body...)
	const pending = schemaDDLShadowName + ".tmp"
	if err := root.Remove(pending); err != nil && !os.IsNotExist(err) {
		return err
	}
	file, err := openSchemaDDLRegular(root, pending, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
	if err == nil {
		var written int
		written, err = file.Write(raw)
		if err == nil && written != len(raw) {
			err = io.ErrShortWrite
		}
	}
	if err == nil {
		err = file.Sync()
	}
	if file != nil {
		err = errors.Join(err, file.Close())
	}
	if err == nil {
		err = root.Rename(pending, schemaDDLShadowName)
	}
	if err == nil {
		err = syncSchemaNamespaceDirectory(root, ".")
	}
	if err != nil {
		return errors.Join(durable.ErrCommitOutcomeUnknown, err)
	}
	return nil
}

func rejectMutableSchemaShadow(directory string) error {
	root, err := os.OpenRoot(directory)
	if err != nil {
		return err
	}
	defer root.Close()
	_, found, err := readSchemaDDLShadowRecord(root)
	if err != nil || found {
		return errors.Join(err, ErrReplicatedSchemaDDLConflict)
	}
	return nil
}

// ObserveReplicatedSchemaDDLShadow reports whether the one retained online
// build slot belongs to operation. It is a read-only dispatch hint; finalizing
// still reauthenticates the complete shadow record and capture stream.
func (a *ReplicatedApply) ObserveReplicatedSchemaDDLShadow(operation [32]byte) (bool, error) {
	if a == nil || a.database == nil || operation == ([32]byte{}) {
		return false, ErrReplicatedApplyClosed
	}
	a.database.mu.RLock()
	err := a.checkLocked()
	directory := a.database.dataDir
	a.database.mu.RUnlock()
	if err != nil {
		return false, err
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return false, err
	}
	defer root.Close()
	record, found, err := readSchemaDDLShadowRecord(root)
	if err != nil || !found {
		return false, err
	}
	if record.Shadow.Operation != operation {
		return false, ErrReplicatedSchemaDDLConflict
	}
	return true, nil
}
