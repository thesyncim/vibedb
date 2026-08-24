package driver

import (
	"bytes"
	"context"
	sqldriver "database/sql/driver"
	"errors"
	"fmt"
	"strconv"

	"github.com/thesyncim/vibedb/store/durable"
	vibejson "github.com/thesyncim/vibejson"
	jsondoc "github.com/thesyncim/vibejson/document"
	"github.com/thesyncim/vibejson/x/byteview"
)

var (
	// ErrGlobalIndexUniqueConflict reports two base locators claiming the same
	// key in one globally unique index incarnation.
	ErrGlobalIndexUniqueConflict = errors.New("vibedb: global unique index key is already claimed")
	// ErrGlobalIndexFence reports a command whose index ID/incarnation does not
	// match the shard-local relation marker or the replicated relation manifest.
	// It fences delayed gateways and physical-relation reuse without a per-entry
	// identity tax.
	ErrGlobalIndexFence = errors.New("vibedb: global index relation incarnation conflicts")
	// ErrGlobalIndexRelation reports a physical relation that is not suitable
	// for raw canonical index keys and compact locator-array documents.
	ErrGlobalIndexRelation = errors.New("vibedb: invalid global index relation")
	// ErrGlobalIndexLookupTooLarge reports a raw index lookup that crossed its
	// caller-supplied row or locator-byte cap. Results are never truncated:
	// callers either observe the complete key prefix or retry with a larger cap.
	ErrGlobalIndexLookupTooLarge = errors.New("vibedb: global index lookup exceeds result bound")
)

const globalIndexMarkerKey = "\x00"

func appendGlobalIndexMarker(dst []byte, indexID, incarnation uint64) []byte {
	dst = append(dst, '[')
	dst = strconv.AppendUint(dst, indexID, 10)
	dst = append(dst, ',')
	dst = strconv.AppendUint(dst, incarnation, 10)
	return append(dst, ']')
}

// ApplyGlobalIndexMutation stages one byte-native global-index entry in the
// active SQL transaction. The caller has already admitted the containing
// distributed participant by virtual bucket. This method still validates the
// relation incarnation and exact locator shape and participates in the SQL
// runtime's serializable conflict validation before publication.
//
// entryKey must be a canonical tuple and therefore cannot begin with the
// reserved zero tag. value is a compact JSON array of string/exact-number base
// locator scalars. PUT is idempotent for the same locator. A unique PUT rejects
// a different locator; DELETE compares its expected locator so a stale delete
// can never remove a newer claim.
func (s *Session) ApplyGlobalIndexMutation(
	ctx context.Context,
	relation string,
	indexID, incarnation uint64,
	entryKey, value []byte,
	locatorCount uint8,
	unique, remove bool,
) (changed bool, err error) {
	if err := s.ready(ctx); err != nil {
		return false, err
	}
	if s.state != SessionInTransaction || s.conn.tx == nil {
		return false, ErrNoTransaction
	}
	if s.conn.tx.readOnly {
		return false, s.fail(ErrReadOnlyTransaction)
	}
	if relation == "" || indexID == 0 || incarnation == 0 ||
		len(entryKey) == 0 || entryKey[0] == 0 ||
		locatorCount == 0 || locatorCount > 8 {
		return false, s.fail(ErrGlobalIndexRelation)
	}
	tx := s.conn.tx
	state := tx.tables[relation]
	if state == nil {
		return false, s.fail(fmt.Errorf("%w: %q", ErrTableNotFound, relation))
	}
	if state.schema != nil || len(state.incarnation.meta.Indexes) != 0 {
		return false, s.fail(fmt.Errorf(
			"%w: relation %q must be schemaless and locally unindexed",
			ErrGlobalIndexRelation, relation,
		))
	}
	if len(entryKey) > state.limits.MaxKeyBytes {
		return false, s.fail(durable.ErrKeyTooLarge)
	}
	if err := validateGlobalIndexLocator(value, locatorCount); err != nil {
		return false, s.fail(err)
	}

	var markerStorage [43]byte
	marker := appendGlobalIndexMarker(markerStorage[:0], indexID, incarnation)

	stagedStorage := [2]stagedTxMutation{}
	staged := stagedStorage[:0]
	tx.trackSerializablePointRead(state, globalIndexMarkerKey)
	scratch, markerFound, lookupErr := state.appendRaw(
		s.conn.pointRaw[:0], globalIndexMarkerKey,
	)
	if lookupErr != nil {
		s.conn.pointRaw = scratch[:0]
		return false, s.fail(lookupErr)
	}
	if markerFound {
		if !bytes.Equal(scratch, marker) {
			s.conn.pointRaw = scratch[:0]
			return false, s.fail(fmt.Errorf(
				"%w: relation %q", ErrGlobalIndexFence, relation,
			))
		}
	} else {
		staged = append(staged, stagedTxMutation{
			key: globalIndexMarkerKey, document: marker, existed: false,
		})
	}
	s.conn.pointRaw = scratch[:0]

	entryKeyString := byteview.String(entryKey)
	tx.trackSerializablePointRead(state, entryKeyString)
	scratch, found, lookupErr := state.appendRaw(s.conn.pointRaw[:0], entryKeyString)
	if lookupErr != nil {
		s.conn.pointRaw = scratch[:0]
		return false, s.fail(lookupErr)
	}
	if found {
		equal := globalIndexLocatorsEqual(scratch, value, locatorCount)
		if remove {
			if !equal {
				s.conn.pointRaw = scratch[:0]
				return false, s.fail(fmt.Errorf(
					"%w: stale delete for relation %q", ErrTransactionConflict, relation,
				))
			}
			staged = append(staged, stagedTxMutation{
				key: entryKeyString, remove: true, existed: true,
			})
		} else if equal {
			// Exact placement identity is already present. Retain the original
			// spelling and avoid a write-amplifying replacement.
			s.conn.pointRaw = scratch[:0]
			if len(staged) == 0 {
				return false, nil
			}
		} else if unique {
			s.conn.pointRaw = scratch[:0]
			return false, s.fail(fmt.Errorf(
				"%w: relation %q: %w",
				ErrTransactionConflict, relation, ErrGlobalIndexUniqueConflict,
			))
		} else {
			s.conn.pointRaw = scratch[:0]
			return false, s.fail(fmt.Errorf(
				"%w: non-unique entry key collision in relation %q",
				ErrTransactionConflict, relation,
			))
		}
	} else if !remove {
		staged = append(staged, stagedTxMutation{
			key: entryKeyString, document: value, existed: false,
		})
	}
	s.conn.pointRaw = scratch[:0]
	if len(staged) == 0 {
		return false, nil
	}
	if err := admitGlobalIndexStaged(state, relation, staged); err != nil {
		return false, s.fail(err)
	}
	if err := contextCheckpoint(ctx); err != nil {
		return false, s.fail(err)
	}
	tx.applyResolvedMutations(state, staged)
	return true, nil
}

// LookupGlobalIndex reads one complete key from a gateway-maintained global
// index relation at a single durable snapshot. A unique index performs one
// point probe. A non-unique index seeks to keyTuple in the ordered primary
// graph and visits only its contiguous locator suffix range.
//
// Each locator passed to visit borrows snapshot or session scratch and is valid
// only until visit returns. maxRows and maxBytes are hard result bounds; -1 is
// the explicit unlimited value. A bound crossing fails the whole lookup rather
// than returning an ambiguous truncated locator set.
func (s *Session) LookupGlobalIndex(
	ctx context.Context,
	relation string,
	indexID, incarnation uint64,
	keyTuple []byte,
	locatorCount uint8,
	unique bool,
	maxRows int,
	maxBytes int64,
	visit func(locator []byte) error,
) error {
	keys := [...][]byte{keyTuple}
	return s.LookupGlobalIndexKeys(
		ctx, relation, indexID, incarnation, keys[:], locatorCount,
		unique, maxRows, maxBytes, visit,
	)
}

// LookupGlobalIndexKeys reads a strictly ordered, deduplicated finite key set
// from one gateway-maintained global index relation at one durable snapshot.
// Incarnation validation (legacy marker or replicated manifest), snapshot
// acquisition, cancellation, and result bounds are shared across the batch.
// This is the finite-domain locator-projection lane;
// it avoids one local snapshot and one network request per IN-list element.
func (s *Session) LookupGlobalIndexKeys(
	ctx context.Context,
	relation string,
	indexID, incarnation uint64,
	keyTuples [][]byte,
	locatorCount uint8,
	unique bool,
	maxRows int,
	maxBytes int64,
	visit func(locator []byte) error,
) error {
	if err := s.ready(ctx); err != nil {
		return err
	}
	if s.state != SessionIdle {
		return ErrTransactionActive
	}
	if relation == "" || indexID == 0 || incarnation == 0 ||
		len(keyTuples) == 0 ||
		locatorCount == 0 || locatorCount > 8 ||
		maxRows < -1 || maxBytes < -1 || visit == nil {
		return ErrGlobalIndexRelation
	}
	for i := range keyTuples {
		if len(keyTuples[i]) == 0 || keyTuples[i][0] == 0 ||
			i != 0 && bytes.Compare(keyTuples[i-1], keyTuples[i]) >= 0 {
			return ErrGlobalIndexRelation
		}
	}

	core := s.conn.db
	if err := rlockContext(ctx, &core.mu); err != nil {
		return err
	}
	table := core.tables[relation]
	if core.closed {
		core.mu.RUnlock()
		return sqldriver.ErrBadConn
	}
	if table == nil {
		core.mu.RUnlock()
		return fmt.Errorf("%w: %q", ErrTableNotFound, relation)
	}
	if table.schema != nil || len(table.meta.Indexes) != 0 {
		core.mu.RUnlock()
		return fmt.Errorf(
			"%w: relation %q must be schemaless and locally unindexed",
			ErrGlobalIndexRelation, relation,
		)
	}
	replicatedRelation, replicated := replicatedGlobalIndexRelation(
		core.catalog.ReplicatedShardStore, relation,
	)
	if replicated && replicatedRelation.Kind != ReplicatedShardRelationGlobalIndex {
		core.mu.RUnlock()
		return fmt.Errorf("%w: relation %q", ErrGlobalIndexRelation, relation)
	}
	if replicated && (replicatedRelation.IndexID != indexID ||
		replicatedRelation.Incarnation != incarnation ||
		replicatedRelation.LocatorCount != locatorCount ||
		replicatedRelation.Unique != unique) {
		core.mu.RUnlock()
		return fmt.Errorf("%w: relation %q", ErrGlobalIndexFence, relation)
	}
	// A freshly provisioned physical index relation has no durable collection
	// until its first claim is written. Under the shard-level read fence this is
	// an exact empty answer: a concurrent first writer cannot publish until the
	// fence is released. There is no marker to validate yet, but there is also
	// no data an older incarnation could expose.
	if table.collection == nil {
		core.mu.RUnlock()
		return nil
	}
	limits, limitsErr := tableMutationLimits(table)
	if limitsErr != nil {
		core.mu.RUnlock()
		return limitsErr
	}
	for i := range keyTuples {
		if len(keyTuples[i]) > limits.MaxKeyBytes {
			core.mu.RUnlock()
			return durable.ErrKeyTooLarge
		}
	}
	snapshot, snapshotErr := table.collection.Snapshot()
	core.mu.RUnlock()
	if snapshotErr != nil {
		return snapshotErr
	}
	defer snapshot.Close()

	var scratch []byte
	var found bool
	var err error
	if !replicated {
		var markerStorage [43]byte
		marker := appendGlobalIndexMarker(markerStorage[:0], indexID, incarnation)
		scratch, found, err = snapshot.AppendRaw(
			s.conn.pointRaw[:0], []byte(globalIndexMarkerKey),
		)
		if err != nil {
			s.conn.pointRaw = scratch[:0]
			return err
		}
		if !found || !bytes.Equal(scratch, marker) {
			s.conn.pointRaw = scratch[:0]
			return fmt.Errorf("%w: relation %q", ErrGlobalIndexFence, relation)
		}
		s.conn.pointRaw = scratch[:0]
	}

	rows := 0
	var resultBytes int64
	emit := func(locator []byte) error {
		if rows&63 == 0 {
			if checkpointErr := contextCheckpoint(ctx); checkpointErr != nil {
				return checkpointErr
			}
		}
		if err := validateGlobalIndexLocator(locator, locatorCount); err != nil {
			return err
		}
		if maxRows != -1 && rows >= maxRows {
			return fmt.Errorf(
				"%w: rows exceed %d", ErrGlobalIndexLookupTooLarge, maxRows,
			)
		}
		locatorBytes := int64(len(locator))
		if maxBytes != -1 && locatorBytes > maxBytes-resultBytes {
			return fmt.Errorf(
				"%w: locator bytes exceed %d", ErrGlobalIndexLookupTooLarge, maxBytes,
			)
		}
		if err := visit(locator); err != nil {
			return err
		}
		rows++
		resultBytes += locatorBytes
		return nil
	}

	for i := range keyTuples {
		keyTuple := keyTuples[i]
		if unique {
			scratch, found, err = snapshot.AppendRaw(s.conn.pointRaw[:0], keyTuple)
			if err == nil && found {
				err = emit(scratch)
			}
			s.conn.pointRaw = scratch[:0]
		} else {
			err = snapshot.RangePrefixRaw(keyTuple, func(_ []byte, locator []byte) error {
				return emit(locator)
			})
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// replicatedGlobalIndexRelation resolves only cold, authenticated catalog
// identity. Replicated command/apply paths never call this helper: they resolve
// dense relation IDs directly to pre-opened handles. A catalog-bound global
// relation carries its incarnation fence once in the manifest, so reads avoid
// the legacy marker point probe and its per-relation storage tax.
func replicatedGlobalIndexRelation(
	identity *ReplicatedShardStoreIdentity,
	table string,
) (ReplicatedShardRelationIdentity, bool) {
	if identity == nil || identity.RelationCount < 2 || table == "" {
		return ReplicatedShardRelationIdentity{}, false
	}
	for ordinal := 1; ordinal < int(identity.RelationCount); ordinal++ {
		relation := identity.Relations[ordinal]
		if relation.Table == table {
			return relation, true
		}
	}
	return ReplicatedShardRelationIdentity{}, false
}

func admitGlobalIndexStaged(
	state *txTable,
	relation string,
	staged []stagedTxMutation,
) error {
	distinct := max(state.highWaterKeys, len(state.order))
	stagedBytes := max(state.highWaterBytes, state.stagedBytes)
	probeBytes := state.stagedBytes
	for i := range staged {
		mutation := &staged[i]
		if previous, present := state.pending[mutation.key]; present {
			probeBytes -= len(mutation.key) + len(previous.document)
		} else {
			distinct++
		}
		probeBytes += len(mutation.key) + len(mutation.document)
		stagedBytes = max(stagedBytes, probeBytes)
	}
	if distinct > state.limits.MaxBatchDocuments {
		return fmt.Errorf(
			"%w: global index relation %q would stage %d keys, limit %d: %w",
			ErrTransactionTooLarge, relation, distinct,
			state.limits.MaxBatchDocuments, durable.ErrBatchTooLarge,
		)
	}
	if stagedBytes > state.limits.MaxBatchBytes {
		return fmt.Errorf(
			"%w: global index relation %q would stage %d bytes, limit %d: %w",
			ErrTransactionTooLarge, relation, stagedBytes,
			state.limits.MaxBatchBytes, durable.ErrBatchTooLarge,
		)
	}
	return nil
}

func validateGlobalIndexLocator(value []byte, count uint8) error {
	if len(value) == 0 {
		return ErrGlobalIndexRelation
	}
	var entries [9]vibejson.IndexEntry
	index, err := vibejson.BuildIndex(value, entries[:])
	if err != nil {
		return fmt.Errorf("%w: locator JSON: %v", ErrGlobalIndexRelation, err)
	}
	root := index.Root()
	length, ok := root.ArrayLen()
	if !ok || length != int(count) {
		return fmt.Errorf(
			"%w: locator has %d values, want %d",
			ErrGlobalIndexRelation, length, count,
		)
	}
	for i := 0; i < length; i++ {
		node, _ := root.Index(i)
		if node.Kind() != jsondoc.String && node.Kind() != jsondoc.Number {
			return fmt.Errorf(
				"%w: locator value %d is not string or number",
				ErrGlobalIndexRelation, i,
			)
		}
	}
	return nil
}

func globalIndexLocatorsEqual(a, b []byte, count uint8) bool {
	var aEntries, bEntries [9]vibejson.IndexEntry
	aIndex, aErr := vibejson.BuildIndex(a, aEntries[:])
	bIndex, bErr := vibejson.BuildIndex(b, bEntries[:])
	if aErr != nil || bErr != nil {
		return false
	}
	aRoot, bRoot := aIndex.Root(), bIndex.Root()
	aLen, aOK := aRoot.ArrayLen()
	bLen, bOK := bRoot.ArrayLen()
	if !aOK || !bOK || aLen != int(count) || bLen != int(count) {
		return false
	}
	for i := 0; i < int(count); i++ {
		aNode, _ := aRoot.Index(i)
		bNode, _ := bRoot.Index(i)
		if aNode.Kind() != bNode.Kind() {
			return false
		}
		switch aNode.Kind() {
		case jsondoc.String:
			if !vibejson.RawJSONStringEqual(
				aNode.Raw().Bytes(), aNode.Entry.Flags(),
				bNode.Raw().Bytes(), bNode.Entry.Flags(),
			) {
				return false
			}
		case jsondoc.Number:
			if !vibejson.JSONNumberEqual(
				aNode.Raw().Bytes(), bNode.Raw().Bytes(),
			) {
				return false
			}
		default:
			return false
		}
	}
	return true
}
