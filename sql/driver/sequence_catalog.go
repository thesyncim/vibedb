package driver

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/thesyncim/vibedb/query"
	"github.com/thesyncim/vibedb/store/durable"
	"github.com/thesyncim/vibejson"
)

// defaultSequenceLogBatch matches PostgreSQL's SEQ_LOG_VALS: we persist the
// sequence's high-water mark at least 32 values ahead so nextval() only hits
// disk once per batch window instead of per call.
const defaultSequenceLogBatch int64 = 32

const maxSequenceStateFileBytes = 4 << 20

type runtimeSequence struct {
	mu           sync.Mutex
	name         string
	meta         sequenceMeta
	curValue     int64
	curCalled    bool
	logRemaining int64
	dropped      bool
}

type seqStateEntry struct {
	PersistedValue int64
	IsCalled       bool
	Generation     uint64
}

type seqStateFileVibe map[string]seqStateEntry

var seqStateEntryFields = vibejson.MakeFieldSet("persisted_value", "is_called", "generation")

var seqStateVibeDecoder = func() vibejson.Decoder[seqStateFileVibe] {
	dec, err := vibejson.CompileDecoder[seqStateFileVibe](vibejson.DecoderOptions{
		MaxDepth:      4,
		CaseSensitive: true,
		Replace:       true,
	})
	if err != nil {
		panic("driver: compile sequence state decoder: " + err.Error())
	}
	return dec
}()

var seqStateVibeEncoder = func() vibejson.Encoder[seqStateFileVibe] {
	enc, err := vibejson.CompileEncoder[seqStateFileVibe](vibejson.EncoderOptions{})
	if err != nil {
		panic("driver: compile sequence state encoder: " + err.Error())
	}
	return enc
}()

func (m *seqStateFileVibe) MarshalVibeJSON(w vibejson.TrustedAppender) vibejson.TrustedAppender {
	entries := map[string]seqStateEntry(*m)
	w = w.RawByteUnchecked('{')
	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	slices.Sort(names)
	for i, name := range names {
		if i != 0 {
			w = w.RawByteUnchecked(',')
		}
		e := entries[name]
		w = w.String(name).RawUnchecked(`:{"persisted_value":`).Int(e.PersistedValue)
		if e.IsCalled {
			w = w.RawUnchecked(`,"is_called":true`)
		}
		if e.Generation != 0 {
			w = w.RawUnchecked(`,"generation":`).Uint(e.Generation)
		}
		w = w.RawByteUnchecked('}')
	}
	return w.RawByteUnchecked('}')
}

func (m *seqStateFileVibe) UnmarshalVibeJSON(c vibejson.DecodeCursor) (vibejson.DecodeCursor, error) {
	if err := c.BeginObject("SQL sequence state"); err != nil {
		return c, errors.New("vibedb: SQL sequence state must be a JSON object")
	}
	decoded := make(map[string]seqStateEntry)
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return c, err
		}
		if !ok {
			break
		}
		if err := checkCatalogSequenceCount(len(decoded) + 1); err != nil {
			return c, err
		}
		var entry seqStateEntry
		if err := decodeSeqStateEntry(&c, &entry); err != nil {
			return c, fmt.Errorf("sequence %q state: %w", name, err)
		}
		decoded[strings.Clone(name)] = entry
	}
	*m = seqStateFileVibe(decoded)
	return c, nil
}

func decodeSeqStateEntry(c *vibejson.DecodeCursor, dst *seqStateEntry) error {
	if err := c.BeginObject("sequence state entry"); err != nil {
		return errors.New("vibedb: sequence state entry must be a JSON object")
	}
	var decoded seqStateEntry
	var seen uint64
	for first := true; ; first = false {
		name, ok, err := c.NextField(first)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		idx, known := seqStateEntryFields.Lookup(name, true)
		if !known {
			return unknownCatalogMember("sequence state", name)
		}
		if err := markCatalogField(&seen, idx, "sequence state", name); err != nil {
			return err
		}
		switch idx {
		case 0:
			err = c.Int(&decoded.PersistedValue)
		case 1:
			err = c.Bool(&decoded.IsCalled)
		case 2:
			err = c.Uint(&decoded.Generation)
		}
		if err != nil {
			return err
		}
	}
	*dst = decoded
	return nil
}

func (d *database) sequenceStatePath() string {
	return d.path + ".sequences"
}

func (d *database) loadSequenceCatalog() error {
	d.seqMu.Lock()
	defer d.seqMu.Unlock()
	d.seqPersistMu.Lock()
	defer d.seqPersistMu.Unlock()

	if d.sequences == nil {
		d.sequences = make(map[string]*runtimeSequence)
	}
	if d.seqPersisted == nil {
		d.seqPersisted = make(map[string]seqStateEntry)
	}

	var diskState map[string]seqStateEntry
	raw, err := os.ReadFile(d.sequenceStatePath())
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("vibedb: read SQL sequence state: %w", err)
		}
	} else if len(raw) > 0 {
		if len(raw) > maxSequenceStateFileBytes {
			return fmt.Errorf("vibedb: SQL sequence state file exceeds %d bytes", maxSequenceStateFileBytes)
		}
		var parsed seqStateFileVibe
		if err := seqStateVibeDecoder.Decode(raw, &parsed); err != nil {
			return fmt.Errorf("vibedb: decode SQL sequence state: %w", err)
		}
		diskState = map[string]seqStateEntry(parsed)
	}

	for name, meta := range d.catalog.Sequences {
		if meta == nil {
			return fmt.Errorf("vibedb: SQL sequence %q has null catalog metadata", name)
		}
		if err := validateSequenceMeta(name, meta); err != nil {
			return err
		}
		if meta.Generation > d.seqGen {
			d.seqGen = meta.Generation
		}
		curVal := meta.PersistedValue
		curCalled := meta.IsCalled
		if diskEntry, ok := diskState[name]; ok && diskEntry.Generation == meta.Generation {
			if diskEntry.PersistedValue >= meta.MinValue && diskEntry.PersistedValue <= meta.MaxValue {
				curVal = diskEntry.PersistedValue
				curCalled = diskEntry.IsCalled
			}
		}
		metaCopy := *meta
		metaCopy.PersistedValue = curVal
		metaCopy.IsCalled = curCalled
		d.sequences[name] = &runtimeSequence{
			name:         name,
			meta:         metaCopy,
			curValue:     curVal,
			curCalled:    curCalled,
			logRemaining: 0,
		}
		d.seqPersisted[name] = seqStateEntry{
			PersistedValue: curVal,
			IsCalled:       curCalled,
			Generation:     meta.Generation,
		}
	}
	return nil
}

func validateSequenceMeta(name string, meta *sequenceMeta) error {
	if meta.Increment == 0 {
		return fmt.Errorf("%w: sequence %q INCREMENT must not be zero", ErrInvalidSequenceDefinition, name)
	}
	if meta.MinValue >= meta.MaxValue {
		return fmt.Errorf("%w: sequence %q MINVALUE (%d) must be less than MAXVALUE (%d)", ErrInvalidSequenceDefinition, name, meta.MinValue, meta.MaxValue)
	}
	if meta.Start < meta.MinValue || meta.Start > meta.MaxValue {
		return fmt.Errorf("%w: sequence %q START (%d) must be between MINVALUE (%d) and MAXVALUE (%d)", ErrInvalidSequenceDefinition, name, meta.Start, meta.MinValue, meta.MaxValue)
	}
	if meta.Cache < 1 {
		return fmt.Errorf("%w: sequence %q CACHE (%d) must be greater than zero", ErrInvalidSequenceDefinition, name, meta.Cache)
	}
	if meta.PersistedValue < meta.MinValue || meta.PersistedValue > meta.MaxValue {
		return fmt.Errorf("%w: sequence %q persisted value (%d) out of bounds [%d, %d]", ErrSequenceValueOutOfRange, name, meta.PersistedValue, meta.MinValue, meta.MaxValue)
	}
	return nil
}

// persistSequenceStateLocked writes d.seqPersisted atomically via temp+fsync+rename+dirsync.
// Caller must hold d.seqPersistMu.
func (d *database) persistSequenceStateLocked() error {
	path := d.sequenceStatePath()
	if len(d.seqPersisted) == 0 {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	v := seqStateFileVibe(d.seqPersisted)
	raw, err := seqStateVibeEncoder.AppendJSON(make([]byte, 0, len(d.seqPersisted)*96+8), &v)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := replaceCatalogFile(tmpName, path); err != nil {
		return err
	}
	ok = true
	if err := d.directorySync(filepath.Dir(path)); err != nil {
		return fmt.Errorf("%w: sync SQL sequence state directory: %w", durable.ErrCommitOutcomeUnknown, err)
	}
	return nil
}

func (d *database) persistSequenceEntry(name string, persistedVal int64, isCalled bool, gen uint64) error {
	d.seqPersistMu.Lock()
	defer d.seqPersistMu.Unlock()
	if d.seqPersisted == nil {
		d.seqPersisted = make(map[string]seqStateEntry)
	}
	prev, hadPrev := d.seqPersisted[name]
	d.seqPersisted[name] = seqStateEntry{
		PersistedValue: persistedVal,
		IsCalled:       isCalled,
		Generation:     gen,
	}
	if err := d.persistSequenceStateLocked(); err != nil {
		if hadPrev {
			d.seqPersisted[name] = prev
		} else {
			delete(d.seqPersisted, name)
		}
		return err
	}
	return nil
}

func (d *database) deletePersistedSequenceEntries(names []string) error {
	if len(names) == 0 {
		return nil
	}
	d.seqPersistMu.Lock()
	defer d.seqPersistMu.Unlock()
	changed := false
	for _, name := range names {
		if _, ok := d.seqPersisted[name]; ok {
			delete(d.seqPersisted, name)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return d.persistSequenceStateLocked()
}

// stepSequenceValue advances cur by inc within [minVal, maxVal] with exact
// uint64 distance arithmetic so signed int64 overflow is impossible even at
// math.MinInt64 and math.MaxInt64.
func stepSequenceValue(name string, cur, inc, minVal, maxVal int64, cycle bool) (next int64, wrapped bool, err error) {
	if inc > 0 {
		remaining := uint64(maxVal) - uint64(cur)
		step := uint64(inc)
		if step <= remaining {
			return int64(uint64(cur) + step), false, nil
		}
		if cycle {
			return minVal, true, nil
		}
		return 0, false, fmt.Errorf("%w: nextval: reached maximum value of sequence %q (%d)", ErrSequenceExhausted, name, maxVal)
	}
	// inc < 0
	remaining := uint64(cur) - uint64(minVal)
	step := uint64(-(inc + 1)) + 1
	if step <= remaining {
		return int64(uint64(cur) - step), false, nil
	}
	if cycle {
		return maxVal, true, nil
	}
	return 0, false, fmt.Errorf("%w: nextval: reached minimum value of sequence %q (%d)", ErrSequenceExhausted, name, minVal)
}

func (d *database) nextSequenceValue(name string) (int64, error) {
	d.seqMu.RLock()
	seq, ok := d.sequences[name]
	if !ok {
		d.seqMu.RUnlock()
		return 0, fmt.Errorf("%w: %q", ErrSequenceNotFound, name)
	}
	seq.mu.Lock()
	d.seqMu.RUnlock()
	defer seq.mu.Unlock()

	if seq.dropped {
		return 0, fmt.Errorf("%w: %q", ErrSequenceNotFound, name)
	}

	// Fast path: serve directly from the pre-allocated durable window with
	// zero disk I/O and zero allocations.
	if seq.logRemaining > 0 {
		next, _, err := stepSequenceValue(
			seq.name, seq.curValue, seq.meta.Increment,
			seq.meta.MinValue, seq.meta.MaxValue, seq.meta.Cycle,
		)
		if err != nil {
			return 0, err
		}
		seq.curValue = next
		seq.curCalled = true
		seq.logRemaining--
		return next, nil
	}

	// Slow path: allocate a new durable high-water mark batch window.
	var first int64
	if !seq.curCalled {
		first = seq.curValue
	} else {
		var err error
		first, _, err = stepSequenceValue(
			seq.name, seq.curValue, seq.meta.Increment,
			seq.meta.MinValue, seq.meta.MaxValue, seq.meta.Cycle,
		)
		if err != nil {
			return 0, err
		}
	}

	batch := seq.meta.Cache
	if batch < defaultSequenceLogBatch {
		batch = defaultSequenceLogBatch
	}
	var hwm int64
	var allocated int64
	if seq.meta.Increment > 0 {
		remaining := uint64(seq.meta.MaxValue) - uint64(first)
		step := uint64(seq.meta.Increment)
		maxSteps := remaining / step
		wantSteps := uint64(batch - 1)
		if wantSteps > maxSteps {
			wantSteps = maxSteps
		}
		hwm = int64(uint64(first) + wantSteps*step)
		allocated = int64(wantSteps) + 1
	} else {
		remaining := uint64(first) - uint64(seq.meta.MinValue)
		step := uint64(-(seq.meta.Increment + 1)) + 1
		maxSteps := remaining / step
		wantSteps := uint64(batch - 1)
		if wantSteps > maxSteps {
			wantSteps = maxSteps
		}
		hwm = int64(uint64(first) - wantSteps*step)
		allocated = int64(wantSteps) + 1
	}

	if err := d.persistSequenceEntry(seq.name, hwm, true, seq.meta.Generation); err != nil {
		return 0, err
	}
	seq.meta.PersistedValue = hwm
	seq.meta.IsCalled = true
	seq.curValue = first
	seq.curCalled = true
	seq.logRemaining = allocated - 1
	return first, nil
}

func (d *database) setSequenceValue(name string, value int64, isCalled bool) (int64, error) {
	d.seqMu.RLock()
	seq, ok := d.sequences[name]
	if !ok {
		d.seqMu.RUnlock()
		return 0, fmt.Errorf("%w: %q", ErrSequenceNotFound, name)
	}
	seq.mu.Lock()
	d.seqMu.RUnlock()
	defer seq.mu.Unlock()

	if seq.dropped {
		return 0, fmt.Errorf("%w: %q", ErrSequenceNotFound, name)
	}
	if value < seq.meta.MinValue || value > seq.meta.MaxValue {
		return 0, fmt.Errorf(
			"%w: setval: value %d is out of bounds for sequence %q (%d..%d)",
			ErrSequenceValueOutOfRange, value, seq.name, seq.meta.MinValue, seq.meta.MaxValue,
		)
	}
	if err := d.persistSequenceEntry(seq.name, value, isCalled, seq.meta.Generation); err != nil {
		return 0, err
	}
	seq.meta.PersistedValue = value
	seq.meta.IsCalled = isCalled
	seq.curValue = value
	seq.curCalled = isCalled
	seq.logRemaining = 0
	return value, nil
}

func (d *database) sequenceExists(name string) bool {
	d.seqMu.RLock()
	seq, ok := d.sequences[name]
	if !ok {
		d.seqMu.RUnlock()
		return false
	}
	seq.mu.Lock()
	d.seqMu.RUnlock()
	alive := !seq.dropped
	seq.mu.Unlock()
	return alive
}

// connSeqSession tracks per-connection/per-session currval and lastval state
// and implements query.SequenceProvider.
type connSeqSession struct {
	mu      sync.Mutex
	db      *database
	currval map[string]int64
	hasLast bool
	lastval int64
}

var _ query.SequenceProvider = (*connSeqSession)(nil)

func (s *connSeqSession) NextVal(name string) (int64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("%w: %q", ErrSequenceNotFound, name)
	}
	val, err := s.db.nextSequenceValue(name)
	if err != nil {
		return 0, err
	}
	s.mu.Lock()
	if s.currval == nil {
		s.currval = make(map[string]int64, 4)
	}
	s.currval[name] = val
	s.hasLast = true
	s.lastval = val
	s.mu.Unlock()
	return val, nil
}

func (s *connSeqSession) CurrVal(name string) (int64, error) {
	if s == nil || s.db == nil || !s.db.sequenceExists(name) {
		return 0, fmt.Errorf("%w: %q", ErrSequenceNotFound, name)
	}
	s.mu.Lock()
	val, ok := s.currval[name]
	s.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("%w: currval of sequence %q is not yet defined in this session", ErrSequenceStateNotInitialized, name)
	}
	return val, nil
}

func (s *connSeqSession) SetVal(name string, value int64, isCalled bool) (int64, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("%w: %q", ErrSequenceNotFound, name)
	}
	val, err := s.db.setSequenceValue(name, value, isCalled)
	if err != nil {
		return 0, err
	}
	if isCalled {
		s.mu.Lock()
		if s.currval == nil {
			s.currval = make(map[string]int64, 4)
		}
		s.currval[name] = val
		s.hasLast = true
		s.lastval = val
		s.mu.Unlock()
	}
	return val, nil
}

func (s *connSeqSession) LastVal() (int64, error) {
	if s == nil {
		return 0, fmt.Errorf("%w: lastval is not yet defined in this session", ErrSequenceStateNotInitialized)
	}
	s.mu.Lock()
	hasLast := s.hasLast
	lastval := s.lastval
	s.mu.Unlock()
	if !hasLast {
		return 0, fmt.Errorf("%w: lastval is not yet defined in this session", ErrSequenceStateNotInitialized)
	}
	return lastval, nil
}

func sequenceTypeBounds(dataType string) (int64, int64) {
	switch dataType {
	case "SMALLINT":
		return math.MinInt16, math.MaxInt16
	case "INTEGER":
		return math.MinInt32, math.MaxInt32
	default:
		return math.MinInt64, math.MaxInt64
	}
}
