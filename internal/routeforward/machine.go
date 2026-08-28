package routeforward

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/thesyncim/vibedb/internal/routegate"
)

type retainedEntry struct {
	Entry
	State             EntryState
	PublishedRevision uint64
	ActiveRevision    uint64
	Certificate       Digest
}

type tombstone struct {
	CatalogGeneration uint64
	AuthorityEpoch    uint64
	PrunedRevision    uint64
	Certificate       Digest
}

// Status is a constant-size replicated control-plane cut.
type Status struct {
	Revision       uint64
	AuthorityEpoch uint64
	Live           uint64
	Tombstones     uint64
}

// Machine is the catalog-RF3 forwarding authority. It is deliberately not
// synchronized; the owning replicated state-machine lane serializes commands.
type Machine struct {
	authority      Digest
	authorityEpoch uint64
	revision       uint64
	live           uint64
	tombstones     uint64
	maxRecords     uint64
	entries        map[Digest]retainedEntry
	retired        map[Digest]tombstone
}

// NewMachine constructs one bounded authority. Revision one is the durable
// genesis cut, so every settlement—including a refusal—has a nonzero revision.
func NewMachine(authority Digest, authorityEpoch, maxRecords uint64) (*Machine, bool) {
	if authority == (Digest{}) || authorityEpoch == 0 || maxRecords == 0 ||
		maxRecords > MaxRetainedRecords {
		return nil, false
	}
	return &Machine{
		authority: authority, authorityEpoch: authorityEpoch, revision: 1,
		maxRecords: maxRecords,
		entries:    make(map[Digest]retainedEntry), retired: make(map[Digest]tombstone),
	}, true
}

// Status returns the fixed replicated head.
func (machine *Machine) Status() Status {
	if machine == nil {
		return Status{}
	}
	return Status{
		Revision: machine.revision, AuthorityEpoch: machine.authorityEpoch,
		Live: machine.live, Tombstones: machine.tombstones,
	}
}

// Entry returns one detached retained forwarding entry.
func (machine *Machine) Entry(key Digest) (Entry, EntryState, bool) {
	if machine == nil || key == (Digest{}) {
		return Entry{}, EntryInvalid, false
	}
	entry, ok := machine.entries[key]
	return entry.Entry, entry.State, ok
}

// Preview derives the exact transition without mutating maps and allocates no
// memory. Catalog storage can durably commit the encoded delta before Apply.
func (machine *Machine) Preview(command Command) Outcome {
	if machine == nil {
		return Outcome{}
	}
	preview := *machine
	return preview.apply(command, false)
}

// Apply performs one deterministic replicated transition.
func (machine *Machine) Apply(command Command) Outcome {
	return machine.apply(command, true)
}

func (machine *Machine) apply(command Command, write bool) Outcome {
	if machine == nil || !validCommand(command) {
		return Outcome{}
	}
	if command.Authority != machine.authority {
		return machine.refusal(command.Key, ReasonUnauthorized)
	}
	if command.AuthorityEpoch != machine.authorityEpoch {
		return machine.refusal(command.Key, ReasonStaleAuthority)
	}
	switch command.Operation {
	case OperationPublish:
		return machine.publish(command, write)
	case OperationActivate:
		return machine.activate(command, write)
	case OperationPrune:
		return machine.prune(command, write)
	case OperationCompactRetired:
		return machine.compactRetired(command, write)
	default:
		return machine.refusal(command.Key, ReasonInvalid)
	}
}

func (machine *Machine) publish(command Command, write bool) Outcome {
	if retired, found := machine.retired[command.Key]; found {
		return machine.outcome(
			command.Key, ReasonRetired, false, EntryInvalid, retired.Certificate,
		)
	}
	if existing, found := machine.entries[command.Key]; found {
		if existing.Entry == command.Entry {
			return machine.outcome(
				command.Key, ReasonIdempotent, false, existing.State,
				entryCertificate(command.Key, existing),
			)
		}
		return machine.refusal(command.Key, ReasonConflict)
	}
	if command.ExpectedRevision != machine.revision {
		return machine.refusal(command.Key, ReasonStaleRevision)
	}
	if machine.live+machine.tombstones >= machine.maxRecords || machine.revision == ^uint64(0) {
		return machine.refusal(command.Key, ReasonCapacity)
	}
	machine.revision++
	machine.live++
	retained := retainedEntry{
		Entry: command.Entry, State: EntryPrepared,
		PublishedRevision: machine.revision,
	}
	retained.Certificate = entryCertificate(command.Key, retained)
	if write {
		machine.entries[command.Key] = retained
	}
	return machine.outcome(
		command.Key, ReasonPublished, true, EntryPrepared,
		entryCertificate(command.Key, retained),
	)
}

func (machine *Machine) activate(command Command, write bool) Outcome {
	entry, found := machine.entries[command.Key]
	if !found {
		if retired, ok := machine.retired[command.Key]; ok {
			return machine.outcome(
				command.Key, ReasonRetired, false, EntryInvalid, retired.Certificate,
			)
		}
		return machine.refusal(command.Key, ReasonNotFound)
	}
	if entry.State == EntryActive {
		return machine.outcome(
			command.Key, ReasonIdempotent, false, EntryActive,
			entryCertificate(command.Key, entry),
		)
	}
	if command.ExpectedRevision != machine.revision {
		return machine.refusal(command.Key, ReasonStaleRevision)
	}
	if machine.revision == ^uint64(0) {
		return machine.refusal(command.Key, ReasonCapacity)
	}
	machine.revision++
	entry.State = EntryActive
	entry.ActiveRevision = machine.revision
	entry.Certificate = Digest{}
	entry.Certificate = entryCertificate(command.Key, entry)
	if write {
		machine.entries[command.Key] = entry
	}
	return machine.outcome(
		command.Key, ReasonActivated, true, EntryActive,
		entryCertificate(command.Key, entry),
	)
}

func (machine *Machine) prune(command Command, write bool) Outcome {
	if retired, found := machine.retired[command.Key]; found {
		return machine.outcome(
			command.Key, ReasonIdempotent, false, EntryInvalid, retired.Certificate,
		)
	}
	entry, found := machine.entries[command.Key]
	if !found {
		return machine.refusal(command.Key, ReasonNotFound)
	}
	if command.ExpectedRevision != machine.revision {
		return machine.refusal(command.Key, ReasonStaleRevision)
	}
	clearance := command.Clearance
	switch {
	case clearance.CatalogGeneration <= entry.Validity.RetainThroughCatalog:
		return machine.refusal(command.Key, ReasonTooEarly)
	case clearance.RouteGateEpoch <= entry.Validity.GateEpoch || clearance.ActivePins != 0:
		return machine.refusal(command.Key, ReasonPinsActive)
	case clearance.OldestRetryApplied <= entry.Validity.SourceAppliedFloor:
		return machine.refusal(command.Key, ReasonRetryWindow)
	case clearance.AuthorityRevision < max(entry.PublishedRevision, entry.ActiveRevision):
		return machine.refusal(command.Key, ReasonStaleRevision)
	case machine.revision == ^uint64(0):
		return machine.refusal(command.Key, ReasonCapacity)
	}
	machine.revision++
	machine.live--
	machine.tombstones++
	tomb := tombstone{
		CatalogGeneration: clearance.CatalogGeneration,
		AuthorityEpoch:    machine.authorityEpoch,
		PrunedRevision:    machine.revision,
	}
	tomb.Certificate = tombstoneCertificate(command.Key, tomb)
	if write {
		delete(machine.entries, command.Key)
		machine.retired[command.Key] = tomb
	}
	return machine.outcome(
		command.Key, ReasonPruned, true, EntryInvalid, tomb.Certificate,
	)
}

func (machine *Machine) compactRetired(command Command, write bool) Outcome {
	if command.ExpectedRevision != machine.revision {
		return machine.refusal(command.Key, ReasonStaleRevision)
	}
	if command.NextAuthorityEpoch != machine.authorityEpoch+1 ||
		machine.authorityEpoch == ^uint64(0) {
		return machine.refusal(command.Key, ReasonStaleAuthority)
	}
	if machine.tombstones == 0 {
		return machine.refusal(command.Key, ReasonNotFound)
	}
	if machine.revision == ^uint64(0) {
		return machine.refusal(command.Key, ReasonCapacity)
	}
	oldEpoch := machine.authorityEpoch
	machine.revision++
	machine.authorityEpoch = command.NextAuthorityEpoch
	machine.tombstones = 0
	if write {
		clear(machine.retired)
	}
	certificate := compactCertificate(
		machine.authority, oldEpoch, machine.authorityEpoch, machine.revision, command.Key,
	)
	return machine.outcome(command.Key, ReasonCompacted, true, EntryInvalid, certificate)
}

func (machine *Machine) outcome(
	key Digest,
	reason Reason,
	mutated bool,
	state EntryState,
	certificate Digest,
) Outcome {
	return Outcome{
		Reason: reason, Mutated: mutated, State: state,
		Revision: machine.revision, Live: machine.live, Tombstones: machine.tombstones,
		Key: key, Certificate: certificate,
	}
}

func (machine *Machine) refusal(key Digest, reason Reason) Outcome {
	return machine.outcome(
		key, reason, false, EntryInvalid,
		refusalCertificate(machine.authority, machine.authorityEpoch, machine.revision, key, reason),
	)
}

// DrainBinding returns an exclusive route-gate binding only after forwarding
// is active and before the successor catalog generation is visible. This is
// the exact publish-before-drain/move/split seam.
func (machine *Machine) DrainBinding(key Digest, currentCatalog uint64) (routegate.Binding, bool) {
	if machine == nil || key == (Digest{}) || currentCatalog == 0 {
		return routegate.Binding{}, false
	}
	entry, found := machine.entries[key]
	if !found || entry.State != EntryActive || currentCatalog >= entry.Validity.ValidFromCatalog {
		return routegate.Binding{}, false
	}
	certificate := entryCertificate(key, entry)
	return routegate.Binding(certificate), certificate != (Digest{})
}

func entryCertificate(key Digest, entry retainedEntry) Digest {
	if entry.Certificate != (Digest{}) {
		return entry.Certificate
	}
	var encoded [EntryBytes]byte
	bytes, err := AppendEntry(encoded[:0], entry.Entry)
	if err != nil {
		return Digest{}
	}
	h := sha256.New()
	_, _ = h.Write([]byte(entryCertificateDomain))
	_, _ = h.Write(key[:])
	_, _ = h.Write(bytes)
	var scalars [17]byte
	scalars[0] = byte(entry.State)
	binary.LittleEndian.PutUint64(scalars[1:9], entry.PublishedRevision)
	binary.LittleEndian.PutUint64(scalars[9:17], entry.ActiveRevision)
	_, _ = h.Write(scalars[:])
	var digest Digest
	_ = h.Sum(digest[:0])
	return digest
}

func tombstoneCertificate(key Digest, tomb tombstone) Digest {
	var material [32 + 8 + 8 + 8]byte
	copy(material[:32], key[:])
	binary.LittleEndian.PutUint64(material[32:40], tomb.CatalogGeneration)
	binary.LittleEndian.PutUint64(material[40:48], tomb.AuthorityEpoch)
	binary.LittleEndian.PutUint64(material[48:56], tomb.PrunedRevision)
	return domainDigest(tombstoneDigestDomain, material[:])
}

func refusalCertificate(authority Digest, epoch, revision uint64, key Digest, reason Reason) Digest {
	var material [32 + 8 + 8 + 32 + 1]byte
	copy(material[:32], authority[:])
	binary.LittleEndian.PutUint64(material[32:40], epoch)
	binary.LittleEndian.PutUint64(material[40:48], revision)
	copy(material[48:80], key[:])
	material[80] = byte(reason)
	return Digest(sha256.Sum256(material[:]))
}

func compactCertificate(
	authority Digest,
	oldEpoch uint64,
	newEpoch uint64,
	revision uint64,
	key Digest,
) Digest {
	var material [32 + 8 + 8 + 8 + 32]byte
	copy(material[:32], authority[:])
	binary.LittleEndian.PutUint64(material[32:40], oldEpoch)
	binary.LittleEndian.PutUint64(material[40:48], newEpoch)
	binary.LittleEndian.PutUint64(material[48:56], revision)
	copy(material[56:], key[:])
	return Digest(sha256.Sum256(material[:]))
}
